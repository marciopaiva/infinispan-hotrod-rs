//! Near caching: a bounded, client-side cache of recently read entries,
//! invalidated through a `CacheListener` (phase 5 of ADR 0001; see
//! `docs/adr/0007-near-caching.md` for the decisions this module
//! implements).
//!
//! This adds no wire surface beyond what `listener.rs` (#4) already
//! has: a `NearCachedCache` is a `RemoteCache` plus an ordinary client
//! listener, interested only in `Modified`/`Removed`/`Expired` (a
//! freshly created entry was never locally cached, so there is nothing
//! to invalidate). If that listener's connection drops, this client
//! does not reconnect it (the same decision `listener.rs` already made
//! for `CacheListener` itself): the near cache instead clears itself
//! and stops serving from the local store, falling back to plain
//! passthrough rather than risk serving data no invalidation feed can
//! ever correct again.

use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::task::JoinHandle;

use crate::error::Result;
use crate::listener::{CacheEvent, CacheEventInterests, ListenOptions};
use crate::remote_cache::RemoteCache;
use crate::wire::Expiration;

/// Options for `RemoteCache::near_cache`.
#[derive(Debug, Clone, Copy)]
pub struct NearCacheOptions {
    /// How many entries the local cache keeps before evicting the least
    /// recently used one.
    pub max_entries: usize,
}

impl Default for NearCacheOptions {
    fn default() -> Self {
        Self {
            max_entries: 10_000,
        }
    }
}

/// A key/value store bounded to `max_entries`, evicting the least
/// recently used entry once over that bound. `entries` gives O(1) lookup
/// by key; `order` maps a monotonic access tick to its key, giving
/// `BTreeMap::pop_first`-based O(log n) access to the least recently
/// used one. This crate forbids `unsafe`, which rules out the usual
/// O(1) LRU (an index-linked list); a near cache's typical size does
/// not call for that complexity anyway.
struct LruStore {
    tick: u64,
    entries: HashMap<Vec<u8>, (Vec<u8>, u64)>,
    order: BTreeMap<u64, Vec<u8>>,
}

impl LruStore {
    fn new() -> Self {
        Self {
            tick: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let (value, old_tick) = self.entries.get(key)?.clone();
        self.order.remove(&old_tick);
        let new_tick = self.next_tick();
        self.order.insert(new_tick, key.to_vec());
        self.entries.insert(key.to_vec(), (value.clone(), new_tick));
        Some(value)
    }

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>, max_entries: usize) {
        if let Some((_, old_tick)) = self.entries.get(&key) {
            self.order.remove(old_tick);
        }
        let new_tick = self.next_tick();
        self.order.insert(new_tick, key.clone());
        self.entries.insert(key, (value, new_tick));
        while self.entries.len() > max_entries {
            let Some((&oldest_tick, _)) = self.order.iter().next() else {
                break;
            };
            if let Some(oldest_key) = self.order.remove(&oldest_tick) {
                self.entries.remove(&oldest_key);
            }
        }
    }

    fn invalidate(&mut self, key: &[u8]) {
        if let Some((_, tick)) = self.entries.remove(key) {
            self.order.remove(&tick);
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

/// Shared state between `NearCachedCache` and its background
/// invalidation task. Locking is synchronous (`std::sync::Mutex`) and
/// never held across an `.await`: every operation on `LruStore` is a
/// plain, non-blocking map access.
struct NearCacheState {
    max_entries: usize,
    alive: AtomicBool,
    store: Mutex<LruStore>,
}

impl NearCacheState {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            alive: AtomicBool::new(true),
            store: Mutex::new(LruStore::new()),
        }
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
    }

    fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key, value, self.max_entries);
    }

    fn invalidate(&self, key: &[u8]) {
        self.store
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .invalidate(key);
    }

    fn clear(&self) {
        self.store.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }

    /// The invalidation feed is gone for good (no reconnection, see the
    /// module docs): drop everything cached so far rather than let it
    /// grow stale forever, and stop the local cache from being
    /// consulted at all from now on.
    fn mark_dead(&self) {
        self.alive.store(false, Ordering::Release);
        self.clear();
    }
}

/// A `RemoteCache` wrapped with a bounded, listener-invalidated local
/// cache for `get`, obtained from `RemoteCache::near_cache`. Every
/// operation `RemoteCache` has beyond `get`/`put`/`remove`/`clear` is
/// reached through `Deref`, unmodified: a write through any of those,
/// through a plain `RemoteCache` bypassing this wrapper entirely, or
/// from another client altogether, still invalidates this cache's
/// local entry, because the listener this type runs in the background
/// receives that event regardless of which API caused it. Not
/// `Clone`: wrap it in an `Arc` to share it across tasks.
pub struct NearCachedCache {
    cache: RemoteCache,
    state: Arc<NearCacheState>,
    invalidation_task: JoinHandle<()>,
}

impl Deref for NearCachedCache {
    type Target = RemoteCache;

    fn deref(&self) -> &RemoteCache {
        &self.cache
    }
}

impl Drop for NearCachedCache {
    fn drop(&mut self) {
        // Aborting drops the task's `CacheListener` with it, which
        // closes its connection; the server clears the listener
        // registration once it notices, same as a bare `CacheListener`
        // drop (see `listener.rs`'s module docs).
        self.invalidation_task.abort();
    }
}

impl NearCachedCache {
    pub(crate) async fn register(cache: RemoteCache, options: NearCacheOptions) -> Result<Self> {
        let listen_options = ListenOptions {
            interests: CacheEventInterests {
                created: false,
                modified: true,
                removed: true,
                expired: true,
            },
            ..ListenOptions::default()
        };
        let mut listener = cache.listen_with(&listen_options).await?;
        let state = Arc::new(NearCacheState::new(options.max_entries));
        let task_state = state.clone();
        let invalidation_task = tokio::spawn(async move {
            loop {
                match listener.next().await {
                    Some(Ok(
                        CacheEvent::Modified { key, .. }
                        | CacheEvent::Removed { key, .. }
                        | CacheEvent::Expired { key, .. },
                    )) => task_state.invalidate(&key),
                    Some(Ok(CacheEvent::Created { .. } | CacheEvent::Custom { .. })) => {}
                    None | Some(Err(_)) => break,
                }
            }
            task_state.mark_dead();
        });
        Ok(Self {
            cache,
            state,
            invalidation_task,
        })
    }

    /// Returns `key`'s value from the local cache if present and the
    /// invalidation feed is still alive; otherwise fetches it from the
    /// remote cache and, on a hit, stores it locally for next time.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.state.is_alive() {
            if let Some(value) = self.state.get(key) {
                return Ok(Some(value));
            }
        }
        let value = self.cache.get(key).await?;
        if self.state.is_alive() {
            if let Some(value) = &value {
                self.state.insert(key.to_vec(), value.clone());
            }
        }
        Ok(value)
    }

    /// Writes through to the remote cache, then invalidates the local
    /// entry: the listener will invalidate it too once its event
    /// arrives, but only after a round trip this closes immediately for
    /// the caller's own writes.
    pub async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        self.cache.put(key, value, lifespan, max_idle).await?;
        self.state.invalidate(key);
        Ok(())
    }

    /// Same reasoning as `put`: remove through to the remote cache
    /// first, then drop the local entry immediately.
    pub async fn remove(&self, key: &[u8]) -> Result<bool> {
        let removed = self.cache.remove(key).await?;
        self.state.invalidate(key);
        Ok(removed)
    }

    /// Clears the remote cache, then the local one: the listener does
    /// not get a per-key event for every entry a `clear` removes, so
    /// this is the only way the local cache would learn about it.
    pub async fn clear(&self) -> Result<()> {
        self.cache.clear().await?;
        self.state.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_with_capacity(max_entries: usize) -> (LruStore, usize) {
        (LruStore::new(), max_entries)
    }

    #[test]
    fn get_returns_none_for_a_missing_key() {
        let (mut store, _) = store_with_capacity(10);
        assert_eq!(store.get(b"missing"), None);
    }

    #[test]
    fn insert_then_get_round_trips_the_value() {
        let (mut store, cap) = store_with_capacity(10);
        store.insert(b"key".to_vec(), b"value".to_vec(), cap);
        assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
    }

    #[test]
    fn insert_overwrites_an_existing_key_without_growing() {
        let (mut store, cap) = store_with_capacity(10);
        store.insert(b"key".to_vec(), b"first".to_vec(), cap);
        store.insert(b"key".to_vec(), b"second".to_vec(), cap);
        assert_eq!(store.get(b"key"), Some(b"second".to_vec()));
        assert_eq!(store.entries.len(), 1);
        assert_eq!(store.order.len(), 1);
    }

    #[test]
    fn invalidate_removes_the_entry() {
        let (mut store, cap) = store_with_capacity(10);
        store.insert(b"key".to_vec(), b"value".to_vec(), cap);
        store.invalidate(b"key");
        assert_eq!(store.get(b"key"), None);
    }

    #[test]
    fn invalidate_on_a_missing_key_is_a_no_op() {
        let (mut store, _) = store_with_capacity(10);
        store.invalidate(b"missing");
        assert_eq!(store.entries.len(), 0);
    }

    #[test]
    fn clear_empties_the_store() {
        let (mut store, cap) = store_with_capacity(10);
        store.insert(b"a".to_vec(), b"1".to_vec(), cap);
        store.insert(b"b".to_vec(), b"2".to_vec(), cap);
        store.clear();
        assert_eq!(store.get(b"a"), None);
        assert_eq!(store.get(b"b"), None);
        assert_eq!(store.entries.len(), 0);
        assert_eq!(store.order.len(), 0);
    }

    #[test]
    fn insert_past_capacity_evicts_the_least_recently_used_entry() {
        let (mut store, cap) = store_with_capacity(2);
        store.insert(b"a".to_vec(), b"1".to_vec(), cap);
        store.insert(b"b".to_vec(), b"2".to_vec(), cap);
        store.insert(b"c".to_vec(), b"3".to_vec(), cap);

        assert_eq!(store.get(b"a"), None, "a was the least recently used");
        assert_eq!(store.get(b"b"), Some(b"2".to_vec()));
        assert_eq!(store.get(b"c"), Some(b"3".to_vec()));
    }

    #[test]
    fn get_refreshes_recency_so_it_survives_the_next_eviction() {
        let (mut store, cap) = store_with_capacity(2);
        store.insert(b"a".to_vec(), b"1".to_vec(), cap);
        store.insert(b"b".to_vec(), b"2".to_vec(), cap);
        // Touch "a" so "b" becomes the least recently used instead.
        store.get(b"a");
        store.insert(b"c".to_vec(), b"3".to_vec(), cap);

        assert_eq!(store.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b"), None, "b was the least recently used");
        assert_eq!(store.get(b"c"), Some(b"3".to_vec()));
    }

    /// `NearCachedCache` wired end to end against a fake Hot Rod server:
    /// a `get` caching locally, a pushed event invalidating it, and the
    /// fail-safe once the listener connection dies. The `LruStore`
    /// itself is exhaustively covered above without any I/O; these
    /// exercise the wiring around it instead.
    mod wiring {
        use std::collections::HashMap as StdHashMap;
        use std::net::SocketAddr;
        use std::sync::RwLock;
        use std::time::Duration;

        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;

        use super::*;
        use crate::client::tests::{read_request_opcode, response_header};
        use crate::client::{ClientInner, HotRodClient};
        use crate::listener::tests::{event_frame, read_listener_id};
        use crate::wire::{read_array, write_array};

        fn client_with_seed(seed_addr: SocketAddr) -> HotRodClient {
            HotRodClient::from_inner(ClientInner {
                seed_addrs: vec![seed_addr],
                active_seed_addr: RwLock::new(seed_addr),
                topology: RwLock::new(None),
                node_origin: RwLock::new(StdHashMap::new()),
                pools: RwLock::new(StdHashMap::new()),
                auth: RwLock::new(None),
                tls: None,
                timeout: RwLock::new(Duration::from_secs(5)),
            })
        }

        /// Polls `near.get(key)` until it returns `expected` or `attempts`
        /// run out, instead of a fixed sleep: the background invalidation
        /// task races the test's own calls, and a cache hit (no network
        /// round trip at all) is a legitimate, repeatable outcome while
        /// that task has not caught up yet.
        async fn poll_until(near: &NearCachedCache, key: &[u8], expected: &[u8]) -> Vec<u8> {
            let mut observed = near.get(key).await.unwrap();
            for _ in 0..200 {
                if observed.as_deref() == Some(expected) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
                observed = near.get(key).await.unwrap();
            }
            observed.expect("key should exist")
        }

        #[tokio::test]
        async fn get_caches_locally_and_skips_the_network_on_a_hit() {
            let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = tcp.local_addr().unwrap();

            let server = tokio::spawn(async move {
                let (mut listen_sock, _) = tcp.accept().await.unwrap();
                let (id, _listener_id) = read_listener_id(&mut listen_sock).await;
                listen_sock
                    .write_all(&response_header(id, 0x26, 0x00))
                    .await
                    .unwrap();

                let (mut op_sock, _) = tcp.accept().await.unwrap();
                let (id, opcode) = read_request_opcode(&mut op_sock).await;
                assert_eq!(opcode, 0x03, "expected a Get request");
                let _key = read_array(&mut op_sock).await.unwrap();
                let mut resp = response_header(id, 0x04, 0x00);
                write_array(&mut resp, b"value");
                op_sock.write_all(&resp).await.unwrap();

                // Kept alive until the test is done with it: dropping it
                // early would close the listener connection and trip the
                // fail-safe this test is not exercising.
                listen_sock
            });

            let client = client_with_seed(addr);
            let cache = client.cache("my-cache");
            let near = cache
                .near_cache(NearCacheOptions::default())
                .await
                .expect("near_cache should register its listener");

            assert_eq!(near.get(b"key").await.unwrap(), Some(b"value".to_vec()));
            // A second `get` for the same key must be served locally: the
            // fake server never accepts a third connection or reads a
            // second request, so this would hang (and the test time out)
            // if it went to the network instead.
            assert_eq!(near.get(b"key").await.unwrap(), Some(b"value".to_vec()));

            drop(server.await.unwrap());
        }

        #[tokio::test]
        async fn a_modified_event_invalidates_the_cached_entry() {
            let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = tcp.local_addr().unwrap();

            let server = tokio::spawn(async move {
                let (mut listen_sock, _) = tcp.accept().await.unwrap();
                let (id, listener_id) = read_listener_id(&mut listen_sock).await;
                listen_sock
                    .write_all(&response_header(id, 0x26, 0x00))
                    .await
                    .unwrap();

                let (mut op_sock, _) = tcp.accept().await.unwrap();
                let (id, opcode) = read_request_opcode(&mut op_sock).await;
                assert_eq!(opcode, 0x03, "expected the first Get request");
                let _key = read_array(&mut op_sock).await.unwrap();
                let mut resp = response_header(id, 0x04, 0x00);
                write_array(&mut resp, b"value");
                op_sock.write_all(&resp).await.unwrap();

                // Modified event for the same key: opcode 0x61, no
                // `isCustom`, not a retry, with a version (see
                // `listener.rs`'s wire notes).
                let frame = event_frame(0, 0x61, &listener_id, 0, false, b"key", Some(7));
                listen_sock.write_all(&frame).await.unwrap();

                let (id, opcode) = read_request_opcode(&mut op_sock).await;
                assert_eq!(
                    opcode, 0x03,
                    "expected a second Get once the entry was invalidated"
                );
                let _key = read_array(&mut op_sock).await.unwrap();
                let mut resp = response_header(id, 0x04, 0x00);
                write_array(&mut resp, b"value-2");
                op_sock.write_all(&resp).await.unwrap();

                listen_sock
            });

            let client = client_with_seed(addr);
            let cache = client.cache("my-cache");
            let near = cache
                .near_cache(NearCacheOptions::default())
                .await
                .expect("near_cache should register its listener");

            assert_eq!(near.get(b"key").await.unwrap(), Some(b"value".to_vec()));

            let after_invalidation = poll_until(&near, b"key", b"value-2").await;
            assert_eq!(after_invalidation, b"value-2");

            drop(server.await.unwrap());
        }

        #[tokio::test]
        async fn the_feed_dying_stops_the_local_cache_from_being_used() {
            let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = tcp.local_addr().unwrap();

            let server = tokio::spawn(async move {
                let (mut listen_sock, _) = tcp.accept().await.unwrap();
                let (id, _listener_id) = read_listener_id(&mut listen_sock).await;
                listen_sock
                    .write_all(&response_header(id, 0x26, 0x00))
                    .await
                    .unwrap();

                let (mut op_sock, _) = tcp.accept().await.unwrap();
                let (id, opcode) = read_request_opcode(&mut op_sock).await;
                assert_eq!(opcode, 0x03, "expected the first Get request");
                let _key = read_array(&mut op_sock).await.unwrap();
                let mut resp = response_header(id, 0x04, 0x00);
                write_array(&mut resp, b"value");
                op_sock.write_all(&resp).await.unwrap();

                // The listener connection dies: no reconnection, so the
                // near cache must fall back to passthrough from here on.
                drop(listen_sock);

                let (id, opcode) = read_request_opcode(&mut op_sock).await;
                assert_eq!(
                    opcode, 0x03,
                    "expected a second Get once the feed died, bypassing any local cache"
                );
                let _key = read_array(&mut op_sock).await.unwrap();
                let mut resp = response_header(id, 0x04, 0x00);
                write_array(&mut resp, b"value-after-death");
                op_sock.write_all(&resp).await.unwrap();
            });

            let client = client_with_seed(addr);
            let cache = client.cache("my-cache");
            let near = cache
                .near_cache(NearCacheOptions::default())
                .await
                .expect("near_cache should register its listener");

            assert_eq!(near.get(b"key").await.unwrap(), Some(b"value".to_vec()));

            let after_death = poll_until(&near, b"key", b"value-after-death").await;
            assert_eq!(after_death, b"value-after-death");

            server.await.unwrap();
        }
    }
}
