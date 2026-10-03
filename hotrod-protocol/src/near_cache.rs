//! Near caching: a bounded, client-side cache of recently read entries,
//! invalidated through a `CacheListener` (phase 5 of ADR 0001; see
//! `docs/adr/0007-near-caching.md` for the decisions this module
//! implements).
//!
//! This adds no wire surface beyond what `listener.rs` (#4) already
//! has. A `NearCachedCache` is a `RemoteCache` plus an ordinary client
//! listener, interested only in `Modified`/`Removed`/`Expired`. A
//! freshly created entry was never locally cached, so there is
//! nothing to invalidate.
//!
//! If the listener's connection drops, this client does not reconnect
//! it, the same decision `listener.rs` already made for
//! `CacheListener` itself. The near cache instead clears itself and
//! stops serving from the local store. Every `get` falls back to
//! plain passthrough from then on, rather than risk serving data no
//! invalidation feed can ever correct again.
//!
//! Does not track per-entry lifespan or max idle. A local hit never
//! reaches the server, so it never refreshes a `max_idle` timer there
//! either; an entry can also outlive its own `lifespan` locally until
//! an `Expired` event (or capacity pressure) removes it. Honoring
//! either would need fetching entry metadata, not just a value (see
//! `connection::VersionedValue`), which this phase leaves out. Treat
//! near caching as an optimization for data that does not depend on
//! either expiration to be observed promptly.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::task::JoinHandle;

use crate::error::Result;
use crate::listener::{CacheEvent, CacheEventInterests, ListenOptions};
use crate::remote_cache::RemoteCache;
use crate::wire::Expiration;

/// Options for `RemoteCache::near_cache`, kept as its own type so a
/// future option (a bloom filter, a TTL override, see
/// `docs/adr/0007-near-caching.md`) can be added without breaking a
/// caller that builds this with `..Default::default()`.
#[derive(Debug, Clone, Copy)]
pub struct NearCacheOptions {
    /// How many entries the local cache keeps before evicting the least
    /// recently used one. `0` is a valid, if unusual, choice: every
    /// entry is evicted as soon as it is inserted, so `get` always
    /// reaches the network. Not an error, since nothing about this
    /// type requires caching anything to be correct.
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
///
/// `generation` counts every call to `invalidate`/`clear`, whether or
/// not the key they name is actually present. `insert_if_current`
/// checks it to close a race `get` would otherwise have: a value
/// fetched from the remote cache can be stale by the time the fetch
/// returns, if an invalidating event for that same key arrived while
/// the fetch was still in flight and found nothing yet to invalidate.
/// Comparing generations (checked and advanced under the same lock
/// that guards every other mutation) catches that case even though
/// the key itself was never in `entries` to begin with.
struct LruStore {
    max_entries: usize,
    tick: u64,
    generation: u64,
    entries: HashMap<Vec<u8>, (Vec<u8>, u64)>,
    order: BTreeMap<u64, Vec<u8>>,
}

impl LruStore {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            tick: 0,
            generation: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.tick += 1;
        self.tick
    }

    fn generation(&self) -> u64 {
        self.generation
    }

    fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let new_tick = self.next_tick();
        let entry = self.entries.get_mut(key)?;
        let old_tick = entry.1;
        entry.1 = new_tick;
        let value = entry.0.clone();
        self.order.remove(&old_tick);
        self.order.insert(new_tick, key.to_vec());
        Some(value)
    }

    /// Inserts `key`/`value`, but only if `generation` still matches:
    /// otherwise some invalidation landed since the caller fetched
    /// this value, and inserting it now could resurrect a value an
    /// event already tried to invalidate. See the struct docs.
    fn insert_if_current(&mut self, key: Vec<u8>, value: Vec<u8>, generation: u64) {
        if generation != self.generation {
            return;
        }
        if let Some((_, old_tick)) = self.entries.get(&key) {
            self.order.remove(old_tick);
        }
        let new_tick = self.next_tick();
        self.order.insert(new_tick, key.clone());
        self.entries.insert(key, (value, new_tick));
        while self.entries.len() > self.max_entries {
            let Some((_, oldest_key)) = self.order.pop_first() else {
                break;
            };
            self.entries.remove(&oldest_key);
        }
    }

    fn invalidate(&mut self, key: &[u8]) {
        self.generation = self.generation.wrapping_add(1);
        if let Some((_, tick)) = self.entries.remove(key) {
            self.order.remove(&tick);
        }
    }

    fn clear(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.entries.clear();
        self.order.clear();
    }

    /// `insert_if_current` unconditionally, for tests that do not care
    /// about the generation check itself.
    #[cfg(test)]
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let generation = self.generation;
        self.insert_if_current(key, value, generation);
    }
}

/// Shared state between `NearCachedCache` and its background
/// invalidation task. Locking is synchronous (`std::sync::Mutex`) and
/// never held across an `.await`: every operation on `LruStore` is a
/// plain, non-blocking map access.
struct NearCacheState {
    alive: AtomicBool,
    store: Mutex<LruStore>,
}

impl NearCacheState {
    fn new(max_entries: usize) -> Self {
        Self {
            alive: AtomicBool::new(true),
            store: Mutex::new(LruStore::new(max_entries)),
        }
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LruStore> {
        self.store.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.lock().get(key)
    }

    fn generation(&self) -> u64 {
        self.lock().generation()
    }

    fn insert_if_current(&self, key: Vec<u8>, value: Vec<u8>, generation: u64) {
        self.lock().insert_if_current(key, value, generation);
    }

    fn invalidate(&self, key: &[u8]) {
        self.lock().invalidate(key);
    }

    fn clear(&self) {
        self.lock().clear();
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

/// Marks a `NearCacheState` dead when dropped, whether that happens
/// because the invalidation task's loop exited normally or because a
/// panic unwound through it: either way, this is the only place that
/// task's exit is observable, so it is the only place that can be
/// trusted to run the fail-safe.
struct MarkDeadOnDrop(Arc<NearCacheState>);

impl Drop for MarkDeadOnDrop {
    fn drop(&mut self) {
        self.0.mark_dead();
    }
}

/// A `RemoteCache` wrapped with a bounded, listener-invalidated local
/// cache for `get`, obtained from `RemoteCache::near_cache`.
///
/// `put` and `remove` invalidate their key locally right away, on top
/// of the background listener. A `put` or `remove` reaching this cache
/// through any other path, a plain `RemoteCache` handle bypassing this
/// wrapper entirely, or another client altogether, is still caught,
/// just on the listener's own asynchronous delay instead of
/// immediately: it generates the same event, and the background
/// listener receives it regardless of which API caused it. `replace`,
/// `put_if_absent`, the versioned operations, and `get_all`/`put_all`
/// are not overridden at all (see `Deref` below): they stay correct
/// the same asynchronous way, just without the immediate, synchronous
/// invalidation `put`/`remove` add on top.
///
/// `clear` is the one write the listener cannot catch on its own: the
/// protocol sends no per-key event for it, so only
/// `NearCachedCache::clear` itself keeps the local cache in sync with
/// a `clear` through this handle. A `clear` through any other handle
/// leaves this cache's local entries stale with nothing to correct
/// them.
///
/// Every operation `RemoteCache` has beyond `get`/`put`/`remove`/
/// `clear` is reached through `Deref`, unmodified. Not `Clone`: wrap
/// it in an `Arc` to share it across tasks.
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
            // Marks the feed dead no matter how this task ends: normal
            // loop exit, or a panic unwinding through it. Without this
            // guard, a panic (say, inside a future bug in `invalidate`)
            // would skip the explicit `mark_dead` call entirely, leaving
            // `alive` stuck `true` with no invalidation feed left to
            // keep it honest.
            let _mark_dead_on_exit = MarkDeadOnDrop(task_state.clone());
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
    /// The local store is not updated if an invalidation for this key
    /// (or a `clear`) happened while the fetch was in flight: see
    /// `LruStore`'s docs for why that matters.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // No generation snapshot, and so no later insert, once the feed
        // is already dead: it would only be thrown away, and computing
        // it still means locking the store for nothing.
        let generation = if self.state.is_alive() {
            if let Some(value) = self.state.get(key) {
                return Ok(Some(value));
            }
            Some(self.state.generation())
        } else {
            None
        };
        let value = self.cache.get(key).await?;
        if let (Some(generation), Some(value)) = (generation, value.as_ref()) {
            self.state
                .insert_if_current(key.to_vec(), value.clone(), generation);
        }
        Ok(value)
    }

    /// Runs `op` against the remote cache, then invalidates local state
    /// via `invalidate` regardless of whether `op` succeeded: a write
    /// whose response was lost to a timeout or a connection error may
    /// still have landed server-side, and invalidating state that in
    /// fact never changed only costs a future cache miss, while not
    /// invalidating state that did risks it never getting corrected at
    /// all. Shared by `put`, `remove` and `clear` so this reasoning
    /// lives in one place rather than three.
    async fn invalidate_after<T>(
        &self,
        op: impl Future<Output = Result<T>>,
        invalidate: impl FnOnce(&NearCacheState),
    ) -> Result<T> {
        let result = op.await;
        invalidate(&self.state);
        result
    }

    /// Writes through to the remote cache, then invalidates the local
    /// entry: the listener invalidates the same key too, once its event
    /// arrives, but this closes the gap immediately instead of waiting
    /// on that round trip.
    pub async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        self.invalidate_after(self.cache.put(key, value, lifespan, max_idle), |state| {
            state.invalidate(key)
        })
        .await
    }

    /// Same reasoning as `put`.
    pub async fn remove(&self, key: &[u8]) -> Result<bool> {
        self.invalidate_after(self.cache.remove(key), |state| state.invalidate(key))
            .await
    }

    /// Clears the remote cache, then the local one. The listener cannot
    /// help here: it gets no per-key event for a `clear`, so this is
    /// the only way the local cache ever learns about one.
    pub async fn clear(&self) -> Result<()> {
        self.invalidate_after(self.cache.clear(), |state| state.clear())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_none_for_a_missing_key() {
        let mut store = LruStore::new(10);
        assert_eq!(store.get(b"missing"), None);
    }

    #[test]
    fn insert_then_get_round_trips_the_value() {
        let mut store = LruStore::new(10);
        store.insert(b"key".to_vec(), b"value".to_vec());
        assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
    }

    #[test]
    fn insert_overwrites_an_existing_key_without_growing() {
        let mut store = LruStore::new(10);
        store.insert(b"key".to_vec(), b"first".to_vec());
        store.insert(b"key".to_vec(), b"second".to_vec());
        assert_eq!(store.get(b"key"), Some(b"second".to_vec()));
        assert_eq!(store.entries.len(), 1);
        assert_eq!(store.order.len(), 1);
    }

    #[test]
    fn invalidate_removes_the_entry() {
        let mut store = LruStore::new(10);
        store.insert(b"key".to_vec(), b"value".to_vec());
        store.invalidate(b"key");
        assert_eq!(store.get(b"key"), None);
    }

    #[test]
    fn invalidate_on_a_missing_key_is_a_no_op() {
        let mut store = LruStore::new(10);
        store.invalidate(b"missing");
        assert_eq!(store.entries.len(), 0);
    }

    #[test]
    fn clear_empties_the_store() {
        let mut store = LruStore::new(10);
        store.insert(b"a".to_vec(), b"1".to_vec());
        store.insert(b"b".to_vec(), b"2".to_vec());
        store.clear();
        assert_eq!(store.get(b"a"), None);
        assert_eq!(store.get(b"b"), None);
        assert_eq!(store.entries.len(), 0);
        assert_eq!(store.order.len(), 0);
    }

    #[test]
    fn invalidate_bumps_the_generation_even_for_a_missing_key() {
        let mut store = LruStore::new(10);
        let generation = store.generation();
        store.invalidate(b"never-cached");
        assert_ne!(store.generation(), generation);
    }

    #[test]
    fn clear_bumps_the_generation() {
        let mut store = LruStore::new(10);
        let generation = store.generation();
        store.clear();
        assert_ne!(store.generation(), generation);
    }

    /// The race `insert_if_current` exists to close: a `get` snapshots
    /// the generation before fetching from the remote cache, and an
    /// invalidation for that same key can land while the fetch is
    /// still in flight, before the key was ever in the store to
    /// invalidate. Skipping the insert once the generation has moved
    /// on, rather than inserting unconditionally, is what keeps that
    /// fetch from resurrecting a value the invalidation already meant
    /// to discard.
    #[test]
    fn insert_if_current_is_skipped_once_the_generation_moved_on() {
        let mut store = LruStore::new(10);
        let generation = store.generation();
        store.invalidate(b"key"); // not cached yet: a no-op besides the bump
        store.insert_if_current(b"key".to_vec(), b"stale".to_vec(), generation);
        assert_eq!(store.get(b"key"), None);
    }

    #[test]
    fn insert_if_current_succeeds_when_the_generation_is_unchanged() {
        let mut store = LruStore::new(10);
        let generation = store.generation();
        store.insert_if_current(b"key".to_vec(), b"value".to_vec(), generation);
        assert_eq!(store.get(b"key"), Some(b"value".to_vec()));
    }

    /// `register`'s background task relies on `MarkDeadOnDrop` to call
    /// `mark_dead` no matter how the task ends, specifically including a
    /// panic unwinding through it, not just its normal loop exit: a
    /// panic skips every statement after it, so if `mark_dead` were only
    /// called explicitly after the loop (as it once was), a panic would
    /// leave `alive` stuck `true` forever with no invalidation feed left
    /// to keep it honest.
    #[test]
    fn mark_dead_on_drop_runs_even_if_the_owning_scope_panics() {
        let state = Arc::new(NearCacheState::new(10));
        state.insert_if_current(b"key".to_vec(), b"value".to_vec(), state.generation());
        assert!(state.is_alive());

        let guard_state = state.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = MarkDeadOnDrop(guard_state);
            panic!("simulated panic inside the invalidation task");
        }));
        assert!(result.is_err());

        assert!(!state.is_alive());
        assert_eq!(state.get(b"key"), None);
    }

    #[test]
    fn insert_past_capacity_evicts_the_least_recently_used_entry() {
        let mut store = LruStore::new(2);
        store.insert(b"a".to_vec(), b"1".to_vec());
        store.insert(b"b".to_vec(), b"2".to_vec());
        store.insert(b"c".to_vec(), b"3".to_vec());

        assert_eq!(store.get(b"a"), None, "a was the least recently used");
        assert_eq!(store.get(b"b"), Some(b"2".to_vec()));
        assert_eq!(store.get(b"c"), Some(b"3".to_vec()));
    }

    #[test]
    fn get_refreshes_recency_so_it_survives_the_next_eviction() {
        let mut store = LruStore::new(2);
        store.insert(b"a".to_vec(), b"1".to_vec());
        store.insert(b"b".to_vec(), b"2".to_vec());
        // Touch "a" so "b" becomes the least recently used instead.
        store.get(b"a");
        store.insert(b"c".to_vec(), b"3".to_vec());

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
        use std::net::SocketAddr;
        use std::time::Duration;

        use tokio::io::AsyncWriteExt;
        use tokio::net::{TcpListener, TcpStream};

        use super::*;
        use crate::client::tests::{read_request_opcode, response_header};
        use crate::client::HotRodClient;
        use crate::listener::tests::{event_frame, read_listener_id};
        use crate::remote_cache::tests::client_with_seeds_and_timeout;
        use crate::wire::{read_array, write_array};

        fn client_with_seed_and_timeout(seed_addr: SocketAddr, timeout: Duration) -> HotRodClient {
            client_with_seeds_and_timeout(vec![seed_addr], seed_addr, timeout)
        }

        fn client_with_seed(seed_addr: SocketAddr) -> HotRodClient {
            client_with_seed_and_timeout(seed_addr, Duration::from_secs(5))
        }

        /// Accepts the first connection on `tcp` and completes an
        /// `AddClientListener` registration on it: every wiring test below
        /// needs this before doing anything test-specific, and they only
        /// differ in what happens on the connection afterward.
        async fn accept_and_register_listener(tcp: &TcpListener) -> (Vec<u8>, TcpStream) {
            let (mut listen_sock, _) = tcp.accept().await.unwrap();
            let (id, listener_id) = read_listener_id(&mut listen_sock).await;
            listen_sock
                .write_all(&response_header(id, 0x26, 0x00))
                .await
                .unwrap();
            (listener_id, listen_sock)
        }

        /// Reads one Get request off `sock` and responds with `value`,
        /// success. Shared by every wiring test below that needs the
        /// server side of a Get round trip.
        async fn serve_get(sock: &mut TcpStream, value: &[u8]) {
            let (id, opcode) = read_request_opcode(sock).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(sock).await.unwrap();
            let mut resp = response_header(id, 0x04, 0x00);
            write_array(&mut resp, value);
            sock.write_all(&resp).await.unwrap();
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
                let (_listener_id, listen_sock) = accept_and_register_listener(&tcp).await;

                let (mut op_sock, _) = tcp.accept().await.unwrap();
                serve_get(&mut op_sock, b"value").await;

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
                let (listener_id, mut listen_sock) = accept_and_register_listener(&tcp).await;

                let (mut op_sock, _) = tcp.accept().await.unwrap();
                serve_get(&mut op_sock, b"value").await;

                // Modified event for the same key: opcode 0x61, no
                // `isCustom`, not a retry, with a version (see
                // `listener.rs`'s wire notes).
                let frame = event_frame(0, 0x61, &listener_id, 0, false, b"key", Some(7));
                listen_sock.write_all(&frame).await.unwrap();

                // A second Get, once the entry was invalidated.
                serve_get(&mut op_sock, b"value-2").await;

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
                let (_listener_id, listen_sock) = accept_and_register_listener(&tcp).await;

                let (mut op_sock, _) = tcp.accept().await.unwrap();
                serve_get(&mut op_sock, b"value").await;

                // The listener connection dies: no reconnection, so the
                // near cache must fall back to passthrough from here on.
                drop(listen_sock);

                // A second Get, once the feed died, bypassing any local
                // cache.
                serve_get(&mut op_sock, b"value-after-death").await;
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

        /// `put`'s write times out (the server never responds), yet the
        /// local entry is still invalidated: the timed-out connection is
        /// not returned to the pool, so the next `get` opens a new one.
        #[tokio::test]
        async fn put_invalidates_the_local_entry_even_when_the_write_times_out() {
            let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = tcp.local_addr().unwrap();

            let server = tokio::spawn(async move {
                let (_listener_id, listen_sock) = accept_and_register_listener(&tcp).await;

                let (mut op_sock, _) = tcp.accept().await.unwrap();
                serve_get(&mut op_sock, b"value").await;

                // A Put that never gets a response.
                let (_id, opcode) = read_request_opcode(&mut op_sock).await;
                assert_eq!(opcode, 0x01, "expected a Put request");

                // A new connection for the second Get: the timed-out one
                // is not returned to the pool.
                let (mut retry_sock, _) = tcp.accept().await.unwrap();
                serve_get(&mut retry_sock, b"value-after-timeout").await;

                listen_sock
            });

            let client = client_with_seed_and_timeout(addr, Duration::from_millis(200));
            let cache = client.cache("my-cache");
            let near = cache
                .near_cache(NearCacheOptions::default())
                .await
                .expect("near_cache should register its listener");

            assert_eq!(near.get(b"key").await.unwrap(), Some(b"value".to_vec()));

            let put_result = near
                .put(
                    b"key",
                    b"new-value",
                    Expiration::Default,
                    Expiration::Default,
                )
                .await;
            assert!(
                matches!(put_result, Err(crate::error::Error::Timeout(_))),
                "expected the put itself to time out, got {put_result:?}"
            );

            // The local entry must already be gone: no polling needed,
            // `put` invalidates before returning, successful or not.
            assert_eq!(
                near.get(b"key").await.unwrap(),
                Some(b"value-after-timeout".to_vec())
            );

            drop(server.await.unwrap());
        }

        /// Same reasoning as the `put` test above, for `clear`: the
        /// remote clear times out, but the local cache is wiped anyway.
        #[tokio::test]
        async fn clear_invalidates_the_local_cache_even_when_it_times_out() {
            let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = tcp.local_addr().unwrap();

            let server = tokio::spawn(async move {
                let (_listener_id, listen_sock) = accept_and_register_listener(&tcp).await;

                let (mut op_sock, _) = tcp.accept().await.unwrap();
                serve_get(&mut op_sock, b"value").await;

                // A Clear that never gets a response.
                let (_id, opcode) = read_request_opcode(&mut op_sock).await;
                assert_eq!(opcode, 0x13, "expected a Clear request");

                // A new connection for the second Get: the timed-out one
                // is not returned to the pool.
                let (mut retry_sock, _) = tcp.accept().await.unwrap();
                serve_get(&mut retry_sock, b"value-after-timeout").await;

                listen_sock
            });

            let client = client_with_seed_and_timeout(addr, Duration::from_millis(200));
            let cache = client.cache("my-cache");
            let near = cache
                .near_cache(NearCacheOptions::default())
                .await
                .expect("near_cache should register its listener");

            assert_eq!(near.get(b"key").await.unwrap(), Some(b"value".to_vec()));

            let clear_result = near.clear().await;
            assert!(
                matches!(clear_result, Err(crate::error::Error::Timeout(_))),
                "expected the clear itself to time out, got {clear_result:?}"
            );

            // No polling needed: `clear` invalidates before returning,
            // successful or not.
            assert_eq!(
                near.get(b"key").await.unwrap(),
                Some(b"value-after-timeout".to_vec())
            );

            drop(server.await.unwrap());
        }
    }
}
