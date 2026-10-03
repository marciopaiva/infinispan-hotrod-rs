//! `HotRodClient`: shared, cheaply-cloneable cluster state (topology,
//! per-node connection pools, authentication, TLS config) that one or more
//! `RemoteCache` handles dispatch operations through concurrently.
//!
//! This is the `HotRodClient`/`RemoteCache` split decided in
//! `docs/adr/0005-connection-pooling-and-client-cache-split.md`, replacing
//! the single-cache, `&mut self` `HotRodCluster` of phase 3 (ADR 0003).
//! `HotRodClient` owns the pools and the topology; `RemoteCache`
//! (`remote_cache.rs`) is a thin handle bound to one cache name, obtained
//! from `HotRodClient::cache`. Every operation on a `RemoteCache` takes
//! `&self`, so independent operations, even against the same node, run
//! concurrently from a single shared `HotRodClient`.
//!
//! `HotRodConnection` is unchanged: it keeps its role as the single,
//! sequential, one-cache connection type; `pool.rs`'s `ConnectionPool`
//! holds several of them per node. A pool is keyed by `(SocketAddr, cache
//! name)`, not address alone: `HotRodConnection` still binds to one cache
//! at connect time (see its module docs), so a connection opened for one
//! cache cannot be handed to a `RemoteCache` for another. See ADR 0005's
//! Consequences section for why this wrinkle was not visible until this
//! module was written.
//!
//! The same cancellation hazard documented on `connection`'s module docs
//! applies to each pooled connection here: `HotRodConnection` marks itself
//! poisoned before every request and clears that mark only once the
//! response is read in full, so a connection abandoned mid-operation comes
//! back out of `PooledGuard::drop` poisoned. `pool.rs` checks that mark
//! before returning a connection to its idle list, evicting it instead,
//! the same safeguard `HotRodCluster::ensure_connection_impl` used to
//! provide for the single connection it kept per node.
//!
//! Every `RwLock` here recovers from poisoning (`unwrap_or_else(|p|
//! p.into_inner())`) rather than panicking the whole client if one task
//! panics while holding a write lock: `HotRodClient` is `Clone`d and
//! shared across tasks specifically so independent operations can run
//! concurrently, and one task's unrelated bug should not brick every
//! `RemoteCache` built from the same client by poisoning shared state
//! they all read. `pool.rs`'s own `Mutex` already followed this rule;
//! this PR's review caught that `ClientInner`'s locks had not.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::net::lookup_host;
use tokio::task::JoinSet;

use crate::connection::{HotRodConnection, DEFAULT_TIMEOUT, DEFAULT_TOPOLOGY_ID};
use crate::error::{Error, Result};
use crate::hash;
use crate::pool::{Checkout, ConnectionPool, PooledGuard};
use crate::remote_cache::RemoteCache;
use crate::tls::TlsConfig;
use crate::topology::TopologyServer;

/// Default cap on how many connections `HotRodClient` keeps open to one
/// node for one cache at once, idle plus checked out. Not configurable
/// yet: no caller has asked for a different bound, and adding one means
/// multiplying the four `connect*` constructors by pool-size variants for
/// a need that is still hypothetical. Revisit if that changes.
const DEFAULT_MAX_CONNECTIONS_PER_NODE: usize = 8;

/// Credentials for one SASL mechanism, kept so a connection opened later
/// to any node, for any cache, can be authenticated the same way as the
/// first one, without the caller authenticating it by hand. `Clone` so a
/// connection can be opened while only holding a read lock on the stored
/// method just long enough to copy it, never across the `.await` that
/// runs the actual handshake.
#[derive(Clone)]
pub(crate) enum AuthMethod {
    Plain {
        authzid: String,
        authcid: String,
        password: String,
    },
    Scram {
        authcid: String,
        password: String,
    },
    Digest {
        authcid: String,
        password: String,
    },
    OAuthBearer {
        authzid: String,
        token: String,
    },
}

impl AuthMethod {
    async fn authenticate(&self, conn: &mut HotRodConnection) -> Result<()> {
        match self {
            AuthMethod::Plain {
                authzid,
                authcid,
                password,
            } => conn.authenticate_plain(authzid, authcid, password).await,
            AuthMethod::Scram { authcid, password } => {
                conn.authenticate_scram(authcid, password).await
            }
            AuthMethod::Digest { authcid, password } => {
                conn.authenticate_digest(authcid, password).await
            }
            AuthMethod::OAuthBearer { authzid, token } => {
                conn.authenticate_oauthbearer(authzid, token).await
            }
        }
    }
}

/// The routing data from the most recently applied topology update, plus
/// the topology id that came with it: the two are always read and
/// written together as one `Arc`, never as two separate locks, so a
/// concurrent reader can never observe one half updated and the other
/// still stale (a torn read this PR's review caught in an earlier
/// version that kept `topology_id` in its own `RwLock` next to this one).
///
/// Always held behind an `Arc` in `ClientInner::topology`: `owner_addr`
/// clones the `Arc` under a brief read lock, then resolves and caches a
/// DNS lookup against its own `resolved_addrs` without holding that lock.
/// If a newer topology replaces it mid-resolution, the stale `Arc` (and
/// whatever this fills into its cache) is simply dropped once the last
/// reader is done with it; nothing reads a cache entry attributed to the
/// wrong topology.
pub(crate) struct ClusterTopology {
    pub(crate) topology_id: i32,
    pub(crate) servers: Vec<TopologyServer>,
    pub(crate) hash_function_version: u8,
    /// Index `n` is segment `n`'s owners, as indices into `servers`,
    /// primary owner first.
    pub(crate) segment_owners: Vec<Vec<u32>>,
    /// Addresses already resolved for this topology, keyed by index into
    /// `servers`.
    pub(crate) resolved_addrs: RwLock<HashMap<u32, SocketAddr>>,
}

pub(crate) struct ClientInner {
    /// Every seed address this instance was constructed with, in the
    /// order given to `connect`/`connect_with_timeout`.
    /// `active_seed_addr` is always one of these; `failover_seed` tries
    /// the rest when it stops responding.
    pub(crate) seed_addrs: Vec<SocketAddr>,
    /// The seed this instance is currently connected to; also the
    /// fallback used before a topology has arrived and the retry target
    /// when a computed owner's connection fails. Never evicted by a
    /// topology update, even if this address stops being listed: it is
    /// the last resort every retry falls back to. Can change at runtime:
    /// see `failover_seed`.
    pub(crate) active_seed_addr: RwLock<SocketAddr>,
    pub(crate) topology: RwLock<Option<Arc<ClusterTopology>>>,
    /// The topology server a pooled address was resolved from, if any:
    /// `None` for the seed, `Some` for a node discovered through a
    /// topology update. Recorded the first time any pool opens a
    /// connection to that address, regardless of which cache the pool is
    /// for, and consulted by `record_topology_update` to decide which
    /// addresses (and therefore which of their per-cache pools) have left
    /// the cluster.
    pub(crate) node_origin: RwLock<HashMap<SocketAddr, Option<TopologyServer>>>,
    /// One pool per `(node, cache name)` pair this client has needed to
    /// talk to so far. See the module docs for why the cache name is part
    /// of the key.
    pub(crate) pools: RwLock<HashMap<(SocketAddr, String), Arc<ConnectionPool>>>,
    pub(crate) auth: RwLock<Option<AuthMethod>>,
    /// Replayed on every connection this instance opens, seed or
    /// topology-discovered node alike. Per-connection TLS verification
    /// (hostname for the seed, CA-only for a discovered node) is decided
    /// per call to `open_and_authenticate`; see the `tls` module docs and
    /// ADR 0004.
    pub(crate) tls: Option<TlsConfig>,
    pub(crate) timeout: RwLock<Duration>,
}

/// A cache client that tracks cluster topology and routes each operation
/// to the segment's primary owner instead of relying on server-side
/// redirects, matching the Java client's intelligent routing.
///
/// Unlike phase 3's `HotRodCluster`, one `HotRodClient` is not bound to a
/// single cache: call `cache` to get a `RemoteCache` handle for a named
/// cache, as many times, for as many cache names, as needed. `HotRodClient`
/// itself is a cheap `Clone` (an `Arc` underneath), so a `RemoteCache` can
/// hold its own copy without the caller managing an `Arc` by hand.
#[derive(Clone)]
pub struct HotRodClient(pub(crate) Arc<ClientInner>);

/// Guards a slot `ConnectionPool::checkout` already popped as
/// `Checkout::NeedsNew` while this client opens and authenticates a real
/// connection for it. Returns the slot as empty on drop unless `fulfill`
/// ran first, so neither a failed open nor this whole `checkout` call
/// being cancelled by its own outer timeout mid-open leaks the slot:
/// found by this PR's own review, reproduced by checking out from a pool
/// of capacity 1 against an address that always refuses to connect,
/// twice in a row. Without this guard, the first failed open never gave
/// its slot back, so the second checkout hung forever waiting on a slot
/// that could never become available again.
struct PendingSlot {
    pool: Arc<ConnectionPool>,
    fulfilled: bool,
}

impl PendingSlot {
    fn new(pool: Arc<ConnectionPool>) -> Self {
        Self {
            pool,
            fulfilled: false,
        }
    }

    fn fulfill(mut self, conn: HotRodConnection) -> PooledGuard {
        self.fulfilled = true;
        PooledGuard::new(self.pool.clone(), conn)
    }
}

impl Drop for PendingSlot {
    fn drop(&mut self) {
        if !self.fulfilled {
            self.pool.return_slot(None);
        }
    }
}

impl HotRodClient {
    /// Builds a client directly from its shared state. `pub(crate)` only:
    /// real callers always go through `connect`/`connect_with_timeout`/
    /// `connect_tls`/`connect_tls_with_timeout`. Used by `remote_cache.rs`
    /// tests that need a `seed_addrs` list containing an address never
    /// dialed during construction, which those constructors cannot
    /// express since they always dial every seed up front.
    #[cfg(test)]
    pub(crate) fn from_inner(inner: ClientInner) -> Self {
        Self(Arc::new(inner))
    }

    /// `pub(crate)` escape hatch for `remote_cache.rs` to reach routing
    /// state (`active_seed_addr`, `pools`, `topology`) this type does not
    /// otherwise expose publicly.
    pub(crate) fn inner(&self) -> &ClientInner {
        &self.0
    }

    /// Connects to the first seed address that accepts a connection, in
    /// order, matching the Java client's failover-on-connect behavior. No
    /// topology is known yet at this point: every operation routes to
    /// this seed until the server's first response carries one. Uses
    /// `DEFAULT_TIMEOUT`; call `connect_with_timeout` for a different
    /// bound.
    pub async fn connect(seed_addrs: &[SocketAddr]) -> Result<Self> {
        Self::connect_with_timeout(seed_addrs, DEFAULT_TIMEOUT).await
    }

    /// Same as `connect`, but with a caller-supplied timeout in place of
    /// `DEFAULT_TIMEOUT`, applied to every connection this instance opens,
    /// now or later when routing discovers a new node.
    ///
    /// Every seed is dialed concurrently rather than one after another,
    /// so the whole call is bounded by `timeout` regardless of how many
    /// seeds are given: an unreachable seed no longer adds its own
    /// `timeout` to the total. The first seed to accept a connection
    /// wins; the others are dropped mid-connect. If every seed fails, the
    /// reported error is the one from the seed listed first in
    /// `seed_addrs`, not whichever happened to finish last.
    pub async fn connect_with_timeout(
        seed_addrs: &[SocketAddr],
        timeout: Duration,
    ) -> Result<Self> {
        Self::connect_with(seed_addrs, timeout, None).await
    }

    /// Same as `connect`, but over TLS (ADR 0004,
    /// `docs/adr/0004-tls-support.md`). `tls.server_name` is verified
    /// against whichever seed this instance connects to; every node
    /// discovered later through a topology update is verified only
    /// against `tls.ca_certificate` (or the OS trust store), never by
    /// hostname: see the `tls` module docs.
    pub async fn connect_tls(seed_addrs: &[SocketAddr], tls: TlsConfig) -> Result<Self> {
        Self::connect_with(seed_addrs, DEFAULT_TIMEOUT, Some(tls)).await
    }

    /// Same as `connect_tls`, but with a caller-supplied timeout in place
    /// of `DEFAULT_TIMEOUT`.
    pub async fn connect_tls_with_timeout(
        seed_addrs: &[SocketAddr],
        tls: TlsConfig,
        timeout: Duration,
    ) -> Result<Self> {
        Self::connect_with(seed_addrs, timeout, Some(tls)).await
    }

    /// Dials every seed concurrently, purely to confirm one is reachable
    /// and pick which becomes `active_seed_addr`: the winning connection
    /// is dropped once that is known, not kept pooled. It cannot be,
    /// since no cache name is known yet at this point; the real pooled
    /// connection for whichever cache a caller opens is opened lazily on
    /// its first operation instead.
    async fn connect_with(
        seed_addrs: &[SocketAddr],
        timeout: Duration,
        tls: Option<TlsConfig>,
    ) -> Result<Self> {
        let mut attempts = JoinSet::new();
        for (index, &addr) in seed_addrs.iter().enumerate() {
            let tls_for_task = tls.clone();
            attempts.spawn(async move {
                let result = HotRodConnection::connect_hash_aware(
                    addr,
                    "",
                    DEFAULT_TOPOLOGY_ID,
                    timeout,
                    tls_for_task.as_ref(),
                    true, // dialing a seed: verify by hostname
                )
                .await;
                (index, addr, result)
            });
        }

        let mut errors: Vec<Option<Error>> = seed_addrs.iter().map(|_| None).collect();
        let mut join_failure: Option<Error> = None;
        while let Some(joined) = attempts.join_next().await {
            match joined {
                Ok((_index, addr, Ok(_conn))) => {
                    return Ok(Self(Arc::new(ClientInner {
                        seed_addrs: seed_addrs.to_vec(),
                        active_seed_addr: RwLock::new(addr),
                        topology: RwLock::new(None),
                        node_origin: RwLock::new(HashMap::new()),
                        pools: RwLock::new(HashMap::new()),
                        auth: RwLock::new(None),
                        tls,
                        timeout: RwLock::new(timeout),
                    })));
                }
                Ok((index, _addr, Err(err))) => errors[index] = Some(err),
                Err(join_err) => {
                    join_failure.get_or_insert_with(|| Error::Io(io::Error::other(join_err)));
                }
            }
        }
        Err(errors
            .into_iter()
            .flatten()
            .next()
            .or(join_failure)
            .unwrap_or_else(|| {
                Error::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "no seed addresses provided",
                ))
            }))
    }

    /// Returns a handle for the named cache. Cheap: clones this client's
    /// `Arc` and stores the name alongside it. Can be called as many
    /// times, for as many cache names, as needed; each `RemoteCache`
    /// shares this client's topology, pools and authentication.
    pub fn cache(&self, name: impl Into<String>) -> RemoteCache {
        RemoteCache::new(self.clone(), name.into())
    }

    /// The timeout currently bounding every operation on this instance's
    /// pooled connections.
    pub fn timeout(&self) -> Duration {
        *self.0.timeout.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Overrides the timeout used from this call onward, in place of the
    /// one given at connect time. Applies immediately to every connection
    /// currently idle in any pool and to every one opened later. A
    /// connection checked out by another task at this moment keeps its
    /// old timeout until it is returned and later checked out again.
    pub fn set_timeout(&self, timeout: Duration) {
        *self.0.timeout.write().unwrap_or_else(|p| p.into_inner()) = timeout;
        for pool in self
            .0
            .pools
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .values()
        {
            pool.set_idle_timeouts(timeout);
        }
    }

    /// Authenticates using SASL PLAIN. See
    /// `HotRodConnection::authenticate_plain`.
    pub async fn authenticate_plain(
        &self,
        authzid: &str,
        authcid: &str,
        password: &str,
    ) -> Result<()> {
        self.authenticate_with(AuthMethod::Plain {
            authzid: authzid.to_string(),
            authcid: authcid.to_string(),
            password: password.to_string(),
        })
        .await
    }

    /// Authenticates using SASL SCRAM-SHA-512. See
    /// `HotRodConnection::authenticate_scram`.
    pub async fn authenticate_scram(&self, authcid: &str, password: &str) -> Result<()> {
        self.authenticate_with(AuthMethod::Scram {
            authcid: authcid.to_string(),
            password: password.to_string(),
        })
        .await
    }

    /// Authenticates using SASL DIGEST-SHA-256. See
    /// `HotRodConnection::authenticate_digest`.
    pub async fn authenticate_digest(&self, authcid: &str, password: &str) -> Result<()> {
        self.authenticate_with(AuthMethod::Digest {
            authcid: authcid.to_string(),
            password: password.to_string(),
        })
        .await
    }

    /// Authenticates using SASL OAUTHBEARER. See
    /// `HotRodConnection::authenticate_oauthbearer`.
    pub async fn authenticate_oauthbearer(&self, authzid: &str, token: &str) -> Result<()> {
        self.authenticate_with(AuthMethod::OAuthBearer {
            authzid: authzid.to_string(),
            token: token.to_string(),
        })
        .await
    }

    /// Opens a throwaway connection to the active seed, runs `method`'s
    /// SASL exchange on it, and only stores `method` for replay once that
    /// exchange succeeds. The connection itself is not pooled: SASL
    /// requests always use an empty cache name regardless of what the
    /// connection was opened with (`HotRodConnection::run_sasl`), so it
    /// cannot usefully serve a later `RemoteCache` operation anyway.
    ///
    /// Every connection any pool opens afterward replays the stored
    /// method through `open_and_authenticate`, but a connection already
    /// idle in a pool from before this call was authenticated
    /// differently; it is not reachable here to re-authenticate in
    /// place. Instead, every pool's idle connections are invalidated once
    /// `method` is stored, so the next checkout for each opens a fresh
    /// one that replays the new credentials rather than handing out a
    /// connection still carrying the old ones. A connection already
    /// checked out by another task when this runs keeps whatever
    /// credential it was opened with until it is next evicted; this
    /// narrower gap (an in-flight connection outliving a credential
    /// refresh) also existed on the single pooled seed connection
    /// `HotRodCluster` used to keep.
    async fn authenticate_with(&self, method: AuthMethod) -> Result<()> {
        let seed = *self
            .0
            .active_seed_addr
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let topology_id = self.current_topology_id();
        let timeout = *self.0.timeout.read().unwrap_or_else(|p| p.into_inner());
        let mut conn = HotRodConnection::connect_hash_aware(
            seed,
            "",
            topology_id,
            timeout,
            self.0.tls.as_ref(),
            true,
        )
        .await?;
        method.authenticate(&mut conn).await?;
        *self.0.auth.write().unwrap_or_else(|p| p.into_inner()) = Some(method);
        for pool in self
            .0
            .pools
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .values()
        {
            pool.invalidate_idle();
        }
        Ok(())
    }

    /// The topology id to send on the next request: the one carried by
    /// the current topology, or `DEFAULT_TOPOLOGY_ID` before any topology
    /// update has arrived. Reads `topology_id` out of the same `Arc` as
    /// `servers`/`segment_owners`, never a separately locked field, so it
    /// can never be stale relative to them (see `ClusterTopology`'s docs).
    fn current_topology_id(&self) -> i32 {
        self.0
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .map_or(DEFAULT_TOPOLOGY_ID, |topology| topology.topology_id)
    }

    /// The address to route `key` to, and the topology server it came
    /// from: the segment's primary owner once a topology is known,
    /// otherwise the seed connection with no origin.
    pub(crate) async fn owner_addr(
        &self,
        key: &[u8],
    ) -> Result<(SocketAddr, Option<TopologyServer>)> {
        let active_seed = *self
            .0
            .active_seed_addr
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let topology = self
            .0
            .topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let Some(topology) = topology else {
            return Ok((active_seed, None));
        };
        if topology.segment_owners.is_empty() {
            return Ok((active_seed, None));
        }
        let segment = hash::segment(
            key,
            topology.segment_owners.len() as u32,
            topology.hash_function_version,
        )?;
        let Some(&primary) = topology.segment_owners[segment as usize].first() else {
            return Ok((active_seed, None));
        };
        // Safe: `topology::read_topology_update` rejects any owner index
        // that is out of range for `servers` before this type is built.
        let server = topology.servers[primary as usize].clone();
        if let Some(&addr) = topology
            .resolved_addrs
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&primary)
        {
            return Ok((addr, Some(server)));
        }
        let addr = resolve_server_addr(&server).await?;
        topology
            .resolved_addrs
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(primary, addr);
        Ok((addr, Some(server)))
    }

    fn pool_for(&self, addr: SocketAddr, cache_name: &str) -> Arc<ConnectionPool> {
        let key = (addr, cache_name.to_string());
        if let Some(pool) = self
            .0
            .pools
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
        {
            return pool.clone();
        }
        self.0
            .pools
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .entry(key)
            .or_insert_with(|| Arc::new(ConnectionPool::new(DEFAULT_MAX_CONNECTIONS_PER_NODE)))
            .clone()
    }

    /// Checks out a connection to `addr` for `cache_name`, opening and
    /// authenticating a new one if the pool has none idle and room for
    /// one more. `origin` is the topology server `addr` was resolved
    /// from (see `owner_addr`); pass `None` for the seed.
    ///
    /// The whole call, slot wait and any new connect-and-authenticate
    /// together, is bounded by this client's timeout, not just the slot
    /// wait: an earlier version only wrapped the wait, so a call that
    /// spent close to the full timeout waiting for a slot and then opened
    /// a slow new connection could take close to twice the timeout a
    /// caller would reasonably expect as a liveness bound, a compounding
    /// effect that could not happen when `HotRodCluster` kept exactly one
    /// connection per node. If this is cancelled (by that outer timeout,
    /// or a failed open) after `pool.checkout()` already returned a
    /// `Checkout::NeedsNew`, `PendingSlot` returns that slot as empty
    /// instead of leaking it.
    pub(crate) async fn checkout(
        &self,
        addr: SocketAddr,
        cache_name: &str,
        origin: Option<TopologyServer>,
    ) -> Result<PooledGuard> {
        let pool = self.pool_for(addr, cache_name);
        let timeout = *self.0.timeout.read().unwrap_or_else(|p| p.into_inner());
        tokio::time::timeout(timeout, async {
            match pool.checkout().await {
                Checkout::Idle(conn) => Ok(PooledGuard::new(pool.clone(), *conn)),
                Checkout::NeedsNew => {
                    let pending = PendingSlot::new(pool.clone());
                    let conn = self.open_and_authenticate(addr, cache_name, origin).await?;
                    Ok(pending.fulfill(conn))
                }
            }
        })
        .await
        .map_err(|_elapsed| Error::Timeout(timeout))?
    }

    /// Opens and authenticates a new connection to `addr` for
    /// `cache_name`, recording `addr`'s origin the first time this client
    /// sees it, regardless of which cache the connection is for.
    /// `pub(crate)`, not just used by `checkout`/`failover_seed`:
    /// `remote_cache.rs`'s `listen`/`listen_with` also call this directly
    /// to get a connection for `listener.rs` to take ownership of, since
    /// a listener's connection is never pooled (see that module's docs).
    pub(crate) async fn open_and_authenticate(
        &self,
        addr: SocketAddr,
        cache_name: &str,
        origin: Option<TopologyServer>,
    ) -> Result<HotRodConnection> {
        let topology_id = self.current_topology_id();
        let timeout = *self.0.timeout.read().unwrap_or_else(|p| p.into_inner());
        let mut conn = HotRodConnection::connect_hash_aware(
            addr,
            cache_name,
            topology_id,
            timeout,
            self.0.tls.as_ref(),
            origin.is_none(),
        )
        .await?;
        let auth = self
            .0
            .auth
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some(auth) = auth {
            auth.authenticate(&mut conn).await?;
        }
        self.0
            .node_origin
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(addr, origin);
        Ok(conn)
    }

    /// Tries every seed address other than the current `active_seed_addr`,
    /// in the order given to `connect`, until one accepts a connection
    /// and, if credentials were set, authenticates. The first to succeed
    /// is pushed into its `(addr, cache_name)` pool as an idle connection
    /// (so the caller's immediate retry finds it ready instead of dialing
    /// a third time, bounded by this client's timeout like every other
    /// wait on a pool slot) and promoted to `active_seed_addr`. Returns
    /// the last error seen if every other seed also fails, or a generic
    /// error if there was no other seed to try in the first place.
    pub(crate) async fn failover_seed(&self, cache_name: &str) -> Result<SocketAddr> {
        let previous = *self
            .0
            .active_seed_addr
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let topology_id = self.current_topology_id();
        let timeout = *self.0.timeout.read().unwrap_or_else(|p| p.into_inner());
        let auth = self
            .0
            .auth
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let mut last_err: Option<Error> = None;
        for &addr in &self.0.seed_addrs {
            if addr == previous {
                continue;
            }
            let mut conn = match HotRodConnection::connect_hash_aware(
                addr,
                cache_name,
                topology_id,
                timeout,
                self.0.tls.as_ref(),
                true, // dialing a seed: verify by hostname
            )
            .await
            {
                Ok(conn) => conn,
                Err(err) => {
                    last_err = Some(err);
                    continue;
                }
            };
            if let Some(auth) = &auth {
                if let Err(err) = auth.authenticate(&mut conn).await {
                    last_err = Some(err);
                    continue;
                }
            }
            self.0
                .node_origin
                .write()
                .unwrap_or_else(|p| p.into_inner())
                .insert(addr, None);
            let pool = self.pool_for(addr, cache_name);
            if tokio::time::timeout(timeout, pool.seed_idle(conn))
                .await
                .is_err()
            {
                last_err = Some(Error::Timeout(timeout));
                continue;
            }
            *self
                .0
                .active_seed_addr
                .write()
                .unwrap_or_else(|p| p.into_inner()) = addr;
            return Ok(addr);
        }
        Err(last_err.unwrap_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotConnected,
                "no other seed address available to fail over to",
            ))
        }))
    }

    /// Applies whatever topology update `conn` parsed from its last
    /// response, if any, and reconciles every pool against it: an
    /// address whose origin server is no longer listed has left the
    /// cluster, so every pool for that address, across every cache, is
    /// closed and dropped rather than kept open forever. The seed address
    /// is always kept regardless, since it is the permanent retry
    /// fallback.
    pub(crate) fn record_topology_update(&self, conn: &mut HotRodConnection) {
        let Some(update) = conn.take_pending_topology_update() else {
            return;
        };

        let new_topology = Arc::new(ClusterTopology {
            topology_id: update.topology_id as i32,
            servers: update.servers,
            hash_function_version: update.hash_function_version,
            segment_owners: update.segment_owners,
            resolved_addrs: RwLock::new(HashMap::new()),
        });
        *self.0.topology.write().unwrap_or_else(|p| p.into_inner()) = Some(new_topology.clone());

        let active_seed = *self
            .0
            .active_seed_addr
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let mut node_origin = self
            .0
            .node_origin
            .write()
            .unwrap_or_else(|p| p.into_inner());
        let stale_addrs: Vec<SocketAddr> = node_origin
            .iter()
            .filter(|(&addr, origin)| {
                addr != active_seed
                    && origin
                        .as_ref()
                        .is_some_and(|origin| !new_topology.servers.contains(origin))
            })
            .map(|(&addr, _)| addr)
            .collect();
        if stale_addrs.is_empty() {
            return;
        }
        for addr in &stale_addrs {
            node_origin.remove(addr);
        }
        drop(node_origin);

        let mut pools = self.0.pools.write().unwrap_or_else(|p| p.into_inner());
        pools.retain(|(pool_addr, _cache_name), pool| {
            if stale_addrs.contains(pool_addr) {
                pool.close();
                false
            } else {
                true
            }
        });
    }
}

async fn resolve_server_addr(server: &TopologyServer) -> Result<SocketAddr> {
    lookup_host((server.host.as_str(), server.port))
        .await?
        .next()
        .ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no address found for {}:{}", server.host, server.port),
            ))
        })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::time::Duration;

    use tokio::net::{TcpListener, TcpStream};

    use crate::varint::{read_vint, read_vlong, write_vint, write_vlong};
    use crate::wire::{read_array, write_array};

    #[tokio::test]
    async fn connect_fails_with_no_seed_addresses() {
        let result = HotRodClient::connect(&[]).await;
        assert!(matches!(result, Err(Error::Io(_))));
    }

    /// Binds a listener and immediately drops it, so the returned address
    /// keeps refusing connections without anything else on it.
    pub(crate) async fn unreachable_addr() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    }

    #[tokio::test]
    async fn connect_with_timeout_succeeds_via_whichever_seed_is_reachable() {
        let dead_addr = unreachable_addr().await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let client = HotRodClient::connect_with_timeout(&[dead_addr, addr], Duration::from_secs(5))
            .await
            .expect("the reachable seed should win the race");

        assert_eq!(*client.0.active_seed_addr.read().unwrap(), addr);
    }

    #[tokio::test]
    async fn connect_with_timeout_fails_when_every_seed_is_unreachable() {
        let dead_addrs = [unreachable_addr().await, unreachable_addr().await];

        let result = HotRodClient::connect_with_timeout(&dead_addrs, Duration::from_secs(5)).await;

        assert!(matches!(result, Err(Error::Io(_))));
    }

    #[tokio::test]
    async fn set_timeout_applies_to_an_already_pooled_connection() {
        use tokio::io::AsyncWriteExt;

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        tokio::spawn(async move {
            // `connect_with_timeout`'s own reachability probe: accepted and
            // immediately dropped by the client without sending anything.
            let (_probe, _) = seed_listener.accept().await.unwrap();

            // The real connection `ping` below opens and, once answered,
            // returns to the pool as idle.
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x17, "expected a Ping request");
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0); // key media type: none
            resp.push(0); // value media type: none
            resp.push(41); // server protocol version
            write_vint(&mut resp, 0); // no supported opcodes listed
            stream.write_all(&resp).await.unwrap();

            // Never responds to the `get` below, which must time out instead.
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let client = HotRodClient::connect_with_timeout(&[seed_addr], Duration::from_secs(30))
            .await
            .expect("connect to seed");
        assert_eq!(client.timeout(), Duration::from_secs(30));
        let cache = client.cache("my-cache");

        cache
            .ping()
            .await
            .expect("ping should succeed and pool the connection");

        client.set_timeout(Duration::from_millis(100));
        assert_eq!(client.timeout(), Duration::from_millis(100));

        let start = tokio::time::Instant::now();
        let result = cache.get(b"key").await;

        assert!(matches!(result, Err(Error::Timeout(_))));
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "set_timeout should have applied to the connection already pooled"
        );
    }

    #[tokio::test]
    async fn owner_addr_reuses_a_cached_resolution_instead_of_resolving_again() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = HotRodClient::connect(&[addr])
            .await
            .expect("connect to seed");

        *client.0.topology.write().unwrap() = Some(Arc::new(ClusterTopology {
            topology_id: 9,
            servers: vec![TopologyServer {
                host: addr.ip().to_string(),
                port: addr.port(),
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0]],
            resolved_addrs: RwLock::new(HashMap::new()),
        }));

        let (first, _origin) = client.owner_addr(b"key").await.expect("first resolution");
        assert_eq!(first, addr);

        // Replace the topology with one whose hostname does not resolve,
        // but carrying forward the already-populated `resolved_addrs`: if
        // `owner_addr` re-resolved instead of using that cache, this call
        // would fail.
        let topology = client.0.topology.read().unwrap().clone().unwrap();
        let mut broken_servers = topology.servers.clone();
        broken_servers[0].host = "this-hostname-does-not-resolve.invalid".to_string();
        *client.0.topology.write().unwrap() = Some(Arc::new(ClusterTopology {
            topology_id: topology.topology_id,
            servers: broken_servers,
            hash_function_version: topology.hash_function_version,
            segment_owners: topology.segment_owners.clone(),
            resolved_addrs: RwLock::new(topology.resolved_addrs.read().unwrap().clone()),
        }));

        let (second, _origin) = client.owner_addr(b"key").await.expect("cached resolution");
        assert_eq!(second, addr);
    }

    #[tokio::test]
    async fn owner_addr_returns_a_typed_error_when_the_owner_host_does_not_resolve() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = seed_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = HotRodClient::connect(&[seed_addr])
            .await
            .expect("connect to seed");

        *client.0.topology.write().unwrap() = Some(Arc::new(ClusterTopology {
            topology_id: 9,
            servers: vec![TopologyServer {
                host: "this-hostname-does-not-resolve.invalid".to_string(),
                port: 7000,
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0]],
            resolved_addrs: RwLock::new(HashMap::new()),
        }));

        let result = client.owner_addr(b"key").await;
        assert!(matches!(result, Err(Error::Io(_))));
    }

    #[tokio::test]
    async fn authenticate_with_does_not_replay_the_old_auth() {
        let old_response = b"\0user\0old-pass".to_vec();
        let new_response = b"\0user\0new-pass".to_vec();

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let expected_old = old_response.clone();
        let expected_new = new_response.clone();
        let seed_task = tokio::spawn(async move {
            let (mut first, _) = seed_listener.accept().await.unwrap();
            serve_plain_auth(&mut first, &expected_old).await;

            let (mut second, _) = seed_listener.accept().await.unwrap();
            serve_plain_auth(&mut second, &expected_new).await;
        });

        // Built directly instead of via `connect`: `connect` itself dials a
        // throwaway reachability probe (see its docs), which would consume
        // `seed_task`'s first `accept` before any real auth exchange ran.
        let client = HotRodClient::from_inner(ClientInner {
            seed_addrs: vec![seed_addr],
            active_seed_addr: RwLock::new(seed_addr),
            topology: RwLock::new(None),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_secs(5)),
        });

        client
            .authenticate_plain("", "user", "old-pass")
            .await
            .expect("authenticate with the old credentials");

        client
            .authenticate_plain("", "user", "new-pass")
            .await
            .expect("authenticate with the new credentials, without replaying the old ones");

        seed_task.await.unwrap();
    }

    /// A connection pooled before `authenticate_plain` is called again
    /// with new credentials must not be handed to a later caller: it was
    /// opened under the old credentials, and nothing re-authenticates it
    /// in place.
    #[tokio::test]
    async fn authenticate_with_invalidates_already_pooled_connections() {
        let old_response = b"\0user\0old-pass".to_vec();
        let new_response = b"\0user\0new-pass".to_vec();

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let expected_old = old_response.clone();
        let expected_new = new_response.clone();
        let seed_task = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;

            async fn serve_ping(stream: &mut TcpStream) {
                let (id, opcode) = read_request_opcode(stream).await;
                assert_eq!(opcode, 0x17, "expected a Ping request");
                let mut resp = response_header(id, 0x18, 0x00);
                resp.push(0);
                resp.push(0);
                resp.push(41);
                write_vint(&mut resp, 0);
                stream.write_all(&resp).await.unwrap();
            }

            // `authenticate_plain("old-pass")` dials its own throwaway
            // connection for the SASL exchange alone (see
            // `HotRodClient::authenticate_with`'s docs); it is never
            // reused, so no Ping follows here.
            let (mut probe, _) = seed_listener.accept().await.unwrap();
            serve_plain_auth(&mut probe, &expected_old).await;

            // The first `ping` opens its own connection, authenticated
            // (again) under the old creds, and pools it once answered.
            let (mut first, _) = seed_listener.accept().await.unwrap();
            serve_plain_auth(&mut first, &expected_old).await;
            serve_ping(&mut first).await;

            // `authenticate_plain("new-pass")`'s own throwaway probe.
            let (mut probe, _) = seed_listener.accept().await.unwrap();
            serve_plain_auth(&mut probe, &expected_new).await;

            // The second `ping`, after re-authenticating, must open a
            // fresh connection under the new creds instead of reusing the
            // invalidated one from above.
            let (mut second, _) = seed_listener.accept().await.unwrap();
            serve_plain_auth(&mut second, &expected_new).await;
            serve_ping(&mut second).await;
        });

        let client = HotRodClient::from_inner(ClientInner {
            seed_addrs: vec![seed_addr],
            active_seed_addr: RwLock::new(seed_addr),
            topology: RwLock::new(None),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_secs(5)),
        });

        client
            .authenticate_plain("", "user", "old-pass")
            .await
            .expect("authenticate with the old credentials");
        client
            .cache("my-cache")
            .ping()
            .await
            .expect("ping pools a connection authenticated with the old credentials");

        client
            .authenticate_plain("", "user", "new-pass")
            .await
            .expect("authenticate with the new credentials");
        client
            .cache("my-cache")
            .ping()
            .await
            .expect("ping should open a fresh connection, not reuse the invalidated one");

        seed_task.await.unwrap();
    }

    /// Reproduces the review finding on `checkout`/`PendingSlot`: before
    /// `PendingSlot` existed, a failed `open_and_authenticate` left the
    /// slot `pool.checkout()` had already handed out neither returned nor
    /// reusable, so repeated failed checkouts against the same pool
    /// permanently shrank its capacity. With the fix, every one of these
    /// fails fast with `Error::Io`, never `Error::Timeout`: a `Timeout`
    /// here would mean an earlier failure had leaked a slot and this one
    /// is stuck waiting on the pool's semaphore instead of even attempting
    /// a connection.
    #[tokio::test]
    async fn checkout_does_not_leak_pool_capacity_when_open_fails() {
        let dead_addr = unreachable_addr().await;
        let client = HotRodClient::from_inner(ClientInner {
            seed_addrs: vec![dead_addr],
            active_seed_addr: RwLock::new(dead_addr),
            topology: RwLock::new(None),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_millis(200)),
        });

        // DEFAULT_MAX_CONNECTIONS_PER_NODE is 8: twice that many failed
        // checkouts against the pool's single address would, under the
        // leak, exhaust its capacity well before the last attempt.
        for attempt in 0..16 {
            let result = client.checkout(dead_addr, "my-cache", None).await;
            assert!(
                matches!(result, Err(Error::Io(_))),
                "attempt {attempt} should fail fast with Io, not hang into Timeout"
            );
        }
    }

    /// Reads one request's fixed header fields far enough to identify the
    /// opcode and message id. The fields in between (flags, intelligence,
    /// topology id, media types, additional params) are already covered
    /// by `header.rs`'s own tests, so they are just consumed here, not
    /// checked.
    pub(crate) async fn read_request_opcode(stream: &mut TcpStream) -> (u64, u8) {
        use tokio::io::AsyncReadExt;
        assert_eq!(
            stream.read_u8().await.unwrap(),
            0xA0,
            "expected a request magic byte"
        );
        let message_id = read_vlong(stream).await.unwrap();
        let _version = stream.read_u8().await.unwrap();
        let opcode = stream.read_u8().await.unwrap();
        let _cache_name = read_array(stream).await.unwrap();
        let _flags = read_vint(stream).await.unwrap();
        let _intelligence = stream.read_u8().await.unwrap();
        let _topology_id = read_vint(stream).await.unwrap();
        let _key_media_type = stream.read_u8().await.unwrap();
        let _value_media_type = stream.read_u8().await.unwrap();
        let _additional_params = read_vint(stream).await.unwrap();
        (message_id, opcode)
    }

    pub(crate) fn response_header(message_id: u64, opcode: u8, status: u8) -> Vec<u8> {
        let mut buf = vec![0xA1];
        write_vlong(&mut buf, message_id);
        buf.push(opcode);
        buf.push(status);
        buf.push(0); // no topology update
        buf
    }

    /// Same shape as `response_header`, but with the topology marker set
    /// and a topology update payload appended, encoded the way
    /// `topology::read_topology_update` expects.
    pub(crate) fn response_header_with_topology(
        message_id: u64,
        opcode: u8,
        status: u8,
        servers: &[(&str, u16)],
    ) -> Vec<u8> {
        let mut buf = vec![0xA1];
        write_vlong(&mut buf, message_id);
        buf.push(opcode);
        buf.push(status);
        buf.push(1); // topology update follows

        write_vint(&mut buf, 9); // new topology id
        write_vint(&mut buf, servers.len() as u32);
        for (host, port) in servers {
            write_array(&mut buf, host.as_bytes());
            buf.extend_from_slice(&port.to_be_bytes());
        }
        buf.push(3); // hash function version
        write_vint(&mut buf, 1); // one segment
        buf.push(1); // one owner
        write_vint(&mut buf, 0); // server 0 owns it

        buf
    }

    /// Serves one `AuthMechList`/`Auth` exchange for SASL PLAIN, asserting
    /// the credentials sent are exactly `expected_plain_response`. Used to
    /// check that a connection opened after the first replays the same
    /// credentials, not just that it authenticates somehow.
    pub(crate) async fn serve_plain_auth(stream: &mut TcpStream, expected_plain_response: &[u8]) {
        use tokio::io::AsyncWriteExt;
        let (id, opcode) = read_request_opcode(stream).await;
        assert_eq!(opcode, 0x21, "expected an AuthMechList request");
        let mut resp = response_header(id, 0x22, 0x00);
        write_vint(&mut resp, 1); // one mechanism offered
        write_array(&mut resp, b"PLAIN");
        stream.write_all(&resp).await.unwrap();

        let (id, opcode) = read_request_opcode(stream).await;
        assert_eq!(opcode, 0x23, "expected an Auth request");
        let mech_name = read_array(stream).await.unwrap();
        assert_eq!(mech_name, b"PLAIN");
        let response = read_array(stream).await.unwrap();
        assert_eq!(
            response, expected_plain_response,
            "connection must replay the same credentials as the first one authenticated"
        );
        let mut resp = response_header(id, 0x24, 0x00);
        resp.push(1); // exchange complete
        write_array(&mut resp, &[]); // no final server message
        stream.write_all(&resp).await.unwrap();
    }
}
