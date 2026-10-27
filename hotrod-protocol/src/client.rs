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
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::connection::{HotRodConnection, DEFAULT_TIMEOUT, DEFAULT_TOPOLOGY_ID};
use crate::error::{Error, Result};
use crate::hash;
use crate::health::NodeHealth;
use crate::pool::{Checkout, ConnectionPool, PooledGuard};
use crate::remote_cache::RemoteCache;
use crate::stats::{CacheStatisticsInner, PoolStatistics};
use crate::tls::TlsConfig;
use crate::topology::TopologyServer;
use crate::wire::Expiration;

/// Default cap on how many connections `HotRodClient` keeps open to one
/// node for one cache at once, idle plus checked out. Not configurable
/// yet: no caller has asked for a different bound, and adding one means
/// multiplying the four `connect*` constructors by pool-size variants for
/// a need that is still hypothetical. Revisit if that changes.
const DEFAULT_MAX_CONNECTIONS_PER_NODE: usize = 8;

/// Default number of retries `RemoteCache`'s dispatch loop makes after
/// an operation's first attempt, before giving up and returning the
/// last attempt's error. Same default as the Java client's
/// `maxRetries` (`docs/adr/0011-retry-policy-and-node-health.md`).
pub(crate) const DEFAULT_MAX_RETRIES: usize = 3;

/// Default quarantine window `NodeHealth` applies to a node once it
/// fails, matching the Java client's default `serverFailureTimeout`
/// (`docs/adr/0011-retry-policy-and-node-health.md`). `None` disables
/// quarantine entirely, the idiomatic equivalent of the Java client's
/// `-1` sentinel.
pub(crate) const DEFAULT_SERVER_FAILURE_TIMEOUT: Duration = Duration::from_secs(30);

/// Default interval `run_switch_back_monitor` waits between attempts
/// to switch back to a cluster an automatic failover just abandoned
/// (`docs/adr/0020-multicluster-proactive-switch-back.md`). No Java
/// client equivalent to match: chosen on the same order of magnitude
/// as `DEFAULT_SERVER_FAILURE_TIMEOUT`, since both bound how long this
/// client tolerates not noticing a cluster's state changed. `None`
/// disables the monitor entirely.
pub(crate) const DEFAULT_SWITCH_BACK_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// The cache name the server reserves for Protobuf schema storage
/// (`docs/adr/0013-remote-query.md`), confirmed against the Java
/// client's `InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME`. An
/// ordinary cache name, not a sentinel this crate treats specially in
/// any other way.
pub(crate) const PROTOBUF_METADATA_CACHE_NAME: &str = "___protobuf_metadata";

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

/// Everything that changes together when the active cluster changes,
/// whether through `switch_to_cluster` or the automatic failover in
/// `try_promote_seed`'s switch branch
/// (`docs/adr/0017-multi-cluster-failover.md`): the cluster's own
/// name and seed list, whichever of those seeds is currently
/// promoted to active, and the topology discovered against it so
/// far. Kept behind one lock, not four independent ones: an earlier
/// version updated these as separate writes, so a concurrent reader
/// (another in-flight operation racing a switch) could observe a
/// half-applied one, e.g. the new cluster's `active_seed_addr`
/// paired with the old cluster's still-live `topology`, routing to a
/// segment owner that has nothing to do with the cluster the client
/// believes it just switched to. Found by review before merge.
pub(crate) struct ActiveCluster {
    /// The name of whichever entry in `ClientInner::clusters` this
    /// reflects.
    pub(crate) name: String,
    /// This cluster's own seed addresses, in the order given to
    /// `connect`/`connect_with_timeout`, or to `add_cluster`.
    /// `active_seed_addr` is always one of these; `failover_seed`
    /// tries the rest when it stops responding.
    pub(crate) seed_addrs: Vec<SocketAddr>,
    /// The seed this instance is currently connected to; also the
    /// fallback used before a topology has arrived and the retry
    /// target when a computed owner's connection fails. Never evicted
    /// by a topology update, even if this address stops being
    /// listed: it is the last resort every retry falls back to. Can
    /// change at runtime: see `failover_seed`.
    pub(crate) active_seed_addr: SocketAddr,
    pub(crate) topology: Option<Arc<ClusterTopology>>,
}

pub(crate) struct ClientInner {
    /// The active cluster's own state, behind one lock: see
    /// `ActiveCluster`'s own docs for why this is not four
    /// independent `RwLock`s.
    pub(crate) active: RwLock<ActiveCluster>,
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
    /// One statistics counter set per cache name this client has
    /// dispatched an operation for, or had `RemoteCache::statistics`
    /// called on. See `docs/adr/0010-client-statistics-and-tracing.md`:
    /// scoped by cache name, the same way `pools` is, not aggregated
    /// across the whole client.
    pub(crate) cache_stats: RwLock<HashMap<String, Arc<CacheStatisticsInner>>>,
    /// The per-node circuit breaker `RemoteCache`'s dispatch loop
    /// consults when building a retry candidate list and updates on
    /// every attempt's outcome. See `docs/adr/0011-retry-policy-and-node-health.md`.
    pub(crate) node_health: NodeHealth,
    /// How long a node stays quarantined after `node_health` records a
    /// failure against it. `None` disables quarantine.
    pub(crate) server_failure_timeout: RwLock<Option<Duration>>,
    /// How many times `RemoteCache`'s dispatch loop retries an
    /// operation, beyond its first attempt, before surfacing the last
    /// attempt's error.
    pub(crate) max_retries: RwLock<usize>,
    /// Every cluster this client knows about, by name, including the
    /// one it was originally constructed with (under
    /// `HotRodClient::DEFAULT_CLUSTER_NAME`). `add_cluster` appends to
    /// this; `switch_to_cluster`/`try_failover_to_live_cluster` read it
    /// to find the seeds of whichever cluster becomes active next. See
    /// `docs/adr/0017-multi-cluster-failover.md`.
    pub(crate) clusters: RwLock<Vec<(String, Vec<SocketAddr>)>>,
    /// Bumped by one on every cluster switch, manual
    /// (`switch_to_cluster`) or automatic (`try_promote_seed`'s switch
    /// branch). `CacheListener::register` subscribes to this and
    /// captures the value current at registration time, so its `next`
    /// can tell a later switch apart from the cluster it was
    /// registered against and stop reading from an abandoned cluster's
    /// connection instead of silently continuing to trust it. See
    /// `docs/adr/0019-multicluster-listener-and-pool-cleanup.md`. Only
    /// `subscribe()`'s `Receiver` is ever read; the `Sender`'s own
    /// current value is never inspected directly.
    pub(crate) cluster_generation: watch::Sender<u64>,
    /// How long `run_switch_back_monitor` waits between attempts to
    /// switch back to a cluster an automatic failover just abandoned;
    /// `None` disables the monitor. Read fresh on every tick (not
    /// captured once when the monitor starts), so toggling this
    /// applies to an already-running monitor the next time it wakes,
    /// the same eventual-consistency rule `set_timeout` already
    /// documents for an idle connection's timeout. See
    /// `docs/adr/0020-multicluster-proactive-switch-back.md`.
    pub(crate) switch_back_check_interval: RwLock<Option<Duration>>,
    /// Per-cluster auth/TLS overrides, keyed by whatever name
    /// `clusters` itself uses (`HotRodClient::DEFAULT_CLUSTER_NAME`
    /// included). A cluster absent here, or present with a field left
    /// `None`, falls back to this client's global `auth`/`tls`. See
    /// `docs/adr/0021-multicluster-per-cluster-auth-and-tls.md`.
    pub(crate) cluster_overrides: RwLock<HashMap<String, ClusterOverride>>,
}

/// One cluster's own auth/TLS, overriding this client's global
/// default (`ClientInner::auth`/`tls`) for every connection dialed to
/// that cluster specifically. `None` in either field inherits the
/// global value; there is no way to override one and not the other
/// independently of this type's own shape, since both inherit the
/// same way.
#[derive(Clone, Default)]
pub(crate) struct ClusterOverride {
    pub(crate) auth: Option<AuthMethod>,
    pub(crate) tls: Option<TlsConfig>,
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
    /// Sentinel name for the cluster this client was originally
    /// constructed with (`connect`/`connect_with_timeout`/`connect_tls`/
    /// `connect_tls_with_timeout`), so `switch_to_cluster` can return to
    /// it the same way it switches to any `add_cluster`-added one.
    /// Matches the Java client's own `DEFAULT_CLUSTER_NAME` in spirit
    /// (an unlikely name for a real cluster to collide with), not its
    /// exact string: `add_cluster` rejects this name outright, so there
    /// is no ambiguity either way.
    pub const DEFAULT_CLUSTER_NAME: &'static str = "__default__";

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
                        active: RwLock::new(ActiveCluster {
                            name: Self::DEFAULT_CLUSTER_NAME.to_string(),
                            seed_addrs: seed_addrs.to_vec(),
                            active_seed_addr: addr,
                            topology: None,
                        }),
                        node_origin: RwLock::new(HashMap::new()),
                        pools: RwLock::new(HashMap::new()),
                        auth: RwLock::new(None),
                        tls,
                        timeout: RwLock::new(timeout),
                        cache_stats: RwLock::new(HashMap::new()),
                        node_health: NodeHealth::default(),
                        server_failure_timeout: RwLock::new(Some(DEFAULT_SERVER_FAILURE_TIMEOUT)),
                        max_retries: RwLock::new(DEFAULT_MAX_RETRIES),
                        clusters: RwLock::new(vec![(
                            Self::DEFAULT_CLUSTER_NAME.to_string(),
                            seed_addrs.to_vec(),
                        )]),
                        cluster_generation: watch::channel(0).0,
                        switch_back_check_interval: RwLock::new(Some(
                            DEFAULT_SWITCH_BACK_CHECK_INTERVAL,
                        )),
                        cluster_overrides: RwLock::new(HashMap::new()),
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

    /// Registers (or replaces) a Protobuf schema, used by remote query
    /// (`docs/adr/0013-remote-query.md`). `___protobuf_metadata` is an
    /// ordinary cache, confirmed against the Java client's own source
    /// (`InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME`) to need no
    /// dedicated opcode: a convenience over `self.cache(...).put(...)`
    /// against that name, named for discoverability. Both `name` and
    /// `content` are sent `WrappedMessage`-wrapped
    /// (`query::wrap_string`), confirmed empirically to be required:
    /// this cache's own storage is fixed to `application/x-protostream`
    /// regardless of what a request declares, and an unwrapped scalar
    /// carries no type of its own for the server to recognize it as a
    /// string rather than, say, an integer.
    pub async fn register_proto_schema(&self, name: &str, content: &str) -> Result<()> {
        self.cache(PROTOBUF_METADATA_CACHE_NAME)
            .put(
                &crate::query::wrap_string(name),
                &crate::query::wrap_string(content),
                Expiration::Default,
                Expiration::Default,
            )
            .await
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

    /// How many times `RemoteCache`'s dispatch loop retries an
    /// operation, beyond its first attempt, before surfacing the last
    /// attempt's error. See
    /// `docs/adr/0011-retry-policy-and-node-health.md`.
    pub fn max_retries(&self) -> usize {
        *self.0.max_retries.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Overrides `max_retries` from this call onward.
    pub fn set_max_retries(&self, max_retries: usize) {
        *self
            .0
            .max_retries
            .write()
            .unwrap_or_else(|p| p.into_inner()) = max_retries;
    }

    /// How long a node stays quarantined (skipped as a retry candidate)
    /// after it fails, or `None` if quarantine is disabled. See
    /// `docs/adr/0011-retry-policy-and-node-health.md`.
    pub fn server_failure_timeout(&self) -> Option<Duration> {
        *self
            .0
            .server_failure_timeout
            .read()
            .unwrap_or_else(|p| p.into_inner())
    }

    /// Overrides `server_failure_timeout` from this call onward. `None`
    /// disables quarantine: every candidate is tried regardless of
    /// recent failures.
    pub fn set_server_failure_timeout(&self, server_failure_timeout: Option<Duration>) {
        *self
            .0
            .server_failure_timeout
            .write()
            .unwrap_or_else(|p| p.into_inner()) = server_failure_timeout;
    }

    /// How long `run_switch_back_monitor` waits between attempts to
    /// switch back to a cluster an automatic failover abandoned, or
    /// `None` if that monitor is disabled. See
    /// `docs/adr/0020-multicluster-proactive-switch-back.md`.
    pub fn switch_back_check_interval(&self) -> Option<Duration> {
        *self
            .0
            .switch_back_check_interval
            .read()
            .unwrap_or_else(|p| p.into_inner())
    }

    /// Overrides `switch_back_check_interval` from this call onward.
    /// `None` disables the monitor: no new one starts after the next
    /// automatic failover, and one already running stops at its next
    /// wake instead of switching back.
    pub fn set_switch_back_check_interval(&self, interval: Option<Duration>) {
        *self
            .0
            .switch_back_check_interval
            .write()
            .unwrap_or_else(|p| p.into_inner()) = interval;
    }

    /// `pub(crate)` accessor for `remote_cache.rs`'s dispatch loop.
    pub(crate) fn node_health(&self) -> &NodeHealth {
        &self.0.node_health
    }

    /// Configures an alternate cluster for failover/disaster recovery
    /// (`docs/adr/0017-multi-cluster-failover.md`), reachable later by
    /// `name` through `switch_to_cluster`, or tried automatically once
    /// every seed of the currently active cluster stops responding.
    /// `name` must not already be configured, and must not be
    /// `DEFAULT_CLUSTER_NAME` (the sentinel reserved for the cluster
    /// this client was originally constructed with); `seed_addrs` must
    /// not be empty. Neither is validated by actually dialing anything:
    /// a cluster that turns out to be unreachable is simply skipped the
    /// next time something tries to fail over to it.
    pub fn add_cluster(&self, name: impl Into<String>, seed_addrs: Vec<SocketAddr>) -> Result<()> {
        let name = name.into();
        if seed_addrs.is_empty() {
            return Err(Error::InvalidClusterConfig(
                "seed_addrs must not be empty".to_string(),
            ));
        }
        let mut clusters = self.0.clusters.write().unwrap_or_else(|p| p.into_inner());
        if clusters.iter().any(|(existing, _)| *existing == name) {
            return Err(Error::InvalidClusterConfig(format!(
                "cluster {name:?} is already configured"
            )));
        }
        clusters.push((name, seed_addrs));
        Ok(())
    }

    /// Switches to the cluster named `name` (`DEFAULT_CLUSTER_NAME`, or
    /// one `add_cluster` added) immediately, without checking that it
    /// is actually reachable: this matches the Java client's own
    /// `manualSwitchToCluster`, not its liveness-checked
    /// `switchToCluster`. If `name` turns out to be unreachable, the
    /// next operation's own retry chain (which also tries every other
    /// configured cluster, see `try_failover_to_live_cluster`) is what
    /// surfaces that, the same as it would for any other connection
    /// failure.
    ///
    /// Resets the known topology: a different cluster's segment
    /// ownership has nothing to do with the one just left behind, the
    /// same reason `try_failover_to_live_cluster` resets it on an
    /// automatic switch.
    ///
    /// Also drains every pool the old cluster owned
    /// (`evict_abandoned_cluster`) and bumps `cluster_generation`, so
    /// any `CacheListener` (raw, or wrapped in a `NearCachedCache`)
    /// registered against the cluster just left behind stops reading
    /// from it instead of silently continuing to trust a feed that no
    /// longer has anything to do with the cluster this client now
    /// routes against. The bump happens while `active`'s write lock is
    /// still held, not after releasing it: `active_seed_and_generation`
    /// reads the seed and subscribes to the generation together under
    /// one read-lock acquisition, so pairing the write side the same
    /// way is what actually makes that combined read atomic relative to
    /// a switch, rather than just moving the race from one pair of
    /// reads to another. Found by review before merge: see
    /// `docs/adr/0019-multicluster-listener-and-pool-cleanup.md`.
    pub fn switch_to_cluster(&self, name: &str) -> Result<()> {
        let clusters = self.0.clusters.read().unwrap_or_else(|p| p.into_inner());
        let (name, seed_addrs) = clusters
            .iter()
            .find(|(existing, _)| existing == name)
            .ok_or_else(|| Error::UnknownCluster(name.to_string()))?
            .clone();
        drop(clusters);
        let &active_seed_addr = seed_addrs.first().ok_or_else(|| {
            Error::InvalidClusterConfig(format!("cluster {name:?} has no seed addresses"))
        })?;
        let old = {
            let mut active = self.0.active.write().unwrap_or_else(|p| p.into_inner());
            let old = std::mem::replace(
                &mut *active,
                ActiveCluster {
                    name,
                    seed_addrs,
                    active_seed_addr,
                    topology: None,
                },
            );
            self.0
                .cluster_generation
                .send_modify(|generation| *generation += 1);
            old
        };
        self.evict_abandoned_cluster(&old);
        // A different cluster's nodes failing recently has nothing to
        // do with this one's own health.
        self.0.node_health.clear_all();
        Ok(())
    }

    /// The name of the cluster this client currently routes against:
    /// `DEFAULT_CLUSTER_NAME` until the first switch, manual or
    /// automatic.
    pub fn active_cluster_name(&self) -> String {
        self.0
            .active
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .name
            .clone()
    }

    /// `active_cluster_name` and the current value of
    /// `cluster_generation`, read together under one acquisition of
    /// `active`'s read lock, the same race-free pairing
    /// `active_seed_addr_and_generation` already gives the seed address
    /// and a live subscription: since every switch bumps the generation
    /// while still holding `active`'s write lock, a read lock held across
    /// both reads here guarantees the pair always describes the same
    /// cluster, never a mix of an old name with a new generation or vice
    /// versa. `Transaction::new` captures both once, at
    /// `RemoteCache::begin_transaction` time, so a later `get`/`commit`
    /// can tell whether a cluster switch has happened since (see the
    /// addendum in `docs/adr/0018-transactions-non-xa-one-phase.md`).
    pub(crate) fn active_cluster_name_and_generation(&self) -> (String, u64) {
        let active = self.0.active.read().unwrap_or_else(|p| p.into_inner());
        (
            active.name.clone(),
            *self.0.cluster_generation.subscribe().borrow(),
        )
    }

    /// `cluster_name`'s own auth/TLS, falling back to this client's
    /// global `auth`/`tls` for whichever field (or both) that
    /// cluster has no override for
    /// (`docs/adr/0021-multicluster-per-cluster-auth-and-tls.md`).
    /// Every connection this client ever dials goes through this:
    /// `try_promote_seed` (any cluster, switching to it or just
    /// trying another of its own seeds) and `open_and_authenticate`/
    /// `authenticate_with` (always the active cluster, see
    /// `open_and_authenticate`'s own docs for why).
    fn effective_auth_and_tls(
        &self,
        cluster_name: &str,
    ) -> (Option<AuthMethod>, Option<TlsConfig>) {
        let overrides = self
            .0
            .cluster_overrides
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let over = overrides.get(cluster_name);
        let auth = over.and_then(|o| o.auth.clone()).or_else(|| {
            self.0
                .auth
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
        });
        let tls = over
            .and_then(|o| o.tls.clone())
            .or_else(|| self.0.tls.clone());
        (auth, tls)
    }

    /// The active seed address and a fresh subscription to this
    /// client's cluster-switch generation counter, read together under
    /// one acquisition of `active`'s read lock. `pub(crate)`:
    /// `RemoteCache::listen_with` dials the returned seed and hands the
    /// receiver to `CacheListener::register`, which captures its
    /// current value and watches for it changing later, so a listener
    /// can tell a switch away from the cluster it was registered
    /// against apart from one that never happened.
    ///
    /// Reading these two as separate calls (seed first, subscribe
    /// later, with the dial itself in between) would reopen exactly
    /// the race this exists to close: `switch_to_cluster`/
    /// `try_promote_seed`'s switch branch bump the generation while
    /// still holding `active`'s write lock, so a read lock acquired
    /// here either observes the old seed with a pre-bump generation
    /// (the later bump then correctly arrives as a change) or the new
    /// seed with the matching post-bump generation (no spurious change
    /// for the switch this listener is already correctly aligned
    /// with), never a mix of the two. Found by review before merge:
    /// see `docs/adr/0019-multicluster-listener-and-pool-cleanup.md`.
    pub(crate) fn active_seed_addr_and_generation(&self) -> (SocketAddr, watch::Receiver<u64>) {
        let active = self.0.active.read().unwrap_or_else(|p| p.into_inner());
        (
            active.active_seed_addr,
            self.0.cluster_generation.subscribe(),
        )
    }

    /// Shared by `failover_seed` (tries the rest of the active
    /// cluster's own seeds) and `try_failover_to_live_cluster` (tries
    /// another cluster's seeds): dials each of `candidates` in order,
    /// skipping `skip`, until one accepts a connection and, if
    /// credentials were set, authenticates. The winner is pushed into
    /// its `(addr, cache_name)` pool as an idle connection and
    /// promoted to `active_seed_addr`. When `switch_to_cluster_name`
    /// names a cluster, `candidates` is taken to be that cluster's
    /// own full seed list, and the whole `ActiveCluster` (name, seed
    /// list, active seed, topology reset to `None`) is replaced in
    /// one write, not as separate field updates (`ActiveCluster`'s
    /// own docs); `failover_seed` passes `None` since it never leaves
    /// the active cluster, so only `active_seed_addr` changes. A real
    /// switch also drains the old cluster's pools and bumps
    /// `cluster_generation`, the same cleanup `switch_to_cluster`
    /// itself performs: see its own docs and
    /// `docs/adr/0019-multicluster-listener-and-pool-cleanup.md`.
    ///
    /// `expected_generation`, when given, guards the commit itself,
    /// not just the decision to attempt it: right before replacing
    /// `active` (under the same write lock the replace itself uses),
    /// this aborts with an error instead of committing if
    /// `cluster_generation` no longer matches. Every existing caller
    /// (`failover_seed`, `try_failover_to_live_cluster`) passes `None`
    /// and keeps committing unconditionally, the same as before this
    /// parameter existed; only `run_switch_back_monitor` passes
    /// `Some(_)`. Without this, a manual `switch_to_cluster` landing
    /// while the monitor's own dial above is still in flight (bounded
    /// only by the connection timeout, not this short) would be
    /// silently overwritten the moment that dial succeeds, since
    /// nothing checked `cluster_generation` again that close to the
    /// commit. Checking only once, before the dial even starts (what
    /// the monitor itself also does, to avoid dialing at all when
    /// already superseded), is not enough on its own: the window
    /// this closes is the one between that early check and the
    /// commit, which the dial's own latency can make arbitrarily
    /// wide. Found by review before merge: see
    /// `docs/adr/0020-multicluster-proactive-switch-back.md`.
    async fn try_promote_seed(
        &self,
        cache_name: &str,
        candidates: &[SocketAddr],
        skip: Option<SocketAddr>,
        switch_to_cluster_name: Option<&str>,
        expected_generation: Option<u64>,
    ) -> Result<SocketAddr> {
        let topology_id = self.current_topology_id();
        let timeout = *self.0.timeout.read().unwrap_or_else(|p| p.into_inner());
        // The cluster whose own auth/TLS override (if any) applies: the
        // one being switched to, or, for `failover_seed`'s "same
        // cluster, another seed" case (`switch_to_cluster_name` is
        // `None`), whichever cluster is currently active.
        let target_cluster_name = match switch_to_cluster_name {
            Some(name) => name.to_string(),
            None => self
                .0
                .active
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .name
                .clone(),
        };
        let (auth, tls) = self.effective_auth_and_tls(&target_cluster_name);
        let mut last_err: Option<Error> = None;
        for &addr in candidates {
            if Some(addr) == skip {
                continue;
            }
            let mut conn = match HotRodConnection::connect_hash_aware(
                addr,
                cache_name,
                topology_id,
                timeout,
                tls.as_ref(),
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
            let switched_from = {
                let mut active = self.0.active.write().unwrap_or_else(|p| p.into_inner());
                if let Some(expected) = expected_generation {
                    if *self.0.cluster_generation.subscribe().borrow() != expected {
                        // Superseded by a newer switch while the dial
                        // above was in flight: abort without ever
                        // touching `active`, rather than silently
                        // overwriting whatever that newer switch just
                        // decided.
                        return Err(Error::Io(io::Error::other(
                            "cluster switch superseded by a newer one before this one could commit",
                        )));
                    }
                }
                match switch_to_cluster_name {
                    Some(name) => {
                        let old = std::mem::replace(
                            &mut *active,
                            ActiveCluster {
                                name: name.to_string(),
                                seed_addrs: candidates.to_vec(),
                                active_seed_addr: addr,
                                topology: None,
                            },
                        );
                        // Bumped while `active`'s write lock is still
                        // held: see `switch_to_cluster`'s own docs for
                        // why.
                        self.0
                            .cluster_generation
                            .send_modify(|generation| *generation += 1);
                        Some(old)
                    }
                    None => {
                        active.active_seed_addr = addr;
                        None
                    }
                }
            };
            if let Some(old) = &switched_from {
                self.evict_abandoned_cluster(old);
                // A different cluster's nodes failing recently has
                // nothing to do with this one's own health.
                self.0.node_health.clear_all();
            }
            return Ok(addr);
        }
        Err(last_err.unwrap_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotConnected,
                "no address available to fail over to",
            ))
        }))
    }

    /// Tried by `remote_cache.rs`'s dispatch loop right before it would
    /// otherwise give up (`docs/adr/0017-multi-cluster-failover.md`):
    /// every candidate of the active cluster, including every other
    /// seed via `failover_seed`, has already failed by this point, so
    /// this tries every other configured cluster in turn, in the order
    /// `add_cluster` was called, until one responds. On success, the
    /// cluster switch this performs is visible both through
    /// `active_cluster_name` and as a `tracing` `WARN` event, since
    /// nothing about the call that triggered it otherwise indicates a
    /// whole cluster was just abandoned.
    ///
    /// Also starts a background monitor
    /// (`docs/adr/0020-multicluster-proactive-switch-back.md`) that
    /// periodically retries switching back to the cluster just
    /// abandoned (`active_name`, its seeds already known from
    /// `clusters`), until it succeeds, is disabled
    /// (`set_switch_back_check_interval(None)`), is superseded by a
    /// later switch of any kind (manual or another automatic
    /// failover; detected via `cluster_generation`, so this never
    /// needs to know about the monitor to cancel it), or every clone
    /// of this client is dropped. A manual `switch_to_cluster` never
    /// starts one: only an automatic failover implies "this was not
    /// the caller's choice, get back if possible".
    pub(crate) async fn try_failover_to_live_cluster(
        &self,
        cache_name: &str,
    ) -> Result<SocketAddr> {
        let active_name = self.active_cluster_name();
        let clusters = self
            .0
            .clusters
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let mut last_err: Option<Error> = None;
        for (name, seeds) in &clusters {
            if *name == active_name {
                continue;
            }
            match self
                .try_promote_seed(cache_name, seeds, None, Some(name), None)
                .await
            {
                Ok(addr) => {
                    tracing::warn!(
                        from_cluster = %active_name,
                        to_cluster = %name,
                        "switched to a failover cluster after the active one became unreachable"
                    );
                    if let Some((_, abandoned_seeds)) =
                        clusters.iter().find(|(n, _)| *n == active_name)
                    {
                        let started_generation = *self.0.cluster_generation.subscribe().borrow();
                        tokio::spawn(run_switch_back_monitor(
                            Arc::downgrade(&self.0),
                            active_name.clone(),
                            abandoned_seeds.clone(),
                            started_generation,
                        ));
                    }
                    return Ok(addr);
                }
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotConnected,
                "no other cluster configured or reachable",
            ))
        }))
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
        let seed = self
            .0
            .active
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .active_seed_addr;
        let topology_id = self.current_topology_id();
        let timeout = *self.0.timeout.read().unwrap_or_else(|p| p.into_inner());
        // Validates against whichever TLS the active cluster actually
        // dials with, honoring an override already set for it rather
        // than always assuming the global `tls`.
        let (_, tls) = self.effective_auth_and_tls(&self.active_cluster_name());
        let mut conn = HotRodConnection::connect_hash_aware(
            seed,
            "",
            topology_id,
            timeout,
            tls.as_ref(),
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

    /// Same as `authenticate_plain`, but only for connections to the
    /// cluster named `name` (`DEFAULT_CLUSTER_NAME`, or one
    /// `add_cluster` added), independently of this client's global
    /// credentials. See `authenticate_for_cluster_with` and
    /// `docs/adr/0021-multicluster-per-cluster-auth-and-tls.md`.
    pub async fn authenticate_plain_for_cluster(
        &self,
        name: &str,
        authzid: &str,
        authcid: &str,
        password: &str,
    ) -> Result<()> {
        self.authenticate_for_cluster_with(
            name,
            AuthMethod::Plain {
                authzid: authzid.to_string(),
                authcid: authcid.to_string(),
                password: password.to_string(),
            },
        )
        .await
    }

    /// Same as `authenticate_scram`, but only for connections to the
    /// cluster named `name`. See `authenticate_plain_for_cluster`.
    pub async fn authenticate_scram_for_cluster(
        &self,
        name: &str,
        authcid: &str,
        password: &str,
    ) -> Result<()> {
        self.authenticate_for_cluster_with(
            name,
            AuthMethod::Scram {
                authcid: authcid.to_string(),
                password: password.to_string(),
            },
        )
        .await
    }

    /// Same as `authenticate_digest`, but only for connections to the
    /// cluster named `name`. See `authenticate_plain_for_cluster`.
    pub async fn authenticate_digest_for_cluster(
        &self,
        name: &str,
        authcid: &str,
        password: &str,
    ) -> Result<()> {
        self.authenticate_for_cluster_with(
            name,
            AuthMethod::Digest {
                authcid: authcid.to_string(),
                password: password.to_string(),
            },
        )
        .await
    }

    /// Same as `authenticate_oauthbearer`, but only for connections to
    /// the cluster named `name`. See `authenticate_plain_for_cluster`.
    pub async fn authenticate_oauthbearer_for_cluster(
        &self,
        name: &str,
        authzid: &str,
        token: &str,
    ) -> Result<()> {
        self.authenticate_for_cluster_with(
            name,
            AuthMethod::OAuthBearer {
                authzid: authzid.to_string(),
                token: token.to_string(),
            },
        )
        .await
    }

    /// `authenticate_with`'s per-cluster counterpart: dials `name`'s own
    /// first seed address (not necessarily the active cluster, matching
    /// `authenticate_with`'s own single-address, no-fallback dial) to
    /// validate `method` before storing it as that cluster's own auth
    /// override, rather than this client's global default. Dials with
    /// `name`'s own TLS override already in place, if any was set via
    /// `set_cluster_tls` before this call.
    ///
    /// Invalidates every pool's idle connections when `name` is the
    /// currently active cluster, the same reason `authenticate_with`
    /// does: an idle pooled connection for one of this cluster's nodes
    /// may have been opened under the credentials just replaced.
    /// Harmless, if slightly broader than strictly necessary, when
    /// `name` is not active: there is normally nothing pooled for an
    /// inactive cluster's nodes anyway (`ClusterOverride`'s own docs).
    async fn authenticate_for_cluster_with(&self, name: &str, method: AuthMethod) -> Result<()> {
        let seed_addrs = {
            let clusters = self.0.clusters.read().unwrap_or_else(|p| p.into_inner());
            clusters
                .iter()
                .find(|(existing, _)| existing == name)
                .ok_or_else(|| Error::UnknownCluster(name.to_string()))?
                .1
                .clone()
        };
        let &addr = seed_addrs.first().ok_or_else(|| {
            Error::InvalidClusterConfig(format!("cluster {name:?} has no seed addresses"))
        })?;
        let timeout = *self.0.timeout.read().unwrap_or_else(|p| p.into_inner());
        let (_, tls) = self.effective_auth_and_tls(name);
        let mut conn = HotRodConnection::connect_hash_aware(
            addr,
            "",
            DEFAULT_TOPOLOGY_ID,
            timeout,
            tls.as_ref(),
            true,
        )
        .await?;
        method.authenticate(&mut conn).await?;
        self.0
            .cluster_overrides
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .entry(name.to_string())
            .or_default()
            .auth = Some(method);
        if self.active_cluster_name() == name {
            for pool in self
                .0
                .pools
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .values()
            {
                pool.invalidate_idle();
            }
        }
        Ok(())
    }

    /// Overrides the TLS configuration used for every connection this
    /// client dials to the cluster named `name` (`DEFAULT_CLUSTER_NAME`,
    /// or one `add_cluster` added), independently of this client's
    /// global `tls`. `None` reverts to the global configuration.
    ///
    /// Purely local bookkeeping, no dial: unlike
    /// `authenticate_plain_for_cluster` and its siblings, `TlsConfig` is
    /// inert data until a real connection attempt uses it, the same
    /// reason this client's own global `tls` has no setter at all
    /// today (fixed at construction). See
    /// `docs/adr/0021-multicluster-per-cluster-auth-and-tls.md`.
    ///
    /// Invalidates every pool's idle connections when `name` is the
    /// currently active cluster, the same reasoning
    /// `authenticate_for_cluster_with` documents.
    pub fn set_cluster_tls(&self, name: &str, tls: Option<TlsConfig>) -> Result<()> {
        {
            let clusters = self.0.clusters.read().unwrap_or_else(|p| p.into_inner());
            if !clusters.iter().any(|(existing, _)| existing == name) {
                return Err(Error::UnknownCluster(name.to_string()));
            }
        }
        self.0
            .cluster_overrides
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .entry(name.to_string())
            .or_default()
            .tls = tls;
        if self.active_cluster_name() == name {
            for pool in self
                .0
                .pools
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .values()
            {
                pool.invalidate_idle();
            }
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
            .active
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .topology
            .as_ref()
            .map_or(DEFAULT_TOPOLOGY_ID, |topology| topology.topology_id)
    }

    /// `active_seed_addr` and a clone of the current topology, if any,
    /// read together under one lock acquisition, not two: the
    /// preamble `owner_addr`, `owner_and_backup_addrs` and
    /// `nodes_and_owned_segments` each start with, so none of the
    /// three can ever pair one cluster's active seed with a
    /// different cluster's topology (see `ActiveCluster`'s own docs).
    fn active_seed_and_topology(&self) -> (SocketAddr, Option<Arc<ClusterTopology>>) {
        let active = self.0.active.read().unwrap_or_else(|p| p.into_inner());
        (active.active_seed_addr, active.topology.clone())
    }

    /// The address to route `key` to, and the topology server it came
    /// from: the segment's primary owner once a topology is known,
    /// otherwise the seed connection with no origin.
    pub(crate) async fn owner_addr(
        &self,
        key: &[u8],
    ) -> Result<(SocketAddr, Option<TopologyServer>)> {
        let (active_seed, topology) = self.active_seed_and_topology();
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
        let addr = resolve_cached_addr(&topology, primary).await?;
        Ok((addr, Some(server)))
    }

    /// Every owner of `key`'s segment, primary first then backups in
    /// the order `segment_owners` lists them, with the active seed
    /// always appended last as the final fallback, or just the active
    /// seed alone if no topology is known yet: the retry candidate
    /// list `RemoteCache`'s dispatch loop builds for a keyed operation
    /// (`docs/adr/0011-retry-policy-and-node-health.md`). Unlike
    /// `owner_addr`, used by `get_stream`/`put_stream`, which pin to
    /// one connection and never retry, so resolving backups for them
    /// would be wasted work.
    ///
    /// An owner whose host fails to resolve is skipped rather than
    /// aborting the whole list: a single bad backup should not discard
    /// a primary owner that already resolved successfully, or the
    /// active seed this always falls back to.
    pub(crate) async fn owner_and_backup_addrs(
        &self,
        key: &[u8],
    ) -> Result<Vec<(SocketAddr, Option<TopologyServer>)>> {
        let (active_seed, topology) = self.active_seed_and_topology();
        let Some(topology) = topology else {
            return Ok(vec![(active_seed, None)]);
        };
        if topology.segment_owners.is_empty() {
            return Ok(vec![(active_seed, None)]);
        }
        let segment = hash::segment(
            key,
            topology.segment_owners.len() as u32,
            topology.hash_function_version,
        )?;
        let owners = &topology.segment_owners[segment as usize];
        let mut result = Vec::with_capacity(owners.len() + 1);
        for &owner in owners {
            // Safe: `topology::read_topology_update` rejects any owner
            // index that is out of range for `servers` before this
            // type is built.
            let server = topology.servers[owner as usize].clone();
            if let Ok(addr) = resolve_cached_addr(&topology, owner).await {
                result.push((addr, Some(server)));
            }
        }
        result.push((active_seed, None));
        Ok(result)
    }

    /// Every node that primary-owns at least one segment, each paired with
    /// the segments it owns: the fan-out plan for `CacheIterator` (see
    /// `iteration.rs`), which opens one server-side iterator per entry here,
    /// one at a time. Resolves and caches each address the same way
    /// `owner_addr` does for a single key.
    ///
    /// Without a topology yet (a single-node cache, or no update has
    /// arrived), returns just the seed with an empty segment list: `None`
    /// segments on `IterationStart` means no filter, so the seed's own
    /// iterator already covers the whole cache.
    pub(crate) async fn nodes_and_owned_segments(
        &self,
    ) -> Result<Vec<(SocketAddr, Option<TopologyServer>, Vec<u32>)>> {
        let (active_seed, topology) = self.active_seed_and_topology();
        let Some(topology) = topology else {
            return Ok(vec![(active_seed, None, Vec::new())]);
        };
        if topology.segment_owners.is_empty() {
            return Ok(vec![(active_seed, None, Vec::new())]);
        }

        let mut segments_by_primary: HashMap<u32, Vec<u32>> = HashMap::new();
        // A segment with no owner at all is not something
        // `read_topology_update` rejects, so it is handled the same way
        // `owner_addr` handles it for a single key: fall back to the
        // seed rather than silently never iterating that segment.
        let mut unowned_segments = Vec::new();
        for (segment, owners) in topology.segment_owners.iter().enumerate() {
            match owners.first() {
                Some(&primary) => segments_by_primary
                    .entry(primary)
                    .or_default()
                    .push(segment as u32),
                None => unowned_segments.push(segment as u32),
            }
        }

        let mut targets = Vec::with_capacity(segments_by_primary.len() + 1);
        if !unowned_segments.is_empty() {
            targets.push((active_seed, None, unowned_segments));
        }
        for (primary, segments) in segments_by_primary {
            // Safe: `topology::read_topology_update` rejects any owner
            // index that is out of range for `servers` before this type is
            // built.
            let server = topology.servers[primary as usize].clone();
            let addr = resolve_cached_addr(&topology, primary).await?;
            targets.push((addr, Some(server), segments));
        }
        Ok(targets)
    }

    fn pool_for(&self, addr: SocketAddr, cache_name: &str) -> Arc<ConnectionPool> {
        get_or_insert_with(&self.0.pools, (addr, cache_name.to_string()), || {
            Arc::new(ConnectionPool::new(DEFAULT_MAX_CONNECTIONS_PER_NODE))
        })
    }

    /// The statistics counters for `cache_name`, created the first
    /// time this cache name is seen, by an operation or by
    /// `RemoteCache::statistics` itself. Same lazy-insert shape as
    /// `pool_for`, sharing `get_or_insert_with` with it rather than
    /// repeating the double-checked-locking pattern a second time.
    pub(crate) fn stats_for(&self, cache_name: &str) -> Arc<CacheStatisticsInner> {
        get_or_insert_with(&self.0.cache_stats, cache_name.to_string(), Arc::default)
    }

    /// A snapshot of every connection pool this client currently has,
    /// one entry per `(node, cache name)` pair it has talked to. See
    /// `docs/adr/0010-client-statistics-and-tracing.md`: this crate's
    /// own addition, with no Java client counterpart.
    pub fn pool_statistics(&self) -> Vec<PoolStatistics> {
        self.0
            .pools
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|((addr, cache_name), pool)| {
                let counts = pool.slot_counts();
                PoolStatistics {
                    address: *addr,
                    cache_name: cache_name.clone(),
                    idle_connections: counts.idle,
                    checked_out_connections: counts.checked_out,
                    max_connections: pool.max_size(),
                }
            })
            .collect()
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
    ///
    /// Always reached for a node of the currently active cluster: `addr`
    /// is either that cluster's own seed or one of its topology-discovered
    /// nodes, never a node of an abandoned cluster (pools are keyed by
    /// address, not by cluster, but `evict_abandoned_cluster` drains an
    /// old cluster's pools on every switch, see ADR 0019). So this
    /// resolves the active cluster's own auth/TLS override, if any.
    pub(crate) async fn open_and_authenticate(
        &self,
        addr: SocketAddr,
        cache_name: &str,
        origin: Option<TopologyServer>,
    ) -> Result<HotRodConnection> {
        let topology_id = self.current_topology_id();
        let timeout = *self.0.timeout.read().unwrap_or_else(|p| p.into_inner());
        let (auth, tls) = self.effective_auth_and_tls(&self.active_cluster_name());
        let mut conn = HotRodConnection::connect_hash_aware(
            addr,
            cache_name,
            topology_id,
            timeout,
            tls.as_ref(),
            origin.is_none(),
        )
        .await?;
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
        // `previous` and `seed_addrs` read together, under one lock
        // acquisition: see `ActiveCluster`'s own docs for why reading
        // them as two separate locks could pair a stale `previous`
        // from one cluster with another's seed list, after a
        // concurrent switch lands between the two reads.
        let (previous, seed_addrs) = {
            let active = self.0.active.read().unwrap_or_else(|p| p.into_inner());
            (active.active_seed_addr, active.seed_addrs.clone())
        };
        self.try_promote_seed(cache_name, &seed_addrs, Some(previous), None, None)
            .await
    }

    /// Applies whatever topology update `conn` parsed from its last
    /// response, if any, and reconciles every pool against it: an
    /// address whose origin server is no longer listed has left the
    /// cluster, so every pool for that address, across every cache, is
    /// closed and dropped rather than kept open forever. The seed address
    /// is always kept regardless, since it is the permanent retry
    /// fallback.
    ///
    /// Also lifts every node's quarantine (`node_health.clear_all()`)
    /// when the topology id actually changed: a new topology already
    /// means the cluster's membership view changed, so stale quarantine
    /// state from before it should not outlive it
    /// (`docs/adr/0011-retry-policy-and-node-health.md`).
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

        // `old_topology_id` read and `topology` written under the
        // same lock acquisition as `active_seed`, not three separate
        // ones: see `ActiveCluster`'s own docs.
        let active_seed = {
            let mut active = self.0.active.write().unwrap_or_else(|p| p.into_inner());
            let old_topology_id = active
                .topology
                .as_ref()
                .map_or(DEFAULT_TOPOLOGY_ID, |topology| topology.topology_id);
            if new_topology.topology_id != old_topology_id {
                self.0.node_health.clear_all();
            }
            active.topology = Some(new_topology.clone());
            active.active_seed_addr
        };
        let node_origin = self.0.node_origin.read().unwrap_or_else(|p| p.into_inner());
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
        drop(node_origin);
        self.evict_pools_for(&stale_addrs);
    }

    /// Closes and removes every pool for an address in `addrs`, across
    /// every cache name, and drops the matching `node_origin` entries.
    /// Shared by `record_topology_update` (an address whose origin
    /// server left the topology) and `evict_abandoned_cluster` (every
    /// address a cluster just switched away from owned): neither case
    /// has any further use for a pool to one of these addresses, since
    /// in the first case the node left the cluster, and in the second
    /// the cluster itself is no longer the active one. See
    /// `docs/adr/0019-multicluster-listener-and-pool-cleanup.md`.
    fn evict_pools_for(&self, addrs: &[SocketAddr]) {
        if addrs.is_empty() {
            return;
        }
        self.0
            .node_origin
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|addr, _| !addrs.contains(addr));
        self.0
            .pools
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(pool_addr, _cache_name), pool| {
                if addrs.contains(pool_addr) {
                    pool.close();
                    false
                } else {
                    true
                }
            });
    }

    /// Drains every pool `old` owned: its own seed list, plus whatever
    /// topology addresses had already been resolved against it.
    /// Called right after a cluster switch (manual or automatic)
    /// replaces `old` as the active cluster, since none of its
    /// addresses belong to the cluster now active. Unlike
    /// `record_topology_update`'s own node-left-the-cluster cleanup,
    /// nothing here is protected as "the active seed": by the time
    /// this runs, `old`'s own seed is no longer active at all. If two
    /// clusters happen to share an address (an unusual configuration),
    /// that address's pool is closed anyway; the Java client's own
    /// "discard and rediscover" switch behavior does not special-case
    /// this either. See
    /// `docs/adr/0019-multicluster-listener-and-pool-cleanup.md`.
    fn evict_abandoned_cluster(&self, old: &ActiveCluster) {
        let mut addrs = old.seed_addrs.clone();
        if let Some(topology) = &old.topology {
            addrs.extend(
                topology
                    .resolved_addrs
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .values()
                    .copied(),
            );
        }
        self.evict_pools_for(&addrs);
    }
}

/// Periodically retries switching back to `target_name` (an
/// automatic failover's own abandoned cluster, with `target_seeds`
/// already known at the time it was abandoned), until it succeeds,
/// is superseded, or every clone of the client is dropped. See
/// `try_failover_to_live_cluster`'s own docs for when this is
/// spawned, and `docs/adr/0020-multicluster-proactive-switch-back.md`
/// for the full design.
///
/// Holds only a `Weak<ClientInner>`, never an `Arc`: unlike
/// `near_cache.rs`'s invalidation task (which deliberately keeps its
/// state alive via a strong `Arc` for as long as any
/// `NearCachedCache` clone exists), this task must never be the
/// reason a `HotRodClient` and everything it owns stays alive after
/// every clone of it is otherwise dropped. `weak.upgrade()` failing
/// is this monitor's own signal to stop, checked on every wake
/// instead of needing an explicit cancellation handle.
async fn run_switch_back_monitor(
    weak: std::sync::Weak<ClientInner>,
    target_name: String,
    target_seeds: Vec<SocketAddr>,
    started_generation: u64,
) {
    loop {
        let Some(inner) = weak.upgrade() else {
            return;
        };
        let current_interval = *inner
            .switch_back_check_interval
            .read()
            .unwrap_or_else(|p| p.into_inner());
        drop(inner);
        let Some(interval) = current_interval else {
            return;
        };
        tokio::time::sleep(interval).await;

        let Some(inner) = weak.upgrade() else {
            return;
        };
        let still_enabled = inner
            .switch_back_check_interval
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .is_some();
        let generation_unchanged =
            *inner.cluster_generation.subscribe().borrow() == started_generation;
        if !still_enabled || !generation_unchanged {
            // Disabled while this monitor was asleep, or something
            // else (a manual switch, or another automatic failover)
            // already changed the active cluster since this monitor
            // started: nothing left for it to do, checked again here
            // rather than only before the sleep, so disabling takes
            // effect as soon as this monitor next wakes, not only
            // after one more switch-back attempt already in flight.
            return;
        }

        let client = HotRodClient(inner);
        match client
            .try_promote_seed(
                "",
                &target_seeds,
                None,
                Some(&target_name),
                Some(started_generation),
            )
            .await
        {
            Ok(_) => {
                tracing::warn!(
                    to_cluster = %target_name,
                    "switched back to the recovered cluster"
                );
                return;
            }
            Err(_) => continue,
        }
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

/// `primary`'s address, from `topology`'s `resolved_addrs` cache if
/// already resolved, or resolved and cached otherwise. Shared by
/// `owner_addr` (one segment's owner) and `nodes_and_owned_segments`
/// (every owner at once), so the two cannot silently diverge on this
/// caching rule the way they once risked doing as separate copies.
///
/// Returns early from the cache-hit branch rather than writing this as
/// a single `if let ... else { ... await ... }` expression: a read
/// guard from the condition of an `if`/`match` used as a `let`
/// initializer is held for the whole statement, not just its own
/// branch, so that shape would hold this lock across
/// `resolve_server_addr`'s `.await` even on the branch that never
/// reaches it.
async fn resolve_cached_addr(topology: &ClusterTopology, primary: u32) -> Result<SocketAddr> {
    if let Some(&addr) = topology
        .resolved_addrs
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .get(&primary)
    {
        return Ok(addr);
    }
    // Safe: every caller only passes a `primary` it already validated
    // as a real index into `topology.servers`.
    let server = &topology.servers[primary as usize];
    let addr = resolve_server_addr(server).await?;
    topology
        .resolved_addrs
        .write()
        .unwrap_or_else(|p| p.into_inner())
        .insert(primary, addr);
    Ok(addr)
}

/// `map`'s value for `key`, inserting `make()`'s result first if
/// none exists yet. A read lock is tried first, so the common case
/// (the key already exists) never takes the write lock at all.
/// Shared by `pool_for` and `stats_for`, the only two places this
/// client lazily creates a keyed, `Arc`-shared value this way.
fn get_or_insert_with<K, V>(map: &RwLock<HashMap<K, V>>, key: K, make: impl FnOnce() -> V) -> V
where
    K: std::hash::Hash + Eq,
    V: Clone,
{
    if let Some(value) = map.read().unwrap_or_else(|p| p.into_inner()).get(&key) {
        return value.clone();
    }
    map.write()
        .unwrap_or_else(|p| p.into_inner())
        .entry(key)
        .or_insert_with(make)
        .clone()
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

    #[tokio::test]
    async fn max_retries_and_server_failure_timeout_default_and_can_be_overridden() {
        let client = HotRodClient::from_inner(ClientInner {
            active: RwLock::new(ActiveCluster {
                name: HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                seed_addrs: vec![],
                active_seed_addr: "127.0.0.1:1".parse().unwrap(),
                topology: None,
            }),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_secs(5)),
            cache_stats: RwLock::new(HashMap::new()),
            node_health: NodeHealth::default(),
            server_failure_timeout: RwLock::new(Some(DEFAULT_SERVER_FAILURE_TIMEOUT)),
            max_retries: RwLock::new(DEFAULT_MAX_RETRIES),
            clusters: RwLock::new(vec![(
                HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                vec![],
            )]),
            cluster_generation: watch::channel(0).0,
            switch_back_check_interval: RwLock::new(Some(DEFAULT_SWITCH_BACK_CHECK_INTERVAL)),
            cluster_overrides: RwLock::new(HashMap::new()),
        });

        assert_eq!(client.max_retries(), DEFAULT_MAX_RETRIES);
        assert_eq!(
            client.server_failure_timeout(),
            Some(DEFAULT_SERVER_FAILURE_TIMEOUT)
        );

        client.set_max_retries(7);
        client.set_server_failure_timeout(None);

        assert_eq!(client.max_retries(), 7);
        assert_eq!(client.server_failure_timeout(), None);
    }

    fn client_for_cluster_tests(seed_addr: SocketAddr) -> HotRodClient {
        HotRodClient::from_inner(ClientInner {
            active: RwLock::new(ActiveCluster {
                name: HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                seed_addrs: vec![seed_addr],
                active_seed_addr: seed_addr,
                topology: Some(Arc::new(ClusterTopology {
                    topology_id: 1,
                    servers: vec![],
                    hash_function_version: 0,
                    segment_owners: vec![],
                    resolved_addrs: RwLock::new(HashMap::new()),
                })),
            }),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_secs(5)),
            cache_stats: RwLock::new(HashMap::new()),
            node_health: NodeHealth::default(),
            server_failure_timeout: RwLock::new(Some(DEFAULT_SERVER_FAILURE_TIMEOUT)),
            max_retries: RwLock::new(DEFAULT_MAX_RETRIES),
            clusters: RwLock::new(vec![(
                HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                vec![seed_addr],
            )]),
            cluster_generation: watch::channel(0).0,
            switch_back_check_interval: RwLock::new(Some(DEFAULT_SWITCH_BACK_CHECK_INTERVAL)),
            cluster_overrides: RwLock::new(HashMap::new()),
        })
    }

    #[test]
    fn add_cluster_rejects_an_empty_seed_list() {
        let client = client_for_cluster_tests("127.0.0.1:1".parse().unwrap());
        let err = client
            .add_cluster("dr", vec![])
            .expect_err("expected InvalidClusterConfig");
        assert!(matches!(err, Error::InvalidClusterConfig(_)));
    }

    #[test]
    fn add_cluster_rejects_a_name_already_in_use() {
        let client = client_for_cluster_tests("127.0.0.1:1".parse().unwrap());
        client
            .add_cluster("dr", vec!["127.0.0.1:2".parse().unwrap()])
            .expect("first add_cluster should succeed");

        let err = client
            .add_cluster("dr", vec!["127.0.0.1:3".parse().unwrap()])
            .expect_err("expected InvalidClusterConfig for a duplicate name");
        assert!(matches!(err, Error::InvalidClusterConfig(_)));

        let err = client
            .add_cluster(
                HotRodClient::DEFAULT_CLUSTER_NAME,
                vec!["127.0.0.1:3".parse().unwrap()],
            )
            .expect_err("expected InvalidClusterConfig for the sentinel name");
        assert!(matches!(err, Error::InvalidClusterConfig(_)));
    }

    #[test]
    fn switch_to_cluster_errors_for_an_unknown_name() {
        let client = client_for_cluster_tests("127.0.0.1:1".parse().unwrap());
        let err = client
            .switch_to_cluster("does-not-exist")
            .expect_err("expected UnknownCluster");
        assert!(matches!(err, Error::UnknownCluster(name) if name == "does-not-exist"));
    }

    #[test]
    fn switch_to_cluster_updates_seed_addrs_active_seed_and_resets_topology() {
        let original_seed: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let dr_seed: SocketAddr = "127.0.0.1:2".parse().unwrap();
        let client = client_for_cluster_tests(original_seed);
        client
            .add_cluster("dr", vec![dr_seed])
            .expect("add_cluster should succeed");
        assert!(
            client.inner().active.read().unwrap().topology.is_some(),
            "test setup should start with a known topology"
        );

        client
            .switch_to_cluster("dr")
            .expect("switch_to_cluster should succeed");

        assert_eq!(client.active_cluster_name(), "dr");
        {
            let active = client.inner().active.read().unwrap();
            assert_eq!(active.active_seed_addr, dr_seed);
            assert_eq!(active.seed_addrs, vec![dr_seed]);
            assert!(
                active.topology.is_none(),
                "switching clusters should discard the old cluster's topology"
            );
        }

        // Switching back to the original cluster works the same way,
        // since it is just another entry in `clusters` under the
        // sentinel name.
        client
            .switch_to_cluster(HotRodClient::DEFAULT_CLUSTER_NAME)
            .expect("switch_to_cluster back to the default should succeed");
        assert_eq!(
            client.active_cluster_name(),
            HotRodClient::DEFAULT_CLUSTER_NAME
        );
        assert_eq!(
            client.inner().active.read().unwrap().active_seed_addr,
            original_seed
        );
    }

    /// `docs/adr/0019-multicluster-listener-and-pool-cleanup.md`: a
    /// switch must bump `cluster_generation` (so a registered
    /// `CacheListener` notices) and drain every pool the old cluster
    /// owned, including its own seed, which `record_topology_update`'s
    /// own cleanup deliberately never touches (there, it is still the
    /// active seed; here, by the time this runs, it no longer is).
    #[tokio::test]
    async fn switch_to_cluster_bumps_generation_and_drains_the_old_cluster_s_pools() {
        let original_seed: SocketAddr = unreachable_addr().await;
        let dr_seed: SocketAddr = unreachable_addr().await;
        let client = client_for_cluster_tests(original_seed);
        client
            .add_cluster("dr", vec![dr_seed])
            .expect("add_cluster should succeed");

        // Populate a pool for the original seed, the same way a real
        // operation would have, so there is something for the switch
        // to drain.
        client.inner().pools.write().unwrap().insert(
            (original_seed, "my-cache".to_string()),
            Arc::new(ConnectionPool::new(1)),
        );

        let (_, generation_rx) = client.active_seed_addr_and_generation();
        assert!(
            !generation_rx.has_changed().unwrap_or(false),
            "no switch has happened yet"
        );

        client
            .switch_to_cluster("dr")
            .expect("switch_to_cluster should succeed");

        assert!(
            generation_rx.has_changed().unwrap(),
            "switch_to_cluster must bump cluster_generation"
        );
        assert!(
            client.pool_statistics().is_empty(),
            "the old cluster's pool must be drained on switch"
        );

        // A combined read taken after the switch has fully completed
        // must see the new seed paired with the already-bumped
        // generation, never the new seed with a stale baseline (which
        // would make a listener registering right now miss this
        // switch): review before merge found `listen_with` doing these
        // two reads separately, with the dial in between, reopening
        // exactly this race.
        let (seed_after, generation_rx_after) = client.active_seed_addr_and_generation();
        assert_eq!(seed_after, dr_seed);
        assert!(
            !generation_rx_after.has_changed().unwrap_or(false),
            "a subscription taken after the switch already completed must not see it as a pending change"
        );
    }

    /// `docs/adr/0020-multicluster-proactive-switch-back.md`: found by
    /// review before merge. `try_promote_seed`'s `expected_generation`
    /// guard must abort without committing if `cluster_generation` no
    /// longer matches by the time the dial succeeds, closing the
    /// window between `run_switch_back_monitor`'s own pre-dial check
    /// and the commit, which the dial's own latency can make
    /// arbitrarily wide. Exercised directly and deterministically
    /// here, rather than by racing a real concurrent switch against a
    /// real dial's latency, which a slow CI runner could make flake
    /// in either direction.
    #[tokio::test]
    async fn try_promote_seed_aborts_without_committing_when_superseded_before_the_commit() {
        let original_seed: SocketAddr = unreachable_addr().await;
        let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = target_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = client_for_cluster_tests(original_seed);
        client
            .add_cluster("target", vec![target_addr])
            .expect("add_cluster should succeed");

        let stale_generation = *client.inner().cluster_generation.subscribe().borrow();
        // Simulate a switch landing after this generation was
        // captured, before the dial below even starts.
        client
            .inner()
            .cluster_generation
            .send_modify(|generation| *generation += 1);

        let result = client
            .try_promote_seed(
                "",
                &[target_addr],
                None,
                Some("target"),
                Some(stale_generation),
            )
            .await;

        assert!(
            result.is_err(),
            "a superseded attempt must not report success"
        );
        assert_eq!(
            client.active_cluster_name(),
            HotRodClient::DEFAULT_CLUSTER_NAME,
            "a superseded attempt must never commit the switch it was attempting"
        );
    }

    /// `run_switch_back_monitor` itself (not just `try_promote_seed`'s
    /// own guard) must give up once superseded, without ever having to
    /// successfully dial its target: exercised directly here, with a
    /// target address that is never bound to anything for the whole
    /// test, rather than through a real automatic failover and a real
    /// recovered listener. `docs/adr/0020-multicluster-proactive-switch-back.md`
    /// explains why: a real "down, then back up on the same port"
    /// integration test is kept to a single, necessary case elsewhere
    /// (`remote_cache::tests::automatic_failover_proactively_switches_back_once_the_original_cluster_recovers`);
    /// this property does not need one, since it never requires the
    /// dial to succeed at all.
    #[tokio::test]
    async fn run_switch_back_monitor_gives_up_once_superseded_while_asleep() {
        let client = client_for_cluster_tests(unreachable_addr().await);
        client.set_switch_back_check_interval(Some(Duration::from_millis(20)));
        let started_generation = *client.inner().cluster_generation.subscribe().borrow();
        let weak = Arc::downgrade(&client.0);
        let never_bound_target = unreachable_addr().await;

        let monitor = tokio::spawn(run_switch_back_monitor(
            weak,
            "ghost-cluster".to_string(),
            vec![never_bound_target],
            started_generation,
        ));
        // Give the monitor time to start its first sleep before
        // superseding it, so this exercises the post-sleep check
        // (`run_switch_back_monitor`'s own docs), not just the
        // pre-sleep one.
        tokio::time::sleep(Duration::from_millis(2)).await;
        client
            .inner()
            .cluster_generation
            .send_modify(|generation| *generation += 1);

        tokio::time::timeout(Duration::from_millis(500), monitor)
            .await
            .expect("the monitor should give up once superseded, not hang")
            .expect("the monitor task should not panic");
    }

    /// `set_switch_back_check_interval(None)` must stop a monitor
    /// already asleep, not just prevent a future one from starting:
    /// see `run_switch_back_monitor`'s own docs for the gap this
    /// closes (checking again right after waking, not only before
    /// sleeping). Exercised directly, the same reason and with the
    /// same safety as the test just above.
    #[tokio::test]
    async fn run_switch_back_monitor_stops_once_disabled_while_asleep() {
        let client = client_for_cluster_tests(unreachable_addr().await);
        client.set_switch_back_check_interval(Some(Duration::from_millis(20)));
        let started_generation = *client.inner().cluster_generation.subscribe().borrow();
        let weak = Arc::downgrade(&client.0);
        let never_bound_target = unreachable_addr().await;

        let monitor = tokio::spawn(run_switch_back_monitor(
            weak,
            "ghost-cluster".to_string(),
            vec![never_bound_target],
            started_generation,
        ));
        tokio::time::sleep(Duration::from_millis(2)).await;
        client.set_switch_back_check_interval(None);

        tokio::time::timeout(Duration::from_millis(500), monitor)
            .await
            .expect("a disabled monitor should stop at its next wake, not hang")
            .expect("the monitor task should not panic");
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

        assert_eq!(client.0.active.read().unwrap().active_seed_addr, addr);
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

        client.0.active.write().unwrap().topology = Some(Arc::new(ClusterTopology {
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
        let topology = client.0.active.read().unwrap().topology.clone().unwrap();
        let mut broken_servers = topology.servers.clone();
        broken_servers[0].host = "this-hostname-does-not-resolve.invalid".to_string();
        client.0.active.write().unwrap().topology = Some(Arc::new(ClusterTopology {
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

        client.0.active.write().unwrap().topology = Some(Arc::new(ClusterTopology {
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
    async fn nodes_and_owned_segments_returns_just_the_seed_without_a_topology() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = HotRodClient::connect(&[addr])
            .await
            .expect("connect to seed");

        let targets = client
            .nodes_and_owned_segments()
            .await
            .expect("nodes_and_owned_segments");
        assert_eq!(targets, vec![(addr, None, Vec::new())]);
    }

    /// Regression test for a review finding: a segment with no owner
    /// listed at all (not something `read_topology_update` rejects) must
    /// not be silently dropped from the fan-out plan, which would make
    /// `cache.iter()` quietly skip whatever it holds. `owner_addr` falls
    /// back to the seed for the identical condition on a single key;
    /// `nodes_and_owned_segments` must do the same for a whole segment.
    #[tokio::test]
    async fn nodes_and_owned_segments_falls_back_to_the_seed_for_an_unowned_segment() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = HotRodClient::connect(&[addr])
            .await
            .expect("connect to seed");

        client.0.active.write().unwrap().topology = Some(Arc::new(ClusterTopology {
            topology_id: 9,
            servers: vec![TopologyServer {
                host: addr.ip().to_string(),
                port: addr.port(),
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0], vec![]],
            resolved_addrs: RwLock::new(HashMap::new()),
        }));

        let targets = client
            .nodes_and_owned_segments()
            .await
            .expect("nodes_and_owned_segments");

        let seed_target = targets
            .iter()
            .find(|(target_addr, origin, _)| *target_addr == addr && origin.is_none());
        assert!(
            seed_target.is_some_and(|(_, _, segments)| segments.contains(&1)),
            "segment 1 has no owner, so it must be covered by a seed fallback target: got {targets:?}"
        );
    }

    #[tokio::test]
    async fn nodes_and_owned_segments_groups_segments_by_primary_owner() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = HotRodClient::connect(&[addr])
            .await
            .expect("connect to seed");

        let other_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let other_addr = other_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = other_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let servers = vec![
            TopologyServer {
                host: addr.ip().to_string(),
                port: addr.port(),
            },
            TopologyServer {
                host: other_addr.ip().to_string(),
                port: other_addr.port(),
            },
        ];
        // Segments 0 and 2 primary-owned by server 0, segment 1 by
        // server 1.
        client.0.active.write().unwrap().topology = Some(Arc::new(ClusterTopology {
            topology_id: 9,
            servers,
            hash_function_version: 3,
            segment_owners: vec![vec![0], vec![1], vec![0]],
            resolved_addrs: RwLock::new(HashMap::new()),
        }));

        let mut targets = client
            .nodes_and_owned_segments()
            .await
            .expect("nodes_and_owned_segments");
        targets.sort_by_key(|(addr, _, _)| *addr);

        let mut expected = vec![(addr, vec![0, 2]), (other_addr, vec![1])];
        expected.sort_by_key(|(addr, _)| *addr);

        for ((got_addr, _origin, mut got_segments), (expected_addr, expected_segments)) in
            targets.into_iter().zip(expected)
        {
            got_segments.sort_unstable();
            assert_eq!(got_addr, expected_addr);
            assert_eq!(got_segments, expected_segments);
        }
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
            active: RwLock::new(ActiveCluster {
                name: HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                seed_addrs: vec![seed_addr],
                active_seed_addr: seed_addr,
                topology: None,
            }),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_secs(5)),
            cache_stats: RwLock::new(HashMap::new()),
            node_health: NodeHealth::default(),
            server_failure_timeout: RwLock::new(Some(DEFAULT_SERVER_FAILURE_TIMEOUT)),
            max_retries: RwLock::new(DEFAULT_MAX_RETRIES),
            clusters: RwLock::new(vec![(
                HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                vec![seed_addr],
            )]),
            cluster_generation: watch::channel(0).0,
            switch_back_check_interval: RwLock::new(Some(DEFAULT_SWITCH_BACK_CHECK_INTERVAL)),
            cluster_overrides: RwLock::new(HashMap::new()),
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
            active: RwLock::new(ActiveCluster {
                name: HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                seed_addrs: vec![seed_addr],
                active_seed_addr: seed_addr,
                topology: None,
            }),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_secs(5)),
            cache_stats: RwLock::new(HashMap::new()),
            node_health: NodeHealth::default(),
            server_failure_timeout: RwLock::new(Some(DEFAULT_SERVER_FAILURE_TIMEOUT)),
            max_retries: RwLock::new(DEFAULT_MAX_RETRIES),
            clusters: RwLock::new(vec![(
                HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                vec![seed_addr],
            )]),
            cluster_generation: watch::channel(0).0,
            switch_back_check_interval: RwLock::new(Some(DEFAULT_SWITCH_BACK_CHECK_INTERVAL)),
            cluster_overrides: RwLock::new(HashMap::new()),
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

    /// `docs/adr/0021-multicluster-per-cluster-auth-and-tls.md`: a
    /// cluster with its own override uses it for the field(s) it set,
    /// and still falls back to this client's global `auth`/`tls` for
    /// whichever field (or both) it left unset, including a cluster
    /// never mentioned in `cluster_overrides` at all.
    #[test]
    fn effective_auth_and_tls_falls_back_to_global_unless_a_cluster_has_its_own_override() {
        let global_auth = AuthMethod::Plain {
            authzid: String::new(),
            authcid: "global-user".to_string(),
            password: "global-pass".to_string(),
        };
        let global_tls = TlsConfig {
            server_name: "global.example".to_string(),
            ..TlsConfig::default()
        };

        let mut overrides = HashMap::new();
        overrides.insert(
            "auth-only".to_string(),
            ClusterOverride {
                auth: Some(AuthMethod::Plain {
                    authzid: String::new(),
                    authcid: "dr-user".to_string(),
                    password: "dr-pass".to_string(),
                }),
                tls: None,
            },
        );
        overrides.insert(
            "tls-only".to_string(),
            ClusterOverride {
                auth: None,
                tls: Some(TlsConfig {
                    server_name: "dr.example".to_string(),
                    ..TlsConfig::default()
                }),
            },
        );

        let client = HotRodClient::from_inner(ClientInner {
            active: RwLock::new(ActiveCluster {
                name: HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                seed_addrs: vec!["127.0.0.1:1".parse().unwrap()],
                active_seed_addr: "127.0.0.1:1".parse().unwrap(),
                topology: None,
            }),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(Some(global_auth)),
            tls: Some(global_tls),
            timeout: RwLock::new(Duration::from_secs(5)),
            cache_stats: RwLock::new(HashMap::new()),
            node_health: NodeHealth::default(),
            server_failure_timeout: RwLock::new(Some(DEFAULT_SERVER_FAILURE_TIMEOUT)),
            max_retries: RwLock::new(DEFAULT_MAX_RETRIES),
            clusters: RwLock::new(vec![(
                HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                vec!["127.0.0.1:1".parse().unwrap()],
            )]),
            cluster_generation: watch::channel(0).0,
            switch_back_check_interval: RwLock::new(Some(DEFAULT_SWITCH_BACK_CHECK_INTERVAL)),
            cluster_overrides: RwLock::new(overrides),
        });

        let (auth, tls) = client.effective_auth_and_tls("auth-only");
        assert!(matches!(
            auth,
            Some(AuthMethod::Plain { ref authcid, .. }) if authcid == "dr-user"
        ));
        assert_eq!(tls.unwrap().server_name, "global.example");

        let (auth, tls) = client.effective_auth_and_tls("tls-only");
        assert!(matches!(
            auth,
            Some(AuthMethod::Plain { ref authcid, .. }) if authcid == "global-user"
        ));
        assert_eq!(tls.unwrap().server_name, "dr.example");

        let (auth, tls) = client.effective_auth_and_tls(HotRodClient::DEFAULT_CLUSTER_NAME);
        assert!(matches!(
            auth,
            Some(AuthMethod::Plain { ref authcid, .. }) if authcid == "global-user"
        ));
        assert_eq!(tls.unwrap().server_name, "global.example");
    }

    /// `authenticate_plain_for_cluster` and `set_cluster_tls` validate
    /// `name` the same way `switch_to_cluster` does, before anything
    /// dials: no network involvement needed to confirm this.
    #[tokio::test]
    async fn authenticate_for_cluster_and_set_cluster_tls_reject_an_unknown_cluster() {
        let seed_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let client = HotRodClient::from_inner(ClientInner {
            active: RwLock::new(ActiveCluster {
                name: HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                seed_addrs: vec![seed_addr],
                active_seed_addr: seed_addr,
                topology: None,
            }),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_secs(5)),
            cache_stats: RwLock::new(HashMap::new()),
            node_health: NodeHealth::default(),
            server_failure_timeout: RwLock::new(Some(DEFAULT_SERVER_FAILURE_TIMEOUT)),
            max_retries: RwLock::new(DEFAULT_MAX_RETRIES),
            clusters: RwLock::new(vec![(
                HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                vec![seed_addr],
            )]),
            cluster_generation: watch::channel(0).0,
            switch_back_check_interval: RwLock::new(Some(DEFAULT_SWITCH_BACK_CHECK_INTERVAL)),
            cluster_overrides: RwLock::new(HashMap::new()),
        });

        assert!(matches!(
            client
                .authenticate_plain_for_cluster("missing", "", "user", "pass")
                .await,
            Err(Error::UnknownCluster(name)) if name == "missing"
        ));
        assert!(matches!(
            client.set_cluster_tls("missing", None),
            Err(Error::UnknownCluster(name)) if name == "missing"
        ));
    }

    /// End-to-end confirmation (`docs/adr/0021-multicluster-per-cluster-auth-and-tls.md`):
    /// a cluster with its own auth override uses those credentials over
    /// the wire, not the global ones, both for `authenticate_plain_for_cluster`'s
    /// own validation dial and for the real connection a later operation
    /// opens once switched to it.
    #[tokio::test]
    async fn authenticate_plain_for_cluster_is_used_once_switched_to_that_cluster() {
        async fn serve_ping(stream: &mut TcpStream) {
            use tokio::io::AsyncWriteExt;
            let (id, opcode) = read_request_opcode(stream).await;
            assert_eq!(opcode, 0x17, "expected a Ping request");
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            write_vint(&mut resp, 0);
            stream.write_all(&resp).await.unwrap();
        }

        let default_response = b"\0global-user\0global-pass".to_vec();
        let dr_response = b"\0dr-user\0dr-pass".to_vec();

        let default_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let default_addr = default_listener.local_addr().unwrap();
        let dr_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dr_addr = dr_listener.local_addr().unwrap();

        let expected_default = default_response.clone();
        let default_task = tokio::spawn(async move {
            // Only `authenticate_plain`'s own validation dial: the
            // default cluster is never switched to or operated on in
            // this test.
            let (mut probe, _) = default_listener.accept().await.unwrap();
            serve_plain_auth(&mut probe, &expected_default).await;
        });

        let expected_dr = dr_response.clone();
        let dr_task = tokio::spawn(async move {
            // `authenticate_plain_for_cluster`'s own validation dial...
            let (mut probe, _) = dr_listener.accept().await.unwrap();
            serve_plain_auth(&mut probe, &expected_dr).await;

            // ...then the real connection `ping` opens once this client
            // has switched to "dr", which must replay the same
            // "dr"-specific credentials, not the global ones.
            let (mut real, _) = dr_listener.accept().await.unwrap();
            serve_plain_auth(&mut real, &expected_dr).await;
            serve_ping(&mut real).await;
        });

        let client = HotRodClient::from_inner(ClientInner {
            active: RwLock::new(ActiveCluster {
                name: HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                seed_addrs: vec![default_addr],
                active_seed_addr: default_addr,
                topology: None,
            }),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_secs(5)),
            cache_stats: RwLock::new(HashMap::new()),
            node_health: NodeHealth::default(),
            server_failure_timeout: RwLock::new(Some(DEFAULT_SERVER_FAILURE_TIMEOUT)),
            max_retries: RwLock::new(DEFAULT_MAX_RETRIES),
            clusters: RwLock::new(vec![
                (
                    HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                    vec![default_addr],
                ),
                ("dr".to_string(), vec![dr_addr]),
            ]),
            cluster_generation: watch::channel(0).0,
            switch_back_check_interval: RwLock::new(Some(DEFAULT_SWITCH_BACK_CHECK_INTERVAL)),
            cluster_overrides: RwLock::new(HashMap::new()),
        });

        client
            .authenticate_plain("", "global-user", "global-pass")
            .await
            .expect("authenticate the global default");
        client
            .authenticate_plain_for_cluster("dr", "", "dr-user", "dr-pass")
            .await
            .expect("authenticate the dr cluster's own override");

        client
            .switch_to_cluster("dr")
            .expect("dr is a configured cluster");
        client
            .cache("my-cache")
            .ping()
            .await
            .expect("ping against dr should replay dr's own credentials");

        default_task.await.unwrap();
        dr_task.await.unwrap();
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
            active: RwLock::new(ActiveCluster {
                name: HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                seed_addrs: vec![dead_addr],
                active_seed_addr: dead_addr,
                topology: None,
            }),
            node_origin: RwLock::new(HashMap::new()),
            pools: RwLock::new(HashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_millis(200)),
            cache_stats: RwLock::new(HashMap::new()),
            node_health: NodeHealth::default(),
            server_failure_timeout: RwLock::new(Some(DEFAULT_SERVER_FAILURE_TIMEOUT)),
            max_retries: RwLock::new(DEFAULT_MAX_RETRIES),
            clusters: RwLock::new(vec![(
                HotRodClient::DEFAULT_CLUSTER_NAME.to_string(),
                vec![dead_addr],
            )]),
            cluster_generation: watch::channel(0).0,
            switch_back_check_interval: RwLock::new(Some(DEFAULT_SWITCH_BACK_CHECK_INTERVAL)),
            cluster_overrides: RwLock::new(HashMap::new()),
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

    /// `pool_statistics` (#54) has no Java equivalent; this is this
    /// crate's own test of it, not a parity check: one pool, one
    /// connection checked out and never returned, confirming the
    /// idle/checked-out split and the address/cache-name labeling.
    #[tokio::test]
    async fn pool_statistics_reflects_idle_and_checked_out_connections() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = HotRodClient::connect(&[addr])
            .await
            .expect("connect to seed");
        let guard = client
            .checkout(addr, "my-cache", None)
            .await
            .expect("checkout");

        let stats = client.pool_statistics();
        assert_eq!(stats.len(), 1);
        let pool = &stats[0];
        assert_eq!(pool.address, addr);
        assert_eq!(pool.cache_name, "my-cache");
        assert_eq!(pool.checked_out_connections, 1);
        assert_eq!(pool.idle_connections, 0);
        assert!(pool.max_connections >= 1);

        drop(guard);
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
        // Not two bare bytes: most requests declare "none" (a single
        // zero byte each), but the query operation declares a real
        // predefined media type (`docs/adr/0013-remote-query.md`),
        // which is longer. `skip_media_type` already knows the real
        // format (used to stay in sync with a response that carries
        // one); reused here so this helper stays correct for both.
        crate::wire::skip_media_type(stream).await.unwrap();
        crate::wire::skip_media_type(stream).await.unwrap();
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
