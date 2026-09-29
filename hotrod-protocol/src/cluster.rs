//! `HotRodCluster`: a hash-aware routing client bound to one cache, spread
//! across a pool of per-node connections.
//!
//! This is additive alongside `HotRodConnection` (see
//! `docs/adr/0003-hash-aware-routing-scope.md`): `HotRodConnection` remains
//! the single-connection, no-topology client from phase 1, unchanged.
//! `HotRodCluster` instead advertises `HashDistributionAware` intelligence,
//! tracks the topology the server reports, and routes each operation to the
//! segment's primary owner, computed locally with the same `MurmurHash3`
//! variant the server uses.
//!
//! The same cancellation hazard documented on `connection`'s module docs
//! applies to each pooled connection here. `HotRodConnection` now enforces
//! it itself, marking a connection poisoned before every request and
//! clearing that mark only once the response is read in full, so a future
//! dropped externally before it resolves leaves the mark set even though it
//! never returns anything for `call` to inspect. `ensure_connection` checks
//! that mark directly before handing a pooled connection to the next
//! operation, evicting and reconnecting it first if it is poisoned, rather
//! than relying only on the operation's own return value.
//!
//! Every operation takes `&mut self`, so one `HotRodCluster` instance runs
//! its operations one at a time, even when they route to different nodes.
//! This is more restrictive than `HotRodConnection`'s own one-request-at-a-
//! time model, which only serializes per connection. A caller that wants
//! operations against different nodes to run concurrently needs multiple
//! `HotRodCluster` instances, each bound to the same seeds, rather than one
//! instance shared behind a lock.

use std::collections::hash_map;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::lookup_host;
use tokio::task::JoinSet;

use crate::connection::{
    HotRodConnection, VersionedResult, VersionedValue, DEFAULT_TIMEOUT, DEFAULT_TOPOLOGY_ID,
};
use crate::error::{Error, Result};
use crate::hash;
use crate::topology::TopologyServer;
use crate::wire::Expiration;

/// Credentials for one SASL mechanism, kept so a connection opened later to
/// a newly discovered node can be authenticated the same way as the seed
/// connection, without the caller authenticating it by hand.
enum AuthMethod {
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

/// The routing data from the most recently applied topology update.
struct ClusterTopology {
    servers: Vec<TopologyServer>,
    hash_function_version: u8,
    /// Index `n` is segment `n`'s owners, as indices into `servers`, primary
    /// owner first.
    segment_owners: Vec<Vec<u32>>,
    /// Addresses already resolved for this topology, keyed by index into
    /// `servers`. A new topology update replaces this whole struct, so the
    /// cache is invalidated for free whenever the servers it was built from
    /// change.
    resolved_addrs: HashMap<u32, SocketAddr>,
}

/// A pooled connection together with the topology server it was opened
/// for. `origin` is `None` for the initial seed connection, opened before
/// any topology update has arrived, and `Some` for a connection opened to
/// a computed owner. `record_topology_update` compares `origin` against
/// each new update's server list to evict a connection whose node has
/// left the cluster, without re-resolving any hostname to do it.
struct PooledConnection {
    origin: Option<TopologyServer>,
    conn: HotRodConnection,
}

/// A cache client that tracks cluster topology and routes each operation to
/// the segment's primary owner instead of relying on server-side
/// redirects, matching the Java client's intelligent routing.
///
/// Bound to one cache, like `HotRodConnection`. A single instance keeps one
/// pooled connection per node it has needed to talk to so far, up to the
/// number of nodes in the latest topology: `record_topology_update` drops
/// any pooled connection to a node that topology no longer lists.
///
/// Every method takes `&mut self`, so one instance runs its operations one
/// at a time regardless of which node each is routed to: sharing a single
/// instance across concurrent tasks serializes them all. For operations
/// against different nodes to run concurrently, use one `HotRodCluster`
/// instance per task instead of sharing one.
pub struct HotRodCluster {
    cache_name: String,
    /// Every seed address this instance was constructed with, in the order
    /// given to `connect`/`connect_with_timeout`. `active_seed_addr` is
    /// always one of these; `failover_seed` tries the rest when it stops
    /// responding.
    seed_addrs: Vec<SocketAddr>,
    /// The seed this instance is currently connected to; also the fallback
    /// used before a topology has arrived and the retry target when a
    /// computed owner's connection fails. Never evicted by a topology
    /// update, even if this address stops being listed: it is the last
    /// resort every retry falls back to. Can change at runtime: see
    /// `failover_seed`.
    active_seed_addr: SocketAddr,
    topology_id: i32,
    topology: Option<ClusterTopology>,
    connections: HashMap<SocketAddr, PooledConnection>,
    auth: Option<AuthMethod>,
    timeout: Duration,
}

/// One of `HotRodConnection`'s cache operations, with its arguments owned
/// so it can be replayed against a second connection on retry. `call`
/// dispatches on this instead of taking a generic closure: a closure
/// capturing owned data cannot satisfy the higher-ranked `for<'c> FnMut(&'c
/// mut HotRodConnection) -> _` bound that returning a future borrowed from
/// `&'c mut HotRodConnection` would need, since a closure body has a single
/// fixed lifetime, not one universally quantified per call.
enum Operation {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>, Expiration, Expiration),
    PutIfAbsent(Vec<u8>, Vec<u8>, Expiration, Expiration),
    Replace(Vec<u8>, Vec<u8>, Expiration, Expiration),
    Remove(Vec<u8>),
    GetWithVersion(Vec<u8>),
    ReplaceIfUnmodified(Vec<u8>, Vec<u8>, u64, Expiration, Expiration),
    RemoveIfUnmodified(Vec<u8>, u64),
    ContainsKey(Vec<u8>),
    Ping,
    Size,
    Clear,
    Stats,
    GetAll(Vec<Vec<u8>>),
    PutAll(Vec<(Vec<u8>, Vec<u8>)>, Expiration, Expiration),
}

enum OperationResult {
    Get(Option<Vec<u8>>),
    Put,
    Bool(bool),
    GetWithVersion(Option<VersionedValue>),
    Versioned(VersionedResult),
    Ping,
    Size(u32),
    Clear,
    Stats(HashMap<String, String>),
    GetAll(HashMap<Vec<u8>, Vec<u8>>),
    PutAll,
}

async fn run_operation(conn: &mut HotRodConnection, op: &Operation) -> Result<OperationResult> {
    match op {
        Operation::Get(key) => Ok(OperationResult::Get(conn.get(key).await?)),
        Operation::Put(key, value, lifespan, max_idle) => {
            conn.put(key, value, *lifespan, *max_idle).await?;
            Ok(OperationResult::Put)
        }
        Operation::PutIfAbsent(key, value, lifespan, max_idle) => Ok(OperationResult::Bool(
            conn.put_if_absent(key, value, *lifespan, *max_idle).await?,
        )),
        Operation::Replace(key, value, lifespan, max_idle) => Ok(OperationResult::Bool(
            conn.replace(key, value, *lifespan, *max_idle).await?,
        )),
        Operation::Remove(key) => Ok(OperationResult::Bool(conn.remove(key).await?)),
        Operation::GetWithVersion(key) => Ok(OperationResult::GetWithVersion(
            conn.get_with_version(key).await?,
        )),
        Operation::ReplaceIfUnmodified(key, value, version, lifespan, max_idle) => {
            Ok(OperationResult::Versioned(
                conn.replace_if_unmodified(key, value, *version, *lifespan, *max_idle)
                    .await?,
            ))
        }
        Operation::RemoveIfUnmodified(key, version) => Ok(OperationResult::Versioned(
            conn.remove_if_unmodified(key, *version).await?,
        )),
        Operation::ContainsKey(key) => Ok(OperationResult::Bool(conn.contains_key(key).await?)),
        Operation::Ping => {
            conn.ping().await?;
            Ok(OperationResult::Ping)
        }
        Operation::Size => Ok(OperationResult::Size(conn.size().await?)),
        Operation::Clear => {
            conn.clear().await?;
            Ok(OperationResult::Clear)
        }
        Operation::Stats => Ok(OperationResult::Stats(conn.stats().await?)),
        Operation::GetAll(keys) => Ok(OperationResult::GetAll(
            conn.get_all(keys.iter().map(|key| key.as_slice())).await?,
        )),
        Operation::PutAll(entries, lifespan, max_idle) => {
            conn.put_all(
                entries
                    .iter()
                    .map(|(key, value)| (key.as_slice(), value.as_slice())),
                *lifespan,
                *max_idle,
            )
            .await?;
            Ok(OperationResult::PutAll)
        }
    }
}

impl HotRodCluster {
    /// Connects to the first seed address that accepts a connection, in
    /// order, matching the Java client's failover-on-connect behavior. No
    /// topology is known yet at this point: every operation routes to this
    /// seed until the server's first response carries one. Uses
    /// `DEFAULT_TIMEOUT`; call `connect_with_timeout` for a different bound.
    pub async fn connect(seed_addrs: &[SocketAddr], cache_name: &str) -> Result<Self> {
        Self::connect_with_timeout(seed_addrs, cache_name, DEFAULT_TIMEOUT).await
    }

    /// Same as `connect`, but with a caller-supplied timeout in place of
    /// `DEFAULT_TIMEOUT`, applied to every connection this instance opens,
    /// now or later when routing discovers a new node.
    ///
    /// Every seed is dialed concurrently rather than one after another, so
    /// the whole call is bounded by `timeout` regardless of how many seeds
    /// are given: an unreachable seed no longer adds its own `timeout` to
    /// the total. The first seed to accept a connection wins; the others
    /// are dropped mid-connect. If every seed fails, the reported error is
    /// the one from the seed listed first in `seed_addrs`, not whichever
    /// happened to finish last.
    pub async fn connect_with_timeout(
        seed_addrs: &[SocketAddr],
        cache_name: &str,
        timeout: Duration,
    ) -> Result<Self> {
        let mut attempts = JoinSet::new();
        for (index, &addr) in seed_addrs.iter().enumerate() {
            let cache_name = cache_name.to_string();
            attempts.spawn(async move {
                let result = HotRodConnection::connect_hash_aware(
                    addr,
                    &cache_name,
                    DEFAULT_TOPOLOGY_ID,
                    timeout,
                )
                .await;
                (index, addr, result)
            });
        }

        let mut errors: Vec<Option<Error>> = seed_addrs.iter().map(|_| None).collect();
        let mut join_failure: Option<Error> = None;
        while let Some(joined) = attempts.join_next().await {
            match joined {
                Ok((_index, addr, Ok(conn))) => {
                    let mut connections = HashMap::new();
                    connections.insert(addr, PooledConnection { origin: None, conn });
                    return Ok(Self {
                        cache_name: cache_name.to_string(),
                        seed_addrs: seed_addrs.to_vec(),
                        active_seed_addr: addr,
                        topology_id: DEFAULT_TOPOLOGY_ID,
                        topology: None,
                        connections,
                        auth: None,
                        timeout,
                    });
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

    /// The timeout currently bounding every operation on this instance's
    /// pooled connections.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Overrides the timeout used from this call onward, in place of the
    /// one given at connect time. Applies immediately to every already
    /// pooled connection and to every one opened later. Applies to every
    /// subsequent operation, not just the next one: a caller that wants a
    /// single call to have a different bound must set it back afterward,
    /// typically to the value `timeout()` returned beforehand.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
        for pooled in self.connections.values_mut() {
            pooled.conn.set_timeout(timeout);
        }
    }

    /// Authenticates using SASL PLAIN. See `HotRodConnection::authenticate_plain`.
    pub async fn authenticate_plain(
        &mut self,
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

    /// Authenticates using SASL SCRAM-SHA-512. See `HotRodConnection::authenticate_scram`.
    pub async fn authenticate_scram(&mut self, authcid: &str, password: &str) -> Result<()> {
        self.authenticate_with(AuthMethod::Scram {
            authcid: authcid.to_string(),
            password: password.to_string(),
        })
        .await
    }

    /// Authenticates using SASL DIGEST-SHA-256. See `HotRodConnection::authenticate_digest`.
    pub async fn authenticate_digest(&mut self, authcid: &str, password: &str) -> Result<()> {
        self.authenticate_with(AuthMethod::Digest {
            authcid: authcid.to_string(),
            password: password.to_string(),
        })
        .await
    }

    /// Authenticates using SASL OAUTHBEARER. See `HotRodConnection::authenticate_oauthbearer`.
    pub async fn authenticate_oauthbearer(&mut self, authzid: &str, token: &str) -> Result<()> {
        self.authenticate_with(AuthMethod::OAuthBearer {
            authzid: authzid.to_string(),
            token: token.to_string(),
        })
        .await
    }

    /// Authenticates the seed connection, reconnecting it first if an
    /// earlier operation failure evicted it from the pool: the seed is not
    /// guaranteed to still be pooled by the time this runs, since
    /// `call`/`call_seed` evict it on `Error::Io`/`Error::Timeout`, and a
    /// caller can re-authenticate (a token refresh, or a retry after an
    /// earlier `authenticate_*` failure) at any point afterward.
    async fn authenticate_with(&mut self, method: AuthMethod) -> Result<()> {
        let seed = self.active_seed_addr;
        let conn = &mut self.ensure_connection(seed, None).await?.conn;
        method.authenticate(conn).await?;
        self.auth = Some(method);
        Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.call(key, Operation::Get(key.to_vec())).await? {
            OperationResult::Get(value) => Ok(value),
            _ => unreachable!("Operation::Get always yields OperationResult::Get"),
        }
    }

    pub async fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        let op = Operation::Put(key.to_vec(), value.to_vec(), lifespan, max_idle);
        match self.call(key, op).await? {
            OperationResult::Put => Ok(()),
            _ => unreachable!("Operation::Put always yields OperationResult::Put"),
        }
    }

    pub async fn put_if_absent(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<bool> {
        let op = Operation::PutIfAbsent(key.to_vec(), value.to_vec(), lifespan, max_idle);
        match self.call(key, op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => unreachable!("Operation::PutIfAbsent always yields OperationResult::Bool"),
        }
    }

    pub async fn replace(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<bool> {
        let op = Operation::Replace(key.to_vec(), value.to_vec(), lifespan, max_idle);
        match self.call(key, op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => unreachable!("Operation::Replace always yields OperationResult::Bool"),
        }
    }

    pub async fn remove(&mut self, key: &[u8]) -> Result<bool> {
        match self.call(key, Operation::Remove(key.to_vec())).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => unreachable!("Operation::Remove always yields OperationResult::Bool"),
        }
    }

    pub async fn get_with_version(&mut self, key: &[u8]) -> Result<Option<VersionedValue>> {
        let op = Operation::GetWithVersion(key.to_vec());
        match self.call(key, op).await? {
            OperationResult::GetWithVersion(value) => Ok(value),
            _ => unreachable!(
                "Operation::GetWithVersion always yields OperationResult::GetWithVersion"
            ),
        }
    }

    pub async fn replace_if_unmodified(
        &mut self,
        key: &[u8],
        value: &[u8],
        version: u64,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<VersionedResult> {
        let op = Operation::ReplaceIfUnmodified(
            key.to_vec(),
            value.to_vec(),
            version,
            lifespan,
            max_idle,
        );
        match self.call(key, op).await? {
            OperationResult::Versioned(value) => Ok(value),
            _ => unreachable!(
                "Operation::ReplaceIfUnmodified always yields OperationResult::Versioned"
            ),
        }
    }

    pub async fn remove_if_unmodified(
        &mut self,
        key: &[u8],
        version: u64,
    ) -> Result<VersionedResult> {
        let op = Operation::RemoveIfUnmodified(key.to_vec(), version);
        match self.call(key, op).await? {
            OperationResult::Versioned(value) => Ok(value),
            _ => unreachable!(
                "Operation::RemoveIfUnmodified always yields OperationResult::Versioned"
            ),
        }
    }

    /// Returns `true` if `key` exists in the cache. Routed the same way as
    /// `get`.
    pub async fn contains_key(&mut self, key: &[u8]) -> Result<bool> {
        let op = Operation::ContainsKey(key.to_vec());
        match self.call(key, op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => unreachable!("Operation::ContainsKey always yields OperationResult::Bool"),
        }
    }

    /// Checks the seed connection is reachable and its handshake still
    /// holds. There is no key to route by, so this always targets the seed
    /// rather than a computed owner.
    pub async fn ping(&mut self) -> Result<()> {
        match self.call_seed(Operation::Ping).await? {
            OperationResult::Ping => Ok(()),
            _ => unreachable!("Operation::Ping always yields OperationResult::Ping"),
        }
    }

    /// The number of entries in the cache. The server computes this
    /// cluster-wide from a single request, so which pooled connection it
    /// is sent against (always the seed here) does not change the result.
    pub async fn size(&mut self) -> Result<u32> {
        match self.call_seed(Operation::Size).await? {
            OperationResult::Size(value) => Ok(value),
            _ => unreachable!("Operation::Size always yields OperationResult::Size"),
        }
    }

    /// Removes every entry from the cache, cluster-wide. Like `size`, the
    /// server fans this out itself.
    pub async fn clear(&mut self) -> Result<()> {
        match self.call_seed(Operation::Clear).await? {
            OperationResult::Clear => Ok(()),
            _ => unreachable!("Operation::Clear always yields OperationResult::Clear"),
        }
    }

    /// Statistics from whichever node the seed connection currently
    /// targets. Unlike `size` and `clear`, this is not aggregated across
    /// the cluster by the protocol: it reflects only that one node.
    pub async fn stats(&mut self) -> Result<HashMap<String, String>> {
        match self.call_seed(Operation::Stats).await? {
            OperationResult::Stats(value) => Ok(value),
            _ => unreachable!("Operation::Stats always yields OperationResult::Stats"),
        }
    }

    /// Fetches every key in `keys` that exists, in one request to the seed
    /// connection. There is no key to route by a single owner: the Propose
    /// step for issue #41 chose the same answer already picked for
    /// `size`/`clear`/`ping`/`stats` in issue #40, always the seed, over
    /// splitting the batch client-side by owner.
    pub async fn get_all(
        &mut self,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        let keys: Vec<Vec<u8>> = keys.into_iter().map(|key| key.as_ref().to_vec()).collect();
        match self.call_seed(Operation::GetAll(keys)).await? {
            OperationResult::GetAll(value) => Ok(value),
            _ => unreachable!("Operation::GetAll always yields OperationResult::GetAll"),
        }
    }

    /// Writes every key/value pair in `entries` in one request to the seed
    /// connection. Routed the same way as `get_all`, for the same reason.
    pub async fn put_all(
        &mut self,
        entries: impl IntoIterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = entries
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_vec(), value.as_ref().to_vec()))
            .collect();
        match self
            .call_seed(Operation::PutAll(entries, lifespan, max_idle))
            .await?
        {
            OperationResult::PutAll => Ok(()),
            _ => unreachable!("Operation::PutAll always yields OperationResult::PutAll"),
        }
    }

    /// Routes `key` to its computed owner, running `op` against that
    /// connection (opening and authenticating it first if this is the first
    /// call to reach it). On an I/O error, the failed connection is dropped
    /// from the pool: if the owner was a computed node, `op` is retried once
    /// against the seed connection (itself subject to failover, see
    /// `run_seed_op`); if the owner already was the seed, `op` goes straight
    /// to `failover_and_retry` instead of trying the same dead address
    /// again. Any other error, or a failure with nowhere left to retry, is
    /// returned as is.
    async fn call(&mut self, key: &[u8], op: Operation) -> Result<OperationResult> {
        let (addr, origin) = self.owner_addr(key).await?;
        let outcome = {
            let pooled = self.ensure_connection(addr, origin).await?;
            run_operation(&mut pooled.conn, &op).await
        };
        let err = match outcome {
            Ok(value) => {
                self.record_topology_update(addr);
                return Ok(value);
            }
            Err(err) => err,
        };
        if !matches!(err, Error::Io(_) | Error::Timeout(_)) {
            return Err(err);
        }
        self.connections.remove(&addr);
        if addr == self.active_seed_addr {
            return self.failover_and_retry(&op, err).await;
        }
        self.run_seed_op(&op).await
    }

    /// Runs `op` against the seed connection, for an operation with no key
    /// to route by. Opens or reopens the seed connection first if an
    /// earlier failure evicted it. On `Error::Io` or `Error::Timeout`,
    /// either opening the connection or running `op`, the connection is
    /// dropped from the pool and `failover_and_retry` takes over.
    async fn call_seed(&mut self, op: Operation) -> Result<OperationResult> {
        self.run_seed_op(&op).await
    }

    /// Shared by `call_seed` and `call`'s owner-to-seed retry: runs `op`
    /// against the seed connection, connecting it first if it is not
    /// already pooled. On `Error::Io`/`Error::Timeout` from either step,
    /// the seed connection is evicted and the failure handed to
    /// `failover_and_retry`. Any other error is returned as is.
    async fn run_seed_op(&mut self, op: &Operation) -> Result<OperationResult> {
        let seed = self.active_seed_addr;
        let attempt = match self.ensure_connection(seed, None).await {
            Ok(pooled) => run_operation(&mut pooled.conn, op).await,
            Err(err) => Err(err),
        };
        match attempt {
            Ok(value) => {
                self.record_topology_update(seed);
                Ok(value)
            }
            Err(err) => {
                if !matches!(err, Error::Io(_) | Error::Timeout(_)) {
                    return Err(err);
                }
                self.connections.remove(&seed);
                self.failover_and_retry(op, err).await
            }
        }
    }

    /// Called once the current seed connection has just failed with
    /// `Error::Io`/`Error::Timeout`. Tries every other seed address (see
    /// `failover_seed`) and, if one accepts a connection, retries `op`
    /// against it once, promoting it to `active_seed_addr`. If no other
    /// seed is reachable either, `original_err`, the failure that triggered
    /// this in the first place, is returned rather than whatever
    /// `failover_seed` itself failed with: that is the error the caller's
    /// operation actually hit.
    async fn failover_and_retry(
        &mut self,
        op: &Operation,
        original_err: Error,
    ) -> Result<OperationResult> {
        let Ok(new_seed) = self.failover_seed().await else {
            return Err(original_err);
        };
        let pooled = self
            .connections
            .get_mut(&new_seed)
            .expect("failover_seed leaves the new seed connection pooled");
        match run_operation(&mut pooled.conn, op).await {
            Ok(value) => {
                self.record_topology_update(new_seed);
                Ok(value)
            }
            Err(err) => {
                if matches!(err, Error::Io(_) | Error::Timeout(_)) {
                    self.connections.remove(&new_seed);
                }
                Err(err)
            }
        }
    }

    /// Tries every seed address other than the current `active_seed_addr`,
    /// in the order given to `connect`, until one accepts a connection and,
    /// if credentials were set, authenticates. The first to succeed is
    /// pooled and promoted to `active_seed_addr`. Returns the last error
    /// seen if every other seed also fails to connect or authenticate, or a
    /// generic error if there was no other seed to try in the first place.
    async fn failover_seed(&mut self) -> Result<SocketAddr> {
        let previous = self.active_seed_addr;
        let mut last_err: Option<Error> = None;
        for &addr in &self.seed_addrs {
            if addr == previous {
                continue;
            }
            let mut conn = match HotRodConnection::connect_hash_aware(
                addr,
                &self.cache_name,
                self.topology_id,
                self.timeout,
            )
            .await
            {
                Ok(conn) => conn,
                Err(err) => {
                    last_err = Some(err);
                    continue;
                }
            };
            if let Some(auth) = &self.auth {
                if let Err(err) = auth.authenticate(&mut conn).await {
                    last_err = Some(err);
                    continue;
                }
            }
            self.connections
                .insert(addr, PooledConnection { origin: None, conn });
            self.active_seed_addr = addr;
            return Ok(addr);
        }
        Err(last_err.unwrap_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotConnected,
                "no other seed address available to fail over to",
            ))
        }))
    }

    /// The address to route `key` to, and the topology server it came
    /// from: the segment's primary owner once a topology is known,
    /// otherwise the seed connection with no origin. The origin is what
    /// `record_topology_update` later checks a new update's server list
    /// against, so it must be `None` exactly when the address is
    /// `active_seed_addr`.
    async fn owner_addr(&mut self, key: &[u8]) -> Result<(SocketAddr, Option<TopologyServer>)> {
        let Some(topology) = &self.topology else {
            return Ok((self.active_seed_addr, None));
        };
        if topology.segment_owners.is_empty() {
            return Ok((self.active_seed_addr, None));
        }
        let segment = hash::segment(
            key,
            topology.segment_owners.len() as u32,
            topology.hash_function_version,
        )?;
        let Some(&primary) = topology.segment_owners[segment as usize].first() else {
            return Ok((self.active_seed_addr, None));
        };
        // Safe: `topology::read_topology_update` rejects any owner index
        // that is out of range for `servers` before this type is built.
        let server = topology.servers[primary as usize].clone();
        if let Some(&addr) = topology.resolved_addrs.get(&primary) {
            return Ok((addr, Some(server)));
        }
        let addr = resolve_server_addr(&server).await?;
        if let Some(topology) = self.topology.as_mut() {
            topology.resolved_addrs.insert(primary, addr);
        }
        Ok((addr, Some(server)))
    }

    /// Opens and authenticates a pooled connection to `addr` if one is not
    /// already there, and returns it either way. `origin` is the topology
    /// server `addr` was resolved from, stored alongside a newly opened
    /// connection for `record_topology_update` to check later; pass `None`
    /// for the seed or a retry fallback.
    ///
    /// A connection already pooled at `addr` is evicted and reopened first
    /// if `HotRodConnection::is_poisoned` reports it unsafe to reuse: a
    /// future dropped mid-operation elsewhere in the program leaves it that
    /// way without ever returning an error here for `call` to react to, so
    /// this is checked directly instead of only trusting a prior
    /// operation's return value.
    ///
    /// Returning the connection directly, instead of a caller repeating the
    /// lookup afterward, keeps the pool entry's existence a fact the borrow
    /// checker enforces rather than an invariant a caller has to assume.
    async fn ensure_connection(
        &mut self,
        addr: SocketAddr,
        origin: Option<TopologyServer>,
    ) -> Result<&mut PooledConnection> {
        if self
            .connections
            .get(&addr)
            .is_some_and(|pooled| pooled.conn.is_poisoned())
        {
            self.connections.remove(&addr);
        }
        match self.connections.entry(addr) {
            hash_map::Entry::Occupied(entry) => Ok(entry.into_mut()),
            hash_map::Entry::Vacant(entry) => {
                let mut conn = HotRodConnection::connect_hash_aware(
                    addr,
                    &self.cache_name,
                    self.topology_id,
                    self.timeout,
                )
                .await?;
                if let Some(auth) = &self.auth {
                    auth.authenticate(&mut conn).await?;
                }
                Ok(entry.insert(PooledConnection { origin, conn }))
            }
        }
    }

    /// Applies whatever topology update the connection at `addr` parsed
    /// from its last response, if any, and reconciles the pool against it:
    /// a pooled connection whose origin server is no longer listed has
    /// left the cluster, and is dropped rather than kept open forever. No
    /// hostname is re-resolved to do this: `origin` is compared to the
    /// update's servers by value, and the seed connection is always kept
    /// regardless, since it is the permanent retry fallback.
    fn record_topology_update(&mut self, addr: SocketAddr) {
        let Some(pooled) = self.connections.get_mut(&addr) else {
            return;
        };
        let Some(update) = pooled.conn.take_pending_topology_update() else {
            return;
        };
        self.topology_id = update.topology_id as i32;

        let seed = self.active_seed_addr;
        self.connections.retain(|&addr, pooled| {
            addr == seed
                || pooled
                    .origin
                    .as_ref()
                    .is_none_or(|origin| update.servers.contains(origin))
        });

        self.topology = Some(ClusterTopology {
            servers: update.servers,
            hash_function_version: update.hash_function_version,
            segment_owners: update.segment_owners,
            resolved_addrs: HashMap::new(),
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
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use crate::varint::{read_vint, read_vlong, write_vint, write_vlong};
    use crate::wire::{read_array, write_array};

    #[tokio::test]
    async fn connect_fails_with_no_seed_addresses() {
        let result = HotRodCluster::connect(&[], "my-cache").await;
        assert!(matches!(result, Err(Error::Io(_))));
    }

    /// Binds a listener and immediately drops it, so the returned address
    /// keeps refusing connections without anything else on it.
    async fn unreachable_addr() -> SocketAddr {
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

        let cluster = HotRodCluster::connect_with_timeout(
            &[dead_addr, addr],
            "my-cache",
            Duration::from_secs(5),
        )
        .await
        .expect("the reachable seed should win the race");

        assert_eq!(cluster.active_seed_addr, addr);
    }

    #[tokio::test]
    async fn connect_with_timeout_fails_when_every_seed_is_unreachable() {
        let dead_addrs = [unreachable_addr().await, unreachable_addr().await];

        let result =
            HotRodCluster::connect_with_timeout(&dead_addrs, "my-cache", Duration::from_secs(5))
                .await;

        assert!(matches!(result, Err(Error::Io(_))));
    }

    #[tokio::test]
    async fn set_timeout_applies_to_an_already_pooled_connection() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = seed_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let mut cluster =
            HotRodCluster::connect_with_timeout(&[seed_addr], "my-cache", Duration::from_secs(30))
                .await
                .expect("connect to seed");
        assert_eq!(cluster.timeout(), Duration::from_secs(30));

        cluster.set_timeout(Duration::from_millis(100));
        assert_eq!(cluster.timeout(), Duration::from_millis(100));

        let start = tokio::time::Instant::now();
        let result = cluster.get(b"key").await;

        assert!(matches!(result, Err(Error::Timeout(_))));
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "set_timeout should have applied to the seed connection already in the pool"
        );
    }

    #[tokio::test]
    async fn timeout_evicts_pooled_connection_and_retries_against_seed() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = seed_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let owner_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let owner_addr = owner_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = owner_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let mut cluster = HotRodCluster::connect_with_timeout(
            &[seed_addr],
            "my-cache",
            Duration::from_millis(100),
        )
        .await
        .expect("connect to seed");

        // A single segment covering every key, owned by `owner_addr`, so
        // `get` below routes there instead of the seed.
        cluster.topology = Some(ClusterTopology {
            servers: vec![TopologyServer {
                host: owner_addr.ip().to_string(),
                port: owner_addr.port(),
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0]],
            resolved_addrs: HashMap::new(),
        });

        let result = cluster.get(b"key").await;

        assert!(matches!(result, Err(Error::Timeout(_))));
        assert!(!cluster.connections.contains_key(&owner_addr));
        assert!(!cluster.connections.contains_key(&seed_addr));
    }

    /// A future dropped before it resolves never returns an `Error::Io`/
    /// `Error::Timeout` for `call` to evict on, unlike the case above. This
    /// checks the other enforcement path instead: `HotRodConnection` itself
    /// comes out of the drop poisoned, and `ensure_connection` evicts and
    /// reconnects it on the next call rather than reusing a connection that
    /// may still have the abandoned request's response arriving on it.
    #[tokio::test]
    async fn a_dropped_operation_future_leaves_the_pooled_connection_poisoned_and_gets_reconnected()
    {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut first, _) = seed_listener.accept().await.unwrap();
            read_request_opcode(&mut first).await;

            let (mut second, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut second).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut second).await.unwrap();
            let resp = response_header(id, 0x04, 0x02); // KEY_DOES_NOT_EXIST
            second.write_all(&resp).await.unwrap();
        });

        let mut cluster = HotRodCluster::connect(&[seed_addr], "my-cache")
            .await
            .expect("connect to seed");

        // The seed never answers the first request, so racing it against a
        // short external timeout drops `cluster.get`'s future mid-flight,
        // the same hazard a caller's own `select!` can trigger. The pooled
        // connection stays in the pool, poisoned, since nothing here ever
        // saw an `Error::Io`/`Error::Timeout` to evict it on.
        let raced = tokio::time::timeout(Duration::from_millis(100), cluster.get(b"key")).await;
        assert!(raced.is_err(), "the outer timeout should win the race");
        assert!(cluster.connections.contains_key(&seed_addr));

        let result = cluster
            .get(b"key")
            .await
            .expect("get should reconnect the poisoned seed instead of reading a desynced stream");
        assert_eq!(result, None);

        seed_task.await.unwrap();
    }

    #[tokio::test]
    async fn seed_failure_fails_over_to_the_next_seed_address() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = seed_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let other_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let other_addr = other_listener.local_addr().unwrap();
        let other_task = tokio::spawn(async move {
            let (mut stream, _) = other_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x04, 0x02); // KEY_DOES_NOT_EXIST
            stream.write_all(&resp).await.unwrap();
        });

        let mut cluster = HotRodCluster::connect_with_timeout(
            &[seed_addr],
            "my-cache",
            Duration::from_millis(100),
        )
        .await
        .expect("connect to seed");
        // Simulates a second seed given at construction time, without
        // racing it against `seed_addr` during connect itself.
        cluster.seed_addrs = vec![seed_addr, other_addr];

        let result = cluster
            .get(b"key")
            .await
            .expect("get should fail over to the other seed");

        assert_eq!(result, None);
        assert_eq!(cluster.active_seed_addr, other_addr);
        assert!(cluster.connections.contains_key(&other_addr));
        assert!(!cluster.connections.contains_key(&seed_addr));

        other_task.await.unwrap();
    }

    #[tokio::test]
    async fn seed_failure_returns_the_original_error_when_no_other_seed_is_reachable() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = seed_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let dead_addr = unreachable_addr().await;

        let mut cluster = HotRodCluster::connect_with_timeout(
            &[seed_addr],
            "my-cache",
            Duration::from_millis(100),
        )
        .await
        .expect("connect to seed");
        cluster.seed_addrs = vec![seed_addr, dead_addr];

        let result = cluster.get(b"key").await;

        assert!(
            matches!(result, Err(Error::Timeout(_))),
            "should surface the seed's own timeout, not failover_seed's connect error"
        );
        assert_eq!(
            cluster.active_seed_addr, seed_addr,
            "active seed stays unchanged when no other seed is reachable"
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

        let mut cluster = HotRodCluster::connect(&[addr], "my-cache")
            .await
            .expect("connect to seed");

        cluster.topology = Some(ClusterTopology {
            servers: vec![TopologyServer {
                host: addr.ip().to_string(),
                port: addr.port(),
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0]],
            resolved_addrs: HashMap::new(),
        });

        let (first, _origin) = cluster.owner_addr(b"key").await.expect("first resolution");
        assert_eq!(first, addr);

        // Break the hostname on the topology directly: if `owner_addr`
        // resolved it again instead of using the cache populated above,
        // this call would fail.
        cluster.topology.as_mut().unwrap().servers[0].host =
            "this-hostname-does-not-resolve.invalid".to_string();

        let (second, _origin) = cluster.owner_addr(b"key").await.expect("cached resolution");
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

        let mut cluster = HotRodCluster::connect(&[seed_addr], "my-cache")
            .await
            .expect("connect to seed");

        cluster.topology = Some(ClusterTopology {
            servers: vec![TopologyServer {
                host: "this-hostname-does-not-resolve.invalid".to_string(),
                port: 7000,
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0]],
            resolved_addrs: HashMap::new(),
        });

        let result = cluster.owner_addr(b"key").await;
        assert!(matches!(result, Err(Error::Io(_))));
    }

    /// Reads one request's fixed header fields far enough to identify the
    /// opcode and message id. The fields in between (flags, intelligence,
    /// topology id, media types, additional params) are already covered by
    /// `header.rs`'s own tests, so they are just consumed here, not checked.
    async fn read_request_opcode(stream: &mut TcpStream) -> (u64, u8) {
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

    fn response_header(message_id: u64, opcode: u8, status: u8) -> Vec<u8> {
        let mut buf = vec![0xA1];
        write_vlong(&mut buf, message_id);
        buf.push(opcode);
        buf.push(status);
        buf.push(0); // no topology update
        buf
    }

    /// Serves one `AuthMechList`/`Auth` exchange for SASL PLAIN, asserting
    /// the credentials sent are exactly `expected_plain_response`. Used to
    /// check that a pooled connection opened after the seed replays the
    /// same credentials, not just that it authenticates somehow.
    async fn serve_plain_auth(stream: &mut TcpStream, expected_plain_response: &[u8]) {
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
            "pooled connection must replay the same credentials as the seed"
        );
        let mut resp = response_header(id, 0x24, 0x00);
        resp.push(1); // exchange complete
        write_array(&mut resp, &[]); // no final server message
        stream.write_all(&resp).await.unwrap();
    }

    #[tokio::test]
    async fn ensure_connection_replays_seed_auth_onto_a_newly_opened_pooled_connection() {
        // RFC 4616 PLAIN response for authzid "", authcid "user", password "pass".
        let expected_plain_response = b"\0user\0pass".to_vec();

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let expected_for_seed = expected_plain_response.clone();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            serve_plain_auth(&mut stream, &expected_for_seed).await;
        });

        let owner_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let owner_addr = owner_listener.local_addr().unwrap();
        let expected_for_owner = expected_plain_response.clone();
        let owner_task = tokio::spawn(async move {
            let (mut stream, _) = owner_listener.accept().await.unwrap();
            serve_plain_auth(&mut stream, &expected_for_owner).await;

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x04, 0x02); // KEY_DOES_NOT_EXIST
            stream.write_all(&resp).await.unwrap();
        });

        let mut cluster = HotRodCluster::connect(&[seed_addr], "my-cache")
            .await
            .expect("connect to seed");
        cluster
            .authenticate_plain("", "user", "pass")
            .await
            .expect("authenticate seed connection");

        cluster.topology = Some(ClusterTopology {
            servers: vec![TopologyServer {
                host: owner_addr.ip().to_string(),
                port: owner_addr.port(),
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0]],
            resolved_addrs: HashMap::new(),
        });

        let result = cluster.get(b"key").await.expect("get against owner");
        assert_eq!(result, None);

        seed_task.await.unwrap();
        owner_task.await.unwrap();
    }

    #[tokio::test]
    async fn authenticate_with_reconnects_the_seed_if_it_was_evicted_from_the_pool() {
        let expected_plain_response = b"\0user\0pass".to_vec();

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let expected_for_seed = expected_plain_response.clone();
        let seed_task = tokio::spawn(async move {
            // The initial connect() connection, evicted and dropped below
            // without ever being used.
            let (_first, _) = seed_listener.accept().await.unwrap();
            // The reconnect authenticate_with triggers once it finds the
            // seed missing from the pool.
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            serve_plain_auth(&mut stream, &expected_for_seed).await;
        });

        let mut cluster = HotRodCluster::connect(&[seed_addr], "my-cache")
            .await
            .expect("connect to seed");

        // Simulates an earlier operation failure evicting the seed
        // connection from the pool, the same as `call`/`call_seed` do on
        // Error::Io/Error::Timeout.
        cluster.connections.remove(&seed_addr);

        cluster
            .authenticate_plain("", "user", "pass")
            .await
            .expect("authenticate should reconnect the evicted seed instead of panicking");

        assert!(cluster.connections.contains_key(&seed_addr));

        seed_task.await.unwrap();
    }

    /// Same shape as `response_header`, but with the topology marker set and
    /// a topology update payload appended, encoded the way
    /// `topology::read_topology_update` expects: a vInt topology id, a
    /// vInt-counted server list (vInt-prefixed host, then a raw big-endian
    /// port), a hash function version byte, and a vInt-counted segment list
    /// naming owners by index into that server list.
    fn response_header_with_topology(
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

    #[tokio::test]
    async fn record_topology_update_evicts_a_pooled_connection_to_a_node_no_longer_listed() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = seed_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let owner_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let owner_addr = owner_listener.local_addr().unwrap();
        let owner_task = tokio::spawn(async move {
            let (mut stream, _) = owner_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();

            // The new topology drops the owner itself, listing only the
            // seed: the node this connection was opened for has left the
            // cluster.
            let resp = response_header_with_topology(
                id,
                0x04,
                0x02, // KEY_DOES_NOT_EXIST
                &[(&seed_addr.ip().to_string(), seed_addr.port())],
            );
            stream.write_all(&resp).await.unwrap();
        });

        let mut cluster = HotRodCluster::connect(&[seed_addr], "my-cache")
            .await
            .expect("connect to seed");

        cluster.topology = Some(ClusterTopology {
            servers: vec![TopologyServer {
                host: owner_addr.ip().to_string(),
                port: owner_addr.port(),
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0]],
            resolved_addrs: HashMap::new(),
        });

        let result = cluster.get(b"key").await.expect("get against owner");
        assert_eq!(result, None);

        assert!(
            cluster.connections.contains_key(&seed_addr),
            "the seed connection must never be evicted"
        );
        assert!(
            !cluster.connections.contains_key(&owner_addr),
            "the owner connection must be evicted once its node leaves the topology"
        );
        assert_eq!(
            cluster.topology.as_ref().unwrap().servers,
            vec![TopologyServer {
                host: seed_addr.ip().to_string(),
                port: seed_addr.port(),
            }]
        );

        owner_task.await.unwrap();
    }

    #[tokio::test]
    async fn contains_key_routes_to_the_computed_owner() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = seed_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let owner_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let owner_addr = owner_listener.local_addr().unwrap();
        let owner_task = tokio::spawn(async move {
            let (mut stream, _) = owner_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x0F, "expected a ContainsKey request");
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x10, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut cluster = HotRodCluster::connect(&[seed_addr], "my-cache")
            .await
            .expect("connect to seed");
        cluster.topology = Some(ClusterTopology {
            servers: vec![TopologyServer {
                host: owner_addr.ip().to_string(),
                port: owner_addr.port(),
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0]],
            resolved_addrs: HashMap::new(),
        });

        let result = cluster
            .contains_key(b"key")
            .await
            .expect("contains_key against owner");
        assert!(result);

        owner_task.await.unwrap();
    }

    /// `size`, `clear`, `ping` and `stats` have no key to route by, so they
    /// always target the seed connection, per the Propose step in issue
    /// #40. The topology here points every key's owner at an address that
    /// never accepts a connection: if any of the four routed there instead
    /// of the seed, the call would hang until the timeout rather than
    /// complete against `seed_task`.
    #[tokio::test]
    async fn size_clear_ping_and_stats_always_target_the_seed() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x29, "expected a Size request");
            let mut resp = response_header(id, 0x2A, 0x00);
            write_vint(&mut resp, 4);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x13, "expected a Clear request");
            let resp = response_header(id, 0x14, 0x00);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x17, "expected a Ping request");
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0); // key media type: none
            resp.push(0); // value media type: none
            resp.push(41); // server protocol version
            write_vint(&mut resp, 0); // no supported opcodes listed
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x15, "expected a Stats request");
            let mut resp = response_header(id, 0x16, 0x00);
            write_vint(&mut resp, 0);
            stream.write_all(&resp).await.unwrap();
        });

        let unreachable = unreachable_addr().await;

        let mut cluster = HotRodCluster::connect(&[seed_addr], "my-cache")
            .await
            .expect("connect to seed");
        cluster.topology = Some(ClusterTopology {
            servers: vec![TopologyServer {
                host: unreachable.ip().to_string(),
                port: unreachable.port(),
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0]],
            resolved_addrs: HashMap::new(),
        });

        assert_eq!(cluster.size().await.expect("size"), 4);
        cluster.clear().await.expect("clear");
        cluster.ping().await.expect("ping");
        assert!(cluster.stats().await.expect("stats").is_empty());

        seed_task.await.unwrap();
    }

    /// `get_all` and `put_all` have no single key to route by either, per
    /// the Propose step in issue #41: same setup as
    /// `size_clear_ping_and_stats_always_target_the_seed`, an owner address
    /// that never accepts a connection, so a call routed there instead of
    /// the seed would hang until the timeout rather than complete.
    #[tokio::test]
    async fn get_all_and_put_all_always_target_the_seed() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x2F, "expected a GetAll request");
            let count = read_vint(&mut stream).await.unwrap();
            for _ in 0..count {
                let _key = read_array(&mut stream).await.unwrap();
            }
            let mut resp = response_header(id, 0x30, 0x00);
            write_vint(&mut resp, 0);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x2D, "expected a PutAll request");
            let _time_units = stream.read_u8().await.unwrap();
            let count = read_vint(&mut stream).await.unwrap();
            for _ in 0..count {
                let _key = read_array(&mut stream).await.unwrap();
                let _value = read_array(&mut stream).await.unwrap();
            }
            let resp = response_header(id, 0x2E, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let unreachable = unreachable_addr().await;

        let mut cluster = HotRodCluster::connect(&[seed_addr], "my-cache")
            .await
            .expect("connect to seed");
        cluster.topology = Some(ClusterTopology {
            servers: vec![TopologyServer {
                host: unreachable.ip().to_string(),
                port: unreachable.port(),
            }],
            hash_function_version: 3,
            segment_owners: vec![vec![0]],
            resolved_addrs: HashMap::new(),
        });

        assert!(cluster
            .get_all([b"key".as_slice()])
            .await
            .expect("get_all")
            .is_empty());
        cluster
            .put_all(
                [(b"key".as_slice(), b"value".as_slice())],
                Expiration::Default,
                Expiration::Default,
            )
            .await
            .expect("put_all");

        seed_task.await.unwrap();
    }
}
