//! `RemoteCache`: a handle to one named cache on a `HotRodClient`.
//!
//! See `client.rs`'s module docs and
//! `docs/adr/0005-connection-pooling-and-client-cache-split.md` for the
//! split this replaces `HotRodCluster` with. Every method here takes
//! `&self`: a `RemoteCache` is cheap to clone (it just clones the
//! `HotRodClient` handle it holds) and safe to share across tasks, since
//! all the state that matters (pools, topology, auth) lives behind
//! `HotRodClient`'s own locking, not behind a borrow this type would have
//! to serialize.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::client::HotRodClient;
use crate::connection::{HotRodConnection, VersionedResult, VersionedValue};
use crate::error::{Error, Result};
use crate::health::NodeHealth;
use crate::iteration::{CacheIterator, IterationOptions, NodeIterator};
use crate::listener::{CacheListener, ListenOptions};
use crate::marshall::Marshaller;
use crate::near_cache::{NearCacheOptions, NearCachedCache};
use crate::query::{Query, QueryResult};
use crate::stats::ClientStatistics;
use crate::streaming::{GetStream, PutStream};
use crate::topology::TopologyServer;
use crate::typed_cache::TypedCache;
use crate::wire::Expiration;

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
    Query(Vec<u8>),
    Exec(String, Vec<(String, Vec<u8>)>),
}

impl Operation {
    /// This operation's name for the tracing span `run_and_record`
    /// opens around every dispatch (`docs/adr/0010-client-statistics-and-tracing.md`,
    /// part 2): every variant gets one, unlike `record_stats`'s
    /// classification, which deliberately leaves some out to match
    /// the Java client's narrower instrumented set.
    fn label(&self) -> &'static str {
        match self {
            Operation::Get(_) => "get",
            Operation::Put(..) => "put",
            Operation::PutIfAbsent(..) => "put_if_absent",
            Operation::Replace(..) => "replace",
            Operation::Remove(_) => "remove",
            Operation::GetWithVersion(_) => "get_with_version",
            Operation::ReplaceIfUnmodified(..) => "replace_if_unmodified",
            Operation::RemoveIfUnmodified(..) => "remove_if_unmodified",
            Operation::ContainsKey(_) => "contains_key",
            Operation::Ping => "ping",
            Operation::Size => "size",
            Operation::Clear => "clear",
            Operation::Stats => "stats",
            Operation::GetAll(_) => "get_all",
            Operation::PutAll(..) => "put_all",
            Operation::Query(_) => "query",
            Operation::Exec(..) => "exec",
        }
    }
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
    Query(QueryResult),
    Exec(Vec<u8>),
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
        Operation::Query(request_bytes) => {
            Ok(OperationResult::Query(conn.query(request_bytes).await?))
        }
        Operation::Exec(task_name, params) => Ok(OperationResult::Exec(
            conn.execute_task(task_name, params).await?,
        )),
    }
}

/// A handle to one named cache, obtained from `HotRodClient::cache`.
/// `Clone`-able like `HotRodClient`: cloning just clones the underlying
/// client handle and copies the cache name.
#[derive(Clone)]
pub struct RemoteCache {
    client: HotRodClient,
    cache_name: String,
}

impl RemoteCache {
    pub(crate) fn new(client: HotRodClient, cache_name: String) -> Self {
        Self { client, cache_name }
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.call(key, Operation::Get(key.to_vec())).await? {
            OperationResult::Get(value) => Ok(value),
            _ => unreachable!("Operation::Get always yields OperationResult::Get"),
        }
    }

    pub async fn put(
        &self,
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
        &self,
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
        &self,
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

    pub async fn remove(&self, key: &[u8]) -> Result<bool> {
        match self.call(key, Operation::Remove(key.to_vec())).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => unreachable!("Operation::Remove always yields OperationResult::Bool"),
        }
    }

    pub async fn get_with_version(&self, key: &[u8]) -> Result<Option<VersionedValue>> {
        let op = Operation::GetWithVersion(key.to_vec());
        match self.call(key, op).await? {
            OperationResult::GetWithVersion(value) => Ok(value),
            _ => unreachable!(
                "Operation::GetWithVersion always yields OperationResult::GetWithVersion"
            ),
        }
    }

    pub async fn replace_if_unmodified(
        &self,
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

    pub async fn remove_if_unmodified(&self, key: &[u8], version: u64) -> Result<VersionedResult> {
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
    pub async fn contains_key(&self, key: &[u8]) -> Result<bool> {
        let op = Operation::ContainsKey(key.to_vec());
        match self.call(key, op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => unreachable!("Operation::ContainsKey always yields OperationResult::Bool"),
        }
    }

    /// Checks the seed connection is reachable and its handshake still
    /// holds. There is no key to route by, so this always targets the
    /// seed rather than a computed owner.
    pub async fn ping(&self) -> Result<()> {
        match self.call_seed(Operation::Ping).await? {
            OperationResult::Ping => Ok(()),
            _ => unreachable!("Operation::Ping always yields OperationResult::Ping"),
        }
    }

    /// The number of entries in the cache. The server computes this
    /// cluster-wide from a single request, so which connection it is sent
    /// against (always the seed here) does not change the result.
    pub async fn size(&self) -> Result<u32> {
        match self.call_seed(Operation::Size).await? {
            OperationResult::Size(value) => Ok(value),
            _ => unreachable!("Operation::Size always yields OperationResult::Size"),
        }
    }

    /// Removes every entry from the cache, cluster-wide. Like `size`, the
    /// server fans this out itself.
    pub async fn clear(&self) -> Result<()> {
        match self.call_seed(Operation::Clear).await? {
            OperationResult::Clear => Ok(()),
            _ => unreachable!("Operation::Clear always yields OperationResult::Clear"),
        }
    }

    /// Statistics from whichever node the seed connection currently
    /// targets. Unlike `size` and `clear`, this is not aggregated across
    /// the cluster by the protocol: it reflects only that one node.
    pub async fn stats(&self) -> Result<HashMap<String, String>> {
        match self.call_seed(Operation::Stats).await? {
            OperationResult::Stats(value) => Ok(value),
            _ => unreachable!("Operation::Stats always yields OperationResult::Stats"),
        }
    }

    /// Fetches every key in `keys` that exists, in one request to the
    /// seed connection, regardless of which node in the cluster actually
    /// owns each key. This is not a correctness gap: a distributed
    /// cache's node forwards a request for a key it does not own to the
    /// real owner over internal cluster RPC and returns the result, the
    /// same as any other node would. Splitting the batch client-side by
    /// owner would save that internal hop for keys the seed does not
    /// hold, but the Propose step for issue #41 (and its revisit in issue
    /// #68) chose to keep the single request to the seed, the same
    /// answer already picked for `size`/`clear`/`ping`/`stats` in issue
    /// #40, rather than take on the added complexity of a client-side
    /// split for a network-efficiency gain, not a bug fix.
    /// `MAX_BULK_ENTRIES` is enforced by the underlying
    /// `HotRodConnection::get_all` this delegates to.
    pub async fn get_all(
        &self,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        let keys: Vec<Vec<u8>> = keys.into_iter().map(|key| key.as_ref().to_vec()).collect();
        match self.call_seed(Operation::GetAll(keys)).await? {
            OperationResult::GetAll(value) => Ok(value),
            _ => unreachable!("Operation::GetAll always yields OperationResult::GetAll"),
        }
    }

    /// Writes every key/value pair in `entries` in one request to the
    /// seed connection. Routed the same way as `get_all`, for the same
    /// reason, and enforcing the same `MAX_BULK_ENTRIES` ceiling.
    pub async fn put_all(
        &self,
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

    /// Registers a listener for this cache's events, with every event
    /// type and no server-side filtering. See `listen_with` for finer
    /// control (selected event types, a server-side filter or converter
    /// factory, replaying the cache's current contents first).
    pub async fn listen(&self) -> Result<CacheListener> {
        self.listen_with(&ListenOptions::default()).await
    }

    /// Registers a listener on a connection dedicated to it alone (see
    /// `listener.rs`'s module docs for why), targeting the current active
    /// seed: events are cache-wide, not routed by key, so there is no
    /// segment owner to pick instead. Not retried against another seed if
    /// this one refuses the connection: unlike `call`/`run_seed_op`,
    /// which node ends up holding a long-lived registration is worth the
    /// caller seeing directly rather than this silently failing over.
    pub async fn listen_with(&self, options: &ListenOptions) -> Result<CacheListener> {
        let seed = self.active_seed_addr();
        let conn = self
            .client
            .open_and_authenticate(seed, &self.cache_name, None)
            .await?;
        CacheListener::register(
            conn,
            self.cache_name.as_bytes(),
            self.client.timeout(),
            options,
        )
        .await
    }

    /// Wraps this cache with a bounded, listener-invalidated local cache
    /// for `get` (phase 5 of ADR 0001, see
    /// `docs/adr/0007-near-caching.md`). Internally registers a listener
    /// the same way `listen_with` does, interested only in
    /// `Modified`/`Removed`/`Expired`.
    pub async fn near_cache(&self, options: NearCacheOptions) -> Result<NearCachedCache> {
        NearCachedCache::register(self.clone(), options).await
    }

    /// Wraps this cache with a typed façade
    /// (`docs/adr/0012-serialization-abstraction.md`): every operation
    /// marshalls its typed arguments through `key_marshaller`/
    /// `value_marshaller` and unmarshalls the result, delegating to
    /// this same byte-oriented `RemoteCache` underneath, so routing,
    /// retries and statistics all keep working unchanged. Synchronous
    /// and cheap, unlike `near_cache`: nothing here needs a network
    /// round trip to set up.
    pub fn typed<MK: Marshaller, MV: Marshaller>(
        &self,
        key_marshaller: MK,
        value_marshaller: MV,
    ) -> TypedCache<MK, MV> {
        TypedCache::new(self.clone(), key_marshaller, value_marshaller)
    }

    /// Runs `query` (Ickle) against this cache, see
    /// `docs/adr/0013-remote-query.md`. No key to route by, same as
    /// `get_all`/`put_all`/`size`/`clear`: always goes to the seed, which
    /// is free to run the query cluster-wide or forward it as needed.
    pub fn query(&self, query: impl Into<String>) -> Query<'_> {
        Query::new(self, query.into())
    }

    pub(crate) async fn run_query(&self, request_bytes: Vec<u8>) -> Result<QueryResult> {
        match self.call_seed(Operation::Query(request_bytes)).await? {
            OperationResult::Query(result) => Ok(result),
            _ => unreachable!("Operation::Query always yields OperationResult::Query"),
        }
    }

    /// Runs a named server-side task, see
    /// `docs/adr/0014-remote-administration.md`. No key to route by,
    /// same as `query`: always goes to the seed.
    pub(crate) async fn run_exec(
        &self,
        task_name: String,
        params: Vec<(String, Vec<u8>)>,
    ) -> Result<Vec<u8>> {
        match self.call_seed(Operation::Exec(task_name, params)).await? {
            OperationResult::Exec(result) => Ok(result),
            _ => unreachable!("Operation::Exec always yields OperationResult::Exec"),
        }
    }

    /// Opens a stream to read `key`'s value in chunks of up to
    /// `batch_size` bytes, instead of buffering it whole like `get`.
    /// `None` on a miss, the same as `get`. Routed to `key`'s computed
    /// owner like any other keyed operation, but not retried or failed
    /// over if that connection fails afterward: see
    /// `docs/adr/0008-streaming.md`.
    pub async fn get_stream(&self, key: &[u8], batch_size: u32) -> Result<Option<GetStream>> {
        let (addr, origin) = self.client.owner_addr(key).await?;
        let mut guard = self.client.checkout(addr, &self.cache_name, origin).await?;
        let start = guard.get_stream_start(key, batch_size).await?;
        // Applied regardless of a hit or a miss: the response can carry
        // a topology update either way, and skipping it on a miss would
        // leave this client routing by a stale topology until some
        // unrelated later call happens to reuse this exact connection.
        self.client.record_topology_update(&mut guard);
        Ok(start.map(|start| GetStream::new(guard, start)))
    }

    /// Opens a stream to write `key`'s value in chunks of up to
    /// `chunk_size` bytes, instead of buffering it whole like `put`.
    /// Nothing is written server-side until `PutStream::finish` sends
    /// the final chunk; see `docs/adr/0008-streaming.md`.
    pub async fn put_stream(
        &self,
        key: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
        chunk_size: usize,
    ) -> Result<PutStream> {
        self.open_put_stream(key, lifespan, max_idle, 0, chunk_size)
            .await
    }

    /// Same as `put_stream`, but the write only commits if `key` does
    /// not already exist, the same condition `put_if_absent` checks.
    pub async fn put_stream_if_absent(
        &self,
        key: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
        chunk_size: usize,
    ) -> Result<PutStream> {
        self.open_put_stream(key, lifespan, max_idle, -1, chunk_size)
            .await
    }

    /// Same as `put_stream`, but the write only commits if the entry's
    /// current version still matches `version` (from `get_with_version`),
    /// the same condition `replace_if_unmodified` checks.
    ///
    /// `version` is sent as the wire's `i64` field, where `0` and `-1`
    /// are the `put_stream`/`put_stream_if_absent` sentinels (see
    /// `docs/adr/0008-streaming.md`): a real version whose bit pattern
    /// happens to equal one of those (as `u64::MAX`, or `0` itself)
    /// would be sent as that sentinel instead of a real conditional
    /// version. Inherent to the wire protocol's own encoding, the same
    /// way the Java client's `long` field has the identical limitation;
    /// not something this method can detect or guard against.
    pub async fn replace_stream_with_version(
        &self,
        key: &[u8],
        version: u64,
        lifespan: Expiration,
        max_idle: Expiration,
        chunk_size: usize,
    ) -> Result<PutStream> {
        self.open_put_stream(key, lifespan, max_idle, version as i64, chunk_size)
            .await
    }

    /// Shared by `put_stream`/`put_stream_if_absent`/
    /// `replace_stream_with_version`: opens the connection and the
    /// stream, differing only in which `version` sentinel
    /// `put_stream_start` gets (`0` unconditional, `-1` if-absent, a
    /// real version for a conditional replace).
    async fn open_put_stream(
        &self,
        key: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
        version: i64,
        chunk_size: usize,
    ) -> Result<PutStream> {
        let (addr, origin) = self.client.owner_addr(key).await?;
        let mut guard = self.client.checkout(addr, &self.cache_name, origin).await?;
        let stream_id = guard
            .put_stream_start(key, lifespan, max_idle, version)
            .await?;
        self.client.record_topology_update(&mut guard);
        Ok(PutStream::new(guard, stream_id, chunk_size))
    }

    /// Opens a cursor over every entry in this cache, with the
    /// server's own default batch size and no server-side filter. See
    /// `iter_with` for finer control, and
    /// `docs/adr/0009-server-side-iteration.md` for how the cluster-wide
    /// fan-out works.
    pub async fn iter(&self) -> Result<CacheIterator> {
        self.iter_with(IterationOptions::default()).await
    }

    /// Opens a cursor over every entry in this cache. On a cluster,
    /// this opens one server-side cursor per node that primary-owns at
    /// least one segment, read one node at a time rather than
    /// concurrently; see the module docs on `iteration.rs` for why.
    /// Opens the first node's cursor eagerly, the same as `get_stream`/
    /// `put_stream` open their own connection eagerly, so a failure to
    /// reach it surfaces here rather than on the first `next_entry`
    /// call.
    pub async fn iter_with(&self, options: IterationOptions) -> Result<CacheIterator> {
        let mut targets: VecDeque<_> = self.client.nodes_and_owned_segments().await?.into();
        let current = match targets.pop_front() {
            Some((addr, origin, segments)) => Some(
                self.open_node_iterator(addr, origin, segments, &options)
                    .await?,
            ),
            None => None,
        };
        Ok(CacheIterator::new(self.clone(), current, targets, options))
    }

    /// Shared by `iter_with` and `CacheIterator::next_entry`: checks
    /// out a connection to `addr` and opens a server-side cursor over
    /// `segments` on it (`None` when `segments` is empty, meaning no
    /// filter: the whole cache, from whichever node this is).
    pub(crate) async fn open_node_iterator(
        &self,
        addr: SocketAddr,
        origin: Option<TopologyServer>,
        segments: Vec<u32>,
        options: &IterationOptions,
    ) -> Result<NodeIterator> {
        let segments_arg = if segments.is_empty() {
            None
        } else {
            Some(segments.as_slice())
        };
        let mut guard = self.client.checkout(addr, &self.cache_name, origin).await?;
        let iteration_id = guard
            .iteration_start(
                segments_arg,
                options.filter_factory.as_ref(),
                options.batch_size_or_default(),
            )
            .await?;
        self.client.record_topology_update(&mut guard);
        Ok(NodeIterator::new(guard, iteration_id))
    }

    /// This cache's client-side statistics: counts and average time
    /// for the operations `docs/adr/0010-client-statistics-and-tracing.md`
    /// instruments, always collected, no configuration flag. Creates
    /// this cache name's counters on first access if nothing has
    /// called an instrumented operation on it yet, so this never
    /// needs an `Option`.
    pub fn statistics(&self) -> ClientStatistics {
        self.client.stats_for(&self.cache_name).snapshot()
    }

    /// Zeroes this cache's statistics and restarts the clock
    /// `time_since_reset` measures from.
    pub fn reset_statistics(&self) {
        self.client.stats_for(&self.cache_name).reset();
    }

    /// Routes `key` to its segment's owners and dispatches `op` through
    /// `dispatch`'s retry chain: primary owner first, then each backup
    /// owner, then the active seed, then every other configured seed
    /// (`docs/adr/0011-retry-policy-and-node-health.md`).
    async fn call(&self, key: &[u8], op: Operation) -> Result<OperationResult> {
        let candidates = self.client.owner_and_backup_addrs(key).await?;
        self.dispatch(candidates, op).await
    }

    /// Dispatches `op` through `dispatch`'s retry chain starting from
    /// the active seed, for an operation with no key to route by.
    async fn call_seed(&self, op: Operation) -> Result<OperationResult> {
        self.dispatch(vec![(self.active_seed_addr(), None)], op)
            .await
    }

    /// Shared retry chain for `call`/`call_seed`
    /// (`docs/adr/0011-retry-policy-and-node-health.md`). `candidates`
    /// is tried in order, skipping an address currently quarantined by
    /// `node_health` unless that would leave none at all to try (a
    /// client should never refuse to attempt anything just because
    /// every known node recently failed, since that might no longer be
    /// true). Once `candidates` is exhausted, falls back to
    /// `failover_seed`, which can be called more than once in the same
    /// chain as `active_seed_addr` keeps advancing through the
    /// configured seed list.
    ///
    /// Bounded by `max_retries` attempts beyond the first, the same
    /// default as the Java client. On `Error::Io`/`Error::Timeout`
    /// (the server never answered, as opposed to answering with an
    /// application error), marks the attempted address failed
    /// (`node_health.mark_failed`) and moves to the next candidate;
    /// this never distinguishes by operation type, matching the Java
    /// client's own `supportRetry()`, which does not either (see the
    /// ADR). Any other error returns immediately. On success, clears
    /// the address's quarantine and returns.
    ///
    /// `tried` makes sure no address is attempted twice in one chain,
    /// including ones `failover_seed` returns: without it, a cluster
    /// with only two seeds both down could bounce back and forth
    /// between them instead of exhausting `max_retries` and stopping.
    /// When `failover_seed` itself cannot reach any other seed, or
    /// only offers one already in `tried`, the chain ends and returns
    /// the last real operation error, not `failover_seed`'s own
    /// connection error, which is about reachability, not about what
    /// the caller's operation actually hit.
    async fn dispatch(
        &self,
        candidates: Vec<(SocketAddr, Option<TopologyServer>)>,
        op: Operation,
    ) -> Result<OperationResult> {
        let node_health = self.client.node_health();
        let mut queue = filter_quarantined(
            candidates,
            node_health,
            self.client.server_failure_timeout(),
        );
        let mut tried: HashSet<SocketAddr> = HashSet::new();
        // `saturating_add`, not `+`: `max_retries` is a public setter
        // taking a plain `usize`, so `usize::MAX` must not wrap this to
        // zero (which would refuse even the first attempt) or panic in
        // a debug build.
        let mut attempts_left = self.client.max_retries().saturating_add(1);
        let mut last_err: Option<Error> = None;

        loop {
            if attempts_left == 0 {
                return Err(last_err.unwrap_or_else(exhausted_candidates_error));
            }

            // At most one `failover_seed` dial per outer iteration:
            // once the pre-built `queue` runs out, exactly one more
            // candidate is sought there. A result already in `tried`
            // (every remaining configured seed down to the one this
            // chain already attempted) ends the chain the same as
            // `queue` and `failover_seed` both having nothing left,
            // rather than dialing again and risking never terminating
            // against a cluster that keeps failing back to the same
            // handful of seeds.
            let mut next = None;
            while let Some(candidate) = queue.pop_front() {
                if !tried.contains(&candidate.0) {
                    next = Some(candidate);
                    break;
                }
            }
            if next.is_none() {
                next = match self.client.failover_seed(&self.cache_name).await {
                    Ok(new_seed) if !tried.contains(&new_seed) => Some((new_seed, None)),
                    _ => None,
                };
            }
            let Some((addr, origin)) = next else {
                return Err(last_err.unwrap_or_else(exhausted_candidates_error));
            };

            attempts_left -= 1;
            tried.insert(addr);

            let outcome = match self.client.checkout(addr, &self.cache_name, origin).await {
                Ok(mut guard) => self.run_and_record(&mut guard, &op).await.inspect(|_| {
                    self.client.record_topology_update(&mut guard);
                }),
                Err(err) => Err(err),
            };

            match outcome {
                Ok(value) => {
                    node_health.clear(addr);
                    return Ok(value);
                }
                Err(err) => {
                    if !matches!(err, Error::Io(_) | Error::Timeout(_)) {
                        return Err(err);
                    }
                    node_health.mark_failed(addr);
                    last_err = Some(err);
                }
            }
        }
    }

    /// Runs `op`, wrapped in one `tracing` span covering every
    /// dispatch (`docs/adr/0010-client-statistics-and-tracing.md`,
    /// part 2): `cache`/`op`/`duration_us` fields, a `WARN`-level
    /// event via `err` on failure, but never the key or value `op`/
    /// `conn` themselves carry, which is exactly why those two
    /// arguments are skipped rather than auto-captured. `WARN`, not
    /// the macro's own `ERROR` default: this span covers one attempt,
    /// and `dispatch` retries an `Error::Io`/`Error::Timeout` attempt
    /// against another node, up to `max_retries` times, before giving
    /// up (`docs/adr/0011-retry-policy-and-node-health.md`), so a
    /// transient failure that a retry then recovers from would
    /// otherwise emit the same severity an operation that truly failed
    /// does, indistinguishable to an observability pipeline that pages
    /// on `ERROR`. One retried logical call still opens more than one
    /// span this way, with no shared id linking them back together:
    /// see ADR 0010's Consequences section for why that is an accepted
    /// limitation, not something this phase fixes. Separately, only if
    /// `op` succeeds, records its timing into this cache's statistics
    /// (part 1): a failed attempt (timed out, I/O error, retried)
    /// measures nothing there, since what matters to the counters is
    /// the cost of an operation that actually worked; the span's own
    /// `duration_us` is recorded either way. `dispatch` is the one
    /// place every operation routes `op` through a connection, so this
    /// is also the one place that needs either kind of instrumentation,
    /// rather than each candidate attempt repeating it.
    #[tracing::instrument(
        name = "hotrod_operation",
        skip(self, conn, op),
        fields(cache = %self.cache_name, op = op.label(), duration_us = tracing::field::Empty),
        err(level = "warn")
    )]
    async fn run_and_record(
        &self,
        conn: &mut HotRodConnection,
        op: &Operation,
    ) -> Result<OperationResult> {
        let start = Instant::now();
        let outcome = run_operation(conn, op).await;
        let elapsed = start.elapsed();
        tracing::Span::current().record("duration_us", elapsed.as_micros() as u64);
        let result = outcome?;
        self.record_stats(op, elapsed, &result);
        Ok(result)
    }

    /// Classifies `op`/`result` into this cache's read/store/remove
    /// counters, matching exactly the set the Java client's
    /// `StatsOperationsFactory` instruments: `contains_key`/`ping`/
    /// `size`/`clear`/`stats` fall through to the catch-all and record
    /// nothing, same as there.
    fn record_stats(&self, op: &Operation, elapsed: Duration, result: &OperationResult) {
        let stats = self.client.stats_for(&self.cache_name);
        match (op, result) {
            (Operation::Get(_), OperationResult::Get(value)) => {
                stats.record_read(elapsed, value.is_some());
            }
            (Operation::GetWithVersion(_), OperationResult::GetWithVersion(value)) => {
                stats.record_read(elapsed, value.is_some());
            }
            (Operation::GetAll(keys), OperationResult::GetAll(found)) => {
                // Counted per requested key, not per unique key found:
                // `found.len()` alone undercounts hits (and so
                // overcounts misses) whenever `keys` repeats a key
                // that was actually found, since a `HashMap` can only
                // ever report it once.
                let hits = keys.iter().filter(|key| found.contains_key(*key)).count() as u64;
                let misses = keys.len() as u64 - hits;
                stats.record_bulk_read(elapsed, hits, misses);
            }
            (
                Operation::Put(..)
                | Operation::PutIfAbsent(..)
                | Operation::Replace(..)
                | Operation::ReplaceIfUnmodified(..)
                | Operation::PutAll(..),
                _,
            ) => stats.record_store(elapsed),
            (Operation::Remove(_) | Operation::RemoveIfUnmodified(..), _) => {
                stats.record_remove(elapsed);
            }
            _ => {}
        }
    }

    fn active_seed_addr(&self) -> std::net::SocketAddr {
        *self
            .client
            .inner()
            .active_seed_addr
            .read()
            .unwrap_or_else(|p| p.into_inner())
    }
}

/// Drops every candidate `node_health` currently quarantines, unless
/// that would leave none at all, in which case `candidates` is kept
/// as given: trying something is always better than refusing outright
/// just because every known node recently failed, which might no
/// longer be true by now. `quarantine: None` (quarantine disabled)
/// skips the filter entirely.
fn filter_quarantined(
    candidates: Vec<(SocketAddr, Option<TopologyServer>)>,
    node_health: &NodeHealth,
    quarantine: Option<Duration>,
) -> VecDeque<(SocketAddr, Option<TopologyServer>)> {
    let Some(quarantine) = quarantine else {
        return candidates.into();
    };
    let healthy: Vec<_> = candidates
        .iter()
        .filter(|(addr, _)| !node_health.is_quarantined(*addr, quarantine))
        .cloned()
        .collect();
    if healthy.is_empty() {
        candidates.into()
    } else {
        healthy.into()
    }
}

/// Built only for the case `dispatch`'s own doc comment calls out as
/// unreachable in practice (`call`/`call_seed` always seed `dispatch`
/// with at least the active seed as a candidate): kept as a real,
/// typed fallback rather than a `panic!`/`unreachable!`, since nothing
/// here actually guarantees a caller can never construct an empty
/// candidate list.
fn exhausted_candidates_error() -> Error {
    Error::Io(io::Error::new(
        io::ErrorKind::NotConnected,
        "no node available to dispatch this operation",
    ))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::client::tests::{
        read_request_opcode, response_header, response_header_with_topology, serve_plain_auth,
        unreachable_addr,
    };
    use crate::client::{
        AuthMethod, ClientInner, ClusterTopology, HotRodClient, DEFAULT_MAX_RETRIES,
        DEFAULT_SERVER_FAILURE_TIMEOUT,
    };
    use crate::health::NodeHealth;
    use crate::protobuf_wire::{read_length_delimited, read_tag, WIRE_TYPE_VARINT};
    use crate::query::test_support::{
        query_response_bytes, wrapped_entity_bytes, wrapped_scalar_bytes,
    };
    use crate::query::{QueryRow, QueryValue};
    use crate::topology::TopologyServer;
    use crate::varint::{read_vint, read_vlong, write_vint};
    use crate::wire::{read_array, write_array};

    /// Builds a client with a fixed seed list and active seed, bypassing
    /// `connect`: several tests need `seed_addrs` to include an address
    /// never dialed during construction (a second seed to fail over to),
    /// which `connect` has no way to express since it always dials every
    /// seed up front. Shared with `near_cache.rs`'s own tests via
    /// `client_with_seeds_and_timeout`, rather than each rebuilding the
    /// same `ClientInner` literal.
    pub(crate) fn client_with_seeds_and_timeout(
        seed_addrs: Vec<SocketAddr>,
        active_seed_addr: SocketAddr,
        timeout: Duration,
    ) -> HotRodClient {
        HotRodClient::from_inner(ClientInner {
            seed_addrs,
            active_seed_addr: RwLock::new(active_seed_addr),
            topology: RwLock::new(None),
            node_origin: RwLock::new(StdHashMap::new()),
            pools: RwLock::new(StdHashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(timeout),
            cache_stats: RwLock::new(StdHashMap::new()),
            node_health: NodeHealth::default(),
            server_failure_timeout: RwLock::new(Some(DEFAULT_SERVER_FAILURE_TIMEOUT)),
            max_retries: RwLock::new(DEFAULT_MAX_RETRIES),
        })
    }

    /// Shared with `typed_cache.rs`'s own tests, same reason as
    /// `client_with_seeds_and_timeout`.
    pub(crate) fn client_with_seeds(
        seed_addrs: Vec<SocketAddr>,
        active_seed_addr: SocketAddr,
    ) -> HotRodClient {
        client_with_seeds_and_timeout(seed_addrs, active_seed_addr, Duration::from_millis(100))
    }

    /// Shared with `typed_cache.rs`'s own tests, same reason as
    /// `client_with_seeds_and_timeout`.
    pub(crate) fn set_topology(client: &HotRodClient, topology: ClusterTopology) {
        *client.inner().topology.write().unwrap() = Some(Arc::new(topology));
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

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        // A single segment covering every key, owned by `owner_addr`, so
        // `get` below routes there instead of the seed.
        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: vec![TopologyServer {
                    host: owner_addr.ip().to_string(),
                    port: owner_addr.port(),
                }],
                hash_function_version: 3,
                segment_owners: vec![vec![0]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );

        let result = client.cache("my-cache").get(b"key").await;

        assert!(matches!(result, Err(Error::Timeout(_))));
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

        let client = client_with_seeds(vec![seed_addr, other_addr], seed_addr);

        let result = client
            .cache("my-cache")
            .get(b"key")
            .await
            .expect("get should fail over to the other seed");

        assert_eq!(result, None);
        assert_eq!(*client.inner().active_seed_addr.read().unwrap(), other_addr);

        other_task.await.unwrap();
    }

    /// Same shape as `seed_failure_fails_over_to_the_next_seed_address`,
    /// but the active seed refuses the TCP connection outright instead of
    /// accepting it and then going quiet: `checkout` itself fails
    /// (`Error::Io`) before `run_operation` ever runs. An earlier version
    /// propagated that failure with a bare `?` in `call`/`run_seed_op`,
    /// skipping failover entirely whenever a node could not even be
    /// connected to, the exact case failover exists for; this is the
    /// regression test the review that caught it asked for.
    #[tokio::test]
    async fn seed_failure_fails_over_when_the_seed_refuses_the_connection() {
        let dead_seed_addr = unreachable_addr().await;

        let other_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let other_addr = other_listener.local_addr().unwrap();
        let other_task = tokio::spawn(async move {
            let (mut stream, _) = other_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x17, "expected a Ping request");
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            write_vint(&mut resp, 0);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![dead_seed_addr, other_addr], dead_seed_addr);

        client
            .cache("my-cache")
            .ping()
            .await
            .expect("ping should fail over when the seed refuses the connection outright");

        assert_eq!(*client.inner().active_seed_addr.read().unwrap(), other_addr);

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
        let client = client_with_seeds(vec![seed_addr, dead_addr], seed_addr);

        let result = client.cache("my-cache").get(b"key").await;

        assert!(
            matches!(result, Err(Error::Timeout(_))),
            "should surface the seed's own timeout, not failover_seed's connect error"
        );
        assert_eq!(
            *client.inner().active_seed_addr.read().unwrap(),
            seed_addr,
            "active seed stays unchanged when no other seed is reachable"
        );
    }

    /// `docs/adr/0011-retry-policy-and-node-health.md`: a keyed
    /// operation's retry chain tries the segment's backup owner before
    /// ever falling back to the seed.
    #[tokio::test]
    async fn retry_tries_the_backup_owner_before_falling_over_to_the_seed() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = primary_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let backup_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backup_addr = backup_listener.local_addr().unwrap();
        let backup_task = tokio::spawn(async move {
            let (mut stream, _) = backup_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();
            let mut resp = response_header(id, 0x04, 0x00);
            write_array(&mut resp, b"from backup");
            stream.write_all(&resp).await.unwrap();
        });

        // Never bound to anything: if the chain wrongly reached the seed
        // before trying the backup owner, this would fail fast with
        // `Error::Io` instead of the backup's response succeeding.
        let seed_addr = unreachable_addr().await;

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: vec![
                    TopologyServer {
                        host: primary_addr.ip().to_string(),
                        port: primary_addr.port(),
                    },
                    TopologyServer {
                        host: backup_addr.ip().to_string(),
                        port: backup_addr.port(),
                    },
                ],
                hash_function_version: 3,
                segment_owners: vec![vec![0, 1]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );

        let result = client
            .cache("my-cache")
            .get(b"key")
            .await
            .expect("get should fail over to the backup owner");

        assert_eq!(result, Some(b"from backup".to_vec()));

        backup_task.await.unwrap();
    }

    /// Regression test for a review finding on this change:
    /// `owner_and_backup_addrs` initially never appended the active
    /// seed to a keyed operation's candidate list at all, so once
    /// every owner was exhausted, `dispatch` fell straight to
    /// `failover_seed`, which excludes the current active seed and so
    /// never actually tried it, unlike the pre-rewrite `run_seed_op`,
    /// which guaranteed the seed as a real second attempt.
    #[tokio::test]
    async fn retry_reaches_the_active_seed_after_primary_and_backup_owners_both_fail() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = primary_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let backup_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backup_addr = backup_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = backup_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();
            let mut resp = response_header(id, 0x04, 0x00);
            write_array(&mut resp, b"from seed");
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: vec![
                    TopologyServer {
                        host: primary_addr.ip().to_string(),
                        port: primary_addr.port(),
                    },
                    TopologyServer {
                        host: backup_addr.ip().to_string(),
                        port: backup_addr.port(),
                    },
                ],
                hash_function_version: 3,
                segment_owners: vec![vec![0, 1]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );

        let result =
            client.cache("my-cache").get(b"key").await.expect(
                "get should fail over to the active seed once primary and backup both fail",
            );

        assert_eq!(result, Some(b"from seed".to_vec()));

        seed_task.await.unwrap();
    }

    /// `docs/adr/0011-retry-policy-and-node-health.md`: `max_retries`
    /// bounds the chain even when more candidates are available.
    #[tokio::test]
    async fn retry_stops_after_max_retries_attempts_even_with_more_candidates_available() {
        let touched = Arc::new(Mutex::new(Vec::new()));
        let mut addrs = Vec::new();
        for i in 0..5u32 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(listener.local_addr().unwrap());
            let touched = touched.clone();
            tokio::spawn(async move {
                if let Ok((_stream, _)) = listener.accept().await {
                    touched.lock().unwrap().push(i);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });
        }

        let seed_addr = unreachable_addr().await;
        let client = client_with_seeds(vec![seed_addr], seed_addr);
        client.set_max_retries(1); // first attempt + one retry = 2 total

        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: addrs
                    .iter()
                    .map(|addr| TopologyServer {
                        host: addr.ip().to_string(),
                        port: addr.port(),
                    })
                    .collect(),
                hash_function_version: 3,
                segment_owners: vec![(0..5u32).collect()],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );

        let result = client.cache("my-cache").get(b"key").await;

        assert!(matches!(result, Err(Error::Timeout(_))));
        assert_eq!(
            *touched.lock().unwrap(),
            vec![0, 1],
            "only max_retries + 1 candidates should ever be attempted"
        );
    }

    /// Regression test for a review finding on this change:
    /// `attempts_left` used to be computed as `max_retries() + 1`,
    /// which overflows (panics in a debug build, wraps to 0 in
    /// release) when a caller sets `max_retries` to `usize::MAX`
    /// intending effectively unlimited retries; a wrap to 0 would
    /// refuse even the first attempt, the opposite of that intent.
    #[tokio::test]
    async fn max_retries_set_to_usize_max_does_not_overflow_or_refuse_the_first_attempt() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x17, "expected a Ping request");
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            write_vint(&mut resp, 0);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        client.set_max_retries(usize::MAX);

        client
            .cache("my-cache")
            .ping()
            .await
            .expect("usize::MAX retries must not overflow attempts_left to zero");

        seed_task.await.unwrap();
    }

    /// `docs/adr/0011-retry-policy-and-node-health.md`: a candidate
    /// `node_health` currently quarantines is skipped in favor of the
    /// next one, as long as skipping it still leaves something to try.
    #[tokio::test]
    async fn retry_skips_a_quarantined_candidate_in_favor_of_the_next_one() {
        let touched_quarantined = Arc::new(Mutex::new(false));
        let quarantined_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let quarantined_addr = quarantined_listener.local_addr().unwrap();
        let touched_quarantined_in_task = touched_quarantined.clone();
        tokio::spawn(async move {
            let (mut stream, _) = quarantined_listener.accept().await.unwrap();
            *touched_quarantined_in_task.lock().unwrap() = true;
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();
            let mut resp = response_header(id, 0x04, 0x00);
            write_array(&mut resp, b"wrong");
            stream.write_all(&resp).await.unwrap();
        });

        let healthy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let healthy_addr = healthy_listener.local_addr().unwrap();
        let healthy_task = tokio::spawn(async move {
            let (mut stream, _) = healthy_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();
            let mut resp = response_header(id, 0x04, 0x00);
            write_array(&mut resp, b"right");
            stream.write_all(&resp).await.unwrap();
        });

        let seed_addr = unreachable_addr().await;
        let client = client_with_seeds(vec![seed_addr], seed_addr);
        client.inner().node_health.mark_failed(quarantined_addr);

        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: vec![
                    TopologyServer {
                        host: quarantined_addr.ip().to_string(),
                        port: quarantined_addr.port(),
                    },
                    TopologyServer {
                        host: healthy_addr.ip().to_string(),
                        port: healthy_addr.port(),
                    },
                ],
                hash_function_version: 3,
                segment_owners: vec![vec![0, 1]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );

        let result = client
            .cache("my-cache")
            .get(b"key")
            .await
            .expect("get should succeed via the non-quarantined backup");

        assert_eq!(result, Some(b"right".to_vec()));
        assert!(
            !*touched_quarantined.lock().unwrap(),
            "the quarantined primary owner must not be attempted while a healthy candidate is available"
        );

        healthy_task.await.unwrap();
    }

    /// `docs/adr/0011-retry-policy-and-node-health.md`: a topology
    /// update that actually changes the topology id lifts every
    /// quarantine, not just the ones for addresses it lists.
    #[tokio::test]
    async fn topology_update_clears_every_quarantine() {
        let owner_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let owner_addr = owner_listener.local_addr().unwrap();
        let owner_task = tokio::spawn(async move {
            let (mut stream, _) = owner_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header_with_topology(
                id,
                0x04,
                0x02, // KEY_DOES_NOT_EXIST
                &[(&owner_addr.ip().to_string(), owner_addr.port())],
            );
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![owner_addr], owner_addr);
        set_topology(
            &client,
            ClusterTopology {
                // Different from `response_header_with_topology`'s
                // hardcoded new id (9), so the update below is a real
                // topology id change.
                topology_id: 1,
                servers: vec![TopologyServer {
                    host: owner_addr.ip().to_string(),
                    port: owner_addr.port(),
                }],
                hash_function_version: 3,
                segment_owners: vec![vec![0]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );

        let quarantined = unreachable_addr().await;
        client.inner().node_health.mark_failed(quarantined);
        assert!(client
            .inner()
            .node_health
            .is_quarantined(quarantined, Duration::from_secs(30)));

        client
            .cache("my-cache")
            .get(b"key")
            .await
            .expect("get against the owner");

        assert!(
            !client
                .inner()
                .node_health
                .is_quarantined(quarantined, Duration::from_secs(30)),
            "a topology update that changes the topology id should clear every quarantine"
        );

        owner_task.await.unwrap();
    }

    #[tokio::test]
    async fn ensure_connection_replays_seed_auth_onto_a_newly_opened_pooled_connection() {
        // RFC 4616 PLAIN response for authzid "", authcid "user", password "pass".
        let expected_plain_response = b"\0user\0pass".to_vec();

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

        let client = client_with_seeds(vec![owner_addr], owner_addr);
        *client.inner().auth.write().unwrap() = Some(AuthMethod::Plain {
            authzid: String::new(),
            authcid: "user".to_string(),
            password: "pass".to_string(),
        });

        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: vec![TopologyServer {
                    host: owner_addr.ip().to_string(),
                    port: owner_addr.port(),
                }],
                hash_function_version: 3,
                segment_owners: vec![vec![0]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );

        let result = client
            .cache("my-cache")
            .get(b"key")
            .await
            .expect("get against owner");
        assert_eq!(result, None);

        owner_task.await.unwrap();
    }

    #[tokio::test]
    async fn record_topology_update_evicts_a_pool_to_a_node_no_longer_listed() {
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

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: vec![TopologyServer {
                    host: owner_addr.ip().to_string(),
                    port: owner_addr.port(),
                }],
                hash_function_version: 3,
                segment_owners: vec![vec![0]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );

        let cache = client.cache("my-cache");
        let result = cache.get(b"key").await.expect("get against owner");
        assert_eq!(result, None);

        assert!(
            !client
                .inner()
                .pools
                .read()
                .unwrap()
                .contains_key(&(owner_addr, "my-cache".to_string())),
            "the owner's pool must be evicted once its node leaves the topology"
        );
        assert_eq!(
            client
                .inner()
                .topology
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .servers,
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

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: vec![TopologyServer {
                    host: owner_addr.ip().to_string(),
                    port: owner_addr.port(),
                }],
                hash_function_version: 3,
                segment_owners: vec![vec![0]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );

        let result = client
            .cache("my-cache")
            .contains_key(b"key")
            .await
            .expect("contains_key against owner");
        assert!(result);

        owner_task.await.unwrap();
    }

    /// `size`, `clear`, `ping` and `stats` have no key to route by, so
    /// they always target the seed connection, per the Propose step in
    /// issue #40. The topology here points every key's owner at an
    /// address that never accepts a connection: if any of the four
    /// routed there instead of the seed, the call would hang until the
    /// timeout rather than complete against `seed_task`.
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

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: vec![TopologyServer {
                    host: unreachable.ip().to_string(),
                    port: unreachable.port(),
                }],
                hash_function_version: 3,
                segment_owners: vec![vec![0]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );
        let cache = client.cache("my-cache");

        assert_eq!(cache.size().await.expect("size"), 4);
        cache.clear().await.expect("clear");
        cache.ping().await.expect("ping");
        assert!(cache.stats().await.expect("stats").is_empty());

        seed_task.await.unwrap();
    }

    /// `register_proto_schema`'s wire format was confirmed only
    /// "empirically" against a live server
    /// (`docs/adr/0013-remote-query.md`); this is its fake-server
    /// regression coverage: a predefined `application/x-protostream`
    /// media type (id 12) declared for both key and value, both
    /// `WrappedMessage`-wrapped as strings, not sent as raw bytes.
    #[tokio::test]
    async fn register_proto_schema_declares_protostream_and_wraps_key_and_value() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            assert_eq!(
                stream.read_u8().await.unwrap(),
                0xA0,
                "expected a request magic byte"
            );
            let id = read_vlong(&mut stream).await.unwrap();
            let _version = stream.read_u8().await.unwrap();
            let opcode = stream.read_u8().await.unwrap();
            assert_eq!(opcode, 0x01, "expected a Put request");
            let cache_name = read_array(&mut stream).await.unwrap();
            assert_eq!(cache_name, b"___protobuf_metadata");
            let _flags = read_vint(&mut stream).await.unwrap();
            let _intelligence = stream.read_u8().await.unwrap();
            let _topology_id = read_vint(&mut stream).await.unwrap();

            for _ in 0..2 {
                assert_eq!(
                    stream.read_u8().await.unwrap(),
                    1,
                    "expected a predefined media type, not \"none\""
                );
                assert_eq!(
                    read_vint(&mut stream).await.unwrap(),
                    12,
                    "expected application/x-protostream's predefined id"
                );
                assert_eq!(
                    read_vint(&mut stream).await.unwrap(),
                    0,
                    "expected no media type parameters"
                );
            }
            let _additional_params = read_vint(&mut stream).await.unwrap();

            let key = read_array(&mut stream).await.unwrap();
            let _time_units = stream.read_u8().await.unwrap();
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(
                key,
                wrapped_scalar_bytes(&QueryValue::String("my-schema.proto".to_string()))
            );
            assert_eq!(
                value,
                wrapped_scalar_bytes(&QueryValue::String("message Foo {}".to_string()))
            );

            let resp = response_header(id, 0x02, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        client
            .register_proto_schema("my-schema.proto", "message Foo {}")
            .await
            .expect("register_proto_schema should succeed");

        seed_task.await.unwrap();
    }

    /// `docs/adr/0013-remote-query.md`: no key to route by, so `query`
    /// always targets the seed, same as `size`/`clear`/`ping`/`stats`.
    /// Without a projection, each result is a whole entity.
    #[tokio::test]
    async fn query_targets_the_seed_and_returns_whole_entities() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let request_body = read_array(&mut stream).await.unwrap();
            assert!(String::from_utf8_lossy(&request_body).contains("FROM org.example.User"));

            let response = query_response_bytes(
                0,
                &[wrapped_entity_bytes("org.example.User", b"entity-bytes")],
                1,
            );
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let result = client
            .cache("my-cache")
            .query("FROM org.example.User")
            .execute()
            .await
            .expect("query should succeed");

        assert_eq!(
            result.rows,
            vec![QueryRow::Entity(b"entity-bytes".to_vec())]
        );
        assert_eq!(result.hit_count, 1);

        seed_task.await.unwrap();
    }

    /// With a projection (`SELECT a, b`), results group into one row per
    /// `projection_size` consecutive `WrappedMessage`s.
    #[tokio::test]
    async fn query_with_projection_returns_columns() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let request_body = read_array(&mut stream).await.unwrap();
            assert!(String::from_utf8_lossy(&request_body).contains("SELECT name, age"));
            // Confirm the named parameter made it into the request too,
            // rather than only checking the response path.
            let mut pos = 0;
            let mut saw_named_parameters = false;
            while let Some((field, wire_type)) = read_tag(&request_body, &mut pos).unwrap() {
                if field == 5 {
                    saw_named_parameters = true;
                    read_length_delimited(&request_body, &mut pos).unwrap();
                } else if wire_type == WIRE_TYPE_VARINT {
                    crate::protobuf_wire::read_varint(&request_body, &mut pos).unwrap();
                } else {
                    read_length_delimited(&request_body, &mut pos).unwrap();
                }
            }
            assert!(
                saw_named_parameters,
                "expected namedParameters in the request"
            );

            let response = query_response_bytes(
                2,
                &[
                    wrapped_scalar_bytes(&QueryValue::String("Alice".to_string())),
                    wrapped_scalar_bytes(&QueryValue::Int32(30)),
                    wrapped_scalar_bytes(&QueryValue::String("Bob".to_string())),
                    wrapped_scalar_bytes(&QueryValue::Int32(40)),
                ],
                2,
            );
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let result = client
            .cache("my-cache")
            .query("SELECT name, age FROM org.example.User WHERE age >= :minAge")
            .param("minAge", QueryValue::Int32(18))
            .execute()
            .await
            .expect("query should succeed");

        assert_eq!(
            result.rows,
            vec![
                QueryRow::Columns(vec![
                    QueryValue::String("Alice".to_string()),
                    QueryValue::Int32(30)
                ]),
                QueryRow::Columns(vec![
                    QueryValue::String("Bob".to_string()),
                    QueryValue::Int32(40)
                ]),
            ]
        );
        assert_eq!(result.hit_count, 2);

        seed_task.await.unwrap();
    }

    /// `get_all` and `put_all` have no single key to route by either, per
    /// the Propose step in issue #41: same setup as
    /// `size_clear_ping_and_stats_always_target_the_seed`, an owner
    /// address that never accepts a connection, so a call routed there
    /// instead of the seed would hang until the timeout rather than
    /// complete.
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

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: vec![TopologyServer {
                    host: unreachable.ip().to_string(),
                    port: unreachable.port(),
                }],
                hash_function_version: 3,
                segment_owners: vec![vec![0]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );
        let cache = client.cache("my-cache");

        assert!(cache
            .get_all([b"key".as_slice()])
            .await
            .expect("get_all")
            .is_empty());
        cache
            .put_all(
                [(b"key".as_slice(), b"value".as_slice())],
                Expiration::Default,
                Expiration::Default,
            )
            .await
            .expect("put_all");

        seed_task.await.unwrap();
    }

    /// Covers `docs/adr/0010-client-statistics-and-tracing.md`'s
    /// instrumented set end to end: a hit and a miss (`get`), a store
    /// (`put`), a remove (`remove`), and a bulk read with one hit and
    /// one miss (`get_all`), checked against `statistics()`'s final
    /// snapshot. No topology is set, so every call routes straight to
    /// the seed, the same simplification
    /// `contains_key_routes_to_the_computed_owner`'s sibling tests
    /// already rely on elsewhere in this file.
    #[tokio::test]
    async fn statistics_records_hits_misses_stores_and_removes() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request (hit)");
            let _key = read_array(&mut stream).await.unwrap();
            let mut resp = response_header(id, 0x04, 0x00);
            write_array(&mut resp, b"value");
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request (miss)");
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x04, 0x02); // KEY_DOES_NOT_EXIST
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x01, "expected a Put request");
            let _key = read_array(&mut stream).await.unwrap();
            let _time_units = stream.read_u8().await.unwrap();
            let _value = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x02, 0x00);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x0B, "expected a Remove request");
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x0C, 0x00);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x2F, "expected a GetAll request");
            let count = read_vint(&mut stream).await.unwrap();
            for _ in 0..count {
                read_array(&mut stream).await.unwrap();
            }
            let mut resp = response_header(id, 0x30, 0x00);
            write_vint(&mut resp, 1); // one entry found
            write_array(&mut resp, b"found-key");
            write_array(&mut resp, b"found-value");
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let cache = client.cache("my-cache");

        assert_eq!(
            cache.get(b"hit-key").await.expect("get"),
            Some(b"value".to_vec())
        );
        assert_eq!(cache.get(b"miss-key").await.expect("get"), None);
        cache
            .put(b"key", b"value", Expiration::Default, Expiration::Default)
            .await
            .expect("put");
        cache.remove(b"key").await.expect("remove");
        cache
            .get_all([b"found-key".as_slice(), b"missing-key".as_slice()])
            .await
            .expect("get_all");

        let stats = cache.statistics();
        assert_eq!(stats.remote_hits, 2, "one from get, one from get_all");
        assert_eq!(stats.remote_misses, 2, "one from get, one from get_all");
        assert_eq!(stats.remote_stores, 1);
        assert_eq!(stats.remote_removes, 1);

        seed_task.await.unwrap();
    }

    /// Regression test for a review finding: `get_all` must count hits
    /// and misses per *requested* key, not per unique key the server
    /// found. Requesting the same existing key twice must record two
    /// hits, not one hit and one miss, which `found.len()` alone would
    /// have given (a `HashMap` only ever reports a key once).
    #[tokio::test]
    async fn statistics_counts_get_all_hits_per_requested_key_not_per_unique_key_found() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x2F, "expected a GetAll request");
            let count = read_vint(&mut stream).await.unwrap();
            assert_eq!(count, 2, "the duplicate key must still be sent twice");
            for _ in 0..count {
                read_array(&mut stream).await.unwrap();
            }
            let mut resp = response_header(id, 0x30, 0x00);
            write_vint(&mut resp, 1); // one unique entry found
            write_array(&mut resp, b"dup-key");
            write_array(&mut resp, b"value");
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let cache = client.cache("my-cache");

        cache
            .get_all([b"dup-key".as_slice(), b"dup-key".as_slice()])
            .await
            .expect("get_all");

        let stats = cache.statistics();
        assert_eq!(
            stats.remote_hits, 2,
            "both requests for the found key must count"
        );
        assert_eq!(stats.remote_misses, 0);

        seed_task.await.unwrap();
    }

    /// `contains_key`/`ping`/`size`/`clear`/`stats` are explicitly not
    /// instrumented by the Java client either (see the ADR); confirms
    /// this client matches that exactly instead of silently
    /// instrumenting more than intended.
    #[tokio::test]
    async fn statistics_does_not_record_uninstrumented_operations() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x0F, "expected a ContainsKey request");
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x10, 0x02); // KEY_DOES_NOT_EXIST
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x17, "expected a Ping request");
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            write_vint(&mut resp, 0);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x29, "expected a Size request");
            let mut resp = response_header(id, 0x2A, 0x00);
            write_vint(&mut resp, 0);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x13, "expected a Clear request");
            let resp = response_header(id, 0x14, 0x00);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x15, "expected a Stats request");
            let mut resp = response_header(id, 0x16, 0x00);
            write_vint(&mut resp, 0);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let cache = client.cache("my-cache");

        cache.contains_key(b"key").await.expect("contains_key");
        cache.ping().await.expect("ping");
        cache.size().await.expect("size");
        cache.clear().await.expect("clear");
        cache.stats().await.expect("stats");

        // Every counter must still be zero; `time_since_reset` is not
        // compared against `ClientStatistics::default()` here, since
        // it legitimately ticks up from this cache's first access
        // onward, reset or not.
        let stats = cache.statistics();
        assert_eq!(stats.remote_hits, 0);
        assert_eq!(stats.remote_misses, 0);
        assert_eq!(stats.average_remote_read_time, Duration::ZERO);
        assert_eq!(stats.remote_stores, 0);
        assert_eq!(stats.average_remote_store_time, Duration::ZERO);
        assert_eq!(stats.remote_removes, 0);
        assert_eq!(stats.average_remote_remove_time, Duration::ZERO);

        seed_task.await.unwrap();
    }

    /// A failed operation (a timeout here) must not be counted: it
    /// measures the cost of an operation that worked, not one that
    /// failed trying.
    #[tokio::test]
    async fn statistics_does_not_record_a_failed_operation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = client_with_seeds_and_timeout(vec![addr], addr, Duration::from_millis(100));
        let cache = client.cache("my-cache");

        let result = cache.get(b"key").await;
        assert!(matches!(result, Err(Error::Timeout(_))));

        let stats = cache.statistics();
        assert_eq!(stats.remote_hits, 0);
        assert_eq!(stats.remote_misses, 0);
        assert_eq!(stats.average_remote_read_time, Duration::ZERO);
    }

    #[tokio::test]
    async fn reset_statistics_zeroes_counts() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x04, 0x02);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let cache = client.cache("my-cache");
        cache.get(b"key").await.expect("get");
        assert_eq!(cache.statistics().remote_misses, 1);

        cache.reset_statistics();
        let stats = cache.statistics();
        assert_eq!(stats.remote_misses, 0);
        assert_eq!(stats.remote_hits, 0);
        assert!(
            stats.time_since_reset < Duration::from_secs(1),
            "reset_statistics should have restarted the clock just now"
        );

        seed_task.await.unwrap();
    }

    /// A minimal `tracing::Subscriber` that just records, for every
    /// span opened, its name and the names of the fields it declared
    /// (not their values: `Attributes` exposes those lazily through a
    /// visitor this test has no need to implement, since confirming
    /// which fields exist is already enough to prove `skip(..)` left
    /// `key`/`value`/`conn`/`op` itself out), and the name of every
    /// event recorded. No extra dependency: everything here comes
    /// from the `tracing` crate this PR already added, not a test-only
    /// mocking crate.
    #[derive(Default)]
    struct RecordingSubscriber {
        spans: Mutex<Vec<(&'static str, Vec<&'static str>)>>,
        events: Mutex<Vec<&'static str>>,
    }

    impl tracing::Subscriber for RecordingSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let fields = attrs.metadata().fields().iter().map(|f| f.name()).collect();
            self.spans
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((attrs.metadata().name(), fields));
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            self.events
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(event.metadata().name());
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    static GLOBAL_TRACING_DEFAULT_INIT: std::sync::Once = std::sync::Once::new();

    /// Installs a permanently-`enabled()` `RecordingSubscriber` (its
    /// own recorded spans/events are never read back; only its
    /// `enabled() -> true` matters here, so there is no need for a
    /// second, otherwise-identical subscriber type just for this) as
    /// tracing's *global* default, then rebuilds the interest cache,
    /// both exactly once for the whole test binary: every call after
    /// the first is a complete no-op, `Once` guarantees that.
    ///
    /// `tracing`'s per-callsite interest cache is global for the
    /// whole test binary, not scoped to a subscriber or a thread:
    /// without this, whichever test anywhere in this binary (tracing-
    /// aware or not, since dozens of other tests in this very module
    /// call `cache.get()`/`put()`/... with no subscriber installed at
    /// all) happens to be the first, on whatever thread, to touch the
    /// `hotrod_operation` callsite decides, for the rest of the
    /// process, whether that callsite is ever dispatched to any
    /// subscriber again at all, including a `set_default` override a
    /// later test installs on its own thread: once cached `never()`,
    /// a thread-local override does not get consulted at all, which
    /// is exactly the flake a version of this test without this fix
    /// hit under the test harness's default parallel execution. A
    /// global default that is always `enabled()` keeps that cached
    /// interest at `sometimes()` instead, so each later occurrence
    /// still asks `Dispatch::current()` fresh, which a `set_default`
    /// guard correctly overrides on its own thread regardless of what
    /// any other concurrent thread is doing. The rebuild only needs
    /// to run once, right after the install it is paired with inside
    /// the same `call_once` closure: by the time that closure returns
    /// to any caller, `Once`'s own happens-before guarantee means the
    /// global default is already visible process-wide, closing the
    /// one race window (a callsite lazily registered by an unrelated,
    /// concurrently-running test while this closure is still running)
    /// that rebuild exists to correct; nothing a later call to this
    /// function could still be racing against. Call this before
    /// installing a thread-local `set_default` override, in every
    /// test that asserts on captured spans/events.
    fn ensure_global_tracing_default() {
        GLOBAL_TRACING_DEFAULT_INIT.call_once(|| {
            // Already-set is fine (e.g. a future caller after some
            // other mechanism won the race to set a global default):
            // this function's whole point is achieved either way, an
            // always-enabled global default now exists.
            let _ = tracing::subscriber::set_global_default(RecordingSubscriber::default());
            tracing::callsite::rebuild_interest_cache();
        });
    }

    /// Covers `docs/adr/0010-client-statistics-and-tracing.md`'s part
    /// 2: a dispatched operation must open exactly one
    /// `hotrod_operation` span, with exactly the field set
    /// `cache`/`op`/`duration_us`, no more and no less. Checking the
    /// full set rather than a blocklist of forbidden names is what
    /// makes this a real proof that
    /// `#[tracing::instrument(skip(self, conn, op), ...)]` is doing
    /// its job: `self`/`conn`/`op` are this function's only other
    /// parameters, and `op` in particular carries the operation's raw
    /// key and value bytes directly, so either one being
    /// auto-captured instead of skipped would show up here as a
    /// fourth field, caught regardless of what that field happened to
    /// be named.
    #[tokio::test]
    async fn run_and_record_opens_a_span_naming_the_operation_without_key_or_value_fields() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();
            let mut resp = response_header(id, 0x04, 0x00);
            write_array(&mut resp, b"value");
            stream.write_all(&resp).await.unwrap();
        });

        ensure_global_tracing_default();
        let subscriber = Arc::new(RecordingSubscriber::default());
        let _default_guard = tracing::subscriber::set_default(subscriber.clone());

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let cache = client.cache("my-cache");
        cache.get(b"super-secret-key").await.expect("get");

        {
            let spans = subscriber.spans.lock().unwrap();
            assert_eq!(spans.len(), 1, "exactly one span per dispatched operation");
            let (name, fields) = &spans[0];
            assert_eq!(*name, "hotrod_operation");
            // The exact field set, not just a blocklist: `run_and_record`'s
            // only parameters are `self`, `conn` and `op`, so a field
            // literally named "key" or "value" could never appear
            // regardless of whether `skip(..)` is correct, and checking
            // for their absence alone would prove nothing. Asserting the
            // full set instead also catches `self`/`conn` ever being
            // auto-captured, which `skip(..)` is what actually prevents.
            let mut sorted_fields = fields.clone();
            sorted_fields.sort_unstable();
            assert_eq!(sorted_fields, ["cache", "duration_us", "op"]);
        }

        seed_task.await.unwrap();
    }

    /// Same span, but on a failed dispatch: `#[instrument(err(level =
    /// "warn"))]` must still emit an event (so a `tracing` subscriber
    /// sees the failure, at `WARN` rather than the macro's `ERROR`
    /// default), without this test needing to inspect the error's own
    /// content or the event's level.
    #[tokio::test]
    async fn run_and_record_emits_an_error_event_on_a_failed_operation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        ensure_global_tracing_default();
        let subscriber = Arc::new(RecordingSubscriber::default());
        let _default_guard = tracing::subscriber::set_default(subscriber.clone());

        let client = client_with_seeds_and_timeout(vec![addr], addr, Duration::from_millis(100));
        let cache = client.cache("my-cache");
        let result = cache.get(b"key").await;
        assert!(matches!(result, Err(Error::Timeout(_))));

        assert!(
            !subscriber.events.lock().unwrap().is_empty(),
            "a failed operation must still emit an error event"
        );
    }
}
