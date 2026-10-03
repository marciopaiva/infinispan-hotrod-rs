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

use std::collections::HashMap;

use crate::client::HotRodClient;
use crate::connection::{HotRodConnection, VersionedResult, VersionedValue};
use crate::error::{Error, Result};
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

    /// Routes `key` to its computed owner, checking out a connection for
    /// this cache (opening and authenticating one first if needed) and
    /// running `op` against it. On an I/O or timeout error, the checked
    /// out connection is left to `PooledGuard::drop` to evict (it comes
    /// back from a failed operation poisoned, see `connection.rs`'s
    /// module docs), and `op` is retried once: against the seed if the
    /// owner was a computed node (`run_seed_op`, itself subject to
    /// failover), or straight to `failover_and_retry` if the owner
    /// already was the seed. Any other error, or a failure with nowhere
    /// left to retry, is returned as is.
    async fn call(&self, key: &[u8], op: Operation) -> Result<OperationResult> {
        let (addr, origin) = self.client.owner_addr(key).await?;
        let outcome = {
            let mut guard = self.client.checkout(addr, &self.cache_name, origin).await?;
            run_operation(&mut guard, &op).await.inspect(|_| {
                self.client.record_topology_update(&mut guard);
            })
        };
        let err = match outcome {
            Ok(value) => return Ok(value),
            Err(err) => err,
        };
        if !matches!(err, Error::Io(_) | Error::Timeout(_)) {
            return Err(err);
        }
        if addr == self.active_seed_addr() {
            return self.failover_and_retry(&op, err).await;
        }
        self.run_seed_op(&op).await
    }

    /// Runs `op` against the seed connection, for an operation with no
    /// key to route by. On `Error::Io`/`Error::Timeout`, `failover_and_retry`
    /// takes over.
    async fn call_seed(&self, op: Operation) -> Result<OperationResult> {
        self.run_seed_op(&op).await
    }

    /// Shared by `call_seed` and `call`'s owner-to-seed retry: runs `op`
    /// against a connection to the seed, for this cache. On
    /// `Error::Io`/`Error::Timeout`, hands off to `failover_and_retry`.
    /// Any other error is returned as is.
    async fn run_seed_op(&self, op: &Operation) -> Result<OperationResult> {
        let seed = self.active_seed_addr();
        let attempt = {
            let mut guard = self.client.checkout(seed, &self.cache_name, None).await?;
            run_operation(&mut guard, op).await.inspect(|_| {
                self.client.record_topology_update(&mut guard);
            })
        };
        match attempt {
            Ok(value) => Ok(value),
            Err(err) => {
                if !matches!(err, Error::Io(_) | Error::Timeout(_)) {
                    return Err(err);
                }
                self.failover_and_retry(op, err).await
            }
        }
    }

    /// Called once the current seed connection has just failed with
    /// `Error::Io`/`Error::Timeout`. Tries every other seed address and,
    /// if one accepts a connection, retries `op` against it once,
    /// promoting it to the active seed. If no other seed is reachable
    /// either, `original_err`, the failure that triggered this in the
    /// first place, is returned rather than whatever `failover_seed`
    /// itself failed with: that is the error the caller's operation
    /// actually hit.
    async fn failover_and_retry(
        &self,
        op: &Operation,
        original_err: Error,
    ) -> Result<OperationResult> {
        let Ok(new_seed) = self.client.failover_seed(&self.cache_name).await else {
            return Err(original_err);
        };
        let mut guard = self
            .client
            .checkout(new_seed, &self.cache_name, None)
            .await?;
        run_operation(&mut guard, op).await.inspect(|_| {
            self.client.record_topology_update(&mut guard);
        })
    }

    fn active_seed_addr(&self) -> std::net::SocketAddr {
        *self.client.inner().active_seed_addr.read().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;
    use std::net::SocketAddr;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::client::tests::{
        read_request_opcode, response_header, response_header_with_topology, serve_plain_auth,
        unreachable_addr,
    };
    use crate::client::{AuthMethod, ClientInner, ClusterTopology, HotRodClient};
    use crate::connection::DEFAULT_TOPOLOGY_ID;
    use crate::topology::TopologyServer;
    use crate::varint::{read_vint, write_vint};
    use crate::wire::read_array;

    /// Builds a client with a fixed seed list and active seed, bypassing
    /// `connect`: several tests need `seed_addrs` to include an address
    /// never dialed during construction (a second seed to fail over to),
    /// which `connect` has no way to express since it always dials every
    /// seed up front.
    fn client_with_seeds(
        seed_addrs: Vec<SocketAddr>,
        active_seed_addr: SocketAddr,
    ) -> HotRodClient {
        HotRodClient::from_inner(ClientInner {
            seed_addrs,
            active_seed_addr: RwLock::new(active_seed_addr),
            topology_id: RwLock::new(DEFAULT_TOPOLOGY_ID),
            topology: RwLock::new(None),
            node_origin: RwLock::new(StdHashMap::new()),
            pools: RwLock::new(StdHashMap::new()),
            auth: RwLock::new(None),
            tls: None,
            timeout: RwLock::new(Duration::from_millis(100)),
        })
    }

    fn set_topology(client: &HotRodClient, topology: ClusterTopology) {
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
}
