//! The plain per-entry cache operations: get, put, remove, replace,
//! the versioned variants, contains_key, ping, size, clear, stats.
//!
//! Split out of `remote_cache.rs`
//! (`docs/adr/0026-split-connection-and-remote-cache-by-domain.md`):
//! see `remote_cache/multimap.rs`'s module docs for the mechanism
//! that lets this `impl RemoteCache` block reach every private
//! field/method it needs with no visibility change.

use std::collections::HashMap;

use crate::connection::{VersionedResult, VersionedValue};
use crate::error::Result;
use crate::wire::Expiration;

use super::{Operation, OperationResult, RemoteCache};

impl RemoteCache {
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap as StdHashMap;
    use std::sync::RwLock;
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use crate::client::tests::{read_request_opcode, response_header, unreachable_addr};
    use crate::client::ClusterTopology;
    use crate::remote_cache::tests::{client_with_seeds, set_topology};
    use crate::topology::TopologyServer;
    use crate::varint::write_vint;
    use crate::wire::read_array;

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
}
