//! Multimap cache dispatch (`docs/adr/0016-multimap-cache.md`). Every
//! operation here targets this `RemoteCache`'s own cache name (an
//! ordinary cache, set by `HotRodClient::multimap_cache`), not a
//! fixed reserved one like counters. `get`/`put`/`remove_key`/
//! `remove_entry`/`contains_key`/`contains_entry` have a key to route
//! by, so they go through `call`, the same routing/retry chain
//! `RemoteCache`'s own keyed methods already use; `size`/
//! `contains_value` have none, so they go to the seed.
//!
//! Split out of `remote_cache.rs`
//! (`docs/adr/0026-split-connection-and-remote-cache-by-domain.md`):
//! `impl RemoteCache` blocks are free to live in any module descended
//! from `remote_cache`, since Rust's privacy rule makes `call`/
//! `call_seed`, private methods defined at `remote_cache.rs`'s root,
//! visible here without any visibility change.

use crate::error::Result;
use crate::wire::Expiration;

use super::{Operation, OperationResult, RemoteCache};

impl RemoteCache {
    pub(crate) async fn multimap_get(
        &self,
        key: &[u8],
        supports_duplicates: bool,
    ) -> Result<Vec<Vec<u8>>> {
        match self
            .call(
                key,
                Operation::MultimapGet(key.to_vec(), supports_duplicates),
            )
            .await?
        {
            OperationResult::MultimapValues(values) => Ok(values),
            _ => {
                unreachable!("Operation::MultimapGet always yields OperationResult::MultimapValues")
            }
        }
    }

    pub(crate) async fn multimap_get_with_metadata(
        &self,
        key: &[u8],
        supports_duplicates: bool,
    ) -> Result<Option<crate::multimap::MultimapEntry>> {
        let op = Operation::MultimapGetWithMetadata(key.to_vec(), supports_duplicates);
        match self.call(key, op).await? {
            OperationResult::MultimapEntry(entry) => Ok(entry),
            _ => unreachable!(
                "Operation::MultimapGetWithMetadata always yields OperationResult::MultimapEntry"
            ),
        }
    }

    pub(crate) async fn multimap_put(
        &self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
        supports_duplicates: bool,
    ) -> Result<()> {
        let op = Operation::MultimapPut(
            key.to_vec(),
            value.to_vec(),
            lifespan,
            max_idle,
            supports_duplicates,
        );
        match self.call(key, op).await? {
            OperationResult::MultimapPut => Ok(()),
            _ => unreachable!("Operation::MultimapPut always yields OperationResult::MultimapPut"),
        }
    }

    pub(crate) async fn multimap_remove_key(
        &self,
        key: &[u8],
        supports_duplicates: bool,
    ) -> Result<bool> {
        let op = Operation::MultimapRemoveKey(key.to_vec(), supports_duplicates);
        match self.call(key, op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => unreachable!("Operation::MultimapRemoveKey always yields OperationResult::Bool"),
        }
    }

    pub(crate) async fn multimap_remove_entry(
        &self,
        key: &[u8],
        value: &[u8],
        supports_duplicates: bool,
    ) -> Result<bool> {
        let op = Operation::MultimapRemoveEntry(key.to_vec(), value.to_vec(), supports_duplicates);
        match self.call(key, op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => {
                unreachable!("Operation::MultimapRemoveEntry always yields OperationResult::Bool")
            }
        }
    }

    /// No key to route by: goes to the seed, same as `RemoteCache::size`.
    pub(crate) async fn multimap_size(&self, supports_duplicates: bool) -> Result<u64> {
        match self
            .call_seed(Operation::MultimapSize(supports_duplicates))
            .await?
        {
            OperationResult::MultimapSize(size) => Ok(size),
            _ => {
                unreachable!("Operation::MultimapSize always yields OperationResult::MultimapSize")
            }
        }
    }

    pub(crate) async fn multimap_contains_entry(
        &self,
        key: &[u8],
        value: &[u8],
        supports_duplicates: bool,
    ) -> Result<bool> {
        let op =
            Operation::MultimapContainsEntry(key.to_vec(), value.to_vec(), supports_duplicates);
        match self.call(key, op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => {
                unreachable!("Operation::MultimapContainsEntry always yields OperationResult::Bool")
            }
        }
    }

    pub(crate) async fn multimap_contains_key(
        &self,
        key: &[u8],
        supports_duplicates: bool,
    ) -> Result<bool> {
        let op = Operation::MultimapContainsKey(key.to_vec(), supports_duplicates);
        match self.call(key, op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => {
                unreachable!("Operation::MultimapContainsKey always yields OperationResult::Bool")
            }
        }
    }

    /// No key to route by: goes to the seed, same as `multimap_size`.
    pub(crate) async fn multimap_contains_value(
        &self,
        value: &[u8],
        supports_duplicates: bool,
    ) -> Result<bool> {
        let op = Operation::MultimapContainsValue(value.to_vec(), supports_duplicates);
        match self.call_seed(op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => {
                unreachable!("Operation::MultimapContainsValue always yields OperationResult::Bool")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap as StdHashMap;
    use std::sync::RwLock;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::client::tests::{read_request_opcode, response_header, unreachable_addr};
    use crate::client::ClusterTopology;
    use crate::remote_cache::tests::{client_with_seeds, set_topology};
    use crate::topology::TopologyServer;
    use crate::varint::write_vint;
    use crate::wire::{read_array, write_array};

    /// Every other operation family dispatched through `call` has at
    /// least one fast test exercising `run_operation`'s match arm for
    /// it (`contains_key_routes_to_the_computed_owner` in
    /// `remote_cache.rs`'s own tests is the representative one for
    /// every keyed operation). Multimap had none: its nine
    /// `Operation`/`OperationResult` variants were only ever reached
    /// by the wire-level tests in `connection::multimap` (which call
    /// `HotRodConnection::multimap_get` directly, bypassing
    /// `run_operation` entirely) and by the live-server test gated
    /// behind `#[ignore]`. A copy-paste mistake in `run_operation`'s
    /// multimap arms, such as returning the wrong `OperationResult`
    /// variant or dispatching to the wrong connection method, would
    /// pass every local `cargo test` run and only surface once a real
    /// server was involved. This test closes that gap for the keyed
    /// half of multimap's operations; the one below covers the two
    /// that target the seed instead.
    #[tokio::test]
    async fn multimap_get_routes_to_the_computed_owner() {
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
            assert_eq!(opcode, 0x67, "expected a MultimapGet request");
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, b"key");
            let supports_duplicates = stream.read_u8().await.unwrap();
            assert_eq!(supports_duplicates, 1);
            let mut resp = response_header(id, 0x68, 0x00);
            write_vint(&mut resp, 1);
            write_array(&mut resp, b"value1");
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
            .multimap_cache("my-multimap", true)
            .get(b"key")
            .await
            .expect("multimap get against owner");
        assert_eq!(result, vec![b"value1".to_vec()]);

        owner_task.await.unwrap();
    }

    /// `multimap_size` and `multimap_contains_value` have no key to
    /// route by, the same reason `size`/`clear`/`ping`/`stats` always
    /// target the seed. See the comment on
    /// `multimap_get_routes_to_the_computed_owner` for why this exists
    /// despite `connection::multimap`'s own wire-level multimap tests.
    #[tokio::test]
    async fn multimap_size_and_contains_value_always_target_the_seed() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x71, "expected a MultimapSize request");
            let supports_duplicates = stream.read_u8().await.unwrap();
            assert_eq!(supports_duplicates, 0);
            let mut resp = response_header(id, 0x72, 0x00);
            write_vint(&mut resp, 7);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x77, "expected a MultimapContainsValue request");
            let time_units = stream.read_u8().await.unwrap();
            assert_eq!(
                time_units, 0x88,
                "both lifespan and max idle infinite (unit nibble 8 each)"
            );
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(value, b"value");
            let supports_duplicates = stream.read_u8().await.unwrap();
            assert_eq!(supports_duplicates, 0);
            let mut resp = response_header(id, 0x78, 0x00);
            resp.push(1);
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
        let multimap = client.multimap_cache("my-multimap", false);

        assert_eq!(multimap.size().await.expect("multimap size"), 7);
        assert!(multimap
            .contains_value(b"value")
            .await
            .expect("multimap contains_value"));

        seed_task.await.unwrap();
    }
}
