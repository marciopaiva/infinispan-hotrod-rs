//! `get_all`/`put_all`: multi-key requests in one round trip, always
//! against the seed connection (issue #41, revisited in issue #68).
//!
//! Split out of `remote_cache.rs`
//! (`docs/adr/0026-split-connection-and-remote-cache-by-domain.md`):
//! see `remote_cache/multimap.rs`'s module docs for the mechanism
//! that lets this `impl RemoteCache` block reach `call_seed` with no
//! visibility change.

use std::collections::HashMap;

use crate::error::Result;
use crate::wire::Expiration;

use super::{Operation, OperationResult, RemoteCache};

impl RemoteCache {
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap as StdHashMap;
    use std::sync::RwLock;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::client::tests::{read_request_opcode, response_header, unreachable_addr};
    use crate::client::ClusterTopology;
    use crate::remote_cache::tests::{client_with_seeds, set_topology};
    use crate::topology::TopologyServer;
    use crate::varint::{read_vint, write_vint};
    use crate::wire::{read_array, Expiration};

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
}
