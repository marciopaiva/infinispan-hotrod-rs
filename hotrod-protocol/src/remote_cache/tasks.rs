//! Remote task execution (#51, `docs/adr/0025-remote-task-execution.md`).
//!
//! Split out of `remote_cache.rs`
//! (`docs/adr/0026-split-connection-and-remote-cache-by-domain.md`):
//! see `remote_cache/multimap.rs`'s module docs for the mechanism
//! that lets this `impl RemoteCache` block reach `call`/`call_seed`
//! with no visibility change.

use crate::error::Result;

use super::{Operation, OperationResult, RemoteCache};

impl RemoteCache {
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

    /// Runs a server-side named task (#51,
    /// `docs/adr/0025-remote-task-execution.md`), unlike `run_exec`
    /// against this cache rather than at the cache-manager level: the
    /// request declares this cache's own name on the wire. No key to
    /// route by, so it goes to the active seed, the same as `query`.
    /// `params`' values are sent as plain byte arrays; the task's own
    /// contract decides how to interpret them, and the result comes
    /// back the same way, raw, for the caller to decode.
    pub async fn execute(
        &self,
        task_name: impl Into<String>,
        params: &[(String, Vec<u8>)],
    ) -> Result<Vec<u8>> {
        match self
            .call_seed(Operation::ExecCache(task_name.into(), params.to_vec()))
            .await?
        {
            OperationResult::ExecCache(result) => Ok(result),
            _ => unreachable!("Operation::ExecCache always yields OperationResult::ExecCache"),
        }
    }

    /// Runs a server-side named task routed to `key`'s owner (#51,
    /// `docs/adr/0025-remote-task-execution.md`). `key` is never sent
    /// as part of the request, confirmed against the Java client's
    /// own `CacheExecuteOperation.getRoutingObject()`: it only picks
    /// which server the task runs against, the same role an ordinary
    /// `get`/`put` key already plays for `call`.
    pub async fn execute_near(
        &self,
        task_name: impl Into<String>,
        params: &[(String, Vec<u8>)],
        key: &[u8],
    ) -> Result<Vec<u8>> {
        match self
            .call(key, Operation::ExecCache(task_name.into(), params.to_vec()))
            .await?
        {
            OperationResult::ExecCache(result) => Ok(result),
            _ => unreachable!("Operation::ExecCache always yields OperationResult::ExecCache"),
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
    use crate::varint::{read_vint, read_vlong};
    use crate::wire::{read_array, write_array};

    /// `execute` (#51, `docs/adr/0025-remote-task-execution.md`)
    /// declares this cache's own name on the wire, unlike the
    /// remote-administration tasks in `admin.rs`, which always
    /// declare an empty one. No key to route by, so it goes to the
    /// seed, same as `query`.
    #[tokio::test]
    async fn execute_sends_the_cache_name_task_name_and_params() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let magic = stream.read_u8().await.unwrap();
            assert_eq!(magic, 0xA0, "expected a request magic byte");
            let id = read_vlong(&mut stream).await.unwrap();
            let _version = stream.read_u8().await.unwrap();
            let opcode = stream.read_u8().await.unwrap();
            assert_eq!(opcode, 0x2B, "expected an Exec request");
            let cache_name = read_array(&mut stream).await.unwrap();
            assert_eq!(
                cache_name, b"my-cache",
                "execute must target the real cache, unlike an admin task"
            );
            let _flags = read_vint(&mut stream).await.unwrap();
            let _intelligence = stream.read_u8().await.unwrap();
            let _topology_id = read_vint(&mut stream).await.unwrap();
            crate::wire::skip_media_type(&mut stream).await.unwrap();
            crate::wire::skip_media_type(&mut stream).await.unwrap();
            let _additional_params = read_vint(&mut stream).await.unwrap();

            let task_name = read_array(&mut stream).await.unwrap();
            assert_eq!(task_name, b"my-task");
            let param_count = read_vint(&mut stream).await.unwrap();
            assert_eq!(param_count, 1);
            let name = read_array(&mut stream).await.unwrap();
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(name, b"greeting");
            assert_eq!(value, b"hello");

            let mut resp = response_header(id, 0x2C, 0x00);
            write_array(&mut resp, b"task result");
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let result = client
            .cache("my-cache")
            .execute("my-task", &[("greeting".to_string(), b"hello".to_vec())])
            .await
            .expect("execute should succeed");

        assert_eq!(result, b"task result");

        seed_task.await.unwrap();
    }

    /// `execute_near` (#51, `docs/adr/0025-remote-task-execution.md`)
    /// routes to the given key's owner, confirmed against the Java
    /// client's own `CacheExecuteOperation.getRoutingObject()`: the
    /// key never appears in the request body, only task name and
    /// params, the exact same encoding `execute` itself sends.
    #[tokio::test]
    async fn execute_near_routes_by_key_without_sending_it() {
        let owner_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let owner_addr = owner_listener.local_addr().unwrap();
        let owner_task = tokio::spawn(async move {
            let (mut stream, _) = owner_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x2B, "expected an Exec request");
            let task_name = read_array(&mut stream).await.unwrap();
            assert_eq!(task_name, b"near-task");
            let param_count = read_vint(&mut stream).await.unwrap();
            assert_eq!(
                param_count, 0,
                "the routing key must not be sent as a param"
            );

            let mut resp = response_header(id, 0x2C, 0x00);
            write_array(&mut resp, b"near result");
            stream.write_all(&resp).await.unwrap();
        });

        // Never bound to anything: if `execute_near` wrongly went to the
        // seed instead of the key's owner, this would fail fast with
        // `Error::Io` instead of the owner's response succeeding.
        let seed_addr = unreachable_addr().await;

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
            .execute_near("near-task", &[], b"routing-key")
            .await
            .expect("execute_near should succeed");

        assert_eq!(result, b"near result");

        owner_task.await.unwrap();
    }
}
