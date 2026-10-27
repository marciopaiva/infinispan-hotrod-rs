//! Remote task execution: `EXEC_REQUEST`/`EXEC_RESPONSE`
//! (`docs/adr/0014-remote-administration.md`,
//! `docs/adr/0025-remote-task-execution.md`).
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): see `connection/multimap.rs`'s module
//! docs for the mechanism that lets this `impl HotRodConnection`
//! block reach every private field/method it needs with no
//! visibility change.

use crate::error::Result;
use crate::header::OpCode;
use crate::varint::write_vint;
use crate::wire::{read_array, write_array};

use super::{with_timeout, HotRodConnection};

impl HotRodConnection {
    /// Shared wire encoding for `EXEC_REQUEST`/`EXEC_RESPONSE`
    /// (`docs/adr/0014-remote-administration.md`,
    /// `docs/adr/0025-remote-task-execution.md`): task name, a vint
    /// count, then `(name, byte[])` pairs, with `cache_name` the only
    /// thing that differs between an admin task and a user-defined
    /// one (see the two callers below). Every parameter is sent as a
    /// plain byte array, never through a `Marshaller`: confirmed
    /// against the Java client's own `CacheExecuteOperation`/
    /// `NoCacheExecuteOperation`, which both hand the server
    /// already-marshalled `byte[]` values, not objects for this crate
    /// to marshal itself.
    async fn exec_request(
        &mut self,
        cache_name: &[u8],
        task_name: &str,
        params: &[(String, Vec<u8>)],
    ) -> Result<Vec<u8>> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, task_name.as_bytes());
            write_vint(&mut body, params.len() as u32);
            for (name, value) in params {
                write_array(&mut body, name.as_bytes());
                write_array(&mut body, value);
            }
            self.write_and_read_header(cache_name, OpCode::Exec, &body)
                .await?;
            read_array(&mut self.stream).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Executes a named task at the cache-manager level
    /// (`docs/adr/0014-remote-administration.md`): the mechanism
    /// every `@@cache@create`-style remote administration call in
    /// `admin.rs` builds on. Targets no specific cache, the same way
    /// `AuthMechList`/`Auth` already use an empty cache name rather
    /// than `self.cache_name`: the task runs at the cache-manager
    /// level, not against whichever cache this connection happens to
    /// be pooled for.
    pub(crate) async fn execute_task(
        &mut self,
        task_name: &str,
        params: &[(String, Vec<u8>)],
    ) -> Result<Vec<u8>> {
        self.exec_request(&[], task_name, params).await
    }

    /// Executes a user-defined named task against this connection's
    /// own cache (#51, `docs/adr/0025-remote-task-execution.md`),
    /// confirmed against the Java client's `CacheExecuteOperation` to
    /// be the only wire difference from `execute_task`: the real
    /// cache name on the header instead of an empty one.
    pub(crate) async fn execute_cache_task(
        &mut self,
        task_name: &str,
        params: &[(String, Vec<u8>)],
    ) -> Result<Vec<u8>> {
        let cache_name = self.cache_name.clone();
        self.exec_request(&cache_name, task_name, params).await
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use crate::connection::tests::{read_request, response_header};
    use crate::wire::{read_array, write_array};

    use super::HotRodConnection;

    #[tokio::test]
    async fn execute_task_sends_task_name_and_params_and_returns_the_response_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x2B, "expected an Exec request");
            let task_name = read_array(&mut stream).await.unwrap();
            assert_eq!(task_name, b"@@cache@create");
            let param_count = crate::varint::read_vint(&mut stream).await.unwrap();
            assert_eq!(param_count, 2);
            let name0 = read_array(&mut stream).await.unwrap();
            let value0 = read_array(&mut stream).await.unwrap();
            assert_eq!(name0, b"name");
            assert_eq!(value0, b"my-new-cache");
            let name1 = read_array(&mut stream).await.unwrap();
            let value1 = read_array(&mut stream).await.unwrap();
            assert_eq!(name1, b"template");
            assert_eq!(value1, b"org.infinispan.DIST_SYNC");

            let mut resp = response_header(id, 0x2C, 0x00);
            write_array(&mut resp, br#""ok""#);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let params = vec![
            ("name".to_string(), b"my-new-cache".to_vec()),
            ("template".to_string(), b"org.infinispan.DIST_SYNC".to_vec()),
        ];
        let response = conn
            .execute_task("@@cache@create", &params)
            .await
            .expect("execute_task");
        assert_eq!(response, br#""ok""#);

        server.await.unwrap();
    }

    /// `execute_task` runs at the cache-manager level, not against
    /// whichever cache this connection happens to be pooled for: it
    /// must declare an empty cache name on the wire even though this
    /// connection was opened against a real, non-empty one.
    #[tokio::test]
    async fn execute_task_declares_an_empty_cache_name_regardless_of_the_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            use tokio::io::AsyncReadExt;
            assert_eq!(stream.read_u8().await.unwrap(), 0xA0);
            let id = crate::varint::read_vlong(&mut stream).await.unwrap();
            let _version = stream.read_u8().await.unwrap();
            let opcode = stream.read_u8().await.unwrap();
            assert_eq!(opcode, 0x2B, "expected an Exec request");
            let cache_name = read_array(&mut stream).await.unwrap();
            assert!(
                cache_name.is_empty(),
                "exec runs at the cache-manager level, not against this connection's own cache"
            );
            let _flags = crate::varint::read_vint(&mut stream).await.unwrap();
            let _intelligence = stream.read_u8().await.unwrap();
            let _topology_id = crate::varint::read_vint(&mut stream).await.unwrap();
            crate::wire::skip_media_type(&mut stream).await.unwrap();
            crate::wire::skip_media_type(&mut stream).await.unwrap();
            let _additional_params = crate::varint::read_vint(&mut stream).await.unwrap();
            let _task_name = read_array(&mut stream).await.unwrap();
            let _param_count = crate::varint::read_vint(&mut stream).await.unwrap();

            let mut resp = response_header(id, 0x2C, 0x00);
            write_array(&mut resp, b"[]");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "a-real-cache-name")
            .await
            .expect("connect");
        conn.execute_task("@@cache@names", &[])
            .await
            .expect("execute_task");

        server.await.unwrap();
    }
}
