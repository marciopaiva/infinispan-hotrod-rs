//! The plain per-entry cache operations: get, put, remove, replace,
//! the versioned variants, contains_key, ping, size, clear, stats.
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): see `connection/multimap.rs`'s module
//! docs for the mechanism that lets this `impl HotRodConnection`
//! block reach every private field/method it needs with no
//! visibility change. `key_value_body`/`versioned_result`/
//! `read_entry_metadata`/`read_versioned_value` stay at `connection.rs`'s
//! root rather than moving here, since `multimap.rs`/`iteration.rs`/
//! `streaming.rs` need them too.

use std::collections::HashMap;

use crate::error::Result;
use crate::header::OpCode;
use crate::varint::read_vint;
use crate::wire::{
    read_array, read_string_map, skip_media_type, write_array, write_expiration_params, Expiration,
};

use super::{
    key_value_body, versioned_result, with_timeout, HotRodConnection, VersionedResult,
    VersionedValue,
};

impl HotRodConnection {
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::Get, &body)
                .await?;
            if header.status.is_not_exist() {
                return Ok(None);
            }
            Ok(Some(read_array(&mut self.stream).await?))
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    pub async fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let body = key_value_body(key, value, lifespan, max_idle);
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::Put, &body)
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Returns `true` if the entry was stored, `false` if the key already
    /// existed and nothing was changed.
    pub async fn put_if_absent(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let body = key_value_body(key, value, lifespan, max_idle);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::PutIfAbsent, &body)
                .await?;
            Ok(!header.status.is_not_executed())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Returns `true` if the key existed and was replaced, `false` if it
    /// did not exist.
    pub async fn replace(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let body = key_value_body(key, value, lifespan, max_idle);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::Replace, &body)
                .await?;
            Ok(!header.status.is_not_executed())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Returns `true` if the key existed and was removed, `false` if it did
    /// not exist.
    pub async fn remove(&mut self, key: &[u8]) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::Remove, &body)
                .await?;
            Ok(!header.status.is_not_exist())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Fetches a value together with its entry version, for use in a later
    /// `replace_if_unmodified` or `remove_if_unmodified` call, and the
    /// entry's full metadata (creation and last-used time, lifespan and
    /// max idle).
    pub async fn get_with_version(&mut self, key: &[u8]) -> Result<Option<VersionedValue>> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::GetWithMetadata, &body)
                .await?;
            if header.status.is_not_exist() {
                return Ok(None);
            }
            self.read_versioned_value().await.map(Some)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Replaces the value only if the entry's current version still matches
    /// `version` (obtained from `get_with_version`).
    pub async fn replace_if_unmodified(
        &mut self,
        key: &[u8],
        value: &[u8],
        version: u64,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<VersionedResult> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            write_expiration_params(&mut body, lifespan, max_idle);
            body.extend_from_slice(&version.to_be_bytes());
            write_array(&mut body, value);

            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::ReplaceIfUnmodified, &body)
                .await?;
            Ok(versioned_result(header.status))
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Removes the entry only if its current version still matches
    /// `version` (obtained from `get_with_version`).
    pub async fn remove_if_unmodified(
        &mut self,
        key: &[u8],
        version: u64,
    ) -> Result<VersionedResult> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            body.extend_from_slice(&version.to_be_bytes());

            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::RemoveIfUnmodified, &body)
                .await?;
            Ok(versioned_result(header.status))
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Checks the server is reachable and the connection's handshake still
    /// holds, without touching any cache entry.
    ///
    /// The response body is not a bare status byte: it carries a media
    /// type pair, the server's protocol version and its supported opcodes
    /// (mirroring `NoCachePingOperation`/`PingResponse`). None of that is
    /// exposed yet, since phase 1 does not negotiate media types or codec
    /// versions, but it is still read off the wire to keep the stream in
    /// sync for the next request.
    pub async fn ping(&mut self) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::Ping, &[])
                .await?;
            skip_media_type(&mut self.stream).await?; // key media type
            skip_media_type(&mut self.stream).await?; // value media type
            let _server_version = tokio::io::AsyncReadExt::read_u8(&mut self.stream).await?;
            let server_ops_count = read_vint(&mut self.stream).await?;
            for _ in 0..server_ops_count {
                let _opcode = tokio::io::AsyncReadExt::read_u16(&mut self.stream).await?;
            }
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// The number of entries in the cache. The server computes this
    /// cluster-wide from a single request; the client does no fan-out of
    /// its own.
    pub async fn size(&mut self) -> Result<u32> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::Size, &[])
                .await?;
            read_vint(&mut self.stream).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Removes every entry from the cache, cluster-wide. Like `size`, the
    /// server fans this out itself.
    pub async fn clear(&mut self) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::Clear, &[])
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Returns `true` if `key` exists in the cache.
    ///
    /// Mirrors `ContainsKeyOperation`: `is_success` and `!is_not_exist`
    /// are checked separately, rather than folded into `!is_not_exist`
    /// alone as `remove` does, to match the Java client's own logic
    /// exactly even though the two statuses this operation can return
    /// never make them disagree today.
    pub async fn contains_key(&mut self, key: &[u8]) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::ContainsKey, &body)
                .await?;
            Ok(header.status.is_success() && !header.status.is_not_exist())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Per-node statistics as name/value string pairs
    /// (`StatsOperation.createResponse`). Not aggregated across the
    /// cluster by the protocol: it reflects only the node this connection
    /// is open to.
    pub async fn stats(&mut self) -> Result<HashMap<String, String>> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::Stats, &[])
                .await?;
            read_string_map(&mut self.stream).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use crate::connection::tests::{read_request, response_header};
    use crate::varint::write_vint;
    use crate::wire::{read_array, write_array, Expiration};

    use super::HotRodConnection;

    /// The ping response body is more than a status byte: a media type
    /// pair, the server's protocol version and its supported opcodes. This
    /// sends back a non-trivial one (a predefined media type with a
    /// parameter, a custom media type, two supported opcodes) and then
    /// serves a `get` on the same connection, so a `get` failing to parse
    /// correctly would prove `ping` left the stream out of sync.
    #[tokio::test]
    async fn ping_consumes_the_full_response_body_and_leaves_the_stream_in_sync() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();

            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x17, "expected a Ping request");
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(1); // key media type: predefined
            write_vint(&mut resp, 42); // media type id
            write_vint(&mut resp, 1); // one parameter
            write_array(&mut resp, b"charset");
            write_array(&mut resp, b"utf-8");
            resp.push(2); // value media type: custom
            write_array(&mut resp, b"application/x-custom");
            write_vint(&mut resp, 0); // no parameters
            resp.push(41); // server protocol version
            write_vint(&mut resp, 2); // two supported opcodes
            resp.extend_from_slice(&0x03u16.to_be_bytes());
            resp.extend_from_slice(&0x04u16.to_be_bytes());
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(
                opcode, 0x03,
                "expected a Get request, proving ping left the stream in sync"
            );
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x04, 0x02); // KEY_DOES_NOT_EXIST
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        conn.ping().await.expect("ping");
        let result = conn.get(b"key").await.expect("get after ping");
        assert_eq!(result, None);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn size_returns_the_count_the_server_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x29, "expected a Size request");
            let mut resp = response_header(id, 0x2A, 0x00);
            write_vint(&mut resp, 7);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let size = conn.size().await.expect("size");
        assert_eq!(size, 7);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn clear_completes_on_a_bare_success_status() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x13, "expected a Clear request");
            let resp = response_header(id, 0x14, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        conn.clear().await.expect("clear");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn contains_key_is_true_when_the_server_reports_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x0F, "expected a ContainsKey request");
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x10, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        assert!(conn.contains_key(b"key").await.expect("contains_key"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn contains_key_is_false_when_the_key_does_not_exist() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x0F, "expected a ContainsKey request");
            let _key = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x10, 0x02); // KEY_DOES_NOT_EXIST
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        assert!(!conn.contains_key(b"key").await.expect("contains_key"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn stats_returns_the_pairs_the_server_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x15, "expected a Stats request");
            let mut resp = response_header(id, 0x16, 0x00);
            write_vint(&mut resp, 2);
            write_array(&mut resp, b"currentNumberOfEntries");
            write_array(&mut resp, b"3");
            write_array(&mut resp, b"timeSinceStart");
            write_array(&mut resp, b"120");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let stats = conn.stats().await.expect("stats");
        assert_eq!(stats.get("currentNumberOfEntries"), Some(&"3".to_string()));
        assert_eq!(stats.get("timeSinceStart"), Some(&"120".to_string()));
        assert_eq!(stats.len(), 2);

        server.await.unwrap();
    }

    /// One half of the entry has a finite duration, the other is immortal,
    /// so both branches of `read_versioned_value` run in the same test.
    #[tokio::test]
    async fn get_with_version_exposes_full_metadata() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x1B, "expected a GetWithMetadata request");
            let _key = read_array(&mut stream).await.unwrap();

            let mut resp = response_header(id, 0x1C, 0x00);
            const INFINITE_MAXIDLE: u8 = 0x02;
            resp.push(INFINITE_MAXIDLE); // finite lifespan, immortal max idle
            resp.extend_from_slice(&1_700_000_000_000u64.to_be_bytes()); // creation
            write_vint(&mut resp, 100); // lifespan seconds
            resp.extend_from_slice(&42u64.to_be_bytes()); // version
            write_array(&mut resp, b"value");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let result = conn
            .get_with_version(b"key")
            .await
            .expect("get_with_version")
            .expect("entry exists");

        assert_eq!(result.value, b"value");
        assert_eq!(result.version, 42);
        assert_eq!(
            result.created,
            Some(std::time::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000))
        );
        assert_eq!(result.lifespan, Expiration::Seconds(100));
        assert_eq!(result.last_used, None);
        assert_eq!(result.max_idle, Expiration::Immortal);

        server.await.unwrap();
    }
}
