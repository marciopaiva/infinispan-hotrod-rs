//! Streaming large values in chunks instead of buffering them whole
//! (`docs/adr/0008-streaming.md`).
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): see `connection/multimap.rs`'s module
//! docs for the mechanism that lets this `impl HotRodConnection`
//! block reach every private field/method it needs with no
//! visibility change. `read_entry_metadata`/`versioned_result` stay
//! at `connection.rs`'s root since the core `get_with_version` path
//! and server-side iteration also depend on them.

use std::time::SystemTime;

use crate::error::Result;
use crate::header::OpCode;
use crate::varint::write_vint;
use crate::wire::{read_array, write_array, write_expiration_params, Expiration};

use super::{versioned_result, with_timeout, HotRodConnection, VersionedResult};

/// What `get_stream_start` returns on a hit: the stream handle
/// (`stream_id`) a matching `get_stream_next`/`get_stream_end` must
/// reuse, whether the whole value already fit in this one response
/// (`complete`), the same metadata `VersionedValue` carries, and the
/// first chunk itself, since `GetStreamStart`'s response carries data,
/// not just a handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamStart {
    pub stream_id: i32,
    pub complete: bool,
    pub version: u64,
    pub created: Option<SystemTime>,
    pub lifespan: Expiration,
    pub last_used: Option<SystemTime>,
    pub max_idle: Expiration,
    pub chunk: Vec<u8>,
}

impl HotRodConnection {
    /// Opens a stream to read `key`'s value in chunks of up to
    /// `batch_size` bytes instead of buffering it whole, per
    /// `docs/adr/0008-streaming.md`. `None` on a miss, the same as
    /// `get`. The response already carries the first chunk (and may
    /// already be `complete` if the whole value fit), not just a
    /// handle: see `StreamStart`.
    pub(crate) async fn get_stream_start(
        &mut self,
        key: &[u8],
        batch_size: u32,
    ) -> Result<Option<StreamStart>> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            write_vint(&mut body, batch_size);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::GetStreamStart, &body)
                .await?;
            if header.status.is_not_exist() {
                return Ok(None);
            }
            let stream_id = tokio::io::AsyncReadExt::read_i32(&mut self.stream).await?;
            let complete = tokio::io::AsyncReadExt::read_u8(&mut self.stream).await? != 0;
            let (created, lifespan, last_used, max_idle, version) =
                self.read_entry_metadata().await?;
            let chunk = read_array(&mut self.stream).await?;
            Ok(Some(StreamStart {
                stream_id,
                complete,
                version,
                created,
                lifespan,
                last_used,
                max_idle,
                chunk,
            }))
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Reads the next chunk of a stream `get_stream_start` opened on
    /// this same connection. Returns `(complete, chunk)`.
    pub(crate) async fn get_stream_next(&mut self, stream_id: i32) -> Result<(bool, Vec<u8>)> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            body.extend_from_slice(&stream_id.to_be_bytes());
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::GetStreamNext, &body)
                .await?;
            // The echoed id is not re-checked here: this stream is
            // always read from the one connection that opened it (see
            // the ADR), so there is nothing for it to have desynced
            // against.
            let _echoed_id = tokio::io::AsyncReadExt::read_i32(&mut self.stream).await?;
            let complete = tokio::io::AsyncReadExt::read_u8(&mut self.stream).await? != 0;
            let chunk = read_array(&mut self.stream).await?;
            Ok((complete, chunk))
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Closes a stream `get_stream_start` opened on this same
    /// connection before it ran to completion on its own. Not needed
    /// (and not sent) once a `get_stream_next` has already reported
    /// `complete`.
    pub(crate) async fn get_stream_end(&mut self, stream_id: i32) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            body.extend_from_slice(&stream_id.to_be_bytes());
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::GetStreamEnd, &body)
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Opens a stream to write `key`'s value in chunks, per
    /// `docs/adr/0008-streaming.md`. `version` selects which write
    /// this commits to once the stream completes: `0` for an
    /// unconditional put, `-1` for put-if-absent, or a real version
    /// from `get_with_version` for a conditional replace. Returns the
    /// stream handle later `put_stream_next`/`put_stream_end` calls on
    /// this same connection must reuse.
    pub(crate) async fn put_stream_start(
        &mut self,
        key: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
        version: i64,
    ) -> Result<i32> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            write_expiration_params(&mut body, lifespan, max_idle);
            body.extend_from_slice(&version.to_be_bytes());
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::PutStreamStart, &body)
                .await?;
            let stream_id = tokio::io::AsyncReadExt::read_i32(&mut self.stream).await?;
            Ok(stream_id)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Sends one chunk of a stream `put_stream_start` opened on this
    /// same connection. The server only performs the write (subject to
    /// whatever `version` `put_stream_start` passed) once `complete`
    /// is `true`; no total size is announced up front, so this is the
    /// only way the server learns the value is finished.
    pub(crate) async fn put_stream_next(
        &mut self,
        stream_id: i32,
        chunk: &[u8],
        complete: bool,
    ) -> Result<VersionedResult> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            body.extend_from_slice(&stream_id.to_be_bytes());
            body.push(complete as u8);
            write_array(&mut body, chunk);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::PutStreamNext, &body)
                .await?;
            Ok(versioned_result(header.status))
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Closes a stream `put_stream_start` opened on this same
    /// connection before it ran to completion (a `put_stream_next`
    /// with `complete: true`) on its own. Never needed in the happy
    /// path: only to abandon a stream cleanly.
    pub(crate) async fn put_stream_end(&mut self, stream_id: i32) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            body.extend_from_slice(&stream_id.to_be_bytes());
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::PutStreamEnd, &body)
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::connection::tests::{read_request, response_header};
    use crate::varint::read_vint;

    /// `GetStreamStart`'s response carries the stream id, `complete`,
    /// the same metadata block `get_with_version` reads, and the first
    /// chunk itself, all in one round trip; this exercises every field
    /// at once the way `get_with_version_exposes_full_metadata` does
    /// for the plain metadata read.
    #[tokio::test]
    async fn get_stream_start_returns_the_stream_id_metadata_and_first_chunk() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0xE9, "expected a GetStreamStart request");
            let _key = read_array(&mut stream).await.unwrap();
            let batch_size = read_vint(&mut stream).await.unwrap();
            assert_eq!(batch_size, 64);

            let mut resp = response_header(id, 0xE8, 0x00);
            resp.extend_from_slice(&7i32.to_be_bytes()); // stream id
            resp.push(0); // complete: false, more chunks follow
            const INFINITE_MAXIDLE: u8 = 0x02;
            resp.push(INFINITE_MAXIDLE);
            resp.extend_from_slice(&1_700_000_000_000u64.to_be_bytes()); // creation
            write_vint(&mut resp, 100); // lifespan seconds
            resp.extend_from_slice(&42u64.to_be_bytes()); // version
            write_array(&mut resp, b"first-chunk");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let start = conn
            .get_stream_start(b"key", 64)
            .await
            .expect("get_stream_start")
            .expect("entry exists");

        assert_eq!(start.stream_id, 7);
        assert!(!start.complete);
        assert_eq!(start.version, 42);
        assert_eq!(start.lifespan, Expiration::Seconds(100));
        assert_eq!(start.max_idle, Expiration::Immortal);
        assert_eq!(start.chunk, b"first-chunk");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_stream_start_returns_none_on_a_miss() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0xE9, "expected a GetStreamStart request");
            let _key = read_array(&mut stream).await.unwrap();
            let _batch_size = read_vint(&mut stream).await.unwrap();
            let resp = response_header(id, 0xE8, 0x02); // KEY_DOES_NOT_EXIST
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let result = conn.get_stream_start(b"key", 64).await.expect("no error");
        assert!(result.is_none());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_stream_next_returns_the_chunk_and_complete_flag() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0xE7, "expected a GetStreamNext request");
            let stream_id = tokio::io::AsyncReadExt::read_i32(&mut stream)
                .await
                .unwrap();
            assert_eq!(stream_id, 7);

            let mut resp = response_header(id, 0xE6, 0x00);
            resp.extend_from_slice(&7i32.to_be_bytes()); // echoed id
            resp.push(1); // complete: true
            write_array(&mut resp, b"last-chunk");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let (complete, chunk) = conn.get_stream_next(7).await.expect("get_stream_next");
        assert!(complete);
        assert_eq!(chunk, b"last-chunk");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_stream_end_sends_the_stream_id() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0xE5, "expected a GetStreamEnd request");
            let stream_id = tokio::io::AsyncReadExt::read_i32(&mut stream)
                .await
                .unwrap();
            assert_eq!(stream_id, 7);
            let resp = response_header(id, 0xE4, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        conn.get_stream_end(7).await.expect("get_stream_end");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn put_stream_start_sends_expiration_and_version_and_returns_the_stream_id() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0xEF, "expected a PutStreamStart request");
            let _key = read_array(&mut stream).await.unwrap();
            let _time_units = stream.read_u8().await.unwrap();
            let lifespan = read_vint(&mut stream).await.unwrap();
            assert_eq!(lifespan, 100);
            let version = tokio::io::AsyncReadExt::read_i64(&mut stream)
                .await
                .unwrap();
            assert_eq!(version, -1, "expected the put-if-absent sentinel");

            let mut resp = response_header(id, 0xEE, 0x00);
            resp.extend_from_slice(&9i32.to_be_bytes()); // stream id
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let stream_id = conn
            .put_stream_start(b"key", Expiration::Seconds(100), Expiration::Immortal, -1)
            .await
            .expect("put_stream_start");
        assert_eq!(stream_id, 9);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn put_stream_next_sends_the_chunk_and_complete_flag() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0xED, "expected a PutStreamNext request");
            let stream_id = tokio::io::AsyncReadExt::read_i32(&mut stream)
                .await
                .unwrap();
            assert_eq!(stream_id, 9);
            let complete = stream.read_u8().await.unwrap();
            assert_eq!(complete, 1, "expected the final, complete chunk");
            let chunk = read_array(&mut stream).await.unwrap();
            assert_eq!(chunk, b"last-chunk");

            let resp = response_header(id, 0xEC, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let result = conn
            .put_stream_next(9, b"last-chunk", true)
            .await
            .expect("put_stream_next");
        assert_eq!(result, VersionedResult::Success);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn put_stream_end_sends_the_stream_id() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0xEB, "expected a PutStreamEnd request");
            let stream_id = tokio::io::AsyncReadExt::read_i32(&mut stream)
                .await
                .unwrap();
            assert_eq!(stream_id, 9);
            let resp = response_header(id, 0xEA, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        conn.put_stream_end(9).await.expect("put_stream_end");

        server.await.unwrap();
    }
}
