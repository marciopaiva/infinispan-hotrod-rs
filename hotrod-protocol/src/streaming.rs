//! Streaming reads and writes for values too large to buffer whole, per
//! `docs/adr/0008-streaming.md` (issue #52).
//!
//! `GetStream`/`PutStream` each hold a `PooledGuard` (the same type
//! `RemoteCache`'s ordinary operations borrow and return per call) for
//! their whole lifetime instead of per call: the server scopes a
//! stream's id to the one connection that opened it (confirmed against
//! `GetStreamNextOperation`/`PutStreamNextOperation` in the Java
//! client, which refuse to run on any other channel), so every
//! `next_chunk`/`write_chunk`/`close`/`finish` call on one of these
//! types must reuse that exact connection, never check out a fresh one
//! from the pool.
//!
//! No automatic reconnection and no failover: like `CacheListener`
//! (#4), these never go through `RemoteCache::call`'s retry machinery.
//! If dropped without an explicit `close`/`finish` first, the
//! underlying connection is marked poisoned before it returns to the
//! pool: the stream state left behind on the server is scoped to that
//! connection, and this client has no way to tell the server to clean
//! it up without already holding a `&mut` to it, so the safe default
//! is to never let the pool hand that connection to an unrelated
//! caller afterward.

use std::time::SystemTime;

use crate::connection::{StreamStart, VersionedResult};
use crate::error::Result;
use crate::pool::PooledGuard;
use crate::wire::Expiration;

/// A value being read in chunks instead of buffered whole, from
/// `RemoteCache::get_stream`. Carries the same metadata
/// `VersionedValue` does, since the server returns it at the same
/// point (`GetStreamStart`'s response).
pub struct GetStream {
    conn: PooledGuard,
    stream_id: i32,
    complete: bool,
    pending_first_chunk: Option<Vec<u8>>,
    closed: bool,
    pub version: u64,
    pub created: Option<SystemTime>,
    pub lifespan: Expiration,
    pub last_used: Option<SystemTime>,
    pub max_idle: Expiration,
}

impl GetStream {
    pub(crate) fn new(conn: PooledGuard, start: StreamStart) -> Self {
        Self {
            conn,
            stream_id: start.stream_id,
            complete: start.complete,
            pending_first_chunk: Some(start.chunk),
            closed: false,
            version: start.version,
            created: start.created,
            lifespan: start.lifespan,
            last_used: start.last_used,
            max_idle: start.max_idle,
        }
    }

    /// Returns the next chunk, or `None` once the value is exhausted.
    /// The first call returns the chunk `GetStreamStart`'s own response
    /// already carried, with no network round trip.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>> {
        if let Some(chunk) = self.pending_first_chunk.take() {
            return Ok(Some(chunk));
        }
        if self.complete {
            return Ok(None);
        }
        let (complete, chunk) = self.conn.get_stream_next(self.stream_id).await?;
        self.complete = complete;
        Ok(Some(chunk))
    }

    /// Ends the stream explicitly. Only needed if the caller stops
    /// before `next_chunk` reports the value exhausted on its own: the
    /// server has already cleaned up a stream that reached that point,
    /// so closing it too would just be a wasted round trip (`Drop`
    /// knows this too, see its own docs below).
    pub async fn close(mut self) -> Result<()> {
        let result = self.conn.get_stream_end(self.stream_id).await;
        self.closed = true;
        result
    }
}

impl Drop for GetStream {
    /// Poisons the connection unless the stream was already exhausted
    /// or explicitly closed: either of those means the server has
    /// already cleaned up its side, so the connection is perfectly
    /// reusable. Anything else, including a cancelled `next_chunk`
    /// (`closed` stays `false` if the future is dropped before it
    /// finishes, the same cancellation-safety shape
    /// `NearCachedCache::invalidate_after` uses), poisons it instead of
    /// risking a future caller reusing a connection the server still
    /// thinks has an open stream on it.
    fn drop(&mut self) {
        if !self.closed && !self.complete {
            self.conn.mark_poisoned();
        }
    }
}

/// A value being written in chunks instead of buffered whole, from
/// `RemoteCache::put_stream`/`put_stream_if_absent`/
/// `replace_stream_with_version`.
pub struct PutStream {
    conn: PooledGuard,
    stream_id: i32,
    chunk_size: usize,
    buffer: Vec<u8>,
    finished: bool,
}

impl PutStream {
    pub(crate) fn new(conn: PooledGuard, stream_id: i32, chunk_size: usize) -> Self {
        Self {
            conn,
            stream_id,
            chunk_size: chunk_size.max(1),
            buffer: Vec::new(),
            finished: false,
        }
    }

    /// Buffers `bytes`, flushing to the server in `chunk_size`-sized
    /// pieces as the buffer fills. Never commits anything server-side
    /// on its own, no matter how much has been flushed: only `finish`
    /// does, by marking the final chunk complete (the protocol carries
    /// no total size up front for the server to commit against
    /// otherwise).
    pub async fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.buffer.extend_from_slice(bytes);
        while self.buffer.len() >= self.chunk_size {
            let chunk: Vec<u8> = self.buffer.drain(..self.chunk_size).collect();
            self.conn
                .put_stream_next(self.stream_id, &chunk, false)
                .await?;
        }
        Ok(())
    }

    /// Sends whatever is left buffered as the final chunk, committing
    /// the write subject to whatever conditional `version` the
    /// `RemoteCache` method that opened this stream passed through to
    /// `put_stream_start` (plain success for `put_stream`, whether the
    /// key was absent for `put_stream_if_absent`, or the usual
    /// `VersionedResult` for `replace_stream_with_version`).
    pub async fn finish(mut self) -> Result<VersionedResult> {
        let chunk = std::mem::take(&mut self.buffer);
        let result = self
            .conn
            .put_stream_next(self.stream_id, &chunk, true)
            .await;
        self.finished = true;
        result
    }

    /// Abandons the write, telling the server to clean the stream up
    /// rather than leaving `Drop` to poison the connection over it.
    /// Only useful when the caller decides partway through not to
    /// finish after all; calling `finish` is enough on its own
    /// otherwise, since the server only needs this explicit `End` for
    /// a stream nothing else resolved.
    pub async fn abandon(mut self) -> Result<()> {
        let result = self.conn.put_stream_end(self.stream_id).await;
        self.finished = true;
        result
    }
}

impl Drop for PutStream {
    /// Poisons the connection unless `finish` or `abandon` already ran
    /// to completion: anything else, including a cancelled
    /// `write_chunk` or a cancelled `finish`/`abandon` itself
    /// (`finished` stays `false` if that future is dropped before it
    /// resolves), leaves the write uncommitted and the server-side
    /// stream state unresolved, so the connection must not be handed
    /// to an unrelated caller afterward.
    fn drop(&mut self) {
        if !self.finished {
            self.conn.mark_poisoned();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::client::tests::{read_request_opcode, response_header};
    use crate::client::HotRodClient;
    use crate::remote_cache::tests::client_with_seeds_and_timeout;
    use crate::wire::{read_array, write_array};

    fn client_with_seed(seed_addr: SocketAddr) -> HotRodClient {
        client_with_seeds_and_timeout(vec![seed_addr], seed_addr, Duration::from_secs(5))
    }

    /// Reads a `GetStreamNext`/`GetStreamEnd`/`PutStreamStart`/
    /// `PutStreamNext`/`PutStreamEnd` request far enough to confirm its
    /// opcode and stream id; those bodies are already covered byte for
    /// byte in `connection.rs`'s own tests, so these only need to drive
    /// the fake server far enough to keep `GetStream`/`PutStream`'s own
    /// behavior (chunk assembly, poisoning) honest.
    async fn read_stream_id(sock: &mut tokio::net::TcpStream, expected_opcode: u8) -> (u64, i32) {
        let (id, opcode) = read_request_opcode(sock).await;
        assert_eq!(opcode, expected_opcode);
        (id, sock.read_i32().await.unwrap())
    }

    #[tokio::test]
    async fn get_stream_reads_chunks_until_complete_without_poisoning_the_connection() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = tcp.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0xE9, "expected a GetStreamStart request");
            let _key = read_array(&mut sock).await.unwrap();
            let _batch_size = crate::varint::read_vint(&mut sock).await.unwrap();
            let mut resp = response_header(id, 0xE8, 0x00);
            resp.extend_from_slice(&1i32.to_be_bytes());
            resp.push(0); // complete: false
            resp.push(0x03); // immortal lifespan and max idle
            resp.extend_from_slice(&0u64.to_be_bytes()); // version
            write_array(&mut resp, b"first-");
            sock.write_all(&resp).await.unwrap();

            let (id, stream_id) = read_stream_id(&mut sock, 0xE7).await;
            assert_eq!(stream_id, 1);
            let mut resp = response_header(id, 0xE6, 0x00);
            resp.extend_from_slice(&1i32.to_be_bytes());
            resp.push(1); // complete: true
            write_array(&mut resp, b"second");
            sock.write_all(&resp).await.unwrap();

            // Proves the connection was not poisoned: reused directly
            // for a Ping, no second accept anywhere in this test.
            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(
                opcode, 0x17,
                "expected a Ping request on the same connection"
            );
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            crate::varint::write_vint(&mut resp, 0);
            sock.write_all(&resp).await.unwrap();
        });

        let client = client_with_seed(addr);
        let cache = client.cache("my-cache");
        let mut stream = cache
            .get_stream(b"key", 64)
            .await
            .expect("get_stream")
            .expect("entry exists");

        assert_eq!(stream.next_chunk().await.unwrap(), Some(b"first-".to_vec()));
        assert_eq!(stream.next_chunk().await.unwrap(), Some(b"second".to_vec()));
        assert_eq!(stream.next_chunk().await.unwrap(), None);
        drop(stream);

        cache
            .ping()
            .await
            .expect("ping should reuse the connection");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_stream_dropped_without_finishing_poisons_the_connection() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut first, _) = tcp.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut first).await;
            assert_eq!(opcode, 0xE9, "expected a GetStreamStart request");
            let _key = read_array(&mut first).await.unwrap();
            let _batch_size = crate::varint::read_vint(&mut first).await.unwrap();
            let mut resp = response_header(id, 0xE8, 0x00);
            resp.extend_from_slice(&1i32.to_be_bytes());
            resp.push(0); // complete: false, more chunks remain unread
            resp.push(0x03);
            resp.extend_from_slice(&0u64.to_be_bytes());
            write_array(&mut resp, b"first-");
            first.write_all(&resp).await.unwrap();

            // The stream is dropped here without reading the rest or
            // closing it: a second, fresh connection is the only way a
            // following Ping can succeed if the first was poisoned.
            let (mut second, _) = tcp.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut second).await;
            assert_eq!(opcode, 0x17, "expected a Ping request on a new connection");
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            crate::varint::write_vint(&mut resp, 0);
            second.write_all(&resp).await.unwrap();
        });

        let client = client_with_seed(addr);
        let cache = client.cache("my-cache");
        let stream = cache
            .get_stream(b"key", 64)
            .await
            .expect("get_stream")
            .expect("entry exists");
        drop(stream);

        cache
            .ping()
            .await
            .expect("ping should open a new connection, not reuse the poisoned one");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_stream_close_ends_the_stream_and_does_not_poison_the_connection() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = tcp.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0xE9, "expected a GetStreamStart request");
            let _key = read_array(&mut sock).await.unwrap();
            let _batch_size = crate::varint::read_vint(&mut sock).await.unwrap();
            let mut resp = response_header(id, 0xE8, 0x00);
            resp.extend_from_slice(&1i32.to_be_bytes());
            resp.push(0); // complete: false
            resp.push(0x03);
            resp.extend_from_slice(&0u64.to_be_bytes());
            write_array(&mut resp, b"first-");
            sock.write_all(&resp).await.unwrap();

            let (id, stream_id) = read_stream_id(&mut sock, 0xE5).await;
            assert_eq!(stream_id, 1);
            let resp = response_header(id, 0xE4, 0x00);
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(
                opcode, 0x17,
                "expected a Ping request on the same connection"
            );
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            crate::varint::write_vint(&mut resp, 0);
            sock.write_all(&resp).await.unwrap();
        });

        let client = client_with_seed(addr);
        let cache = client.cache("my-cache");
        let mut stream = cache
            .get_stream(b"key", 64)
            .await
            .expect("get_stream")
            .expect("entry exists");
        assert_eq!(stream.next_chunk().await.unwrap(), Some(b"first-".to_vec()));
        stream.close().await.expect("close");

        cache
            .ping()
            .await
            .expect("ping should reuse the connection");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn put_stream_flushes_at_chunk_size_and_finish_commits_the_rest() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = tcp.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0xEF, "expected a PutStreamStart request");
            let _key = read_array(&mut sock).await.unwrap();
            let _time_units = sock.read_u8().await.unwrap();
            let _version = sock.read_i64().await.unwrap();
            let mut resp = response_header(id, 0xEE, 0x00);
            resp.extend_from_slice(&5i32.to_be_bytes());
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0xED, "expected the first PutStreamNext request");
            let stream_id = sock.read_i32().await.unwrap();
            assert_eq!(stream_id, 5);
            let complete = sock.read_u8().await.unwrap();
            assert_eq!(complete, 0, "the buffer just filled, not finished yet");
            let chunk = read_array(&mut sock).await.unwrap();
            assert_eq!(chunk, b"abcd");
            let resp = response_header(id, 0xEC, 0x00);
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0xED, "expected the final PutStreamNext request");
            let stream_id = sock.read_i32().await.unwrap();
            assert_eq!(stream_id, 5);
            let complete = sock.read_u8().await.unwrap();
            assert_eq!(
                complete, 1,
                "expected finish to mark the last chunk complete"
            );
            let chunk = read_array(&mut sock).await.unwrap();
            assert_eq!(chunk, b"ef");
            let resp = response_header(id, 0xEC, 0x00);
            sock.write_all(&resp).await.unwrap();
        });

        let client = client_with_seed(addr);
        let cache = client.cache("my-cache");
        let mut stream = cache
            .put_stream(b"key", Expiration::Default, Expiration::Default, 4)
            .await
            .expect("put_stream");
        stream.write_chunk(b"abcdef").await.expect("write_chunk");
        let result = stream.finish().await.expect("finish");
        assert_eq!(result, VersionedResult::Success);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn put_stream_dropped_without_finishing_poisons_the_connection() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut first, _) = tcp.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut first).await;
            assert_eq!(opcode, 0xEF, "expected a PutStreamStart request");
            let _key = read_array(&mut first).await.unwrap();
            let _time_units = first.read_u8().await.unwrap();
            let _version = first.read_i64().await.unwrap();
            let mut resp = response_header(id, 0xEE, 0x00);
            resp.extend_from_slice(&5i32.to_be_bytes());
            first.write_all(&resp).await.unwrap();

            // Dropped here without a single write_chunk/finish/abandon
            // call; a following Ping must open a new connection.
            let (mut second, _) = tcp.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut second).await;
            assert_eq!(opcode, 0x17, "expected a Ping request on a new connection");
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            crate::varint::write_vint(&mut resp, 0);
            second.write_all(&resp).await.unwrap();
        });

        let client = client_with_seed(addr);
        let cache = client.cache("my-cache");
        let stream = cache
            .put_stream(b"key", Expiration::Default, Expiration::Default, 4)
            .await
            .expect("put_stream");
        drop(stream);

        cache
            .ping()
            .await
            .expect("ping should open a new connection, not reuse the poisoned one");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn put_stream_abandon_ends_the_stream_and_does_not_poison_the_connection() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = tcp.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0xEF, "expected a PutStreamStart request");
            let _key = read_array(&mut sock).await.unwrap();
            let _time_units = sock.read_u8().await.unwrap();
            let _version = sock.read_i64().await.unwrap();
            let mut resp = response_header(id, 0xEE, 0x00);
            resp.extend_from_slice(&5i32.to_be_bytes());
            sock.write_all(&resp).await.unwrap();

            let (id, stream_id) = read_stream_id(&mut sock, 0xEB).await;
            assert_eq!(stream_id, 5);
            let resp = response_header(id, 0xEA, 0x00);
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(
                opcode, 0x17,
                "expected a Ping request on the same connection"
            );
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            crate::varint::write_vint(&mut resp, 0);
            sock.write_all(&resp).await.unwrap();
        });

        let client = client_with_seed(addr);
        let cache = client.cache("my-cache");
        let stream = cache
            .put_stream(b"key", Expiration::Default, Expiration::Default, 4)
            .await
            .expect("put_stream");
        stream.abandon().await.expect("abandon");

        cache
            .ping()
            .await
            .expect("ping should reuse the connection");

        server.await.unwrap();
    }
}
