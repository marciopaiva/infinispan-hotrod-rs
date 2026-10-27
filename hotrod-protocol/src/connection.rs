//! A single sequential connection to one cache on a Hot Rod server.
//!
//! Phase 1 (ADR 0001) deliberately does not pipeline or multiplex requests:
//! each call writes a request and waits for its response before the next
//! call may proceed. There is one connection per `HotRodConnection`, no
//! pooling, no cluster topology awareness.
//!
//! Every connect and operation is bounded by a timeout (`DEFAULT_TIMEOUT`
//! unless overridden via `connect_with_timeout`), so a hung server or a
//! partitioned network cannot block a call forever. See `Error::Timeout`
//! for what a caller must do with the connection after one fires.
//!
//! That hazard is not specific to the internal timeout: Hot Rod's
//! protocol needs one full write-then-read cycle per request to stay in
//! sync, so dropping an operation's future before it resolves, for any
//! reason, can leave the connection with a partial frame in flight. A
//! caller's own `tokio::time::timeout` racing this client's, a `select!`
//! that resolves another branch first, or an aborted task all have the
//! same effect as `Error::Timeout`: the connection must be reconnected,
//! never reused.
//!
//! This is enforced, not just documented: every operation marks the
//! connection poisoned before it writes its request, and clears that mark
//! only once the response has been read in full. A future dropped before
//! that point leaves the mark set, and every later operation on the same
//! connection then fails fast with `Error::PoisonedConnection` instead of
//! touching an already desynced stream.

use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::BufStream;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::error::{Error, Result};
use crate::header::OpCode;
use crate::tls::{self, TlsConfig, Transport};
use crate::topology::{ClientIntelligence, TopologyUpdate};
use crate::varint::read_vint;
use crate::wire::{read_array, write_array, write_expiration_params, Expiration};

// Domain submodules (`docs/adr/0026-split-connection-and-remote-cache-
// by-domain.md`): each adds its own `impl HotRodConnection` block.
// Free to reach every private field/method defined in this root
// module (`self.stream`, `write_and_read_header`, `with_timeout`,
// `key_value_body`, ...) since Rust's privacy rule makes a private
// item visible to every descendant of its defining module, with no
// visibility change needed anywhere.
mod auth;
mod bulk;
mod core_ops;
mod counters;
mod iteration;
mod listeners;
mod multimap;
mod query;
mod streaming;
mod tasks;
mod transactions;

pub(crate) use streaming::StreamStart;

/// Sent on every request until the sender has topology awareness to report
/// a real one: `HotRodConnection` always sends this, and it's also what
/// `HotRodClient` starts a fresh pooled connection with before its first
/// topology update arrives. Encoded as `u32::from_ne_bytes` would not do
/// here: it must go through the same unsigned-vint path as any other
/// topology id (see `varint` module docs).
pub(crate) const DEFAULT_TOPOLOGY_ID: i32 = -1;

/// Bounds the initial TCP connect and the full write-then-read cycle of
/// every operation and authentication round, so a hung server or a
/// partitioned network cannot block a call forever. Override with
/// `connect_with_timeout` when a caller's network needs a tighter or
/// looser bound. See `Error::Timeout` for what happens to a connection
/// after one fires.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Races `fut` against `timeout`, turning an elapsed deadline into
/// `Error::Timeout` instead of leaving the caller to wait forever.
pub(crate) async fn with_timeout<T>(
    timeout: Duration,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result,
        Err(_elapsed) => Err(Error::Timeout(timeout)),
    }
}

/// The outcome of a versioned write (`replace_if_unmodified`,
/// `remove_if_unmodified`): whether the server's copy still had the version
/// the caller expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionedResult {
    /// The write went through.
    Success,
    /// The key exists, but its version had already changed.
    Stale,
    /// The key does not exist.
    NotFound,
}

/// A value together with the entry version needed to make a later
/// `replace_if_unmodified` or `remove_if_unmodified` call, plus the entry's
/// full metadata as returned by the server.
///
/// `created` and `last_used` are `None` exactly when `lifespan` and
/// `max_idle` respectively are `Expiration::Immortal`: the server never
/// sends a timestamp for a half of the entry that does not expire.
/// `Expiration::Default` is never produced here; it exists only for the
/// write side (`put` and friends).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub value: Vec<u8>,
    pub version: u64,
    pub created: Option<SystemTime>,
    pub lifespan: Expiration,
    pub last_used: Option<SystemTime>,
    pub max_idle: Expiration,
}

/// A single sequential connection to one cache on a Hot Rod server. See
/// the module docs above for the cancellation-safety and poisoning
/// rules every operation on this type follows; `HotRodClient`
/// (`client.rs`) is what most callers want instead, since it pools
/// several of these per node and tracks cluster topology on top.
pub struct HotRodConnection {
    stream: BufStream<Transport>,
    cache_name: Vec<u8>,
    next_message_id: u64,
    intelligence: ClientIntelligence,
    topology_id: i32,
    /// The topology update parsed from the most recent response, if any.
    /// Cleared by `take_pending_topology_update`.
    pending_topology_update: Option<TopologyUpdate>,
    timeout: Duration,
    /// Set before a request is written, cleared only once its response has
    /// been read in full. See the module docs for what this guards against.
    poisoned: bool,
}

impl HotRodConnection {
    /// Opens a TCP connection and targets the given cache. An empty
    /// `cache_name` targets the server's default cache. Uses
    /// `DEFAULT_TIMEOUT`; call `connect_with_timeout` for a different bound.
    pub async fn connect(addr: impl ToSocketAddrs, cache_name: &str) -> Result<Self> {
        Self::connect_with(
            addr,
            cache_name,
            ClientIntelligence::Basic,
            DEFAULT_TOPOLOGY_ID,
            DEFAULT_TIMEOUT,
            None,
            true,
        )
        .await
    }

    /// Same as `connect`, but with a caller-supplied timeout in place of
    /// `DEFAULT_TIMEOUT`, applied to the initial connect and to every
    /// operation and authentication round on the resulting connection.
    pub async fn connect_with_timeout(
        addr: impl ToSocketAddrs,
        cache_name: &str,
        timeout: Duration,
    ) -> Result<Self> {
        Self::connect_with(
            addr,
            cache_name,
            ClientIntelligence::Basic,
            DEFAULT_TOPOLOGY_ID,
            timeout,
            None,
            true,
        )
        .await
    }

    /// Same as `connect`, but over TLS (ADR 0004,
    /// `docs/adr/0004-tls-support.md`), verifying the server's certificate
    /// both by hostname (against `tls.server_name`) and by chain (against
    /// `tls.ca_certificate` or the OS trust store).
    pub async fn connect_tls(
        addr: impl ToSocketAddrs,
        cache_name: &str,
        tls: &TlsConfig,
    ) -> Result<Self> {
        Self::connect_with(
            addr,
            cache_name,
            ClientIntelligence::Basic,
            DEFAULT_TOPOLOGY_ID,
            DEFAULT_TIMEOUT,
            Some(tls),
            true,
        )
        .await
    }

    /// Same as `connect_tls`, but with a caller-supplied timeout in place of
    /// `DEFAULT_TIMEOUT`.
    pub async fn connect_tls_with_timeout(
        addr: impl ToSocketAddrs,
        cache_name: &str,
        tls: &TlsConfig,
        timeout: Duration,
    ) -> Result<Self> {
        Self::connect_with(
            addr,
            cache_name,
            ClientIntelligence::Basic,
            DEFAULT_TOPOLOGY_ID,
            timeout,
            Some(tls),
            true,
        )
        .await
    }

    /// Opens a TCP connection that advertises `HashDistributionAware`
    /// intelligence, for use as one of `HotRodClient`'s pooled per-node
    /// connections. `topology_id` is the id already known to the cluster, so
    /// the server does not resend a topology update the client already has.
    ///
    /// `verify_hostname` is `false` for a connection to a node discovered
    /// through a topology update, which carries only an address, never a
    /// hostname to check: see the `tls` module docs and ADR 0004. It is
    /// `true` for a seed dial, the only case with a real hostname to verify.
    pub(crate) async fn connect_hash_aware(
        addr: impl ToSocketAddrs,
        cache_name: &str,
        topology_id: i32,
        timeout: Duration,
        tls: Option<&TlsConfig>,
        verify_hostname: bool,
    ) -> Result<Self> {
        Self::connect_with(
            addr,
            cache_name,
            ClientIntelligence::HashDistributionAware,
            topology_id,
            timeout,
            tls,
            verify_hostname,
        )
        .await
    }

    async fn connect_with(
        addr: impl ToSocketAddrs,
        cache_name: &str,
        intelligence: ClientIntelligence,
        topology_id: i32,
        timeout: Duration,
        tls: Option<&TlsConfig>,
        verify_hostname: bool,
    ) -> Result<Self> {
        let stream = with_timeout(timeout, async {
            let tcp = TcpStream::connect(addr).await?;
            tcp.set_nodelay(true)?;
            match tls {
                Some(tls) => tls::handshake(tcp, tls, verify_hostname).await,
                None => Ok(Transport::Plain(tcp)),
            }
        })
        .await?;
        Ok(Self {
            stream: BufStream::new(stream),
            cache_name: cache_name.as_bytes().to_vec(),
            next_message_id: 1,
            intelligence,
            topology_id,
            pending_topology_update: None,
            timeout,
            poisoned: false,
        })
    }

    /// `true` once a prior operation left this connection with a possible
    /// partial frame in flight and every further operation is refusing to
    /// run. `pool.rs`'s `PooledGuard` checks this before returning a
    /// connection to its idle list, instead of waiting to see whether that
    /// operation happens to come back with an error.
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Marks this connection poisoned directly, for a caller outside
    /// the usual `begin_operation`/`end_operation` pair around a single
    /// request/response: `streaming.rs` calls this when a `GetStream`/
    /// `PutStream` is dropped without an explicit `close`/`finish`,
    /// since the server-side stream state that leaves behind is scoped
    /// to this connection (see `docs/adr/0008-streaming.md`), and the
    /// pool must not hand this connection to an unrelated caller
    /// afterward.
    pub(crate) fn mark_poisoned(&mut self) {
        self.poisoned = true;
    }

    /// Refuses to start a new operation over a connection a prior one left
    /// poisoned, then marks the connection poisoned itself. Cleared only by
    /// `end_operation(true)`, once the new operation's response has been
    /// read in full.
    fn begin_operation(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::PoisonedConnection);
        }
        self.poisoned = true;
        Ok(())
    }

    /// Clears the poisoned mark `begin_operation` set, but only when
    /// `succeeded` is true: an operation that failed partway through
    /// reading its response leaves the connection poisoned, since the exact
    /// byte position the stream stopped at is unknown.
    fn end_operation(&mut self, succeeded: bool) {
        if succeeded {
            self.poisoned = false;
        }
    }

    /// Returns the topology update parsed from the most recently completed
    /// operation, if the server sent one, taking it so a later call returns
    /// `None` until another update arrives.
    pub(crate) fn take_pending_topology_update(&mut self) -> Option<TopologyUpdate> {
        self.pending_topology_update.take()
    }

    /// Consumes the connection and hands back its raw transport. Used by
    /// `listener.rs` once `add_client_listener` has confirmed the server
    /// accepted the registration: from that point on, the socket carries an
    /// unbounded stream of event frames instead of one response per
    /// request, which is not a shape `HotRodConnection`'s own
    /// `write_and_read_header`/poisoning model was built for (see its
    /// module docs). `CacheListener` reads directly off this transport
    /// instead.
    pub(crate) fn into_transport(self) -> BufStream<Transport> {
        self.stream
    }

    /// The timeout currently bounding every operation and authentication
    /// round on this connection.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Overrides the timeout used from this call onward, in place of the one
    /// given at connect time. Applies to every subsequent operation and
    /// authentication round, not just the next one: a caller that wants a
    /// single call to have a different bound must set it back afterward,
    /// typically to the value `timeout()` returned beforehand.
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    /// The metadata block (`GetWithMetadataOperation.readMetadataValue`):
    /// flags select which timestamp/duration pairs are present, then an
    /// 8-byte version always follows. The timestamps are server
    /// wall-clock time in epoch milliseconds (`TimeService.wallClockTime`),
    /// the durations are in seconds. Shared by `read_versioned_value` and
    /// `get_stream_start`, which both carry this same block before their
    /// own value/chunk. Returns `(created, lifespan, last_used,
    /// max_idle, version)`.
    async fn read_entry_metadata(
        &mut self,
    ) -> Result<(
        Option<SystemTime>,
        Expiration,
        Option<SystemTime>,
        Expiration,
        u64,
    )> {
        const INFINITE_LIFESPAN: u8 = 0x01;
        const INFINITE_MAXIDLE: u8 = 0x02;

        let flags = tokio::io::AsyncReadExt::read_u8(&mut self.stream).await?;
        let (created, lifespan) = if flags & INFINITE_LIFESPAN == 0 {
            let creation = tokio::io::AsyncReadExt::read_u64(&mut self.stream).await?;
            let lifespan = read_vint(&mut self.stream).await?;
            (
                Some(UNIX_EPOCH + Duration::from_millis(creation)),
                Expiration::Seconds(lifespan as u64),
            )
        } else {
            (None, Expiration::Immortal)
        };
        let (last_used, max_idle) = if flags & INFINITE_MAXIDLE == 0 {
            let last_used = tokio::io::AsyncReadExt::read_u64(&mut self.stream).await?;
            let max_idle = read_vint(&mut self.stream).await?;
            (
                Some(UNIX_EPOCH + Duration::from_millis(last_used)),
                Expiration::Seconds(max_idle as u64),
            )
        } else {
            (None, Expiration::Immortal)
        };
        let version = tokio::io::AsyncReadExt::read_u64(&mut self.stream).await?;
        Ok((created, lifespan, last_used, max_idle, version))
    }

    /// `read_entry_metadata`'s block, followed by the value itself.
    async fn read_versioned_value(&mut self) -> Result<VersionedValue> {
        let (created, lifespan, last_used, max_idle, version) = self.read_entry_metadata().await?;
        let value = read_array(&mut self.stream).await?;

        Ok(VersionedValue {
            value,
            version,
            created,
            lifespan,
            last_used,
            max_idle,
        })
    }

    async fn write_and_read_header(
        &mut self,
        cache_name: &[u8],
        opcode: OpCode,
        body: &[u8],
    ) -> Result<(u64, crate::header::ResponseHeader)> {
        let message_id = self.next_message_id;
        self.next_message_id += 1;

        // Query is the one operation whose request/response bodies are
        // themselves Protobuf, and the server picks how to decode them
        // based on this declared media type
        // (`docs/adr/0013-remote-query.md`). `___protobuf_metadata`'s
        // own storage is fixed to protostream server-side regardless
        // of what a request declares, confirmed empirically (a write
        // declaring "none" was rejected outright); this applies to
        // every operation against that cache, not just
        // `register_proto_schema`'s write, since a read would need the
        // same declaration to correctly interpret what comes back.
        // `PROTOBUF_METADATA_CACHE_NAME`'s triple-underscore prefix is
        // Infinispan's own reserved-name convention for internal
        // caches, not something a real user cache plausibly collides
        // with. Every other operation, against every other cache,
        // still declares "none", unchanged.
        let media_type = if matches!(opcode, OpCode::Query)
            || cache_name == crate::client::PROTOBUF_METADATA_CACHE_NAME.as_bytes()
        {
            crate::header::RequestMediaType::Protostream
        } else {
            crate::header::RequestMediaType::None
        };

        let mut header = crate::header::write_and_read_header(
            &mut self.stream,
            message_id,
            cache_name,
            opcode,
            self.intelligence,
            self.topology_id,
            body,
            media_type,
        )
        .await?;
        if let Some(update) = &header.topology_update {
            self.topology_id = update.topology_id as i32;
        }
        self.pending_topology_update = std::mem::take(&mut header.topology_update);
        Ok((message_id, header))
    }
}

fn key_value_body(key: &[u8], value: &[u8], lifespan: Expiration, max_idle: Expiration) -> Vec<u8> {
    let mut body = Vec::new();
    write_array(&mut body, key);
    write_expiration_params(&mut body, lifespan, max_idle);
    write_array(&mut body, value);
    body
}

fn versioned_result(status: crate::status::Status) -> VersionedResult {
    if status.is_not_exist() {
        VersionedResult::NotFound
    } else if status.is_not_executed() {
        VersionedResult::Stale
    } else {
        VersionedResult::Success
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};

    use crate::varint::{read_vlong, write_vlong};

    /// Reads one request's fixed header fields far enough to identify the
    /// opcode and message id, discarding the rest: those fields are
    /// already covered by `header.rs`'s own tests. `pub(crate)`: reused
    /// by domain submodules' own test modules (e.g. `connection::
    /// multimap::tests`), which need it under the exact same path,
    /// `crate::connection::tests::read_request`.
    pub(crate) async fn read_request(stream: &mut TcpStream) -> (u64, u8) {
        assert_eq!(stream.read_u8().await.unwrap(), 0xA0);
        let message_id = read_vlong(stream).await.unwrap();
        let _version = stream.read_u8().await.unwrap();
        let opcode = stream.read_u8().await.unwrap();
        let _cache_name = read_array(stream).await.unwrap();
        let _flags = crate::varint::read_vint(stream).await.unwrap();
        let _intelligence = stream.read_u8().await.unwrap();
        let _topology_id = crate::varint::read_vint(stream).await.unwrap();
        // Not two bare bytes: most requests declare "none" (one zero
        // byte each), but the query operation and any operation
        // against `___protobuf_metadata` declare a real predefined
        // media type (`docs/adr/0013-remote-query.md`), which is
        // longer. `skip_media_type` already knows the real format.
        crate::wire::skip_media_type(stream).await.unwrap();
        crate::wire::skip_media_type(stream).await.unwrap();
        let _additional_params = crate::varint::read_vint(stream).await.unwrap();
        (message_id, opcode)
    }

    /// `pub(crate)` for the same reason as `read_request` above.
    pub(crate) fn response_header(message_id: u64, opcode: u8, status: u8) -> Vec<u8> {
        let mut buf = vec![0xA1];
        write_vlong(&mut buf, message_id);
        buf.push(opcode);
        buf.push(status);
        buf.push(0); // no topology update
        buf
    }

    #[tokio::test]
    async fn operation_times_out_when_the_server_never_responds() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let mut conn = HotRodConnection::connect_with_timeout(addr, "", Duration::from_millis(100))
            .await
            .expect("connect should succeed even though the server stays silent");

        let result = conn.get(b"key").await;

        assert!(matches!(result, Err(Error::Timeout(_))));
    }

    /// A timeout leaves the connection poisoned: a following operation must
    /// fail immediately with `Error::PoisonedConnection`, never touching
    /// the network, rather than reading from a stream that may still have
    /// the earlier request's response landing on it mid-frame.
    #[tokio::test]
    async fn operation_after_a_timeout_fails_fast_instead_of_reusing_the_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let mut conn = HotRodConnection::connect_with_timeout(addr, "", Duration::from_millis(100))
            .await
            .expect("connect should succeed even though the server stays silent");

        let first = conn.get(b"key").await;
        assert!(matches!(first, Err(Error::Timeout(_))));

        let start = tokio::time::Instant::now();
        let second = conn.get(b"key").await;

        assert!(matches!(second, Err(Error::PoisonedConnection)));
        assert!(
            start.elapsed() < Duration::from_millis(50),
            "a poisoned connection should fail immediately, not wait out another timeout"
        );
    }

    #[tokio::test]
    async fn set_timeout_overrides_the_bound_used_by_the_next_operation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let mut conn = HotRodConnection::connect_with_timeout(addr, "", Duration::from_secs(30))
            .await
            .expect("connect should succeed even though the server stays silent");
        assert_eq!(conn.timeout(), Duration::from_secs(30));

        conn.set_timeout(Duration::from_millis(100));
        assert_eq!(conn.timeout(), Duration::from_millis(100));

        let start = tokio::time::Instant::now();
        let result = conn.get(b"key").await;

        assert!(matches!(result, Err(Error::Timeout(_))));
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "the overridden timeout should have fired, not the 30s one from connect"
        );
    }
}
