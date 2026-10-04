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

use std::collections::HashMap;
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::BufStream;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::digest::DigestSha256Mechanism;
use crate::error::{Error, Result};
use crate::header::OpCode;
use crate::listener::{ServerFactory, MAX_FACTORY_PARAMS};
use crate::sasl::{OAuthBearerMechanism, PlainMechanism, SaslMechanism};
use crate::scram::ScramSha512Mechanism;
use crate::tls::{self, TlsConfig, Transport};
use crate::topology::{ClientIntelligence, TopologyUpdate};
use crate::varint::{read_vint, write_signed_vint, write_vint};
use crate::wire::{
    read_array, read_string, read_string_map, skip_media_type, write_array,
    write_expiration_params, Expiration,
};

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

/// Safety ceiling on the number of entries `get_all`/`put_all` accept in
/// one call, checked before a single byte is written. This is not a
/// protocol limit either: a real Hot Rod server enforces its own maximum
/// frame size and rejects whatever does not fit, but that rejection would
/// arrive only after this client already built and sent a frame that could
/// be gigabytes long, for a request that was always going to fail. A
/// caller with more entries than this needs several smaller calls instead,
/// the same way `get`/`put` scale to many keys today.
pub const MAX_BULK_ENTRIES: usize = 100_000;

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

/// One `iteration_next` call's worth of a server-side iteration. An
/// empty `entries` is how the protocol signals the cursor is exhausted;
/// it carries no flag of its own for that. The response also carries
/// which segments were finished in this batch, but this phase never
/// retries a segment on another node (see
/// `docs/adr/0009-server-side-iteration.md`), so `iteration_next` reads
/// and discards those bytes rather than keeping a field nothing reads.
pub(crate) struct IterationBatch {
    pub entries: Vec<(Vec<u8>, VersionedValue)>,
}

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

    /// Authenticates the connection using SASL PLAIN. Must be called before
    /// any cache operation if the server requires authentication.
    ///
    /// `authzid` is the authorization id; pass an empty string unless the
    /// server is configured to distinguish it from `authcid`.
    pub async fn authenticate_plain(
        &mut self,
        authzid: &str,
        authcid: &str,
        password: &str,
    ) -> Result<()> {
        self.run_sasl(PlainMechanism::new(authzid, authcid, password))
            .await
    }

    /// Authenticates the connection using SASL SCRAM-SHA-512 (RFC 5802).
    pub async fn authenticate_scram(&mut self, authcid: &str, password: &str) -> Result<()> {
        self.run_sasl(ScramSha512Mechanism::new(authcid, password))
            .await
    }

    /// Authenticates the connection using SASL DIGEST-SHA-256, Elytron's
    /// generalization of RFC 2831 DIGEST-MD5. The digest-uri's server-name
    /// half is fixed to `infinispan`; see `digest.rs` for why.
    pub async fn authenticate_digest(&mut self, authcid: &str, password: &str) -> Result<()> {
        self.run_sasl(DigestSha256Mechanism::new(authcid, password))
            .await
    }

    /// Authenticates the connection using SASL OAUTHBEARER (RFC 7628) with
    /// a bearer token obtained elsewhere, typically from an OIDC provider.
    ///
    /// This mechanism has unit test coverage only: live-server coverage
    /// needs a token-backed realm the CI fixture does not provide yet, see
    /// `sasl.rs`.
    pub async fn authenticate_oauthbearer(&mut self, authzid: &str, token: &str) -> Result<()> {
        self.run_sasl(OAuthBearerMechanism::new(authzid, token))
            .await
    }

    /// Drives a `SaslMechanism` through the server's `AuthMechList`/`Auth`
    /// exchange: confirms the mechanism is offered, then loops sending
    /// responses and feeding back challenges until the server marks the
    /// exchange complete.
    async fn run_sasl(&mut self, mut mechanism: impl SaslMechanism) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let empty_cache_name: Vec<u8> = Vec::new();

            self.write_and_read_header(&empty_cache_name, OpCode::AuthMechList, &[])
                .await?;
            let mech_count = read_vint(&mut self.stream).await?;
            let mut offered = Vec::with_capacity(mech_count as usize);
            for _ in 0..mech_count {
                offered.push(read_string(&mut self.stream).await?);
            }
            if !offered.iter().any(|mech| mech == mechanism.name()) {
                return Err(Error::UnsupportedSaslMechanism(
                    mechanism.name().to_string(),
                ));
            }

            let mut challenge: Option<Vec<u8>> = None;
            loop {
                let Some(response) = mechanism.respond(challenge.as_deref())? else {
                    return Ok(());
                };
                let mut body = Vec::new();
                write_array(&mut body, mechanism.name().as_bytes());
                write_array(&mut body, &response);
                self.write_and_read_header(&empty_cache_name, OpCode::Auth, &body)
                    .await?;

                let complete = tokio::io::AsyncReadExt::read_u8(&mut self.stream).await? > 0;
                let bytes = read_array(&mut self.stream).await?;
                if complete {
                    mechanism.finish(&bytes)?;
                    return Ok(());
                }
                challenge = Some(bytes);
            }
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

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

    /// Registers a client listener, `body` already encoded by
    /// `listener.rs` (listener id, include-current-state flag,
    /// filter/converter factory names and parameters, raw-data flag,
    /// event interests). Confirms the server accepted it; the connection
    /// is expected to be handed to `listener.rs` via `into_transport`
    /// right after, not reused for further operations here.
    pub(crate) async fn add_client_listener(&mut self, body: &[u8]) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::AddClientListener, body)
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

    /// Fetches every key in `keys` that exists, in one request. A key with
    /// no entry is simply missing from the result map, the same as `get`
    /// returning `None` for it.
    ///
    /// Returns `Error::BatchTooLarge` without writing anything if `keys` has
    /// more than `MAX_BULK_ENTRIES` entries. A caller with more keys than
    /// that needs several smaller calls instead.
    pub async fn get_all(
        &mut self,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        let keys: Vec<Vec<u8>> = keys.into_iter().map(|key| key.as_ref().to_vec()).collect();
        if keys.len() > MAX_BULK_ENTRIES {
            return Err(Error::BatchTooLarge {
                what: "get_all",
                len: keys.len(),
                max: MAX_BULK_ENTRIES,
            });
        }
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_vint(&mut body, keys.len() as u32);
            for key in &keys {
                write_array(&mut body, key);
            }
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::GetAll, &body)
                .await?;

            let size = read_vint(&mut self.stream).await?;
            let mut result = HashMap::new();
            for _ in 0..size {
                let key = read_array(&mut self.stream).await?;
                let value = read_array(&mut self.stream).await?;
                result.insert(key, value);
            }
            Ok(result)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Writes every key/value pair in `entries` in one request, all sharing
    /// the same `lifespan`/`max_idle`.
    ///
    /// Same `MAX_BULK_ENTRIES` ceiling as `get_all`, checked the same way.
    pub async fn put_all(
        &mut self,
        entries: impl IntoIterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = entries
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_vec(), value.as_ref().to_vec()))
            .collect();
        if entries.len() > MAX_BULK_ENTRIES {
            return Err(Error::BatchTooLarge {
                what: "put_all",
                len: entries.len(),
                max: MAX_BULK_ENTRIES,
            });
        }
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_expiration_params(&mut body, lifespan, max_idle);
            write_vint(&mut body, entries.len() as u32);
            for (key, value) in &entries {
                write_array(&mut body, key);
                write_array(&mut body, value);
            }
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::PutAll, &body)
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
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

    /// Opens a server-side cursor over this connection's cache, scoped to
    /// `segments` (`None` for the whole cache, used for a single-node
    /// cache or a node with no topology yet) and, if given, evaluated
    /// through a deployed filter/converter factory (`ServerFactory`'s
    /// doc comment covers that this client never runs the logic itself).
    /// Always requests metadata for every entry, so `iteration_next` can
    /// always fill it in: `docs/adr/0009-server-side-iteration.md` covers
    /// why there is nothing worth saving by asking for less. Returns the
    /// iteration id `iteration_next`/`iteration_end` must reuse on this
    /// same connection.
    pub(crate) async fn iteration_start(
        &mut self,
        segments: Option<&[u32]>,
        filter: Option<&ServerFactory>,
        batch_size: u32,
    ) -> Result<Vec<u8>> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            match segments {
                None => write_signed_vint(&mut body, -1),
                Some(segments) => {
                    let bitset = write_segment_bitset(segments);
                    write_signed_vint(&mut body, bitset.len() as i32);
                    body.extend_from_slice(&bitset);
                }
            }
            match filter {
                None => write_signed_vint(&mut body, -1),
                Some(factory) => {
                    if factory.params.len() > MAX_FACTORY_PARAMS {
                        return Err(Error::BatchTooLarge {
                            what: "a server-side iteration filter/converter factory's parameters",
                            len: factory.params.len(),
                            max: MAX_FACTORY_PARAMS,
                        });
                    }
                    let name_bytes = factory.name.as_bytes();
                    write_signed_vint(&mut body, name_bytes.len() as i32);
                    body.extend_from_slice(name_bytes);
                    body.push(factory.params.len() as u8);
                    for param in &factory.params {
                        write_array(&mut body, param);
                    }
                }
            }
            write_vint(&mut body, batch_size);
            body.push(1); // metadata: always requested
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::IterationStart, &body)
                .await?;
            read_array(&mut self.stream).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Reads the next batch of entries from a cursor `iteration_start`
    /// opened on this same connection. An empty `entries` on the
    /// returned `IterationBatch` means the cursor is exhausted: the
    /// caller must still send `iteration_end`, since unlike
    /// `get_stream_end` the server does not clean up on its own once a
    /// cursor runs dry (confirmed against the Java client, which sends
    /// it immediately on seeing this). `Error::InvalidIteration` means
    /// the server no longer knows this cursor at all; this phase does
    /// not retry that, see the ADR.
    pub(crate) async fn iteration_next(&mut self, iteration_id: &[u8]) -> Result<IterationBatch> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, iteration_id);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::IterationNext, &body)
                .await?;
            // Finished segments: read to stay in sync with the wire, then
            // discarded. See `IterationBatch`'s doc comment for why.
            read_array(&mut self.stream).await?;
            let entries_count = read_vint(&mut self.stream).await?;
            let mut entries = Vec::new();
            if entries_count > 0 {
                let projections = read_vint(&mut self.stream).await?;
                if projections != 1 {
                    return Err(Error::MalformedIterationResponse(format!(
                        "server sent {projections} value projections per entry, expected 1"
                    )));
                }
                for _ in 0..entries_count {
                    let has_metadata = tokio::io::AsyncReadExt::read_u8(&mut self.stream).await?;
                    if has_metadata != 1 {
                        return Err(Error::MalformedIterationResponse(
                            "entry carries no metadata, even though IterationStart requested it"
                                .to_string(),
                        ));
                    }
                    let (created, lifespan, last_used, max_idle, version) =
                        self.read_entry_metadata().await?;
                    let key = read_array(&mut self.stream).await?;
                    let value = read_array(&mut self.stream).await?;
                    entries.push((
                        key,
                        VersionedValue {
                            value,
                            version,
                            created,
                            lifespan,
                            last_used,
                            max_idle,
                        },
                    ));
                }
            }
            if header.status.is_invalid_iteration() {
                return Err(Error::InvalidIteration);
            }
            Ok(IterationBatch { entries })
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Closes a cursor `iteration_start` opened on this same connection.
    /// Must always be sent, even after `iteration_next` reports the
    /// cursor exhausted: see `iteration_next`'s doc comment. A server
    /// reply of `INVALID_ITERATION` here just means the cursor is
    /// already gone, which is the outcome this call wants either way, so
    /// it is not surfaced as an error.
    pub(crate) async fn iteration_end(&mut self, iteration_id: &[u8]) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, iteration_id);
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::IterationEnd, &body)
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    async fn write_and_read_header(
        &mut self,
        cache_name: &[u8],
        opcode: OpCode,
        body: &[u8],
    ) -> Result<(u64, crate::header::ResponseHeader)> {
        let message_id = self.next_message_id;
        self.next_message_id += 1;

        let mut header = crate::header::write_and_read_header(
            &mut self.stream,
            message_id,
            cache_name,
            opcode,
            self.intelligence,
            self.topology_id,
            body,
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

/// Encodes `segments` the way `java.util.BitSet.toByteArray()` would:
/// segment `n` set means byte `n / 8` has bit `n % 8` set, counted from
/// the byte's least significant bit, with the array only as long as the
/// highest segment given needs (empty if `segments` is empty). Both
/// `IterationStart`'s segment filter and `IterationNext`'s
/// finished-segments use this exact shape; confirmed against
/// `Codec30.writeIteratorStartOperation` and
/// `IterableIterationResult.segmentsToBytes`, since nothing in this
/// crate already encodes a `BitSet` this way.
fn write_segment_bitset(segments: &[u32]) -> Vec<u8> {
    let Some(&max) = segments.iter().max() else {
        return Vec::new();
    };
    let mut bytes = vec![0u8; (max / 8) as usize + 1];
    for &segment in segments {
        bytes[(segment / 8) as usize] |= 1 << (segment % 8);
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use crate::varint::{read_vlong, write_vint, write_vlong};

    /// Reads one request's fixed header fields far enough to identify the
    /// opcode and message id, discarding the rest: those fields are
    /// already covered by `header.rs`'s own tests.
    async fn read_request(stream: &mut TcpStream) -> (u64, u8) {
        assert_eq!(stream.read_u8().await.unwrap(), 0xA0);
        let message_id = read_vlong(stream).await.unwrap();
        let _version = stream.read_u8().await.unwrap();
        let opcode = stream.read_u8().await.unwrap();
        let _cache_name = read_array(stream).await.unwrap();
        let _flags = crate::varint::read_vint(stream).await.unwrap();
        let _intelligence = stream.read_u8().await.unwrap();
        let _topology_id = crate::varint::read_vint(stream).await.unwrap();
        let _key_media_type = stream.read_u8().await.unwrap();
        let _value_media_type = stream.read_u8().await.unwrap();
        let _additional_params = crate::varint::read_vint(stream).await.unwrap();
        (message_id, opcode)
    }

    fn response_header(message_id: u64, opcode: u8, status: u8) -> Vec<u8> {
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

    #[tokio::test]
    async fn get_all_returns_only_the_keys_the_server_found() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x2F, "expected a GetAll request");
            let count = crate::varint::read_vint(&mut stream).await.unwrap();
            let mut keys = Vec::new();
            for _ in 0..count {
                keys.push(read_array(&mut stream).await.unwrap());
            }
            assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

            let mut resp = response_header(id, 0x30, 0x00);
            write_vint(&mut resp, 1);
            write_array(&mut resp, b"a");
            write_array(&mut resp, b"value-a");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let result = conn
            .get_all([b"a".as_slice(), b"b".as_slice()])
            .await
            .expect("get_all");
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(b"a".as_slice()), Some(&b"value-a".to_vec()));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn put_all_completes_on_a_bare_success_status() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x2D, "expected a PutAll request");
            let _time_units = stream.read_u8().await.unwrap();
            let count = crate::varint::read_vint(&mut stream).await.unwrap();
            let mut entries = Vec::new();
            for _ in 0..count {
                let key = read_array(&mut stream).await.unwrap();
                let value = read_array(&mut stream).await.unwrap();
                entries.push((key, value));
            }
            assert_eq!(entries, vec![(b"a".to_vec(), b"1".to_vec())]);

            let resp = response_header(id, 0x2E, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        conn.put_all(
            [(b"a".as_slice(), b"1".as_slice())],
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("put_all");

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

    /// The oversized-batch check must reject the call before a single byte
    /// reaches the network: the listener below accepts a connection and then
    /// does nothing else, so this test would hang if `get_all` tried to read
    /// a response instead of failing fast on the length check.
    #[tokio::test]
    async fn get_all_rejects_a_batch_over_the_limit_without_touching_the_network() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");

        let keys = vec![b"key".to_vec(); MAX_BULK_ENTRIES + 1];
        let result = conn.get_all(keys).await;

        assert!(matches!(
            result,
            Err(Error::BatchTooLarge {
                what: "get_all",
                len,
                max: MAX_BULK_ENTRIES,
            }) if len == MAX_BULK_ENTRIES + 1
        ));
    }

    #[tokio::test]
    async fn put_all_rejects_a_batch_over_the_limit_without_touching_the_network() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");

        let entries = vec![(b"key".to_vec(), b"value".to_vec()); MAX_BULK_ENTRIES + 1];
        let result = conn
            .put_all(entries, Expiration::Default, Expiration::Default)
            .await;

        assert!(matches!(
            result,
            Err(Error::BatchTooLarge {
                what: "put_all",
                len,
                max: MAX_BULK_ENTRIES,
            }) if len == MAX_BULK_ENTRIES + 1
        ));
    }

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
            let batch_size = crate::varint::read_vint(&mut stream).await.unwrap();
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
            let _batch_size = crate::varint::read_vint(&mut stream).await.unwrap();
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
            let lifespan = crate::varint::read_vint(&mut stream).await.unwrap();
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

    #[test]
    fn segment_bitset_encodes_scattered_and_adjacent_bits() {
        // Segment 0 and 3 set bits 0 and 3 of byte 0 (0x09); segment 9
        // sets bit 1 of byte 1 (0x02); segments 16 and 17 set bits 0
        // and 1 of byte 2 (0x03).
        let bytes = write_segment_bitset(&[0, 3, 9, 16, 17]);
        assert_eq!(bytes, vec![0x09, 0x02, 0x03]);
    }

    #[test]
    fn segment_bitset_of_an_empty_slice_is_an_empty_array() {
        assert_eq!(write_segment_bitset(&[]), Vec::<u8>::new());
    }

    #[tokio::test]
    async fn iteration_start_without_segments_or_a_filter_sends_both_sentinels() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x31, "expected an IterationStart request");
            let segments_sentinel = stream.read_u8().await.unwrap();
            assert_eq!(segments_sentinel, 0x01, "zigzag(-1): no segment filter");
            let filter_sentinel = stream.read_u8().await.unwrap();
            assert_eq!(filter_sentinel, 0x01, "zigzag(-1): no filter factory");
            let batch_size = crate::varint::read_vint(&mut stream).await.unwrap();
            assert_eq!(batch_size, 128);
            let metadata = stream.read_u8().await.unwrap();
            assert_eq!(metadata, 1, "metadata is always requested");

            let mut resp = response_header(id, 0x32, 0x00);
            write_array(&mut resp, b"iteration-id");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let iteration_id = conn
            .iteration_start(None, None, 128)
            .await
            .expect("iteration_start");
        assert_eq!(iteration_id, b"iteration-id");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn iteration_start_encodes_segments_and_a_filter_factory() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x31, "expected an IterationStart request");

            let bitset_len = crate::varint::read_vint(&mut stream).await.unwrap();
            assert_eq!(bitset_len, 4, "zigzag(2) == 4: a 2-byte bitset");
            let mut bitset = [0u8; 2];
            stream.read_exact(&mut bitset).await.unwrap();
            assert_eq!(bitset, [0x01, 0x02], "segments 0 and 9 set");

            let name_len = crate::varint::read_vint(&mut stream).await.unwrap();
            assert_eq!(name_len, 10, "zigzag(5) == 10: a 5-byte factory name");
            let mut name = vec![0u8; 5];
            stream.read_exact(&mut name).await.unwrap();
            assert_eq!(&name, b"my-fn");
            let param_count = stream.read_u8().await.unwrap();
            assert_eq!(param_count, 1);
            let param = read_array(&mut stream).await.unwrap();
            assert_eq!(param, b"param");

            let _batch_size = crate::varint::read_vint(&mut stream).await.unwrap();
            let _metadata = stream.read_u8().await.unwrap();

            let mut resp = response_header(id, 0x32, 0x00);
            write_array(&mut resp, b"iteration-id");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let filter = ServerFactory {
            name: "my-fn".to_string(),
            params: vec![b"param".to_vec()],
        };
        conn.iteration_start(Some(&[0, 9]), Some(&filter), 64)
            .await
            .expect("iteration_start");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn iteration_next_returns_entries_with_metadata() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x33, "expected an IterationNext request");
            let iteration_id = read_array(&mut stream).await.unwrap();
            assert_eq!(iteration_id, b"iteration-id");

            let mut resp = response_header(id, 0x34, 0x00);
            write_array(&mut resp, &write_segment_bitset(&[2])); // finished segments
            write_vint(&mut resp, 1); // entries count
            write_vint(&mut resp, 1); // value projections
            resp.push(1); // metadata present
            const INFINITE_MAXIDLE: u8 = 0x02;
            resp.push(INFINITE_MAXIDLE);
            resp.extend_from_slice(&1_700_000_000_000u64.to_be_bytes()); // creation
            write_vint(&mut resp, 100); // lifespan seconds
            resp.extend_from_slice(&42u64.to_be_bytes()); // version
            write_array(&mut resp, b"key");
            write_array(&mut resp, b"value");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let batch = conn
            .iteration_next(b"iteration-id")
            .await
            .expect("iteration_next");

        assert_eq!(batch.entries.len(), 1);
        let (key, value) = &batch.entries[0];
        assert_eq!(key, b"key");
        assert_eq!(value.value, b"value");
        assert_eq!(value.version, 42);
        assert_eq!(value.lifespan, Expiration::Seconds(100));
        assert_eq!(value.max_idle, Expiration::Immortal);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn iteration_next_returns_no_entries_on_exhaustion() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x33, "expected an IterationNext request");
            let _iteration_id = read_array(&mut stream).await.unwrap();

            let mut resp = response_header(id, 0x34, 0x00);
            write_array(&mut resp, &[]); // no finished segments
            write_vint(&mut resp, 0); // entries count: exhausted
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let batch = conn
            .iteration_next(b"iteration-id")
            .await
            .expect("iteration_next");

        assert!(batch.entries.is_empty());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn iteration_next_surfaces_invalid_iteration_as_a_typed_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x33, "expected an IterationNext request");
            let _iteration_id = read_array(&mut stream).await.unwrap();

            let mut resp = response_header(id, 0x34, 0x05); // INVALID_ITERATION
            write_array(&mut resp, &[]);
            write_vint(&mut resp, 0);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let result = conn.iteration_next(b"iteration-id").await;
        assert!(matches!(result, Err(Error::InvalidIteration)));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn iteration_end_sends_the_iteration_id() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x35, "expected an IterationEnd request");
            let iteration_id = read_array(&mut stream).await.unwrap();
            assert_eq!(iteration_id, b"iteration-id");
            let resp = response_header(id, 0x36, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        conn.iteration_end(b"iteration-id")
            .await
            .expect("iteration_end");

        server.await.unwrap();
    }

    /// `IterationEnd` on a cursor the server already forgot about (already
    /// closed, or reaped) is not an error: the caller just wanted the
    /// cursor gone, which it already is.
    #[tokio::test]
    async fn iteration_end_tolerates_invalid_iteration_as_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x35, "expected an IterationEnd request");
            let _iteration_id = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x36, 0x05); // INVALID_ITERATION
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        conn.iteration_end(b"iteration-id")
            .await
            .expect("iteration_end treats INVALID_ITERATION as already closed");

        server.await.unwrap();
    }
}
