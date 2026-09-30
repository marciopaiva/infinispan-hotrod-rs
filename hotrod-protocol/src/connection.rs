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

use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::digest::DigestSha256Mechanism;
use crate::error::{Error, Result};
use crate::header::{read_response_header, write_request_header, OpCode};
use crate::sasl::{OAuthBearerMechanism, PlainMechanism, SaslMechanism};
use crate::scram::ScramSha512Mechanism;
use crate::tls::{self, TlsConfig, Transport};
use crate::topology::{ClientIntelligence, TopologyUpdate};
use crate::varint::{read_vint, write_vint};
use crate::wire::{
    read_array, read_string, read_string_map, skip_media_type, write_array,
    write_expiration_params, Expiration,
};

/// Sent on every request until the sender has topology awareness to report
/// a real one: `HotRodConnection` always sends this, and it's also what
/// `HotRodCluster` starts a fresh pooled connection with before its first
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
async fn with_timeout<T>(timeout: Duration, fut: impl Future<Output = Result<T>>) -> Result<T> {
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
    /// intelligence, for use as one of `HotRodCluster`'s pooled per-node
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
    /// run. `HotRodCluster` checks this before handing a pooled connection
    /// to the next operation, instead of waiting to see whether that
    /// operation happens to come back with an error.
    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned
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

    /// Mirrors `GetWithMetadataOperation.readMetadataValue`: flags select
    /// which timestamp/duration pairs are present, then an 8-byte version
    /// and the value always follow. The timestamps are server wall-clock
    /// time in epoch milliseconds (`TimeService.wallClockTime`), the
    /// durations are in seconds.
    async fn read_versioned_value(&mut self) -> Result<VersionedValue> {
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

        let mut request = Vec::with_capacity(32 + body.len());
        write_request_header(
            &mut request,
            message_id,
            cache_name,
            opcode,
            self.intelligence,
            self.topology_id,
        );
        request.extend_from_slice(body);

        self.stream.write_all(&request).await?;
        self.stream.flush().await?;

        let mut header =
            read_response_header(&mut self.stream, message_id, opcode, self.intelligence).await?;
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
}
