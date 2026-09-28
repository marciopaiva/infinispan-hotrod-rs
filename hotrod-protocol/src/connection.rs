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

use std::future::Future;
use std::time::Duration;

use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::digest::DigestSha256Mechanism;
use crate::error::{Error, Result};
use crate::header::{read_response_header, write_request_header, OpCode};
use crate::sasl::{OAuthBearerMechanism, PlainMechanism, SaslMechanism};
use crate::scram::ScramSha512Mechanism;
use crate::topology::{ClientIntelligence, TopologyUpdate};
use crate::varint::read_vint;
use crate::wire::{read_array, read_string, write_array, write_expiration_params, Expiration};

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
/// `replace_if_unmodified` or `remove_if_unmodified` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedValue {
    pub value: Vec<u8>,
    pub version: u64,
}

pub struct HotRodConnection {
    stream: BufStream<TcpStream>,
    cache_name: Vec<u8>,
    next_message_id: u64,
    intelligence: ClientIntelligence,
    topology_id: i32,
    /// The topology update parsed from the most recent response, if any.
    /// Cleared by `take_pending_topology_update`.
    pending_topology_update: Option<TopologyUpdate>,
    timeout: Duration,
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
        )
        .await
    }

    /// Opens a TCP connection that advertises `HashDistributionAware`
    /// intelligence, for use as one of `HotRodCluster`'s pooled per-node
    /// connections. `topology_id` is the id already known to the cluster, so
    /// the server does not resend a topology update the client already has.
    pub(crate) async fn connect_hash_aware(
        addr: impl ToSocketAddrs,
        cache_name: &str,
        topology_id: i32,
        timeout: Duration,
    ) -> Result<Self> {
        Self::connect_with(
            addr,
            cache_name,
            ClientIntelligence::HashDistributionAware,
            topology_id,
            timeout,
        )
        .await
    }

    async fn connect_with(
        addr: impl ToSocketAddrs,
        cache_name: &str,
        intelligence: ClientIntelligence,
        topology_id: i32,
        timeout: Duration,
    ) -> Result<Self> {
        let tcp = with_timeout(timeout, async { Ok(TcpStream::connect(addr).await?) }).await?;
        tcp.set_nodelay(true)?;
        Ok(Self {
            stream: BufStream::new(tcp),
            cache_name: cache_name.as_bytes().to_vec(),
            next_message_id: 1,
            intelligence,
            topology_id,
            pending_topology_update: None,
            timeout,
        })
    }

    /// Returns the topology update parsed from the most recently completed
    /// operation, if the server sent one, taking it so a later call returns
    /// `None` until another update arrives.
    pub(crate) fn take_pending_topology_update(&mut self) -> Option<TopologyUpdate> {
        self.pending_topology_update.take()
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
        let timeout = self.timeout;
        with_timeout(timeout, async {
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
        .await
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let timeout = self.timeout;
        with_timeout(timeout, async {
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
        .await
    }

    pub async fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        let timeout = self.timeout;
        with_timeout(timeout, async {
            let body = key_value_body(key, value, lifespan, max_idle);
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::Put, &body)
                .await?;
            Ok(())
        })
        .await
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
        let timeout = self.timeout;
        with_timeout(timeout, async {
            let body = key_value_body(key, value, lifespan, max_idle);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::PutIfAbsent, &body)
                .await?;
            Ok(!header.status.is_not_executed())
        })
        .await
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
        let timeout = self.timeout;
        with_timeout(timeout, async {
            let body = key_value_body(key, value, lifespan, max_idle);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::Replace, &body)
                .await?;
            Ok(!header.status.is_not_executed())
        })
        .await
    }

    /// Returns `true` if the key existed and was removed, `false` if it did
    /// not exist.
    pub async fn remove(&mut self, key: &[u8]) -> Result<bool> {
        let timeout = self.timeout;
        with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::Remove, &body)
                .await?;
            Ok(!header.status.is_not_exist())
        })
        .await
    }

    /// Fetches a value together with its entry version, for use in a later
    /// `replace_if_unmodified` or `remove_if_unmodified` call.
    pub async fn get_with_version(&mut self, key: &[u8]) -> Result<Option<VersionedValue>> {
        let timeout = self.timeout;
        with_timeout(timeout, async {
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
        .await
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
        let timeout = self.timeout;
        with_timeout(timeout, async {
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
        .await
    }

    /// Removes the entry only if its current version still matches
    /// `version` (obtained from `get_with_version`).
    pub async fn remove_if_unmodified(
        &mut self,
        key: &[u8],
        version: u64,
    ) -> Result<VersionedResult> {
        let timeout = self.timeout;
        with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            body.extend_from_slice(&version.to_be_bytes());

            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::RemoveIfUnmodified, &body)
                .await?;
            Ok(versioned_result(header.status))
        })
        .await
    }

    /// Mirrors `GetWithMetadataOperation.readMetadataValue`: flags select
    /// which timestamp/duration pairs are present, then an 8-byte version
    /// and the value always follow. Timestamps and durations are consumed
    /// to keep the stream in sync but are not part of phase 1's API.
    async fn read_versioned_value(&mut self) -> Result<VersionedValue> {
        const INFINITE_LIFESPAN: u8 = 0x01;
        const INFINITE_MAXIDLE: u8 = 0x02;

        let flags = tokio::io::AsyncReadExt::read_u8(&mut self.stream).await?;
        if flags & INFINITE_LIFESPAN == 0 {
            let _creation = tokio::io::AsyncReadExt::read_u64(&mut self.stream).await?;
            let _lifespan = read_vint(&mut self.stream).await?;
        }
        if flags & INFINITE_MAXIDLE == 0 {
            let _last_used = tokio::io::AsyncReadExt::read_u64(&mut self.stream).await?;
            let _max_idle = read_vint(&mut self.stream).await?;
        }
        let version = tokio::io::AsyncReadExt::read_u64(&mut self.stream).await?;
        let value = read_array(&mut self.stream).await?;

        Ok(VersionedValue { value, version })
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
    use tokio::net::TcpListener;

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
}
