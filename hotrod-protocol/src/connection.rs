//! A single sequential connection to one cache on a Hot Rod server.
//!
//! Phase 1 (ADR 0001) deliberately does not pipeline or multiplex requests:
//! each call writes a request and waits for its response before the next
//! call may proceed. There is one connection per `HotRodConnection`, no
//! pooling, no cluster topology awareness.

use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::error::{Error, Result};
use crate::header::{read_response_header, write_request_header, OpCode};
use crate::sasl::plain_response;
use crate::varint::read_vint;
use crate::wire::{read_array, read_string, write_array, write_expiration_params, Expiration};

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
}

impl HotRodConnection {
    /// Opens a TCP connection and targets the given cache. An empty
    /// `cache_name` targets the server's default cache.
    pub async fn connect(addr: impl ToSocketAddrs, cache_name: &str) -> Result<Self> {
        let tcp = TcpStream::connect(addr).await?;
        tcp.set_nodelay(true)?;
        Ok(Self {
            stream: BufStream::new(tcp),
            cache_name: cache_name.as_bytes().to_vec(),
            next_message_id: 1,
        })
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
        let empty_cache_name: Vec<u8> = Vec::new();

        self.write_and_read_header(&empty_cache_name, OpCode::AuthMechList, &[])
            .await?;
        let mech_count = read_vint(&mut self.stream).await?;
        let mut offered = Vec::with_capacity(mech_count as usize);
        for _ in 0..mech_count {
            offered.push(read_string(&mut self.stream).await?);
        }
        if !offered.iter().any(|mech| mech == "PLAIN") {
            return Err(Error::UnsupportedSaslMechanism("PLAIN".to_string()));
        }

        let response = plain_response(authzid, authcid, password);
        let mut body = Vec::new();
        write_array(&mut body, b"PLAIN");
        write_array(&mut body, &response);
        self.write_and_read_header(&empty_cache_name, OpCode::Auth, &body)
            .await?;

        let complete = tokio::io::AsyncReadExt::read_u8(&mut self.stream).await? > 0;
        let _challenge = read_array(&mut self.stream).await?;
        if !complete {
            return Err(Error::AuthenticationFailed(
                "server requested a further SASL step, which PLAIN does not support".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
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
    }

    pub async fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        let body = key_value_body(key, value, lifespan, max_idle);
        let cache_name = self.cache_name.clone();
        self.write_and_read_header(&cache_name, OpCode::Put, &body)
            .await?;
        Ok(())
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
        let body = key_value_body(key, value, lifespan, max_idle);
        let cache_name = self.cache_name.clone();
        let (_message_id, header) = self
            .write_and_read_header(&cache_name, OpCode::PutIfAbsent, &body)
            .await?;
        Ok(!header.status.is_not_executed())
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
        let body = key_value_body(key, value, lifespan, max_idle);
        let cache_name = self.cache_name.clone();
        let (_message_id, header) = self
            .write_and_read_header(&cache_name, OpCode::Replace, &body)
            .await?;
        Ok(!header.status.is_not_executed())
    }

    /// Returns `true` if the key existed and was removed, `false` if it did
    /// not exist.
    pub async fn remove(&mut self, key: &[u8]) -> Result<bool> {
        let mut body = Vec::new();
        write_array(&mut body, key);
        let cache_name = self.cache_name.clone();
        let (_message_id, header) = self
            .write_and_read_header(&cache_name, OpCode::Remove, &body)
            .await?;
        Ok(!header.status.is_not_exist())
    }

    /// Fetches a value together with its entry version, for use in a later
    /// `replace_if_unmodified` or `remove_if_unmodified` call.
    pub async fn get_with_version(&mut self, key: &[u8]) -> Result<Option<VersionedValue>> {
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
    }

    /// Removes the entry only if its current version still matches
    /// `version` (obtained from `get_with_version`).
    pub async fn remove_if_unmodified(
        &mut self,
        key: &[u8],
        version: u64,
    ) -> Result<VersionedResult> {
        let mut body = Vec::new();
        write_array(&mut body, key);
        body.extend_from_slice(&version.to_be_bytes());

        let cache_name = self.cache_name.clone();
        let (_message_id, header) = self
            .write_and_read_header(&cache_name, OpCode::RemoveIfUnmodified, &body)
            .await?;
        Ok(versioned_result(header.status))
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
        write_request_header(&mut request, message_id, cache_name, opcode);
        request.extend_from_slice(body);

        self.stream.write_all(&request).await?;
        self.stream.flush().await?;

        let header = read_response_header(&mut self.stream, message_id, opcode).await?;
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
