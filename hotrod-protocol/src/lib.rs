//! Pure Rust client for the Infinispan Hot Rod binary wire protocol.
//!
//! `HotRodConnection` (phase 1, see
//! `docs/adr/0001-mirror-java-client-scope.md`) is a single sequential
//! connection to one cache, authenticated with PLAIN, SCRAM-SHA-512,
//! DIGEST-SHA-256 or OAUTHBEARER, exposing the core operations (`get`,
//! `put`, `remove`, `put_if_absent`, `replace`, `replace_if_unmodified`,
//! `remove_if_unmodified`, `contains_key`, `ping`, `size`, `clear`,
//! `stats`, `get_all`, `put_all`). `HotRodClient` (phase 3, see
//! `docs/adr/0003-hash-aware-routing-scope.md`, restructured by
//! `docs/adr/0005-connection-pooling-and-client-cache-split.md`) tracks
//! cluster topology and holds a pool of such connections per node, per
//! cache; `cache` returns a `RemoteCache` handle for a named cache, whose
//! operations route to the segment's primary owner instead of relying on
//! server-side redirects and run concurrently across however many
//! `RemoteCache`/`HotRodClient` handles a caller shares between tasks.
//! Either `HotRodConnection` or `HotRodClient` also connects over TLS
//! (`connect_tls`/`connect_tls_with_timeout`, see
//! `docs/adr/0004-tls-support.md`). Every connect and operation on either
//! type is bounded by a timeout (`DEFAULT_TIMEOUT` unless overridden).
//! `RemoteCache::listen`/`listen_with` (phase 4, see
//! `docs/adr/0006-client-listeners.md`) register a client listener on its
//! own dedicated connection, returning a `CacheListener` to pull
//! `CacheEvent`s from. `RemoteCache::near_cache` (phase 5, see
//! `docs/adr/0007-near-caching.md`) builds on that listener to keep a
//! bounded, invalidated local cache of recently read entries.
//! `RemoteCache::get_stream`/`put_stream`/`put_stream_if_absent`/
//! `replace_stream_with_version` (see `docs/adr/0008-streaming.md`)
//! read or write a value in chunks instead of buffering it whole,
//! returning a `GetStream`/`PutStream` pinned to the one pooled
//! connection that opened it. `RemoteCache::iter`/`iter_with` (see
//! `docs/adr/0009-server-side-iteration.md`) walk every entry in a
//! cache through a server-side `CacheIterator`, opening one cursor per
//! node on a cluster instead of requiring the caller to already know
//! every key.

#![forbid(unsafe_code)]

mod client;
mod connection;
mod digest;
mod error;
mod hash;
mod header;
mod health;
mod iteration;
mod listener;
mod near_cache;
mod pool;
mod remote_cache;
mod sasl;
mod scram;
mod stats;
mod status;
mod streaming;
mod tls;
mod topology;
mod varint;
mod wire;

pub use client::HotRodClient;
pub use connection::{HotRodConnection, VersionedResult, VersionedValue, DEFAULT_TIMEOUT};
pub use error::{Error, Result};
pub use iteration::{CacheIterator, IterationEntry, IterationOptions};
pub use listener::{CacheEvent, CacheEventInterests, CacheListener, ListenOptions, ServerFactory};
pub use near_cache::{NearCacheOptions, NearCachedCache};
pub use remote_cache::RemoteCache;
pub use stats::{ClientStatistics, NearCacheStatistics, PoolStatistics};
pub use streaming::{GetStream, PutStream};
pub use tls::TlsConfig;
pub use wire::Expiration;

/// Thin `pub` wrappers around a handful of otherwise `pub(crate)` wire
/// parsers, so `fuzz/`'s targets can call into them, only compiled
/// when the `fuzzing` feature is on (never the default; nothing
/// outside `fuzz/` enables it). This is the only place this crate's
/// real public API differs with that feature on versus off.
///
/// Plain re-exports (`pub use crate::varint::read_vint`) do not work
/// here: Rust rejects re-exporting a `pub(crate)` item through a more
/// permissive `pub` path outright (`E0364`), and several of these
/// parsers take or return `pub(crate)` types (`OpCode`,
/// `ClientIntelligence`, `TopologyUpdate`, the SASL mechanism structs)
/// that cannot appear in a `pub` function's signature either
/// (`E0446`). Each wrapper below is a genuinely new function instead,
/// free to call a `pub(crate)` function from inside this same crate
/// and expose only already-public types at its own boundary:
/// `TopologyUpdate` and `ResponseHeader` are dropped (`.map(|_| ())`,
/// fuzzing only cares whether parsing panics or allocates
/// unreasonably, never the parsed shape), and `read_response_header`
/// hardcodes an opcode/intelligence pair instead of taking one as a
/// parameter.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzz_internal {
    use crate::error::Result;
    use std::collections::HashMap;
    use tokio::io::AsyncRead;

    pub async fn read_vint(reader: &mut (impl AsyncRead + Unpin)) -> Result<u32> {
        crate::varint::read_vint(reader).await
    }

    pub async fn read_vlong(reader: &mut (impl AsyncRead + Unpin)) -> Result<u64> {
        crate::varint::read_vlong(reader).await
    }

    pub async fn read_array(reader: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
        crate::wire::read_array(reader).await
    }

    pub async fn read_string(reader: &mut (impl AsyncRead + Unpin)) -> Result<String> {
        crate::wire::read_string(reader).await
    }

    pub async fn read_string_map(
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<HashMap<String, String>> {
        crate::wire::read_string_map(reader).await
    }

    pub async fn read_topology_update(reader: &mut (impl AsyncRead + Unpin)) -> Result<()> {
        crate::topology::read_topology_update(reader)
            .await
            .map(|_| ())
    }

    /// Takes `&[u8]` directly, not a generic reader like the wrappers
    /// above: the real `message_id` check this fixture otherwise
    /// can't satisfy needs peeking a few leading bytes independently
    /// of the real parse below, only possible because fuzzed input is
    /// a byte slice (`Copy`), never a true single-pass stream.
    ///
    /// Peeks the same magic byte and `message_id` varint the real
    /// parser is about to read on its own, and passes that back in as
    /// the "expected" id, so every input reaches the opcode/status/
    /// topology_marker/`read_topology_update` logic this target
    /// exists to exercise by construction, rather than depending on
    /// libFuzzer rediscovering one exact multi-byte value by mutation
    /// alone (unlike the magic byte, a single fixed value coverage
    /// feedback pins trivially, matching an arbitrary varint exactly
    /// is a much harder target to hit by chance). `HashDistributionAware`
    /// (not `Basic`) so a topology update present in the fuzzed bytes
    /// is actually parsed too.
    pub async fn read_response_header(data: &[u8]) -> Result<()> {
        let mut peek = data;
        let _magic = tokio::io::AsyncReadExt::read_u8(&mut peek)
            .await
            .unwrap_or(0);
        let message_id = crate::varint::read_vlong(&mut peek).await.unwrap_or(0);

        let mut reader = data;
        crate::header::read_response_header(
            &mut reader,
            message_id,
            crate::header::OpCode::Get,
            crate::topology::ClientIntelligence::HashDistributionAware,
        )
        .await
        .map(|_| ())
    }

    /// Drives a full three-step exchange on one mechanism instance:
    /// `respond(None)` (the client-first message, always the same
    /// shape for fixed credentials), `respond(Some(server_first))`
    /// (processes salt/iterations/nonce, the one `StdResult` callers
    /// actually get back), then `respond(Some(server_final))` (the
    /// signature-verification branch). A single `respond(Some(..))`
    /// call, as an earlier version of this wrapper did, can only ever
    /// reach the server-first branch: `expected_server_signature`
    /// (SCRAM)/`expected_rspauth` (DIGEST) stay `None` until a prior
    /// `Some` call sets them, so the verification branch is
    /// structurally unreachable without this third call.
    pub fn scram_respond(
        authcid: &str,
        password: &str,
        server_first: &[u8],
        server_final: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        use crate::sasl::SaslMechanism;
        let mut mechanism = crate::scram::ScramSha512Mechanism::new(authcid, password);
        mechanism.respond(None)?;
        mechanism.respond(Some(server_first))?;
        mechanism.respond(Some(server_final))
    }

    /// Same shape as `scram_respond`, for DIGEST-SHA-256.
    pub fn digest_respond(
        authcid: &str,
        password: &str,
        server_first: &[u8],
        server_final: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        use crate::sasl::SaslMechanism;
        let mut mechanism = crate::digest::DigestSha256Mechanism::new(authcid, password);
        mechanism.respond(None)?;
        mechanism.respond(Some(server_first))?;
        mechanism.respond(Some(server_final))
    }
}
