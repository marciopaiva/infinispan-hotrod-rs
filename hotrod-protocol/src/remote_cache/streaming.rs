//! Streaming large values in chunks instead of buffering them whole
//! (`docs/adr/0008-streaming.md`).
//!
//! Split out of `remote_cache.rs`
//! (`docs/adr/0026-split-connection-and-remote-cache-by-domain.md`):
//! see `remote_cache/multimap.rs`'s module docs for the mechanism
//! that lets this `impl RemoteCache` block reach `RemoteCache`'s
//! otherwise-private `client`/`cache_name` fields with no visibility
//! change. No dedicated tests here: the wire-level shape is covered
//! by `connection::streaming`'s own tests.

use crate::error::Result;
use crate::streaming::{GetStream, PutStream};
use crate::wire::Expiration;

use super::RemoteCache;

impl RemoteCache {
    /// Opens a stream to read `key`'s value in chunks of up to
    /// `batch_size` bytes, instead of buffering it whole like `get`.
    /// `None` on a miss, the same as `get`. Routed to `key`'s computed
    /// owner like any other keyed operation, but not retried or failed
    /// over if that connection fails afterward: see
    /// `docs/adr/0008-streaming.md`.
    pub async fn get_stream(&self, key: &[u8], batch_size: u32) -> Result<Option<GetStream>> {
        let (addr, origin) = self.client.owner_addr(key).await?;
        let mut guard = self.client.checkout(addr, &self.cache_name, origin).await?;
        let start = guard.get_stream_start(key, batch_size).await?;
        // Applied regardless of a hit or a miss: the response can carry
        // a topology update either way, and skipping it on a miss would
        // leave this client routing by a stale topology until some
        // unrelated later call happens to reuse this exact connection.
        self.client.record_topology_update(&mut guard);
        Ok(start.map(|start| GetStream::new(guard, start)))
    }

    /// Opens a stream to write `key`'s value in chunks of up to
    /// `chunk_size` bytes, instead of buffering it whole like `put`.
    /// Nothing is written server-side until `PutStream::finish` sends
    /// the final chunk; see `docs/adr/0008-streaming.md`.
    pub async fn put_stream(
        &self,
        key: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
        chunk_size: usize,
    ) -> Result<PutStream> {
        self.open_put_stream(key, lifespan, max_idle, 0, chunk_size)
            .await
    }

    /// Same as `put_stream`, but the write only commits if `key` does
    /// not already exist, the same condition `put_if_absent` checks.
    pub async fn put_stream_if_absent(
        &self,
        key: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
        chunk_size: usize,
    ) -> Result<PutStream> {
        self.open_put_stream(key, lifespan, max_idle, -1, chunk_size)
            .await
    }

    /// Same as `put_stream`, but the write only commits if the entry's
    /// current version still matches `version` (from `get_with_version`),
    /// the same condition `replace_if_unmodified` checks.
    ///
    /// `version` is sent as the wire's `i64` field, where `0` and `-1`
    /// are the `put_stream`/`put_stream_if_absent` sentinels (see
    /// `docs/adr/0008-streaming.md`): a real version whose bit pattern
    /// happens to equal one of those (as `u64::MAX`, or `0` itself)
    /// would be sent as that sentinel instead of a real conditional
    /// version. Inherent to the wire protocol's own encoding, the same
    /// way the Java client's `long` field has the identical limitation;
    /// not something this method can detect or guard against.
    pub async fn replace_stream_with_version(
        &self,
        key: &[u8],
        version: u64,
        lifespan: Expiration,
        max_idle: Expiration,
        chunk_size: usize,
    ) -> Result<PutStream> {
        self.open_put_stream(key, lifespan, max_idle, version as i64, chunk_size)
            .await
    }

    /// Shared by `put_stream`/`put_stream_if_absent`/
    /// `replace_stream_with_version`: opens the connection and the
    /// stream, differing only in which `version` sentinel
    /// `put_stream_start` gets (`0` unconditional, `-1` if-absent, a
    /// real version for a conditional replace).
    async fn open_put_stream(
        &self,
        key: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
        version: i64,
        chunk_size: usize,
    ) -> Result<PutStream> {
        let (addr, origin) = self.client.owner_addr(key).await?;
        let mut guard = self.client.checkout(addr, &self.cache_name, origin).await?;
        let stream_id = guard
            .put_stream_start(key, lifespan, max_idle, version)
            .await?;
        self.client.record_topology_update(&mut guard);
        Ok(PutStream::new(guard, stream_id, chunk_size))
    }
}
