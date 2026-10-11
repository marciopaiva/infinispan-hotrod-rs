//! Multimap cache: a cache where each key maps to a collection of
//! values instead of one (`docs/adr/0016-multimap-cache.md`).
//!
//! Not a special cache type server-side: an ordinary Hot Rod cache,
//! reached through its own nine opcodes instead of the main cache
//! API's. `supports_duplicates` is a flag this crate sends on every
//! call, not a cache-level setting the server remembers: confirmed
//! against the Java client's own `MultimapCacheManager.get`, which
//! takes it the same way.

use std::time::SystemTime;

use crate::client::HotRodClient;
use crate::error::Result;
use crate::remote_cache::RemoteCache;
use crate::wire::Expiration;

/// One key's values plus its entry metadata, returned by
/// `MultimapCache::get_with_metadata`. Same shape as the main cache
/// API's `VersionedValue`, except `values` is a collection instead of
/// one value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultimapEntry {
    pub values: Vec<Vec<u8>>,
    pub version: u64,
    pub created: Option<SystemTime>,
    pub lifespan: Expiration,
    pub last_used: Option<SystemTime>,
    pub max_idle: Expiration,
}

/// A handle to one multimap cache, obtained from
/// `HotRodClient::multimap_cache`. Byte-oriented like `RemoteCache`;
/// no `TypedCache` integration in this phase.
#[derive(Clone)]
pub struct MultimapCache {
    cache: RemoteCache,
    supports_duplicates: bool,
}

impl HotRodClient {
    /// Returns a handle for the multimap cache named `name`.
    /// `supports_duplicates` is sent with every call this handle
    /// makes (`docs/adr/0016-multimap-cache.md`): the server decides
    /// what it means per call, this is not a setting the cache itself
    /// remembers.
    pub fn multimap_cache(
        &self,
        name: impl Into<String>,
        supports_duplicates: bool,
    ) -> MultimapCache {
        MultimapCache {
            cache: self.cache(name),
            supports_duplicates,
        }
    }
}

impl MultimapCache {
    /// The value this handle was created with, not a round trip to
    /// the server.
    pub fn supports_duplicates(&self) -> bool {
        self.supports_duplicates
    }

    /// Every value stored under `key`, empty if it has no entry
    /// (confirmed against `GetMultimapOperation`: a missing key reads
    /// as an empty collection, not a distinguishable third case).
    pub async fn get(&self, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.cache.multimap_get(key, self.supports_duplicates).await
    }

    /// Same as `get`, plus the entry's metadata (creation, lifespan,
    /// last used, max idle, version). `None` if `key` has no entry.
    pub async fn get_with_metadata(&self, key: &[u8]) -> Result<Option<MultimapEntry>> {
        self.cache
            .multimap_get_with_metadata(key, self.supports_duplicates)
            .await
    }

    /// Adds `value` to the collection stored under `key`, creating it
    /// if needed. `lifespan`/`max_idle` apply to the whole entry, the
    /// same as the main cache API's `put`.
    pub async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        self.cache
            .multimap_put(key, value, lifespan, max_idle, self.supports_duplicates)
            .await
    }

    /// Removes `key` and every value stored under it.
    pub async fn remove_key(&self, key: &[u8]) -> Result<bool> {
        self.cache
            .multimap_remove_key(key, self.supports_duplicates)
            .await
    }

    /// Removes just `value` from `key`'s collection, leaving any
    /// other values under it untouched.
    pub async fn remove_entry(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        self.cache
            .multimap_remove_entry(key, value, self.supports_duplicates)
            .await
    }

    /// The number of key/value pairs in the whole cache: every value
    /// under every key counts separately, confirmed against
    /// `SizeMultimapOperation` to be a plain count, not the number of
    /// distinct keys.
    pub async fn size(&self) -> Result<u64> {
        self.cache.multimap_size(self.supports_duplicates).await
    }

    pub async fn contains_entry(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        self.cache
            .multimap_contains_entry(key, value, self.supports_duplicates)
            .await
    }

    pub async fn contains_key(&self, key: &[u8]) -> Result<bool> {
        self.cache
            .multimap_contains_key(key, self.supports_duplicates)
            .await
    }

    /// Whether `value` is stored under any key in the whole cache,
    /// not just one.
    pub async fn contains_value(&self, value: &[u8]) -> Result<bool> {
        self.cache
            .multimap_contains_value(value, self.supports_duplicates)
            .await
    }
}
