//! Distributed counters (`docs/adr/0015-distributed-counters.md`).
//!
//! Every operation targets the fixed cache `org.infinispan.COUNTER`
//! (`COUNTER_CACHE_NAME`), confirmed against the Java client's own
//! `CounterOperationFactory`: unlike remote administration
//! (`docs/adr/0014-remote-administration.md`), a counter operation is
//! not cache-less, it is an ordinary cache operation against one
//! fixed, reserved cache name.

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::client::HotRodClient;
use crate::error::{Error, Result};
use crate::remote_cache::RemoteCache;
use crate::varint::{read_vint, write_vint};

pub(crate) const COUNTER_CACHE_NAME: &str = "org.infinispan.COUNTER";

/// Whether a counter survives a cluster restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Storage {
    Volatile,
    Persistent,
}

/// A counter's kind, and, for the two kinds that carry one, its own
/// extra configuration. `Weak`, `BoundedStrong` and `UnboundedStrong`
/// map to one combined two-bit field in the wire encoding
/// (`CounterEncodeUtil.java`'s own flags byte), not two independent
/// flags: `0b00` unbounded strong, `0b01` weak, `0b10` bounded
/// strong; `0b11` is unused and rejected by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterType {
    Weak { concurrency_level: u32 },
    BoundedStrong { lower_bound: i64, upper_bound: i64 },
    UnboundedStrong,
}

/// A counter's configuration, passed to `CounterManager::define` and
/// returned by `CounterManager::get_configuration`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CounterConfiguration {
    pub counter_type: CounterType,
    pub initial_value: i64,
    pub storage: Storage,
}

const TYPE_UNBOUNDED_STRONG: u8 = 0x00;
const TYPE_WEAK: u8 = 0x01;
const TYPE_BOUNDED_STRONG: u8 = 0x02;
const TYPE_MASK: u8 = 0x03;
const PERSISTENT_BIT: u8 = 0x04;

/// Shared by `COUNTER_CREATE`'s request body and
/// `COUNTER_GET_CONFIGURATION`'s response body: both are the exact
/// same encoding (confirmed against `CounterEncodeUtil`'s matched
/// `encodeConfiguration`/`decodeConfiguration` pair).
pub(crate) fn encode_configuration(buf: &mut Vec<u8>, config: &CounterConfiguration) {
    let type_bits = match config.counter_type {
        CounterType::UnboundedStrong => TYPE_UNBOUNDED_STRONG,
        CounterType::Weak { .. } => TYPE_WEAK,
        CounterType::BoundedStrong { .. } => TYPE_BOUNDED_STRONG,
    };
    let persistent_bit = match config.storage {
        Storage::Volatile => 0,
        Storage::Persistent => PERSISTENT_BIT,
    };
    buf.push(type_bits | persistent_bit);
    match config.counter_type {
        CounterType::Weak { concurrency_level } => write_vint(buf, concurrency_level),
        CounterType::BoundedStrong {
            lower_bound,
            upper_bound,
        } => {
            buf.extend_from_slice(&lower_bound.to_be_bytes());
            buf.extend_from_slice(&upper_bound.to_be_bytes());
        }
        CounterType::UnboundedStrong => {}
    }
    buf.extend_from_slice(&config.initial_value.to_be_bytes());
}

pub(crate) async fn decode_configuration<R: AsyncRead + Unpin>(
    stream: &mut R,
) -> Result<CounterConfiguration> {
    let flags = stream.read_u8().await?;
    let storage = if flags & PERSISTENT_BIT != 0 {
        Storage::Persistent
    } else {
        Storage::Volatile
    };
    let counter_type = match flags & TYPE_MASK {
        TYPE_UNBOUNDED_STRONG => CounterType::UnboundedStrong,
        TYPE_WEAK => {
            let concurrency_level = read_vint(stream).await?;
            CounterType::Weak { concurrency_level }
        }
        TYPE_BOUNDED_STRONG => {
            let lower_bound = stream.read_i64().await?;
            let upper_bound = stream.read_i64().await?;
            CounterType::BoundedStrong {
                lower_bound,
                upper_bound,
            }
        }
        other => {
            return Err(Error::MalformedCounterConfiguration(format!(
                "unknown counter type bits: {other:#04x}"
            )))
        }
    };
    let initial_value = stream.read_i64().await?;
    Ok(CounterConfiguration {
        counter_type,
        initial_value,
        storage,
    })
}

/// A handle for defining and discovering counters, obtained from
/// `HotRodClient::counters`. None of its operations route by key (a
/// counter name is not a cache key), so, like `RemoteCache::query`,
/// every call here goes to the active seed.
pub struct CounterManager<'a> {
    client: &'a HotRodClient,
}

impl HotRodClient {
    /// Returns a handle for distributed counters
    /// (`docs/adr/0015-distributed-counters.md`).
    pub fn counters(&self) -> CounterManager<'_> {
        CounterManager { client: self }
    }
}

impl<'a> CounterManager<'a> {
    fn cache(&self) -> RemoteCache {
        self.client.cache(COUNTER_CACHE_NAME)
    }

    /// Defines `name` with `config` if it is not already defined.
    /// Returns `true` if the counter was created now, `false` if it
    /// was already defined: idempotent, not an error, confirmed
    /// against the Java client's own `CounterManager::defineCounter`.
    pub async fn define(&self, name: &str, config: CounterConfiguration) -> Result<bool> {
        self.cache().counter_define(name.to_string(), config).await
    }

    /// Does not error for a name that was never defined: it answers
    /// `false`, the same way the server itself distinguishes this
    /// one operation's "not defined" (a dedicated non-error status)
    /// from the `KeyDoesNotExist` every other counter operation uses.
    pub async fn is_defined(&self, name: &str) -> Result<bool> {
        self.cache().counter_is_defined(name.to_string()).await
    }

    /// `None` for a name that was never defined: unlike
    /// `StrongCounter::get_value`/`WeakCounter::get_value` and most
    /// other counter operations, this one does not error for that
    /// case (`docs/adr/0015-distributed-counters.md`), matching the
    /// same shape every other "might not exist" read in this crate
    /// already uses.
    pub async fn get_configuration(&self, name: &str) -> Result<Option<CounterConfiguration>> {
        self.cache()
            .counter_get_configuration(name.to_string())
            .await
    }

    /// Lists every counter name known to the cluster.
    pub async fn names(&self) -> Result<Vec<String>> {
        self.cache().counter_names().await
    }

    /// Returns a handle to a strong counter, without checking that
    /// `name` is actually defined as one (or defined at all): that
    /// only matters once an operation is actually called on it.
    pub fn strong_counter(&self, name: impl Into<String>) -> StrongCounter {
        StrongCounter {
            cache: self.cache(),
            name: name.into(),
        }
    }

    /// Same as `strong_counter`, but for a weak counter.
    pub fn weak_counter(&self, name: impl Into<String>) -> WeakCounter {
        WeakCounter {
            cache: self.cache(),
            name: name.into(),
        }
    }
}

/// A handle to one strong counter: atomic, and, if bounded, rejects
/// an update that would move it past its configured bound
/// (`Error::CounterOutOfBounds`). Obtained from
/// `CounterManager::strong_counter`.
#[derive(Clone)]
pub struct StrongCounter {
    cache: RemoteCache,
    name: String,
}

impl StrongCounter {
    /// The name this handle was obtained with, not a round trip to
    /// the server.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Fetches the counter's current value. `Error::CounterNotFound`
    /// if `name` was never defined.
    pub async fn get_value(&self) -> Result<i64> {
        self.cache.counter_get(self.name.clone()).await
    }

    /// Adds `delta` (negative to subtract) and returns the new
    /// value. `Error::CounterOutOfBounds` if this counter is bounded
    /// and the result would cross its configured bound (the value is
    /// left unchanged); `Error::CounterNotFound` if `name` was never
    /// defined.
    pub async fn add_and_get(&self, delta: i64) -> Result<i64> {
        self.cache
            .counter_add_and_get(self.name.clone(), delta)
            .await
    }

    /// Same as `add_and_get(1)`.
    pub async fn increment_and_get(&self) -> Result<i64> {
        self.add_and_get(1).await
    }

    /// Same as `add_and_get(-1)`.
    pub async fn decrement_and_get(&self) -> Result<i64> {
        self.add_and_get(-1).await
    }

    /// Swaps the counter's value to `update` only if it is currently
    /// `expect`, returning the value it had **before** the call, not
    /// a boolean: confirmed against the Java client's own
    /// `CompareAndSwapOperation`, which computes success client-side
    /// as `previous == expect`, exactly what `compare_and_set` below
    /// does.
    pub async fn compare_and_swap(&self, expect: i64, update: i64) -> Result<i64> {
        self.cache
            .counter_compare_and_swap(self.name.clone(), expect, update)
            .await
    }

    /// Same as `compare_and_swap`, but returns whether the swap
    /// happened instead of the counter's previous value.
    pub async fn compare_and_set(&self, expect: i64, update: i64) -> Result<bool> {
        Ok(self.compare_and_swap(expect, update).await? == expect)
    }

    /// Sets the counter's value to `value`, returning its previous
    /// value.
    pub async fn get_and_set(&self, value: i64) -> Result<i64> {
        self.cache
            .counter_get_and_set(self.name.clone(), value)
            .await
    }

    /// Resets the counter to its configured initial value.
    /// `Error::CounterNotFound` if `name` was never defined.
    pub async fn reset(&self) -> Result<()> {
        self.cache.counter_reset(self.name.clone()).await
    }

    /// Clears the counter's current value. Confirmed against a live
    /// server to **not** undefine the counter, despite its name: a
    /// read right after still sees it as defined
    /// (`CounterManager::is_defined`) and `get_value` returns its
    /// configured initial value again instead of
    /// `Error::CounterNotFound`, the same as if it had just been
    /// defined. There is no operation in this protocol that erases a
    /// counter's definition once made; this is as close as the wire
    /// format gets to "removing" one.
    pub async fn remove(&self) -> Result<()> {
        self.cache.counter_remove(self.name.clone()).await
    }
}

/// A handle to one weak counter: not atomic (reads may lag writes),
/// and does not expose `compare_and_swap`/`get_and_set`, the same
/// restriction the Java client's own `WeakCounter` has. Obtained from
/// `CounterManager::weak_counter`.
#[derive(Clone)]
pub struct WeakCounter {
    cache: RemoteCache,
    name: String,
}

impl WeakCounter {
    /// The name this handle was obtained with, not a round trip to
    /// the server.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Fetches the counter's current value. `Error::CounterNotFound`
    /// if `name` was never defined.
    pub async fn get_value(&self) -> Result<i64> {
        self.cache.counter_get(self.name.clone()).await
    }

    /// Adds `delta` without reporting the new value: confirmed
    /// against the Java client's own `WeakCounterImpl.add`, which
    /// calls the exact same wire operation as `StrongCounter::add_and_get`
    /// and simply discards the result.
    pub async fn add(&self, delta: i64) -> Result<()> {
        self.cache
            .counter_add_and_get(self.name.clone(), delta)
            .await?;
        Ok(())
    }

    /// Same as `add(1)`.
    pub async fn increment(&self) -> Result<()> {
        self.add(1).await
    }

    /// Same as `add(-1)`.
    pub async fn decrement(&self) -> Result<()> {
        self.add(-1).await
    }

    /// Resets the counter to its configured initial value.
    /// `Error::CounterNotFound` if `name` was never defined.
    pub async fn reset(&self) -> Result<()> {
        self.cache.counter_reset(self.name.clone()).await
    }

    /// Clears the counter's current value without undefining it: see
    /// `StrongCounter::remove`'s doc comment for the behavior
    /// confirmed against a live server.
    pub async fn remove(&self) -> Result<()> {
        self.cache.counter_remove(self.name.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn round_trip(config: CounterConfiguration) {
        let mut buf = Vec::new();
        encode_configuration(&mut buf, &config);
        let decoded = decode_configuration(&mut buf.as_slice())
            .await
            .expect("decode_configuration");
        assert_eq!(decoded, config);
    }

    #[tokio::test]
    async fn round_trips_an_unbounded_strong_volatile_counter() {
        round_trip(CounterConfiguration {
            counter_type: CounterType::UnboundedStrong,
            initial_value: 0,
            storage: Storage::Volatile,
        })
        .await;
    }

    #[tokio::test]
    async fn round_trips_a_weak_persistent_counter() {
        round_trip(CounterConfiguration {
            counter_type: CounterType::Weak {
                concurrency_level: 16,
            },
            initial_value: 42,
            storage: Storage::Persistent,
        })
        .await;
    }

    #[tokio::test]
    async fn round_trips_a_bounded_strong_counter_with_negative_bounds() {
        round_trip(CounterConfiguration {
            counter_type: CounterType::BoundedStrong {
                lower_bound: -100,
                upper_bound: 100,
            },
            initial_value: -5,
            storage: Storage::Volatile,
        })
        .await;
    }

    #[tokio::test]
    async fn rejects_the_reserved_type_bits() {
        let mut buf = Vec::new();
        buf.push(0x03); // reserved: both type bits set, volatile
        buf.extend_from_slice(&0i64.to_be_bytes());

        let err = decode_configuration(&mut buf.as_slice())
            .await
            .expect_err("0x03 type bits should be rejected");
        assert!(matches!(err, Error::MalformedCounterConfiguration(_)));
    }
}
