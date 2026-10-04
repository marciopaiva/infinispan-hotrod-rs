//! Client-side statistics, per `docs/adr/0010-client-statistics-and-tracing.md`
//! (issue #54): counts and average time for read/store/remove
//! operations, mirroring the Java client's `RemoteCache.
//! clientStatistics()`, plus connection pool state, which has no Java
//! counterpart. Always collected, no configuration flag: incrementing
//! a handful of atomics per operation is negligible next to the
//! network round trip it rides alongside.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// A running sum and count, not a histogram: the same choice the Java
/// client's `ClientStatistics` makes, enough for a current average
/// without the cost of tracking percentiles.
#[derive(Debug, Default)]
struct TimedCounter {
    count: AtomicU64,
    total_nanos: AtomicU64,
}

impl TimedCounter {
    fn record(&self, elapsed: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_nanos
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
    }

    fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// `Duration::ZERO` if nothing has been recorded yet, rather than
    /// dividing by zero.
    fn average(&self) -> Duration {
        let count = self.count();
        if count == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(self.total_nanos.load(Ordering::Relaxed) / count)
    }

    fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.total_nanos.store(0, Ordering::Relaxed);
    }
}

/// The shared, per-cache-name counters `ClientInner::cache_stats`
/// holds one of for every cache name a `RemoteCache` has been
/// obtained for. `RemoteCache::statistics`/`reset_statistics` are the
/// only public way to read or reset one.
#[derive(Debug, Default)]
pub(crate) struct CacheStatisticsInner {
    hits: AtomicU64,
    misses: AtomicU64,
    reads: TimedCounter,
    stores: TimedCounter,
    removes: TimedCounter,
    reset_at: RwLock<Option<Instant>>,
}

impl CacheStatisticsInner {
    pub(crate) fn record_read(&self, elapsed: Duration, hit: bool) {
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        self.reads.record(elapsed);
    }

    /// Same as `record_read`, but for a bulk read (`get_all`) that can
    /// contribute more than one hit or miss from a single call.
    pub(crate) fn record_bulk_read(&self, elapsed: Duration, hits: u64, misses: u64) {
        self.hits.fetch_add(hits, Ordering::Relaxed);
        self.misses.fetch_add(misses, Ordering::Relaxed);
        self.reads.record(elapsed);
    }

    pub(crate) fn record_store(&self, elapsed: Duration) {
        self.stores.record(elapsed);
    }

    pub(crate) fn record_remove(&self, elapsed: Duration) {
        self.removes.record(elapsed);
    }

    pub(crate) fn snapshot(&self) -> ClientStatistics {
        let reset_at = *self.reset_at.read().unwrap_or_else(|p| p.into_inner());
        ClientStatistics {
            remote_hits: self.hits.load(Ordering::Relaxed),
            remote_misses: self.misses.load(Ordering::Relaxed),
            average_remote_read_time: self.reads.average(),
            remote_stores: self.stores.count(),
            average_remote_store_time: self.stores.average(),
            remote_removes: self.removes.count(),
            average_remote_remove_time: self.removes.average(),
            time_since_reset: reset_at.map_or(Duration::ZERO, |at| at.elapsed()),
        }
    }

    pub(crate) fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.reads.reset();
        self.stores.reset();
        self.removes.reset();
        *self.reset_at.write().unwrap_or_else(|p| p.into_inner()) = Some(Instant::now());
    }
}

/// A point-in-time read of one cache's client-side statistics, from
/// `RemoteCache::statistics`. Mirrors the Java client's
/// `RemoteCache.clientStatistics()`, minus the near-cache fields:
/// see `NearCacheStatistics` and the ADR for why those live on
/// `NearCachedCache` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClientStatistics {
    pub remote_hits: u64,
    pub remote_misses: u64,
    pub average_remote_read_time: Duration,
    pub remote_stores: u64,
    pub average_remote_store_time: Duration,
    pub remote_removes: u64,
    pub average_remote_remove_time: Duration,
    /// How long it has been since the last `reset_statistics`, or
    /// since this cache's statistics were first created if that never
    /// happened.
    pub time_since_reset: Duration,
}

/// The shared hit/miss/invalidation counters `NearCachedCache` keeps
/// alongside its `LruStore`. Unlike `CacheStatisticsInner`, this is
/// not shared across clones by cache name through `ClientInner`: it
/// lives directly in the same `Arc` `NearCachedCache` already clones
/// around, since a near cache's statistics only make sense for that
/// specific wrapper (see the ADR for why this differs from the plain
/// per-cache-name statistics above).
#[derive(Debug, Default)]
pub(crate) struct NearCacheStatisticsInner {
    hits: AtomicU64,
    misses: AtomicU64,
    invalidations: AtomicU64,
    reset_at: RwLock<Option<Instant>>,
}

impl NearCacheStatisticsInner {
    pub(crate) fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_invalidation(&self) {
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self, size: usize) -> NearCacheStatistics {
        let reset_at = *self.reset_at.read().unwrap_or_else(|p| p.into_inner());
        NearCacheStatistics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            size,
            time_since_reset: reset_at.map_or(Duration::ZERO, |at| at.elapsed()),
        }
    }

    pub(crate) fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.invalidations.store(0, Ordering::Relaxed);
        *self.reset_at.write().unwrap_or_else(|p| p.into_inner()) = Some(Instant::now());
    }
}

/// A point-in-time read of a `NearCachedCache`'s own statistics, from
/// `NearCachedCache::statistics`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NearCacheStatistics {
    pub hits: u64,
    pub misses: u64,
    pub invalidations: u64,
    /// How many entries are cached locally right now: a live read of
    /// `LruStore`'s current length, not a counter, so it is already
    /// correct without needing `reset_statistics` to touch it.
    pub size: usize,
    pub time_since_reset: Duration,
}

/// One connection pool's state, from `HotRodClient::pool_statistics`.
/// This crate's own addition: the Java client has no equivalent, see
/// the ADR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolStatistics {
    pub address: SocketAddr,
    pub cache_name: String,
    pub idle_connections: usize,
    pub checked_out_connections: usize,
    pub max_connections: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timed_counter_average_is_zero_with_no_samples() {
        let counter = TimedCounter::default();
        assert_eq!(counter.average(), Duration::ZERO);
        assert_eq!(counter.count(), 0);
    }

    #[test]
    fn timed_counter_averages_recorded_samples() {
        let counter = TimedCounter::default();
        counter.record(Duration::from_millis(10));
        counter.record(Duration::from_millis(20));
        assert_eq!(counter.count(), 2);
        assert_eq!(counter.average(), Duration::from_millis(15));
    }

    #[test]
    fn timed_counter_reset_clears_count_and_average() {
        let counter = TimedCounter::default();
        counter.record(Duration::from_millis(10));
        counter.reset();
        assert_eq!(counter.count(), 0);
        assert_eq!(counter.average(), Duration::ZERO);
        counter.record(Duration::from_millis(5));
        assert_eq!(counter.average(), Duration::from_millis(5));
    }

    #[test]
    fn cache_statistics_tracks_hits_misses_and_average_times_separately() {
        let stats = CacheStatisticsInner::default();
        stats.record_read(Duration::from_millis(10), true);
        stats.record_read(Duration::from_millis(30), false);
        stats.record_store(Duration::from_millis(5));
        stats.record_remove(Duration::from_millis(7));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.remote_hits, 1);
        assert_eq!(snapshot.remote_misses, 1);
        assert_eq!(snapshot.average_remote_read_time, Duration::from_millis(20));
        assert_eq!(snapshot.remote_stores, 1);
        assert_eq!(snapshot.average_remote_store_time, Duration::from_millis(5));
        assert_eq!(snapshot.remote_removes, 1);
        assert_eq!(
            snapshot.average_remote_remove_time,
            Duration::from_millis(7)
        );
    }

    #[test]
    fn cache_statistics_bulk_read_adds_every_hit_and_miss_from_one_call() {
        let stats = CacheStatisticsInner::default();
        stats.record_bulk_read(Duration::from_millis(10), 3, 2);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.remote_hits, 3);
        assert_eq!(snapshot.remote_misses, 2);
        assert_eq!(
            snapshot.remote_stores, 0,
            "a read must not count as a store"
        );
    }

    #[test]
    fn cache_statistics_reset_zeroes_counts_without_breaking_later_averages() {
        let stats = CacheStatisticsInner::default();
        stats.record_read(Duration::from_millis(100), true);
        stats.reset();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.remote_hits, 0);
        assert_eq!(snapshot.average_remote_read_time, Duration::ZERO);

        stats.record_read(Duration::from_millis(10), true);
        assert_eq!(
            stats.snapshot().average_remote_read_time,
            Duration::from_millis(10)
        );
    }

    #[test]
    fn near_cache_statistics_tracks_hits_misses_invalidations_and_live_size() {
        let stats = NearCacheStatisticsInner::default();
        stats.record_hit();
        stats.record_hit();
        stats.record_miss();
        stats.record_invalidation();

        let snapshot = stats.snapshot(7);
        assert_eq!(snapshot.hits, 2);
        assert_eq!(snapshot.misses, 1);
        assert_eq!(snapshot.invalidations, 1);
        assert_eq!(snapshot.size, 7);
    }

    #[test]
    fn near_cache_statistics_reset_zeroes_counters_but_not_live_size() {
        let stats = NearCacheStatisticsInner::default();
        stats.record_hit();
        stats.reset();
        let snapshot = stats.snapshot(3);
        assert_eq!(snapshot.hits, 0);
        assert_eq!(snapshot.size, 3, "size is read live, not reset");
    }
}
