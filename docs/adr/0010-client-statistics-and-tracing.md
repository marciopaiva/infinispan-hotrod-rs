# ADR 0010: Always-on client statistics, local-only tracing

## Status

Accepted, implemented in #54.

## Context

#54 asks for two things the Java client has and `hotrod-protocol`
does not: client-side statistics (`RemoteCache.clientStatistics()`)
and OpenTelemetry-based tracing (`telemetry/`), on top of the
server-side `stats` operation this crate already exposes. The issue
flags this directly: "Needs its own ADR: what to instrument and which
crate (if any) to depend on for tracing (likely `tracing`)."

The design is pinned against the Java client's own source
(`infinispan/infinispan`, `client/hotrod-client`), the same discipline
every prior phase used.

**Statistics** (`jmx/RemoteCacheClientStatisticsMXBean.java`,
implemented in `impl/ClientStatistics.java`): plain sum-and-count
counters (`AtomicLong`, not a histogram) for `remoteHits`/
`remoteMisses` plus an average read time, `remoteStores` plus an
average store time, `remoteRemoves` plus an average remove time, and
`nearCacheHits`/`nearCacheMisses`/`nearCacheInvalidations`/
`nearCacheSize`. `resetStatistics()` and `getTimeSinceReset()` round
it out. Instrumentation is a decorator: `StatsOperationsFactory`
wraps each relevant `HotRodOperation` in a `StatisticOperation`,
timing `writeOperationRequest`→`createResponse`. Covered: get,
getWithMetadata, remove, put, putIfAbsent, replace,
replaceIfUnmodified, removeIfUnmodified, putAll, getAll.
**`containsKey` is explicitly commented as not instrumented;
`size`/`clear`/`stats`/`ping` are left out too.** Collection only
happens at all if `statistics().enabled()` is on (default off): when
off, the decorator is never installed, a real zero cost, not just a
per-call flag check.

**"Telemetry"** (`telemetry/impl/`): **does not create spans**. It
only propagates an *already active* OpenTelemetry context
(`Context.current()`, assuming some auto-instrumentation agent
already started a span) as a W3C traceparent, sent as an additional
request parameter purely so the server can correlate. Covers remove,
removeIfUnmodified, replaceIfUnmodified, put, putIfAbsent, replace,
clear, size — **not get/getAll/putAll**, and the code itself has
open questions about streaming and listeners. `opentelemetry-api` is
an optional Maven dependency, detected by reflection, with a no-op
fallback.

## Decision

**Statistics are always on, no configuration flag.** Unlike the Java
client's `statistics().enabled()` gate (default off, a real
zero-cost-when-disabled decorator that is never installed),
incrementing a handful of `AtomicU64`s per operation costs
nanoseconds, negligible next to a network round trip that costs tens
of microseconds at best. Always collecting is simpler to implement,
simpler to use (no configuration surface, no "did I forget to enable
this" failure mode), and the cost difference from the Java approach
is not measurable against the network.

**Tracing is local spans only, via the `tracing` crate, with no
propagation to the server.** What the Java client calls "telemetry"
is a different feature: W3C traceparent propagation of a context an
external agent already created, with partial, self-admittedly
incomplete coverage (not get/getAll/putAll), requiring a real
wire-protocol extension (an additional request parameter this crate
has never sent) plus an optional `opentelemetry`/
`tracing-opentelemetry` dependency. `tracing` (already an indirect
dependency through Tokio itself) gives local spans and events an
embedding application's own subscriber can use for log correlation
and local latency measurement, independent of whether the Infinispan
server is itself instrumented. Cross-process propagation is a
different, bigger feature, left for its own proposal if it turns out
to matter.

**Statistics are scoped per cache name, not aggregated across the
whole client.** Mirrors `RemoteCache.clientStatistics()`:
`ClientInner` gains `cache_stats: RwLock<HashMap<String,
Arc<CacheStatisticsInner>>>`, created lazily the first time a cache
name is seen, the same pattern `pools` (keyed by `(SocketAddr,
String)`) already uses.

**Instrumented set matches the Java client's exactly.** `get`/
`get_with_version` as reads (hit if `Some`, miss if `None`); `put`/
`put_if_absent`/`replace`/`replace_if_unmodified`/`put_all` as
stores; `remove`/`remove_if_unmodified` as removes; `get_all` as a
read (hits = keys found, misses = keys requested minus keys found, a
natural extension of the single-key notion the Java client does not
spell out in as much detail but does not contradict either).
`contains_key`/`ping`/`size`/`clear`/`stats` are left out, matching
the Java client. Streaming (#52) and server-side iteration (#53) are
out of scope for this phase, the same cut the Java client makes for
its own streaming ("handled separately, only if enabled") — revisit
if it turns out to matter.

**Only a successful operation counts.** An error (timeout, I/O,
server-reported failure) adds to neither the count nor the average
time: this measures the cost of an operation that worked, not one
that failed trying. A bulk call (`put_all`/`get_all`) adds 1 per
call, not 1 per entry in the batch: it measures the call's own cost,
the same granularity the Java decorator wraps (one `HotRodOperation`,
not each entry inside it).

**Near cache gets its own statistics snapshot, not a field bolted
onto the same `ClientStatistics` the Java client uses.** Unlike Java,
where near caching is intrinsic configuration on the same
`RemoteCacheManager`, this crate's `NearCachedCache` (ADR 0007) is a
distinct wrapper: not every `RemoteCache` has one, and more than one
`NearCachedCache` with different options can wrap clones of the same
`RemoteCache`. Hits/misses/invalidations live in the same shared
state `NearCachedCache` already keeps (the same `Arc` the `LruStore`
and generation counter use), not forced into `ClientInner` keyed by
cache name.

**Pool state is this project's own addition, with no Java
counterpart.** The roadmap already asked for "connection and pool
state"; `pool.rs` already has the raw data (`slots`), so this just
exposes it: `HotRodClient::pool_statistics()`, kept separate from the
per-cache `ClientStatistics` because the granularity differs (per
`(node, cache)`, not per cache alone).

## Consequences

* New public API: `RemoteCache::statistics`/`reset_statistics`
  (`ClientStatistics`), `NearCachedCache::statistics`/
  `reset_statistics` (`NearCacheStatistics`), `HotRodClient::
  pool_statistics` (`Vec<PoolStatistics>`). Additive: nothing
  existing changes shape.
* New dependency: `tracing`. No new dependency for statistics itself
  (`std::sync::atomic` and `std::time::Instant` suffice).
* `pool.rs`'s `ConnectionPool` gains a stored `max_size` field (today
  only passed through to the semaphore and the initial vec, never
  kept) so `pool_statistics` has something to subtract the live idle
  count from.
* Not implemented, left for a later, independent, additive change if
  it turns out to matter: cross-process trace-context propagation to
  the server (what the Java client actually calls "telemetry"),
  statistics for streaming (#52) and server-side iteration (#53).
