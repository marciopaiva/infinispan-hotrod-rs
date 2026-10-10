# ADR 0011: Retry policy and node health tracking

## Status

Accepted, implemented in #97.

## Context

The roadmap's "Reliability and observability" theme flagged this as
the last item with no issue filed: "an operation-aware retry policy in
particular, since blindly retrying `put`/`replace`/the versioned
operations risks silent duplicate writes". Before this phase,
`RemoteCache::call`/`call_seed`/`run_seed_op`/`failover_and_retry`
retried an operation exactly once, against a single fallback target,
on `Error::Io`/`Error::Timeout`. No retry count was configurable, and
`ClientInner` tracked no per-node health: a dead node cost the full
client timeout on every operation routed to it, with no memory of the
previous failure.

The design is pinned against the Java client's own source
(`infinispan/infinispan`, `client/hotrod-client`), the same discipline
every prior phase used.

**`supportRetry()`** (`HotRodOperation`/`AbstractHotRodOperation`) is
binary per operation class, not "idempotent vs. conditional":
`put`, `putIfAbsent`, `replace`, `replaceIfUnmodified`,
`removeIfUnmodified`, `remove`, `get` and every other data operation
inherit `true`. Only operations tied to a specific cursor or
connection (streaming, iteration) return `false`, which already
matches what this client does: those never go through `call`.

**The real safety does not come from the operation type, it comes
from whether the server ever answered.**
`OperationDispatcher.checkException` only refuses a retry once the
failure already carries a real `errorStatusCode` from the server
(`isServerError()`); a transport failure (`TransportException`, no
status code) is always retryable, regardless of operation type. This
is exactly the gate this client already used (`Error::Io`/
`Error::Timeout`), confirmed correct by the Java source rather than
changed.

`maxRetries` (`ConfigurationProperties.DEFAULT_MAX_RETRIES = 3`): each
retry goes to another node via round-robin, excluding nodes already
tried in this chain, with no delay between attempts.

**The real circuit breaker**:
`OperationDispatcher.connectionFailedServers`, a set backed by a
Caffeine cache with `expireAfterWrite` (TTL), config
`serverFailureTimeout` (default 30000 ms, `-1` disables it). A single
connection failure is enough to enter the set; it clears on the next
success against that address or when a new topology arrives. No
consecutive-failure count.

Once `maxRetries` is exhausted, the Java client propagates the error
from the *last* attempt, not a synthetic one. Its own documentation
(`con_hotrod_failover.adoc`) acknowledges the residual ambiguity on
conditional operations under failover and recommends a transactional
cache for callers who need a stronger guarantee, rather than a
smarter retry policy at the protocol level.

## Decision

**Mirror the Java client's retry safety gate: no differentiation by
operation type.** `put`/`replace`/the versioned operations stay
retryable under `Error::Io`/`Error::Timeout`, exactly as before this
phase. The residual ambiguity (a timed-out request that the server
had, in fact, already applied) is documented as an accepted
limitation below, the same way the Java client's own documentation
does, rather than inventing a stricter semantics with no precedent in
the reference client.

**Try backup owners before falling back to the seed, an addition of
its own.** `ClusterTopology::segment_owners` already carries each
segment's backup owners; `owner_addr` only ever used the first. The
Java client's balancer is not key-hash-aware, so it has no way to
make this choice; this client already does the hashing to find the
primary owner, so resolving the rest of the same list costs nothing
extra. Dispatching to a non-primary owner is not a new correctness
risk: it is exactly what already happens when a call falls back to
the seed.

**`NodeHealth` (new module `health.rs`), a TTL-based circuit breaker,
no new dependency.** A `HashMap<SocketAddr, Instant>` behind an
`RwLock` plays the same role as the Java client's Caffeine-backed
`connectionFailedServers` at this scale (one client process, not
thousands of tracked servers): `mark_failed`, `is_quarantined`,
`clear`, `clear_all`. `ClientInner` gains `node_health: NodeHealth`
and `server_failure_timeout: RwLock<Option<Duration>>` (default
`Some(Duration::from_secs(30))`, matching the Java default;
`None` disables quarantine, the idiomatic equivalent of its `-1`).

**`max_retries` configurable, default 3, same as the Java client.**
`ClientInner` gains `max_retries: RwLock<usize>`; `HotRodClient` gains
`max_retries()`/`set_max_retries()` and
`server_failure_timeout()`/`set_server_failure_timeout()`, the same
getter/setter shape `timeout()`/`set_timeout()` already uses.

**`call`/`call_seed` rewritten over one generalized retry chain
(`RemoteCache::dispatch`).** Candidates, in order: for a keyed
operation, the primary owner, then each backup owner, then the active
seed; for a seedless operation, just the active seed. Once that list
is exhausted, `failover_seed` extends it, and can be called more than
once in the same chain as `active_seed_addr` keeps advancing through
the configured seed list. A candidate currently quarantined is
skipped when building the list, unless skipping it would leave
nothing to try at all: refusing to attempt anything just because
every known node recently failed would be worse than trying one that
might have recovered since. An address already tried in this chain is
never tried again, even one `failover_seed` offers again: without
that, a cluster with only two seeds both down could bounce between
them instead of exhausting `max_retries` and stopping. Exhausted
retries or candidates return the last real operation error, never
`failover_seed`'s own connection error, matching the original
`failover_and_retry`'s behavior.

**Quarantine clears on success, and entirely on a real topology
change.** `dispatch` clears an address's quarantine the moment an
attempt against it succeeds. `record_topology_update` clears every
quarantine (`node_health.clear_all()`) when the arriving topology id
differs from the one currently held: a topology update already means
the cluster's membership view changed, so stale quarantine state
should not outlive it.

## Consequences

* New public API: `HotRodClient::max_retries`/`set_max_retries`,
  `HotRodClient::server_failure_timeout`/
  `set_server_failure_timeout`. Additive: nothing existing changes
  shape, and the new defaults (`max_retries = 3`,
  `server_failure_timeout = Some(30s)`) reproduce the same two-attempt
  outcome the old hardcoded chain had whenever there is nothing beyond
  the seed to fail over to, which is what every existing failover test
  already exercises.
* `remote_cache.rs`'s `call`/`call_seed`/`run_seed_op`/
  `failover_and_retry` collapse into `call`/`call_seed`/`dispatch`.
  `client.rs` gains `owner_and_backup_addrs`, used only by `dispatch`:
  `owner_addr` (used by `get_stream`/`put_stream`, which pin to one
  connection and never retry) is left as is rather than built on top
  of it, to avoid resolving backup addresses those two never use.
* The residual ambiguity the Java client's own documentation already
  accepts is unchanged here: a conditional operation
  (`put_if_absent`/`replace_if_unmodified`/`remove_if_unmodified`)
  that times out may have already been applied on the server before
  the response was lost, and a retry against the next candidate cannot
  tell the difference. Not solved by this phase, the same way it is
  not solved by the Java client; a caller needing a stronger guarantee
  still wants a transactional cache (#47), not a smarter retry.
* A node that fails once is skipped for up to `server_failure_timeout`
  even if it would have recovered a moment later, on every operation
  routed to it, not just the one that first observed the failure.
  Accepted: the alternative (a consecutive-failure count before
  quarantining) is not what the Java client does either, and this
  phase chose to mirror it rather than invent an unproven policy.
* Not implemented, left for later if real usage shows it matters:
  round-robin fallback across every node the topology lists, not just
  the configured seeds, once backup owners and the configured seeds
  are both exhausted; backoff between attempts (the Java client has
  none either); anything about multi-cluster/cross-site failover,
  which is the separate #56.
