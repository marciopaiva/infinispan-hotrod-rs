# ADR 0017: Multi-cluster failover

## Status

Accepted, implemented in #56.

## Context

The last item in the "High availability" roadmap theme, issue #56
(already filed): configuring one or more alternate clusters for
disaster recovery, with the client switching to one automatically if
the active cluster becomes entirely unreachable, or on demand.

The design is pinned against the real Java client source
(`infinispan/infinispan`), the same discipline every prior phase
used.

**Entirely client-side, no new opcodes.** Confirmed in
`OperationDispatcher`/`ConfigurationBuilder`/`ClusterConfigurationBuilder`:
switching clusters is nothing more than closing the connections open
to the old one and reconnecting to the new one's own configured seed
addresses. The wire protocol has no concept of "cluster" at all.

**Configuration and manual switching (Java API)**:
`ConfigurationBuilder.addCluster(name).addClusterNodes(...)` at
configuration time; `RemoteCacheManager.switchToCluster(name)` at
runtime. The cluster this client was originally built with is just
another entry in the same internal list, under a reserved sentinel
name (`DEFAULT_CLUSTER_NAME`), so switching back to it later uses the
exact same mechanism as switching to any other configured cluster.

**Automatic trigger**: `OperationDispatcher.handleConnectionFailure`
accumulates addresses that failed to connect; once that set covers
every seed of the active cluster and at least one alternate is
configured, it tries each alternate in turn (`findLiveCluster0`,
confirmed via a `NoCachePingOperation`) until one responds, then
switches to it.

**No proactive switch-back.** The original cluster participates in
the exact same mechanism as any other configured one if the
currently active cluster later dies; nothing polls to see if it has
recovered while a failover cluster stays healthy.

**Topology is discarded on a switch.** `TopologyInfo.switchCluster`
resets every known cache's topology to the new cluster's bare seed
list, confirmed via a sentinel topology id (`SWITCH_CLUSTER_TOPOLOGY
= -2`) distinct from "no topology yet" (`-1`): the client does not
carry over the old cluster's segment ownership, and does not already
know the new cluster's either, until a real operation's response
brings one.

**Authentication, TLS and intelligence are shared globally**, not
configurable per cluster in the Java client beyond an optional SNI
hostname override (a TLS detail, out of scope here).
`ClientIntelligence` is not even configurable per client in this
crate yet (`HotRodClient` always dials `HashDistributionAware`,
confirmed in `connection.rs::connect_hash_aware`), so a per-cluster
override would have nothing to attach to.

**No dedicated error for "every cluster failed."** `findLiveCluster0`
just returns nothing found; the operation that triggered the whole
check falls through to the ordinary retry-exhausted path it would
have hit anyway.

## Decision

**Both automatic and manual failover are in scope**, reusing the
retry chain `docs/adr/0011-retry-policy-and-node-health.md` already
built rather than adding a second, parallel one.

**`switch_to_cluster` switches blind**, synchronously, with no
liveness check against the target: matching the Java client's own
`manualSwitchToCluster`, not its separately liveness-checked
`switchToCluster`. An unreachable target surfaces through the next
operation's own retry chain (which also tries every other configured
cluster automatically), the same as any other connection failure
would.

## Design

### `ClientInner` (`client.rs`)

`seed_addrs` changes from a plain `Vec<SocketAddr>` to
`RwLock<Vec<SocketAddr>>`: it now reflects whichever cluster is
currently active, and `switch_to_cluster`/the automatic failover
below replace it wholesale on a switch. Two new fields:

```rust
pub(crate) clusters: RwLock<Vec<(String, Vec<SocketAddr>)>>,
pub(crate) active_cluster_name: RwLock<String>,
```

`clusters` holds every cluster this client knows about, including
the one it was constructed with, under
`HotRodClient::DEFAULT_CLUSTER_NAME`, added automatically by
`connect`/`connect_with_timeout`/`connect_tls`/
`connect_tls_with_timeout`.

### Public API (`HotRodClient`)

```rust
impl HotRodClient {
    pub const DEFAULT_CLUSTER_NAME: &'static str = "__default__";

    pub fn add_cluster(&self, name: impl Into<String>, seed_addrs: Vec<SocketAddr>) -> Result<()>;
    pub fn switch_to_cluster(&self, name: &str) -> Result<()>;
    pub fn active_cluster_name(&self) -> String;
}
```

Plain synchronous getter/setter methods directly on `HotRodClient`,
matching `max_retries`/`set_max_retries`/`server_failure_timeout`/
`set_server_failure_timeout` (ADR 0011): no separate builder type,
consistent with how every other piece of this client's runtime
configuration already works.

`add_cluster` rejects an empty `seed_addrs` and a `name` already in
use (including `DEFAULT_CLUSTER_NAME` itself) with
`Error::InvalidClusterConfig`. `switch_to_cluster` rejects an
unconfigured `name` with `Error::UnknownCluster`; on success it
replaces `seed_addrs`/`active_seed_addr` (the new cluster's first
seed) /`active_cluster_name`, and resets `topology` to `None`, the
same "nothing carries over" rule the Java client's sentinel topology
id encodes.

### Automatic failover

`failover_seed` and the new `try_failover_to_live_cluster` share one
helper, `try_promote_seed`, generalizing what `failover_seed` already
did (dial, authenticate, seed the pool, promote `active_seed_addr`)
to optionally also switch clusters when it succeeds:
`failover_seed` passes `switch: None` (same cluster, just another of
its own seeds); `try_failover_to_live_cluster` passes
`Some((name, seeds))` for whichever alternate cluster it is
currently trying.

`try_failover_to_live_cluster` iterates every entry in `clusters`
except the currently active one, in the order `add_cluster` was
called, trying each one's seeds in turn via `try_promote_seed` until
one responds. On success, it emits a `tracing` `WARN` event (`from_cluster`,
`to_cluster`) for visibility, since nothing about the operation that
triggered it would otherwise indicate a whole cluster was just
abandoned.

`remote_cache.rs`'s `dispatch` already tries `failover_seed` as one
more candidate before giving up; this adds exactly one more fallback
in the same place, right before the final `exhausted_candidates_error`:
if `try_failover_to_live_cluster` finds a live alternate, its address
becomes the next candidate and the same retry loop continues,
without consuming an extra attempt for the switch itself (only the
real connection attempt that follows does, same as any other
candidate). Since this lives in `dispatch`, it covers every operation
that goes through it uniformly, `call` and `call_seed` alike,
including query/administration/counters/multimap, with no change
needed in any of those modules.

### Test fixture

`ci/infinispan-multicluster/` (new): two genuinely independent
single-node servers, no shared Docker network or JGroups discovery
between them, unlike `ci/infinispan-cluster/`'s two nodes of one
logical cluster. A multi-cluster failover test against two nodes that
already share one cluster's data could pass by accident, since both
would show the same distributed content regardless of whether
failover actually switched anything; `multicluster_*` live tests
confirm real independence instead (a key written to one is absent
from the other). Wired into `ci.yml` as its own `multicluster-test`
job, the same pattern `cluster-test`/`tls-test` already use;
`live-test`'s and `release.yml`'s selections now also skip
`multicluster_` tests, alongside `cluster_`/`tls_`.

## Consequences

* New public API: `HotRodClient::add_cluster`/`switch_to_cluster`/
  `active_cluster_name`/`DEFAULT_CLUSTER_NAME`,
  `Error::UnknownCluster`, `Error::InvalidClusterConfig`. Additive:
  nothing existing changes shape, though `ClientInner::seed_addrs`'s
  internal type changed (pub(crate) only, no public surface affected).
* Confirmed against two real, independent live servers
  (`ci/infinispan-multicluster`): `add_cluster`/`switch_to_cluster`
  move traffic to the second server and back, each cluster's own
  cache state staying genuinely independent.
* Confirmed against a fake server: the automatic trigger inside
  `dispatch` finds and switches to a live alternate cluster when
  every seed of the active one refuses the connection, completing
  the original operation the caller issued without any special
  handling on their part.
* **Not hash-routed or topology-aware for the failover decision
  itself.** `try_failover_to_live_cluster` just dials each
  alternate's configured seeds in turn; it does not consult or
  preserve any topology, matching the Java client's own "discard and
  rediscover" behavior on a switch.
* Not implemented, left for later if it turns out to matter:
  per-cluster authentication, TLS or `ClientIntelligence` overrides
  (none of the three are configurable per cluster in the Java client
  beyond SNI hostname, and `ClientIntelligence` is not configurable
  per client at all in this crate yet); proactively switching back to
  a recovered cluster while a failover cluster stays healthy (the
  Java client does not do this either); migrating existing listeners
  or near caches to the new cluster on a switch (their own dedicated
  connections are untouched, the same gap the Java client's own
  research did not rule out either); actively draining the old
  cluster's now-idle connection pools (they just sit unused, cheap to
  keep in case of a later switch back).
