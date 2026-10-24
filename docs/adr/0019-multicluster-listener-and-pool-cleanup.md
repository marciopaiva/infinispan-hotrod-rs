# ADR 0019: Multi-cluster failover, listener/near cache cleanup and pool draining

## Status

Accepted, implemented in #115.

## Context

ADR 0017 (multi-cluster failover, #56) documented three gaps left out
of its own scope, in its "Consequences" section: no proactive
switch-back to a recovered cluster, listeners and near caches not
migrating on a switch, and the old cluster's connection pools not
being drained. Issue #115 tracked all three together. Research this
phase did, reading `client.rs`, `listener.rs`, `near_cache.rs` and
`pool.rs` again, found that the second item is not just a missing
feature: it is a real correctness risk, not named as one anywhere in
the original issue or ADR 0017 itself.

**The risk**: `HotRodClient::switch_to_cluster` (and automatic
failover) never touch an already-registered `CacheListener` or
`NearCachedCache`. A listener's dedicated connection
(`listener.rs`'s module docs) stays open and keeps receiving events
from whichever node it was registered against, which may be
perfectly healthy, just no longer the active cluster. A
`NearCachedCache` built before the switch keeps serving `get` from
its local store, populated by reads that, from the moment of the
switch onward, go to the *new* cluster (every ordinary `RemoteCache`
operation already routes there), but is only invalidated by events
from the *old* one. The two clusters have no reason to agree about
what changed, so this is worse than a dropped listener connection
(already handled, see below): here nothing drops, so the near cache
never notices its invalidation feed became irrelevant and keeps
trusting it, serving entries that nothing is actually keeping correct
anymore.

Pool draining is the second item. `record_topology_update` already
closes pools for nodes that left the topology, but only reacts once a
real topology update arrives for the new cluster, and deliberately
never touches the address that is the *active* seed at the time it
runs (that exception exists because, in that function's context, the
active seed never changes). A cluster switch changes exactly that
address, and nothing else in the client ever revisits it afterward:
the old cluster's seed pool leaks for the rest of the client's
lifetime.

Proactive switch-back (the third item) is a bigger, independent
feature: a background task polling the previously-active cluster
while a failover one stays healthy, with its own configuration for
the polling interval. ADR 0017 already noted the Java client does not
do this either. Deferred to its own issue
(#117), not bundled with the two
contained fixes below.

## Decision

This phase fixes the two contained items: listener/near cache
termination on a switch, and pool draining. Both reuse mechanisms this
crate already has; neither changes `hotrod-protocol`'s public API in
a way existing callers need to react to (one new `Error` variant,
additive).

**No migration, only termination.** A listener or near cache
registered against the cluster a switch leaves behind is not
reconnected against the new one. This matches the "no reconnection"
decision ADR 0006 already made for an ordinary dropped connection:
building an exception to it specifically for a cluster switch would
contradict that decision rather than extend it. Terminating the
listener promptly, instead of leaving it to silently trust a now
disconnected-in-spirit feed, is what actually closes the correctness
risk; a caller that wants continuity registers a new listener (or
near cache) against the now-active cluster, the same as it already
has to for a dropped connection.

## Design

### Cluster-switch generation (`client.rs`)

`ClientInner` gains `cluster_generation: tokio::sync::watch::Sender<u64>`
(the `sync` feature of the existing `tokio` dependency; no new
dependency). Bumped by one (`send_modify`) in exactly the two places
that already replace `ActiveCluster` wholesale: `switch_to_cluster`,
and `try_promote_seed`'s `switch_to_cluster_name: Some(_)` branch
(automatic failover). `HotRodClient::subscribe_cluster_generation`
(`pub(crate)`) hands out a `watch::Receiver` to whoever needs to
watch for a later switch.

### `CacheListener` reacts to a switch (`listener.rs`)

`CacheListener::register` takes a `watch::Receiver<u64>`, captured at
registration time. `next()` already documents that it is designed to
be raced externally with `tokio::select!` ("not bounded by this
listener's timeout ... allows pairing with `tokio::select!` or an
external timeout"), so the race is built into `next()` itself instead
of asking every caller to repeat it:

```rust
let magic = tokio::select! {
    biased;
    Ok(()) = self.generation_rx.changed() => {
        self.poisoned = true;
        return Some(Err(Error::ListenerClusterSwitched));
    }
    byte = self.stream.read_u8() => match byte { /* unchanged */ },
};
```

New error variant: `Error::ListenerClusterSwitched`. The connection
itself is never touched; this client simply stops reading from it,
the same terminal, no-reconnection outcome a dropped connection
already produces (`Error::PoisonedConnection` on every call after
the first).

`biased` polls the generation branch first: if a switch already
happened, this is caught immediately rather than risking one more
stale event being read first. If the client (and its
`cluster_generation` sender) has been dropped entirely,
`changed()` resolves to an `Err` that the `Ok(())` pattern does not
match; tokio's own `select!` disables that branch for the rest of
that call instead of repolling an `Err` that would otherwise resolve
again immediately, so this never busy-loops.

**`near_cache.rs` needed no change at all.** Its background
invalidation task already treats `None | Some(Err(_)) => break`
identically, triggering the existing `mark_dead` fail-safe (the same
one a dropped connection already triggers). Fixing `next()` fixes the
near cache for free: `RemoteCache::listen_with` already passes
`self.client.subscribe_cluster_generation()` through to
`CacheListener::register`, and `NearCachedCache::register` goes
through `listen_with`, not a separate path.

### Pool draining (`client.rs`)

The pool-closing logic `record_topology_update` already had (remove
stale `node_origin` entries, `close()` and drop the matching pools)
is extracted into `evict_pools_for(&self, addrs: &[SocketAddr])`,
unchanged in behavior for that caller. A second helper,
`evict_abandoned_cluster(&self, old: &ActiveCluster)`, collects the
old cluster's own seed list plus whatever topology addresses had
already been resolved against it, and calls `evict_pools_for` with
that full list, no "active seed" exception: by the time this runs,
`old`'s own seed is not active anymore at all. Called from
`switch_to_cluster` and from `try_promote_seed`'s switch branch, right
after each replaces the old `ActiveCluster`.

Known limitation, accepted rather than engineered around: if two
clusters happen to share an address (an unusual configuration), that
address's pool is closed on a switch away even though it might still
be relevant. The Java client's own "discard and rediscover" switch
behavior does not special-case this either.

## Consequences

* New: `Error::ListenerClusterSwitched`. Additive; nothing existing
  changes shape.
* A `CacheListener` (raw, or inside a `NearCachedCache`) registered
  against a cluster a switch leaves behind now terminates promptly
  instead of silently continuing to receive events, or (for a near
  cache) silently continuing to trust an invalidation feed that no
  longer has anything to do with the cluster `get` now reads from.
  Confirmed against a fake server (`listener::tests::next_returns_a_terminal_error_once_the_cluster_generation_changes`,
  `near_cache::tests::wiring::switching_clusters_stops_the_local_cache_from_being_used`)
  and against two real, independent servers
  (`multicluster_switch_to_cluster_terminates_a_listener_on_the_old_cluster`,
  `ci/infinispan-multicluster`).
* `switch_to_cluster`/automatic failover now drain every pool the
  abandoned cluster owned, including its own former active seed.
  Confirmed against a fake server
  (`client::tests::switch_to_cluster_bumps_generation_and_drains_the_old_cluster_s_pools`):
  `pool_statistics()` no longer lists the old cluster's pool after a
  switch.
* Not implemented, deferred to its own issue (#117): proactively
  switching back to a recovered cluster while a failover one stays
  healthy. Needs a background polling task and its own configuration,
  a bigger and more independent decision than either fix above;
  the Java client does not do this either.
