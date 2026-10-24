# ADR 0020: Multi-cluster failover, proactive switch-back

## Status

Accepted, implemented in #117.

## Context

Last item split out of #115 (`docs/adr/0019-multicluster-listener-and-pool-cleanup.md`),
which fixed the other two gaps ADR 0017 had left open: listener/near
cache termination and pool draining on a switch. This one was kept
separate because it is a genuinely new, independent piece of
behavior, not a cleanup of something the client already does: today,
once `HotRodClient::try_failover_to_live_cluster` moves traffic to an
alternate cluster, nothing ever checks whether the originally active
one has recovered. The only way back is a manual `switch_to_cluster`
or another automatic failover. ADR 0017 already noted the Java client
does not do this either, so building it is a deliberate addition,
not parity-chasing.

Research this phase did, reading `client.rs` again, found that no
new protocol piece or even much new logic is needed: `try_promote_seed`
(the private helper `switch_to_cluster` and
`try_failover_to_live_cluster` already share, see ADR 0017's own
design section) already does everything a switch back needs: dial,
authenticate, promote the seed, replace `ActiveCluster` under the
right lock, drain the old cluster's pools, bump `cluster_generation`
(ADR 0019). The only genuinely new thing is *when* to call it again,
pointed the other way.

## Decision

**A background monitor starts only after a real automatic failover,
never after a manual `switch_to_cluster`.** A manual switch is a
deliberate choice; proactively undoing it because the cluster the
caller left recovered would fight that choice instead of respecting
it. This matches the issue's own framing: the gap is specifically
about the client not reacting on its own after something it decided
on its own (an automatic failover), not about second-guessing the
caller.

**Enabled by default, 60 second interval, confirmed with the
maintainer.** The monitor only ever runs after a real failover has
already happened, so a client that never experiences one pays
nothing for this; once one does, a client with no opinion on the
matter benefits from recovering on its own rather than staying on
the failover cluster forever by default.

## Design

### Reusing `try_promote_seed`

`try_failover_to_live_cluster` already knows, at the exact moment it
switches successfully, the name and seed list of the cluster it is
abandoning (`active_name`, found in its own already-cloned `clusters`
snapshot). Right after its existing `tracing::warn!`, it now also
spawns a background task holding only a `Weak<ClientInner>` (never an
`Arc`: see "Lifecycle" below) and that cluster's own name and seeds,
plus the `cluster_generation` value current at that moment.

```rust
async fn run_switch_back_monitor(
    weak: Weak<ClientInner>,
    target_name: String,
    target_seeds: Vec<SocketAddr>,
    started_generation: u64,
) {
    loop {
        // read switch_back_check_interval; None or the client gone -> return
        // sleep(interval)
        // re-read: still enabled, and cluster_generation unchanged -> else return
        // try_promote_seed("", &target_seeds, None, Some(&target_name))
        //   Ok(_) -> warn! and return (done)
        //   Err(_) -> loop again
    }
}
```

`cache_name = ""` for the reachability dial, the same convention
`HotRodClient::connect_with` already uses for its own seed-liveness
check at construction time: neither is tied to any specific cache.

### Cancellation is entirely passive

`cluster_generation` (ADR 0019) already changes on every switch,
manual or automatic. The monitor compares the current value against
the one it captured at birth; a mismatch means some other switch
already happened, so it gives up. Neither `switch_to_cluster` nor
`try_failover_to_live_cluster` needs to know a monitor exists to
cancel one: a later switch of any kind automatically supersedes an
earlier monitor's reason to keep trying.

This also naturally handles a second automatic failover happening
before the first monitor succeeds (A fails over to B, then B also
fails over to C): the stale monitor (still aimed at A) sees the
generation bumped again by the B-to-C switch and exits, instead of
racing the new monitor (aimed at B) or switching somewhere no longer
relevant.

### Disabling takes effect at the next wake, not after one more attempt

`ClientInner::switch_back_check_interval` is read fresh both before
the sleep (to know how long to wait) and again right after waking
(alongside the generation check), rather than only once per loop
iteration. The first version only checked it before sleeping, so
disabling the monitor while it was already asleep would still let
one more switch-back attempt through before the next iteration's
check caught it. Checking again immediately after the sleep closes
that gap: `set_switch_back_check_interval(None)` stops a monitor
already in flight before its next attempt, not after one more.

### Lifecycle: `Weak`, not `Arc`

The monitor holds `Weak<ClientInner>`, upgraded fresh on every wake.
Unlike `near_cache.rs`'s invalidation task (which deliberately keeps
its state alive via a strong `Arc` for as long as any
`NearCachedCache` clone exists, with an explicit `AbortOnDrop`), this
task must never be the reason a `HotRodClient` and everything it
owns outlives every clone of it. `weak.upgrade()` failing is the
monitor's own signal to stop; no `AbortOnDrop` or other explicit
cancellation handle is needed, since there is no cycle to break.

### Public API (`HotRodClient`)

```rust
pub fn switch_back_check_interval(&self) -> Option<Duration>;
pub fn set_switch_back_check_interval(&self, interval: Option<Duration>);
```

Plain synchronous getter/setter, matching
`max_retries`/`set_max_retries` and
`server_failure_timeout`/`set_server_failure_timeout` (ADR 0011): no
builder, no new `connect*` parameter. `DEFAULT_SWITCH_BACK_CHECK_INTERVAL`
is 60 seconds, the same order of magnitude as
`DEFAULT_SERVER_FAILURE_TIMEOUT`.

### Test coverage: why no live-server test

`ci/infinispan-multicluster/`'s own existing live test
(`multicluster_switch_to_cluster_moves_traffic_to_an_independent_server`)
already documents why it only exercises the manual switch path: the
automatic trigger needs a node to actually stop responding, and
nothing in that test harness can stop a running container. The same
constraint applies here, more so: exercising this phase for real
would also need the "dead" node to start responding again partway
through, with no tool available to make that happen against real
infrastructure in this environment. Fake-server coverage (below)
exercises the exact same code path (`try_promote_seed`) the live
servers would run, with the one part that differs (dialing a real
socket) already covered, unchanged, by the existing live
multi-cluster test.

## Consequences

* New public API: `HotRodClient::switch_back_check_interval`/
  `set_switch_back_check_interval`. Additive; nothing existing
  changes shape.
* Enabled by default (60s): a client that experiences an automatic
  failover now recovers on its own once the abandoned cluster
  responds again, without the caller polling or remembering to call
  `switch_to_cluster` back.
* A manual `switch_to_cluster` is never second-guessed: confirmed by
  `remote_cache::tests::a_manual_switch_supersedes_a_pending_switch_back_monitor`.
* `set_switch_back_check_interval(None)` stops a monitor already
  running, not just future ones: confirmed by
  `remote_cache::tests::disabling_the_switch_back_interval_stops_a_pending_monitor`.
* Confirmed against a fake server end to end
  (`remote_cache::tests::automatic_failover_proactively_switches_back_once_the_original_cluster_recovers`):
  a real automatic failover, the original address accepting
  connections again, and `active_cluster_name()` returning to it with
  no further action from the caller.
* No live-server test, deliberately: see "Test coverage" above.
* Not implemented: any retry limit (the monitor keeps trying at the
  configured interval until superseded or disabled, with no bound on
  attempts); per-cluster intervals (one setting applies regardless of
  which cluster a monitor targets, the same granularity
  `max_retries`/`server_failure_timeout` already have).
