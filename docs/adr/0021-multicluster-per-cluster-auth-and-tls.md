# ADR 0021: Multi-cluster failover, per-cluster authentication and TLS

## Status

Accepted, implemented in #114.

## Context

ADR 0017 (`docs/adr/0017-multi-cluster-failover.md`) deliberately left
authentication, TLS and `ClientIntelligence` shared globally across
every configured cluster, matching the Java client (which only allows
a per-cluster SNI hostname override beyond that), and explicitly
deferred a real per-cluster override: "Worth revisiting if a real
deployment needs different credentials per cluster (a plausible
disaster-recovery scenario the current design does not support)."

This phase picked that up. The maintainer chose the scope up front,
between four options presented: **authentication and TLS per
cluster, explicitly not `ClientIntelligence`**. Unlike auth and TLS,
`ClientIntelligence` is not configurable even globally in this crate
today (`connect_hash_aware` always dials `HashDistributionAware`,
confirmed in `connection.rs`); making it overridable per cluster
would first need making it configurable at all, for a use case
nobody has stated and the Java client itself does not support either.

Reading the current code (`client.rs`) confirmed exactly how narrow
the actual gap was:

* `ClientInner.auth: RwLock<Option<AuthMethod>>` and
  `ClientInner.tls: Option<TlsConfig>` are each a single global value,
  read by every dial of every seed of any cluster: `try_promote_seed`
  (shared by `failover_seed` and `try_failover_to_live_cluster`),
  `open_and_authenticate` (pool `checkout` and `listen`/`listen_with`),
  and `authenticate_with` (the live SASL validation dial behind the
  public `authenticate_plain`/`scram`/`digest`/`oauthbearer`).
* Pools are keyed by address, not by cluster, but in practice only
  ever hold live connections for whichever cluster is currently
  active: a switch drains the abandoned cluster's pools
  (`evict_abandoned_cluster`, ADR 0019). So `open_and_authenticate`
  never needs a cluster name threaded in; it always means the active
  cluster.
* `AuthMethod` is `pub(crate)`, never exposed directly: the public
  surface is four dedicated methods, each building the right variant
  and delegating to `authenticate_with`. `TlsConfig` is already
  `pub`, `Clone`, plain data with no setter at all today (fixed once
  at construction).

## Decision

One override slot per cluster, `None` in either field meaning
"inherit the global value", resolved at the point of dialing rather
than threaded through every call as an extra parameter. No new
`Error` variant: `Error::UnknownCluster`/`Error::InvalidClusterConfig`
(ADR 0017) already fit every new validation this needed.

## Design

### Storage

```rust
#[derive(Clone, Default)]
pub(crate) struct ClusterOverride {
    pub(crate) auth: Option<AuthMethod>,
    pub(crate) tls: Option<TlsConfig>,
}
```

New `ClientInner` field: `cluster_overrides: RwLock<HashMap<String,
ClusterOverride>>`, default empty.

### One resolution helper, three call sites

```rust
fn effective_auth_and_tls(&self, cluster_name: &str) -> (Option<AuthMethod>, Option<TlsConfig>)
```

Looks up `cluster_name` in `cluster_overrides`; each field falls back
to the client's global `auth`/`tls` independently when the override
either does not exist for that cluster or left that particular field
`None`.

* `try_promote_seed` resolves the cluster being targeted (the one
  named by `switch_to_cluster_name` when switching, or whichever
  cluster is currently active when `failover_seed` is just trying
  another of its own seeds) and calls this once, before iterating
  candidates, exactly where it already read the two fields directly.
* `open_and_authenticate` always resolves with `active_cluster_name()`:
  it is only ever reached for a node of the currently active cluster
  (see "pools" above).
* `authenticate_with` keeps writing into the global `auth` slot
  unchanged (it is explicitly setting the global default), but now
  dials its validation probe using the active cluster's own effective
  TLS instead of unconditionally the global one, so it honors an
  override already in place for the cluster it is actually probing.

### Public API

```rust
impl HotRodClient {
    pub async fn authenticate_plain_for_cluster(&self, name: &str, authzid: &str, authcid: &str, password: &str) -> Result<()>;
    pub async fn authenticate_scram_for_cluster(&self, name: &str, authcid: &str, password: &str) -> Result<()>;
    pub async fn authenticate_digest_for_cluster(&self, name: &str, authcid: &str, password: &str) -> Result<()>;
    pub async fn authenticate_oauthbearer_for_cluster(&self, name: &str, authzid: &str, token: &str) -> Result<()>;
    pub fn set_cluster_tls(&self, name: &str, tls: Option<TlsConfig>) -> Result<()>;
}
```

Four cluster-scoped siblings of the existing global methods, same
parameter shapes, sharing a private `authenticate_for_cluster_with`
the same way the global ones share `authenticate_with`: dials `name`'s
own first seed address (a single attempt, no fallback iteration,
matching `authenticate_with`'s own simplicity) with `name`'s own
effective TLS, runs the SASL exchange, and only then stores the
method into that cluster's `ClusterOverride.auth`. `name` must already
be configured (`add_cluster`, or `DEFAULT_CLUSTER_NAME`); an unknown
name is rejected with `Error::UnknownCluster` before anything is
dialed, the same validation `switch_to_cluster` already does.

`set_cluster_tls` is a plain, synchronous setter: no live dial, since
`TlsConfig` is inert data until a real connection attempt uses it
(the same reason the global `tls` field has no setter at all).

Both invalidate every pool's idle connections when `name` is the
currently active cluster, the same reasoning `authenticate_with`
already documents for the global case: an idle pooled connection for
one of that cluster's nodes may have been opened under whatever was
in effect before this call.

### Test coverage

* `effective_auth_and_tls_falls_back_to_global_unless_a_cluster_has_its_own_override`
  (`client.rs`): a cluster with only an auth override, one with only a
  TLS override, and a cluster with neither, confirming each field
  falls back to the global value independently and exactly where
  expected. Deterministic, no I/O.
* `authenticate_plain_for_cluster_is_used_once_switched_to_that_cluster`
  (`client.rs`): two fake TCP listeners (the default cluster and a
  "dr" one), the global `authenticate_plain` set to one credential and
  `authenticate_plain_for_cluster("dr", ...)` to a different one, then
  `switch_to_cluster("dr")` plus a real `ping`; confirms the "dr" fake
  server receives the "dr"-specific SASL response both for the
  validation dial and for the real connection the `ping` opens
  afterward, reusing the existing `serve_plain_auth` helper already
  shared with ADR 0011's own auth tests.
* `authenticate_for_cluster_and_set_cluster_tls_reject_an_unknown_cluster`:
  no network involvement needed, since this validates before dialing.
* **No new fake-TLS-server test.** `connect_hash_aware`'s actual TLS
  handshake mechanics (certificate verification, SNI, mutual TLS) are
  already fully exercised by `tls.rs`'s own tests; what this phase
  adds is purely the *selection* of which `TlsConfig` a given cluster
  dials with, which `effective_auth_and_tls`'s own unit test above
  already covers precisely (`tls-only"`'s override is the one actually
  returned, not the global one). Building a second, parallel
  fake-TLS-server harness at the `HotRodClient` level, just to re-prove
  a handshake `tls.rs` already proves, was judged not worth the
  maintenance cost for what the resolution-level test already confirms
  directly.

## Consequences

* New public API: `HotRodClient::authenticate_plain_for_cluster`/
  `authenticate_scram_for_cluster`/`authenticate_digest_for_cluster`/
  `authenticate_oauthbearer_for_cluster`/`set_cluster_tls`. Additive:
  nothing existing changes shape.
* A cluster configured without any override keeps behaving exactly as
  before this phase (falls through to the global `auth`/`tls`
  unchanged), confirmed by
  `effective_auth_and_tls_falls_back_to_global_unless_a_cluster_has_its_own_override`.
* **`ClientIntelligence` per-cluster override is explicitly out of
  scope**, by the maintainer's own choice: it is not configurable even
  globally in this crate today, has no stated use case, and the Java
  client does not support it either. A separate, future issue would
  need to first make it configurable at all before a per-cluster
  override has anything to attach to.
* Not implemented: a getter for a cluster's current override (neither
  the global `auth` nor `tls` has one either, for consistency); a way
  to override only `auth` xor `tls` through a single call (the two
  setters are already independent, so this is just calling both).
