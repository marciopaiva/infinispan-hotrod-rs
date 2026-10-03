# ADR 0005: Connection pooling and the HotRodClient/RemoteCache split

## Status

Accepted, implemented in #77.

## Context

`HotRodCluster` (`cluster.rs`) holds at most one `HotRodConnection` per
node, in a `HashMap<SocketAddr, PooledConnection>`, and every operation
takes `&mut self`. One instance therefore runs its operations one at a
time, even when two of them route to different nodes: the second waits
for the first regardless of which node it targets. This was documented
rather than fixed when it was first raised (#27), since no concurrency
requirement existed yet to justify the scope. TLS, client listeners
(#4), near caching (#5), streaming (#52) and multi-cluster failover
(#56) all assume a client that can hold several live connections per
node and dispatch to them concurrently, so this needs solving before
those land, not alongside the last one that runs into it. See
`docs/roadmap.md` for the full picture of what this blocks.

Three questions need an answer before implementation starts:

**Pooling mechanism.** `HotRodConnection` already enforces its own
poisoning rule (`is_poisoned`, set before a request is written, cleared
only once the response is read in full), which a pool of several
connections per node still needs to check before handing one out.
A generic pooling crate (`deadpool`, `bb8`) does not know this rule, so
it would not save the part of the work that is actually specific to
Hot Rod; it would only replace the `Vec`/semaphore bookkeeping, which is
small by comparison. Pulling in such a crate is also a new dependency,
a stop-and-ask item under CLAUDE.md section 5.

**Public API shape.** `HotRodCluster` manages exactly one cache today,
same as `HotRodConnection`. ADR 0003 already flagged that a
`RemoteCacheManager`/`RemoteCache`-style split was a bigger conceptual
jump the codebase did not need at the time, deferring it to "its own
decision with its own Propose step" if a real use case asked for it
later. Pooling is that use case: the pools and topology tracking are
naturally per-cluster state shared across every cache a caller talks
to, while the operation methods (`get`/`put`/...) are naturally
per-cache. Splitting them now means the pool is built once as shared
state instead of duplicated per cache, and avoids a second breaking
API change later when multi-cache support is asked for on its own.

**Pool sizing and eviction under concurrent checkout.**
`record_topology_update` (cluster.rs:909) currently assumes exclusive
access: it removes any pooled connection whose origin server is no
longer in the latest topology. Once several tasks can have a
connection to the same node checked out at once, eviction can no
longer just remove entries from a map; it has to mark the node's pool
closed and let checked-out connections observe that status when they
try to return, rather than touching connections another task currently
owns.

## Options considered

### Pooling mechanism

1. **Hand-rolled per-node pool**: a `tokio::sync::Semaphore` bounding
   the number of connections to a node, plus a `Mutex<Vec<HotRodConnection>>`
   of idle ones. Checkout acquires a permit and pops an idle connection,
   opening and authenticating a new one if none is idle; a RAII guard
   checks `is_poisoned` on drop and only returns a healthy connection to
   the idle list. No new dependency.
2. **`deadpool`/`bb8`**: less pool-bookkeeping code, but neither crate
   has any notion of the poisoning rule above, so the integration still
   has to wrap every checkout in the same poison check this project
   would otherwise write directly. New dependency, decided against
   taking on for marginal savings.

### Public API shape

1. **`HotRodClient` / `RemoteCache` split**: `HotRodClient` owns the
   seed addresses, topology, per-node pools, authentication and TLS
   config, shareable via `Arc<HotRodClient>` and cheap to clone.
   `RemoteCache` is a thin handle (`client: Arc<HotRodClient>,
   cache_name: String`) exposing the same operation methods
   `HotRodCluster` has today, obtained from `HotRodClient::cache(name)`.
2. **Keep `HotRodCluster`, change methods to `&self`**: smaller rename
   footprint, but the one-cache-per-instance shape stays, so a caller
   wanting several caches against the same cluster still has to
   duplicate the pools, and multi-cache support later would still need
   a second breaking change to split client state from cache state.

## Decision

**Hand-rolled pool, no new dependency.** A `Semaphore` plus
`Mutex<Vec<Slot>>` per node (`Slot` being either an idle connection or an
empty marker meaning "open one"), wrapped so that:

* Checkout: acquire a permit, then pop the most recently returned slot
  (LIFO, not FIFO) from the vec; a connection slot is reused as is, an
  empty one means the caller opens and authenticates a new connection.
  The permit is `forget()`-ten immediately after the pop, not held for
  the connection's lifetime: it exists only to make `slots.len()`
  waitable, not to track which connection "owns" it.
* Return: a guard type returned from checkout pushes a slot back on
  drop and calls `add_permits(1)` to match, a connection slot if
  `is_poisoned()` is false, an empty one otherwise, matching the
  existing poisoning rule from `HotRodConnection`.
* Eviction: a node that leaves the topology has its pool marked closed
  immediately and every slot currently idle in it replaced with an
  empty one (closing those connections); new checkouts past that point
  get empty slots too, so nothing reconnects to a dead node. A
  connection currently checked out by another task is left alone; its
  guard checks the closed flag on return and turns into an empty slot
  instead of a connection one, rather than the eviction logic reaching
  into a slot another task is using.
* Pool size: fixed at a `DEFAULT_MAX_CONNECTIONS_PER_NODE` constant (8),
  the same pattern `DEFAULT_TIMEOUT` already uses, rather than a
  configurable parameter threaded through all four `connect*`
  constructors for a need that is still hypothetical.

**This went through two wrong designs before landing on the one above**,
caught by validating against a real two-node cluster
(`ci/infinispan-kind/`) before this ADR's own implementation was
committed, not by the unit tests, which all used at most one concurrent
checkout per pool and so never exercised the bug:

1. A permit tied to each connection's whole lifetime (acquired once,
   travelling with the connection between the idle vec and a checked-out
   guard, released only when the connection was actually closed) plus a
   separate idle `Vec<(HotRodConnection, OwnedSemaphorePermit)>`.
   Deadlocked past `max_size` concurrent checkouts: a checkout that found
   the vec momentarily empty committed to waiting on the semaphore for a
   *new* permit, but a healthy return never freed one, since the permit
   stayed bound to the connection it travelled with. Only an actual close
   released a permit, so every checkout beyond the first `max_size` hung
   until its operation timeout, every time, under real concurrent load.
2. A bounded `mpsc` channel of slots, replacing the semaphore entirely:
   fixed the hang (every checkout is one `recv`, every return is one
   `send`, so a waiting `recv` wakes on either kind of return), but its
   FIFO order meant a freshly returned, still-open connection sat behind
   the pool's other, still-empty initial slots. The very next checkout
   popped an empty slot instead of the live connection and opened a
   redundant new one, which a mock single-accept test listener cannot
   tolerate (and which defeats connection reuse in general).

The final design keeps the single-vec simplicity of (2) but pops
most-recently-returned-first, and keeps bounding via a semaphore like
(1) but only as a counter, never as a per-connection lease.

**Adopt the `HotRodClient` / `RemoteCache` split.** `HotRodCluster` is
renamed and restructured into:

```
HotRodClient
    |
    +-- topology manager
    +-- connection pools (one per (node, cache name) pair, as above;
    |                      see Consequences for why the cache name is
    |                      part of the key)
    +-- router (segment owner -> pool)
    +-- authentication, TLS config
    |
RemoteCache (one per named cache, borrowed from HotRodClient)
    |
    +-- get / put / remove / bulk / ...
```

`HotRodClient::connect`/`connect_with_timeout`/`connect_tls`/
`connect_tls_with_timeout` mirror `HotRodCluster`'s current
constructors, but no longer take a cache name: that moves to
`HotRodClient::cache(name) -> RemoteCache`. `RemoteCache`'s methods take
`&self`, so independent operations against different nodes, or even the
same node, run concurrently from a single `HotRodClient`/`RemoteCache`
pair shared across tasks, which is the whole point of this change.
`HotRodConnection` is unchanged: it keeps its role as the single,
sequential connection type the pool holds several of.

This is a breaking change to `hotrod-protocol`'s public API:
`HotRodCluster` and its one-cache-per-instance, `&mut self` methods go
away. There is no deprecation path kept alongside it; the project has
no stable release yet that a migration would need to bridge.

## Consequences

* `HotRodCluster` is removed; `HotRodClient` and `RemoteCache` take its
  place in the public API. Every caller of `HotRodCluster` (including
  the `cluster_*` tests in `hotrod-protocol/tests/live_server.rs`) needs
  updating to the new split.
* Authentication (`authenticate_plain`/`authenticate_scram`/
  `authenticate_digest`/`authenticate_oauthbearer`) moves to
  `HotRodClient`, since it is replayed onto every pooled connection
  regardless of which cache a caller later opens.
* `record_topology_update`'s eviction logic changes from removing map
  entries outright to marking a node's pool closed, since a pooled
  connection may be checked out by another task when a topology update
  arrives.
* No new dependency: pooling is `tokio::sync::Semaphore` plus
  `std::sync::Mutex`/`RwLock`. `tokio`'s `sync` Cargo feature had to be
  turned on (it was not needed before this), which pulls in no
  additional crate, only code already inside the `tokio` dependency
  this project already has.
* Pools are keyed by `(SocketAddr, cache name)`, not address alone.
  `HotRodConnection` still binds to one cache at connect time (its
  module docs, unchanged by this ADR), even though `cache_name` is
  technically a per-request field on the wire
  (`write_request_header`), not a handshake parameter. A connection
  opened for one cache therefore cannot be handed to a `RemoteCache`
  for another, so each `(node, cache)` pair needs its own pool. This
  was not visible until `client.rs` was written; revisiting
  `HotRodConnection`'s cache binding to let one connection serve
  several caches is out of scope here.
* #78 (the multi-node CI fixture) becomes more directly load-bearing
  once this lands: concurrent dispatch across nodes is exactly the
  behavior that fixture needs to verify under a real cluster, not just
  unit tests against local TCP listeners.
