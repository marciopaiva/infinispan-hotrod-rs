# ADR 0003: Scope phase 3 to additive hash-aware routing, no permanent multi-node CI fixture yet

## Status

Accepted

## Context

ADR 0001 lists phase 3 as "cluster topology and hash-aware intelligent
routing, so the client resolves the owning node itself instead of
relying on server-side redirects." Today `HotRodConnection` is a single
TCP connection that always advertises `CLIENT_INTELLIGENCE_BASIC` and
errors out if the server ever sends a topology update.

Building real hash-aware routing raises three questions that are cheap
to get wrong and expensive to unwind later:

* Should this be a breaking change to `HotRodConnection`, or a new,
  additive type?
* Should the new type manage one cache, like `HotRodConnection` does
  today, or many caches at once, closer to the Java client's
  `RemoteCacheManager`/`RemoteCache` split?
* Hash-aware routing only matters once a cache is distributed across
  multiple nodes. The current CI fixture runs a single node with a
  `local-cache`. Does this phase also stand up a permanent multi-node CI
  fixture?

## Decision

**Additive API.** `HotRodConnection` keeps its current behavior
unchanged (`CLIENT_INTELLIGENCE_BASIC`, topology id `-1`, errors on any
topology update). A new type, `HotRodCluster`, is added alongside it for
hash-aware routing. Nothing about the phase 1/2 public surface changes.

**One cache per `HotRodCluster`.** Same shape as `HotRodConnection`: a
`RemoteCacheManager`/`RemoteCache` split is a bigger conceptual jump the
codebase does not need yet. If a real use case asks for managing several
caches through one client later, that is its own decision with its own
Propose step, not something to speculate on now.

**No permanent multi-node CI fixture in this phase.** Live validation for
this phase needs a real multi-node cluster with a distributed cache,
built manually via `podman` for validation before each commit is called
done, the same discipline phase 1 and 2 used for their live-server
checks. Standing up a multi-node cluster as a permanent, always-on CI job
is its own piece of infrastructure work with its own cost (slower CI,
more moving parts to keep green), and is deferred to a follow-up issue
rather than bundled into this phase.

**The exact hash algorithm is pinned against real Java output, not
derived from memory.** Infinispan's `MurmurHash3` is a specific, fixed
variant, and getting the bit manipulation subtly wrong would silently
misroute requests rather than fail loudly, since a Hot Rod server always
still serves a request sent to the wrong node, just less efficiently. The
same technique phase 2 used for SASL applies here: a small, throwaway
Java harness calls Infinispan's real `MurmurHash3` class on known
inputs, and those outputs become fixed-vector unit tests.

## Consequences

* `hotrod-protocol`'s public API gains `HotRodCluster` (new module
  `cluster.rs`), alongside the unchanged `HotRodConnection`.
* `header.rs`'s request/response framing becomes parameterized over
  client intelligence and topology id instead of hardcoding
  `CLIENT_INTELLIGENCE_BASIC`, since `HotRodCluster`'s per-node
  connections need `HASH_DISTRIBUTION_AWARE` framing.
* New modules `hash.rs` (Infinispan's `MurmurHash3` and segment
  computation) and `topology.rs` (the topology-aware response payload).
* A follow-up issue is needed if continuous CI coverage for multi-node
  routing is wanted later; until then, correctness rests on unit tests
  with real-server-derived fixed vectors plus manual live validation
  before each commit lands.
* That follow-up issue was #78: a permanent two-node Docker fixture
  (`ci/infinispan-cluster/`) and a `cluster-test` job in `ci.yml`,
  running on every push and PR. The question this ADR left open is
  answered; the historical record above (no fixture at the time this
  phase shipped) stays as written.
