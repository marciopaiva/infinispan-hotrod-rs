# Roadmap proposal

This document is a proposal, not a decision. It is a candidate ordering
for the open issues, based on an external review of the current `main`
against the Hot Rod protocol spec and the official Java and Go clients.
Individual items still go through the Analyze/Propose/Resolve steps in
`CLAUDE.md` section 4 before any code is written; this file exists so
that step starts from a shared picture of the whole gap instead of one
issue at a time. It groups the open issues by theme rather than listing
them as one flat queue, but the ordering within and across themes is
still the thing that changes as work lands; the issue numbers and ADRs
linked throughout are the ground truth, not the prose around them.

## Where the project stands

`hotrod-protocol` is closer to a well-built protocol core than to a
complete client. It already has connection-level correctness that
matters for a binary protocol: typed errors instead of generic I/O
failures, allocation limits on every server-declared length
(`Error::DeclaredLengthTooLarge`, `MAX_BULK_ENTRIES`), bounded varint
decoding (`Error::MalformedVarint`), and connection poisoning so a
cancelled operation can never leave a later read misaligned with the
wire. `HotRodClient` adds real topology tracking and hash-aware
routing, not just a TCP client that happens to also parse Hot Rod
frames.

The gap is everything a production deployment expects around that core:
TLS was the first piece (`docs/adr/0004-tls-support.md`, #46). Connection
pooling and concurrent dispatch followed
(`docs/adr/0005-connection-pooling-and-client-cache-split.md`, #77), then
client listeners (`docs/adr/0006-client-listeners.md`, #4) and near
caching (`docs/adr/0007-near-caching.md`, #5), released together as
v0.5.0. Streaming (`docs/adr/0008-streaming.md`, #52) and server-side
iteration (`docs/adr/0009-server-side-iteration.md`, #53) are done too,
unreleased. Typed serialization and the PHP bridge are still open.

## The structural change that gated the rest (done, #77)

`HotRodCluster` used to take `&mut self` on every method, so one
instance serialized every operation, even ones that route to different
nodes. `HotRodClient`/`RemoteCache` (ADR 0005) replaced it: the client
holds a bounded pool of connections per `(node, cache)` pair and every
`RemoteCache` operation takes `&self`, so independent operations,
including ones against the same node, run concurrently. The shape that
shipped:

```
HotRodClient
    |
    +-- topology manager
    +-- connection pools (one per (node, cache) pair)
    +-- router (segment owner -> pool)
    +-- authentication, TLS config
    |
RemoteCache (one per named cache, borrowed from HotRodClient)
    |
    +-- get / put / remove / bulk / listen / near_cache / ...
```

`HotRodConnection` kept its role as the single, sequential connection
type; the pool holds several of them per node instead of `HotRodCluster`
holding exactly one per node. TLS, listeners, near caching, streaming
and multi-cluster failover all assume a client that can hold several
live connections and dispatch to them concurrently, so this needed to
land before those features, not alongside the last one that ran into it.

## Themes

Six P0/P1 items are done: pooling (#77), the multi-node CI fixture
(#78), client listeners (#4), near caching (#5), streaming (#52) and
server-side iteration (#53). What is left groups into four themes,
each independent of the others, so any of them can go next in whatever
order is actually needed. Within a theme, order matters more, since
later items there tend to build on earlier ones.

### Large data access

Both items in this theme are done.

* ~~Streaming (#52, GetStream/PutStream)~~ (done). Values no longer
  have to be fully buffered in memory: `RemoteCache::get_stream`/
  `put_stream`/`put_stream_if_absent`/`replace_stream_with_version`
  read or write one in chunks, pinned to the one pooled connection
  that opened the stream, per `docs/adr/0008-streaming.md`.
* ~~Server-side iteration (#53, retrieveEntries/keySet/entrySet/
  values)~~ (done). `RemoteCache::iter`/`iter_with` walk a whole cache
  without the caller already knowing its keys, needed for export,
  migration and cache inspection tooling. On a cluster, opens one
  cursor per owning node, sequentially; see
  `docs/adr/0009-server-side-iteration.md` for why that trade was made
  over the Java client's concurrent fan-out.

### Reliability and observability

* **Client statistics and telemetry (#54).** Needed before this client
  is trusted in production: operation counts and latency, connection
  and pool state, near-cache hit/miss. Cheap to add now that pooling
  exists to instrument.
* Retry policy, node health tracking and circuit breaking are natural
  extensions of this theme (an operation-aware retry policy in
  particular, since blindly retrying `put`/`replace`/the versioned
  operations risks silent duplicate writes), but none of them has an
  issue filed yet. Worth proposing once #54 gives a concrete picture of
  what is actually failing in practice, rather than designing retry
  behavior against a guess.
* Fuzzing the wire parsers (`read_vint`, `read_vlong`, `read_array`,
  `read_topology_update`, the SASL challenge parsers) was already
  planned before this rewrite and still has no issue filed; it belongs
  here, since a parser that survives a fuzzer is exactly the kind of
  reliability this theme is about.

### Typed data

* **Serialization abstraction.** `hotrod-protocol` is deliberately
  byte-oriented today, with no built-in notion of typed values; a
  `Marshaller`-shaped trait would let a caller work with typed values
  without baking a specific format into the protocol layer. No issue
  filed yet; precedes remote query.
* **Remote query, Ickle/Protobuf (#50).** Depends on the marshaller
  above and on Protobuf schema registration against the server.

### High availability

* **Multi-cluster failover (#56).** Disaster-recovery feature, not
  needed for a single-cluster deployment; independent of the themes
  above.

### Specialized data structures and administration

Each of these has its own dedicated Hot Rod operations, independent of
`RemoteCache` and of each other; small individual surfaces.

* **Multimap cache (#48)**
* **Distributed counters (#49)**
* **Remote administration (#55)**, create/remove caches: a management
  plane, kept separate from the data-plane work above.

### Deferred

* **Transaction support (#47).** Two-phase commit across a changing
  cluster topology is its own hard problem; needs the pool and
  topology work settled first, which it now is, but still wants its
  own Analyze/Propose pass rather than piggybacking on another phase.
* **GSSAPI authentication (#9).** Already deferred: needs a system
  Kerberos library, which conflicts with this project's preference for
  no new system dependencies (the same reasoning ADR 0002 used to
  defer it originally).
* **Remote task execution (#51).** No current use case pulling on it.

This intentionally does not try to move on everything at once. Each
theme, and each item within one, lands after what it depends on, not
in parallel with it.

## Near cache: known gaps

The near cache (#5, `docs/adr/0007-near-caching.md`) is bounded and
LRU-evicted today (`NearCacheOptions::max_entries`), invalidated through
a listener with a documented fail-safe if that listener dies. What the
ADR explicitly left out, in order of how much it costs in practice:

* No per-entry `lifespan`/`max_idle` tracking: a cached value can
  outlive its own lifespan locally, and a local hit never refreshes
  `max_idle` server-side. `get_with_version`/`VersionedValue` already
  carry this metadata; using it instead of a bare value is the natural
  fix, not yet taken on.
* The race-closing generation counter is store-wide, not per key, so
  heavy concurrent writes to unrelated keys can push the local hit rate
  well below normal. A bounded per-key tombstone (reusing the same LRU
  eviction already in `LruStore`) would fix this without reopening the
  unbounded-growth problem a naive per-key scheme would have.
* No in-flight request coalescing: concurrent `get` misses on the same
  key each run their own independent fetch instead of sharing one.
* No bloom-filter support (the Java client's `addNearCacheListener`
  traffic optimization): without it, this client's listener receives
  invalidation for every key that changes in the whole remote cache,
  not just the ones it has cached, and filters client-side instead.
* No hit/miss/invalidation statistics; folds naturally into the
  broader client-statistics theme above (#54) once that exists.

None of these block the themes above; revisit if real usage shows one
of them actually costing something, rather than preemptively.

## PHP: not just a wrapper

The PHP bridge is where this project can be more than "one more Hot Rod
client." The recommendation is to design it around three integrations
that make it easy to adopt inside existing PHP frameworks, rather than
mirroring the Rust API method by method:

* A `Psr\SimpleCache\CacheInterface` (PSR-16) implementation, so any
  library or framework that already speaks PSR-16 works against
  Infinispan with no glue code.
* A Symfony Cache adapter, since Symfony's cache pool interface is a
  common integration point in that ecosystem.
* A PHP session handler (`session_set_save_handler`), which turns
  Infinispan into a distributed session store for PHP-FPM deployments
  without a separate Redis dependency. TTL maps directly onto the
  existing `Expiration`/lifespan support.

Async Rust does not need to become async PHP: the extension can run
Tokio internally while exposing a synchronous PHP API, the same way any
other PHP extension hides its own I/O model. `getAsync`-style APIs are
explicitly out of scope for the first PHP milestone.

This comes after every theme above: building it against transport,
event and serialization layers that are still moving would mean redoing
it.

## Guiding principles

A few things this project has actually done repeatedly, worth stating
once instead of re-discovering per phase:

* **Cancellation is part of correctness, not an edge case.**
  `HotRodConnection` poisons itself before writing a request and clears
  the mark only once the response is read in full; `CacheListener` does
  the same around each event frame; `NearCachedCache::invalidate_after`
  invalidates from a guard's `Drop` specifically so a cancelled caller
  cannot skip it. Any new operation that holds state across an
  `.await` needs the same question asked of it: what does a future
  dropped mid-flight leave behind?
* **Protocol details come from the real client source, not memory.**
  Every phase so far (hash routing, SASL mechanisms, listener framing,
  streaming) pinned its wire format against the Java client's actual
  source on GitHub before writing a line of parsing code, which is
  also how streaming caught that its own response opcodes break the
  "request plus one" pattern every other operation follows. Iteration
  needs the same treatment before its own Propose step.
* **No new dependency without the Analyze/Propose step.** Demonstrated
  repeatedly: GSSAPI deferred partly over a system Kerberos dependency,
  `rustls` chosen over `native-tls` to avoid a C one, connection pooling
  and near caching's LRU both shipped with none at all.
* **Test against a real server, not only a fake one.** Unit tests
  against an in-process fake server catch wire-format regressions
  fast; they have also missed real bugs a live Infinispan server
  caught immediately (a nonexistent opcode, a wrong error-response
  path). Every phase ends with `#[ignore]`d live-server tests, wired
  into CI rather than left to run only by hand.

## Explicitly out of scope for now

* A `hotrod-wire` / `hotrod-client` / `hotrod-php` crate split. Worth
  reconsidering once typed data exists too; premature before that.
* Automatic PHP array to Protobuf/Java-object mapping. The PHP API
  should offer explicit `putBytes`/`putJson`/a configurable serializer,
  not implicit conversion.
* Putting the Rust-vs-Java benchmark numbers in the README as a
  performance claim. The current benchmark is single-connection,
  single-machine, sequential and already documented as informal; it
  does not measure concurrency, pooling, TLS, payload size or p99
  latency, which is what production-adjacent numbers would need.
