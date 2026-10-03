# Roadmap proposal

This document is a proposal, not a decision. It is a candidate ordering
for the open issues, based on an external review of the current `main`
against the Hot Rod protocol spec and the official Java and Go clients.
Individual items still go through the Analyze/Propose/Resolve steps in
`CLAUDE.md` section 4 before any code is written; this file exists so
that step starts from a shared picture of the whole gap instead of one
issue at a time.

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
TLS was the first piece and is now done (`docs/adr/0004-tls-support.md`,
#46). Connection pooling and concurrent dispatch is now done too
(`docs/adr/0005-connection-pooling-and-client-cache-split.md`, #77).
Events, near caching, streaming and typed serialization are still open;
the PHP bridge still comes last.

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
    +-- get / put / remove / bulk / ...
```

`HotRodConnection` kept its role as the single, sequential connection
type; the pool holds several of them per node instead of `HotRodCluster`
holding exactly one per node. TLS, listeners, near caching, streaming
and multi-cluster failover all assume a client that can hold several
live connections and dispatch to them concurrently, so this needed to
land before those features, not alongside the last one that ran into it.

## Next up, now that #77 has landed

The structural blocker is gone, so every P1 item below is unblocked and
can start whenever its own Propose step is ready; none of them has a
further dependency on each other.

* **#78 (multi-node CI fixture) is the natural next pick, not just
  because it is still P0.** #77 was itself validated by hand against a
  real two-node cluster under `ci/infinispan-kind/` (a disposable `kind`
  fixture, built for that one validation, not wired into any workflow).
  That fixture is a reasonable starting point for #78's permanent
  version, not a from-scratch effort: the main open questions are
  whether a `kind`-based fixture (already proven to work with this
  project's `podman`-only CI runners) is the right shape for a
  standing CI job, and how to keep its runtime acceptable on every push
  rather than only running it by hand before a risky change.
* **#4 (client listeners) is the next P1 item to Propose**, per the
  existing ordering: it is what #5 (near caching) depends on, and
  nothing about it depends on #78 landing first, so the two can proceed
  in parallel if there is bandwidth for both.
* #52 (streaming) and #53 (server-side iteration) remain independent of
  #4/#5 and of each other; either can be picked up next instead of, or
  alongside, listeners if that is a better fit for what is needed next.

## Proposed ordering

| Priority | Item | Issue | Why this order |
| --- | --- | --- | --- |
| ~~P0~~ | ~~Connection pooling / concurrent operations~~ | #77 (done) | Structural blocker described above |
| P0 | Multi-node CI fixture | #78 | The cluster code path is currently exercised only by `#[ignore]`d manual tests; failover and rebalance need a real multi-node run in CI. `ci/infinispan-kind/` is a starting point, see above |
| P1 | Client listeners (cache events) | #4 | Prerequisite for near caching; unblocked now, next to Propose |
| P1 | Near caching | #5 | Large latency win once listeners exist, particularly for the PHP bridge |
| P1 | Streaming (GetStream/PutStream) | #52 | Values are currently always fully buffered in memory |
| P1 | Server-side iteration | #53 | Only way to walk a cache without already knowing its keys |
| P1 | Client statistics and telemetry | #54 | Needed before this is trusted in production, cheap to add once pooling exists |
| P2 | Serialization abstraction (`Marshaller` trait) | (new, precedes #50) | `hotrod-protocol` is deliberately byte-oriented today; query needs typed values first |
| P2 | Remote query (Ickle / Protobuf) | #50 | Depends on the marshaller and on Protobuf schema registration |
| P2 | Multi-cluster failover | #56 | Disaster-recovery feature, not needed for a single-cluster deployment |
| P3 | Multimap cache | #48 | Independent of the above, small surface |
| P3 | Distributed counters | #49 | Independent of the above, small surface |
| P3 | Remote administration | #55 | Management plane, separate from the data-plane work above |
| P4 | Transaction support | #47 | Needs the pool and topology work settled first; two-phase commit across a changing cluster is its own hard problem |
| P4 | GSSAPI authentication | #9 | Already deferred, needs a system Kerberos library |
| P4 | Remote task execution | #51 | No current use case pulling on it |

This intentionally does not try to move on GSSAPI, transactions,
counters, multimap, admin, query, streaming, near caching and PHP at
the same time. Each one lands after the layer it depends on, not in
parallel with it.

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

## Milestones

* **Transport and concurrency**: connection pooling, retry/backoff,
  multi-node CI, fuzzing the wire parsers (`read_vint`, `read_vlong`,
  `read_array`, `read_topology_update`, the SASL challenge parsers).
* **Events and near cache**: client listeners, an async `Stream` of
  cache events, a bounded LRU near cache invalidated through those
  events.
* **Large data**: `get_stream`/`put_stream`, server-side iteration,
  both integrated with Tokio's `AsyncRead`/`AsyncWrite` rather than a
  bespoke chunk API.
* **Typed data**: the `Marshaller` trait, a Protobuf implementation,
  then remote query on top of it.
* **PHP ecosystem**: the extension itself, PSR-16, the Symfony adapter,
  the session handler, prebuilt binaries. This comes last because it
  depends on the transport, event and serialization layers being
  settled; building it against a moving core would mean redoing it.

## Explicitly out of scope for now

* A `hotrod-wire` / `hotrod-client` / `hotrod-php` crate split. Worth
  reconsidering once pooling, listeners and streaming exist; premature
  before that.
* Automatic PHP array to Protobuf/Java-object mapping. The PHP API
  should offer explicit `putBytes`/`putJson`/a configurable serializer,
  not implicit conversion.
* Putting the Rust-vs-Java benchmark numbers in the README as a
  performance claim. The current benchmark is single-connection,
  single-machine, sequential and already documented as informal; it
  does not measure concurrency, pooling, TLS, payload size or p99
  latency, which is what production-adjacent numbers would need.
