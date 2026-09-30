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
wire. `HotRodCluster` adds real topology tracking and hash-aware
routing, not just a TCP client that happens to also parse Hot Rod
frames.

The gap is everything a production deployment expects around that core:
TLS was the first piece and is now done (`docs/adr/0004-tls-support.md`,
#46). Concurrency, pooling, events, near caching, streaming, typed
serialization and the PHP bridge are still open.

## The one structural change that gates the rest

`HotRodCluster` methods take `&mut self`. One instance therefore
serializes every operation, even ones that route to different nodes.
Wrapping it in `Arc<Mutex<..>>` would not fix this, it would just move
the serialization point. TLS, listeners, near caching, streaming and
multi-cluster failover all assume a client that can hold several live
connections and dispatch to them concurrently, so this needs to be
addressed before those features, not alongside the last one that runs
into it.

The proposed shape, evolving out of the current `HotRodConnection` /
`HotRodCluster` split rather than replacing it outright:

```
HotRodClient
    |
    +-- topology manager
    +-- connection pools (one per node)
    +-- router (segment owner -> pool)
    +-- authentication, TLS config
    |
RemoteCache (one per named cache, borrowed from HotRodClient)
    |
    +-- get / put / remove / bulk / ...
```

`HotRodConnection` keeps its role as the single, sequential connection
type; the pool holds several of them per node instead of `HotRodCluster`
holding exactly one per node. This is its own ADR before implementation
starts, since it changes the public API of `hotrod-protocol`.

## Proposed ordering

| Priority | Item | Issue | Why this order |
| --- | --- | --- | --- |
| P0 | Connection pooling / concurrent operations | (new, precedes the rest) | Structural blocker described above |
| P0 | Multi-node CI fixture | (new) | The cluster code path is currently exercised only by `#[ignore]`d manual tests; failover and rebalance need a real multi-node run in CI |
| P1 | Client listeners (cache events) | #4 | Prerequisite for near caching |
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
