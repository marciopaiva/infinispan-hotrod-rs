# ADR 0001: Target the Java Hot Rod client's feature scope, in phases

## Status

Accepted

## Context

No Rust client for the Hot Rod binary protocol exists today. The Java
client is the reference implementation and defines the fullest feature
set: `RemoteCacheManager`/`RemoteCache`, conditional operations
(`putIfAbsent`, `replace`, versioned replace), multiple SASL mechanisms,
cache event listeners, hash-aware intelligent routing, and near caching.

Matching all of that before a first working client exists would delay any
usable release indefinitely.

## Decision

Use the Java client's feature set as the long-term target for
`hotrod-protocol`, not as the initial scope. Build it in phases, each one
a working, testable increment:

1. Core operations: `get`, `put`, `remove`, and the conditional operations
   (`putIfAbsent`, `replace`, versioned replace), with PLAIN as the only
   SASL mechanism.
2. Broader SASL support: DIGEST-SHA-256, SCRAM-SHA-512, GSSAPI,
   OAUTHBEARER.
3. Cluster topology and hash-aware intelligent routing, so the client
   resolves the owning node itself instead of relying on server-side
   redirects.
4. Cache event listeners, including server-side event filtering.
5. Near caching: a client-side cache of recently accessed entries, with
   eviction.

Each phase is tracked as a GitHub issue. Starting a phase still goes
through the Analyze/Propose steps in `CLAUDE.md` section 4, since each one
touches the public API of `hotrod-protocol`.

## Consequences

* Early releases will not match the Java client's feature set. That is
  intentional, not an oversight, and should be stated plainly in release
  notes.
* The public API of `hotrod-protocol` is not considered stable until
  phase 1 ships. It may still change once phase 1 is done, but each such
  change needs its own Propose step per section 5.
* Phases 2 through 5 can reorder if a real use case needs one earlier than
  planned. The order above is a default, not a commitment.
