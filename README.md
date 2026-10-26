# infinispan-hotrod-rs

A pure Rust client for the Hot Rod binary wire protocol used by
[Infinispan](https://infinispan.org) and Red Hat Data Grid, plus a PHP
extension that exposes it to PHP userland.

## Status

`hotrod-protocol` mirrors the official Java client's core cache API and
its cluster-aware routing. Everything else is an open gap, tracked as
its own issue. See `docs/adr/` for how each phase was scoped and
`hotrod-protocol/CHANGELOG.md` for release notes.

The table below keeps two things apart that a single yes/no column
tends to blur together: whether a feature has a public API at all
(`hotrod-protocol`), and whether that API has actually been exercised
against a real Infinispan server rather than only the fake-server
harness used by the rest of the unit tests. "Validated" means there is
a test under `hotrod-protocol/tests/live_server.rs`, gated behind
`#[ignore]` and run in CI against a real server
(`.github/workflows/ci.yml`), that puts that specific feature through
its paces. "Partial" means some paths are covered that way and others
are not, explained in the notes. Neither column is a claim of
production readiness on its own: the per-row notes carry the
limitations that still apply even where a feature is implemented and
validated.

| Feature | Java client | `hotrod-protocol` | Validated against a real server |
| --- | --- | --- | --- |
| Core operations (get, put, remove, putIfAbsent, replace, versioned variants) | Yes | Yes | Yes |
| Bulk operations (getAll, putAll) | Yes | Yes | Yes |
| containsKey, ping, size, clear, stats | Yes | Yes | Yes |
| Full entry metadata (creation, last used, lifespan, max idle) | Yes | Yes | No (fake-server tests only so far) |
| Authentication: PLAIN, SCRAM-SHA-512, DIGEST-SHA-256, OAUTHBEARER | Yes | Yes | Partial (PLAIN, SCRAM-SHA-512 and DIGEST-SHA-256 are; OAUTHBEARER has no OAuth provider in the CI fixture yet, so it is fake-server tested only) |
| Authentication: GSSAPI | Yes | No ([#9](https://github.com/marciopaiva/infinispan-hotrod-rs/issues/9)) | n/a |
| TLS and mutual TLS | Yes | Yes | Yes |
| Cluster topology tracking, hash-aware routing | Yes | Yes | Yes |
| Configurable retry policy, per-node circuit breaker | Yes | Yes | No (no live test injects a mid-operation node failure yet; the ambiguous-execution-on-retry semantics in `docs/adr/0011-retry-policy-and-node-health.md` are documented, not failure-tested against a real server) |
| Client listeners (cache events) | Yes | Yes | Yes |
| Near caching | Yes | Yes | Yes |
| Transactions | Yes | Yes (`NON_XA`, one-phase commit only; real XA and recovery not yet implemented) | Yes |
| Multimap cache | Yes | Yes | Yes |
| Counters | Yes | Yes (not hash-routed by counter name like the Java client; always goes to the seed; value-change listeners supported) | Yes |
| Typed values (`Marshaller` trait, `TypedCache`) | Yes | Yes (bytes/UTF-8 built in; bring your own format otherwise; also covers remote query and multimap cache) | Yes |
| Remote query (Protobuf / Ickle) | Yes | Yes (entities/projections as bytes or scalars; `execute_statement` for DELETE/UPDATE; `stream` pages through large result sets instead of buffering) | Partial (the request/response path is, but only with zero matching rows: this crate has no Protobuf marshaller of its own to store a real entity with, see `docs/adr/0013-remote-query.md`) |
| Remote task execution | Yes | Yes (`execute`/`execute_near`; params and result are raw bytes, no typed wrapper yet) | No (the CI server image has no script engine to deploy a task with, confirmed empirically and documented in `docs/adr/0025-remote-task-execution.md`) |
| Streaming for large values | Yes | Yes | Yes |
| Server-side iteration | Yes | Yes | Yes |
| Client statistics | Yes | Yes | Yes |
| Tracing: local spans (via the `tracing` crate) | No | Yes | n/a (local-only, nothing server-side to validate against) |
| Tracing: trace-context propagation to the server | Yes | No ([#54](https://github.com/marciopaiva/infinispan-hotrod-rs/issues/54)) | n/a |
| Remote administration (create/remove/list caches, reindex, index schema, config attribute, alias, templates) | Yes | Yes | Yes |
| Multi-cluster failover | Yes | Yes (plus proactive switch-back once an abandoned cluster recovers, and per-cluster auth/TLS overrides) | Partial (a manual switch between clusters is; automatic failover triggered by a live connection failure is covered by a fake-server unit test only, not yet by a live one) |

The PHP extension has not started yet.

## Layout

* `hotrod-protocol/`: the protocol client itself. No PHP awareness, no
  dependency on the extension crate.
* `php-ext/`: a PHP extension built with
  [`ext-php-rs`](https://github.com/davidcole1340/ext-php-rs) that bridges
  `hotrod-protocol` into PHP.

## Why

No official Hot Rod client exists for Rust, and no PHP client exists for
the Hot Rod protocol either. The closest prior art is a REST-only Rust
client (`infinispan-rs`, archived) and a PHP wrapper around the official
C++ client (`php-hotrod`, unmaintained since its first commits). Neither
implements the binary protocol in a language-native way.

## License

Dual licensed under MIT or Apache-2.0, at your option. See `LICENSE-MIT`
and `LICENSE-APACHE`.
