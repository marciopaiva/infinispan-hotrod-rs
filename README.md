# infinispan-hotrod-rs

A pure Rust client for the Hot Rod binary wire protocol used by
[Infinispan](https://infinispan.org) and Red Hat Data Grid, plus a PHP
extension, not started yet, that will expose it to PHP userland.

[![CI](https://github.com/marciopaiva/infinispan-hotrod-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/marciopaiva/infinispan-hotrod-rs/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/hotrod-protocol.svg)](https://crates.io/crates/hotrod-protocol)
[![docs.rs](https://docs.rs/hotrod-protocol/badge.svg)](https://docs.rs/hotrod-protocol)
[![Downloads](https://img.shields.io/crates/d/hotrod-protocol.svg)](https://crates.io/crates/hotrod-protocol)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

[Install](#install) | [Quick start](#quick-start) | [Status](#status) |
[Feature matrix](#feature-matrix) | [Known limitations](#known-limitations) |
[Layout](#layout) | [Documentation](#documentation) | [Why](#why) |
[License](#license)

## Status

`hotrod-protocol` is at 0.8.0 and implements nearly all of the official
Java client's feature set: the core cache API, cluster-aware routing,
TLS, client listeners, near caching, transactions, distributed
counters, multimap caches, remote query, remote task execution and
remote administration. What remains is small and named, not an open
category: GSSAPI authentication, trace-context propagation to the
server, and a short list of behaviors that are implemented and
unit-tested but not yet exercised against a real cluster (see
[Known limitations](#known-limitations) and the notes in the
[feature matrix](#feature-matrix)).

The public API is pre-1.0: a minor version bump may still break it.
See [the API stability policy](docs/adr/0027-api-stability-policy.md)
for exactly what that promise covers.

The PHP extension (`php-ext/`) has not started. Every claim above is
about `hotrod-protocol` alone.

## Install

```toml
[dependencies]
hotrod-protocol = "0.8"
tokio = { version = "1", features = ["full"] }
```

`hotrod-protocol` needs an async runtime: every connect and operation
is async on [Tokio](https://tokio.rs). Built for Rust edition 2021. TLS,
when used, goes through [`rustls`](https://github.com/rustls/rustls),
never OpenSSL, so there is no system TLS library to install.

## Quick start

Against a single node:

```rust
use hotrod_protocol::{Expiration, HotRodConnection};

let mut conn = HotRodConnection::connect("127.0.0.1:11222", "my-cache").await?;
conn.authenticate_plain("", "user", "password").await?;

conn.put(b"key", b"value", Expiration::Default, Expiration::Default)
    .await?;
let value = conn.get(b"key").await?;
```

Against a cluster, `HotRodClient` tracks topology and routes each
request directly to the node that owns the key, instead of relying on
server-side redirects. `RemoteCache` is the handle for one named cache:
cheap to clone, safe to share across tasks.

```rust
use hotrod_protocol::{Expiration, HotRodClient};

let seeds = ["127.0.0.1:11222".parse()?, "127.0.0.1:11322".parse()?];
let client = HotRodClient::connect(&seeds).await?;
client.authenticate_plain("", "user", "password").await?;
let cache = client.cache("distributed");

cache
    .put(b"key", b"value", Expiration::Default, Expiration::Default)
    .await?;
let value = cache.get(b"key").await?;
```

More examples, client listeners, near caching, TLS, transactions,
typed values and remote query among them, are in
[`hotrod-protocol/README.md`](hotrod-protocol/README.md).

## Feature matrix

The table keeps two things apart that a single yes/no column tends to
blur: whether a feature has a public API at all (`hotrod-protocol`),
and whether that API has been exercised against a real Infinispan
server rather than only the fake-server harness the rest of the unit
tests use.

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
| PHP extension | n/a | Not started | n/a |

"Validated" means there is a test under
`hotrod-protocol/tests/live_server.rs`, gated behind `#[ignore]` and
run in CI against a real server (`.github/workflows/ci.yml`), that
puts that specific feature through its paces. "Partial" means some
paths are covered that way and others are not, explained in the row's
own notes. Neither column is a claim of production readiness on its
own: a row's notes carry the limitations that still apply even where a
feature is implemented and validated.

## Known limitations

- GSSAPI authentication is not implemented
  ([#9](https://github.com/marciopaiva/infinispan-hotrod-rs/issues/9)).
- Trace-context propagation to the server is not implemented
  ([#54](https://github.com/marciopaiva/infinispan-hotrod-rs/issues/54)):
  local `tracing` spans work today, the server never sees them.
- Transactions are `NON_XA`, one-phase commit only. Real XA and
  recovery are not implemented.
- Counters are not hash-routed by name the way the Java client routes
  them: every counter operation goes through the active seed.
- A handful of behaviors are implemented and unit-tested but not yet
  exercised against a real server: remote query with actual matching
  rows, remote task execution, the retry policy under a live
  mid-operation node failure, and automatic multi-cluster failover.
  See the matching rows' notes in the [feature matrix](#feature-matrix).
- The PHP extension has not started.

## Layout

* `hotrod-protocol/`: the protocol client itself. No PHP awareness, no
  dependency on the extension crate.
* `php-ext/`: a PHP extension built with
  [`ext-php-rs`](https://github.com/davidcole1340/ext-php-rs) that bridges
  `hotrod-protocol` into PHP.
* `docs/`: [Architecture Decision Records](docs/adr/) for how each
  phase was scoped, and a [roadmap](docs/roadmap.md) proposal for
  what is left.
* `ci/`: the scripts that start the real Infinispan servers CI and
  local live-server test runs use (plain, TLS, cluster, multi-cluster).
* `bench/`: a small Rust benchmark harness, dev-only, not published.
* `fuzz/`: fuzz targets for the wire-level parsers (`cargo fuzz`).

## Documentation

* [Architecture Decision Records](docs/adr/): the record of what was
  decided and why, one per architectural choice.
* [Roadmap](docs/roadmap.md): a proposal for ordering the open issues,
  not a commitment.
* [Changelog](hotrod-protocol/CHANGELOG.md): release notes, per
  version.
* [`hotrod-protocol/README.md`](hotrod-protocol/README.md): the full
  usage guide, with more examples than fit here.

## Why

No official Hot Rod client exists for Rust, and no PHP client exists for
the Hot Rod protocol either. The closest prior art is a REST-only Rust
client (`infinispan-rs`, archived) and a PHP wrapper around the official
C++ client (`php-hotrod`, unmaintained since its first commits). Neither
implements the binary protocol in a language-native way, and neither is
maintained.

## License

Dual licensed under MIT or Apache-2.0, at your option. See `LICENSE-MIT`
and `LICENSE-APACHE`.
