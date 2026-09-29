# infinispan-hotrod-rs

A pure Rust client for the Hot Rod binary wire protocol used by
[Infinispan](https://infinispan.org) and Red Hat Data Grid, plus a PHP
extension that exposes it to PHP userland.

## Status

`hotrod-protocol` mirrors the official Java client's core cache API and
its cluster-aware routing. Everything else is an open gap, tracked as
its own issue. See `docs/adr/` for how each phase was scoped and
`hotrod-protocol/CHANGELOG.md` for release notes.

| Feature | Java client | `hotrod-protocol` |
| --- | --- | --- |
| Core operations (get, put, remove, putIfAbsent, replace, versioned variants) | Yes | Yes |
| Bulk operations (getAll, putAll, removeAll) | Yes | Yes |
| containsKey, ping, size, clear, stats | Yes | Yes |
| Full entry metadata (creation, last used, lifespan, max idle) | Yes | Yes |
| Authentication: PLAIN, SCRAM-SHA-512, DIGEST-SHA-256, OAUTHBEARER | Yes | Yes |
| Authentication: GSSAPI | Yes | No ([#9](https://github.com/marciopaiva/infinispan-hotrod-rs/issues/9)) |
| TLS and mutual TLS | Yes | No |
| Cluster topology tracking, hash-aware routing | Yes | Yes |
| Client listeners (cache events) | Yes | No ([#4](https://github.com/marciopaiva/infinispan-hotrod-rs/issues/4)) |
| Near caching | Yes | No ([#5](https://github.com/marciopaiva/infinispan-hotrod-rs/issues/5)) |
| Transactions | Yes | No |
| Multimap cache | Yes | No |
| Counters | Yes | No |
| Remote query (Protobuf / Ickle) | Yes | No |
| Remote task execution | Yes | No |
| Streaming for large values | Yes | No |
| Server-side iteration | Yes | No |
| Stats and telemetry (metrics, tracing) | Yes | No |
| Remote administration | Yes | No |
| Multi-cluster failover | Yes | No |

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
