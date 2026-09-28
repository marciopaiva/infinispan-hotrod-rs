# infinispan-hotrod-rs

A pure Rust client for the Hot Rod binary wire protocol used by
[Infinispan](https://infinispan.org) and Red Hat Data Grid, plus a PHP
extension that exposes it to PHP userland.

## Status

Phase 1 complete: a single connection to one cache, PLAIN authentication,
and the core operations (`get`, `put`, `remove`, `put_if_absent`,
`replace`, `replace_if_unmodified`, `remove_if_unmodified`).

Phase 2 complete: SCRAM-SHA-512, DIGEST-SHA-256 and OAUTHBEARER
authentication (GSSAPI deferred to its own issue). See
`docs/adr/0001-mirror-java-client-scope.md` for the full roadmap,
`docs/adr/0002-phase-2-sasl-scope.md` for how phase 2 was scoped, and
`hotrod-protocol/CHANGELOG.md` for release notes.

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
