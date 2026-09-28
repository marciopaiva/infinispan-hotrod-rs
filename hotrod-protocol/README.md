# hotrod-protocol

Pure Rust client for the Hot Rod binary wire protocol used by
[Infinispan](https://infinispan.org) and Red Hat Data Grid.

## Status

Phase 1 complete: a single connection to one cache, PLAIN authentication,
and the core operations (`get`, `put`, `remove`, `put_if_absent`,
`replace`, `replace_if_unmodified`, `remove_if_unmodified`).

Phase 2 complete: SCRAM-SHA-512 and DIGEST-SHA-256 authentication, both
validated against a live server, plus OAUTHBEARER with unit test coverage
only (it needs a token-backed realm the CI fixture does not provide yet).
GSSAPI is deferred to its own issue.

Phase 3 complete: `HotRodCluster` tracks cluster topology and routes
each request to the segment's primary owner using Infinispan's own
`MurmurHash3`, instead of relying on server-side redirects. Validated
manually against a local two-node cluster, since hash-aware routing has
nothing to exercise on the single-node CI fixture's local cache. See
the
[roadmap ADR](https://github.com/marciopaiva/infinispan-hotrod-rs/blob/main/docs/adr/0001-mirror-java-client-scope.md),
the
[phase 2 scope ADR](https://github.com/marciopaiva/infinispan-hotrod-rs/blob/main/docs/adr/0002-phase-2-sasl-scope.md),
the
[phase 3 scope ADR](https://github.com/marciopaiva/infinispan-hotrod-rs/blob/main/docs/adr/0003-hash-aware-routing-scope.md)
and the
[changelog](https://github.com/marciopaiva/infinispan-hotrod-rs/blob/main/hotrod-protocol/CHANGELOG.md)
for details.

## Usage

```rust
use hotrod_protocol::{Expiration, HotRodConnection};

let mut conn = HotRodConnection::connect("127.0.0.1:11222", "").await?;
conn.authenticate_plain("", "user", "password").await?;
// Or: conn.authenticate_scram("user", "password").await?;
// Or: conn.authenticate_digest("user", "password").await?;

conn.put(b"key", b"value", Expiration::Default, Expiration::Default)
    .await?;
let value = conn.get(b"key").await?;
```

Against a distributed cache spread over a cluster, `HotRodCluster`
routes each request directly to its owning node:

```rust
use hotrod_protocol::{Expiration, HotRodCluster};

let seeds = ["127.0.0.1:11222".parse()?, "127.0.0.1:11322".parse()?];
let mut cluster = HotRodCluster::connect(&seeds, "distributed").await?;
cluster.authenticate_plain("", "user", "password").await?;

cluster
    .put(b"key", b"value", Expiration::Default, Expiration::Default)
    .await?;
let value = cluster.get(b"key").await?;
```

## License

Dual licensed under MIT or Apache-2.0, at your option. See the
repository's `LICENSE-MIT` and `LICENSE-APACHE`.
