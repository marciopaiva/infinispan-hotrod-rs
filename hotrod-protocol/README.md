# hotrod-protocol

Pure Rust client for the Hot Rod binary wire protocol used by
[Infinispan](https://infinispan.org) and Red Hat Data Grid.

## Status

Phase 1 complete: a single connection to one cache, PLAIN authentication,
and the core operations (`get`, `put`, `remove`, `put_if_absent`,
`replace`, `replace_if_unmodified`, `remove_if_unmodified`). See the
[roadmap ADR](https://github.com/marciopaiva/infinispan-hotrod-rs/blob/main/docs/adr/0001-mirror-java-client-scope.md)
for what comes next and the
[changelog](https://github.com/marciopaiva/infinispan-hotrod-rs/blob/main/hotrod-protocol/CHANGELOG.md)
for release notes.

## Usage

```rust
use hotrod_protocol::{Expiration, HotRodConnection};

let mut conn = HotRodConnection::connect("127.0.0.1:11222", "").await?;
conn.authenticate_plain("", "user", "password").await?;

conn.put(b"key", b"value", Expiration::Default, Expiration::Default)
    .await?;
let value = conn.get(b"key").await?;
```

## License

Dual licensed under MIT or Apache-2.0, at your option. See the
repository's `LICENSE-MIT` and `LICENSE-APACHE`.
