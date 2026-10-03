# hotrod-protocol

Pure Rust client for the Hot Rod binary wire protocol used by
[Infinispan](https://infinispan.org) and Red Hat Data Grid.

## Status

`hotrod-protocol` mirrors the official Java client's core cache API and
its cluster-aware routing. Everything else is an open gap, tracked as
its own issue. See the
[ADRs](https://github.com/marciopaiva/infinispan-hotrod-rs/tree/main/docs/adr)
for how each phase was scoped and the
[changelog](https://github.com/marciopaiva/infinispan-hotrod-rs/blob/main/hotrod-protocol/CHANGELOG.md)
for release notes.

| Feature | Java client | `hotrod-protocol` |
| --- | --- | --- |
| Core operations (get, put, remove, putIfAbsent, replace, versioned variants) | Yes | Yes |
| Bulk operations (getAll, putAll, removeAll) | Yes | Yes |
| containsKey, ping, size, clear, stats | Yes | Yes |
| Full entry metadata (creation, last used, lifespan, max idle) | Yes | Yes |
| Authentication: PLAIN, SCRAM-SHA-512, DIGEST-SHA-256, OAUTHBEARER | Yes | Yes |
| Authentication: GSSAPI | Yes | No ([#9](https://github.com/marciopaiva/infinispan-hotrod-rs/issues/9)) |
| TLS and mutual TLS | Yes | Yes |
| Cluster topology tracking, hash-aware routing | Yes | Yes |
| Client listeners (cache events) | Yes | Yes |
| Near caching | Yes | Yes |
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

Against a distributed cache spread over a cluster, `HotRodClient`
tracks topology and routes each request directly to its owning node.
Unlike `HotRodConnection`, it is not bound to one cache: `cache` returns
a `RemoteCache` handle for a named cache, and both the client and its
cache handles are cheap to clone and share across tasks, since their
operations take `&self`:

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

A `RemoteCache` can also register a client listener, delivered on its
own dedicated connection as `CacheEvent`s pulled one at a time:

```rust
let mut listener = cache.listen().await?;
while let Some(event) = listener.next().await {
    match event? {
        CacheEvent::Created { key, .. } => println!("created {key:?}"),
        CacheEvent::Removed { key, .. } => println!("removed {key:?}"),
        _ => {}
    }
}
```

`near_cache` wraps a `RemoteCache` with a bounded local cache for `get`,
invalidated through a listener running in the background. Everything
besides `get`/`put`/`remove`/`clear` is still reached straight through
to the underlying `RemoteCache`:

```rust
use hotrod_protocol::NearCacheOptions;

let near = cache.near_cache(NearCacheOptions::default()).await?;
near.put(b"key", b"value", Expiration::Default, Expiration::Default)
    .await?;
let value = near.get(b"key").await?; // served from the network once, cached after
let count = near.size().await?; // reached through to the underlying RemoteCache
```

## License

Dual licensed under MIT or Apache-2.0, at your option. See the
repository's `LICENSE-MIT` and `LICENSE-APACHE`.
