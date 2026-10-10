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
| Configurable retry policy, per-node circuit breaker | Yes | Yes |
| Client listeners (cache events) | Yes | Yes |
| Near caching | Yes | Yes |
| Transactions | Yes | No |
| Multimap cache | Yes | No |
| Counters | Yes | No |
| Remote query (Protobuf / Ickle) | Yes | No |
| Remote task execution | Yes | No |
| Streaming for large values | Yes | Yes |
| Server-side iteration | Yes | Yes |
| Client statistics | Yes | Yes |
| Tracing: local spans (via the `tracing` crate) | No | Yes |
| Tracing: trace-context propagation to the server | Yes | No |
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

`get_stream`/`put_stream` read or write a value in chunks instead of
buffering it whole:

```rust
let mut put = cache
    .put_stream(b"key", Expiration::Default, Expiration::Default, 8192)
    .await?;
put.write_chunk(&data).await?; // call as many times as needed
put.finish().await?; // nothing is written until this commits it

let mut get = cache.get_stream(b"key", 8192).await?.expect("entry exists");
while let Some(chunk) = get.next_chunk().await? {
    process(chunk);
}
```

`iter`/`iter_with` walk every entry in the cache server-side, one batch
at a time, instead of requiring the caller to already know every key.
On a cluster, this opens one cursor per node in turn, covering every
segment:

```rust
let mut entries = cache.iter().await?;
while let Some(entry) = entries.next_entry().await? {
    process(entry.key, entry.value);
}
```

`statistics`/`reset_statistics` report hit/miss counts and average
read/store/remove time for a `RemoteCache`, always collected (no
configuration flag needed); `near_cache_statistics` reports the same
idea for a near cache's own hit/miss/invalidation counts and current
local size. `pool_statistics` reports idle and checked-out connection
counts per node, with no Java client equivalent:

```rust
let stats = cache.statistics();
println!("{} hits, {} misses", stats.remote_hits, stats.remote_misses);

for pool in client.pool_statistics() {
    println!("{}: {} idle, {} checked out", pool.address, pool.idle_connections, pool.checked_out_connections);
}
```

Every operation also opens a [`tracing`](https://docs.rs/tracing) span
named `hotrod_operation`, carrying the cache name and the operation's
name and duration (never the key or value), with an error event on
failure. Install any `tracing` subscriber to see them; without one,
this costs nothing:

```rust
tracing_subscriber::fmt::init(); // or any other subscriber
cache.get(b"key").await?; // now shows up as a span in whatever the subscriber does with it
```

## License

Dual licensed under MIT or Apache-2.0, at your option. See the
repository's `LICENSE-MIT` and `LICENSE-APACHE`.
