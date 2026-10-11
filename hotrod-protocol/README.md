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
| Bulk operations (getAll, putAll) | Yes | Yes |
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
| Multimap cache | Yes | Yes |
| Counters | Yes | Yes (not hash-routed by counter name like the Java client; always goes to the seed) |
| Typed values (`Marshaller` trait, `TypedCache`) | Yes | Yes (bytes/UTF-8 built in; bring your own format otherwise) |
| Remote query (Protobuf / Ickle) | Yes | Yes (entities/projections as bytes or scalars; no DELETE/UPDATE statements yet) |
| Remote task execution | Yes | No |
| Streaming for large values | Yes | Yes |
| Server-side iteration | Yes | Yes |
| Client statistics | Yes | Yes |
| Tracing: local spans (via the `tracing` crate) | No | Yes |
| Tracing: trace-context propagation to the server | Yes | No |
| Remote administration (create/remove/list caches) | Yes | Yes |
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

`typed` wraps a `RemoteCache` with a typed façade: marshal/unmarshal
keys and values through a `Marshaller` instead of handling raw bytes
directly. `BytesMarshaller`/`Utf8Marshaller` ship built in; implement
`Marshaller` yourself for JSON, Protobuf or anything else:

```rust
use hotrod_protocol::Utf8Marshaller;

let typed = cache.typed(Utf8Marshaller, Utf8Marshaller);
typed
    .put(&"key".to_string(), &"value".to_string(), Expiration::Default, Expiration::Default)
    .await?;
let value = typed.get(&"key".to_string()).await?;
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

`register_proto_schema` registers a `.proto` schema with the server;
`query` runs an Ickle query, with named parameters and paging, against
entries stored in that schema's format:

```rust
client
    .register_proto_schema("book.proto", "package demo;\nmessage Book {\n  optional string title = 1;\n}\n")
    .await?;

let result = cache
    .query("FROM demo.Book WHERE title = :title")
    .param("title", QueryValue::String("Dune".to_string()))
    .max_results(10)
    .execute()
    .await?;

for row in result.rows {
    if let QueryRow::Entity(bytes) = row {
        // decode `bytes` with whatever Marshaller matches the Book schema
    }
}
```

`administration` creates, removes and lists caches, no shell or REST
access to the server needed. `CacheConfig::Template` names an
existing template, `CacheConfig::Definition` passes a full
configuration document (XML, JSON or YAML, auto-detected server-side):

```rust
use hotrod_protocol::{AdminFlag, CacheConfig};

client
    .administration()
    .with_flags([AdminFlag::Volatile])
    .create_cache("sessions", CacheConfig::Template("org.infinispan.DIST_SYNC".to_string()))
    .await?;

let names = client.administration().cache_names().await?;
client.administration().remove_cache("sessions").await?;
```

`counters` defines and uses distributed counters. A `StrongCounter`
is atomic and, if bounded, rejects an update that would cross its
configured bound; a `WeakCounter` is cheaper but does not support
`compare_and_swap`/`get_and_set`:

```rust
use hotrod_protocol::{CounterConfiguration, CounterType, Storage};

let counters = client.counters();
counters
    .define(
        "requests-served",
        CounterConfiguration {
            counter_type: CounterType::UnboundedStrong,
            initial_value: 0,
            storage: Storage::Persistent,
        },
    )
    .await?;

let counter = counters.strong_counter("requests-served");
let total = counter.increment_and_get().await?;
```

`multimap_cache` is a cache where each key maps to a collection of
values instead of one. Not a special cache type server-side: the
named cache still has to exist, the same as any other:

```rust
let tags = client.multimap_cache("tags-by-user", true);
tags.put(b"user:42", b"admin", Expiration::Default, Expiration::Default).await?;
tags.put(b"user:42", b"beta-tester", Expiration::Default, Expiration::Default).await?;
let values = tags.get(b"user:42").await?; // every value stored under this key
```

## License

Dual licensed under MIT or Apache-2.0, at your option. See the
repository's `LICENSE-MIT` and `LICENSE-APACHE`.
