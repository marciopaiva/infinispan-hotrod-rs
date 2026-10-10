# ADR 0012: Serialization abstraction (Marshaller trait + TypedCache)

## Status

Accepted, implemented in #99.

## Context

The "Typed data" roadmap theme opened with: "`hotrod-protocol` is
deliberately byte-oriented today, with no built-in notion of typed
values; a `Marshaller`-shaped trait would let a caller work with typed
values without baking a specific format into the protocol layer." No
issue existed yet. This phase precedes remote query (#50), which
depends on it and on Protobuf schema registration against the server.

Confirmed by reading the current code: every `RemoteCache` method
(`get`/`put`/`put_if_absent`/`replace`/`replace_if_unmodified`/
`remove`/`remove_if_unmodified`/`get_with_version`/`contains_key`/
`get_all`/`put_all`/streaming/iteration/listening) is byte-oriented
(`&[u8]`/`Vec<u8>`), without exception. `VersionedValue` only types
its metadata (version, timestamps, expiration); `value` stays
`Vec<u8>`. The protocol itself negotiates no media type on any data
operation today: `wire.rs`'s `write_no_media_type_pair` always writes
two zero bytes on every request. `hotrod-protocol`'s `Cargo.toml` has
no serialization dependency.

The design is pinned against the Java client's own source
(`infinispan/infinispan`), the same discipline every prior phase used.

**`Marshaller`** (`commons/.../marshall/Marshaller.java`): a single
interface covering both directions (`objectToByteBuffer`/
`objectFromByteBuffer`), plus `mediaType()`/`isMarshallable()`/
`getBufferSizePredictor()`. No real type reflection: `K`/`V` on
`RemoteCache<K,V>` are the caller's own cast responsibility, not
something the interface enforces.

Built-in marshallers cover a spectrum with no Rust equivalent or need
(native Java serialization, JBoss Marshalling) and two trivial ones
that do translate directly: a `byte[]` passthrough
(`IdentityMarshaller`/`BytesOnlyMarshaller`) and a UTF-8 `String` one
(`UTF8StringMarshaller`).

**Media type is configuration, not per-request negotiation.** It only
appears on the wire in the per-cache `Ping` response
(`key_media_type`/`value_media_type`, the same two-byte format
`wire.rs`'s `skip_media_type` already knows how to skip), used to
*discover* the server's storage media type for that cache, never to
negotiate one per operation.

**The default marshaller changed version.** Before Infinispan 10, it
was `GenericJBossMarshaller`; from 10.0 onward, `ProtoStreamMarshaller`
(Protobuf), still the default today. This confirms remote query (#50)
is where a real, heavier marshaller and real media-type negotiation
belong; this phase is only the client-side convenience layer that
precedes it.

Marshalling errors have a dedicated type, `MarshallingException`.

## Decision

**`TypedCache<MK, MV>` wraps `RemoteCache` via `Deref`, the same
pattern `NearCachedCache` already uses.** Additive, no risk to the
existing public API: no `RemoteCache` method changes shape.
`TypedCache` only marshals/unmarshals and delegates to the existing
byte-oriented `RemoteCache`, so routing (including backup owners), the
retry chain and its circuit breaker (ADR 0011), statistics and tracing
(ADR 0010) all keep working with no change at all: the marshalled key
is exactly what `hash::segment` has always routed on.

**No new dependency in this phase.** Only the `Marshaller` trait plus
two dependency-free marshallers (raw bytes, UTF-8 string). A
`serde_json` (or Protobuf) marshaller is left for a separate future
proposal, when and if it is wanted: #50 (remote query) is where
Protobuf actually belongs, with its own dependency decision.

## Design

**`Marshaller` is an associated-type trait, not a type parameter
(`marshall.rs`).**

```rust
pub trait Marshaller: Send + Sync {
    type Value;
    type Error: std::error::Error + Send + Sync + 'static;

    fn marshall(&self, value: &Self::Value) -> Result<Vec<u8>, Self::Error>;
    fn unmarshall(&self, bytes: &[u8]) -> Result<Self::Value, Self::Error>;
}
```

`Value` as an associated type, not `Marshaller<T>`, so `TypedCache<MK,
MV>` only has to name the marshaller types (`K`/`V` follow as
`MK::Value`/`MV::Value`), the same way `Iterator::Item` saves an
iterator from also being generic over what it yields.

Two built-in implementations, both zero-dependency: `BytesMarshaller`
(`Value = Vec<u8>`, `Error = Infallible`, passthrough, mirroring the
Java client's `IdentityMarshaller`/`BytesOnlyMarshaller`) and
`Utf8Marshaller` (`Value = String`, `Error = FromUtf8Error`,
mirroring `UTF8StringMarshaller`).

`Error::Marshalling(Box<dyn std::error::Error + Send + Sync>)` added
to `error.rs`: `TypedCache` wraps any `MK::Error`/`MV::Error` into
this, so every method still returns this crate's own `Result<T>`
rather than forcing a specific error type on every marshaller
implementor.

**`TypedCache<MK, MV>` (`typed_cache.rs`), `Arc`-wrapped marshallers,
not a `Clone` bound on `MK`/`MV` themselves**, so a marshaller can hold
state that is not cheap or not possible to clone (a compiled schema,
for instance) without this type needing to change later.
`RemoteCache::typed(key_marshaller, value_marshaller) ->
TypedCache<MK, MV>` is synchronous and cheap, unlike `near_cache`
(which registers a listener): nothing here needs a network round trip
to set up.

Typed methods cover `get`/`put`/`put_if_absent`/`replace`/
`remove`/`get_with_version`/`replace_if_unmodified`/
`remove_if_unmodified`/`contains_key`/`get_all`/`put_all`, each
marshalling its arguments, delegating to the matching byte-oriented
method, then unmarshalling the result. `TypedVersionedValue<V>` (new,
separate type, not a generic parameter retrofitted onto
`VersionedValue`) mirrors `VersionedValue`'s metadata with `value: V`
instead of `Vec<u8>`, so the existing public byte-oriented type never
changes shape. `version: u64` on the versioned operations stays a
plain `u64` in both the typed and byte-oriented surface: an opaque
server-issued token, not something to marshal.

Everything else (`get_stream`/`put_stream*`, `iter`/`iter_with`,
`listen`/`listen_with`, `near_cache`, `statistics`, `ping`/`size`/
`clear`/`stats`) is reached through `Deref`, unmodified, the same way
`NearCachedCache` leaves most of `RemoteCache` untouched rather than
reimplementing it: no typed equivalent in this phase.

## Consequences

* New public API: `Marshaller`, `BytesMarshaller`, `Utf8Marshaller`
  (`marshall.rs`); `TypedCache`, `TypedVersionedValue`
  (`typed_cache.rs`); `RemoteCache::typed`; `Error::Marshalling`.
  Additive: nothing existing changes shape.
* A caller who wants JSON, Protobuf or anything else implements
  `Marshaller` themselves against whatever crate they already depend
  on. `hotrod-protocol` does not pick a serialization format for them,
  consistent with the roadmap's own stated preference elsewhere
  (explicit `putBytes`/`putJson` over implicit conversion for the PHP
  bridge) against guessing a format on a caller's behalf.
* `get_all`/`put_all`'s typed versions inherit the same routing
  trade-off the byte-oriented ones already have (always the seed
  connection, never split client-side by owner, per issues #40/#41/
  #68): nothing new here, just carried through unchanged.
* No typed version of streaming, server-side iteration, client
  listeners or near caching in this phase. Add one later for whichever
  of these turns out to need it, rather than speculatively building
  all four now.
* No real media-type negotiation with the server (the per-cache
  `Ping` discovery the Java client does) and no built-in marshaller
  beyond raw bytes and UTF-8 text. Both belong to remote query (#50),
  which actually needs them.
