# ADR 0023: TypedCache integration for remote query and multimap cache

## Status

Accepted, implemented in #111.

## Context

ADR 0013 (`RemoteCache::query`, #50) and ADR 0016 (`MultimapCache`,
#48) each deliberately left out `TypedCache` integration as the same
kind of cut: "results stay raw bytes for the caller's own
`Marshaller` to decode." Issue #111 asked for a single pass covering
both, since `TypedCache` itself (ADR 0012, #99) already established
the pattern this phase generalizes, not a new one.

Reading `typed_cache.rs`, `query.rs` and `multimap.rs` in full
confirmed the shape of the gap:

* `TypedCache<MK, MV>` already wraps `RemoteCache` via `Deref`,
  marshalling arguments and unmarshalling results around the
  existing byte-oriented methods, with `RemoteCache::typed` (the
  entry point) living in `remote_cache.rs` and the wrapper type in
  `typed_cache.rs`. The same split applies to `MultimapCache`.
* A query's named parameters (`Query::param(name, QueryValue)`) are
  already typed by the Ickle type system itself: `QueryValue` has no
  relationship to a cache's own key type. Only the **row** side needs
  a `Marshaller`, and only for `QueryRow::Entity(bytes)` (a whole
  entity); `QueryRow::Columns(Vec<QueryValue>)` (a projection) is
  already scalar-typed by the wire format, with nothing for a
  `Marshaller` to do.
* `MultimapCache` is structurally a near-twin of `RemoteCache`'s own
  CRUD surface (key, value(s), metadata), so its typed wrapper is a
  close copy of `TypedCache` itself, not a new pattern: the only
  structural difference is `get`/`get_with_metadata` returning a
  collection of values instead of one.

## Decision

Two additive wrappers, reusing `Marshaller`/`Error::Marshalling`
exactly as `TypedCache` already does, with no change to either
existing ADR's wire format: `TypedCache::query` (new method, types
living alongside `TypedCache` itself, since a query has no separate
"how do I get one" entry point of its own) and `TypedMultimapCache`
(new sibling module to `multimap.rs`, mirroring the
`remote_cache.rs`/`typed_cache.rs` split).

## Design

### Shared: `wrap_marshalling_error` moves to `error.rs`

`typed_cache.rs`'s private `fn wrap` (wraps any marshaller error into
`Error::Marshalling`, tagged `MarshallingSide::Key`/`Value`) moved to
`error.rs` as `pub(crate) fn wrap_marshalling_error`, used by
`typed_cache.rs` unchanged and by the two new call sites below. A
move, not a duplicate: `error.rs` already owns `Error::Marshalling`/
`MarshallingSide`, so this is where the one function that constructs
it belongs now that more than one module needs it.
`Error::Marshalling`'s own doc comment (which previously named
`TypedCache::wrap` as its one and only caller) is updated to name
`wrap_marshalling_error` and all three callers instead; the "never
constructed anywhere else" rule itself is unchanged.

### `TypedCache::query` (in `typed_cache.rs`)

```rust
pub enum TypedQueryRow<V> { Entity(V), Columns(Vec<QueryValue>) }

pub struct TypedQueryResult<V> {
    pub rows: Vec<TypedQueryRow<V>>,
    pub hit_count: u32,
    pub hit_count_exact: bool,
}

pub struct TypedQuery<'a, MV: Marshaller> {
    query: Query<'a>,
    value_marshaller: Arc<MV>,
}

impl<'a, MV: Marshaller> TypedQuery<'a, MV> {
    pub fn param(self, name: impl Into<String>, value: QueryValue) -> Self; // delegates to Query::param unchanged
    pub fn start_offset(self, start_offset: i64) -> Self;
    pub fn max_results(self, max_results: i32) -> Self;
    pub async fn execute(self) -> Result<TypedQueryResult<MV::Value>>;
    pub async fn execute_statement(self) -> Result<u32>; // no value involved, passes through unchanged
}

impl<MK: Marshaller, MV: Marshaller> TypedCache<MK, MV> {
    pub fn query(&self, query: impl Into<String>) -> TypedQuery<'_, MV>;
}
```

`TypedQuery` is generic over `MV` only, not `MK`: a query has no key
to marshal, so `TypedCache::query`'s own `MK` parameter (bound by the
surrounding `impl` block) plays no part in the returned type.
`execute` unmarshals only `QueryRow::Entity`'s bytes through the
value marshaller (wrapped as `MarshallingSide::Value` on failure);
`QueryRow::Columns` passes its `Vec<QueryValue>` through verbatim
into `TypedQueryRow::Columns`, mirroring the Java client's own split
(a `RemoteQuery<T>` marshals whole entities via the configured
`SerializationContext`, never touches a projection's own scalar
values). `execute_statement` has no value to unmarshal at all, so it
passes `Query::execute_statement`'s `u32` straight through.

### `TypedMultimapCache` (new `typed_multimap.rs`)

```rust
pub struct TypedMultimapEntry<V> {
    pub values: Vec<V>,
    pub version: u64,
    pub created: Option<SystemTime>,
    pub lifespan: Expiration,
    pub last_used: Option<SystemTime>,
    pub max_idle: Expiration,
}

pub struct TypedMultimapCache<MK: Marshaller, MV: Marshaller> { /* cache, key_marshaller, value_marshaller */ }
// Clone (hand-written, same reason TypedCache's is), Deref<Target = MultimapCache>.

impl<MK: Marshaller, MV: Marshaller> TypedMultimapCache<MK, MV> {
    pub async fn get(&self, key: &MK::Value) -> Result<Vec<MV::Value>>;
    pub async fn get_with_metadata(&self, key: &MK::Value) -> Result<Option<TypedMultimapEntry<MV::Value>>>;
    pub async fn put(&self, key: &MK::Value, value: &MV::Value, lifespan: Expiration, max_idle: Expiration) -> Result<()>;
    pub async fn remove_key(&self, key: &MK::Value) -> Result<bool>;
    pub async fn remove_entry(&self, key: &MK::Value, value: &MV::Value) -> Result<bool>;
    pub async fn contains_entry(&self, key: &MK::Value, value: &MV::Value) -> Result<bool>;
    pub async fn contains_key(&self, key: &MK::Value) -> Result<bool>;
    pub async fn contains_value(&self, value: &MV::Value) -> Result<bool>;
}
```

Each marshals its arguments, delegates to the matching
`MultimapCache` method, unmarshals the result: line-for-line
`TypedCache`'s own shape, with `Vec<MV::Value>` in place of a single
value where `MultimapCache::get` already returns a collection.
`size`/`supports_duplicates` have nothing key- or value-shaped to
marshal, so they are reached through `Deref`, unmodified, the same
way `TypedCache` leaves untyped `RemoteCache` methods untouched.

`multimap.rs` gains one new method, mirroring `RemoteCache::typed`'s
own one-liner:

```rust
impl MultimapCache {
    pub fn typed<MK: Marshaller, MV: Marshaller>(&self, key_marshaller: MK, value_marshaller: MV) -> TypedMultimapCache<MK, MV>;
}
```

### Out of scope

Everything either ADR already left out (streaming/iteration/
listeners/near-caching for `TypedCache`; hash-routing for
`MultimapCache`) is unaffected; this phase adds nothing new to either
list. `register_proto_schema`/schema registration stays untouched:
this is about decoding a caller's own entity bytes, not about
protostream negotiation.

## Consequences

* New public API: `TypedCache::query`, `TypedQuery`,
  `TypedQueryResult`, `TypedQueryRow`; `MultimapCache::typed`,
  `TypedMultimapCache`, `TypedMultimapEntry`. Additive: nothing
  existing changes shape.
* Confirmed against a fake server: `TypedQuery::execute` unmarshals a
  whole-entity row and passes a projected-column row through
  unchanged, in two separate round trips (a single `QueryResponse`
  is either all-entity or all-columns, never mixed, confirmed against
  `query.rs`'s own `projection_size` field); `TypedQuery::execute_statement`
  passes its affected-row count through unchanged; every
  `TypedMultimapCache` method round-trips through a fake server
  against the real multimap opcodes, including the
  hardcoded-immortal-expiration quirk ADR 0016 already documented for
  `remove_entry`/`contains_entry`/`contains_value`.
* A failing value marshaller on a whole-entity query row, or on any
  `TypedMultimapCache` argument, surfaces as `Error::Marshalling`,
  the same guarantee `TypedCache` itself already has.
