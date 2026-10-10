# ADR 0013: Remote query (Ickle)

## Status

Accepted, implemented in #50.

## Context

The last item in the "Typed data" roadmap theme, issue #50 (already
filed): "The Java client supports server-side querying via
`QueryOperation`/`Search`, which depends on protobuf marshalling and
schema registration... Needs its own ADR: this is the biggest single
feature gap and likely requires a marshalling story (protobuf
dependency) before query itself can be scoped." The serialization
abstraction (#99, ADR 0012) resolved the "marshalling story" with no
new dependency; this phase resolves the part that issue already
expected to be a real dependency: the query protocol's own envelope.

The design is pinned against the Java client's own source
(`infinispan/infinispan` and `infinispan/protostream`), the same
discipline every prior phase used, plus live-server experimentation
that corrected a real gap the initial research missed (see
"Corrected mid-flight" below).

**Opcodes**: `QUERY_REQUEST = 0x1F`, `QUERY_RESPONSE = 0x20`
(`HotRodConstants.java`, client and server side; `server/core`'s
`DefaultQuerySerializer` for how the server itself decodes a
request). Normal "response = request + 1" convention.

**`QueryRequest`** (the request body, after the usual Hot Rod header)
is itself serialized as **real binary Protobuf**, a small, fixed
schema Infinispan defines (confirmed against the generated
`proto.lock` in `infinispan/infinispan`, not guessed): `queryString`
(tag 1, string), `startOffset` (tag 3, int64), `maxResults` (tag 4,
int32), `namedParameters` (tag 5, list of `NamedParameter`: `name`
tag 1 string, `value` tag 2 `WrappedMessage`), `local` (tag 6, bool),
`hitCountAccuracy` (tag 7, int32).

**`QueryResponse`**: `numResults` (tag 1, int32), `projectionSize`
(tag 2, int32), `results` (tag 3, list of `WrappedMessage`),
`hitCount` (tag 4, int32), `hitCountExact` (tag 5, bool). When
`projectionSize == 0`, each `WrappedMessage` in `results` is a whole
entity; when `> 0` (`SELECT a, b`), each row is `projectionSize`
consecutive `WrappedMessage`s in that same flat list (confirmed
against the Java client's own `QueryResponse.extractResults`, not
guessed).

**`WrappedMessage`** (`infinispan/protostream`,
`message-wrapping.proto`): a hand-written "oneof", one field per
possible scalar type (`wrappedDouble` 1 fixed64, `wrappedFloat` 2
fixed32, `wrappedInt64` 3 varint, `wrappedUInt64` 4 varint,
`wrappedInt32` 5 varint, `wrappedBool` 8 varint, `wrappedString` 9
length-delimited, `wrappedBytes` 10 length-delimited, `wrappedUInt32`
11 varint, and others this phase leaves out: fixed/sfixed/sint
variants, char/short/byte/date/instant, enum, containers). For a
domain entity (not a scalar): `wrappedTypeName` (tag 16, string) or
`wrappedTypeId` (tag 19, uint32) identifies the type, followed by
`wrappedMessage` (tag 17, bytes) carrying the entity's own Protobuf
bytes, opaque to this crate. `null` is just `wrappedEmpty` (tag 26,
bool, value ignored). Varint confirmed as standard Protocol Buffers
LEB128; `sint32`/`sint64` use standard zigzag
(`(v << 1) ^ (v >> 31)` for 32 bits) — **the same formula
`varint.rs::write_signed_vint` already uses for Hot Rod's own
"SignedVInt"**, and the same varint encoding `read_vint`/`write_vint`/
`read_vlong`/`write_vlong` already implement. Length-delimited framing
(a vint length then raw bytes) is the same shape
`wire.rs::read_array`/`write_array` already use.

**Schema registration needs no new opcode.**
`___protobuf_metadata` (`InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME`)
is an ordinary cache; the Java client's newer administrative API
(`administration().schemas().createOrUpdate`) uses the generic named-task
mechanism only for server-side validation and better error messages,
not because the protocol requires it.

**Indexing is entirely server configuration.** No `QueryRequest`
field distinguishes indexed from non-indexed. A real constraint, but
also server-side: a remote cache must use `application/x-protostream`
to accept query at all, indexed or not.

### Corrected mid-flight: media type is not optional for this operation

The initial research concluded media-type negotiation could stay out
of scope, since Hot Rod normally lets a client declare "none" (opaque
bytes) for every operation. Live-server testing (`ci/infinispan`,
Infinispan 15.1) disproved this for two specific writes, both fixed
before merge:

* **The query envelope itself.** Declaring "none" (as every other
  operation does) made the server misparse `QueryRequest` at its
  very first, otherwise byte-correct, field
  (`IllegalStateException: Unexpected tag`): `server/core`'s
  `DefaultQuerySerializer.decodeQueryRequest` picks how to interpret
  the request body from the media type the request declares, and
  "none" picks the wrong transcoder.
* **`register_proto_schema`'s write to `___protobuf_metadata`.**
  Declaring "none" made the server reject even a key that already
  was a string (`CacheException: The key must be a String: class
  java.lang.Integer`): this cache's own storage is fixed to
  `application/x-protostream` regardless of what a request declares,
  and an unwrapped scalar carries no type of its own, so the decoder
  guessed the shortest-looking type (binary varint) instead of a
  string.

Both are fixed by declaring the predefined `application/x-protostream`
media type (id 12, confirmed against the Java client's own
`MediaTypeIds.java`) for both key and value, and, for schema
registration specifically, `WrappedMessage`-wrapping both the key
(schema name) and value (schema source) as scalars first (confirmed
empirically: a correctly-declared-protostream but still-unwrapped
string value was still rejected). This is a narrow, two-call-site
fix (`header::RequestMediaType::Protostream`, used only by the query
operation and by writes to `___protobuf_metadata`), not general
media-type negotiation: every other operation still declares "none",
unchanged.

What remains genuinely out of scope, confirmed by the same testing:
making an arbitrary `RemoteCache`/`TypedCache` write queryable (i.e.
one of a user's own domain entities, stored under a key the user
chooses) needs its *value* declared `application/x-protostream`
too, which this phase does not add a way to do for ordinary writes.
A caller who wants their own entries queryable today needs their
cache itself configured with protostream encoding server-side (the
same requirement the Java client's own documentation already states),
not something negotiated per request by this crate.

## Decision

**Protobuf envelope implemented by hand, no new dependency.** A new
module (`protobuf_wire.rs`) with generic primitives (tag, varint and
zigzag reusing `varint.rs`'s own encoding, fixed32/fixed64,
length-delimited matching `wire.rs`'s shape), with
`QueryRequest`/`QueryResponse`/`WrappedMessage` encoded on top of
that in `query.rs`. Keeps the discipline this crate already
demonstrated (SASL, SCRAM, topology, all hand-written) rather than
bringing in `prost`/`protobuf` (which typically needs `protoc`
available at build time) for four small, fixed messages that never
change.

**v1 scope**: the query opcode; schema registration (a convenience
over plain `put`/`get`, no new opcode, now confirmed to need the
`WrappedMessage` + protostream media-type fix above); `RemoteCache::query`
with named parameters, offset/limit, execution and results (whole
entities and projected columns). Out of this phase: `executeStatement`
(DELETE/UPDATE Ickle), streaming/iterating large result sets, the
protocol's JSON variant, `TypedCache::query` (results stay raw bytes
for the caller's own `Marshaller` to decode), and any general way to
mark an ordinary write's value as protostream-encoded (see above).

## Design

### `protobuf_wire.rs` (new): generic primitives

Free functions, not tied to any specific message, operating on an
already-buffered `&[u8]` with an explicit cursor (not `AsyncRead`):
unlike every other wire format in this crate, a query request or
response body is always read whole first, so parsing it is plain,
synchronous, in-memory work.

```rust
pub(crate) fn write_tag(buf: &mut Vec<u8>, field: u32, wire_type: u8);
pub(crate) fn read_tag(buf: &[u8], pos: &mut usize) -> Result<Option<(u32, u8)>>; // None at a clean end
pub(crate) fn write_varint(buf: &mut Vec<u8>, value: u64);
pub(crate) fn read_varint(buf: &[u8], pos: &mut usize) -> Result<u64>;
pub(crate) fn write_int32/read_int32, write_int64/read_int64; // sign-extended plain varint, per the protobuf spec's own quirk
pub(crate) fn zigzag_encode(value: i64) -> u64;
pub(crate) fn zigzag_decode(value: u64) -> i64; // not called yet in v1; kept for sint32/sint64 if QueryValue grows them
pub(crate) fn write_length_delimited(buf: &mut Vec<u8>, bytes: &[u8]);
pub(crate) fn read_length_delimited(buf: &[u8], pos: &mut usize) -> Result<Vec<u8>>;
pub(crate) fn write_fixed32/read_fixed32, write_fixed64/read_fixed64;
pub(crate) fn skip_field(buf: &[u8], pos: &mut usize, wire_type: u8) -> Result<()>; // tolerates an unknown field
```

### `query.rs` (new): the specific messages and the public API

`QueryValue` (public, what a named parameter or a projected column
can be): `String`, `Int64`, `Int32`, `UInt64`, `UInt32`, `Double`,
`Float`, `Bool`, `Bytes`, `Null`. Covers `WrappedMessage`'s common
scalar fields; the rarer ones are left out of this phase.

`QueryRow` (public): `Entity(Vec<u8>)` (the caller decides the schema
via their own `Marshaller`) or `Columns(Vec<QueryValue>)` (when
`projection_size > 0`).

```rust
pub struct Query<'a> { /* cache, query string, offset, max_results, named parameters */ }

impl<'a> Query<'a> {
    pub fn param(self, name: impl Into<String>, value: QueryValue) -> Self;
    pub fn start_offset(self, start_offset: i64) -> Self;
    pub fn max_results(self, max_results: i32) -> Self;
    pub async fn execute(self) -> Result<QueryResult>;
}

pub struct QueryResult {
    pub rows: Vec<QueryRow>,
    pub hit_count: u32,
    pub hit_count_exact: bool,
}

impl RemoteCache {
    pub fn query(&self, query: impl Into<String>) -> Query<'_>;
}
```

A consuming builder (`self` by value on each method, like
`IterationOptions` already does), echoing the Java client's
`Query<T>`/`.execute()` closely enough to be recognizable, without
replicating its full `Query<T>`/`QueryFactory` surface.

### Schema registration

```rust
impl HotRodClient {
    pub async fn register_proto_schema(&self, name: &str, content: &str) -> Result<()>;
}
```

`self.cache("___protobuf_metadata").put(wrap_string(name), wrap_string(content), ...)`,
reusing `RemoteCache::put` and `query::wrap_string` (a small
`pub(crate)` helper sharing `WrappedMessage`'s own string-scalar
encoding). `PROTOBUF_METADATA_CACHE_NAME` is a `pub(crate)` constant.

### Protocol wiring

`header.rs`: `OpCode::Query = 0x1F`; a new `RequestMediaType` enum
(`None`, the existing default, or `Protostream`) threaded through
`write_request_header`/`write_and_read_header`.
`HotRodConnection::write_and_read_header` (its private wrapper)
decides `Protostream` for the query opcode or a write to
`___protobuf_metadata`, `None` for everything else, so every other
call site is unchanged. `connection.rs` gains
`HotRodConnection::query(request_bytes: &[u8]) -> Result<QueryResult>`,
wrapping/unwrapping the Protobuf bytes in one Hot Rod array the same
way the Java client does (`ByteBufUtil.writeArray`, never written
field by field into the Hot Rod buffer directly). `remote_cache.rs`'s
`Query::execute` calls `self.cache.call_seed(Operation::Query(bytes))`
(no key to route by; the same decision already made for
`get_all`/`put_all`/`size`/`clear`, since a query can scan the whole
cache).

### Errors

`Error::MalformedQueryResponse(String)` (same shape as
`MalformedEvent`/`MalformedIterationResponse`), for a `WrappedMessage`
with an inconsistent field combination (e.g. both `type_name` and
`type_id`, or neither alongside a `wrapped_message`) or an unknown
wire type on a tag.

## Consequences

* New public API: `RemoteCache::query`, `Query`, `QueryResult`,
  `QueryRow`, `QueryValue`; `HotRodClient::register_proto_schema`;
  `Error::MalformedQueryResponse`. Additive: nothing existing changes
  shape.
* Confirmed against a live server (`ci/infinispan`): schema
  registration followed by a query against an unpopulated but
  correctly-typed cache round-trips with zero rows and no error,
  exercising the real Protobuf envelope end to end, not just this
  crate's own fake server.
* **Making a caller's own entries queryable is not solved by this
  phase.** Only the query operation's envelope and schema
  registration got the protostream media-type fix; an ordinary
  `RemoteCache`/`TypedCache` write still always declares "none".
  A caller who wants their own writes to be queryable needs their
  cache configured with protostream encoding server-side today (the
  same prerequisite the Java client's own documentation states); a
  per-write "store this as protostream" capability in this crate is
  left for a future proposal if that turns out to matter in practice.
* Not implemented, left for later if it turns out to matter:
  `executeStatement` (DELETE/UPDATE Ickle), streaming/iterating very
  large result sets instead of buffering the whole response, the
  protocol's JSON media-type variant, and `TypedCache::query`.
