# ADR 0024: Streaming remote query results

## Status

Accepted, implemented in #109.

## Context

`Query::execute` (ADR 0013) always buffers the whole `QueryResponse`
in one `QueryResult`. Issue #109 asks for incremental iteration
instead, and names the open question directly: does the query opcode
have a paging/cursor mechanism analogous to server-side iteration
(ADR 0009), or is this purely client-side `startOffset`/`maxResults`
batching?

Checked directly against the Java client's own source
(`client/hotrod-client/.../impl/query/RemoteQuery.java`, fetched in
full, not guessed): there is no cursor. `RemoteQuery.iterator()` is

```java
public CloseableIterator<T> iterator() {
   if (maxResults == -1 && startOffset == 0) {
      log.warnPerfRemoteIterationWithoutPagination(queryString);
   }
   return Closeables.iterator(list().iterator());
}
```

`list()` still calls the ordinary, fully-buffering `execute()`; the
method only logs a performance warning when a caller iterates without
having set `startOffset`/`maxResults` themselves. The protocol offers
nothing beyond what ADR 0013 already implements for a plain
`execute()`: no new opcode, no server-side state to manage, no
`INVALID_ITERATION`-style status to detect exhaustion with. Pagination
is something every Java caller is expected to do by hand, request by
request; this client's own "streaming" is the same thing, done once,
inside the crate, instead of by every caller.

## Decision

**`Query::stream(self, page_size: u32) -> QueryStream<'a>`**, consuming
the existing builder. `QueryStream::next_row` repeats the exact same
`encode_query_request`/`run_query` round trip `execute` already uses,
with an internally advancing `start_offset`, buffering only the
current page (not the whole result set) between calls. A page shorter
than requested is the only exhaustion signal available, the same
ambiguity any offset/limit pagination scheme has: an
exactly-page-sized final page still needs one more, empty round trip
before `next_row` reports `None`. `0` for `page_size` falls back to a
built-in default (100); this is not a wire-level default to mirror
(the query protocol has no page/cursor concept of its own), unlike
`IterationOptions::batch_size_or_default`'s `0`, which does have a
real server-side default behind it (ADR 0009).

**`max_results`, already set on the builder before `.stream()`, keeps
meaning "cap the total row count across the whole stream"**, not page
size. `QueryStream` tracks a remaining budget (`Option<i64>`,
`None` when `max_results` was never set) and shrinks the final page's
own requested size to fit it, stopping once it reaches zero without
another round trip. `start_offset`, if set, is still where the first
page starts. The alternative, repurposing `max_results` itself to mean
"page size" under `.stream()`, was rejected: it would be a different
meaning for the same field depending on which terminal method is
called, surprising for anyone who already read `execute`'s own doc
comment for `max_results`.

**`TypedQuery::stream` ships in the same phase.** `TypedQuery` already
wraps `Query` directly (ADR 0023, #111); the typed/raw split for a
stream is the same one-line match `TypedQuery::execute` already does
per row (`QueryRow::Entity` through the value marshaller,
`QueryRow::Columns` unchanged), so there is no reason to leave it for
a follow-up issue the way #111 itself had to for `execute`.

**No new `Error` variant, no new opcode, no change to
`connection.rs`/`header.rs`.** Every error `execute()` could already
return (`Error::Server`, `Error::Io`, the retry/transport errors
`call_seed` already surfaces) comes back the same way from
`next_row()`, since it is the exact same operation, called more than
once.

**Not solved, a known limitation**: the same consistency caveat any
OFFSET/LIMIT pagination scheme has over mutable data. A row inserted
or removed between two pages can be skipped or duplicated relative to
a single, frozen snapshot; this client takes no lock and keeps no
server-side cursor to prevent it, matching ADR 0009's own acceptance
of a fixed-at-start topology with no mid-iteration consistency
guarantee. `hit_count`/`hit_count_exact`, exposed on `QueryStream` for
convenience, reflect whichever page most recently answered, not one
value frozen for the whole stream.

## Design

### `query.rs`

```rust
const DEFAULT_QUERY_STREAM_PAGE_SIZE: u32 = 100;

pub struct QueryStream<'a> {
    cache: &'a RemoteCache,
    query: String,
    named_parameters: Vec<(String, QueryValue)>,
    next_offset: i64,
    page_size: i32,
    remaining_cap: Option<i64>, // from Query::max_results, if it was set
    buffer: VecDeque<QueryRow>,
    exhausted: bool,
    hit_count: u32,
    hit_count_exact: bool,
}

impl<'a> Query<'a> {
    pub fn stream(self, page_size: u32) -> QueryStream<'a>;
}

impl<'a> QueryStream<'a> {
    pub async fn next_row(&mut self) -> Result<Option<QueryRow>>;
    pub fn hit_count(&self) -> u32;
    pub fn hit_count_exact(&self) -> bool;
}
```

`page_size: u32` is clamped to `i32::MAX` before use, so an
unreasonably large value cannot cast-wrap into a negative
`maxResults` on the wire.

### `typed_cache.rs`

```rust
pub struct TypedQueryStream<'a, MV: Marshaller> {
    stream: QueryStream<'a>,
    value_marshaller: Arc<MV>,
}

impl<'a, MV: Marshaller> TypedQuery<'a, MV> {
    pub fn stream(self, page_size: u32) -> TypedQueryStream<'a, MV>;
}

impl<'a, MV: Marshaller> TypedQueryStream<'a, MV> {
    pub async fn next_row(&mut self) -> Result<Option<TypedQueryRow<MV::Value>>>;
    pub fn hit_count(&self) -> u32;
    pub fn hit_count_exact(&self) -> bool;
}
```

### Naming note

`QueryStream`/`TypedQueryStream` are unrelated to this crate's
existing `GetStreamStart`/`PutStreamStart` feature (ADR 0008,
"Streaming for large values"), which streams one large value's own
bytes. The word collision is real but accepted: "stream" is the
issue's own word for what this does, and the two features live in
different modules (`streaming.rs` versus `query.rs`/`typed_cache.rs`)
with doc comments on `QueryStream` itself pointing out the distinction
for anyone who finds both while reading generated docs.

## Consequences

* New public API: `Query::stream`, `QueryStream`,
  `TypedQuery::stream`, `TypedQueryStream`. Additive: nothing existing
  changes shape, `execute`/`execute_statement` are unchanged.
* `max_results` carries a second, context-dependent meaning (a total
  cap under `execute`/`execute_statement`, still a total cap but now
  shared across many requests under `stream`), documented on
  `Query::stream`'s own doc comment rather than introduced as a
  separate field, to avoid growing the builder's surface for a
  distinction that does not change what the field represents.
* Not implemented, left for later if it turns out to matter: a single
  consistent snapshot across pages (not available without a
  server-side cursor this protocol does not have), and surfacing
  `QueryStream` as a `futures::Stream` (this crate has no such
  dependency; `next_row` mirrors `CacheIterator::next_entry`'s own
  shape instead, ADR 0009).
