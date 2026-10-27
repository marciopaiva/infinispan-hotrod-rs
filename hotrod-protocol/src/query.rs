//! Remote query (Ickle), `docs/adr/0013-remote-query.md`.
//!
//! `QueryRequest`/`QueryResponse` (the request and response bodies for
//! `OpCode::Query`) are themselves Protobuf messages, a small, fixed
//! schema Infinispan itself defines (confirmed against the real
//! generated `proto.lock` schema in `infinispan/infinispan` and the
//! `message-wrapping.proto` source in `infinispan/protostream`, not
//! guessed). `WrappedMessage` is how each result (and each named
//! parameter's value) is self-describing on the wire: either a
//! scalar, or a type name/id plus opaque bytes for a message type.
//! Those opaque bytes are exactly the Protobuf encoding of the
//! caller's own domain entity, which this crate never decodes itself:
//! that stays the caller's `Marshaller`'s job, the same as every
//! other value `RemoteCache` returns.
//!
//! This module owns the Protobuf encoding of that one small envelope,
//! built on `protobuf_wire.rs`'s generic primitives; it has no
//! awareness of Ickle syntax itself (that is parsed and executed
//! entirely server-side) or of any user-registered `.proto` schema.

use crate::error::{Error, Result};
use crate::protobuf_wire::{
    read_fixed32, read_fixed64, read_int32, read_int64, read_length_delimited, read_tag,
    read_varint, skip_field, write_fixed32, write_fixed64, write_int32, write_int64,
    write_length_delimited, write_tag, write_varint, WIRE_TYPE_FIXED32, WIRE_TYPE_FIXED64,
    WIRE_TYPE_LENGTH_DELIMITED, WIRE_TYPE_VARINT,
};
use crate::remote_cache::RemoteCache;

/// A scalar value a query's named parameter can take, or a projected
/// column can come back as. Covers `WrappedMessage`'s common scalar
/// fields (`docs/adr/0013-remote-query.md`); the rarer ones
/// (char/short/byte/date/instant/enum/fixed-width/zigzag integers,
/// containers) are left out of this phase; an entity-typed column,
/// which this crate cannot interpret itself, surfaces as `Bytes`.
///
/// `#[non_exhaustive]` (`docs/adr/0027-api-stability-policy.md`):
/// the doc comment above already names the scalar kinds a later
/// phase is expected to add, so adding one of them stays a
/// non-breaking change instead of forcing a minor version bump.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum QueryValue {
    String(String),
    Int64(i64),
    Int32(i32),
    UInt64(u64),
    UInt32(u32),
    Double(f64),
    Float(f32),
    Bool(bool),
    Bytes(Vec<u8>),
    Null,
}

/// One row of a query's results: a whole matching entity (its raw
/// Protobuf-encoded bytes, for the caller's own `Marshaller` to
/// decode) when the query has no projection, or one value per
/// projected column (`SELECT a, b, ...`) when it does.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryRow {
    Entity(Vec<u8>),
    Columns(Vec<QueryValue>),
}

/// The decoded shape of one `WrappedMessage`
/// (`docs/adr/0013-remote-query.md`): either a scalar, or a message
/// type (identified by a name or a numeric id, never both) wrapping
/// opaque bytes.
enum WrappedValue {
    Scalar(QueryValue),
    /// `type_id`/`type_name` are not kept here: nothing downstream
    /// consumes them (`QueryRow::Entity` carries only the bytes, see
    /// ADR 0013), so `decode_wrapped_value` only uses them locally,
    /// to validate that exactly one was present before discarding
    /// both.
    Entity(Vec<u8>),
}

const WRAPPED_DOUBLE: u32 = 1;
const WRAPPED_FLOAT: u32 = 2;
const WRAPPED_INT64: u32 = 3;
const WRAPPED_UINT64: u32 = 4;
const WRAPPED_INT32: u32 = 5;
const WRAPPED_BOOL: u32 = 8;
const WRAPPED_STRING: u32 = 9;
const WRAPPED_BYTES: u32 = 10;
const WRAPPED_UINT32: u32 = 11;
const WRAPPED_MESSAGE: u32 = 17;
const WRAPPED_TYPE_NAME: u32 = 16;
const WRAPPED_TYPE_ID: u32 = 19;
const WRAPPED_EMPTY: u32 = 26;

/// `WrappedMessage`-wraps a plain UTF-8 string: `___protobuf_metadata`
/// needs both its key and value written this way, confirmed
/// empirically against a live server
/// (`docs/adr/0013-remote-query.md`), so `HotRodClient::
/// register_proto_schema` reuses this instead of duplicating
/// `write_wrapped_scalar`'s string case.
pub(crate) fn wrap_string(value: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    write_wrapped_scalar(&mut buf, &QueryValue::String(value.to_string()));
    buf
}

/// Writes `value` as a `WrappedMessage`'s bytes: used for a named
/// parameter's value, which is always a scalar in this phase (a query
/// parameter is never itself a whole entity).
fn write_wrapped_scalar(buf: &mut Vec<u8>, value: &QueryValue) {
    match value {
        QueryValue::Double(v) => {
            write_tag(buf, WRAPPED_DOUBLE, WIRE_TYPE_FIXED64);
            write_fixed64(buf, v.to_bits());
        }
        QueryValue::Float(v) => {
            write_tag(buf, WRAPPED_FLOAT, WIRE_TYPE_FIXED32);
            write_fixed32(buf, v.to_bits());
        }
        QueryValue::Int64(v) => {
            write_tag(buf, WRAPPED_INT64, WIRE_TYPE_VARINT);
            write_int64(buf, *v);
        }
        QueryValue::UInt64(v) => {
            write_tag(buf, WRAPPED_UINT64, WIRE_TYPE_VARINT);
            write_varint(buf, *v);
        }
        QueryValue::Int32(v) => {
            write_tag(buf, WRAPPED_INT32, WIRE_TYPE_VARINT);
            write_int32(buf, *v);
        }
        QueryValue::UInt32(v) => {
            write_tag(buf, WRAPPED_UINT32, WIRE_TYPE_VARINT);
            write_varint(buf, u64::from(*v));
        }
        QueryValue::Bool(v) => {
            write_tag(buf, WRAPPED_BOOL, WIRE_TYPE_VARINT);
            write_varint(buf, u64::from(*v));
        }
        QueryValue::String(v) => {
            write_tag(buf, WRAPPED_STRING, WIRE_TYPE_LENGTH_DELIMITED);
            write_length_delimited(buf, v.as_bytes());
        }
        QueryValue::Bytes(v) => {
            write_tag(buf, WRAPPED_BYTES, WIRE_TYPE_LENGTH_DELIMITED);
            write_length_delimited(buf, v);
        }
        QueryValue::Null => {
            write_tag(buf, WRAPPED_EMPTY, WIRE_TYPE_VARINT);
            write_varint(buf, 1);
        }
    }
}

/// Decodes one `WrappedMessage` from `bytes`, already isolated by the
/// caller (a length-delimited field's own content, never a larger
/// buffer with more fields after it): tolerant of field order and of
/// fields this phase does not recognize (`skip_field`), matching the
/// forward-compatible stance the format's own documentation asks for.
fn decode_wrapped_value(bytes: &[u8]) -> Result<WrappedValue> {
    /// A known field's wire type didn't match what that field is
    /// defined to carry: reading it with the field's own reader would
    /// consume the wrong byte count and desync every field after it,
    /// so this is reported as malformed instead.
    fn check_wire_type(field: u32, actual: u8, expected: u8) -> Result<()> {
        if actual != expected {
            return Err(Error::MalformedQueryResponse(format!(
                "WrappedMessage field {field} has wire type {actual}, expected {expected}"
            )));
        }
        Ok(())
    }

    let mut pos = 0;
    let mut scalar: Option<QueryValue> = None;
    let mut type_id: Option<u32> = None;
    let mut type_name: Option<String> = None;
    let mut message_bytes: Option<Vec<u8>> = None;
    let mut is_empty = false;

    while let Some((field, wire_type)) = read_tag(bytes, &mut pos)? {
        match field {
            WRAPPED_DOUBLE => {
                check_wire_type(field, wire_type, WIRE_TYPE_FIXED64)?;
                scalar = Some(QueryValue::Double(f64::from_bits(read_fixed64(
                    bytes, &mut pos,
                )?)));
            }
            WRAPPED_FLOAT => {
                check_wire_type(field, wire_type, WIRE_TYPE_FIXED32)?;
                scalar = Some(QueryValue::Float(f32::from_bits(read_fixed32(
                    bytes, &mut pos,
                )?)));
            }
            WRAPPED_INT64 => {
                check_wire_type(field, wire_type, WIRE_TYPE_VARINT)?;
                scalar = Some(QueryValue::Int64(read_int64(bytes, &mut pos)?));
            }
            WRAPPED_UINT64 => {
                check_wire_type(field, wire_type, WIRE_TYPE_VARINT)?;
                scalar = Some(QueryValue::UInt64(read_varint(bytes, &mut pos)?));
            }
            WRAPPED_INT32 => {
                check_wire_type(field, wire_type, WIRE_TYPE_VARINT)?;
                scalar = Some(QueryValue::Int32(read_int32(bytes, &mut pos)?));
            }
            WRAPPED_BOOL => {
                check_wire_type(field, wire_type, WIRE_TYPE_VARINT)?;
                scalar = Some(QueryValue::Bool(read_varint(bytes, &mut pos)? != 0));
            }
            WRAPPED_STRING => {
                check_wire_type(field, wire_type, WIRE_TYPE_LENGTH_DELIMITED)?;
                scalar = Some(QueryValue::String(String::from_utf8(
                    read_length_delimited(bytes, &mut pos)?,
                )?));
            }
            WRAPPED_BYTES => {
                check_wire_type(field, wire_type, WIRE_TYPE_LENGTH_DELIMITED)?;
                scalar = Some(QueryValue::Bytes(read_length_delimited(bytes, &mut pos)?));
            }
            WRAPPED_UINT32 => {
                check_wire_type(field, wire_type, WIRE_TYPE_VARINT)?;
                scalar = Some(QueryValue::UInt32(read_varint(bytes, &mut pos)? as u32));
            }
            WRAPPED_TYPE_NAME => {
                check_wire_type(field, wire_type, WIRE_TYPE_LENGTH_DELIMITED)?;
                type_name = Some(String::from_utf8(read_length_delimited(bytes, &mut pos)?)?);
            }
            WRAPPED_TYPE_ID => {
                check_wire_type(field, wire_type, WIRE_TYPE_VARINT)?;
                type_id = Some(read_varint(bytes, &mut pos)? as u32);
            }
            WRAPPED_MESSAGE => {
                check_wire_type(field, wire_type, WIRE_TYPE_LENGTH_DELIMITED)?;
                message_bytes = Some(read_length_delimited(bytes, &mut pos)?);
            }
            WRAPPED_EMPTY => {
                check_wire_type(field, wire_type, WIRE_TYPE_VARINT)?;
                read_varint(bytes, &mut pos)?;
                is_empty = true;
            }
            _ => skip_field(bytes, &mut pos, wire_type)?,
        }
    }

    if is_empty {
        return Ok(WrappedValue::Scalar(QueryValue::Null));
    }
    if let Some(bytes) = message_bytes {
        if type_id.is_some() == type_name.is_some() {
            return Err(Error::MalformedQueryResponse(format!(
                "a WrappedMessage has a message payload but {} a type id and a type name",
                if type_id.is_some() { "both" } else { "neither" }
            )));
        }
        return Ok(WrappedValue::Entity(bytes));
    }
    scalar.map(WrappedValue::Scalar).ok_or_else(|| {
        Error::MalformedQueryResponse("a WrappedMessage has no recognized field set".to_string())
    })
}

const NAMED_PARAMETER_NAME: u32 = 1;
const NAMED_PARAMETER_VALUE: u32 = 2;

fn write_named_parameter(buf: &mut Vec<u8>, name: &str, value: &QueryValue) {
    write_tag(buf, NAMED_PARAMETER_NAME, WIRE_TYPE_LENGTH_DELIMITED);
    write_length_delimited(buf, name.as_bytes());

    let mut wrapped = Vec::new();
    write_wrapped_scalar(&mut wrapped, value);
    write_tag(buf, NAMED_PARAMETER_VALUE, WIRE_TYPE_LENGTH_DELIMITED);
    write_length_delimited(buf, &wrapped);
}

const QUERY_REQUEST_QUERY_STRING: u32 = 1;
const QUERY_REQUEST_START_OFFSET: u32 = 3;
const QUERY_REQUEST_MAX_RESULTS: u32 = 4;
const QUERY_REQUEST_NAMED_PARAMETERS: u32 = 5;
const QUERY_REQUEST_HIT_COUNT_ACCURACY: u32 = 7;

/// Encodes a `QueryRequest` (`docs/adr/0013-remote-query.md`): the
/// Protobuf body `OpCode::Query`'s request carries, after the normal
/// Hot Rod header. `local` is left unset/omitted, not exposed in this
/// phase's `Query` builder: a field protostream's own generated
/// schema declares optional, so omitting it is exactly as valid on
/// the wire as sending its default (`local = false`), not a shortcut
/// around the format. `hit_count_accuracy` is `None` for a plain
/// `execute()` (field omitted, same as before this parameter
/// existed) and `Some(1)` for `execute_statement()`, matching the
/// Java client's own `QueryOperation.writeOperationRequest`
/// (`withHitCount = false` unconditionally sets `hitCountAccuracy(1)`).
pub(crate) fn encode_query_request(
    query: &str,
    start_offset: i64,
    max_results: Option<i32>,
    hit_count_accuracy: Option<i32>,
    named_parameters: &[(String, QueryValue)],
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_tag(
        &mut buf,
        QUERY_REQUEST_QUERY_STRING,
        WIRE_TYPE_LENGTH_DELIMITED,
    );
    write_length_delimited(&mut buf, query.as_bytes());

    write_tag(&mut buf, QUERY_REQUEST_START_OFFSET, WIRE_TYPE_VARINT);
    write_int64(&mut buf, start_offset);

    if let Some(max_results) = max_results {
        write_tag(&mut buf, QUERY_REQUEST_MAX_RESULTS, WIRE_TYPE_VARINT);
        write_int32(&mut buf, max_results);
    }

    if let Some(hit_count_accuracy) = hit_count_accuracy {
        write_tag(&mut buf, QUERY_REQUEST_HIT_COUNT_ACCURACY, WIRE_TYPE_VARINT);
        write_int32(&mut buf, hit_count_accuracy);
    }

    for (name, value) in named_parameters {
        write_tag(
            &mut buf,
            QUERY_REQUEST_NAMED_PARAMETERS,
            WIRE_TYPE_LENGTH_DELIMITED,
        );
        let mut param = Vec::new();
        write_named_parameter(&mut param, name, value);
        write_length_delimited(&mut buf, &param);
    }

    buf
}

const QUERY_RESPONSE_PROJECTION_SIZE: u32 = 2;
const QUERY_RESPONSE_RESULTS: u32 = 3;
const QUERY_RESPONSE_HIT_COUNT: u32 = 4;
const QUERY_RESPONSE_HIT_COUNT_EXACT: u32 = 5;

/// Decodes a `QueryResponse`'s bytes (`OpCode::Query`'s response
/// body) straight into the public `rows`/`hit_count`/`hit_count_exact`
/// shape: `numResults` is not surfaced separately, since it is always
/// recoverable as `rows.len()`.
pub(crate) fn decode_query_response(buf: &[u8]) -> Result<QueryResult> {
    let mut pos = 0;
    let mut projection_size = 0i32;
    let mut results = Vec::new();
    let mut hit_count = 0u32;
    let mut hit_count_exact = false;

    while let Some((field, wire_type)) = read_tag(buf, &mut pos)? {
        match field {
            QUERY_RESPONSE_PROJECTION_SIZE => projection_size = read_int32(buf, &mut pos)?,
            QUERY_RESPONSE_RESULTS => {
                let entry = read_length_delimited(buf, &mut pos)?;
                results.push(decode_wrapped_value(&entry)?);
            }
            QUERY_RESPONSE_HIT_COUNT => hit_count = read_int32(buf, &mut pos)? as u32,
            QUERY_RESPONSE_HIT_COUNT_EXACT => hit_count_exact = read_varint(buf, &mut pos)? != 0,
            _ => skip_field(buf, &mut pos, wire_type)?,
        }
    }

    let rows = build_rows(projection_size, results)?;
    Ok(QueryResult {
        rows,
        hit_count,
        hit_count_exact,
    })
}

/// Groups `results` into rows: one `WrappedValue` per row when there
/// is no projection, or `projection_size` consecutive ones per row
/// when there is. `QueryResponse.results` is a single flat repeated
/// field on the wire either way; this grouping is implicit, recovered
/// only from `projection_size` (confirmed against the Java client's
/// own `QueryResponse.extractResults`, not guessed).
fn build_rows(projection_size: i32, results: Vec<WrappedValue>) -> Result<Vec<QueryRow>> {
    if projection_size <= 0 {
        return Ok(results
            .into_iter()
            .map(|value| match value {
                WrappedValue::Entity(bytes) => QueryRow::Entity(bytes),
                WrappedValue::Scalar(value) => QueryRow::Columns(vec![value]),
            })
            .collect());
    }

    let size = projection_size as usize;
    if !results.len().is_multiple_of(size) {
        return Err(Error::MalformedQueryResponse(format!(
            "{} projected results is not a multiple of the projection size {size}",
            results.len()
        )));
    }

    // Consumes `results` by value instead of `chunks()`-borrowing it,
    // so each `QueryValue` moves into its row directly rather than
    // being cloned out of a borrowed chunk.
    let mut rows = Vec::with_capacity(results.len() / size);
    let mut results = results.into_iter();
    loop {
        let row: Vec<QueryValue> = (&mut results)
            .take(size)
            .map(|value| match value {
                WrappedValue::Scalar(value) => value,
                WrappedValue::Entity(bytes) => QueryValue::Bytes(bytes),
            })
            .collect();
        if row.is_empty() {
            break;
        }
        rows.push(QueryRow::Columns(row));
    }
    Ok(rows)
}

/// The result of executing a `Query`: one `QueryRow` per matching
/// entry (or per matching row, when the query projects specific
/// columns), plus the total hit count Infinispan computed (`hit_count`),
/// which can exceed `rows.len()` when `max_results` bounded how many
/// actually came back. `hit_count_exact` is `false` when Infinispan
/// only estimated the count rather than computing it precisely (a
/// server-side trade-off for very large result sets).
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub rows: Vec<QueryRow>,
    pub hit_count: u32,
    pub hit_count_exact: bool,
}

/// A query against one cache's entries, built with `RemoteCache::query`.
/// Mirrors the Java client's `RemoteCache.query(String)` closely
/// enough to be recognizable (a builder ending in `execute()`),
/// without replicating its full `Query<T>`/`QueryFactory` surface.
pub struct Query<'a> {
    cache: &'a RemoteCache,
    query: String,
    start_offset: i64,
    max_results: Option<i32>,
    named_parameters: Vec<(String, QueryValue)>,
}

impl<'a> Query<'a> {
    pub(crate) fn new(cache: &'a RemoteCache, query: String) -> Self {
        Self {
            cache,
            query,
            start_offset: 0,
            max_results: None,
            named_parameters: Vec::new(),
        }
    }

    /// Binds `name` (as referenced in the Ickle query string, e.g.
    /// `:name`) to `value`.
    pub fn param(mut self, name: impl Into<String>, value: QueryValue) -> Self {
        self.named_parameters.push((name.into(), value));
        self
    }

    /// Skips this many matching entries before the first one returned.
    pub fn start_offset(mut self, start_offset: i64) -> Self {
        self.start_offset = start_offset;
        self
    }

    /// Caps how many entries come back. `None` (the default) leaves
    /// the cap to the server.
    pub fn max_results(mut self, max_results: i32) -> Self {
        self.max_results = Some(max_results);
        self
    }

    /// Runs the query against the server, always against the seed
    /// connection (no key to route by, the same as `get_all`/
    /// `put_all`/`size`/`clear`), consuming this builder.
    pub async fn execute(self) -> Result<QueryResult> {
        self.cache
            .run_query(encode_query_request(
                &self.query,
                self.start_offset,
                self.max_results,
                None,
                &self.named_parameters,
            ))
            .await
    }

    /// Runs a DELETE or UPDATE Ickle statement, returning the number
    /// of affected entries. The wire round trip is identical to
    /// `execute` (the same `QueryRequest`/`QueryResponse`, see
    /// `docs/adr/0013-remote-query.md`): this always sets
    /// `hitCountAccuracy` to 1, matching the Java client's own
    /// `executeStatement`, and reads the affected-row count back from
    /// `QueryResponse.hit_count` instead of `rows`, which the server
    /// leaves empty for a statement rather than a SELECT.
    pub async fn execute_statement(self) -> Result<u32> {
        let result = self
            .cache
            .run_query(encode_query_request(
                &self.query,
                self.start_offset,
                self.max_results,
                Some(1),
                &self.named_parameters,
            ))
            .await?;
        Ok(result.hit_count)
    }

    /// Streams this query's results page by page instead of buffering
    /// them all at once in `execute`'s one `QueryResult`
    /// (`docs/adr/0024-remote-query-streaming.md`): there is no
    /// server-side cursor for query results on this wire, confirmed
    /// against the Java client's own `RemoteQuery`, which does this
    /// exact repeated-request pagination by hand too. `page_size` is
    /// how many rows each underlying request asks for; `0` uses a
    /// built-in default. `max_results`, if set on this builder before
    /// calling this, still caps the stream's total row count across
    /// every page (not the page size); `start_offset`, if set, is
    /// still where the first page starts.
    pub fn stream(self, page_size: u32) -> QueryStream<'a> {
        QueryStream::new(self, page_size)
    }
}

/// `0` for `QueryStream::page_size` falls back to this. Not a
/// wire-level default: the query protocol has no page/cursor concept
/// of its own to mirror (`docs/adr/0024-remote-query-streaming.md`),
/// unlike `IterationOptions::batch_size_or_default`'s `0`, which does
/// have a real server-side default to fall back to.
const DEFAULT_QUERY_STREAM_PAGE_SIZE: u32 = 100;

/// Incremental results of a `Query::stream` call: repeats the same
/// `execute`-shaped request with an advancing `start_offset` one page
/// at a time, instead of one request that buffers every matching row
/// (`docs/adr/0024-remote-query-streaming.md`). A page shorter than
/// requested is the only exhaustion signal this protocol offers (the
/// same ambiguity any offset/limit pagination scheme has): an
/// exactly-page-sized final page still needs one more, empty round
/// trip before this reports exhaustion.
///
/// Not related to this crate's `GetStreamStart`/`PutStreamStart`
/// feature (`docs/adr/0008-streaming.md`), which streams one large
/// value's own bytes; this streams a query's rows.
pub struct QueryStream<'a> {
    cache: &'a RemoteCache,
    query: String,
    named_parameters: Vec<(String, QueryValue)>,
    next_offset: i64,
    page_size: i32,
    /// From `Query::max_results`, if the caller set it before
    /// `.stream()`: the remaining row budget across the whole stream,
    /// decremented as pages come back.
    remaining_cap: Option<i64>,
    buffer: std::collections::VecDeque<QueryRow>,
    exhausted: bool,
    hit_count: u32,
    hit_count_exact: bool,
}

impl<'a> QueryStream<'a> {
    fn new(query: Query<'a>, page_size: u32) -> Self {
        let page_size = page_size.min(i32::MAX as u32);
        let page_size = if page_size == 0 {
            DEFAULT_QUERY_STREAM_PAGE_SIZE
        } else {
            page_size
        } as i32;

        Self {
            cache: query.cache,
            query: query.query,
            named_parameters: query.named_parameters,
            next_offset: query.start_offset,
            page_size,
            remaining_cap: query.max_results.map(i64::from),
            buffer: std::collections::VecDeque::new(),
            exhausted: false,
            hit_count: 0,
            hit_count_exact: false,
        }
    }

    /// Returns the next row, fetching another page from the server
    /// once the current one is drained. `Ok(None)` once the query is
    /// exhausted (no more rows, or the `max_results` budget is spent).
    pub async fn next_row(&mut self) -> Result<Option<QueryRow>> {
        loop {
            if let Some(row) = self.buffer.pop_front() {
                return Ok(Some(row));
            }
            if self.exhausted {
                return Ok(None);
            }

            let fetch = match self.remaining_cap {
                Some(remaining) if remaining <= 0 => {
                    self.exhausted = true;
                    return Ok(None);
                }
                Some(remaining) => self.page_size.min(remaining as i32),
                None => self.page_size,
            };

            let result = self
                .cache
                .run_query(encode_query_request(
                    &self.query,
                    self.next_offset,
                    Some(fetch),
                    None,
                    &self.named_parameters,
                ))
                .await?;

            self.hit_count = result.hit_count;
            self.hit_count_exact = result.hit_count_exact;

            let returned = result.rows.len() as i64;
            self.next_offset += returned;
            if let Some(remaining) = self.remaining_cap.as_mut() {
                *remaining -= returned;
            }
            if returned < i64::from(fetch) {
                self.exhausted = true;
            }
            if returned == 0 {
                return Ok(None);
            }
            self.buffer.extend(result.rows);
        }
    }

    /// The total number of matching entries Infinispan reported, from
    /// whichever page most recently answered (`0` before the first
    /// page arrives). Not a single frozen count across the whole
    /// stream: see `docs/adr/0024-remote-query-streaming.md`.
    pub fn hit_count(&self) -> u32 {
        self.hit_count
    }

    /// Whether `hit_count` is exact rather than a server-side
    /// estimate, from the same most-recent page.
    pub fn hit_count_exact(&self) -> bool {
        self.hit_count_exact
    }
}

/// Test-only wire-building helpers shared across modules
/// (`remote_cache.rs`'s own `query`/`register_proto_schema` tests),
/// so a fake server there builds fixtures the same way this module's
/// own tests do, from the one real encoding instead of a second,
/// hand-rolled copy of the same magic numbers that could silently
/// drift from it.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) fn wrapped_scalar_bytes(value: &QueryValue) -> Vec<u8> {
        let mut buf = Vec::new();
        write_wrapped_scalar(&mut buf, value);
        buf
    }

    pub(crate) fn wrapped_entity_bytes(type_name: &str, entity_bytes: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        write_tag(&mut buf, WRAPPED_TYPE_NAME, WIRE_TYPE_LENGTH_DELIMITED);
        write_length_delimited(&mut buf, type_name.as_bytes());
        write_tag(&mut buf, WRAPPED_MESSAGE, WIRE_TYPE_LENGTH_DELIMITED);
        write_length_delimited(&mut buf, entity_bytes);
        buf
    }

    pub(crate) fn query_response_bytes(
        projection_size: i32,
        wrapped_results: &[Vec<u8>],
        hit_count: i32,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        write_tag(&mut buf, QUERY_RESPONSE_PROJECTION_SIZE, WIRE_TYPE_VARINT);
        write_int32(&mut buf, projection_size);
        for wrapped in wrapped_results {
            write_tag(&mut buf, QUERY_RESPONSE_RESULTS, WIRE_TYPE_LENGTH_DELIMITED);
            write_length_delimited(&mut buf, wrapped);
        }
        write_tag(&mut buf, QUERY_RESPONSE_HIT_COUNT, WIRE_TYPE_VARINT);
        write_int32(&mut buf, hit_count);
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::wrapped_scalar_bytes;
    use super::*;

    #[test]
    fn wrapped_scalar_round_trips_every_query_value_variant() {
        let cases = [
            QueryValue::String("hello".to_string()),
            QueryValue::Int64(-42),
            QueryValue::Int32(-7),
            QueryValue::UInt64(42),
            QueryValue::UInt32(7),
            QueryValue::Double(1.5),
            QueryValue::Float(2.5),
            QueryValue::Bool(true),
            QueryValue::Bool(false),
            QueryValue::Bytes(vec![1, 2, 3]),
            QueryValue::Null,
        ];
        for value in cases {
            let bytes = wrapped_scalar_bytes(&value);
            let decoded = decode_wrapped_value(&bytes).unwrap();
            match decoded {
                WrappedValue::Scalar(decoded_value) => assert_eq!(decoded_value, value),
                WrappedValue::Entity { .. } => panic!("expected a scalar"),
            }
        }
    }

    #[test]
    fn wrapped_entity_decodes_message_bytes_identified_by_type_name() {
        let mut buf = Vec::new();
        write_tag(&mut buf, WRAPPED_TYPE_NAME, WIRE_TYPE_LENGTH_DELIMITED);
        write_length_delimited(&mut buf, b"org.example.User");
        write_tag(&mut buf, WRAPPED_MESSAGE, WIRE_TYPE_LENGTH_DELIMITED);
        write_length_delimited(&mut buf, b"entity-bytes");

        match decode_wrapped_value(&buf).unwrap() {
            WrappedValue::Entity(bytes) => assert_eq!(bytes, b"entity-bytes"),
            WrappedValue::Scalar(_) => panic!("expected an entity"),
        }
    }

    #[test]
    fn wrapped_entity_decodes_message_bytes_identified_by_type_id() {
        let mut buf = Vec::new();
        write_tag(&mut buf, WRAPPED_TYPE_ID, WIRE_TYPE_VARINT);
        write_varint(&mut buf, 42);
        write_tag(&mut buf, WRAPPED_MESSAGE, WIRE_TYPE_LENGTH_DELIMITED);
        write_length_delimited(&mut buf, b"entity-bytes");

        match decode_wrapped_value(&buf).unwrap() {
            WrappedValue::Entity(bytes) => assert_eq!(bytes, b"entity-bytes"),
            WrappedValue::Scalar(_) => panic!("expected an entity"),
        }
    }

    #[test]
    fn wrapped_message_with_both_type_name_and_type_id_is_rejected() {
        let mut buf = Vec::new();
        write_tag(&mut buf, WRAPPED_TYPE_NAME, WIRE_TYPE_LENGTH_DELIMITED);
        write_length_delimited(&mut buf, b"org.example.User");
        write_tag(&mut buf, WRAPPED_TYPE_ID, WIRE_TYPE_VARINT);
        write_varint(&mut buf, 42);
        write_tag(&mut buf, WRAPPED_MESSAGE, WIRE_TYPE_LENGTH_DELIMITED);
        write_length_delimited(&mut buf, b"entity-bytes");

        assert!(matches!(
            decode_wrapped_value(&buf),
            Err(Error::MalformedQueryResponse(_))
        ));
    }

    #[test]
    fn wrapped_message_with_no_type_discriminator_is_rejected() {
        let mut buf = Vec::new();
        write_tag(&mut buf, WRAPPED_MESSAGE, WIRE_TYPE_LENGTH_DELIMITED);
        write_length_delimited(&mut buf, b"entity-bytes");

        assert!(matches!(
            decode_wrapped_value(&buf),
            Err(Error::MalformedQueryResponse(_))
        ));
    }

    #[test]
    fn wrapped_message_with_no_recognized_field_is_rejected() {
        assert!(matches!(
            decode_wrapped_value(&[]),
            Err(Error::MalformedQueryResponse(_))
        ));
    }

    #[test]
    fn query_request_encodes_query_string_offset_limit_and_parameters() {
        let bytes = encode_query_request(
            "FROM Foo WHERE bar = :baz",
            10,
            Some(5),
            None,
            &[("baz".to_string(), QueryValue::Int32(3))],
        );

        let mut pos = 0;
        let (field, wire_type) = read_tag(&bytes, &mut pos).unwrap().unwrap();
        assert_eq!(
            (field, wire_type),
            (QUERY_REQUEST_QUERY_STRING, WIRE_TYPE_LENGTH_DELIMITED)
        );
        assert_eq!(
            read_length_delimited(&bytes, &mut pos).unwrap(),
            b"FROM Foo WHERE bar = :baz"
        );

        let (field, _) = read_tag(&bytes, &mut pos).unwrap().unwrap();
        assert_eq!(field, QUERY_REQUEST_START_OFFSET);
        assert_eq!(read_int64(&bytes, &mut pos).unwrap(), 10);

        let (field, _) = read_tag(&bytes, &mut pos).unwrap().unwrap();
        assert_eq!(field, QUERY_REQUEST_MAX_RESULTS);
        assert_eq!(read_int32(&bytes, &mut pos).unwrap(), 5);

        let (field, _) = read_tag(&bytes, &mut pos).unwrap().unwrap();
        assert_eq!(field, QUERY_REQUEST_NAMED_PARAMETERS);
        let param_bytes = read_length_delimited(&bytes, &mut pos).unwrap();
        let mut param_pos = 0;
        let (field, _) = read_tag(&param_bytes, &mut param_pos).unwrap().unwrap();
        assert_eq!(field, NAMED_PARAMETER_NAME);
        assert_eq!(
            read_length_delimited(&param_bytes, &mut param_pos).unwrap(),
            b"baz"
        );
        let (field, _) = read_tag(&param_bytes, &mut param_pos).unwrap().unwrap();
        assert_eq!(field, NAMED_PARAMETER_VALUE);
        let value_bytes = read_length_delimited(&param_bytes, &mut param_pos).unwrap();
        assert_eq!(
            decode_wrapped_value(&value_bytes).unwrap(),
            WrappedValue::Scalar(QueryValue::Int32(3))
        );

        assert_eq!(pos, bytes.len());
    }

    #[test]
    fn query_request_omits_hit_count_accuracy_unless_requested() {
        let bytes = encode_query_request("FROM Foo", 0, None, None, &[]);
        let mut pos = 0;
        while let Some((field, wire_type)) = read_tag(&bytes, &mut pos).unwrap() {
            assert_ne!(field, QUERY_REQUEST_HIT_COUNT_ACCURACY);
            skip_field(&bytes, &mut pos, wire_type).unwrap();
        }
    }

    #[test]
    fn query_request_sets_hit_count_accuracy_for_execute_statement() {
        let bytes = encode_query_request("DELETE FROM Foo WHERE bar = :baz", 0, None, Some(1), &[]);

        let mut pos = 0;
        let (field, _) = read_tag(&bytes, &mut pos).unwrap().unwrap(); // query string
        read_length_delimited(&bytes, &mut pos).unwrap();
        assert_eq!(field, QUERY_REQUEST_QUERY_STRING);

        let (field, _) = read_tag(&bytes, &mut pos).unwrap().unwrap(); // start offset
        read_int64(&bytes, &mut pos).unwrap();
        assert_eq!(field, QUERY_REQUEST_START_OFFSET);

        let (field, wire_type) = read_tag(&bytes, &mut pos).unwrap().unwrap();
        assert_eq!(
            (field, wire_type),
            (QUERY_REQUEST_HIT_COUNT_ACCURACY, WIRE_TYPE_VARINT)
        );
        assert_eq!(read_int32(&bytes, &mut pos).unwrap(), 1);
        assert_eq!(pos, bytes.len());
    }

    impl PartialEq for WrappedValue {
        fn eq(&self, other: &Self) -> bool {
            match (self, other) {
                (WrappedValue::Scalar(a), WrappedValue::Scalar(b)) => a == b,
                _ => false,
            }
        }
    }
    impl std::fmt::Debug for WrappedValue {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                WrappedValue::Scalar(v) => write!(f, "Scalar({v:?})"),
                WrappedValue::Entity { .. } => write!(f, "Entity"),
            }
        }
    }

    #[test]
    fn query_response_without_projection_decodes_whole_entities() {
        let mut buf = Vec::new();
        write_tag(&mut buf, QUERY_RESPONSE_PROJECTION_SIZE, WIRE_TYPE_VARINT);
        write_int32(&mut buf, 0);

        for entity in [b"entity-1".as_slice(), b"entity-2".as_slice()] {
            let mut wrapped = Vec::new();
            write_tag(&mut wrapped, WRAPPED_TYPE_NAME, WIRE_TYPE_LENGTH_DELIMITED);
            write_length_delimited(&mut wrapped, b"org.example.User");
            write_tag(&mut wrapped, WRAPPED_MESSAGE, WIRE_TYPE_LENGTH_DELIMITED);
            write_length_delimited(&mut wrapped, entity);

            write_tag(&mut buf, QUERY_RESPONSE_RESULTS, WIRE_TYPE_LENGTH_DELIMITED);
            write_length_delimited(&mut buf, &wrapped);
        }

        write_tag(&mut buf, QUERY_RESPONSE_HIT_COUNT, WIRE_TYPE_VARINT);
        write_int32(&mut buf, 2);
        write_tag(&mut buf, QUERY_RESPONSE_HIT_COUNT_EXACT, WIRE_TYPE_VARINT);
        write_varint(&mut buf, 1);

        let result = decode_query_response(&buf).unwrap();
        assert_eq!(
            result.rows,
            vec![
                QueryRow::Entity(b"entity-1".to_vec()),
                QueryRow::Entity(b"entity-2".to_vec()),
            ]
        );
        assert_eq!(result.hit_count, 2);
        assert!(result.hit_count_exact);
    }

    #[test]
    fn query_response_with_projection_groups_columns_into_rows() {
        let mut buf = Vec::new();
        write_tag(&mut buf, QUERY_RESPONSE_PROJECTION_SIZE, WIRE_TYPE_VARINT);
        write_int32(&mut buf, 2);

        for value in [
            QueryValue::String("Alice".to_string()),
            QueryValue::Int32(30),
            QueryValue::String("Bob".to_string()),
            QueryValue::Int32(40),
        ] {
            write_tag(&mut buf, QUERY_RESPONSE_RESULTS, WIRE_TYPE_LENGTH_DELIMITED);
            write_length_delimited(&mut buf, &wrapped_scalar_bytes(&value));
        }

        let result = decode_query_response(&buf).unwrap();
        assert_eq!(
            result.rows,
            vec![
                QueryRow::Columns(vec![
                    QueryValue::String("Alice".to_string()),
                    QueryValue::Int32(30)
                ]),
                QueryRow::Columns(vec![
                    QueryValue::String("Bob".to_string()),
                    QueryValue::Int32(40)
                ]),
            ]
        );
    }

    #[test]
    fn query_response_rejects_a_result_count_not_divisible_by_projection_size() {
        let mut buf = Vec::new();
        write_tag(&mut buf, QUERY_RESPONSE_PROJECTION_SIZE, WIRE_TYPE_VARINT);
        write_int32(&mut buf, 2);
        write_tag(&mut buf, QUERY_RESPONSE_RESULTS, WIRE_TYPE_LENGTH_DELIMITED);
        write_length_delimited(&mut buf, &wrapped_scalar_bytes(&QueryValue::Int32(1)));

        assert!(matches!(
            decode_query_response(&buf),
            Err(Error::MalformedQueryResponse(_))
        ));
    }
}
