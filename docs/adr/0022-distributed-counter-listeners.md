# ADR 0022: Distributed counter listeners

## Status

Accepted, implemented in #113.

## Context

ADR 0015 (`docs/adr/0015-distributed-counters.md`, #49) deliberately
cut `COUNTER_ADD_LISTENER`/`COUNTER_REMOVE_LISTENER`
(`0x5A`/`0x5B`, `0x5C`/`0x5D`): "core first, listeners later if they
turn out to matter," the same cut remote query made for streaming.
Issue #113 already predicted the shape: "would likely follow the same
shape as `RemoteCache::listen` (#4,
`docs/adr/0006-client-listeners.md`)."

Confirmed against the Java client and server source
(`infinispan/infinispan`, fetched directly, not guessed): the
prediction holds almost exactly, with the counter protocol simpler
than the cache one in every place it differs.

**Request shape** (`client/hotrod-client/.../counter/operation/`
`AddListenerOperation.java`/`BaseCounterOperation.java`,
`RemoveListenerOperation.java`): `CounterAddListener`'s and
`CounterRemoveListener`'s request body is `counterName` (a Hot Rod
string) then `listenerId` (a Hot Rod array, **not** the fixed
16-byte raw id cache listeners use), on the fixed
`org.infinispan.COUNTER` cache name every other counter opcode
already targets. Both are a plain "response = request + 1" pair
(`0x5A`→`0x5B`, `0x5C`→`0x5D`): unlike `CounterIsDefined`'s `+2`
(ADR 0015), nothing sits between either of these and its own
response.

**Event shape** (`COUNTER_EVENT_RESPONSE = 0x66`, confirmed in
`HotRodConstants.java` on both sides). Server encoder
(`server/hotrod/.../counter/listener/ClientCounterEvent.java`,
`writeTo`) and client decoder (`Codec30.readCounterEvent`) agree the
frame, after the generic magic/message-id/opcode, is: status byte
(always success), topology marker (always `0`, same as a cache
event), `counterName` (string), `listenerId` (array), one byte of
packed old/new `CounterState` (bits 0-1 old:
`0`=valid/`1`=lower/`2`=upper; bits 2-3 new:
`0`=valid/`4`=lower/`8`=upper), `oldValue` (i64), `newValue` (i64).
No `isCustom`/`isRetried` bytes, no filter/converter factory, no
interests bitmask: the counter listener protocol has no server-side
filtering concept at all, so `AddListener`'s request has nothing
beyond `counterName`/`listenerId`, and there is no `ListenOptions`
counterpart to design.

**API shape**: the Java client exposes this as
`StrongCounter.addListener`/`WeakCounter.addListener` (returning a
`Handle<CounterListener>`), not on `CounterManager`.

## Decision

A new `counter_listener.rs`, structurally a smaller sibling of
`listener.rs`, reusing every architectural decision ADR 0006 already
made (a dedicated connection, no automatic reconnection, no
`futures_core`/`tokio_stream::Stream` implementation, the same
cluster-generation race guard ADR 0019 added) rather than
re-deciding any of them for counters.

## Design

### `counter_listener.rs`

```rust
pub enum CounterState { Valid, LowerBoundReached, UpperBoundReached }

pub struct CounterEvent {
    pub old_value: i64,
    pub old_state: CounterState,
    pub new_value: i64,
    pub new_state: CounterState,
}

pub struct CounterListener { /* dedicated connection, listener id, counter name, ... */ }

impl CounterListener {
    pub(crate) async fn register(conn, counter_name, cache_name, timeout, generation_rx) -> Result<Self>;
    pub async fn next(&mut self) -> Option<Result<CounterEvent>>;
    pub async fn close(mut self) -> Result<()>;
}
```

`register`/`next`/`close` are line-for-line `CacheListener`'s own
(same poisoning rule: a flag set once a frame's magic byte arrives,
cleared only once the whole frame parses; the same `tokio::select!`
race against `generation_rx`; the same `read_..._after_magic` split
from `header::read_response_header`, since an event frame has no
pending request to validate against). Every difference follows
directly from the wire format above:

* The `AddListener`/`RemoveListener` request body is `counterName`
  (`write_array(body, counter_name.as_bytes())`, matching
  `connection.rs`'s own existing `counter_get`/`counter_reset`/etc.
  convention for encoding a counter name) then `listener_id`
  (`write_array`). No factories, no interests, no
  `include_current_state`.
* `read_counter_event_after_magic` reads `counterName` (`read_string`)
  before `listenerId` (`read_array`), the opposite field order from
  a cache event, and validates **both** against what this listener
  registered with. Cache events only validate `listener_id` (a cache
  event carries no cache-name field to check at all); a counter event
  does carry its own counter name, so this checks it too, catching a
  field-order mistake a `listener_id`-only check could miss.
* The event body itself (one state byte, two `i64`s) replaces cache
  events' `is_custom`/`is_retried`/key/version entirely.
  `decode_old_state`/`decode_new_state` mirror `ClientCounterEvent`'s
  own bit layout exactly.
* `Error::MalformedEvent` is reused as-is for every new
  malformed-frame case (an unrecognized event opcode, a counter-name
  or listener-id mismatch, an unrecognized state bit pattern): the
  same shape ADR 0006 already established, no new `Error` variant
  needed.

### Protocol wiring

`header.rs`'s `OpCode` gains `CounterAddListener = 0x5A` and
`CounterRemoveListener = 0x5C`, both plain "request + 1" (no change
to `expected_response_opcode`, whose default arm already covers
them), replacing the "deliberately out of scope for this phase"
comment this ADR resolves. `COUNTER_EVENT` (`0x66`) is **not** added
to `OpCode`, following the exact precedent `listener.rs` already set
for cache event opcodes (`EVENT_CREATED` etc.): a response-only,
no-request opcode is a local `const u8` in the module that consumes
it, never passed through `expected_response_opcode`, which is a
request-to-response mapping with nothing to map from for an opcode
the client never sends.

`connection.rs` gains one method, `counter_add_listener`, a copy of
`add_client_listener` with `OpCode::CounterAddListener` in place of
`OpCode::AddClientListener`. No `counter_remove_listener` method is
needed: `close`, like `CacheListener::close`, operates directly on
the raw transport via `header::write_and_read_header` once
`into_transport` has already handed it over, never through
`HotRodConnection` again.

### Where `listen` is reached from

Mirrors the Java client's own shape exactly: `StrongCounter::listen`/
`WeakCounter::listen`, not `CounterManager::listen`. Both delegate to
a new `pub(crate)` `RemoteCache::listen_for_counter`, a direct copy
of `listen_with`'s own shape (the active seed and cluster generation
read together under one lock, per ADR 0019; `open_and_authenticate`
against it; then `CounterListener::register`). `listen_for_counter`
is `pub(crate)` only: calling it on a bare `RemoteCache` (bypassing
`StrongCounter`/`WeakCounter`) is not a supported entry point, the
same as counters' own CRUD operations.

### Test coverage

`counter_listener.rs::tests` mirrors `listener.rs::tests` one for one
(register, every `CounterState` combination in sequence, a clean
close, a cluster-generation switch, a server error, an unrecognized
status byte, a connection dropped mid-frame, `close`): the same
coverage shape, substituting the counter event's own payload. No
separate test was added for `listen_for_counter`/`StrongCounter::listen`'s
own wiring: `listen_with`, the direct cache-listener analog, has none
either (it is a thin, visibly-correct seed-and-cache-name lookup, not
independent logic), so adding one here would be asymmetric rigor
relative to the precedent this phase is deliberately mirroring, not
a gap.

## Consequences

* New public API: `CounterState`, `CounterEvent`, `CounterListener`,
  `StrongCounter::listen`, `WeakCounter::listen`. Additive: nothing
  existing changes shape.
* New `OpCode` variants `CounterAddListener`/`CounterRemoveListener`;
  both fit the existing "request + 1" convention, so no change to
  `header.rs`'s response-matching logic was needed, unlike
  `CounterIsDefined`'s own `+2` exception.
* No server-side filtering exists in the counter listener protocol
  itself: there is no `ListenOptions` counterpart for counters, and
  none is planned.
* Not implemented, inherited unchanged from ADR 0006/0015: automatic
  reconnection, a `Stream` trait implementation, hash-routing by
  counter name (counter listeners go to the active seed, the same as
  every other counter operation).
