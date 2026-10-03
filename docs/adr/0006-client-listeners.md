# ADR 0006: Client listeners on a dedicated connection, no reconnection

## Status

Accepted, implemented in #4.

## Context

ADR 0001 lists "cache event listeners, including server-side event
filtering" as phase 4. Nothing of the sort exists yet: `lib.rs` said so
plainly ("No listeners, no near caching yet") until this phase landed.
#5 (near caching) depends on this: invalidating a client-side cache on
a remote change needs to know when that change happened, and that only
comes from a listener.

The wire format is not derived from memory. It is pinned against the
Java client's own source
(`infinispan/infinispan` on GitHub, same reference implementation ADR
0001 already treats as authoritative), the same discipline already
used for `MurmurHash3` (ADR 0003) and the SASL mechanisms (ADR 0002):
`HotRodConstants.java` for the opcodes, `Codec30.java`'s
`writeClientListenerParams`/`writeClientListenerInterests`/
`readCacheEvent` for the exact field order, and
`ClientListenerOperation.java` for how a listener id is generated.

**Opcodes:** `ADD_CLIENT_LISTENER_REQUEST=0x25` /
`_RESPONSE=0x26`, `REMOVE_CLIENT_LISTENER_REQUEST=0x27` /
`_RESPONSE=0x28`. Event opcodes, pushed by the server rather than sent
by the client: `CACHE_ENTRY_CREATED_EVENT_RESPONSE=0x60`,
`_MODIFIED=0x61`, `_REMOVED=0x62`, `_EXPIRED=0x63`.

**`AddClientListener` request body**, in order: a 16-byte listener id
(client-generated; the Java client fills it with a random UUID, this
one uses `rand`, already a dependency), an `includeCurrentState` byte,
a filter factory name (empty string if none) with, only when non-empty,
a **single-byte** parameter count followed by that many byte arrays
(not a vInt-counted length like every other count in this protocol:
`Codec30.writeNamedFactory` writes it as one byte), the same shape
again for a converter factory, a `useRawData` byte, and a vInt bitmask
of event-type interests (`0x01` created, `0x02` modified, `0x04`
removed, `0x08` expired).

"Including server-side event filtering" from ADR 0001's phase
description turns out to mean exactly this much: sending a factory name
and parameters the server already has a deployed implementation for.
`hotrod-protocol` never evaluates filter or converter logic itself,
only transports the name and parameters; the actual filtering runs
server-side.

**Event frames**, once `AddClientListener` succeeds: the generic
response shape (magic, message id, opcode, status, a topology marker
always `0` for events) followed by listener id, an `isCustom` byte (`0`
normal, `1` an unmarshalled custom object, `2` raw custom bytes), an
`isRetried` byte, then for a normal event a key and, for created/
modified only, an 8-byte version; for a custom event (`isCustom` `1` or
`2`), raw bytes instead, which this crate never attempts to unmarshal
either way, consistent with it being "deliberately byte-oriented" (ADR
0005). Confirmed directly in `Codec30.readCacheEvent`: the generic
frame decoder reads magic/message id/opcode first and only then decides
whether this is a reply to a specific pending request (opcode validated
against what that request expects) or an event (opcode outside that
range, routed to the event parser instead, with no pending request to
validate against at all).

That last point is the one with real architectural weight. Every
operation on `HotRodConnection` writes one request and reads exactly
one matching response before the next call may proceed (see its module
docs); `header::read_response_header` enforces this by checking the
response opcode against the one specific request it was just asked to
send. An event frame fits neither: it can arrive at any time, matches
no pending request, and uses a different opcode range entirely. Phase 4
needs a different read loop, not a variant of the existing one.

## Options considered

**Where a listener's events are read from:**

1. **A connection dedicated to the listener alone**, separate from
   `HotRodClient`'s pool (#77). Once registered, nothing else is ever
   sent or expected on it besides events (and, on `close`, one
   `RemoveClientListener` round trip). `HotRodConnection` and the pool
   stay exactly as they are.
2. **Multiplex events onto a pooled connection.** Hot Rod protocol 2.8+
   (this project targets 4.1) allows ordinary requests and event
   frames to interleave on the same connection. Doing this would mean
   teaching `HotRodConnection`'s read path to demultiplex: tell an
   event frame apart from a response by opcode before deciding whether
   to match it against the in-flight request or hand it to a
   listener's channel, while that same connection is also being
   checked in and out of a pool by unrelated operations.

Option 2 is strictly more capable (one less connection per listener,
no step backward to pre-2.8 connection semantics) but changes a model
every prior phase (TLS, pooling, hash-aware routing) has so far left
alone. Option 1 keeps `HotRodConnection` and `pool.rs` untouched and
reuses `HotRodConnection` for everything up to the point registration
succeeds (connect, TLS, SASL auth, the `AddClientListener` request
itself), handing off only the raw transport afterward
(`HotRodConnection::into_transport`).

**Reconnection on connection loss:**

1. **None.** `CacheListener::next` returns `None` on a clean close or a
   terminal `Err` on anything else. The caller decides whether to
   register a new listener.
2. **Automatic**: detect the drop, reopen a connection, replay
   `AddClientListener` with the same listener id, resume the same
   stream transparently.

Option 2 is closer to what a caller migrating from the Java client
might expect, but it is meaningfully more logic (detecting which
failures are reconnect-worthy, replaying registration, deciding how
many attempts before giving up) for a first version of a feature this
project has had zero production experience with yet.

## Decision

**Option 1 for both: a dedicated connection, no automatic
reconnection.** `RemoteCache::listen`/`listen_with` open a connection
to the current active seed (events are cache-wide, not routed by key,
so there is no segment owner to pick instead), authenticate it with
whatever credentials `HotRodClient` already holds, send
`AddClientListener`, and on success hand the raw transport to a new
`CacheListener`. Not retried against another seed if this one refuses
the connection: unlike a single `get`/`put`, which node ends up holding
a long-lived registration is worth the caller seeing directly rather
than this failing over silently.

`listener.rs` is additive, the same shape every prior phase has used:
`HotRodConnection` gains exactly two things for this
(`add_client_listener`, which reuses its existing
`write_and_read_header`/poisoning machinery for the one request/response
round trip registration needs, and `into_transport`, the handoff
itself); nothing else about it changes.

**No `futures_core::Stream`/`tokio_stream::Stream` implementation.**
The roadmap's own milestone notes describe this as "an async `Stream`
of cache events," but implementing either trait for real means
depending on `futures-core` or `tokio-stream`. This project has
consistently avoided new dependencies where a small amount of its own
code does the job (ADR 0002 ruled out GSSAPI partly over a system
dependency, ADR 0004 chose `rustls` over `native-tls` specifically to
avoid a C one, ADR 0005 landed pooling with none at all). `CacheListener`
instead exposes a plain inherent `async fn next(&mut self) ->
Option<Result<CacheEvent>>`: a pull-based stream in the ordinary sense
of the word, without the trait. Adopting the real trait later, if
ecosystem interop actually needs it, is a small, independent dependency
decision to make at that point, not one this phase needs to force.

**Server-side filtering is transport-only.** `ListenOptions::filter_factory`
and `converter_factory` carry a name and raw parameters through to the
wire exactly as described above. `hotrod-protocol` does not ship, and
has no way to evaluate, any filter or converter logic of its own; a
caller using either option is pointing at something already deployed
on the server.

## Consequences

* New public API: `RemoteCache::listen`/`listen_with`,
  `CacheListener` (`next`, `close`), `CacheEvent`
  (`Created`/`Modified`/`Removed`/`Expired`/`Custom`),
  `CacheEventInterests`, `ListenOptions`, `ServerFactory`. Additive:
  nothing existing changes shape.
* `HotRodConnection` gains `pub(crate) add_client_listener` and
  `pub(crate) into_transport`; `HotRodClient::open_and_authenticate`
  becomes `pub(crate)` (previously private) so `RemoteCache::listen_with`
  can get a connection to hand off, the same way `checkout` already
  does internally.
* New `OpCode` variants `AddClientListener` (`0x25`) and
  `RemoveClientListener` (`0x27`); both fit the existing
  request-opcode-plus-one convention for their responses, so no change
  to `header.rs`'s response-matching logic was needed.
* New `Error::MalformedEvent(String)` for an event frame that violates
  an invariant this client relies on (an unrecognized event opcode, or
  one carrying a listener id other than the dedicated connection's own)
  mirroring the existing `MalformedChallenge` for SASL.
* A dropped `CacheListener` that never called `close` leaves the
  server to notice the connection is gone and clean up the registration
  on its own, the same as it would for any other dead Hot Rod
  connection; this is not a leak, just not as immediate as calling
  `close` explicitly.
* No new dependency: `rand`, already used for SCRAM's and DIGEST's
  nonces, generates the listener id too.
* `CacheListener` carries the same poisoning rule
  `HotRodConnection` documents and enforces for itself (see its module
  docs): a flag set once a frame's magic byte has arrived, cleared only
  once that whole frame parses successfully. Caught by this PR's own
  review: `next`'s doc explicitly allows pairing it with
  `tokio::select!`/an external timeout, so a future dropped mid-frame is
  exactly as reachable here as it is for any `HotRodConnection`
  operation, and an `UnexpectedEof` partway through a frame is a real
  failure, not the clean, between-frames close it would have been
  mistaken for without the fix.
* A filter/converter factory's parameter count is capped at 255
  (`MAX_FACTORY_PARAMS`), the most a single wire byte can express
  (`Codec30.writeNamedFactory`): checked before writing anything,
  instead of silently truncating a larger count into a desynced
  request. Also caught by review.
* `header::write_and_read_header` (write a request, read its response
  header) is factored out of `HotRodConnection::write_and_read_header`
  so `CacheListener::close`'s `RemoveClientListener` round trip can
  share it instead of hand-rolling a second copy that tracks a
  listener's connection having no topology state to update.
* Known, accepted limitations for this phase, candidates for a
  follow-up issue if a real use case asks for them: no automatic
  reconnection, no failover across seeds when first registering a
  listener, and no `futures_core`/`tokio_stream::Stream`
  implementation.
