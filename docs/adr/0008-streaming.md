# ADR 0008: Streaming reads and writes, pinned to one pooled connection

## Status

Accepted, implemented in #52.

## Context

`hotrod-protocol` always buffers a whole value in memory for `get`/
`put`. #52 asks for the Hot Rod streaming operations (read or write a
value in chunks) the Java client already has, and says so explicitly:
"Needs its own ADR: streaming API shape... is a bigger design surface
than a single method."

The wire format is pinned against the real source, not memory, the
same discipline every prior phase used: `infinispan/infinispan`'s
`client/hotrod-client` module (the operation classes and
`HotRodConstants.java`), `server/hotrod`'s `HotRodOperation.java` (the
server-side opcode table, to cross-check the client's), and
`documentation/src/main/asciidoc/topics/hotrod_protocol.adoc` (the
protocol's own prose spec) for the protocol version.

**A legacy, discontinued pair of opcodes already exists and is not
what this phase implements.** `GET_STREAM_REQUEST`/`PUT_STREAM_REQUEST`
(`0x37`/`0x39`) date to Hot Rod 2.6 and the adoc states directly (Hot
Rod Protocol 4.1 section): "This version officially drops support for
the previous GET_STREAM and PUT_STREAM operations added in 2.6." The
current Java client's streaming classes never send these two; they use
the six opcodes below instead.

**Opcodes** (`HotRodConstants.java`, client and server agree):

| Operation | Request | Response |
| --- | --- | --- |
| GetStreamStart | `0xE9` | `0xE8` |
| GetStreamNext | `0xE7` | `0xE6` |
| GetStreamEnd | `0xE5` | `0xE4` |
| PutStreamStart | `0xEF` | `0xEE` |
| PutStreamNext | `0xED` | `0xEC` |
| PutStreamEnd | `0xEB` | `0xEA` |

These six are the one place in this protocol, confirmed directly
against the constants table, where the response opcode is the request
opcode **minus** one, not plus one like every other operation this
client implements. `header.rs`'s `OpCode::expected_response_opcode`
now special-cases exactly these six instead of assuming the pattern
holds universally.

**Request/response bodies**, confirmed against the operation classes
(`GetStreamStartOperation.java`, `GetStreamNextOperation.java`,
`GetStreamEndOperation.java`, and the three `PutStream*` equivalents):

* `GetStreamStart`: key (array) + `batchSize` (vInt). Response:
  `streamId` (`i32`) + `complete` (bool) + the same metadata block
  `GetWithMetadata` already returns (flags, created/lifespan,
  lastUsed/maxIdle, an 8-byte version) + the first chunk itself (vInt
  length + bytes, the same shape as any other array on this wire). The
  response is not just a handle: if the whole value fits in one
  `batchSize`, `complete` is already `true` and no `GetStreamNext` is
  ever needed.
* `GetStreamNext`: `streamId` only. Response: the echoed `streamId`
  (this client does not re-check it, see below), `complete`, and the
  next chunk.
* `GetStreamEnd`: `streamId`. Response: status only. Must be sent
  explicitly to abandon a stream before it reports `complete` on its
  own; the Java client sends this from the `InputStream`'s `close()`,
  never implicitly.
* `PutStreamStart`: key + expiration in the same **write** encoding
  `put`/`replace` already use (`write_expiration_params`: one byte of
  time units, then an optional vLong lifespan and/or max idle) +
  `version` (`i64`, 8 bytes): `0` for an unconditional put, `-1` for
  put-if-absent, any other value for a conditional replace against a
  version from `get_with_version`. Response: `streamId` only.
* `PutStreamNext`: `streamId` + `complete` (bool) + chunk. Response:
  status only. No total size is announced up front; the server only
  performs the write, subject to whatever `version` `PutStreamStart`
  carried, once a `PutStreamNext` arrives with `complete: true`.
* `PutStreamEnd`: `streamId`. Response: status only. Never needed in
  the happy path, since the final `complete: true` chunk already
  finishes the operation; exists only to abandon a stream cleanly.

## Decision

**A stream is pinned to the one connection that opened it, reusing
`pool.rs`'s existing `PooledGuard` rather than a new connection
model.** The Java client enforces this on the client side directly:
`GetStreamNextOperation`/`PutStreamNextOperation` compare the channel
a `Next`/`End` call would run on against the one `Start` used, and
throw `TransportException` on a mismatch, because the server scopes
`streamId` to that one connection, not to the cluster or to the key.
This is neither the general pool model (#77, any idle connection
serves any request) nor the dedicated, never-returned connection model
a `CacheListener` uses (#4): it is a third shape, a specific pooled
connection held for longer than one call, then returned once the
stream ends. `pool.rs`'s `PooledGuard` already supports exactly this:
it can be held for an arbitrary lifetime (not just one call), already
derefs to `HotRodConnection`, and already returns the connection to
the pool on `Drop`. `GetStream`/`PutStream` (`streaming.rs`) just hold
one for as long as the stream is open; no new connection-handling
concept was needed. Because of this pinning, this client also never
re-checks the echoed `streamId` `GetStreamNext`'s response carries:
there is only ever one stream in flight on a connection this type
owns, so it cannot have desynced against a different one the way the
Java client's single multiplexed-per-server channel can.

**No retry, no failover.** The Java client disables retry on these
operations outright (`supportRetry()` returns `false` on all four
`*Next`/`*End` operations), consistent with the state being pinned to
one connection: there is nothing a retry against a different node
could do with a `streamId` that only means something on the one
connection that is now gone. `get_stream`/`put_stream*` check out a
connection for `key`'s computed owner once and use it directly,
bypassing `RemoteCache::call`'s retry-and-failover machinery entirely.

**Dropped without an explicit `close`/`finish`: poison the
connection, unless the stream had already resolved on its own.** The
Java client calls `close()`/`finish()` from a `finally` block, which
Rust's synchronous `Drop` cannot do for an `async` operation like
`GetStreamEnd`/`PutStreamEnd`. Sending nothing on drop would leave the
server's stream state dangling on a connection this client would
otherwise return to the pool for an unrelated caller to reuse, with no
way to know whether the server tolerates a new request arriving on a
connection it still thinks has a stream open. Rather than find out
against a real server (not something this phase can verify safely),
`GetStream`/`PutStream`'s `Drop` marks the connection poisoned instead,
the same mechanism `HotRodConnection` already uses for a cancelled
operation; `pool.rs` already discards poisoned connections instead of
reusing them. The one exception: a `GetStream` already reported
`complete` by a `next_chunk` call (including the first one, if the
whole value fit in `GetStreamStart`'s own response) needs no poisoning
on drop, since the server has already cleaned up that stream on its
own by that point; `PutStream` has no equivalent case, since nothing
commits server-side until an explicit `finish`/`abandon` call says so.

**Minimal async methods now, not `AsyncRead`/`AsyncWrite`.**
`GetStream::next_chunk`/`PutStream::write_chunk`/`finish` return and
take plain `Vec<u8>`/`&[u8]` chunks. The issue itself suggests
something `AsyncRead`/`AsyncWrite`-shaped, and `tokio`'s `io-util`
feature (already enabled, no new dependency) supports implementing
either trait by hand. Decided to ship the plain chunk API first and
prove the underlying mechanism (chunking, the connection-pinning,
poisoning on an unresolved drop) correct on its own, rather than take
on a hand-rolled `poll_read`/`poll_write` state machine in the same
change; a `AsyncRead`/`AsyncWrite` wrapper around this same API is a
non-breaking addition to make later if it turns out to matter.

**All three conditional `PutStream` variants now, not just the
unconditional one.** `put_stream`, `put_stream_if_absent` and
`replace_stream_with_version` all go through the same
`put_stream_start`, differing only in which `version` sentinel they
pass (`0`, `-1`, or a real version). Since the mechanism is identical,
there was no real reason to ship only one and defer the other two.

## Consequences

* New public API: `RemoteCache::get_stream`/`put_stream`/
  `put_stream_if_absent`/`replace_stream_with_version`, `GetStream`
  (`next_chunk`, `close`, plus `version`/`created`/`lifespan`/
  `last_used`/`max_idle` fields mirroring `VersionedValue`), `PutStream`
  (`write_chunk`, `finish`, `abandon`). Additive: nothing existing
  changes shape.
* New `OpCode` variants for all six opcodes; `expected_response_opcode`
  is no longer a blanket "request plus one" and now special-cases
  these six, confirmed against the real constants rather than assumed
  from the pattern every other opcode happens to follow.
* `HotRodConnection::read_versioned_value`'s metadata-block parsing is
  factored out into `read_entry_metadata`, shared with
  `get_stream_start`'s response, which carries the identical block
  before a chunk instead of a whole value.
* `HotRodConnection` gains a direct `mark_poisoned` method, for
  `streaming.rs` to call from outside the usual
  `begin_operation`/`end_operation` pair around a single call, since a
  stream's `Drop` needs to poison a connection no individual operation
  is currently running on.
* No new dependency and no `unsafe`, consistent with every prior
  phase.
* Not implemented, left for a later, independent, additive change if
  it turns out to matter: `impl AsyncRead`/`AsyncWrite` for
  `GetStream`/`PutStream`.
