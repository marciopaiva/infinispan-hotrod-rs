# ADR 0009: Server-side iteration, one sequential cursor per node

## Status

Accepted, implemented in #53.

## Context

`RemoteCache::get_all` only returns entries for keys the caller already
knows. #53 asks for the Hot Rod server-side iteration the Java client
exposes as `retrieveEntries`/`keySet`/`entrySet`/`values`, and flags the
open design question directly: "Needs its own ADR: cursor lifetime and
cancellation (dropping a Rust iterator/stream mid-scan needs to close
the server-side cursor, or leak it) is a real design question."

The wire format is pinned against the real source, the same discipline
every prior phase used: `infinispan/infinispan`'s `client/hotrod-client`
module (`IterationStartOperation.java`, `IterationNextOperation.java`,
`IterationEndOperation.java`, `Codec30.java`, `HotRodConstants.java`)
and `server/hotrod`/`server/core` (`CacheRequestProcessor.java`,
`DefaultIterationManager.java`, `Encoder2x.java`,
`IterableIterationResult.java`), since this is one of the places where
client and server have to agree on a shape neither side's public docs
spell out byte for byte.

**Opcodes** (`HotRodConstants.java`, client and server agree):

| Operation | Request | Response |
| --- | --- | --- |
| IterationStart | `0x31` | `0x32` |
| IterationNext | `0x33` | `0x34` |
| IterationEnd | `0x35` | `0x36` |

These follow the ordinary "response is request plus one" convention;
unlike streaming (#52), nothing here needs a special case in
`OpCode::expected_response_opcode`. The two features are otherwise
unrelated protocols from different Hot Rod versions (iteration dates to
2.3/Infinispan 8.0, streaming to 4.1/Infinispan 15.1) and share no code.

**A new status byte, `INVALID_ITERATION` (`0x05`), is not an error in
this protocol's usual sense.** It means the server no longer knows the
`iterationId`: reaped after five minutes idle (`DefaultIterationManager`
uses a hardcoded `Caffeine` cache with no protocol-level configuration
for that window), or the server restarted. Unlike every status
`is_error()` already recognized, its response body carries no message
string, confirmed against both server call sites:
`IterationNext`'s body is shaped exactly like a normal exhausted batch
(an empty finished-segments array, an entry count of zero), and
`IterationEnd`'s is empty either way (`emptyResponse`, status aside).
`Status` gained `is_invalid_iteration`, included in `is_known()` but
deliberately not in `is_error()`.

**Request/response bodies**, confirmed against the operation and codec
classes above:

* `IterationStart`: segments (a signed vInt sentinel of `-1` for no
  filter, or the byte length of a segment bitset followed by its bytes)
  + filter/converter factory name (the same `-1`-sentinel signed vInt
  scheme, not the empty-string sentinel `AddClientListener`'s
  `writeNamedFactory` uses for the same idea; the two are genuinely
  different encodings on this wire) + the factory's parameters, only
  present at all when a factory was given (a byte count, then each
  parameter as an ordinary array) + `batchSize` (a plain, unsigned
  vInt) + a `metadata` bool. Response: the iteration id as a plain
  array. The signed vInt is `SignedNumeric`'s ZigZag encoding
  (`writeSignedVInt`/`encode(i) = (i << 1) ^ (i >> 31)`), not the raw
  bit-reinterpretation `varint.rs` already uses for a negative topology
  id: `encode(-1)` is the single byte `0x01`, not five bytes of set
  bits. Confusing the two would have silently sent the wrong bytes for
  every segment count and filter name length, so `write_signed_vint`
  is its own function, documented against both shapes it must not be
  mistaken for.
* `IterationNext`: the iteration id only. Response: finished segments
  (the same bitset shape as the request's segment filter) + an entry
  count (vInt) + the entries. An empty response is how the protocol
  signals the cursor exhausted; there is no separate flag for it. If
  the count is non-zero, a value-projections count (vInt) follows,
  which must be `1`: this crate never sends a query (#50), so the
  server replying with more would be a protocol mismatch. Each entry is
  a presence byte for metadata (always `1` here, since `IterationStart`
  always asks for it) + the identical metadata block
  `read_entry_metadata` already extracts for `GetWithMetadata`/
  `GetStreamStart` + the key (array) + the value (array, repeated once
  per projection, always one here).
* `IterationEnd`: the iteration id. Response: status only.

**The segment bitset is Java's `BitSet.toByteArray()`**: bit `n` set
means byte `n / 8` has bit `n % 8` set, counted from the byte's least
significant bit, and the array is only as long as the highest set bit
needs. Nothing in this crate already encoded a `BitSet` this way, so
`write_segment_bitset` is new; there is no corresponding read side kept
in production code, since nothing here acts on the finished-segments
the response carries (see below).

**The server does not clean up a finished cursor on its own.**
Confirmed directly in the Java client, which calls `IterationEnd`
itself the moment `IterationNext` reports an empty batch, not just when
a caller stops early: the comment on that call path says plainly that
the server does not do this automatically. This is the opposite of
streaming's `GetStreamEnd`, which the server-side protocol resolves on
its own once a value is fully read. `connection.rs`'s `iteration_next`
mirrors the Java client exactly: it is the one place that detects
exhaustion, so it is the one place that sends `IterationEnd`, from
inside that same call rather than leaving it to a caller-visible
`close`.

## Decision

**A whole-cache iteration needs one server-side cursor per node, opened
sequentially.** Unlike streaming, which pins one key to one connection,
iterating a distributed cache means covering segments spread across
every node. The Java client confirms this without ambiguity: it opens
one `IterationStart` per node that primary-owns at least one segment
(`getPrimarySegmentsByAddress`, the same information
`client.rs::owner_addr` already computes for a single key, generalized
here to every segment at once in the new `nodes_and_owned_segments`),
then merges the results. This client opens those cursors **one at a
time**, exhausting a node's cursor before moving to the next, instead
of the Java client's concurrent Reactive Streams fan-out. Simpler to
implement and test, still correct (no segment is skipped), at the cost
of being slower end to end on a many-node cluster; concurrent fan-out
is an additive change to make later if that cost turns out to matter in
practice, the same way streaming deferred `AsyncRead`/`AsyncWrite`.

Each individual node's cursor still follows the same connection-pinning
rule streaming established: a `NodeIterator` holds one `PooledGuard`
for as long as that node's cursor is open, since `IterationNext`/
`IterationEnd` are scoped to the connection that sent `IterationStart`,
confirmed the same way as streaming's equivalent operations.

**Topology is fixed at the start of the whole iteration, not
re-checked between nodes.** `nodes_and_owned_segments` is called once,
by `RemoteCache::iter_with`, before the first node's cursor opens. If
the cluster rebalances mid-iteration, a later target in the list may no
longer be the real owner of the segments recorded for it. Accepted as a
known limitation, in the same spirit as the next point.

**No retry or failover, on either axis.** If one node's cursor fails,
or comes back `INVALID_ITERATION` partway through (for example, a
rebalance invalidated it), this client does not reroute that node's
unfinished segments to another owner the way the Java client does; the
error propagates to the caller as is, same as streaming's "no retry"
stance, and explicitly simpler than the reference client. Likewise,
`CacheIterator` never re-fetches a fresher topology if a node in
`remaining_targets` turns out to be wrong by the time it is reached.

**`IterationEnd` tolerates `INVALID_ITERATION` as success, but
`IterationNext` does not.** For `iteration_end`, the two outcomes mean
the same thing from the caller's side: the cursor is gone, which is the
only thing that call ever wanted. For `iteration_next`, the same status
mid-scan means something really went wrong with a cursor the caller
still expected to read from, so it surfaces as `Error::InvalidIteration`
instead.

**Filter/converter factory support ships now, reusing `ServerFactory`
(#4).** The request body already needs a sentinel-shaped slot for the
factory name and parameters regardless, so exposing it as a public
`IterationOptions::filter_factory` was little extra work once the
sentinel scheme was confirmed. Segment selection is not exposed the
same way: `nodes_and_owned_segments`' internal per-node segment lists
are not something a caller can override, since #53's scope is "iterate
the whole cache," not "iterate a caller-chosen subset of segments."

## Consequences

* New public API: `RemoteCache::iter`/`iter_with`, `CacheIterator`
  (`next_entry`), `IterationEntry` (`key`/`value`/`version`/`created`/
  `lifespan`/`last_used`/`max_idle`, the same shape `VersionedValue`
  has, flattened instead of nested), `IterationOptions`
  (`batch_size`/`filter_factory`). Additive: nothing existing changes
  shape.
* New `OpCode` variants for `IterationStart`/`IterationNext`/
  `IterationEnd`, needing no change to `expected_response_opcode`'s
  default rule.
* `Status` gains `is_invalid_iteration`, a new known-but-not-error
  status distinct from `is_error()`'s message-carrying family.
* New `Error::InvalidIteration` and `Error::MalformedIterationResponse`
  variants.
* `client.rs` gains `nodes_and_owned_segments`, generalizing
  `owner_addr`'s single-segment resolution to every segment at once.
* `varint.rs` gains `write_signed_vint` (ZigZag), distinct from the
  existing raw bit-reinterpretation `write_vint` already uses for a
  negative topology id; the two must not be confused, see the Context
  section above.
* `listener.rs`'s `MAX_FACTORY_PARAMS` becomes `pub(crate)`, shared
  with `IterationStart`'s factory parameters instead of duplicated.
* Not implemented, left for a later, independent, additive change if
  the sequential fan-out's end-to-end latency turns out to matter on a
  many-node cluster: concurrent per-node fan-out, matching the Java
  client's Reactive Streams approach.
* Not implemented, out of scope for #53 as written: a caller-chosen
  segment subset, query projections (depends on #50), and retrying an
  orphaned segment on another node after a mid-iteration failure.
