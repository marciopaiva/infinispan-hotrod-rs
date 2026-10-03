# ADR 0007: Near caching with no bloom filter, a hand-rolled LRU and a passthrough fail-safe

## Status

Accepted, implemented in #5.

## Context

ADR 0001 lists a near cache invalidated through listener events as
phase 5, the next item after phase 4 (#4, client listeners). The idea:
`RemoteCache::get` is a network round trip every time; a near cache
keeps a bounded, local copy of recently read entries and relies on the
listener built in #4 to learn when one of them changed somewhere else,
instead of polling or guessing a time-to-live.

The design is pinned against the Java client's own reference
implementation (`infinispan/infinispan` on GitHub), the same
discipline already used for the wire protocol itself:
`NearCacheMode.java`, `NearCacheService.java`.

Two things that implementation confirms and that shape this ADR:

* **No new wire opcode.** Near caching is just an ordinary
  `@ClientListener` (`InvalidatedNearCacheListener`), registered the
  same way #4's `AddClientListener` already works, interested in
  `Modified`, `Removed` and `Expired` but **not** `Created`: an entry
  that was just created was never in the local cache to begin with, so
  there is nothing to invalidate.
* **An optional bloom-filter optimization exists**
  (`addNearCacheListener(listener, bloomFilterBits)`), letting the
  server skip sending invalidation for keys this client never cached.
  Without it, the client gets invalidation for every key that changes
  in the whole cache, not just the ones it happens to have cached
  locally, and filters client-side; that is exactly what `listen_with`
  (#4) already does unmodified. The bloom filter only reduces network
  traffic; it changes no observable behavior.

## Options considered

**Bloom filter:**

1. **Skip it.** Register a plain listener (`listen_with`, unmodified)
   with the right interests, and invalidate whatever key each event
   names.
2. **Implement it.** Extra request parameters, a client-side
   `MurmurHash3`-based bloom filter structure, and periodic filter
   updates sent back to the server as the local cache's key set
   changes.

Option 2 is a genuine traffic optimization for a near cache backing a
large, busy remote cache, but it is additional wire surface and
client-side state for a feature this project has zero production
experience with yet. Skipping it changes no correctness property: a
near cache without the bloom filter still invalidates correctly, it
just does so over a noisier event stream.

**Bounded eviction, without a new dependency:**

1. **A crate like `lru`.** Would need the Analyze/Propose step CLAUDE.md
   section 5 requires for any new dependency, and this project has
   turned that down before for less (ADR 0002 partly rejected GSSAPI
   over a system dependency, ADR 0004 picked `rustls` over `native-tls`
   specifically to avoid a C one, ADR 0005 shipped connection pooling
   with none at all).
2. **A hand-rolled structure using only `std`.** `HashMap<Vec<u8>,
   (Vec<u8>, tick)>` for O(1) lookup by key, plus a `BTreeMap<tick,
   Vec<u8>>` recording access order, whose `pop_first` gives O(log n)
   access to the least recently used entry without an index-linked
   list (this crate is `#![forbid(unsafe_code)]`, and the usual O(1)
   LRU needs one).

Option 2 trades a true O(1) LRU for an O(log n) one, in exchange for no
new dependency and no `unsafe`. A near cache's typical size does not
make that trade costly.

**What happens when the invalidation feed dies** (no reconnection, the
decision ADR 0006 already made for `CacheListener` itself):

1. **Degrade to passthrough.** Clear the local cache and stop
   consulting it: every `get` goes to the network from then on, same
   as a plain `RemoteCache`.
2. **Keep serving the local cache as is.** Faster, but risks serving
   data that has silently drifted from the server forever, with no
   signal that invalidation stopped working.

Option 2 trades correctness for speed in a way nothing can detect or
recover from later, since there is no reconnection to begin with.

## Decision

**No bloom filter.** `RemoteCache::near_cache` registers a plain
listener via the existing `listen_with`, with interests `modified`,
`removed`, `expired` (not `created`). If this turns out to generate
too much invalidation traffic in practice, the bloom filter is a
self-contained, additive extension to propose separately; nothing
about today's design blocks it.

**A hand-rolled `LruStore`**, private to `near_cache.rs`: the
`HashMap` + `BTreeMap` structure described above. No new dependency,
no `unsafe`.

**Passthrough on a dead feed.** `NearCachedCache` tracks an `alive`
flag alongside the store. The background task reading
`CacheListener::next` clears the store and flips `alive` to `false`
the moment `next` returns `None` or an `Err` (the feed is gone for
good, per ADR 0006). From then on, `get` always goes to the network
and never repopulates the local cache; `put`/`remove`/`clear` keep
delegating to the underlying `RemoteCache` exactly as they did before,
since correctness there never depended on the local cache being
usable in the first place.

**Synchronous local invalidation on `put`, `remove` and `clear`, in
addition to the listener.** Any write, through any path (another
`NearCachedCache`, a plain `RemoteCache`, or another client entirely),
reaches the same listener and invalidates the same way eventually.
Overriding these three on `NearCachedCache` itself only closes the
window between a caller's own write and that write's own event coming
back, rather than relying on the asynchronous round trip for it.
`replace`, `put_if_absent`, the versioned operations, and
`get_all`/`put_all` are not overridden: they stay correct through the
listener alone, just without that synchronous shortcut, which is
enough for this phase. `clear` needs the override for a different
reason: the protocol does not emit a per-key event for every entry a
`clear` removes, so without it the local cache would have no way to
learn a `clear` happened at all.

**`NearCachedCache` is `Clone`, like `RemoteCache` and `HotRodClient`.**
The original decision here was the opposite: no `Clone`, caller wraps
it in an `Arc` to share it, so the type itself never has to track
"last clone dropped." Review caught why that does not actually hold:
`NearCachedCache` derefs to `RemoteCache` (see below), and
`RemoteCache` is `Clone`, so `.clone()` on a `NearCachedCache` compiled
regardless of the type's own intent, resolving through `Deref` to
`RemoteCache::clone` and silently handing back a bare `RemoteCache`,
with no local cache or listener at all, instead of a second handle
onto this one. The only way to close that off within the type system
is to give `NearCachedCache` a real `Clone` of its own, which then
wins resolution before `Deref` is ever consulted. The background
task's `JoinHandle` now lives behind `AbortOnDrop`, itself behind an
`Arc` every clone shares, so it aborts once, when the last clone (or
the original) drops, the same point a single-owner type would have
aborted at.

**Everything else reached through `Deref<Target = RemoteCache>`.**
`NearCachedCache` only has its own `get`/`put`/`remove`/`clear`;
`ping`, `size`, `stats`, `contains_key`, `replace`, and the rest come
from the wrapped `RemoteCache` unmodified.

## Consequences

* New public API: `RemoteCache::near_cache`, `NearCacheOptions`
  (`max_entries`), `NearCachedCache` (`get`, `put`, `remove`, `clear`,
  `Clone`, `Deref<Target = RemoteCache>`). Additive: nothing existing
  changes shape.
* No new dependency and no `unsafe`, consistent with every prior
  phase.
* No new wire opcode: `near_cache` is built entirely on `listen_with`
  (#4), with `listener.rs`'s `read_listener_id`/`event_frame` test
  helpers made `pub(crate)` so `near_cache.rs`'s own tests can drive a
  fake `AddClientListener` exchange the same way `listener.rs`'s do,
  instead of duplicating that wire-level setup.
* A near cache's local copy can briefly disagree with the server
  between a write by another client and that write's invalidation
  event arriving; this is near caching's ordinary, accepted trade-off
  (eventual rather than strict consistency), the same one the Java
  client's `INVALIDATED` mode accepts, not a gap this client
  introduces.
* Once the listener connection dies, a `NearCachedCache` runs forever
  as a plain passthrough to its underlying `RemoteCache`: there is no
  way to ask it to try reconnecting, matching ADR 0006's existing
  "the caller registers a new listener instead" stance, just surfaced
  here as "the caller builds a new `NearCachedCache` instead."
* Bloom-filter support (`addNearCacheListener`'s traffic optimization)
  is explicitly out of scope, left as a future, independent proposal
  if invalidation traffic turns out to matter in practice.
* No in-flight request coalescing: concurrent `get` calls that each
  miss on the same key each run their own independent network fetch,
  rather than one caller's fetch being shared by the others. Flagged
  by review as worth naming explicitly rather than leaving as an
  implicit gap: deliberately out of scope for the same reason as the
  bloom filter, a traffic optimization with its own complexity
  (tracking in-flight fetches per key, deciding how late a waiter can
  still join one), not a correctness requirement this phase needs.
* No per-entry lifespan or `max_idle`. A local hit never reaches the
  server, so an entry that is read often never refreshes its
  `max_idle` timer there either, and one past its `lifespan` can keep
  being served locally until an `Expired` event or capacity pressure
  removes it. `get_with_version`/`VersionedValue` already carry this
  metadata, so a future revision could fetch and track it instead of
  a bare value; this phase does not, and `near_cache.rs`'s module docs
  call this out directly rather than leave it to be discovered by
  surprise.
* `get` snapshots `LruStore`'s generation counter before fetching from
  the remote cache, and only inserts the result if that counter is
  still the same afterward. Caught by this PR's own review: without
  it, an invalidating event for a key not yet cached (a no-op besides
  the counter bump) could land while a `get` for that same key was
  still in flight, and the fetch would then cache a value the event
  had no way to know to invalidate, since it was never in the store to
  begin with. The same counter closes an equivalent race against
  `clear`.
* That generation counter is store-wide, not per key: an invalidation
  for any key bumps it, so a concurrent `get` miss on an unrelated key
  discards its fetch too, not just one racing an invalidation for the
  same key it is fetching. Under heavy, steady writes to a busy shared
  cache this can push the local hit rate well below the "noticeably
  lower" this ADR originally estimated, toward never successfully
  caching anything while writes keep arriving. A per-key scheme needs
  its own tombstone of "last invalidated tick" for keys no longer
  cached, so the same race does not just reopen for them; that
  tombstone set could itself be bounded with the same LRU eviction
  `LruStore` already has for cached entries, rather than growing
  without bound, so the complexity this ADR originally raised against
  a per-key scheme is a real but solvable cost, not a blocking one.
  Still deferred for this phase: v1 keeps one structure to reason
  about, not two: revisit with a bounded per-key scheme if the hit
  rate under real write-heavy load turns out to need it.
* `put`, `remove` and `clear` now invalidate their local state
  unconditionally, before inspecting whether the remote call
  succeeded, rather than only on `Ok`. Also caught by review: a write
  whose response is lost to a timeout or a connection error may still
  have landed server-side, and skipping the local invalidation in
  that case left the stale pre-write value cached with nothing left to
  correct it (the listener's own event for that write depends on the
  write having been seen by the server, which this client cannot
  distinguish from "lost entirely" by the response alone). Invalidating
  unconditionally only costs a future cache miss when the write in
  fact never landed.
* The "any write through any path still invalidates this cache"
  framing in an earlier draft of this ADR and of `near_cache.rs`'s own
  doc comments overstated `clear`: fixed to call out `clear` as the
  one exception the listener cannot help with regardless of which
  handle performs it, not just a slower path to the same guarantee.
* `NearCachedCache` is now `Clone` after all, for the reason given
  above under "Decision": confirmed with a standalone `rustc` repro
  before fixing it, since it is easy to assume a type without its own
  `Clone` simply cannot be cloned, when a blanket `Deref` to a `Clone`
  target means it can, just not usefully.
* `put`, `remove` and `clear` share one `invalidate_after` helper,
  which also closes a cancellation gap review caught: invalidating
  only after `op.await` resolved skipped it entirely if the caller's
  own future was dropped first (a `select!` losing a race, or an
  external timeout around the call), since nothing placed after an
  abandoned `.await` never runs. `invalidate_after` now invalidates
  from a guard's `Drop` instead, which still fires when the enclosing
  future (and the guard inside it) is dropped mid-poll, not only on
  normal return.
* `near_cache` itself is one more method reachable through
  `NearCachedCache`'s `Deref` to `RemoteCache`: calling it on an
  existing `NearCachedCache` builds a second, independent wrapper (its
  own listener, its own local store) rather than composing with the
  one already there. Still correct, just redundant; documented on the
  type rather than blocked, since blocking it would mean auditing
  every `RemoteCache` method reachable the same way for whether it
  "makes sense" on top of an existing wrapper, not just this one.
* `get` locks the store once on a miss (`NearCacheState::
  get_or_generation`) instead of twice (a separate `get` call followed
  by a separate `generation` call for the same logical step). Caught
  by review as an unnecessary doubling of lock/unlock overhead on
  every miss, not a correctness issue.
