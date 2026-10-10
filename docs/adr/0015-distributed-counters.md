# ADR 0015: Distributed counters

## Status

Accepted, implemented in #49.

## Context

The second of three remaining items in the "Specialized data
structures and administration" roadmap theme, issue #49 (already
filed): strong (atomic, optionally bounded) and weak (non-atomic,
cheaper) distributed counters.

The design is pinned against the real Java client and server source
(`infinispan/infinispan`), the same discipline every prior phase
used, across several focused research rounds (the wire format has
more independent small decisions packed into it than any phase since
remote query), plus live-server experimentation that corrected a
wrong assumption about what `remove` actually does (see "Corrected
mid-flight" below).

**12 opcodes, all against one fixed cache name.** Confirmed in
`HotRodConstants.java` on both sides: `COUNTER_CREATE` (`0x4B`/`0x4C`),
`COUNTER_GET_CONFIGURATION` (`0x4D`/`0x4E`), `COUNTER_IS_DEFINED`
(`0x4F`/`0x51`), `COUNTER_ADD_AND_GET` (`0x52`/`0x53`), `COUNTER_RESET`
(`0x54`/`0x55`), `COUNTER_GET` (`0x56`/`0x57`), `COUNTER_CAS`
(`0x58`/`0x59`), `COUNTER_REMOVE` (`0x5E`/`0x5F`), `COUNTER_GET_NAMES`
(`0x64`/`0x65`), `COUNTER_GET_AND_SET` (`0x7F`/`0x80`). Two more,
`COUNTER_ADD_LISTENER`/`COUNTER_REMOVE_LISTENER` (`0x5A`/`0x5B`,
`0x5C`/`0x5D`), are deliberately out of scope (see below). Every one
of these, including the "global" `COUNTER_GET_NAMES`, declares
`org.infinispan.COUNTER` as its cache name in the ordinary Hot Rod
header, confirmed against the Java client's own
`CounterOperationFactory`: unlike remote administration (ADR 0014),
this is not a cache-less mechanism.

**`COUNTER_IS_DEFINED`'s response opcode is `request + 2`, not `+1`.**
`0x50`, between `0x4F` and `0x51`, is reserved in `HotRodConstants.java`
for a historical `ERROR_RESPONSE` opcode that nothing actually sends
(errors are reported through a response's status byte, not a
dedicated opcode); this just leaves a gap in the numbering.

**The configuration encoding is one combined field, not independent
bits.** `CounterEncodeUtil.java`'s flags byte packs the counter's kind
into bits 0-1 as a single two-value field (`0b00` unbounded strong,
`0b01` weak, `0b10` bounded strong; `0b11` is rejected server-side),
with bit 2 (`0x04`) independently marking persistent vs. volatile
storage. After the flags byte: `concurrencyLevel` (vInt) for weak,
`lowerBound`+`upperBound` (two longs) for bounded strong, nothing for
unbounded strong; always a trailing `initialValue` (one long).
`COUNTER_CREATE`'s request body and `COUNTER_GET_CONFIGURATION`'s
response body share this exact encoding (confirmed: one is the
matched pair of the other in `CounterEncodeUtil`).

**Status bytes carry real information beyond success/failure, and do
so inconsistently across operations** (all confirmed against the
server's own `CounterRequestProcessor`/`BaseCounterOperation`, not
inferred from the client alone):

* `COUNTER_CREATE` carries no response body either way:
  `NO_ERROR_STATUS` means created now, any other non-error status
  (in practice `SUCCESS_WITH_PREVIOUS`) means it was already defined,
  which is not an error. The Java client's own `defineCounter` derives
  its `boolean` purely from this distinction.
* `COUNTER_IS_DEFINED` carries no response body either way:
  `NO_ERROR_STATUS` means defined, a confirmed
  `OperationNotExecuted` (`0x01`) means not defined. This is *not*
  `KeyDoesNotExist` (`0x02`), unlike every other operation below:
  `is_defined` on a counter that was never created does not look like
  a missing entry to this one operation.
* `COUNTER_ADD_AND_GET`/`COUNTER_CAS`/`COUNTER_GET_AND_SET`, on a
  bounded strong counter, return `NOT_EXECUTED_WITH_PREVIOUS` (`0x04`)
  with an **empty** body when the update would move the counter past
  its bound: confirmed on the server side (`emptyResponse`), not just
  inferred from the client skipping a read after checking the status.
  The counter's value is left unchanged.
* `COUNTER_GET`/`COUNTER_GET_CONFIGURATION`/`COUNTER_RESET`/
  `COUNTER_REMOVE` return `KeyDoesNotExist` (`0x02`), silent and
  message-less, for a counter that was never defined
  (`missingCounterResponse`). The Java client's own handling of this
  is inconsistent across these four: `GetValueOperation`/
  `ResetOperation`/`RemoveOperation` turn it into a thrown
  `CounterException`, while `GetConfigurationOperation` returns `null`
  silently. This crate makes its own, consistent choice instead (see
  Decision).
* `COUNTER_CAS` returns the counter's **previous** value, not a
  boolean: success is `previous == expect`, computed client-side,
  exactly as the Java client does.

**`StrongCounter`/`WeakCounter` are an API-level distinction, not a
protocol one.** The same `COUNTER_ADD_AND_GET` opcode backs both
`StrongCounter::add_and_get` and `WeakCounter::add` (the Java client's
own `WeakCounterImpl.add` calls the identical operation and discards
the result); CAS and `get_and_set` are simply never instantiated by
`WeakCounterImpl`. Nothing confirmed in this research shows the
*server* rejecting a CAS against a weak counter; the restriction is
Java-client-only, as far as this phase's research could tell.

### Corrected mid-flight: `remove` does not undefine a counter

The initial research, reading `StrongCounter.remove`'s own javadoc
("it removes this counter from the cluster"), assumed `remove` would
make a counter behave as never-defined afterward: `is_defined` false,
further reads failing. Live-server testing (`ci/infinispan`,
Infinispan 15.1) disproved this: right after `remove`, `is_defined`
still reports `true`, and a read returns the counter's configured
initial value again, not an error. `reset` and `remove` are
observably the same thing from this client's side: both clear the
counter back to its initial value, neither erases its definition.
Nothing in the twelve opcodes this phase covers undefines a counter
once created; `CounterManager` has no `remove` of its own distinct
from `StrongCounter`/`WeakCounter::remove` in this phase's scope, and
investigating whether one exists at a different opcode (the Java
client's `CounterManager.remove(String)` javadoc describes a stronger
"removes the counter from the cluster, invalidates every live
instance" operation that may or may not be the exact same wire call)
is left for later if it turns out to matter.

## Decision

**No counter listeners in v1.** Full CRUD (create, read,
add/increment, CAS, reset) for both counter types, without
`COUNTER_ADD_LISTENER`/`COUNTER_REMOVE_LISTENER` or value-change
events. The same cut #50 already made for streaming large query
result sets: the core first, an extension later if it turns out to
matter.

**Every operation goes to the seed, not hash-routed by counter
name**, even though the Java client's own `CompareAndSwapOperation`
and `StrongCounterImpl` *do* hash-route by counter name (confirmed:
`useConsistentHash()` returns `true` for strong counter reads/writes,
`false` for weak counters and the "global" operations). This is a
deliberate simplification, not an oversight: correctness does not
depend on hitting the owning node directly (any node can still
process the request; it is a latency optimization, the same role
topology-aware routing already plays for the main cache API), and
adding it here would mean computing a counter name's owning segment
through the exact same hash machinery `RemoteCache::call` already
uses, for a feature whose v1 scope is already large. Left as a known
gap, not solved by this phase; see Consequences.

**Counter-not-found is normalized into one consistent error**,
instead of replicating the Java client's own split (an exception for
three operations, a silent `null` for a fourth): `Error::CounterNotFound`
for `get_value`/`add_and_get`/`compare_and_swap`/`get_and_set`/
`reset`/`remove` (confirmed to need the same handling, by the shared
`missingCounterResponse` mechanism these all go through server-side),
but `get_configuration` keeps returning `Ok(None)`, the same shape
every other "might not exist" read in this crate already uses.

## Design

### `counter.rs` (new)

```rust
pub enum Storage { Volatile, Persistent }

pub enum CounterType {
    Weak { concurrency_level: u32 },
    BoundedStrong { lower_bound: i64, upper_bound: i64 },
    UnboundedStrong,
}

pub struct CounterConfiguration {
    pub counter_type: CounterType,
    pub initial_value: i64,
    pub storage: Storage,
}

pub struct CounterManager<'a> { /* client */ }

impl HotRodClient {
    pub fn counters(&self) -> CounterManager<'_>;
}

impl<'a> CounterManager<'a> {
    pub async fn define(&self, name: &str, config: CounterConfiguration) -> Result<bool>;
    pub async fn is_defined(&self, name: &str) -> Result<bool>;
    pub async fn get_configuration(&self, name: &str) -> Result<Option<CounterConfiguration>>;
    pub async fn names(&self) -> Result<Vec<String>>;
    pub fn strong_counter(&self, name: impl Into<String>) -> StrongCounter;
    pub fn weak_counter(&self, name: impl Into<String>) -> WeakCounter;
}

pub struct StrongCounter { /* cache, name */ }
impl StrongCounter {
    pub async fn get_value(&self) -> Result<i64>;
    pub async fn add_and_get(&self, delta: i64) -> Result<i64>;
    pub async fn increment_and_get(&self) -> Result<i64>;
    pub async fn decrement_and_get(&self) -> Result<i64>;
    pub async fn compare_and_swap(&self, expect: i64, update: i64) -> Result<i64>; // previous value
    pub async fn compare_and_set(&self, expect: i64, update: i64) -> Result<bool>;
    pub async fn get_and_set(&self, value: i64) -> Result<i64>; // previous value
    pub async fn reset(&self) -> Result<()>;
    pub async fn remove(&self) -> Result<()>; // clears the value, see above
}

pub struct WeakCounter { /* cache, name */ }
impl WeakCounter {
    pub async fn get_value(&self) -> Result<i64>;
    pub async fn add(&self, delta: i64) -> Result<()>;
    pub async fn increment(&self) -> Result<()>;
    pub async fn decrement(&self) -> Result<()>;
    pub async fn reset(&self) -> Result<()>;
    pub async fn remove(&self) -> Result<()>;
}
```

`counter.rs` also owns `encode_configuration`/`decode_configuration`,
the shared `CounterCreate`/`CounterGetConfiguration` wire shape, the
same way `query.rs` owns `QueryRequest`/`QueryResponse`'s encoding
rather than `connection.rs`.

### Protocol wiring

`header.rs` gains the ten opcodes above, plus a special case in
`expected_response_opcode` for `CounterIsDefined`'s `+2`.
`connection.rs` gains one method per opcode (`counter_define`,
`counter_is_defined`, `counter_get_configuration`, `counter_names`,
`counter_get`, `counter_add_and_get`, `counter_compare_and_swap`,
`counter_get_and_set`, `counter_reset`, `counter_remove`), each
against `self.cache_name` directly: by the time one of these runs,
that is already `org.infinispan.COUNTER`, set when `CounterManager`
obtained its `RemoteCache` via `client.cache(COUNTER_CACHE_NAME)` (no
`execute_task`-style override needed, unlike ADR 0014's `Exec`, since
counters are not cache-less). `remote_cache.rs` gains one `Operation`
variant and one `pub(crate)` dispatch method per opcode, all routed
through `call_seed` (no key to route by, per the seed-only decision
above), mirroring `run_exec`/`run_query`'s existing shape.

### Errors

`Error::MalformedCounterConfiguration(String)` for a flags byte whose
type bits decode to the reserved `0b11`, or any other configuration
this client cannot interpret. `Error::CounterOutOfBounds`, inferred
from `NOT_EXECUTED_WITH_PREVIOUS` on the three bounded-strong writes.
`Error::CounterNotFound(String)`, inferred from `KeyDoesNotExist` on
the six operations that need a counter to already exist (see
Decision). `status.rs` gains `Status::has_previous`, filling in a
predicate its own module doc already mentioned but never
implemented, used by `counter_define` to tell "created now" from
"already defined."

## Consequences

* New public API: `HotRodClient::counters`, `CounterManager`,
  `CounterConfiguration`, `CounterType`, `Storage`, `StrongCounter`,
  `WeakCounter`, `Error::MalformedCounterConfiguration`,
  `Error::CounterOutOfBounds`, `Error::CounterNotFound`. Additive:
  nothing existing changes shape.
* Confirmed against a live server (`ci/infinispan`): both counter
  types round-trip define/is_defined/get_configuration/names, every
  strong-counter write (including a real out-of-bounds rejection on a
  bounded counter), reset and remove (confirmed to clear the value,
  not the definition, correcting the initial research).
* **Not hash-routed.** Every counter operation goes to the seed,
  unlike the Java client's own hash-aware routing for strong counter
  reads and writes. Correctness is unaffected; a cluster deployment
  pays an extra internal hop the Java client would not, left as a
  possible future optimization rather than solved here.
* Not implemented, left for later if it turns out to matter: counter
  listeners (value-change events), and whatever distinction (if any)
  Java's `CounterManager.remove(String)` has from
  `StrongCounter`/`WeakCounter::remove`, now that live testing shows
  the latter does not undefine a counter.
