# ADR 0018: Transactions, v1 (NON_XA, one-phase commit only)

## Status

Accepted, implemented in #47.

## Context

The last substantial item in the roadmap, issue #47 (already filed).
The issue as filed described full XA transaction support
(`PrepareTransactionOperation`, `CompleteTransactionOperation`,
`ForgetTransactionOperation`, `RecoveryOperation`). The design below
is pinned against the real Java client source
(`infinispan/infinispan`), the same discipline every prior phase
used, and corrects several details the issue got wrong.

**Five real opcodes, not four.** `COMMIT_REQUEST` (`0x3D`/`0x3E`),
`ROLLBACK_REQUEST` (`0x3F`/`0x40`), `FORGET_TX_REQUEST`
(`0x79`/`0x7A`), `FETCH_TX_RECOVERY_REQUEST` (`0x7B`/`0x7C`),
`PREPARE_TX_2_REQUEST` (`0x7D`/`0x7E`). There is no single
"complete transaction" opcode: the Java client picks `COMMIT_REQUEST`
or `ROLLBACK_REQUEST` dynamically from a local boolean once a
two-phase prepare has already happened.

**Transactionality is a per-cache server setting, on both sides.**
The server needs `<transaction mode="NONE|BATCH|NON_XA|
NON_DURABLE_XA|FULL_XA"/>` in that cache's own configuration; the
wire has no opcode to ask whether a cache is transactional, so a
client only finds out by trying a real prepare and seeing it fail.

**The Java API is real JTA**
(`jakarta.transaction.TransactionManager`, an external coordinator),
not a Hot-Rod-specific design. Rust has no equivalent, so this
crate's API is necessarily its own design, not a translation.

**Two-phase commit exists on the wire** (`onePhaseCommit` is an
explicit boolean in the prepare body), but this crate has, and will
have in this phase, no external JTA coordinator managing several
resources under one transaction. Every transaction this client can
construct involves exactly one resource (one cache), which is
precisely the case real JTA itself already optimizes to a one-phase
commit. **This collapses the v1 scope drastically**: only
`PREPARE_TX_2_REQUEST` with `onePhaseCommit=true` is needed; the
other four opcodes stay out of scope entirely.

**No connection is pinned to a transaction.** Everything before
`commit` lives in a local, in-memory map (the Java client's own
`TransactionContext`), never touching the network. `get` performs an
ordinary versioned read (for later optimistic conflict detection);
`put`/`remove` are purely local. Only `commit` sends anything, in one
batch.

**Routing**: the Java client sends `PREPARE_TX_2` to whichever node
owns the first modified key, as a latency heuristic the client code's
own comment is visibly unsure about, not a protocol requirement. The
server resolves multi-key, multi-owner transactions internally
either way.

**Wire format**, confirmed field by field against
`PrepareTransactionOperation`/`Modification` on the Java side:

* Request: `Xid` (`formatId` as a signed vInt, then two 32-byte
  Hot-Rod arrays: `globalTransactionId`, `branchQualifier`),
  `onePhaseCommit` (byte, `1`), `recoverable` (byte, `0`), `timeoutMs`
  as a raw big-endian `i64` (not a vLong), a vInt modification count,
  then each modification: key, one control byte, the key's version
  as a raw big-endian `u64` only when it was read first and existed,
  and, unless the modification is a removal, the usual
  expiration-tagged value.
* Control byte, confirmed as a bitmask combined with `|`:
  `NOT_READ = 0x1` (never read in this transaction), `NON_EXISTING =
  0x2` (read and found absent), `REMOVE_OP = 0x4`. `NOT_READ` and
  `NON_EXISTING` are mutually exclusive in practice, never both set.
* Response: the ordinary Hot Rod status byte is almost always
  success; the real outcome is the signed 4-byte `i32` XA result code
  that follows it (`XA_OK = 0`, `XA_RDONLY = 3`, `XA_RBROLLBACK =
  100`, `XAER_RMERR = -3`). The one exception is status
  `NOT_PUT_REMOVED_REPLACED` (`0x01`), which carries no body at all:
  a concurrent prepare for the same transaction id conflicted with
  this one.

**Maturity on the Java side is real**, with a substantial test suite
(`client/hotrod-client/src/test/java/.../tx/`) against a real
cluster, not an experimental feature.

## Decision

**V1 covers only `NON_XA`, always one-phase commit, no recovery.**
Real XA (`NON_DURABLE_XA`/`FULL_XA`, with `XAResource`/recovery/
`FORGET_TX`/`FETCH_TX_RECOVERY`) is deferred to a later phase with its
own issue and ADR; the research above is the starting point for it.
One-phase commit is always applicable here, never a loss of
generality against this phase's own goal, since this crate has no
external multi-resource JTA coordinator to need the two-phase form
for.

## Design

### Public API (`transaction.rs`, `remote_cache.rs`)

```rust
impl RemoteCache {
    pub fn begin_transaction(&self) -> Transaction;
}

pub struct Transaction { /* ... */ }

impl Transaction {
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub fn put(&mut self, key: &[u8], value: &[u8], lifespan: Expiration, max_idle: Expiration);
    pub fn remove(&mut self, key: &[u8]);
    pub async fn commit(self) -> Result<()>;
    pub fn rollback(self);
}
```

`Transaction` owns a `RemoteCache` by value (cheap to clone, the same
pattern `MultimapCache`/`StrongCounter` already use), not a borrow:
there is no reason to tie a transaction's lifetime to the handle that
started it.

`get` checks this transaction's own pending writes first
(read-your-own-writes, no network call for a key already touched
locally); otherwise it reads the server via the existing
`get_with_version`, recording either the version seen or that the key
did not exist, for `commit` to use later. `put`/`remove` only update
local state. `rollback` discards that state and returns; dropping a
`Transaction` without calling either has the exact same effect, since
nothing was ever sent to the server before `commit` in a one-phase
design.

`commit` generates a fresh `Xid`, builds one `Modification` per
pending write (version and `not_read` derived from what `get`
recorded, or `NOT_READ` if the key was never read in this
transaction), and sends them all in one `PREPARE_TX_2` request.
`Ok(())` means every write committed; any `Err` means none of them
did. The wire does not say which individual write conflicted, so the
usual response to a rolled-back transaction is to retry the whole
thing from scratch, not to inspect the failure further.

Routing: `run_prepare_transaction` always goes to the seed, like
`query`/`size`/`clear`, rather than the first-modified-key owner the
Java client favors. That heuristic is a latency optimization, not a
protocol requirement, and this crate already made the equivalent
simplification for counters (ADR 0015). It can be revisited if
first-key routing turns out to matter for real workloads.

### `Xid` stability across retries

A transaction's `Xid` is generated once, in `Transaction::commit`,
not inside the retried low-level operation: `RemoteCache::call_seed`
retries a failed attempt against another candidate node on a
transport error, and a fresh `Xid` per attempt would let an earlier,
seemingly-failed attempt that actually succeeded server-side collide
with a second, differently-identified attempt instead of being
recognized as the same transaction. `Operation::PrepareTransaction`
therefore carries the `Xid` as part of its own owned data
(`Xid, Vec<Modification>`), the same discipline every other retried
operation already follows for its arguments, and
`connection.rs::prepare_transaction_one_phase` takes the `Xid` as a
parameter instead of generating one itself.

### Protocol (`header.rs`, `connection.rs`)

Only `OpCode::PrepareTx = 0x7D` (response `0x7E`) in this phase.
`HotRodConnection::prepare_transaction_one_phase` serializes the
`Xid`, the fixed flags (`onePhaseCommit = true`, `recoverable =
false`), the timeout and the modification list, reads back the
status, maps `is_not_executed()` (covering `NOT_PUT_REMOVED_REPLACED`
with no body) to `Error::TransactionRolledBack`, and otherwise reads
the 4-byte XA result code and maps it with
`transaction::decode_prepare_response`: `XA_OK`/`XA_RDONLY` succeed,
`XA_RBROLLBACK` is `Error::TransactionRolledBack`, anything else is
`Error::TransactionCommitFailed`.

The `Xid` itself is generated internally
(`format_id`/`global_transaction_id`/`branch_qualifier`, the latter
two 32 random bytes each) and never exposed on the public API: there
is no external coordinator in this phase that would need a real one
passed in.

### Test fixture

`ci/infinispan/infinispan.xml` gained `transactional-test`, a
`local-cache` with `<transaction mode="NON_XA"/>`, reusing the
existing single-node fixture rather than needing a dedicated one the
way multi-cluster failover (ADR 0017) did.

## Consequences

* New public API: `RemoteCache::begin_transaction`, `Transaction`
  (`get`/`put`/`remove`/`commit`/`rollback`), `Error::
  TransactionRolledBack`, `Error::TransactionCommitFailed`. Additive:
  nothing existing changes shape.
* Confirmed against a fake server: the exact `PREPARE_TX_2` request
  body (Xid, flags, timeout, one modification with its control byte
  and expiration-tagged value) and both response shapes (a
  successful XA result, and the no-body `NOT_PUT_REMOVED_REPLACED`
  status).
* Confirmed with unit tests that read-your-own-writes never touches
  the network, and that `rollback` never does either.
* Confirmed against a real server (`ci/infinispan/infinispan.xml`'s
  `transactional-test` cache): commit persists a write, rollback
  discards one, and a genuine write-skew conflict between two
  transactions reading and then writing the same key rolls the
  second commit back with `Error::TransactionRolledBack`.
* **Not implemented, deferred to a later phase if it turns out to
  matter**: real XA (`NON_DURABLE_XA`/`FULL_XA`, `XAResource`,
  recovery via `FETCH_TX_RECOVERY_REQUEST`/`FORGET_TX_REQUEST`, a
  genuine two-phase commit via separate `COMMIT_REQUEST`/
  `ROLLBACK_REQUEST` calls); hash-routing the prepare by the first
  modified key's owner instead of always using the seed; any
  integration with an external transaction coordinator (no JTA
  equivalent exists in Rust; a real need to coordinate this cache
  with another transactional resource would be a new proposal, not a
  natural extension of this one).

## Addendum: pinning a transaction to its starting cluster

This ADR never considered multi-cluster failover
(`docs/adr/0017-multi-cluster-failover.md`), which did not exist yet
when it was written. Once both features coexisted, a gap surfaced on
review: `Transaction::get` and `Transaction::commit` each resolved
`HotRodClient`'s *currently* active cluster fresh, independently, on
every call. If an automatic failover switched the active cluster
while a transaction was in flight, `commit` could send the final
`PREPARE_TX_2`, carrying versions read from the old cluster, to
whatever cluster happened to be active by the time `commit` ran. That
request is meaningless against a cluster that never saw the reads it
claims to validate: it could "succeed" by coincidence, or fail with
the same generic `Error::TransactionRolledBack`/
`Error::TransactionCommitFailed` an ordinary optimistic-lock conflict
produces, with no way for the caller to tell the two apart.

The fix: a `Transaction` now captures the active cluster's name and
`ClientInner::cluster_generation` together, once, at
`begin_transaction` time (`HotRodClient::
active_cluster_name_and_generation`, read under one acquisition of
the same lock `active_seed_addr_and_generation` already uses for its
own race-free pairing). Every `get` and the final `commit` check that
generation again before doing anything else. Unchanged, the
transaction proceeds exactly as before: this adds no cost to the
common case, since a transaction's whole lifetime is normally far
shorter than the interval between failovers. Changed, the operation
fails immediately with the new `Error::TransactionClusterChanged`,
without attempting any network call at all.

This alone would not be enough: `RemoteCache::dispatch`'s own retry
chain can itself cross from the active cluster to a different
configured one as a last resort
(`HotRodClient::try_failover_to_live_cluster`), once every candidate
of the current cluster is exhausted. A transaction whose own pinned
cluster has genuinely gone down must not let that last resort pull it
onto a cluster its reads never touched either. `dispatch` gained a
`cross_cluster_failover` flag; the two new pinned call paths
(`call_pinned`/`call_seed_pinned`, used only by
`Transaction::get`/`commit`) pass `false`, so a pinned operation
exhausts only its own cluster's candidates and surfaces that
cluster's own connection error instead of silently landing anywhere
else. `call`/`call_seed`, used by everything other than
`Transaction`, are unaffected: they keep crossing clusters exactly as
`docs/adr/0017-multi-cluster-failover.md` already decided.

Additive only: `Error::TransactionClusterChanged { pinned, current }`
is a new variant; nothing existing changes shape. No live-server test
was added, since nothing here needs a real server beyond what the new
fake-server tests already exercise (`remote_cache.rs`:
`transaction_commit_does_not_fail_over_to_another_cluster_when_pinned`,
`transaction_get_fails_fast_when_the_active_cluster_already_changed`,
`transaction_commit_fails_fast_when_the_active_cluster_changed_since_
begin`), and the existing live
`transaction_commit_and_rollback_against_a_real_server` test only
ever runs against a single cluster.
