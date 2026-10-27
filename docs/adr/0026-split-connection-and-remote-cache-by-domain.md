# ADR 0026: Split `connection.rs`/`remote_cache.rs` by domain

## Status

Accepted. Phase 1 (multimap) implemented alongside this ADR; phases 2
(counters) and 3 (streaming) implemented as follow-up PRs. Later
phases are sequenced below but each needs its own confirmation before
it starts.

## Context

Two independent external reviews of the public repository, read
separately in this project's working sessions, both named the same
two files as the project's biggest remaining maintainability risk:
`hotrod-protocol/src/connection.rs` (170029 bytes, 4099 lines) and
`hotrod-protocol/src/remote_cache.rs` (164593 bytes, 3841 lines). Both
reviews' byte counts were confirmed accurate against the real files.
Independently of that, this project's own recent history shows the
same symptom from the inside: near caching, multimap dispatch
coverage, transaction cluster pinning and two new live tests all
touched one or both of these files in the same few weeks, regardless
of how unrelated the features were. A file that every change has to
pass through, no matter the feature, costs more to review and to
reason about than the sum of what is actually in it.

Confirmed directly in the code before any change:

* `connection.rs` has exactly one `impl HotRodConnection { ... }`
  block (lines 170–1974, 73 methods), followed by
  `#[cfg(test)] mod tests` (2056–4099, 2044 lines, almost exactly half
  the file). The struct's fields (`stream`, `cache_name`,
  `next_message_id`, `intelligence`, `topology_id`,
  `pending_topology_update`, `timeout`, `poisoned`) carry no explicit
  visibility modifier: private to the `connection` module and, by
  Rust's own privacy rule, to every descendant module of it too.
  A handful of private free functions (`key_value_body`,
  `versioned_result`, `read_entry_metadata`/`read_versioned_value`)
  are genuinely used across more than one domain (core, streaming,
  multimap); a few more (`read_array_collection`, `read_multimap_bool`,
  `write_segment_bitset`) are used by exactly one domain each.
  `with_timeout` is the one helper also reused outside this file
  (`listener.rs`, `counter_listener.rs`), so it stays `pub(crate)`.
* `remote_cache.rs` has `Operation`/`OperationResult` (two enums, one
  per operation that can be replayed on retry) plus the free function
  `run_operation` (a 31-arm match dispatching each `Operation` variant
  to its `HotRodConnection` method), none of which need any
  `RemoteCache` field access at all. Then one
  `impl RemoteCache { ... }` block (311–1464), already grouped by
  domain in practice (core, bulk, listeners, near cache, typed, query,
  transactions, task execution, counters, multimap, streaming,
  iteration, statistics), plus a ~330-line dispatch/retry group
  (`call`/`call_seed`/`call_pinned`/`call_seed_pinned`/
  `check_pinned_cluster`/`dispatch`/`run_and_record`/`record_stats`/
  `active_seed_addr`/`active_cluster_name_and_generation`, and the
  free functions `filter_quarantined`/`exhausted_candidates_error`)
  that every domain's methods call into. Then
  `#[cfg(test)] pub(crate) mod tests` (1506–3841, 2336 lines, 61% of
  the file), which also hosts three helpers (`client_with_seeds`,
  `client_with_seeds_and_timeout`, `set_topology`) imported by seven
  other files' own test modules via the exact path
  `crate::remote_cache::tests::{...}`.

## Decision

**Split both files into domain submodules, in place, with no public
path or visibility change.** Rust resolves `mod foo;` declared inside
`src/connection.rs` to `src/connection/foo.rs` (the 2018-edition flat
layout; no `mod.rs` rename needed, so `connection.rs` keeps existing
exactly where it is and `crate::connection::HotRodConnection`'s path
never changes). Rust's privacy rule, "a private item is visible in
its defining module and every descendant of that module", means an
`impl HotRodConnection { ... }` block written inside the new
`connection/multimap.rs` can call `self.stream`,
`self.write_and_read_header(...)`, or any other private field or
method still defined at `connection.rs`'s root, with nothing anywhere
changed from private to `pub(crate)`. The identical reasoning applies
to `remote_cache.rs` and its `call`/`call_seed`/`dispatch` group. No
two domain submodules ever need to see each other directly, because
every helper already confirmed to be shared across more than one
domain stays at the owning file's root, where every descendant can
already reach it. This is the same mechanism, one level deeper, that
already lets `php-ext` depend on `hotrod-protocol` without either
side's internals leaking the wrong way: visibility only ever widens
going down a module tree, never sideways.

Net effect: pure code motion. No behavior change, no new
`pub(crate)` surface beyond what already existed, no change to
`lib.rs`'s re-exports. Each phase's diff should read as "this exact
block of lines moved from file A to file B, verbatim".

**Target layout.**

`connection.rs` keeps: the struct definition, `VersionedResult`/
`VersionedValue` (too widely imported elsewhere, via `lib.rs`'s
re-export, to be worth relocating for a marginal size gain),
lifecycle (`connect*`, `is_poisoned`/`mark_poisoned`/
`begin_operation`/`end_operation`, `take_pending_topology_update`,
`into_transport`, `timeout`/`set_timeout`), the framing core
(`write_and_read_header`), and the confirmed cross-domain helpers
(`with_timeout`, `key_value_body`, `versioned_result`,
`read_entry_metadata`, `read_versioned_value`). Everything else moves
to `connection/<domain>.rs`, each with its own
`impl HotRodConnection` block: `auth.rs` (SASL), `core_ops.rs`
(get/put/remove/replace/contains_key/ping/size/clear/stats),
`listeners.rs` (`add_client_listener`/`counter_add_listener`),
`query.rs`, `tasks.rs` (`exec_request`/`execute_task`/
`execute_cache_task`), `counters.rs`, `multimap.rs` (plus its own
private `read_array_collection`/`read_multimap_bool`, used nowhere
else), `transactions.rs`, `bulk.rs`, `streaming.rs` (plus
`StreamStart`), `iteration.rs` (plus `IterationBatch` and
`write_segment_bitset`).

`remote_cache.rs` keeps: the `RemoteCache` struct, `new`, and the
whole dispatch/retry group. `Operation`/`OperationResult`/
`run_operation` move to `remote_cache/operation.rs`. Everything else
moves to `remote_cache/<domain>.rs`: `core_ops.rs`, `bulk.rs`,
`listeners.rs`, `query.rs`, `transactions.rs` (plus
`get_with_version_pinned`), `tasks.rs`, `counters.rs`, `multimap.rs`,
`streaming.rs`, `iteration.rs`, `facades.rs` (the handful of one-line
synchronous delegates: `near_cache`, `typed`, `statistics`,
`reset_statistics`).

**Tests, in both files**, follow the same production split:
`#[cfg(test)] mod tests` becomes `<file>/tests/mod.rs`, with a
`support.rs` holding exactly the helpers already confirmed shared
across files (`client_with_seeds`/`client_with_seeds_and_timeout`/
`set_topology` for `remote_cache`), re-exported with
`pub(crate) use support::*;` at the `tests` module root so every
external `crate::remote_cache::tests::{...}` import keeps resolving
unchanged. Each domain's tests move into `tests/<domain>.rs` next to
its production code, since both files' tests are already contiguous
and ordered by domain today.

**Rollout: one domain per PR, not one 300 KB diff.** Each phase gets
its own branch, PR, full CI run and explicit confirmation before the
next phase starts, the same branch+PR+CI rhythm every other change in
this project already follows. Phase 1 (this ADR's own implementation)
is multimap: the most self-contained domain in both files, with
dedicated fake-server tests already contiguous in both files and its
own live test, and the domain most recently touched (multimap
dispatch coverage, a prior change), so its exact current shape is
well understood and easy to verify line-for-line during the move.
Later phases (counters, streaming, iteration, transactions, bulk,
query + tasks, auth + listeners) are sequenced smallest/most
self-contained first, leaving the dispatch/retry core and plain
`core_ops` for last since they are the largest and most cross-cutting;
none of them start without asking first.

## Consequences

* No public API change: every type's path, every method's visibility,
  every re-export in `lib.rs` stays exactly as it is today. This is
  refactor-only, confirmed by construction (the mechanism relies on
  Rust's own privacy rules, not on relaxing any visibility) rather
  than by exhaustively diffing every signature after the fact.
* `connection.rs`/`remote_cache.rs` shrink to the struct definition,
  the genuinely cross-cutting helpers, and (for `remote_cache.rs`)
  the dispatch/retry core: the part of each file every domain
  actually depends on, with nothing domain-specific left to compete
  for review attention.
* Ten-plus new files appear under `connection/` and `remote_cache/`
  over the full rollout, each scoped to one domain, each small enough
  to read in one sitting. `git blame`/`git log -- <path>` on a domain
  file now only shows that domain's own history, not everything else
  that happened to land in the same monolith.
* Each phase is reviewed as its own small PR; a mistake in one phase
  (a helper moved to the wrong place, a test accidentally dropped) is
  caught by that phase's own CI run before the next phase begins,
  rather than discovered deep inside one enormous diff.
* Not addressed by this ADR: any actual behavior change, new feature,
  or API stability policy (a separate, smaller follow-up the same
  external reviews also raised). This is structure only.
