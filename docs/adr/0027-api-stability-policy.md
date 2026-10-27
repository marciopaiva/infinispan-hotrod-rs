# ADR 0027: API stability policy for `hotrod-protocol`

## Status

Accepted

## Context

`docs/adr/0001-mirror-java-client-scope.md` said the public API was
"not considered stable until phase 1 ships" and left what happens
after that undefined. The crate is now at version 0.7.0. Phases 1
through 5 of ADR 0001 are long done, and the crate has grown well
past that original scope: cluster topology and hash-aware routing
(ADR 0003, ADR 0005), listeners, near caching, streaming, server-side
iteration, transactions, multi-cluster failover, distributed
counters, multimaps, remote query, administration and remote task
execution. `docs/adr/0026-split-connection-and-remote-cache-by-domain.md`
just settled the module boundaries behind that API. This is a natural
point to say plainly what "stable" means from here, since CLAUDE.md
section 5 already requires stopping to ask before a breaking change
to this crate's public API, but names no definition of "breaking" to
apply that rule against.

Checked directly against the crate as it stands today: no public
type anywhere uses `#[non_exhaustive]`. Every public enum, `Error`
included, is exhaustively matchable from outside the crate. That
means, under any ordinary semver discipline, adding a new variant to
any of them is already a breaking change, not an addition. This ADR
states that plainly instead of leaving each future change to
rediscover it on its own.

## Decision

**Surface covered.** "Public API" means exactly what
`hotrod-protocol/src/lib.rs`'s `pub use` list re-exports, together
with the public methods, fields and trait implementations reachable
from those re-exported types. A `pub` item inside a private module
that `lib.rs` never re-exports is not public API: nothing outside the
crate can reach it, regardless of its own visibility keyword. The
`fuzz_internal` module (`#[cfg(feature = "fuzzing")]`,
`#[doc(hidden)]`) is explicitly excluded: it exists only for `fuzz/`'s
own targets and carries no stability guarantee, with the feature on
or off.

**Versioning.** The crate follows ordinary Cargo semver for a pre-1.0
package: a patch release (0.x.y to 0.x.(y+1)) only adds or fixes
without breaking anything; a minor release (0.x.0 to 0.(x+1).0) may
break, the same rule Cargo's own dependency resolution already
assumes for a `0.x` version requirement. Reaching 1.0, and the
stricter promises that would come with it, is not decided by this
ADR.

**What counts as breaking**, and therefore needs its own Propose and
Resolve step under CLAUDE.md section 5 before it is implemented, and
a minor version bump when it ships:

* Removing or renaming a public item: a type, function, method,
  field or re-export.
* Changing a public function's or method's signature: a parameter's
  type, a parameter's position, the return type, or a new required
  parameter.
* Adding a public field to a struct a caller can build with a struct
  literal, unless that struct is already `#[non_exhaustive]`.
* Adding a variant to a public enum, unless that enum is already
  `#[non_exhaustive]`: an exhaustive `match` outside the crate stops
  compiling otherwise.
* Narrowing what a public function accepts, or weakening a
  documented guarantee it makes (for example, a method documented to
  never panic that now can).
* Removing a trait implementation a public type previously had, or
  changing a public trait's own method signatures.

**What counts as non-breaking**, shippable in a patch or minor
release without that stop-and-ask step, though still subject to the
project's ordinary review:

* Adding a new public item that changes nothing already public.
* Adding a variant to a public enum already marked
  `#[non_exhaustive]`.
* Widening what a public function accepts, or strengthening a
  documented guarantee.
* An internal refactor, such as ADR 0026's module split, that
  touches no public signature.
* Adding a new trait implementation for an existing public type.

**`#[non_exhaustive]` going forward.** A newly introduced public enum
or struct whose set of variants or fields is plausibly going to grow,
error types and event or result enums above all, should be marked
`#[non_exhaustive]` at the point it is introduced, so a later
addition to it stays non-breaking. This ADR does not retroactively
add `#[non_exhaustive]` to anything that is public today, `Error`
included: doing so is itself a breaking change for any caller that
exhaustively matches one of these types right now, and needs its own
Propose and Resolve step, as deliberate follow-up work, not bundled
into this policy.

**Deprecation.** Pre-1.0, no deprecation window is promised. A
removal lands directly in the next minor release, documented under
the CHANGELOG's "Removed" heading, with migration guidance in that
entry whenever the replacement is not obvious from the name alone.
Whether 1.0 should promise a deprecation window first is a question
for whatever ADR proposes reaching 1.0, not this one.

**Record-keeping.** Every breaking change gets a CHANGELOG entry
under "Changed" or "Removed" naming the old and the new shape, the
version bump this policy calls for, and the Propose and Resolve step
CLAUDE.md section 5 already requires. This ADR clarifies that rule;
it does not relax it.

## Consequences

* CLAUDE.md section 5's "stop and ask before breaking the public
  API" rule now has a shared definition of "breaking" behind it,
  so a future change does not need to re-derive one from scratch.
* `Error`, and every other public enum that is exhaustive today,
  stays a real source of friction: adding a new error case remains
  breaking until a separate, explicitly confirmed follow-up marks it
  `#[non_exhaustive]`. That follow-up is flagged here, not done here.
* The crate stays on pre-1.0 semver discipline. Nothing here commits
  to a 1.0 date or to the criteria for declaring one.
