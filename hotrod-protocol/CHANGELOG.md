# Changelog

All notable changes to `hotrod-protocol` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and
versioning follows [Semantic Versioning](https://semver.org/).

## [0.4.0] - 2026-09-29

### Fixed

- `read_vint`/`read_vlong` no longer loop forever on a continuation byte
  run that never terminates. Each now stops after the number of bytes
  the encoding can ever need (5 for a vInt, 10 for a vLong, the same
  bound the Java client enforces) and returns `Error::MalformedVarint`
  instead of shifting past the target type's width, which could panic
  with overflow checks on or silently produce a wrong value in release.

### Removed

- `remove_all` on `HotRodConnection` and `HotRodCluster`. It sent opcode
  `0x45`, which does not exist in the real Hot Rod protocol: there is no
  bulk remove operation defined at all, which is also why the official
  Java client has no public `removeAll(Set)`. A real server rejects the
  request with `HotRodUnknownOperationException: Unknown operation 69`;
  this had gone unnoticed since the unit tests for it ran only against a
  fake in-process server that echoes back whatever opcode it is given. A
  caller that needs the same effect can loop over `remove` for each key,
  the same workaround the Java client itself relies on.

## [0.3.0] - 2026-09-29

Phase 3 of the roadmap, scoped down in
`docs/adr/0003-hash-aware-routing-scope.md`: cluster topology tracking
and hash-aware routing to the segment's primary owner, instead of
relying on server-side redirects.

### Added

- `HotRodCluster`: a new, additive entry point bound to one cache across
  a multi-node cluster. Exposes the same operations as
  `HotRodConnection` (`get`, `put`, `remove`, `put_if_absent`,
  `replace`, `replace_if_unmodified`, `remove_if_unmodified`,
  `get_with_version`), plus the matching `authenticate_*` methods,
  replayed automatically on every pooled connection it opens.
- Infinispan's own `MurmurHash3` variant and Hot Rod segment computation
  (`(hash & 0x7FFFFFFF) % num_segments`), cross-validated against real
  Java output. Only hash function version 3 (the current default) is
  implemented; an unrecognized version is reported as
  `Error::UnsupportedHashFunctionVersion` rather than silently guessed.
- Parsing of Hot Rod topology-aware responses (server list, hash
  function version, per-segment owners), received when a connection
  advertises `HASH_DISTRIBUTION_AWARE` client intelligence.
- A configurable timeout on `HotRodConnection` and `HotRodCluster`.
  `DEFAULT_TIMEOUT` (30 seconds) bounds the initial connect and the full
  write-then-read body of every operation and SASL round unless
  overridden with the new `connect_with_timeout` constructors. Timing
  out returns `Error::Timeout` instead of hanging forever; the
  connection may have a partial frame in flight afterward and must not
  be reused, the same hazard `Error::Io` already carries.
  `HotRodCluster` treats `Error::Timeout` the same as `Error::Io` in its
  pooled-connection retry logic. `DEFAULT_TIMEOUT` is now re-exported from
  the crate root, so a caller overriding the default can reference it
  (for example, `DEFAULT_TIMEOUT * 2`) instead of repeating the literal.
- `timeout`/`set_timeout` on `HotRodConnection` and `HotRodCluster`, so a
  caller can give a single slow operation more time, or a
  latency-sensitive one less, without reconnecting with a different
  fixed timeout. The override applies to every operation from the call
  onward, not just the next one, and on `HotRodCluster` reaches every
  already pooled connection immediately, not only ones opened later; a
  caller restoring the previous bound afterward uses the value
  `timeout()` returned beforehand.
- `contains_key`, `ping`, `size`, `clear` and `stats` on `HotRodConnection`
  and `HotRodCluster`. On `HotRodCluster`, `contains_key` routes by the
  key's segment owner like `get` and `put` already do; the other four
  have no key to route by and always go to the seed connection.
- `get_all` and `put_all` on `HotRodConnection` and `HotRodCluster`, each
  fetching or writing several keys in one request. No batch size limit is
  enforced: the caller is responsible for not handing over more entries
  than `wire::MAX_ARRAY_LEN` and the server can accept in one frame. On
  `HotRodCluster`, both always target the seed connection, the same
  routing already chosen for `size`/`clear`/`ping`/`stats`, rather than
  splitting the batch client-side by segment owner.
- `VersionedValue`, returned by `get_with_version`, now carries the entry's
  full metadata alongside the value and version: `created`, `lifespan`,
  `last_used` and `max_idle`. `lifespan` and `max_idle` reuse the existing
  `Expiration` type; `created` and `last_used` are `None` exactly when the
  corresponding side is immortal, since the server never sends a timestamp
  for a half of the entry that does not expire.

### Changed

- `HotRodConnection`'s request/response header handling now threads an
  explicit client intelligence and topology id through internally. Its
  public behavior is unchanged: it still advertises `BASIC` intelligence
  and topology id `-1`, exactly as before.
- `HotRodCluster::ensure_connection` now returns the pooled connection
  itself instead of just confirming it is there. `call` no longer looks
  it back up afterward through two `.expect("ensure_connection just
  inserted it")` calls: that invariant held only because nothing ran
  between the insert and the lookup, which was already fragile and would
  have needed re-proving against any future change to the pool. The
  connection's existence is now a fact the borrow checker enforces, not
  one a caller assumes.

### Fixed

- `HotRodCluster` no longer panics when a topology update references an
  owner index that is out of range for the server list it came with.
  `read_topology_update` now rejects such a payload with the new
  `Error::InvalidTopologyOwnerIndex` as soon as it is decoded, instead of
  letting it reach the indexing that used it.
- A declared array length (`wire::read_array`) or a declared server or
  segment count (`topology::read_topology_update`) is now rejected with
  the new `Error::DeclaredLengthTooLarge` before it is used to size an
  allocation. A corrupted or hostile value previously reached
  `Vec::with_capacity` or `vec![0u8; len]` directly; since Rust aborts the
  whole process on an allocation failure rather than raising a catchable
  panic, an oversized length could take down the embedding process, not
  just the call in progress.
- `HotRodCluster` no longer re-resolves a segment owner's hostname on
  every call. The resolved address is now cached per topology and only
  redone once a new topology update replaces it.
- `HotRodCluster` no longer keeps a pooled connection open forever once
  its node leaves the cluster. Each pooled connection now remembers the
  topology server it was opened for, and `record_topology_update` drops
  any connection whose server is missing from the latest update, without
  re-resolving any hostname to check. The seed connection is exempt: it
  is the permanent fallback every retry falls back to.
- `HotRodCluster::connect_with_timeout` now dials every seed address
  concurrently instead of one after another. With N seeds, an unreachable
  one used to add its own full `timeout` to the total before the next
  seed was even tried; now the whole call is bounded by `timeout`
  regardless of how many seeds are given. The first seed to connect wins
  and the rest are dropped mid-connect; if every seed fails, the reported
  error is the one from the seed listed first, not whichever attempt
  happened to finish last.

### Documented

- The module docs on `HotRodConnection` and `HotRodCluster` now state
  explicitly that dropping an operation's future before it resolves, for
  any reason, is the same hazard `Error::Timeout` already carries: the
  connection may have a partial frame in flight and must be reconnected,
  not reused. For `HotRodCluster` specifically, a pooled connection left
  in that state is not evicted, since `call` only does so on an
  `Error::Io` or `Error::Timeout` return value, which a dropped future
  never produces.
- `HotRodCluster`'s module and struct docs now state explicitly that
  every operation takes `&mut self`, so one instance serializes all its
  operations, even ones routed to different nodes. This is more
  restrictive than `HotRodConnection`'s own one-request-at-a-time model,
  and was previously undocumented. A caller that wants operations
  against different nodes to run concurrently needs one `HotRodCluster`
  instance per task, not one shared behind a lock.

### Tested

- `HotRodCluster::ensure_connection` replaying the seed's SASL PLAIN
  credentials onto a newly opened pooled connection, and `owner_addr`
  surfacing a typed error instead of hanging or panicking when a
  topology's owner host does not resolve. Both previously had coverage
  only through the live-server tests, which are `#[ignore]`d by default
  and need a real cluster.

## [0.2.0] - 2026-09-28

Phase 2 of the roadmap, scoped down in
`docs/adr/0002-phase-2-sasl-scope.md`: SCRAM-SHA-512, DIGEST-SHA-256
and OAUTHBEARER authentication. GSSAPI is deferred to its own issue since
it needs FFI to a system Kerberos library.

### Added

- `authenticate_scram`: SASL SCRAM-SHA-512 authentication (RFC 5802),
  including verification of the server's final signature.
- `authenticate_digest`: SASL DIGEST-SHA-256 authentication, Elytron's
  generalization of RFC 2831 DIGEST-MD5 to other hash algorithms. The
  digest-uri's server-name half is fixed to `infinispan`.
- `authenticate_oauthbearer`: SASL OAUTHBEARER authentication (RFC 7628)
  with a caller-supplied bearer token. Covered by unit tests only: live
  coverage needs a token-backed realm the CI fixture does not provide
  yet.
- `Error::MalformedChallenge`, `Error::ScramServerVerificationFailed` and
  `Error::DigestServerVerificationFailed` for the new mechanisms'
  failure modes.

### Changed

- `authenticate_plain` now runs on the same generic multi-round SASL
  driver as the new mechanisms, with no behavior change.

## [0.1.1] - 2026-09-28

### Fixed

- Included a `README.md` in the published crate package, referenced via
  `readme` in `Cargo.toml`. The 0.1.0 package had none, so crates.io
  showed no description for it.

## [0.1.0] - 2026-09-28

Phase 1 of the roadmap in `docs/adr/0001-mirror-java-client-scope.md`: a
single sequential connection to one cache, authenticated with SASL PLAIN,
supporting the core cache operations.

### Added

- Hot Rod protocol 4.1 request and response header framing.
- SASL PLAIN authentication.
- Core operations: `get`, `put`, `remove`, `put_if_absent`, `replace`,
  `replace_if_unmodified`, `remove_if_unmodified`, `get_with_version`.
- Typed errors for connection, protocol and server-reported failures.
