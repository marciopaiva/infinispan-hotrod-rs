# Changelog

All notable changes to `hotrod-protocol` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and
versioning follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `fuzz/`: a `cargo-fuzz` dev tool (never part of the normal build,
  tests or CI) covering the wire parsers (`read_vint`/`read_vlong`,
  `read_array`/`read_string`/`read_string_map`,
  `read_topology_update`, `read_response_header`, the SCRAM/DIGEST
  challenge parsers), see `fuzz/README.md`. Adds a `fuzzing` Cargo
  feature, off by default and meant only for `fuzz/`'s own use: it
  exposes thin `pub` wrappers around a handful of otherwise
  `pub(crate)` parsers through a `#[doc(hidden)]` module, with no
  effect on the real public API when left off.
- Tracing: every dispatched operation opens a
  [`tracing`](https://docs.rs/tracing) span named `hotrod_operation`,
  at the same point statistics (below) are recorded, carrying the
  cache name and the operation's name and duration, with an error
  event on failure, never the key or value either carries, per
  `docs/adr/0010-client-statistics-and-tracing.md` (#54, part 2 of
  2). Deliberately not cross-process trace-context propagation to the
  server (what the Java client itself calls "telemetry"): that is a
  bigger, separate feature, left for its own proposal if it turns out
  to matter. No overhead without a `tracing` subscriber installed.
- Client statistics: `RemoteCache::statistics`/`reset_statistics`
  report hit/miss counts and average read/store/remove time, always
  collected with no configuration flag, per the same ADR (#54, part 1
  of 2). Instruments exactly the operations the Java client's
  `clientStatistics()` does (get, get_with_version, put,
  put_if_absent, replace, replace_if_unmodified, remove,
  remove_if_unmodified, put_all, get_all); contains_key/ping/size/
  clear/stats are not instrumented, matching the Java client there
  too. `NearCachedCache::near_cache_statistics`/
  `reset_near_cache_statistics` report a near cache's own
  hit/miss/invalidation counts and current local size, kept separate
  from the plain per-cache statistics above since a near cache is its
  own distinct wrapper (ADR 0007), unlike the Java client where near
  caching is intrinsic client configuration. `HotRodClient::
  pool_statistics` reports idle/checked-out connection counts per
  node; this one has no Java client equivalent.
- Server-side iteration: `RemoteCache::iter`/`iter_with` walk every
  entry in a cache through a server-side cursor instead of requiring
  the caller to already know every key, per
  `docs/adr/0009-server-side-iteration.md` (#53). On a cluster,
  `CacheIterator` opens one cursor per node that primary-owns at least
  one segment, in turn, so no segment is skipped; `IterationOptions`
  sets the batch size and an optional server-side filter/converter
  factory (`ServerFactory`, already used by client listeners, #4). No
  retry or failover if a node's cursor fails partway through, the same
  stance streaming (#52) already takes.
- Streaming: `RemoteCache::get_stream`/`put_stream`/
  `put_stream_if_absent`/`replace_stream_with_version` read or write a
  value in chunks instead of buffering it whole, per
  `docs/adr/0008-streaming.md` (#52). `GetStream::next_chunk` pulls
  chunks one at a time (`None` once exhausted) and also carries the
  same metadata `get_with_version` does (`version`, `created`,
  `lifespan`, `last_used`, `max_idle`); `PutStream::write_chunk`
  buffers and flushes at a caller-chosen chunk size, and nothing
  commits server-side until `finish` sends the last one. Each stream
  stays pinned to the one pooled connection that opened it for its
  whole lifetime (the server scopes it that way), with no automatic
  reconnection or failover if that connection fails midway. Dropping
  either type without an explicit `close`/`finish`/`abandon` poisons
  the connection first, so the pool never hands it to an unrelated
  caller while the server still thinks a stream is open on it.

## [0.5.0] - 2026-10-03

### Added

- Near caching: `RemoteCache::near_cache` wraps a cache with a bounded,
  listener-invalidated local cache for `get`, per
  `docs/adr/0007-near-caching.md` (#5). `NearCacheOptions` sets how many
  entries it keeps (`max_entries`, least-recently-used eviction). The
  returned `NearCachedCache` is cheap to clone and share across tasks,
  like `RemoteCache` itself: every clone shares the same local cache
  and background listener.
  `put`/`remove`/`clear` write through and invalidate the local entry
  immediately; everything else (`replace`, bulk operations, ...) is
  reached straight through to the wrapped `RemoteCache`, still kept
  correct by the same background listener, just on its ordinary
  asynchronous delay. If that listener's connection drops, the local
  cache is cleared and stops being used at all: every `get` goes to the
  network from then on rather than risk serving data no invalidation
  feed can ever correct again.
- Client listeners: `RemoteCache::listen`/`listen_with` register a
  listener on its own dedicated connection and return a `CacheListener`
  to pull `CacheEvent`s (`Created`/`Modified`/`Removed`/`Expired`/
  `Custom`) from with `next()`, per
  `docs/adr/0006-client-listeners.md` (#4). `ListenOptions` selects
  event types (`CacheEventInterests`), whether to replay the cache's
  current contents first, and an optional server-side filter or
  converter factory (`ServerFactory`) already deployed on the server;
  `hotrod-protocol` only transports the factory name and parameters, it
  never evaluates filter/converter logic itself. No automatic
  reconnection: a dropped connection ends the listener (`next` returns
  `None` or a terminal `Err`), and registering a new one is the
  caller's call to make.

### Changed

- `HotRodCluster` is replaced by `HotRodClient` and `RemoteCache`, per
  `docs/adr/0005-connection-pooling-and-client-cache-split.md` (#77).
  `HotRodCluster` kept at most one connection per node and serialized
  every operation through `&mut self`, even operations routed to
  different nodes. `HotRodClient` now pools several connections per
  node (per cache) and dispatches through `&self`, so independent
  operations run concurrently. A cache is no longer bound at connect
  time: `HotRodClient::connect(seeds)` returns a client shared across
  any number of named caches, and `client.cache("my-cache")` returns a
  `RemoteCache` handle carrying the operations `HotRodCluster` used to
  expose directly (`get`, `put`, `remove`, `ping`, `size`, ...). This is
  a breaking change with no deprecation path:
  `HotRodCluster::connect(seeds, "my-cache")` becomes
  `HotRodClient::connect(seeds).await?.cache("my-cache")`.
  `authenticate_plain`/`authenticate_scram`/`authenticate_digest`/
  `authenticate_oauthbearer` move from the cache type to `HotRodClient`,
  since credentials are replayed onto every pooled connection regardless
  of which cache a caller later opens.

## [0.4.0] - 2026-09-29

### Added

- `connect_tls`/`connect_tls_with_timeout` on `HotRodConnection` and
  `HotRodCluster`, alongside the existing plain-text constructors, per
  `docs/adr/0004-tls-support.md` (#46). The new `TlsConfig` carries the
  seed's expected `server_name`, an optional PEM `ca_certificate` (the
  OS trust store is used when absent) and an optional `client_identity`
  (PEM certificate and key) for mutual TLS. The seed connection gets full
  hostname and CA-chain verification; a node discovered later through a
  topology update is verified against the same CA chain only, since the
  topology update carries no hostname to check against, exactly as the
  ADR decided. `Error::TlsHandshake` and `Error::InvalidTlsMaterial`
  report the new failure modes instead of a generic `Error::Io`.
- `Error::BatchTooLarge` and the new `MAX_BULK_ENTRIES` constant (100,000).
  `HotRodConnection::get_all`/`put_all` now reject a batch over that size
  before writing anything, instead of building and sending a frame that
  could grow unbounded with the caller's input and that a real server was
  always going to reject once its own frame size limit kicked in.
  `HotRodCluster::get_all`/`put_all` inherit the same ceiling, since both
  delegate to the `HotRodConnection` methods.

### Fixed

- `read_vint`/`read_vlong` no longer loop forever on a continuation byte
  run that never terminates. Each now stops after the number of bytes
  the encoding can ever need (5 for a vInt, 10 for a vLong, the same
  bound the Java client enforces) and returns `Error::MalformedVarint`
  instead of shifting past the target type's width, which could panic
  with overflow checks on or silently produce a wrong value in release.
- `HotRodCluster` now fails over to the other seed addresses it was
  constructed with once the active one stops responding, instead of
  leaving `size`, `clear`, `ping`, `stats`, `get_all`, `put_all` and
  every keyed operation broken for the lifetime of the instance.
  `connect`/`connect_with_timeout` already failed over between seeds at
  bootstrap; nothing repeated that afterward until now.
- `HotRodCluster::authenticate_with` no longer panics once the seed
  connection has been evicted from the pool by an earlier
  `Error::Io`/`Error::Timeout` failure. It now reconnects the seed the
  same way `call`/`ensure_connection` already do, instead of indexing
  the pool directly and assuming the entry is still there.
- `HotRodCluster::authenticate_with` no longer replays the old
  credentials in `self.auth` onto a freshly reconnected seed before
  authenticating it with the new method. Reconnecting the evicted seed
  went through the same auto-authenticate path every other pooled
  connection uses, which applied the outdated method first: a call meant
  to refresh an expired token could fail on stale credentials before the
  new ones were ever tried, or leave the connection authenticated twice
  in a row with two different methods.
- The connection-poisoning rule the module docs on `HotRodConnection` and
  `HotRodCluster` already described is now enforced instead of merely
  documented. `HotRodConnection` marks itself poisoned before writing a
  request and clears the mark only once the response has been read in
  full; a future dropped before that point, for `Error::Timeout` or any
  other reason, leaves the mark set, and every later operation on that
  connection then fails fast with the new `Error::PoisonedConnection`
  instead of reading from a stream that may still have a partial frame
  in flight. `HotRodCluster::ensure_connection` checks a pooled
  connection's poisoned mark directly before handing it to the next
  operation, evicting and reconnecting it first if needed, rather than
  only reacting to whatever `Error::Io`/`Error::Timeout` an operation
  happens to return.

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

### Documented

- `HotRodCluster::get_all`/`put_all` now explain in their doc comments why
  always routing to the seed connection, regardless of which node owns
  each key, is a network-efficiency trade-off and not a correctness gap:
  a distributed cache node forwards a request for a key it does not own to
  the real owner over internal cluster RPC. Splitting the batch
  client-side by owner was considered and rejected for this issue, as a
  bigger change than a hardening fix warrants.

### Tested

- Live-server coverage for `contains_key`, `ping`, `size`, `clear`, `stats`,
  `get_all` and `put_all`, on both `HotRodConnection` and `HotRodCluster`.
  Every operation added since v0.3.0 previously had only unit-test coverage
  against a fake in-process server that echoes back whatever opcode it is
  given, the same gap that let the wrong `remove_all` opcode above pass
  unnoticed until a real server rejected it. `tests/live_server.rs` now
  serializes its tests behind a shared lock, since `clear` wipes the whole
  cache and would otherwise race against another test's keys under cargo's
  default parallel test execution.
- Live-server coverage for `connect_tls` against a real Infinispan server
  with TLS enabled, covering a successful `put`/`get`/`remove` round trip
  and rejection of a server certificate signed by an untrusted CA. Unlike
  the `cluster_*` tests, this fixture needs its own server, started with
  the new `ci/infinispan-tls/setup.sh` and torn down with
  `ci/infinispan-tls/teardown.sh`; like `cluster_*` at the time, it
  stayed out of every CI workflow and was meant to be run by hand (wired
  in later by #83, the same way #78 did for `cluster_*`).

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
