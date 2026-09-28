# Changelog

All notable changes to `hotrod-protocol` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and
versioning follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
  pooled-connection retry logic.

### Changed

- `HotRodConnection`'s request/response header handling now threads an
  explicit client intelligence and topology id through internally. Its
  public behavior is unchanged: it still advertises `BASIC` intelligence
  and topology id `-1`, exactly as before.

### Fixed

- `HotRodCluster` no longer panics when a topology update references an
  owner index that is out of range for the server list it came with.
  `read_topology_update` now rejects such a payload with the new
  `Error::InvalidTopologyOwnerIndex` as soon as it is decoded, instead of
  letting it reach the indexing that used it.

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
