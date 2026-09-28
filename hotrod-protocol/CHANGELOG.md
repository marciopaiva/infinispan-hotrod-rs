# Changelog

All notable changes to `hotrod-protocol` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and
versioning follows [Semantic Versioning](https://semver.org/).

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
