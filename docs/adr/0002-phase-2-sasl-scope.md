# ADR 0002: Scope phase 2 to the portable SASL mechanisms, defer GSSAPI

## Status

Accepted

## Context

ADR 0001 lists phase 2 as "broader SASL support: DIGEST-SHA-256,
SCRAM-SHA-512, GSSAPI, OAUTHBEARER." Looking at what each mechanism needs:

* DIGEST-SHA-256 and SCRAM-SHA-512 are pure Rust work. Both need hashing
  and keyed hashing (SHA-256, HMAC-SHA512, PBKDF2), which small,
  well-established crates provide with no system dependency.
* OAUTHBEARER is single-shot, like PLAIN. It only needs the client to
  accept a bearer token and format it per RFC 7628.
* GSSAPI is different in kind. It authenticates against Kerberos, which
  in practice means binding to a system GSSAPI library. That is FFI to
  code this project does not control, on a library that is not present
  on every platform. Section 2 of `CLAUDE.md` rules out unsafe FFI for
  the protocol core, and GSSAPI cannot be done without it.

Building all four together would gate DIGEST, SCRAM and OAUTHBEARER,
which are ready to implement now, behind a much larger decision about
which Kerberos binding to use and how to keep it optional per platform.

## Decision

Phase 2 covers DIGEST-SHA-256, SCRAM-SHA-512 and OAUTHBEARER. GSSAPI is
deferred to its own issue, to be scoped separately once the FFI and
platform-support question has its own Propose step.

New dependencies for this phase: `sha2`, `hmac`, `pbkdf2` for the hashing
and key derivation DIGEST and SCRAM need, and `rand`, `base64` for the
client nonce and encoding SCRAM's messages need. All five are pure Rust,
widely used, and have no system library requirement.

The authentication API stays one method per mechanism, following the
existing `authenticate_plain` shape, rather than a single entrypoint
taking a mechanism enum. Each mechanism takes structurally different
credentials (username and password for DIGEST and SCRAM, a bearer token
for OAUTHBEARER), so a shared signature would need its own enum or trait
object for little benefit at this size.

## Consequences

* `hotrod-protocol`'s dependency list grows by five pure Rust crates.
* GSSAPI needs its own issue and its own Propose step before work starts
  on it, covering the Kerberos binding choice and platform support.
* The public API gains `authenticate_digest`, `authenticate_scram` and
  `authenticate_oauthbearer`, alongside the existing
  `authenticate_plain`.
