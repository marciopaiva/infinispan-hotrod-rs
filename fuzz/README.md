# Fuzzing

Fuzzes `hotrod-protocol`'s wire parsers directly, bypassing the
network entirely: each target feeds raw bytes straight into a parser
that would otherwise only ever see them after a real TCP read. Dev
tool, not part of the normal build or CI (see
`docs/adr/0010-client-statistics-and-tracing.md`'s neighbor, the
roadmap entry this implements, for why): running `cargo build`/
`test`/`clippy` at the repository root never touches this directory
or its dependencies.

## Prerequisites

* A nightly toolchain, only to *run* `cargo-fuzz` (libFuzzer's
  sanitizer instrumentation needs unstable compiler flags). The main
  crate itself stays on stable; nothing here changes that.
  ```
  rustup toolchain install nightly
  ```
* `cargo-fuzz`:
  ```
  cargo install cargo-fuzz
  ```

## Running a target

From the repository root:

```bash
cargo +nightly fuzz run varint
cargo +nightly fuzz run array_and_string
cargo +nightly fuzz run topology_update
cargo +nightly fuzz run response_header
cargo +nightly fuzz run sasl_challenge
```

Each runs until stopped (`Ctrl-C`) or until it finds a crash. Bound a
session instead with `-- -max_total_time=<seconds>`, e.g.:

```bash
cargo +nightly fuzz run varint -- -max_total_time=60
```

A crash writes the triggering input under `fuzz/artifacts/<target>/`
(gitignored); reproduce it directly with:

```bash
cargo +nightly fuzz run varint fuzz/artifacts/varint/<the-file>
```

`fuzz/corpus/<target>/` (also gitignored) accumulates interesting
inputs across runs, speeding up later sessions; delete it to start
from scratch.

## Targets

* **`varint`**: `read_vint`/`read_vlong`, the vLEB128 decoders every
  length-prefixed field on the wire goes through first.
* **`array_and_string`**: `read_array`/`read_string`/
  `read_string_map`, every length-prefixed byte array or string this
  client reads off the wire.
* **`topology_update`**: `read_topology_update`, the most structurally
  complex parser in the crate (servers, hash function version,
  per-segment owner lists), already carrying allocation limits against
  hostile input.
* **`response_header`**: `read_response_header`, the real entry point
  for any byte a server sends back; exercises the three parsers above
  by composition, with a topology update always expected
  (`ClientIntelligence::HashDistributionAware`) so that path is live
  too.
* **`sasl_challenge`**: `ScramSha512Mechanism`/`DigestSha256Mechanism`'s
  `respond`, fed arbitrary bytes as the server's challenge (fixed,
  non-fuzzed credentials): the one part of either exchange where the
  bytes come from the server, not from this client's own caller.

## Why a `fuzzing` feature on the main crate

The parsers above are `pub(crate)`, as they should stay for anyone
using `hotrod-protocol` normally. `hotrod-protocol/src/lib.rs`'s
`fuzz_internal` module, compiled only with `--features fuzzing` (which
this crate's `Cargo.toml` turns on for its one dependency on
`hotrod-protocol`), wraps each one in a thin `pub` function that takes
and returns only already-public types, so the real public API is
identical with that feature on or off.
