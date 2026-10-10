# ADR 0014: Remote administration

## Status

Accepted, implemented in #55.

## Context

The first of three remaining items in the "Specialized data structures
and administration" roadmap theme, issue #55 (already filed):
creating, removing and listing caches from a client, without the
operator needing shell or REST access to the server.

The design is pinned against the real Java client and server source
(`infinispan/infinispan`), the same discipline every prior phase
used, plus live-server experimentation that corrected a wrong
assumption the initial research made about cache templates (see
"Corrected mid-flight" below).

**There are no dedicated administration opcodes.** Confirmed against
`HotRodConstants.java` on both the client and server side: creating,
removing and listing caches are not wire-level operations of their
own. They are all server-side tasks invoked through the already
generic `EXEC_REQUEST`/`EXEC_RESPONSE` mechanism
(`0x2B`/`0x2C`), the same one a user-defined remote task (#51, out of
scope here) would use.

**`EXEC_REQUEST`'s body** (confirmed against the Java client's own
`NoCacheExecuteOperation` and the protocol documentation): task name
(a Hot Rod string/array), a parameter count (vInt), then per
parameter a name (string) and a value (a plain byte array, always the
parameter's UTF-8 text, never marshalled through any `Marshaller`).
**`EXEC_RESPONSE`**: the usual status byte, then on success a byte
array whose contents are a JSON document.

**Confirmed tasks** (`CacheCreateTask.java`, protocol documentation):
`@@cache@create`/`@@cache@getorcreate` (parameters `name`, exactly
one of `template` or `configuration`, and `flags`), `@@cache@remove`
(`name`, `flags`), `@@cache@names` (no parameters, returns a JSON
array of strings). `configuration` accepts a full XML document, a
bare XML fragment, JSON or YAML, auto-detected server-side (confirmed
against the Java client's own tests; its protocol documentation
claims XML only, which is stale). `flags` is a comma-joined string of
`CacheContainerAdmin.AdminFlag` values (`VOLATILE`, `UPDATE`); the
documentation's claim that these are space-separated is also stale.

Other tasks exist (`@@cache@reindex`, `@@cache@updateindexschema`,
`@@cache@updateConfigurationAttribute`, `@@cache@assignAlias`,
`@@template@create`/`@@template@remove`) and are deliberately left
out of this phase: each is a small, independent addition on top of
the same `execute_task` primitive this phase introduces.

**Errors are never typed on the wire.** Any failure (the cache
already exists, the template is missing, the configuration is
invalid) comes back as the same generic server error every other
operation already produces (`Error::Server { status, message }`),
with a plain-text message that includes an `ISPNxxxxxx` code.
Distinguishing these programmatically would mean parsing that text,
which is out of scope: it is not a stable contract.

### Corrected mid-flight: predefined cache templates are not available

The initial research found `org.infinispan.DIST_SYNC` named as a
predefined template in the Java client's own test suite and assumed
it would be available on any server. Live-server testing
(`ci/infinispan`, Infinispan 15.1) disproved this: both
`org.infinispan.DIST_SYNC` and `org.infinispan.LOCAL` were rejected
with `ISPN000374: No such template`. This is specific to the test
image's configuration, not a protocol fact: `template` itself works
as documented once a template that the server actually has is named.
This phase's own live test sidesteps the question entirely by using
`CacheConfig::Definition` (a configuration fragment) instead of
`CacheConfig::Template`, which needed no further corrections once the
media type fix from #50 (ADR 0013) was already in place for this
cache's own default-cache writes.

## Decision

**Reuse the generic task mechanism, add no new public opcode beyond
`Exec`.** `HotRodConnection::execute_task` is the one new low-level
primitive, reusable by any future named-task feature (including #51,
if it is ever picked back up), not something `admin.rs` owns
privately.

**`@@cache@names`'s JSON response is parsed by hand, no new
dependency.** A small parser for exactly one shape, a flat JSON array
of strings, the same call already made for the Protobuf envelope in
ADR 0013: this crate does not need a general JSON library for a
single, fixed response shape.

**v1 scope**: create, get-or-create, remove and list caches, with
`AdminFlag::Volatile`/`AdminFlag::Update`. Out of this phase: every
other confirmed task listed above, and typed differentiation of
server errors (not available on the wire, see above).

## Design

### `connection.rs`: `execute_task`

```rust
pub(crate) async fn execute_task(
    &mut self,
    task_name: &str,
    params: &[(String, Vec<u8>)],
) -> Result<Vec<u8>>; // raw bytes of a successful response
```

Always declares an empty cache name on the wire, regardless of which
cache this connection is pooled for: a task runs at the cache-manager
level, the same way `AuthMechList`/`Auth` already use an empty cache
name rather than `self.cache_name`. `header.rs` gains `OpCode::Exec =
0x2B` (response `0x2C`, the usual "response = request + 1"
convention); no new `RequestMediaType` variant, since task parameters
are always plain UTF-8 bytes, not Protobuf.

### `admin.rs` (new)

```rust
pub enum AdminFlag { Volatile, Update }

pub enum CacheConfig {
    Template(String),
    Definition(String), // XML, a bare fragment, JSON or YAML; server auto-detects
}

pub struct Administration<'a> { /* client, flags */ }

impl HotRodClient {
    pub fn administration(&self) -> Administration<'_>;
}

impl<'a> Administration<'a> {
    pub fn with_flags(self, flags: impl IntoIterator<Item = AdminFlag>) -> Self;
    pub async fn create_cache(&self, name: &str, config: CacheConfig) -> Result<()>;
    pub async fn get_or_create_cache(&self, name: &str, config: CacheConfig) -> Result<()>;
    pub async fn remove_cache(&self, name: &str) -> Result<()>;
    pub async fn cache_names(&self) -> Result<Vec<String>>;
}
```

No key to route by: every call runs through `RemoteCache::run_exec`
against `client.cache("")`, the same `call_seed`/retry/circuit
breaker chain `RemoteCache::query` already uses, reusing the default
cache's own connection pool rather than inventing a second one.
This is safe only because `execute_task` always overrides the cache
name on the wire regardless of what `self.cache_name` the pooled
connection happens to carry (see above); `Administration` never
exposes that internal `RemoteCache` or calls any of its other
methods, so there is no risk of it being used for an ordinary
get/put against the default cache by mistake.

`flags`, when non-empty, is sent as one extra `flags` parameter,
comma-joined; when empty, the parameter is left out entirely rather
than sent as an empty string, since the two were indistinguishable in
testing and omitting it is the more conservative choice.

### Errors

`Error::MalformedAdminResponse(String)` (same shape as
`MalformedQueryResponse`), for `cache_names`'s hand-rolled JSON array
parser failing on a response that is not what the task is documented
to return. The server-side task itself succeeded by the time this can
fire; it is never used for anything the server already reports as an
`Error::Server`.

## Consequences

* New public API: `HotRodClient::administration`, `Administration`,
  `AdminFlag`, `CacheConfig`, `Error::MalformedAdminResponse`.
  Additive: nothing existing changes shape.
* Confirmed against a live server (`ci/infinispan`): create (with
  flags), get-or-create on an already-existing cache, list and
  remove all round-trip correctly end to end.
* Whether removing a cache that does not exist is an error could not
  be confirmed either way during this phase's research (no test
  found, nothing documented): this client passes the call through
  unchanged and surfaces whatever the server returns.
* Not implemented, left for later if it turns out to matter:
  `@@cache@reindex`, `@@cache@updateindexschema`,
  `@@cache@updateConfigurationAttribute`, `@@cache@assignAlias`,
  `@@template@create`/`@@template@remove`, and typed differentiation
  of server errors.
