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
* **`create_cache`/`remove_cache` are not retry-safe**, the same
  trade-off ADR 0011 already accepted for every conditional
  operation: `dispatch`'s retry chain only distinguishes a transport
  failure (`Io`/`Timeout`) from a real server response, never
  operation semantics, matching the Java client's own
  `OperationDispatcher`. A connection dropping after the server
  already applied a `create_cache` (without `AdminFlag::Update`) or a
  `remove_cache` means the retried attempt sees the already-changed
  state and surfaces a server error for an operation that, from the
  caller's point of view, already succeeded. Nothing in this phase
  changes that general policy for administration specifically; a
  caller that cannot tolerate it should check `cache_names()` before
  treating a failure as final.
* **`with_flags` does not check that a flag applies to the call it
  ends up on.** `AdminFlag::Update` only has documented meaning for
  `create_cache`/`get_or_create_cache`; nothing stops a caller from
  setting it on a handle later used for `remove_cache`, which simply
  forwards it as that task's `flags` parameter with unconfirmed
  effect. Left as a documentation note (`Administration::with_flags`'s
  own doc comment) rather than a type-level restriction, since
  splitting `Administration` into per-operation handles just to
  enforce this would be a bigger API change than the risk warrants.
* Not implemented, left for later if it turns out to matter:
  typed differentiation of server errors (still not available on the
  wire).

### Addendum: the six remaining tasks (#112)

The six tasks this ADR originally deferred are implemented, each a
thin new case of the same `exec_cache()`/`run_exec` shape
`create_cache_with_task`/`remove_cache` already established. No new
opcode, no new `Error` variant, no change to `execute_task`/`run_exec`
themselves. Confirmed against the Java client's own source
(`RemoteCacheManagerAdminImpl.java`, fetched directly):

* `Administration::reindex_cache`/`update_index_schema` send only
  `name`, never `flags`: Java's own `reindexCache`/`updateIndexSchema`
  use a bare `Collections.singletonMap(CACHE_NAME, ...)`, not the
  `flags`-aware map every other task (including the four below) uses.
  Java also has a timeout-overloaded variant of both, but that bound
  is enforced purely client-side (`TimeoutHotRodOperation`) and sends
  nothing extra to the server either way; this crate has no per-call
  timeout override anywhere (one client-wide `HotRodClient::timeout`/
  `set_timeout`), so neither method gets one here either, for
  consistency with every other operation in this crate.
* `Administration::update_configuration_attribute(name, attribute,
  value)`: `name`, `attribute`, `value`, then `flags` if set.
* `Administration::assign_alias(alias_name, cache_name)`: wire
  parameters `name` (the cache being aliased) then `alias` (the alias
  itself), then `flags` if set. The method's own argument order
  matches Java's `assignAlias(String aliasName, String cacheName)`
  even though the wire parameter names spell the opposite order
  (`CACHE_NAME` before `ALIAS_NAME` in `RemoteCacheManagerAdminImpl`).
* `Administration::create_template(name, configuration)`: `name`,
  then `configuration` only when `Some` (omitted entirely for `None`,
  the same "omitted, not empty" convention `flags` already uses
  throughout this file), then `flags` if set. Unlike `create_cache`,
  there is no way to base a template on another already-defined
  template through this task: Java's own `createTemplate` takes a
  nullable configuration document, never a template name.
  `Administration::remove_template(name)`: `name`, then `flags` if
  set.
* All six discard their response body on success, the same as every
  Java caller of `submitOperation` already does: the body is a JSON
  string on the wire, never inspected either side.
* **Not revisited: routing.** Reading `RemoteCacheManagerAdminImpl.java`
  in full while confirming the above surfaced that the Java client
  actually routes every one of these tasks, including the already
  shipped `create_cache`/`remove_cache`, to a node associated with the
  cache name (`operationDispatcher.addressForCache(name)`), not
  blindly to the active seed the way this crate's own `exec_cache()`
  always does. This phase does not change that: `create_cache`/
  `remove_cache`'s own routing is already shipped, public behavior,
  out of scope for an issue that asked for six new tasks, not a
  routing change; and the same latency-only trade-off (correctness
  unaffected, an extra internal hop paid) is the exact one ADR 0015
  already accepted, deliberately, for distributed counters. The six
  new tasks here follow the existing, already-decided choice rather
  than introducing a second, inconsistent one.
