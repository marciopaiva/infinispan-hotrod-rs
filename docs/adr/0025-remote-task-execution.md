# ADR 0025: Remote task execution

## Status

Accepted, implemented in #51.

## Context

Issue #51 (already filed): the Java client can invoke a server-side
script or task via `RemoteCache.execute(taskName, params)` /
`execute(taskName, params, key)`. The issue itself named the two open
questions that had blocked this: parameter marshalling and result
typing, "likely related to the marshalling question raised in the
remote query issue" (`docs/adr/0013-remote-query.md`).

The wire mechanism this needs already exists and is already
exercised in production. `docs/adr/0014-remote-administration.md`
implemented `EXEC_REQUEST`/`EXEC_RESPONSE` (`0x2B`/`0x2C`) for the
server's own `@@cache@...` tasks, through
`HotRodConnection::execute_task` and `RemoteCache::run_exec`. That ADR
named this issue directly: "the same [mechanism] a user-defined remote
task (#51, out of scope here) would use." What was missing was a
public entry point for an arbitrary, user-defined task name, not just
the built-in admin ones, and the two design questions above.

Checked directly against the Java client's own source (not guessed):

* **`CacheExecuteOperation.java`** (used by `RemoteCache.execute`, the
  cache-scoped call) and **`NoCacheExecuteOperation.java`** (used only
  by the admin tasks, at the cache-manager level) write the exact same
  request body in `writeOperationRequest`: task name, a vInt count,
  then `(name, byte[])` pairs. The only wire-level difference between
  the two is the **cache name declared in the Hot Rod header**: an
  admin task declares an empty one (already what `execute_task`
  does); a user task declares the real cache name the `RemoteCache`
  it was called on points to.
* **The routing key, `execute`'s third argument, is never written
  into the request body.** `CacheExecuteOperation` takes it as a
  constructor argument used only by `getRoutingObject()`, which
  `OperationDispatcher` reads to pick which server connection carries
  the request. It plays exactly the role an ordinary `get`/`put` key
  already plays for this crate's `RemoteCache::call(key, op)`. A task
  that never looks at its routing key, or is called through the
  two-argument overload with none, still runs identically wherever it
  lands; the key only picks a server, nothing about what the task
  receives.
* **Parameter marshalling, confirmed in `RemoteCacheImpl.execute`**:
  `Map<String, ?> params` is turned into `Map<String, byte[]>` by
  running every value through `keyToBytes`, the cache's own key-side
  marshaller, before the operation ever sees it. On the wire, by the
  time `CacheExecuteOperation` writes anything, there is no type
  information left: only named byte arrays, the exact shape this
  crate's `run_exec` already sends for admin tasks
  (`Vec<(String, Vec<u8>)>`). Using the *key* marshaller rather than
  the value one for an arbitrary task parameter is a Java client
  convention worth noting, not a contract this crate needs to
  replicate: nothing about the wire format requires it, and baking it
  into a typed wrapper here would silently assume every task parameter
  is key-shaped.
* **Result typing, confirmed in `CacheExecuteOperation.createResponse`
  vs. `NoCacheExecuteOperation.createResponse`**: the cache-scoped
  operation reads the response as a vInt-prefixed byte array and runs
  it through the *default* marshaller (`CacheUnmarshaller.readOther`);
  the no-cache (admin) operation instead reads the same bytes as a
  plain UTF-8 string. Either way, on the wire the response is one
  vInt-prefixed byte array, exactly what `conn.execute_task` already
  reads via `read_array`. Decoding those bytes is already left to the
  caller for admin tasks today (`admin.rs` parses `@@cache@names`'s
  result as JSON itself); nothing here needs to change that.

So this phase needs no new opcode and no new wire message: it needs a
cache-scoped sibling of `execute_task` that declares the connection's
real cache name instead of forcing it empty, a public `RemoteCache`
API over it with and without a routing key, and a decision to keep
both the parameters and the result as raw bytes rather than inventing
a marshalling convention the protocol itself does not require.

## Decision

**Two new public methods on `RemoteCache`:**

```rust
impl RemoteCache {
    pub async fn execute(
        &self,
        task_name: impl Into<String>,
        params: &[(String, Vec<u8>)],
    ) -> Result<Vec<u8>>;

    pub async fn execute_near(
        &self,
        task_name: impl Into<String>,
        params: &[(String, Vec<u8>)],
        key: &[u8],
    ) -> Result<Vec<u8>>;
}
```

`execute` has no key to route by, so it goes to the active seed, the
same as `query` and every remote-administration call. `execute_near`
routes to `key`'s owner through the same `call(key, op)` retry chain
an ordinary `get`/`put` already uses; `key` is never put on the wire,
only used to pick a server, confirmed above against
`CacheExecuteOperation.getRoutingObject()`.

**Params and the result stay raw bytes.** A task's own contract
decides how to interpret its parameters and what its result means;
this crate does not guess a marshalling scheme the protocol itself
does not impose. This mirrors `RemoteCache::query`'s own v1 scope
(`docs/adr/0013-remote-query.md`): ship the byte-oriented entry point
first, let a typed wrapper be a separate, later proposal if a concrete
need for one surfaces.

**No `TypedCache::execute` in this phase.** Unlike query, issue #51
does not ask for one, and there is no obviously correct marshaller to
reach for: `TypedCache` marshals cache *entries* (keys and values), a
task's parameters and result are neither. The Java client's own
convention of marshalling every param through the key marshaller is
noted above specifically as something not worth repeating blindly.
Left for a later proposal if a real use case names a concrete
marshalling need; see Consequences.

**No new `Error` variant, no new opcode.** Reuses `OpCode::Exec`
(`0x2B`) and every error `run_exec`/`execute_task` can already
surface (`Error::Server`, `Error::Io`, transport/retry errors through
`call`/`call_seed`).

## Design

### `connection.rs`

`execute_task`'s body is factored into a shared private helper,
`exec_request`, taking the cache name as a parameter, so the admin
path ("always empty") and the new user-task path ("always this
connection's own cache name") share one encoder/decoder instead of
duplicating the wire-writing loop:

```rust
async fn exec_request(
    &mut self,
    cache_name: &[u8],
    task_name: &str,
    params: &[(String, Vec<u8>)],
) -> Result<Vec<u8>>;

pub(crate) async fn execute_task(
    &mut self,
    task_name: &str,
    params: &[(String, Vec<u8>)],
) -> Result<Vec<u8>> {
    self.exec_request(&[], task_name, params).await
}

pub(crate) async fn execute_cache_task(
    &mut self,
    task_name: &str,
    params: &[(String, Vec<u8>)],
) -> Result<Vec<u8>> {
    let cache_name = self.cache_name.clone();
    self.exec_request(&cache_name, task_name, params).await
}
```

### `remote_cache.rs`

A new `Operation::ExecCache(String, Vec<(String, Vec<u8>)>)` variant,
dispatched to `conn.execute_cache_task`, alongside the existing
`Operation::Exec` the admin tasks already use. `RemoteCache::execute`
dispatches it through `call_seed`; `RemoteCache::execute_near`
dispatches the same variant through `call(key, ..)`.

## Consequences

* New public API: `RemoteCache::execute`, `RemoteCache::execute_near`.
  Additive: nothing existing changes shape, and `execute_task`'s own
  behavior (always an empty cache name, for `admin.rs`) is unchanged,
  just refactored to share `exec_request` with the new
  `execute_cache_task`.
* **No `TypedCache::execute`.** A caller working through `TypedCache`
  still reaches `execute`/`execute_near` via `Deref` to the underlying
  `RemoteCache`, getting raw bytes for params and the result the same
  as any other caller. A typed wrapper, if one turns out to matter in
  practice, needs its own proposal to pick a marshalling convention
  for task parameters and results, since neither maps cleanly onto
  `TypedCache`'s existing key/value marshallers.
* A caller invoking a task that ignores its routing key, or passing
  none via `execute`, gets the same result either way; the key only
  ever affects which server receives the request.
* **No live-server test for `execute`/`execute_near` themselves**, by
  design, confirmed empirically during a later documentation and test
  audit: `tests/live_server.rs`'s CI fixture (`infinispan/server:15.1`)
  ships the `infinispan-scripting` module but no script engine to run
  a user-defined task with (uploading a JavaScript task over REST
  fails with `ISPN027510: Cannot find an appropriate script engine`,
  confirmed by running the exact upload against that image). A real
  live test would need either a heavier server image carrying a
  script engine or a compiled Java task deployed into the server's
  `lib` directory, both disproportionate to the gain for a mechanism
  whose wire encoding is already covered end to end by two unit tests
  (`remote_cache.rs`: one asserting the request's cache name, task
  name and params and the response bytes; one asserting the routing
  key never reaches the wire). Left as a known limitation rather than
  worked around.
