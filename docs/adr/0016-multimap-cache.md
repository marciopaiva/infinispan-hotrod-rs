# ADR 0016: Multimap cache

## Status

Accepted, implemented in #48.

## Context

The last item in the "Specialized data structures and administration"
roadmap theme, issue #48 (already filed): a cache where each key maps
to a collection of values instead of one.

The design is pinned against the real Java client and server source
(`infinispan/infinispan`), the same discipline every prior phase
used, across two research rounds: the wire format summary from an
earlier planning session, then a focused follow-up confirming field
order and response shape operation by operation, since one detail in
the summary (`removeEntry`/`containsEntry`/`containsValue` sending
expiration fields) was unusual enough to verify directly rather than
trust at face value.

**Nine opcodes**, confirmed in `MultimapHotRodConstants.java` (client)
and `HotRodConstants.java` (server), `0x67`-`0x78`: `Get`
(`0x67`/`0x68`), `GetWithMetadata` (`0x69`/`0x6A`), `Put`
(`0x6B`/`0x6C`), `RemoveKey` (`0x6D`/`0x6E`), `RemoveEntry`
(`0x6F`/`0x70`), `Size` (`0x71`/`0x72`), `ContainsEntry`
(`0x73`/`0x74`), `ContainsKey` (`0x75`/`0x76`), `ContainsValue`
(`0x77`/`0x78`). All nine are a plain `request + 1`, no gap like
`COUNTER_IS_DEFINED` had (ADR 0015).

**Not a special cache type server-side.** A multimap cache is an
ordinary Hot Rod cache, reached through these nine opcodes instead of
the main cache API's; it still has to exist (created or pre-configured
like any other named cache) before use.

**`supportsDuplicates` is a per-call flag, not a cache setting.**
Every one of the nine always ends its request body with this one
byte (confirmed in `Codec40`; `Codec30`, protocol 3.x, writes nothing,
but this crate only ever speaks 4.1, so it always sends it).

**Field order per operation**, confirmed against the Java client's
own operation classes:

* `Get`/`GetWithMetadata`: `key`, `supportsDuplicates`. No
  expiration fields at all (`AbstractMultimapKeyOperation`).
  `GetWithMetadata`'s response is the same metadata shape this
  crate's own `read_entry_metadata` already decodes for the main
  cache API (flags byte, conditional creation/lifespan, conditional
  last-used/max-idle, then an unconditional version), followed by the
  collection.
* `Put`: `key`, `lifespan`, `maxIdle`, `value`, `supportsDuplicates`.
  The same shape `key_value_body` already builds for the main cache
  API's own `put`.
* `RemoveKey`/`ContainsKey`: `key`, `supportsDuplicates`. No
  expiration, no value.
* **`RemoveEntry`/`ContainsEntry` send `lifespan`/`maxIdle` they never
  use**: `key`, `lifespan`, `maxIdle`, `value`, `supportsDuplicates`,
  confirmed against `AbstractMultimapKeyValueOperation` (the shared
  base both extend, alongside `Put`), which the Java client itself
  hardcodes to "immortal" when building these two operations.
  `ContainsEntryMultimapOperation` carries a `// TODO: this should be
  refactored in ISPN-16469 to not have expiration` comment: a known
  inconsistency in the wire format on the Java side, not something
  this phase invents or could have avoided while matching the real
  protocol.
* `ContainsValue`: no key at all, just `lifespan`, `maxIdle`, `value`,
  `supportsDuplicates` (confirmed against
  `ContainsValueMultimapOperation`), the same hardcoded-expiration
  quirk as above.
* `Size`: just `supportsDuplicates`, nothing else. Response is a
  plain `vLong`, read unconditionally: there is no key for a
  "does not exist" status to apply to.
* Every boolean response (`RemoveKey`/`RemoveEntry`/`ContainsEntry`/
  `ContainsKey`/`ContainsValue`) is `KeyDoesNotExist` with no body
  meaning `false`, any other status meaning read one more byte
  (`1` = true).
* `Get`'s collection (and `GetWithMetadata`'s, after its metadata) is
  a `vInt` count followed by that many length-prefixed arrays.
  `KeyDoesNotExist` for this one carries **no body at all**, not even
  a zero count: confirmed empirically against a live server after the
  initial implementation read a collection unconditionally and hung
  waiting for bytes the server never sent (see "Corrected mid-flight"
  below), matching how the Java client's own `GetMultimapOperation`
  only reads a count when the status says to.

## Corrected mid-flight: two real gaps found by live-server testing

**`Get` on a missing key hangs instead of failing fast if its
response is assumed to always carry a collection.** The first
implementation read the `vInt` count unconditionally, matching what
the initial research summary said ("`Get` devolve `vInt` de contagem
+ N arrays"); against a real server, a `KeyDoesNotExist` response
turned out to carry no body at all, so that unconditional read blocked
waiting for bytes that would never arrive, surfacing only as
`Error::Timeout` after the full configured timeout elapsed, not a
quick, clear failure. Fixed by checking
`status.is_not_exist()` first, the same shortcut `get_with_version`
already uses for the main cache API's own `GetWithMetadata`.

**A cache created and used within the same test can hit this
single-node server's own topology quirk.** Once the `Get` fix above
was in place, a sequence of `get` then `remove_key` against a cache
created moments earlier via `administration().get_or_create_cache`
(`docs/adr/0014-remote-administration.md`) still intermittently paid a
full retry timeout on the second call. Root-caused with targeted
tracing, not guessed: the first keyed operation after the cache's own
topology update applies receives an owner address this test's network
cannot reach (likely a container-internal address this single-node
server advertises itself with), and the very next keyed call is the
first to actually route by it, paying the timeout once before
`NodeHealth` quarantines that address (`docs/adr/0011-retry-policy-and-node-health.md`)
and every later call goes straight to the reachable seed. Confirmed,
by testing the same sequence against the ordinary main cache API on
the same freshly created cache, to be unrelated to multimap's own
wire format: nothing before this phase ever exercised "freshly
created cache, then an immediate topology-aware keyed call" together
in one test, so this was always a latent, general characteristic of
this test environment, never actually triggered until now. The
existing retry/circuit-breaker chain already handles it correctly
(one slow call, then fast ones); the fix here is to the test fixture,
not the client: `ci/infinispan/infinispan.xml` now pre-provisions a
`multimap-test` cache at server startup, the same way the existing
`default` cache already avoids this for every other test, instead of
creating one mid-test.

## Decision

**Byte-oriented, like `RemoteCache`.** No `TypedCache` integration in
this phase, the same cut ADR 0013 already made for `RemoteCache::query`.

**Routing matches whether an operation has a key.**
`get`/`get_with_metadata`/`put`/`remove_key`/`remove_entry`/
`contains_key`/`contains_entry` route by key, reusing `RemoteCache`'s
own `call` (the same owner/backup/seed retry chain its keyed methods
already use); `size`/`contains_value` have no key, so they go to the
seed via `call_seed`, the same as `RemoteCache::query`/`size`/`clear`.

## Design

### `multimap.rs` (new)

```rust
pub struct MultimapEntry {
    pub values: Vec<Vec<u8>>,
    pub version: u64,
    pub created: Option<SystemTime>,
    pub lifespan: Expiration,
    pub last_used: Option<SystemTime>,
    pub max_idle: Expiration,
}

pub struct MultimapCache { /* cache, supports_duplicates */ }

impl HotRodClient {
    pub fn multimap_cache(&self, name: impl Into<String>, supports_duplicates: bool) -> MultimapCache;
}

impl MultimapCache {
    pub fn supports_duplicates(&self) -> bool;
    pub async fn get(&self, key: &[u8]) -> Result<Vec<Vec<u8>>>;
    pub async fn get_with_metadata(&self, key: &[u8]) -> Result<Option<MultimapEntry>>;
    pub async fn put(&self, key: &[u8], value: &[u8], lifespan: Expiration, max_idle: Expiration) -> Result<()>;
    pub async fn remove_key(&self, key: &[u8]) -> Result<bool>;
    pub async fn remove_entry(&self, key: &[u8], value: &[u8]) -> Result<bool>;
    pub async fn size(&self) -> Result<u64>;
    pub async fn contains_entry(&self, key: &[u8], value: &[u8]) -> Result<bool>;
    pub async fn contains_key(&self, key: &[u8]) -> Result<bool>;
    pub async fn contains_value(&self, value: &[u8]) -> Result<bool>;
}
```

`remove_entry`/`contains_entry`/`contains_value` send
`Expiration::Immortal` for both lifespan and max idle internally
(hardcoded in `connection.rs`, not exposed as parameters): matching
the Java client's own choice, and there being no sensible value a
caller could supply for fields the operation never uses anyway.

### Protocol wiring

`header.rs` gains the nine opcodes above, all plain `request + 1`.
`connection.rs` gains one method per opcode, against
`self.cache_name` directly (an ordinary cache, no override needed
unlike `execute_task`/counters). Two small shared helpers:
`read_array_collection` (the `vInt` count plus N arrays shape `Get`/
`GetWithMetadata` share) and `read_multimap_bool` (the
`KeyDoesNotExist`-means-`false` shape every boolean response shares).
`remote_cache.rs` gains one `Operation` variant and one `pub(crate)`
dispatch method per opcode, reusing the existing `Bool`
`OperationResult` variant for every boolean-returning operation
instead of adding five near-identical ones.

## Consequences

* New public API: `HotRodClient::multimap_cache`, `MultimapCache`,
  `MultimapEntry`. Additive: nothing existing changes shape.
* Confirmed against a live server (`ci/infinispan`): the full
  lifecycle (put, get, get_with_metadata, size, every
  contains/remove variant) round-trips correctly on a pre-provisioned
  cache.
* `ci/infinispan/infinispan.xml` gained a pre-provisioned
  `multimap-test` cache, needed only because this is the first test
  in this crate to combine "cache created or used for the first time"
  with "topology-aware keyed routing" in one run; see "Corrected
  mid-flight" above.
* Not implemented, left for later if it turns out to matter:
  `TypedCache` integration, and whatever Java's own `ISPN-16469`
  eventually does about `removeEntry`/`containsEntry` sending
  expiration fields they never use.
