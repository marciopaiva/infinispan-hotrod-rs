//! Pure Rust client for the Infinispan Hot Rod binary wire protocol.
//!
//! `HotRodConnection` (phase 1, see
//! `docs/adr/0001-mirror-java-client-scope.md`) is a single sequential
//! connection to one cache, authenticated with PLAIN, SCRAM-SHA-512,
//! DIGEST-SHA-256 or OAUTHBEARER, exposing the core operations (`get`,
//! `put`, `remove`, `put_if_absent`, `replace`, `replace_if_unmodified`,
//! `remove_if_unmodified`, `contains_key`, `ping`, `size`, `clear`,
//! `stats`, `get_all`, `put_all`). `HotRodClient` (phase 3, see
//! `docs/adr/0003-hash-aware-routing-scope.md`, restructured by
//! `docs/adr/0005-connection-pooling-and-client-cache-split.md`) tracks
//! cluster topology and holds a pool of such connections per node, per
//! cache; `cache` returns a `RemoteCache` handle for a named cache, whose
//! operations route to the segment's primary owner instead of relying on
//! server-side redirects and run concurrently across however many
//! `RemoteCache`/`HotRodClient` handles a caller shares between tasks.
//! Either `HotRodConnection` or `HotRodClient` also connects over TLS
//! (`connect_tls`/`connect_tls_with_timeout`, see
//! `docs/adr/0004-tls-support.md`). Every connect and operation on either
//! type is bounded by a timeout (`DEFAULT_TIMEOUT` unless overridden).
//! `RemoteCache::listen`/`listen_with` (phase 4, see
//! `docs/adr/0006-client-listeners.md`) register a client listener on its
//! own dedicated connection, returning a `CacheListener` to pull
//! `CacheEvent`s from. No near caching yet.

#![forbid(unsafe_code)]

mod client;
mod connection;
mod digest;
mod error;
mod hash;
mod header;
mod listener;
mod pool;
mod remote_cache;
mod sasl;
mod scram;
mod status;
mod tls;
mod topology;
mod varint;
mod wire;

pub use client::HotRodClient;
pub use connection::{HotRodConnection, VersionedResult, VersionedValue, DEFAULT_TIMEOUT};
pub use error::{Error, Result};
pub use listener::{CacheEvent, CacheEventInterests, CacheListener, ListenOptions, ServerFactory};
pub use remote_cache::RemoteCache;
pub use tls::TlsConfig;
pub use wire::Expiration;
