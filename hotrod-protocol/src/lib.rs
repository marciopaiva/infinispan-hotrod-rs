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
//! `CacheEvent`s from. `RemoteCache::near_cache` (phase 5, see
//! `docs/adr/0007-near-caching.md`) builds on that listener to keep a
//! bounded, invalidated local cache of recently read entries.
//! `RemoteCache::get_stream`/`put_stream`/`put_stream_if_absent`/
//! `replace_stream_with_version` (see `docs/adr/0008-streaming.md`)
//! read or write a value in chunks instead of buffering it whole,
//! returning a `GetStream`/`PutStream` pinned to the one pooled
//! connection that opened it. `RemoteCache::iter`/`iter_with` (see
//! `docs/adr/0009-server-side-iteration.md`) walk every entry in a
//! cache through a server-side `CacheIterator`, opening one cursor per
//! node on a cluster instead of requiring the caller to already know
//! every key.

#![forbid(unsafe_code)]

mod client;
mod connection;
mod digest;
mod error;
mod hash;
mod header;
mod iteration;
mod listener;
mod near_cache;
mod pool;
mod remote_cache;
mod sasl;
mod scram;
mod status;
mod streaming;
mod tls;
mod topology;
mod varint;
mod wire;

pub use client::HotRodClient;
pub use connection::{HotRodConnection, VersionedResult, VersionedValue, DEFAULT_TIMEOUT};
pub use error::{Error, Result};
pub use iteration::{CacheIterator, IterationEntry, IterationOptions};
pub use listener::{CacheEvent, CacheEventInterests, CacheListener, ListenOptions, ServerFactory};
pub use near_cache::{NearCacheOptions, NearCachedCache};
pub use remote_cache::RemoteCache;
pub use streaming::{GetStream, PutStream};
pub use tls::TlsConfig;
pub use wire::Expiration;
