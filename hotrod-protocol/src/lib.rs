//! Pure Rust client for the Infinispan Hot Rod binary wire protocol.
//!
//! `HotRodConnection` (phase 1, see
//! `docs/adr/0001-mirror-java-client-scope.md`) is a single sequential
//! connection to one cache, authenticated with PLAIN, SCRAM-SHA-512,
//! DIGEST-SHA-256 or OAUTHBEARER, exposing the core operations (`get`,
//! `put`, `remove`, `put_if_absent`, `replace`, `replace_if_unmodified`,
//! `remove_if_unmodified`). `HotRodCluster` (phase 3, see
//! `docs/adr/0003-hash-aware-routing-scope.md`) wraps a pool of such
//! connections across a multi-node cluster, tracking its topology and
//! routing each operation to the segment's primary owner instead of
//! relying on server-side redirects. Every connect and operation on either
//! type is bounded by a timeout (`DEFAULT_TIMEOUT` unless overridden). No
//! listeners, no near caching yet.

#![forbid(unsafe_code)]

mod cluster;
mod connection;
mod digest;
mod error;
mod hash;
mod header;
mod sasl;
mod scram;
mod status;
mod topology;
mod varint;
mod wire;

pub use cluster::HotRodCluster;
pub use connection::{HotRodConnection, VersionedResult, VersionedValue};
pub use error::{Error, Result};
pub use wire::Expiration;
