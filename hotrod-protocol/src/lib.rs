//! Pure Rust client for the Infinispan Hot Rod binary wire protocol.
//!
//! Phase 1 (see `docs/adr/0001-mirror-java-client-scope.md`): a single
//! sequential connection to one cache, PLAIN authentication, and the core
//! operations (`get`, `put`, `remove`, `put_if_absent`, `replace`,
//! `replace_if_unmodified`, `remove_if_unmodified`). No connection pooling,
//! no cluster topology awareness, no listeners, no near caching yet.

#![forbid(unsafe_code)]

mod connection;
mod digest;
mod error;
mod header;
mod sasl;
mod scram;
mod status;
mod varint;
mod wire;

pub use connection::{HotRodConnection, VersionedResult, VersionedValue};
pub use error::{Error, Result};
pub use wire::Expiration;
