//! Error type for the Hot Rod client.
//!
//! Wire-level and server-reported failures are kept distinct: `Server`
//! carries the status byte and message the server sent back, everything
//! else is a client-side or transport failure.

use std::io;
use std::time::Duration;

/// Errors that can occur while talking to a Hot Rod server.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("invalid response magic byte: expected 0xA1, got {0:#04x}")]
    InvalidMagic(u8),

    #[error("response message id {actual} does not match request message id {expected}")]
    MessageIdMismatch { expected: u64, actual: u64 },

    #[error("expected response opcode {expected:#04x}, got {actual:#04x}")]
    UnexpectedOpcode { expected: u8, actual: u8 },

    #[error("server returned an unrecognized status byte: {0:#04x}")]
    UnknownStatus(u8),

    #[error("server sent an unrecognized media type definition byte: {0:#04x}")]
    UnknownMediaTypeDefinition(u8),

    #[error("server error (status {status:#04x}): {message}")]
    Server { status: u8, message: String },

    #[error("string received from server is not valid UTF-8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    #[error("SASL mechanism {0} is not offered by the server")]
    UnsupportedSaslMechanism(String),

    #[error("malformed SASL challenge: {0}")]
    MalformedChallenge(String),

    #[error("SCRAM server verification failed: the server's final signature did not match")]
    ScramServerVerificationFailed,

    #[error("DIGEST-SHA-256 server verification failed: the server's rspauth did not match")]
    DigestServerVerificationFailed,

    // The server only sends a topology update when the client advertises
    // TOPOLOGY_AWARE or HASH_DISTRIBUTION_AWARE intelligence. This client
    // always advertises BASIC (phase 1 has no topology support, see ADR
    // 0001 and issue #3), so receiving one here means the server disagrees
    // about that, and continuing to read would desync the stream.
    #[error(
        "received an unexpected topology update: topology-aware routing is not implemented yet"
    )]
    UnsupportedTopologyUpdate,

    #[error("server's topology update uses hash function version {0}, which this client does not implement")]
    UnsupportedHashFunctionVersion(u8),

    #[error(
        "topology update references owner index {index}, but only {num_servers} servers were listed"
    )]
    InvalidTopologyOwnerIndex { index: u32, num_servers: usize },

    // A corrupted or hostile length prefix could otherwise ask for an
    // allocation large enough to abort the process (Rust's allocator calls
    // `handle_alloc_error` on failure, which is not a catchable panic),
    // which would take down the whole embedding process, not just this
    // call. Rejecting the declared length before allocating turns that into
    // an ordinary typed error.
    #[error("server declared {declared} {what}, which exceeds this client's limit of {max}")]
    DeclaredLengthTooLarge {
        what: &'static str,
        declared: u32,
        max: u32,
    },

    // A corrupted or hostile stream could otherwise send an unbounded run
    // of continuation bytes (the 0x80 bit set on every byte), shifting
    // `result` past the target type's width: a panic with overflow checks
    // on, or a silently wrong, masked value in release. The encoding never
    // needs more than 5 bytes for a vInt or 10 for a vLong, the same bound
    // the Java client enforces.
    #[error("malformed varint: continuation bit still set after {max_bytes} bytes")]
    MalformedVarint { max_bytes: u8 },

    /// Fires when a connect, or an operation's full write-then-read cycle,
    /// does not finish within the configured timeout. The connection this
    /// happened on may have an unwritten or unread partial protocol frame
    /// left on the wire and must not be reused: treat it the same as an
    /// `Io` error and reconnect.
    ///
    /// This is one way a connection ends up with a partial frame in
    /// flight, not the only one: see the module docs on `HotRodConnection`
    /// and `HotRodCluster` for the general rule, which also covers a
    /// caller dropping the operation's future before it resolves for a
    /// reason of its own.
    #[error("operation timed out after {0:?}")]
    Timeout(Duration),

    /// A prior operation on this connection ended without its response
    /// being read in full (`Error::Timeout`, or the operation's future
    /// dropped before resolving for a reason of its own), so the stream may
    /// have a partial frame in flight. `HotRodConnection` now enforces the
    /// rule the two errors above only documented: it refuses every further
    /// operation once this happens, instead of leaving a caller free to
    /// reuse a connection that is silently desynced. Reconnect instead.
    #[error("connection is poisoned by a prior operation that did not complete: reconnect instead of reusing it")]
    PoisonedConnection,
}

/// Result alias for `hotrod_protocol::Error`.
pub type Result<T> = std::result::Result<T, Error>;
