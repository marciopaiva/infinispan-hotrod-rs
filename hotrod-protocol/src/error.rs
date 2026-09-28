//! Error type for the Hot Rod client.
//!
//! Wire-level and server-reported failures are kept distinct: `Server`
//! carries the status byte and message the server sent back, everything
//! else is a client-side or transport failure.

use std::io;

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
}

/// Result alias for `hotrod_protocol::Error`.
pub type Result<T> = std::result::Result<T, Error>;
