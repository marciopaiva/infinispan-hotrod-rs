//! SASL PLAIN mechanism support (phase 1 of ADR 0001; broader mechanisms
//! are phase 2, issue #2).
//!
//! PLAIN is single-shot: the client sends one response and the server
//! either completes or fails, there is no challenge round-trip.

/// Builds the PLAIN mechanism response: `authzid\0authcid\0password`,
/// per RFC 4616.
pub(crate) fn plain_response(authzid: &str, authcid: &str, password: &str) -> Vec<u8> {
    let mut response = Vec::with_capacity(authzid.len() + authcid.len() + password.len() + 2);
    response.extend_from_slice(authzid.as_bytes());
    response.push(0);
    response.extend_from_slice(authcid.as_bytes());
    response.push(0);
    response.extend_from_slice(password.as_bytes());
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_rfc4616_response() {
        let response = plain_response("", "alice", "s3cr3t");
        assert_eq!(response, b"\0alice\0s3cr3t");
    }
}
