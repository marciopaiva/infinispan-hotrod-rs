//! SASL mechanism support and the generic multi-round driver that runs
//! any of them against the server's `AuthMechList`/`Auth` operations.

use crate::error::Result;

/// One SASL mechanism's client-side state machine. `HotRodConnection`
/// drives this through zero or more challenge/response rounds via
/// `run_sasl`; the mechanism itself knows nothing about the wire framing.
pub(crate) trait SaslMechanism {
    /// The IANA SASL mechanism name, as advertised by `AuthMechList` and
    /// sent on every `Auth` request.
    fn name(&self) -> &'static str;

    /// Produces the response bytes for the next round, or `None` when the
    /// mechanism has nothing left to send and considers the exchange over.
    /// `challenge` is `None` only for the very first call.
    ///
    /// `None` must be honored regardless of what the server's wire-level
    /// "complete" flag says: Infinispan sets that flag from whether a
    /// challenge payload is attached to its response, not from whether the
    /// SASL layer underneath has actually finished, so a mechanism that
    /// verifies a final server message (SCRAM) can find itself done before
    /// the server admits it on the wire.
    fn respond(&mut self, challenge: Option<&[u8]>) -> Result<Option<Vec<u8>>>;

    /// Called once, after the server marks the exchange complete, with
    /// whatever bytes came back on that final response (often empty).
    /// Mechanisms that verify a server-side proof override this; others
    /// accept the default no-op.
    fn finish(&mut self, final_bytes: &[u8]) -> Result<()> {
        let _ = final_bytes;
        Ok(())
    }
}

/// PLAIN is single-shot: the client sends one response built from the
/// credentials and the server either completes or fails, there is no
/// challenge round-trip (RFC 4616).
pub(crate) struct PlainMechanism {
    response: Vec<u8>,
}

impl PlainMechanism {
    pub(crate) fn new(authzid: &str, authcid: &str, password: &str) -> Self {
        Self {
            response: plain_response(authzid, authcid, password),
        }
    }
}

impl SaslMechanism for PlainMechanism {
    fn name(&self) -> &'static str {
        "PLAIN"
    }

    fn respond(&mut self, _challenge: Option<&[u8]>) -> Result<Option<Vec<u8>>> {
        Ok(Some(self.response.clone()))
    }
}

/// Builds the PLAIN mechanism response: `authzid\0authcid\0password`,
/// per RFC 4616.
fn plain_response(authzid: &str, authcid: &str, password: &str) -> Vec<u8> {
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
