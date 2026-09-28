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

/// OAUTHBEARER (RFC 7628) is single-shot on the success path: the client
/// sends a bearer token obtained elsewhere (an OIDC provider, typically)
/// and the server either completes or reports a failure. RFC 7628 section
/// 3.1 has the server report that failure as a further challenge carrying
/// a JSON error object rather than as an outright SASL failure, which the
/// client must acknowledge with a single `0x01` byte before the server
/// will fail the exchange; this mechanism does that so a bad token still
/// surfaces as a normal server error instead of leaving the connection
/// desynchronized.
///
/// This client has no OIDC test fixture yet, so this mechanism carries
/// unit test coverage only; live-server coverage is a follow-up once a
/// token-backed realm is available.
pub(crate) struct OAuthBearerMechanism {
    initial_response: Vec<u8>,
    sent_initial_response: bool,
}

impl OAuthBearerMechanism {
    pub(crate) fn new(authzid: &str, token: &str) -> Self {
        Self {
            initial_response: oauthbearer_initial_response(authzid, token),
            sent_initial_response: false,
        }
    }
}

impl SaslMechanism for OAuthBearerMechanism {
    fn name(&self) -> &'static str {
        "OAUTHBEARER"
    }

    fn respond(&mut self, challenge: Option<&[u8]>) -> Result<Option<Vec<u8>>> {
        if challenge.is_none() {
            self.sent_initial_response = true;
            return Ok(Some(self.initial_response.clone()));
        }
        if self.sent_initial_response {
            self.sent_initial_response = false;
            return Ok(Some(vec![0x01]));
        }
        Ok(None)
    }
}

/// Builds the RFC 7628 initial client response: a GS2 header with no
/// channel binding, followed by the kvsep-delimited `auth=Bearer <token>`
/// key/value pair.
fn oauthbearer_initial_response(authzid: &str, token: &str) -> Vec<u8> {
    const KVSEP: u8 = 0x01;

    let mut response = Vec::new();
    response.extend_from_slice(b"n,");
    if !authzid.is_empty() {
        response.push(b'a');
        response.push(b'=');
        response.extend_from_slice(authzid.as_bytes());
    }
    response.push(b',');
    response.push(KVSEP);
    response.extend_from_slice(b"auth=Bearer ");
    response.extend_from_slice(token.as_bytes());
    response.push(KVSEP);
    response.push(KVSEP);
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

    #[test]
    fn builds_oauthbearer_initial_response_without_authzid() {
        let response = oauthbearer_initial_response("", "the-token");
        assert_eq!(response, b"n,,\x01auth=Bearer the-token\x01\x01");
    }

    #[test]
    fn builds_oauthbearer_initial_response_with_authzid() {
        let response = oauthbearer_initial_response("user@example.com", "the-token");
        assert_eq!(
            response,
            b"n,a=user@example.com,\x01auth=Bearer the-token\x01\x01"
        );
    }

    #[test]
    fn acknowledges_a_failure_challenge_then_ends_the_exchange() {
        let mut mechanism = OAuthBearerMechanism::new("", "bad-token");
        assert!(mechanism.respond(None).unwrap().is_some());

        let response = mechanism
            .respond(Some(br#"{"status":"invalid_token"}"#))
            .unwrap();
        assert_eq!(response, Some(vec![0x01]));

        assert_eq!(mechanism.respond(Some(b"")).unwrap(), None);
    }
}
