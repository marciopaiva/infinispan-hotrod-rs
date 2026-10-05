//! SCRAM-SHA-512 SASL mechanism (RFC 5802, generalized from SHA-1 to
//! SHA-512, no channel binding).
//!
//! The exchange has three messages: client-first, server-first (carrying
//! the salt and iteration count), and client-final (carrying the proof).
//! The server's own signature is checked against the value this client
//! computed independently: a mismatch means the server does not know the
//! password, which must be a hard failure rather than a silent accept.
//!
//! Per RFC 5802, the client sends nothing after verifying the server's
//! final message: the exchange is over as soon as that signature checks
//! out. Infinispan's wire encoding does not make this visible through
//! its "complete" flag (that flag only reflects whether a challenge
//! payload is attached to the response, not whether the SASL layer
//! underneath has finished), so `respond` returns `None` once it has
//! verified the signature, telling the generic driver to stop rather
//! than send a confirmation round the server no longer expects.

use std::collections::HashMap;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::RngCore;
use sha2::{Digest, Sha512};

use crate::error::{Error, Result};
use crate::sasl::SaslMechanism;

type HmacSha512 = Hmac<Sha512>;

/// Safety ceiling on the server-first-message's `i=` iteration count,
/// checked before it is ever passed to `pbkdf2_hmac`. Not a protocol
/// limit: real servers use counts from the low thousands up to the
/// few hundred thousand (NIST/OWASP guidance for PBKDF2). Without
/// this, a malicious or compromised server naming
/// `u32::MAX` here costs this client roughly four billion
/// PBKDF2-HMAC-SHA512 rounds, a CPU-exhaustion denial of service
/// during authentication, confirmed by a fuzz run against
/// `fuzz_internal::scram_respond` (`fuzz/fuzz_targets/sasl_challenge.rs`)
/// timing out on exactly this input. Same reasoning
/// `topology.rs`'s `MAX_TOPOLOGY_COUNT` already uses for a hostile
/// server-declared count elsewhere.
const MAX_SCRAM_ITERATIONS: u32 = 1_000_000;

pub(crate) struct ScramSha512Mechanism {
    password: String,
    client_nonce: String,
    client_first_message_bare: String,
    expected_server_signature: Option<Vec<u8>>,
}

impl ScramSha512Mechanism {
    pub(crate) fn new(authcid: &str, password: &str) -> Self {
        let client_nonce = generate_nonce();
        let client_first_message_bare =
            format!("n={},r={}", escape_username(authcid), client_nonce);
        Self {
            password: password.to_string(),
            client_nonce,
            client_first_message_bare,
            expected_server_signature: None,
        }
    }

    /// Verifies the server's `v=` signature against the one computed while
    /// building the client-final-message. A mismatch means the server does
    /// not know the password.
    fn verify_server_signature(&self, message: &[u8]) -> Result<()> {
        let message = std::str::from_utf8(message).map_err(|_| {
            Error::MalformedChallenge("server-final-message is not valid UTF-8".to_string())
        })?;
        let directives = parse_directives(message)?;
        let signature = STANDARD.decode(directive(&directives, "v")?).map_err(|_| {
            Error::MalformedChallenge(
                "server-final-message signature is not valid base64".to_string(),
            )
        })?;

        if self.expected_server_signature.as_deref() == Some(signature.as_slice()) {
            Ok(())
        } else {
            Err(Error::ScramServerVerificationFailed)
        }
    }
}

impl SaslMechanism for ScramSha512Mechanism {
    fn name(&self) -> &'static str {
        "SCRAM-SHA-512"
    }

    fn respond(&mut self, challenge: Option<&[u8]>) -> Result<Option<Vec<u8>>> {
        let Some(server_first_message) = challenge else {
            return Ok(Some(
                format!("n,,{}", self.client_first_message_bare).into_bytes(),
            ));
        };

        if self.expected_server_signature.is_some() {
            // This is the server-final-message carrying `v=`. RFC 5802
            // has nothing left for the client to send once it is
            // verified, so the exchange ends here regardless of what the
            // server's wire-level "complete" flag on this response said.
            self.verify_server_signature(server_first_message)?;
            return Ok(None);
        }

        let server_first_message = std::str::from_utf8(server_first_message).map_err(|_| {
            Error::MalformedChallenge("server-first-message is not valid UTF-8".to_string())
        })?;
        let directives = parse_directives(server_first_message)?;

        let server_nonce = directive(&directives, "r")?;
        if !server_nonce.starts_with(&self.client_nonce) {
            return Err(Error::MalformedChallenge(
                "server nonce does not extend the client nonce".to_string(),
            ));
        }

        let salt = STANDARD.decode(directive(&directives, "s")?).map_err(|_| {
            Error::MalformedChallenge("server-first-message salt is not valid base64".to_string())
        })?;
        let iterations: u32 = directive(&directives, "i")?.parse().map_err(|_| {
            Error::MalformedChallenge(
                "server-first-message iteration count is not a number".to_string(),
            )
        })?;
        if iterations > MAX_SCRAM_ITERATIONS {
            return Err(Error::DeclaredLengthTooLarge {
                what: "a SCRAM-SHA-512 iteration count",
                declared: iterations,
                max: MAX_SCRAM_ITERATIONS,
            });
        }

        let mut salted_password = [0u8; 64];
        pbkdf2_hmac::<Sha512>(
            self.password.as_bytes(),
            &salt,
            iterations,
            &mut salted_password,
        );

        let client_key = hmac_sha512(&salted_password, b"Client Key");
        let stored_key = Sha512::digest(&client_key);

        let client_final_message_without_proof =
            format!("c={},r={}", STANDARD.encode("n,,"), server_nonce);
        let auth_message = format!(
            "{},{},{}",
            self.client_first_message_bare,
            server_first_message,
            client_final_message_without_proof
        );

        let client_signature = hmac_sha512(&stored_key, auth_message.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        let server_key = hmac_sha512(&salted_password, b"Server Key");
        self.expected_server_signature = Some(hmac_sha512(&server_key, auth_message.as_bytes()));

        Ok(Some(
            format!(
                "{},p={}",
                client_final_message_without_proof,
                STANDARD.encode(client_proof)
            )
            .into_bytes(),
        ))
    }

    fn finish(&mut self, final_bytes: &[u8]) -> Result<()> {
        // Reached only if the server bundles `v=` with its "complete"
        // flag instead of sending it as a further non-final challenge.
        self.verify_server_signature(final_bytes)
    }
}

fn hmac_sha512(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha512::new_from_slice(key).expect("HMAC accepts a key of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn generate_nonce() -> String {
    let mut bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut bytes);
    STANDARD.encode(bytes)
}

/// Escapes a username per RFC 5802 section 5.1: `,` and `=` cannot appear
/// literally in a `n=` directive, so they are replaced by `=2C` and `=3D`.
/// The replacement reads from the original characters, not the growing
/// output, so a literal `=` in the input is never re-escaped.
fn escape_username(username: &str) -> String {
    let mut escaped = String::with_capacity(username.len());
    for c in username.chars() {
        match c {
            ',' => escaped.push_str("=2C"),
            '=' => escaped.push_str("=3D"),
            other => escaped.push(other),
        }
    }
    escaped
}

fn parse_directives(message: &str) -> Result<HashMap<String, String>> {
    let mut directives = HashMap::new();
    for part in message.split(',') {
        let mut kv = part.splitn(2, '=');
        let key = kv.next().unwrap_or("");
        let value = kv
            .next()
            .ok_or_else(|| Error::MalformedChallenge(format!("malformed directive: {part}")))?;
        directives.insert(key.to_string(), value.to_string());
    }
    Ok(directives)
}

fn directive<'a>(directives: &'a HashMap<String, String>, key: &str) -> Result<&'a str> {
    directives
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| Error::MalformedChallenge(format!("message is missing directive {key}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_comma_and_equals_without_double_escaping() {
        assert_eq!(escape_username("a,b=c"), "a=2Cb=3Dc");
    }

    #[test]
    fn client_first_message_carries_bare_message_after_gs2_header() {
        let mut mechanism = ScramSha512Mechanism::new("alice", "s3cr3t");
        let message = mechanism.respond(None).unwrap().unwrap();
        let message = String::from_utf8(message).unwrap();
        assert_eq!(
            message,
            format!("n,,{}", mechanism.client_first_message_bare)
        );
        assert!(message.contains("n=alice"));
    }

    #[test]
    fn rejects_server_nonce_that_does_not_extend_client_nonce() {
        let mut mechanism = ScramSha512Mechanism::new("alice", "s3cr3t");
        mechanism.respond(None).unwrap();
        let challenge = b"r=not-the-client-nonce,s=c2FsdA==,i=4096";
        let err = mechanism.respond(Some(challenge)).unwrap_err();
        assert!(matches!(err, Error::MalformedChallenge(_)));
    }

    /// Regression test for a review finding (surfaced by fuzzing this
    /// exact function via `fuzz_internal::scram_respond`): a
    /// malicious or compromised server naming an absurd iteration
    /// count must be rejected before it ever reaches `pbkdf2_hmac`,
    /// not run for real.
    #[test]
    fn rejects_an_iteration_count_over_the_safety_limit() {
        let mut mechanism = ScramSha512Mechanism::new("alice", "s3cr3t");
        mechanism.respond(None).unwrap();
        let challenge = format!(
            "r={}fake-server-nonce,s=c2FsdA==,i={}",
            mechanism.client_nonce,
            MAX_SCRAM_ITERATIONS as u64 + 1
        );
        let err = mechanism.respond(Some(challenge.as_bytes())).unwrap_err();
        assert!(matches!(
            err,
            Error::DeclaredLengthTooLarge {
                declared,
                max: MAX_SCRAM_ITERATIONS,
                ..
            } if declared == MAX_SCRAM_ITERATIONS + 1
        ));
    }

    /// Captured from a real WildFly Elytron 2.6.0.Final `SaslServer` and
    /// `SaslClient` (the exact library Infinispan uses) driven in-process
    /// with username "testuser", password "testpass". The client nonce
    /// this client would normally generate at random is overridden with
    /// the one Java's client used for that run, so this crate's proof and
    /// server-signature verification can be checked byte for byte against
    /// Java's own values instead of only against each other.
    #[test]
    fn matches_a_real_elytron_scram_sha_512_exchange() {
        let client_nonce = "u{KSK:?h^0_Fv!+0&a8QZz=r9mam<S(&F5*Q~qr^R|aNN~P>";
        let server_first_message: &[u8] = br#"r=u{KSK:?h^0_Fv!+0&a8QZz=r9mam<S(&F5*Q~qr^R|aNN~P>5HXB1V`L3yPAj)CbipC'<T_v}HQ-,s=OumLQqvE8v01nG3E,i=20000"#;
        let expected_client_final: &[u8] = br#"c=biws,r=u{KSK:?h^0_Fv!+0&a8QZz=r9mam<S(&F5*Q~qr^R|aNN~P>5HXB1V`L3yPAj)CbipC'<T_v}HQ-,p=0qmuMd9EM5rhtcjaQ0xXw6Dkf72dyg5OYfQiZn8c55SZ0GYD0EaHDJNzvE7w9wYklTOzb9eiHcBiWzBJxYQ59Q=="#;
        let server_final_message: &[u8] = br#"v=cYD+i7GYtue7LfEgLHSWdvlMJq15c5p0kHb4n9lmgMNObhS51YN0bjPJitAnH+dbWWVIT8kJG5Hn10jewNMMYQ=="#;

        let mut mechanism = ScramSha512Mechanism::new("testuser", "testpass");
        mechanism.client_nonce = client_nonce.to_string();
        mechanism.client_first_message_bare = format!("n=testuser,r={client_nonce}");

        let client_final = mechanism
            .respond(Some(server_first_message))
            .unwrap()
            .unwrap();
        assert_eq!(client_final, expected_client_final);

        assert!(mechanism
            .respond(Some(server_final_message))
            .unwrap()
            .is_none());
    }

    #[test]
    fn detects_forged_server_signature() {
        let mut mechanism = ScramSha512Mechanism::new("alice", "s3cr3t");
        mechanism.respond(None).unwrap();
        let server_nonce = format!("{}serversuffix", mechanism.client_nonce);
        let challenge = format!("r={server_nonce},s=c2FsdA==,i=4096");
        mechanism.respond(Some(challenge.as_bytes())).unwrap();

        let err = mechanism
            .respond(Some(b"v=bm90dGhlcmlnaHRzaWduYXR1cmU="))
            .unwrap_err();
        assert!(matches!(err, Error::ScramServerVerificationFailed));
    }
}
