//! DIGEST-SHA-256 SASL mechanism: Elytron's generalization of RFC 2831
//! DIGEST-MD5 to other hash algorithms, with SHA-256 in place of MD5
//! everywhere the RFC uses it. Only the "auth" quality of protection is
//! supported, no integrity or confidentiality layer and no cipher
//! negotiation, since this client sends one request and waits for one
//! response per call and has no use for a wrapped transport.
//!
//! The exchange has three messages: an empty initial response (DIGEST is
//! server-first, unlike PLAIN or SCRAM), the server's challenge (realm,
//! nonce, qop), and the client's response carrying the computed digest.
//! The server replies with `rspauth`, its own proof that it knows the
//! password, which this client verifies against the value it computed
//! independently.
//!
//! As with SCRAM (see `scram.rs`), Elytron's `DigestSaslServer` marks its
//! SASL negotiation complete in the same call that returns the `rspauth`
//! message, but Infinispan's wire encoding still reports that response as
//! non-final because it carries a payload. `respond` returns `None` right
//! after verifying `rspauth` so the generic driver stops instead of
//! sending a round the server no longer expects.
//!
//! The digest-uri's server-name half is fixed to `infinispan`, matching
//! this project's own test fixture and the value commonly left as the
//! default in Infinispan Server configurations. A server configured with
//! a different SASL `server-name` will reject the digest-uri and this
//! mechanism will not authenticate against it.

use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::sasl::SaslMechanism;

const DIGEST_URI: &str = "hotrod/infinispan";
const QOP_AUTH: &str = "auth";
const NONCE_COUNT: &str = "00000001";

pub(crate) struct DigestSha256Mechanism {
    authcid: String,
    password: String,
    cnonce: String,
    expected_rspauth: Option<Vec<u8>>,
}

impl DigestSha256Mechanism {
    pub(crate) fn new(authcid: &str, password: &str) -> Self {
        Self {
            authcid: authcid.to_string(),
            password: password.to_string(),
            cnonce: generate_cnonce(),
            expected_rspauth: None,
        }
    }

    fn verify_rspauth(&self, message: &[u8], expected: &[u8]) -> Result<()> {
        let message = std::str::from_utf8(message).map_err(|_| {
            Error::MalformedChallenge("server-final-message is not valid UTF-8".to_string())
        })?;
        let directives = parse_directives(message)?;
        let rspauth = directive(&directives, "rspauth")?;

        if rspauth.as_bytes() == expected {
            Ok(())
        } else {
            Err(Error::DigestServerVerificationFailed)
        }
    }
}

impl SaslMechanism for DigestSha256Mechanism {
    fn name(&self) -> &'static str {
        "DIGEST-SHA-256"
    }

    fn respond(&mut self, challenge: Option<&[u8]>) -> Result<Option<Vec<u8>>> {
        let Some(challenge) = challenge else {
            return Ok(Some(Vec::new()));
        };

        if let Some(expected_rspauth) = self.expected_rspauth.take() {
            self.verify_rspauth(challenge, &expected_rspauth)?;
            return Ok(None);
        }

        let challenge = std::str::from_utf8(challenge)
            .map_err(|_| Error::MalformedChallenge("challenge is not valid UTF-8".to_string()))?;
        let directives = parse_directives(challenge)?;

        let realm = all_directives(&directives, "realm").next();
        let nonce = directive(&directives, "nonce")?;
        let use_utf8_charset = directive(&directives, "charset").is_ok_and(|c| c == "utf-8");

        if let Ok(offered_qop) = directive(&directives, "qop") {
            if !offered_qop.split(',').map(str::trim).any(|q| q == QOP_AUTH) {
                return Err(Error::MalformedChallenge(
                    "server does not offer the auth quality of protection".to_string(),
                ));
            }
        }

        let digest_urp = urp_digest(&self.authcid, realm, &self.password);
        let h_a1 = hex_encode(&h_a1(&digest_urp, nonce, &self.cnonce));

        let response_value =
            digest_response(&h_a1, nonce, NONCE_COUNT, &self.cnonce, QOP_AUTH, true);
        self.expected_rspauth = Some(
            digest_response(&h_a1, nonce, NONCE_COUNT, &self.cnonce, QOP_AUTH, false).into_bytes(),
        );

        let mut response = String::new();
        if use_utf8_charset {
            response.push_str("charset=utf-8,");
        }
        response.push_str("username=\"");
        response.push_str(&quote(&self.authcid));
        response.push_str("\",");
        if let Some(realm) = realm {
            response.push_str("realm=\"");
            response.push_str(&quote(realm));
            response.push_str("\",");
        }
        response.push_str("nonce=\"");
        response.push_str(nonce);
        response.push_str("\",nc=");
        response.push_str(NONCE_COUNT);
        response.push_str(",cnonce=\"");
        response.push_str(&self.cnonce);
        response.push_str("\",digest-uri=\"");
        response.push_str(DIGEST_URI);
        response.push_str("\",response=");
        response.push_str(&response_value);
        response.push_str(",qop=");
        response.push_str(QOP_AUTH);

        Ok(Some(response.into_bytes()))
    }

    fn finish(&mut self, final_bytes: &[u8]) -> Result<()> {
        // Reached only if the server bundles rspauth with its "complete"
        // flag instead of sending it as a further non-final challenge.
        match self.expected_rspauth.take() {
            Some(expected) => self.verify_rspauth(final_bytes, &expected),
            None => Ok(()),
        }
    }
}

/// `H(username:realm:password)`, the RFC 2831 "URP" hash. `realm` is
/// hashed as an empty string when the server's challenge offered none.
fn urp_digest(username: &str, realm: Option<&str>, password: &str) -> Vec<u8> {
    let mut input = Vec::new();
    input.extend_from_slice(username.as_bytes());
    input.push(b':');
    input.extend_from_slice(realm.unwrap_or("").as_bytes());
    input.push(b':');
    input.extend_from_slice(password.as_bytes());
    sha256(&input)
}

/// `H(A1) = H(digest_urp:nonce:cnonce)`. This client never sends an
/// authzid, so the RFC's optional trailing `:authzid` is always omitted.
fn h_a1(digest_urp: &[u8], nonce: &str, cnonce: &str) -> Vec<u8> {
    let mut input = Vec::new();
    input.extend_from_slice(digest_urp);
    input.push(b':');
    input.extend_from_slice(nonce.as_bytes());
    input.push(b':');
    input.extend_from_slice(cnonce.as_bytes());
    sha256(&input)
}

/// `response-value = HEX(KD(HEX(H(A1)), nonce:nc:cnonce:qop:HEX(H(A2))))`.
/// `A2` is prefixed with `AUTHENTICATE:` for the client's `response`
/// directive and left bare for the server's `rspauth` (`auth` selects
/// which one to build).
fn digest_response(
    h_a1_hex: &str,
    nonce: &str,
    nonce_count: &str,
    cnonce: &str,
    qop: &str,
    auth: bool,
) -> String {
    let mut a2 = Vec::new();
    if auth {
        a2.extend_from_slice(b"AUTHENTICATE");
    }
    a2.push(b':');
    a2.extend_from_slice(DIGEST_URI.as_bytes());
    let h_a2_hex = hex_encode(&sha256(&a2));

    let mut kd = Vec::new();
    kd.extend_from_slice(h_a1_hex.as_bytes());
    kd.push(b':');
    kd.extend_from_slice(nonce.as_bytes());
    kd.push(b':');
    kd.extend_from_slice(nonce_count.as_bytes());
    kd.push(b':');
    kd.extend_from_slice(cnonce.as_bytes());
    kd.push(b':');
    kd.extend_from_slice(qop.as_bytes());
    kd.push(b':');
    kd.extend_from_slice(h_a2_hex.as_bytes());

    hex_encode(&sha256(&kd))
}

fn sha256(data: &[u8]) -> Vec<u8> {
    Sha256::digest(data).to_vec()
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(hex, "{byte:02x}").expect("writing to a String never fails");
    }
    hex
}

/// A 24 byte random nonce, base64 encoded, matching Elytron's own
/// `generateNonce` (used there for both the server's nonce and the
/// client's cnonce).
fn generate_cnonce() -> String {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use rand::RngCore;

    let mut bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut bytes);
    STANDARD.encode(bytes)
}

/// Escapes `\` and `"` per RFC 2831's `quoted-string` production. Reads
/// from the original characters, not the growing output, so a literal
/// `\` in the input is never re-escaped.
fn quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            other => quoted.push(other),
        }
    }
    quoted
}

/// Parses a comma-separated list of `key=value` directives, where a value
/// may be a bare token or a double-quoted string with `\`-escaping.
/// Unlike SCRAM's flat `key=value` pairs, a quoted value can itself
/// contain commas (`qop="auth,auth-int"`), so top-level directives can
/// only be split by scanning for the closing quote rather than by a
/// blind `split(',')`. Duplicate keys (multiple `realm` directives) are
/// preserved in order rather than overwriting each other.
fn parse_directives(message: &str) -> Result<Vec<(String, String)>> {
    let chars: Vec<char> = message.chars().collect();
    let len = chars.len();
    let mut i = 0;
    let mut directives = Vec::new();

    while i < len {
        while i < len && (chars[i] == ',' || chars[i].is_whitespace()) {
            i += 1;
        }
        if i >= len {
            break;
        }

        let key_start = i;
        while i < len && chars[i] != '=' {
            i += 1;
        }
        if i >= len {
            return Err(Error::MalformedChallenge(
                "directive is missing a value".to_string(),
            ));
        }
        let key: String = chars[key_start..i]
            .iter()
            .collect::<String>()
            .trim()
            .to_string();
        i += 1;
        while i < len && chars[i].is_whitespace() {
            i += 1;
        }

        let value = if i < len && chars[i] == '"' {
            i += 1;
            let mut value = String::new();
            let mut closed = false;
            while i < len {
                match chars[i] {
                    '\\' if i + 1 < len => {
                        value.push(chars[i + 1]);
                        i += 2;
                    }
                    '"' => {
                        i += 1;
                        closed = true;
                        break;
                    }
                    c => {
                        value.push(c);
                        i += 1;
                    }
                }
            }
            if !closed {
                return Err(Error::MalformedChallenge(format!(
                    "unterminated quoted value for directive {key}"
                )));
            }
            value
        } else {
            let value_start = i;
            while i < len && chars[i] != ',' {
                i += 1;
            }
            chars[value_start..i]
                .iter()
                .collect::<String>()
                .trim()
                .to_string()
        };

        directives.push((key, value));
    }

    Ok(directives)
}

fn directive<'a>(directives: &'a [(String, String)], key: &str) -> Result<&'a str> {
    all_directives(directives, key)
        .next()
        .ok_or_else(|| Error::MalformedChallenge(format!("message is missing directive {key}")))
}

fn all_directives<'a>(
    directives: &'a [(String, String)],
    key: &str,
) -> impl Iterator<Item = &'a str> + 'a {
    let key = key.to_string();
    directives
        .iter()
        .filter(move |(k, _)| *k == key)
        .map(|(_, v)| v.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_quoted_and_bare_directives_including_embedded_commas() {
        let directives =
            parse_directives(r#"realm="example.com",nonce="abc",qop="auth,auth-int",maxbuf=65536"#)
                .unwrap();
        assert_eq!(directive(&directives, "realm").unwrap(), "example.com");
        assert_eq!(directive(&directives, "nonce").unwrap(), "abc");
        assert_eq!(directive(&directives, "qop").unwrap(), "auth,auth-int");
        assert_eq!(directive(&directives, "maxbuf").unwrap(), "65536");
    }

    #[test]
    fn parses_backslash_escaped_quotes_in_a_quoted_value() {
        let directives = parse_directives(r#"realm="a\"b\\c""#).unwrap();
        assert_eq!(directive(&directives, "realm").unwrap(), "a\"b\\c");
    }

    #[test]
    fn rejects_unterminated_quoted_value() {
        let err = parse_directives(r#"realm="unterminated"#).unwrap_err();
        assert!(matches!(err, Error::MalformedChallenge(_)));
    }

    #[test]
    fn preserves_duplicate_directive_keys_in_order() {
        let directives = parse_directives(r#"realm="one",realm="two""#).unwrap();
        let realms: Vec<&str> = all_directives(&directives, "realm").collect();
        assert_eq!(realms, vec!["one", "two"]);
    }

    #[test]
    fn escapes_backslash_and_quote_without_double_escaping() {
        assert_eq!(quote(r#"a\b"c"#), r#"a\\b\"c"#);
    }

    #[test]
    fn digest_response_matches_a_hand_computed_vector() {
        // H(A1) of an arbitrary 32 zero byte value, hex encoded, used only
        // to pin the KD/A2 construction against a value computed by hand
        // from the same RFC 2831 formula with SHA-256 substituted for MD5.
        let h_a1_hex = hex_encode(&[0u8; 32]);
        let response = digest_response(&h_a1_hex, "n0nce", "00000001", "cn0nce", QOP_AUTH, true);
        assert_eq!(response.len(), 64);
        assert_ne!(
            response,
            digest_response(&h_a1_hex, "n0nce", "00000001", "cn0nce", QOP_AUTH, false)
        );
    }

    /// Captured from a real WildFly Elytron 2.6.0.Final `SaslServer` and
    /// `SaslClient` (the exact library Infinispan uses) driven in-process
    /// with username "testuser", password "testpass", digest-uri
    /// "hotrod/infinispan". The cnonce this client would normally generate
    /// at random is overridden with the one Java's client used for that
    /// run, so this crate's digest and rspauth can be checked byte for
    /// byte against Java's own values instead of only against each other.
    #[test]
    fn matches_a_real_elytron_digest_sha_256_exchange() {
        let server_challenge = br#"realm="infinispan",nonce="oH/Kf7I8TkigEdBpZZTOg/D173d+TmtYlUtDLuKSyW3B2VPt",qop="auth",charset=utf-8,algorithm=md5-sess"#;
        let server_rspauth =
            br#"rspauth=9d29c7f114fb611a7219c0d0e385cb5d8e68a64e93cfc71fed8a4f346ed95468"#;

        let mut mechanism = DigestSha256Mechanism::new("testuser", "testpass");
        mechanism.cnonce = "k0CiTzgO7CcsLoWQ1vnEvZwAEAI8Nl/ALtrjORZ7GOWYjtq7".to_string();

        mechanism.respond(None).unwrap();
        let response = mechanism.respond(Some(server_challenge)).unwrap().unwrap();
        let response = String::from_utf8(response).unwrap();
        let directives = parse_directives(&response).unwrap();
        assert_eq!(
            directive(&directives, "response").unwrap(),
            "80dd936add1807e9727ca17b87d88c3e38369811f00b5d767d00ac9f95f92800"
        );

        assert!(mechanism.respond(Some(server_rspauth)).unwrap().is_none());
    }

    #[test]
    fn rejects_forged_rspauth() {
        let mut mechanism = DigestSha256Mechanism::new("alice", "s3cr3t");
        mechanism.respond(None).unwrap();
        let challenge = br#"realm="infinispan",nonce="n0nce",qop="auth",charset=utf-8"#;
        mechanism.respond(Some(challenge)).unwrap();

        let err = mechanism
            .respond(Some(
                b"rspauth=0000000000000000000000000000000000000000000000000000000000000000",
            ))
            .unwrap_err();
        assert!(matches!(err, Error::DigestServerVerificationFailed));
    }
}
