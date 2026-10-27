//! SASL authentication: the server's `AuthMechList`/`Auth` exchange.
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): see `connection/multimap.rs`'s module
//! docs for the mechanism that lets this `impl HotRodConnection`
//! block reach every private field/method it needs with no
//! visibility change. No dedicated tests here: each mechanism's wire
//! exchange is covered by `sasl.rs`/`digest.rs`/`scram.rs`'s own unit
//! tests, and the full authenticated round trip by `client.rs`'s and
//! `tests/live_server.rs`'s.

use crate::digest::DigestSha256Mechanism;
use crate::error::{Error, Result};
use crate::header::OpCode;
use crate::sasl::{OAuthBearerMechanism, PlainMechanism, SaslMechanism};
use crate::scram::ScramSha512Mechanism;
use crate::wire::{read_array, read_string, write_array};

use super::{with_timeout, HotRodConnection};

impl HotRodConnection {
    /// Authenticates the connection using SASL PLAIN. Must be called before
    /// any cache operation if the server requires authentication.
    ///
    /// `authzid` is the authorization id; pass an empty string unless the
    /// server is configured to distinguish it from `authcid`.
    pub async fn authenticate_plain(
        &mut self,
        authzid: &str,
        authcid: &str,
        password: &str,
    ) -> Result<()> {
        self.run_sasl(PlainMechanism::new(authzid, authcid, password))
            .await
    }

    /// Authenticates the connection using SASL SCRAM-SHA-512 (RFC 5802).
    pub async fn authenticate_scram(&mut self, authcid: &str, password: &str) -> Result<()> {
        self.run_sasl(ScramSha512Mechanism::new(authcid, password))
            .await
    }

    /// Authenticates the connection using SASL DIGEST-SHA-256, Elytron's
    /// generalization of RFC 2831 DIGEST-MD5. The digest-uri's server-name
    /// half is fixed to `infinispan`; see `digest.rs` for why.
    pub async fn authenticate_digest(&mut self, authcid: &str, password: &str) -> Result<()> {
        self.run_sasl(DigestSha256Mechanism::new(authcid, password))
            .await
    }

    /// Authenticates the connection using SASL OAUTHBEARER (RFC 7628) with
    /// a bearer token obtained elsewhere, typically from an OIDC provider.
    ///
    /// This mechanism has unit test coverage only: live-server coverage
    /// needs a token-backed realm the CI fixture does not provide yet, see
    /// `sasl.rs`.
    pub async fn authenticate_oauthbearer(&mut self, authzid: &str, token: &str) -> Result<()> {
        self.run_sasl(OAuthBearerMechanism::new(authzid, token))
            .await
    }

    /// Drives a `SaslMechanism` through the server's `AuthMechList`/`Auth`
    /// exchange: confirms the mechanism is offered, then loops sending
    /// responses and feeding back challenges until the server marks the
    /// exchange complete.
    async fn run_sasl(&mut self, mut mechanism: impl SaslMechanism) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let empty_cache_name: Vec<u8> = Vec::new();

            self.write_and_read_header(&empty_cache_name, OpCode::AuthMechList, &[])
                .await?;
            let mech_count = crate::varint::read_vint(&mut self.stream).await?;
            let mut offered = Vec::with_capacity(mech_count as usize);
            for _ in 0..mech_count {
                offered.push(read_string(&mut self.stream).await?);
            }
            if !offered.iter().any(|mech| mech == mechanism.name()) {
                return Err(Error::UnsupportedSaslMechanism(
                    mechanism.name().to_string(),
                ));
            }

            let mut challenge: Option<Vec<u8>> = None;
            loop {
                let Some(response) = mechanism.respond(challenge.as_deref())? else {
                    return Ok(());
                };
                let mut body = Vec::new();
                write_array(&mut body, mechanism.name().as_bytes());
                write_array(&mut body, &response);
                self.write_and_read_header(&empty_cache_name, OpCode::Auth, &body)
                    .await?;

                let complete = tokio::io::AsyncReadExt::read_u8(&mut self.stream).await? > 0;
                let bytes = read_array(&mut self.stream).await?;
                if complete {
                    mechanism.finish(&bytes)?;
                    return Ok(());
                }
                challenge = Some(bytes);
            }
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }
}
