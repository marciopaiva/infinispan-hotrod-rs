//! TLS transport for connections to the Hot Rod server (ADR 0004,
//! `docs/adr/0004-tls-support.md`).
//!
//! A node discovered through a topology update carries only an address,
//! never a hostname (see `topology::TopologyServer`), so a connection
//! opened to one cannot be verified by hostname the way the seed
//! connection is. `connect_with` in `connection.rs` threads a
//! `verify_hostname` flag down to `build_client_config` for exactly this
//! reason: `true` for the seed, `false` for every node a `HotRodCluster`
//! opens afterward. Either way the certificate must still chain to the
//! configured CA; only the hostname/IP match is skipped.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{verify_server_cert_signed_by_trust_anchor, WebPkiServerVerifier};
use rustls::pki_types::{
    CertificateDer, PrivateKeyDer, ServerName, SignatureVerificationAlgorithm, UnixTime,
};
use rustls::server::ParsedCertificate;
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

use crate::error::{Error, Result};

/// Configuration for a TLS-protected connection to a Hot Rod server.
///
/// There is no field here to disable certificate verification: ADR 0004
/// rejected that as a public-API footgun. To test against a self-signed
/// certificate, point `ca_certificate` at that certificate (or its issuing
/// CA) instead of relying on the OS trust store.
#[derive(Clone, Default)]
pub struct TlsConfig {
    /// Verified against the seed connection's certificate. Not derived
    /// from the connect address, since that may be a bare IP with no
    /// hostname to check. Cluster nodes discovered later through a
    /// topology update are, by contrast, verified only against
    /// `ca_certificate` (or the OS trust store), never by this name: see
    /// the module docs.
    pub server_name: String,
    /// PEM-encoded CA certificate(s) trusted for the server's chain.
    /// `None` defers to the operating system's trust store via
    /// `rustls-native-certs`.
    pub ca_certificate: Option<Vec<u8>>,
    /// PEM-encoded certificate chain and private key presented for mutual
    /// TLS. `None` skips client authentication.
    pub client_identity: Option<(Vec<u8>, Vec<u8>)>,
}

/// Either half of a plain or TLS-wrapped connection, so the rest of
/// `connection.rs` can read and write through `BufStream<Transport>`
/// without knowing which one it has. `Tls` is boxed: `TlsStream` carries a
/// full `rustls` session buffer, and leaving it unboxed would make every
/// `Transport::Plain` pay for that size too.
pub(crate) enum Transport {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Transport::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Transport::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Transport::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Transport::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Transport::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Verifies the certificate chain against the configured trust anchors,
/// same as the default `WebPkiServerVerifier`, but skips the
/// hostname/IP match. Used for cluster nodes discovered through a
/// topology update, which carry no hostname to check against: see the
/// module docs and ADR 0004's decision.
///
/// `WebPkiServerVerifier` has no public way to verify a chain without also
/// matching a name, so this calls the lower-level
/// `verify_server_cert_signed_by_trust_anchor` directly instead and never
/// calls `verify_server_name` at all. `inner` is kept only to delegate the
/// signature-checking methods below, which do not involve a hostname.
#[derive(Debug)]
struct ChainOnlyVerifier {
    inner: Arc<WebPkiServerVerifier>,
    roots: Arc<RootCertStore>,
    supported_algs: &'static [&'static dyn SignatureVerificationAlgorithm],
}

impl ServerCertVerifier for ChainOnlyVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        let cert = ParsedCertificate::try_from(end_entity)?;
        verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.supported_algs,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn load_root_store(ca_certificate: Option<&[u8]>) -> Result<RootCertStore> {
    let mut store = RootCertStore::empty();
    match ca_certificate {
        Some(pem) => {
            for cert in rustls_pemfile::certs(&mut std::io::Cursor::new(pem)) {
                let cert = cert.map_err(|err| {
                    Error::InvalidTlsMaterial(format!("invalid CA certificate PEM: {err}"))
                })?;
                store.add(cert).map_err(|err| {
                    Error::InvalidTlsMaterial(format!("invalid CA certificate: {err}"))
                })?;
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            if let Some(err) = native.errors.first() {
                return Err(Error::InvalidTlsMaterial(format!(
                    "failed to load OS trust store: {err}"
                )));
            }
            for cert in native.certs {
                store.add(cert).map_err(|err| {
                    Error::InvalidTlsMaterial(format!("invalid OS trust store entry: {err}"))
                })?;
            }
        }
    }
    Ok(store)
}

fn load_client_identity(
    client_identity: &(Vec<u8>, Vec<u8>),
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let (cert_pem, key_pem) = client_identity;
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem))
            .collect::<std::result::Result<_, _>>()
            .map_err(|err| {
                Error::InvalidTlsMaterial(format!("invalid client certificate PEM: {err}"))
            })?;
    if certs.is_empty() {
        return Err(Error::InvalidTlsMaterial(
            "client_identity certificate PEM contains no certificate".to_string(),
        ));
    }
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(key_pem))
        .map_err(|err| Error::InvalidTlsMaterial(format!("invalid client key PEM: {err}")))?
        .ok_or_else(|| {
            Error::InvalidTlsMaterial("client_identity key PEM contains no private key".to_string())
        })?;
    Ok((certs, key))
}

/// Builds the `rustls::ClientConfig` for one connection attempt, applying
/// `verify_hostname` as described on the module docs.
fn build_client_config(config: &TlsConfig, verify_hostname: bool) -> Result<ClientConfig> {
    let root_store = Arc::new(load_root_store(config.ca_certificate.as_deref())?);

    let builder = ClientConfig::builder();
    let builder = if verify_hostname {
        builder.with_root_certificates(root_store)
    } else {
        let inner = WebPkiServerVerifier::builder(root_store.clone())
            .build()
            .map_err(|err| Error::TlsHandshake(format!("failed to build verifier: {err}")))?;
        let supported_algs = rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .all;
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(ChainOnlyVerifier {
                inner,
                roots: root_store,
                supported_algs,
            }))
    };

    let client_config = match &config.client_identity {
        Some(identity) => {
            let (certs, key) = load_client_identity(identity)?;
            builder.with_client_auth_cert(certs, key).map_err(|err| {
                Error::InvalidTlsMaterial(format!("invalid client identity: {err}"))
            })?
        }
        None => builder.with_no_client_auth(),
    };
    Ok(client_config)
}

/// Completes a TLS handshake over an already-connected `TcpStream`,
/// verifying the server's certificate by hostname when `verify_hostname`
/// is `true`, or against the configured CA chain alone otherwise.
pub(crate) async fn handshake(
    tcp: TcpStream,
    config: &TlsConfig,
    verify_hostname: bool,
) -> Result<Transport> {
    let client_config = build_client_config(config, verify_hostname)?;
    let connector = TlsConnector::from(Arc::new(client_config));
    let server_name = ServerName::try_from(config.server_name.clone())
        .map_err(|err| Error::InvalidTlsMaterial(format!("invalid server_name: {err}")))?;
    let stream = connector
        .connect(server_name, tcp)
        .await
        .map_err(|err| Error::TlsHandshake(err.to_string()))?;
    Ok(Transport::Tls(Box::new(stream)))
}

#[cfg(test)]
mod tests {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
    use rustls::server::WebPkiClientVerifier;
    use rustls::ServerConfig;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// A self-signed CA, and the parameters/key needed to sign further
    /// certificates with it via `Issuer::from_params`.
    struct TestCa {
        cert_pem: String,
        params: CertificateParams,
        key: KeyPair,
    }

    fn make_ca() -> TestCa {
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key = KeyPair::generate().unwrap();
        let cert_pem = params.self_signed(&key).unwrap().pem();
        TestCa {
            cert_pem,
            params,
            key,
        }
    }

    /// A leaf certificate signed by `ca`, valid for `san` (a DNS name or IP).
    fn make_leaf(ca: &TestCa, san: &str) -> (String, String) {
        let params = CertificateParams::new(vec![san.to_string()]).unwrap();
        let key = KeyPair::generate().unwrap();
        let issuer = Issuer::from_params(&ca.params, &ca.key);
        let cert_pem = params.signed_by(&key, &issuer).unwrap().pem();
        (cert_pem, key.serialize_pem())
    }

    fn server_config(cert_pem: &str, key_pem: &str) -> ServerConfig {
        let (certs, key) =
            load_client_identity(&(cert_pem.as_bytes().to_vec(), key_pem.as_bytes().to_vec()))
                .unwrap();
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap()
    }

    /// Same as `server_config`, but requiring the client to present a
    /// certificate signed by `client_ca`.
    fn server_config_requiring_client_cert(
        cert_pem: &str,
        key_pem: &str,
        client_ca_pem: &str,
    ) -> ServerConfig {
        let (certs, key) =
            load_client_identity(&(cert_pem.as_bytes().to_vec(), key_pem.as_bytes().to_vec()))
                .unwrap();
        let client_roots = Arc::new(load_root_store(Some(client_ca_pem.as_bytes())).unwrap());
        let client_verifier = WebPkiClientVerifier::builder(client_roots).build().unwrap();
        ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .unwrap()
    }

    /// Accepts exactly one TLS connection on `listener` with `server_config`,
    /// then writes `b"ok"` and shuts the stream down. The returned handle
    /// carries whatever the accept/handshake/write sequence produced,
    /// including a rejected handshake (e.g. a missing client certificate),
    /// since a test exercising a failing handshake expects this side to
    /// fail too.
    fn spawn_server(
        listener: TcpListener,
        server_config: ServerConfig,
    ) -> tokio::task::JoinHandle<std::io::Result<()>> {
        tokio::spawn(async move {
            let (tcp, _peer) = listener.accept().await?;
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
            let mut stream = acceptor.accept(tcp).await?;
            stream.write_all(b"ok").await?;
            stream.shutdown().await?;
            Ok(())
        })
    }

    /// Same as `Result::unwrap_err`, but without requiring `Transport: Debug`
    /// (it isn't, since `TlsStream` doesn't implement it either).
    fn expect_err(result: Result<Transport>) -> Error {
        match result {
            Ok(_) => panic!("expected the handshake to fail, but it succeeded"),
            Err(err) => err,
        }
    }

    #[tokio::test]
    async fn handshake_succeeds_with_a_trusted_ca_and_matching_hostname() {
        let ca = make_ca();
        let (leaf_cert, leaf_key) = make_leaf(&ca, "localhost");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_done = spawn_server(listener, server_config(&leaf_cert, &leaf_key));

        let tcp = TcpStream::connect(addr).await.unwrap();
        let config = TlsConfig {
            server_name: "localhost".to_string(),
            ca_certificate: Some(ca.cert_pem.into_bytes()),
            client_identity: None,
        };
        let mut transport = handshake(tcp, &config, true).await.unwrap();

        let mut buf = [0u8; 2];
        transport.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ok");
        server_done.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn handshake_fails_when_the_certificate_is_not_signed_by_the_configured_ca() {
        let ca = make_ca();
        let other_ca = make_ca();
        let (leaf_cert, leaf_key) = make_leaf(&other_ca, "localhost");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        spawn_server(listener, server_config(&leaf_cert, &leaf_key));

        let tcp = TcpStream::connect(addr).await.unwrap();
        let config = TlsConfig {
            server_name: "localhost".to_string(),
            ca_certificate: Some(ca.cert_pem.into_bytes()),
            client_identity: None,
        };
        let err = expect_err(handshake(tcp, &config, true).await);
        assert!(matches!(err, Error::TlsHandshake(_)), "{err:?}");
    }

    #[tokio::test]
    async fn hostname_mismatch_is_rejected_for_the_seed_but_skipped_for_a_topology_node() {
        let ca = make_ca();
        // The leaf is valid for a name that never matches `server_name`
        // below, mirroring a topology-discovered node whose certificate was
        // issued for its own hostname, not the seed's.
        let (leaf_cert, leaf_key) = make_leaf(&ca, "some-other-node.example");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        spawn_server(listener, server_config(&leaf_cert, &leaf_key));
        let tcp = TcpStream::connect(addr).await.unwrap();
        let config = TlsConfig {
            server_name: "localhost".to_string(),
            ca_certificate: Some(ca.cert_pem.clone().into_bytes()),
            client_identity: None,
        };
        let err = expect_err(handshake(tcp, &config, true).await);
        assert!(matches!(err, Error::TlsHandshake(_)), "{err:?}");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_done = spawn_server(listener, server_config(&leaf_cert, &leaf_key));
        let tcp = TcpStream::connect(addr).await.unwrap();
        let mut transport = handshake(tcp, &config, false).await.unwrap();

        let mut buf = [0u8; 2];
        transport.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ok");
        server_done.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn mutual_tls_is_rejected_without_a_client_certificate_and_accepted_with_one() {
        let server_ca = make_ca();
        let (server_cert, server_key) = make_leaf(&server_ca, "localhost");
        let client_ca = make_ca();
        let (client_cert, client_key) = make_leaf(&client_ca, "test-client");

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_done = spawn_server(
            listener,
            server_config_requiring_client_cert(&server_cert, &server_key, &client_ca.cert_pem),
        );
        let tcp = TcpStream::connect(addr).await.unwrap();
        let config_without_client_cert = TlsConfig {
            server_name: "localhost".to_string(),
            ca_certificate: Some(server_ca.cert_pem.clone().into_bytes()),
            client_identity: None,
        };
        // TLS 1.3's client finishes its own handshake before it can learn
        // whether the server accepted its (here, absent) certificate, so
        // `handshake` itself may return `Ok` or `Err` depending on timing.
        // Either way the connection must be unusable: if it came back
        // `Ok`, the rejection surfaces on the first read instead, once the
        // server has processed the client's Certificate message and
        // aborted with an alert.
        if let Ok(mut transport) = handshake(tcp, &config_without_client_cert, true).await {
            let mut buf = [0u8; 2];
            assert!(transport.read_exact(&mut buf).await.is_err());
        }
        // The server side of a rejected handshake fails too: assert on it so
        // a change that makes the server accept unauthenticated clients
        // (defeating the point of this test) shows up here, not just as a
        // client-side symptom.
        assert!(server_done.await.unwrap().is_err());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_done = spawn_server(
            listener,
            server_config_requiring_client_cert(&server_cert, &server_key, &client_ca.cert_pem),
        );
        let tcp = TcpStream::connect(addr).await.unwrap();
        let config_with_client_cert = TlsConfig {
            server_name: "localhost".to_string(),
            ca_certificate: Some(server_ca.cert_pem.into_bytes()),
            client_identity: Some((client_cert.into_bytes(), client_key.into_bytes())),
        };
        let mut transport = handshake(tcp, &config_with_client_cert, true)
            .await
            .unwrap();

        let mut buf = [0u8; 2];
        transport.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ok");
        server_done.await.unwrap().unwrap();
    }
}
