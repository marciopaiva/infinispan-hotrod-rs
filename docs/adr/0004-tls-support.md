# ADR 0004: TLS support for connections to the Hot Rod server

## Status

Accepted

## Context

`HotRodConnection` and `HotRodCluster` only ever open a plain
`tokio::net::TcpStream` (`connection.rs`, `stream: BufStream<TcpStream>`).
Nothing in the wire protocol itself carries encryption; TLS at the Hot
Rod layer is the same idea as HTTPS on top of HTTP: the client wraps the
TCP stream in a TLS session before the first Hot Rod frame goes out. The
Java client supports this through `SslConfiguration`: a trust store, an
optional client certificate and key for mutual TLS, and SNI.

Three questions need an answer before implementation starts, each with a
real cost if gotten wrong:

**Which TLS library.** The two realistic choices are `rustls` (pure
Rust) and `native-tls`/`openssl` (a safe wrapper around the platform's
TLS library, which on Linux means linking OpenSSL). `CLAUDE.md` section 2
already rules out "no unsafe FFI, no C++ client wrapped" for
`hotrod-protocol`. A safe wrapper crate does not violate that rule
literally, since the `unsafe` lives inside the wrapper, not in this
crate. But it does pull in a C library and its own build-time
requirements (a system OpenSSL, or vendoring it), which is the same kind
of platform-dependency cost the project already chose to avoid for
GSSAPI in ADR 0002. `rustls` has no system dependency, is maintained by
the same organization behind `tokio`, and `tokio-rustls` is the
established async integration.

**How a caller enables it.** `HotRodConnection::connect` and
`connect_with_timeout` take `impl ToSocketAddrs`, and `HotRodCluster`
opens further connections on its own, both at initial connect
(`connect`) and later when reconnecting an evicted node or a topology
update introduces a node the client has not talked to yet
(`ensure_connection_impl` in `cluster.rs`). Whatever TLS configuration a
caller supplies has to be stored on the connection/cluster and reused
for every one of those internal reconnects, not just the first one, the
same way `self.auth` is already stored and replayed today.

**Hostname verification for cluster-discovered nodes.** TLS certificate
verification needs a hostname, not just a `SocketAddr`. The seed address
a caller passes to `connect`/`connect_tls` can carry a hostname if the
caller supplies one, but `HotRodCluster` learns about the *other* nodes
in the cluster from the server's topology update, which is a bare list
of IP and port (`topology.rs`), with no hostname attached. Real
Infinispan deployments commonly issue one certificate per node with the
node's own hostname in it, which this client would have no way to
check against an IP-only address. This is not a detail to paper over
silently: getting it wrong either rejects a legitimate cluster (too
strict) or silently accepts a node the client cannot actually verify
(too lax).

## Options considered

1. **`rustls` + an internal `Transport` enum** (`Plain(TcpStream)` /
   `Tls(TlsStream<TcpStream>)`) inside `HotRodConnection`, selected by
   which constructor the caller calls (`connect` vs `connect_tls`).
   Keeps the existing plain-TCP constructors and their behavior
   untouched, additive only, same pattern ADR 0003 used for
   `HotRodCluster` alongside `HotRodConnection`.
2. **Same as (1), but native-tls/openssl instead of rustls.** Rejected:
   reintroduces exactly the C-library platform dependency ADR 0002 chose
   not to take on for GSSAPI, for no offsetting benefit here.
3. **Make `HotRodConnection` generic over `AsyncRead + AsyncWrite`**
   instead of an enum. Rejected for now: it would leak a generic
   parameter into `HotRodCluster` and every public signature that holds
   a connection, a much larger API change than the feature needs. The
   enum keeps the type erasure internal.

## Decision

Go with option 1: `rustls`, plumbed in additively.

* New dependencies: `rustls`, `tokio-rustls`, and a trust-store source
  (`rustls-native-certs` to use the OS trust store by default, since
  that matches what most deployments expect without extra setup).
* A `TlsConfig` struct: server name for verification (separate from the
  connection address, so a caller can connect by IP but still verify
  against the certificate's hostname), an optional custom CA bundle for
  self-signed/dev certificates, and an optional client certificate and
  key for mutual TLS. No option to disable verification entirely: a
  footgun like that belongs in a test-only helper behind `#[cfg(test)]`
  at most, not in the public API.
* `HotRodConnection::connect_tls` / `connect_tls_with_timeout`, mirroring
  the existing plain constructors, taking a `TlsConfig` alongside the
  address.
* `HotRodCluster::connect_tls`, storing the `TlsConfig` the same way it
  already stores `self.auth`, and reusing it in `ensure_connection_impl`
  for every reconnect and every newly discovered node.

**Cluster-discovered nodes are verified against the configured CA only,
not by hostname.** The seed connection gets full verification, hostname
included, since the caller explicitly configured an address for it. A
node `HotRodCluster` first learns about from a topology update has no
hostname to check, only an IP, so pinning verification to the seed's
server name was rejected: it only works if every node in the cluster
shares one certificate (a wildcard, or one SAN listing every node), and
silently breaks failover the moment an operator issues a certificate per
node instead, which is a common and legitimate PKI setup. Requiring a
valid chain to the configured CA, without a hostname match, keeps that
failover working under either setup. It is a real trust boundary, not a
skipped check: a peer still needs a certificate the configured CA
actually issued, the same trust model internal service meshes use for
peers whose identity is inherently dynamic. This needs to be documented
plainly on `TlsConfig` so a caller does not assume topology-discovered
peers get the same hostname-checked verification the seed does.

## Consequences

* `hotrod-protocol`'s dependency list grows by `rustls`, `tokio-rustls`,
  and `rustls-native-certs`, all pure Rust.
* The public API gains `TlsConfig`, `HotRodConnection::connect_tls` /
  `connect_tls_with_timeout`, and `HotRodCluster::connect_tls`, alongside
  the unchanged plain-TCP constructors.
* `TlsConfig`'s doc comment must state clearly that cluster-discovered
  peers are authenticated by CA trust, not by hostname identity, since
  that is weaker than what the seed connection gets and is exactly the
  kind of thing a caller would otherwise assume works fully.
