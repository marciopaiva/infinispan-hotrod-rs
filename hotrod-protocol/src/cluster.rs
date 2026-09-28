//! `HotRodCluster`: a hash-aware routing client bound to one cache, spread
//! across a pool of per-node connections.
//!
//! This is additive alongside `HotRodConnection` (see
//! `docs/adr/0003-hash-aware-routing-scope.md`): `HotRodConnection` remains
//! the single-connection, no-topology client from phase 1, unchanged.
//! `HotRodCluster` instead advertises `HashDistributionAware` intelligence,
//! tracks the topology the server reports, and routes each operation to the
//! segment's primary owner, computed locally with the same `MurmurHash3`
//! variant the server uses.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;

use tokio::net::lookup_host;

use crate::connection::{HotRodConnection, VersionedResult, VersionedValue, DEFAULT_TOPOLOGY_ID};
use crate::error::{Error, Result};
use crate::hash;
use crate::topology::TopologyServer;
use crate::wire::Expiration;

/// Credentials for one SASL mechanism, kept so a connection opened later to
/// a newly discovered node can be authenticated the same way as the seed
/// connection, without the caller authenticating it by hand.
enum AuthMethod {
    Plain {
        authzid: String,
        authcid: String,
        password: String,
    },
    Scram {
        authcid: String,
        password: String,
    },
    Digest {
        authcid: String,
        password: String,
    },
    OAuthBearer {
        authzid: String,
        token: String,
    },
}

impl AuthMethod {
    async fn authenticate(&self, conn: &mut HotRodConnection) -> Result<()> {
        match self {
            AuthMethod::Plain {
                authzid,
                authcid,
                password,
            } => conn.authenticate_plain(authzid, authcid, password).await,
            AuthMethod::Scram { authcid, password } => {
                conn.authenticate_scram(authcid, password).await
            }
            AuthMethod::Digest { authcid, password } => {
                conn.authenticate_digest(authcid, password).await
            }
            AuthMethod::OAuthBearer { authzid, token } => {
                conn.authenticate_oauthbearer(authzid, token).await
            }
        }
    }
}

/// The routing data from the most recently applied topology update.
struct ClusterTopology {
    servers: Vec<TopologyServer>,
    hash_function_version: u8,
    /// Index `n` is segment `n`'s owners, as indices into `servers`, primary
    /// owner first.
    segment_owners: Vec<Vec<u32>>,
}

/// A cache client that tracks cluster topology and routes each operation to
/// the segment's primary owner instead of relying on server-side
/// redirects, matching the Java client's intelligent routing.
///
/// Bound to one cache, like `HotRodConnection`. A single instance keeps one
/// pooled connection per node it has needed to talk to so far.
pub struct HotRodCluster {
    cache_name: String,
    /// The seed this instance is currently connected to; also the fallback
    /// used before a topology has arrived and the retry target when a
    /// computed owner's connection fails.
    active_seed_addr: SocketAddr,
    topology_id: i32,
    topology: Option<ClusterTopology>,
    connections: HashMap<SocketAddr, HotRodConnection>,
    auth: Option<AuthMethod>,
}

/// One of `HotRodConnection`'s cache operations, with its arguments owned
/// so it can be replayed against a second connection on retry. `call`
/// dispatches on this instead of taking a generic closure: a closure
/// capturing owned data cannot satisfy the higher-ranked `for<'c> FnMut(&'c
/// mut HotRodConnection) -> _` bound that returning a future borrowed from
/// `&'c mut HotRodConnection` would need, since a closure body has a single
/// fixed lifetime, not one universally quantified per call.
enum Operation {
    Get(Vec<u8>),
    Put(Vec<u8>, Vec<u8>, Expiration, Expiration),
    PutIfAbsent(Vec<u8>, Vec<u8>, Expiration, Expiration),
    Replace(Vec<u8>, Vec<u8>, Expiration, Expiration),
    Remove(Vec<u8>),
    GetWithVersion(Vec<u8>),
    ReplaceIfUnmodified(Vec<u8>, Vec<u8>, u64, Expiration, Expiration),
    RemoveIfUnmodified(Vec<u8>, u64),
}

enum OperationResult {
    Get(Option<Vec<u8>>),
    Put,
    Bool(bool),
    GetWithVersion(Option<VersionedValue>),
    Versioned(VersionedResult),
}

async fn run_operation(conn: &mut HotRodConnection, op: &Operation) -> Result<OperationResult> {
    match op {
        Operation::Get(key) => Ok(OperationResult::Get(conn.get(key).await?)),
        Operation::Put(key, value, lifespan, max_idle) => {
            conn.put(key, value, *lifespan, *max_idle).await?;
            Ok(OperationResult::Put)
        }
        Operation::PutIfAbsent(key, value, lifespan, max_idle) => Ok(OperationResult::Bool(
            conn.put_if_absent(key, value, *lifespan, *max_idle).await?,
        )),
        Operation::Replace(key, value, lifespan, max_idle) => Ok(OperationResult::Bool(
            conn.replace(key, value, *lifespan, *max_idle).await?,
        )),
        Operation::Remove(key) => Ok(OperationResult::Bool(conn.remove(key).await?)),
        Operation::GetWithVersion(key) => Ok(OperationResult::GetWithVersion(
            conn.get_with_version(key).await?,
        )),
        Operation::ReplaceIfUnmodified(key, value, version, lifespan, max_idle) => {
            Ok(OperationResult::Versioned(
                conn.replace_if_unmodified(key, value, *version, *lifespan, *max_idle)
                    .await?,
            ))
        }
        Operation::RemoveIfUnmodified(key, version) => Ok(OperationResult::Versioned(
            conn.remove_if_unmodified(key, *version).await?,
        )),
    }
}

impl HotRodCluster {
    /// Connects to the first seed address that accepts a connection, in
    /// order, matching the Java client's failover-on-connect behavior. No
    /// topology is known yet at this point: every operation routes to this
    /// seed until the server's first response carries one.
    pub async fn connect(seed_addrs: &[SocketAddr], cache_name: &str) -> Result<Self> {
        let mut last_err: Option<Error> = None;
        for &addr in seed_addrs {
            match HotRodConnection::connect_hash_aware(addr, cache_name, DEFAULT_TOPOLOGY_ID).await
            {
                Ok(conn) => {
                    let mut connections = HashMap::new();
                    connections.insert(addr, conn);
                    return Ok(Self {
                        cache_name: cache_name.to_string(),
                        active_seed_addr: addr,
                        topology_id: DEFAULT_TOPOLOGY_ID,
                        topology: None,
                        connections,
                        auth: None,
                    });
                }
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no seed addresses provided",
            ))
        }))
    }

    /// Authenticates using SASL PLAIN. See `HotRodConnection::authenticate_plain`.
    pub async fn authenticate_plain(
        &mut self,
        authzid: &str,
        authcid: &str,
        password: &str,
    ) -> Result<()> {
        self.authenticate_with(AuthMethod::Plain {
            authzid: authzid.to_string(),
            authcid: authcid.to_string(),
            password: password.to_string(),
        })
        .await
    }

    /// Authenticates using SASL SCRAM-SHA-512. See `HotRodConnection::authenticate_scram`.
    pub async fn authenticate_scram(&mut self, authcid: &str, password: &str) -> Result<()> {
        self.authenticate_with(AuthMethod::Scram {
            authcid: authcid.to_string(),
            password: password.to_string(),
        })
        .await
    }

    /// Authenticates using SASL DIGEST-SHA-256. See `HotRodConnection::authenticate_digest`.
    pub async fn authenticate_digest(&mut self, authcid: &str, password: &str) -> Result<()> {
        self.authenticate_with(AuthMethod::Digest {
            authcid: authcid.to_string(),
            password: password.to_string(),
        })
        .await
    }

    /// Authenticates using SASL OAUTHBEARER. See `HotRodConnection::authenticate_oauthbearer`.
    pub async fn authenticate_oauthbearer(&mut self, authzid: &str, token: &str) -> Result<()> {
        self.authenticate_with(AuthMethod::OAuthBearer {
            authzid: authzid.to_string(),
            token: token.to_string(),
        })
        .await
    }

    async fn authenticate_with(&mut self, method: AuthMethod) -> Result<()> {
        let seed = self.active_seed_addr;
        let conn = self
            .connections
            .get_mut(&seed)
            .expect("connect() always leaves the seed connection in the pool");
        method.authenticate(conn).await?;
        self.auth = Some(method);
        Ok(())
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.call(key, Operation::Get(key.to_vec())).await? {
            OperationResult::Get(value) => Ok(value),
            _ => unreachable!("Operation::Get always yields OperationResult::Get"),
        }
    }

    pub async fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        let op = Operation::Put(key.to_vec(), value.to_vec(), lifespan, max_idle);
        match self.call(key, op).await? {
            OperationResult::Put => Ok(()),
            _ => unreachable!("Operation::Put always yields OperationResult::Put"),
        }
    }

    pub async fn put_if_absent(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<bool> {
        let op = Operation::PutIfAbsent(key.to_vec(), value.to_vec(), lifespan, max_idle);
        match self.call(key, op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => unreachable!("Operation::PutIfAbsent always yields OperationResult::Bool"),
        }
    }

    pub async fn replace(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<bool> {
        let op = Operation::Replace(key.to_vec(), value.to_vec(), lifespan, max_idle);
        match self.call(key, op).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => unreachable!("Operation::Replace always yields OperationResult::Bool"),
        }
    }

    pub async fn remove(&mut self, key: &[u8]) -> Result<bool> {
        match self.call(key, Operation::Remove(key.to_vec())).await? {
            OperationResult::Bool(value) => Ok(value),
            _ => unreachable!("Operation::Remove always yields OperationResult::Bool"),
        }
    }

    pub async fn get_with_version(&mut self, key: &[u8]) -> Result<Option<VersionedValue>> {
        let op = Operation::GetWithVersion(key.to_vec());
        match self.call(key, op).await? {
            OperationResult::GetWithVersion(value) => Ok(value),
            _ => unreachable!(
                "Operation::GetWithVersion always yields OperationResult::GetWithVersion"
            ),
        }
    }

    pub async fn replace_if_unmodified(
        &mut self,
        key: &[u8],
        value: &[u8],
        version: u64,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<VersionedResult> {
        let op = Operation::ReplaceIfUnmodified(
            key.to_vec(),
            value.to_vec(),
            version,
            lifespan,
            max_idle,
        );
        match self.call(key, op).await? {
            OperationResult::Versioned(value) => Ok(value),
            _ => unreachable!(
                "Operation::ReplaceIfUnmodified always yields OperationResult::Versioned"
            ),
        }
    }

    pub async fn remove_if_unmodified(
        &mut self,
        key: &[u8],
        version: u64,
    ) -> Result<VersionedResult> {
        let op = Operation::RemoveIfUnmodified(key.to_vec(), version);
        match self.call(key, op).await? {
            OperationResult::Versioned(value) => Ok(value),
            _ => unreachable!(
                "Operation::RemoveIfUnmodified always yields OperationResult::Versioned"
            ),
        }
    }

    /// Routes `key` to its computed owner, running `op` against that
    /// connection (opening and authenticating it first if this is the first
    /// call to reach it). On an I/O error, the failed connection is dropped
    /// from the pool and `op` is retried once against the seed connection;
    /// any other error, or a second failure, is returned as is.
    async fn call(&mut self, key: &[u8], op: Operation) -> Result<OperationResult> {
        let addr = self.owner_addr(key).await?;
        self.ensure_connection(addr).await?;
        let outcome = {
            let conn = self
                .connections
                .get_mut(&addr)
                .expect("ensure_connection just inserted it");
            run_operation(conn, &op).await
        };
        let err = match outcome {
            Ok(value) => {
                self.record_topology_update(addr);
                return Ok(value);
            }
            Err(err) => err,
        };
        if !matches!(err, Error::Io(_)) {
            return Err(err);
        }
        self.connections.remove(&addr);
        if addr == self.active_seed_addr {
            return Err(err);
        }

        let seed = self.active_seed_addr;
        self.ensure_connection(seed).await?;
        let conn = self
            .connections
            .get_mut(&seed)
            .expect("ensure_connection just inserted it");
        match run_operation(conn, &op).await {
            Ok(value) => {
                self.record_topology_update(seed);
                Ok(value)
            }
            Err(err) => {
                if matches!(err, Error::Io(_)) {
                    self.connections.remove(&seed);
                }
                Err(err)
            }
        }
    }

    /// The address to route `key` to: the segment's primary owner once a
    /// topology is known, otherwise the seed connection.
    async fn owner_addr(&mut self, key: &[u8]) -> Result<SocketAddr> {
        let Some(topology) = &self.topology else {
            return Ok(self.active_seed_addr);
        };
        if topology.segment_owners.is_empty() {
            return Ok(self.active_seed_addr);
        }
        let segment = hash::segment(
            key,
            topology.segment_owners.len() as u32,
            topology.hash_function_version,
        )?;
        let Some(&primary) = topology.segment_owners[segment as usize].first() else {
            return Ok(self.active_seed_addr);
        };
        let server = topology.servers[primary as usize].clone();
        resolve_server_addr(&server).await
    }

    /// Opens and authenticates a pooled connection to `addr` if one is not
    /// already there.
    async fn ensure_connection(&mut self, addr: SocketAddr) -> Result<()> {
        if self.connections.contains_key(&addr) {
            return Ok(());
        }
        let mut conn =
            HotRodConnection::connect_hash_aware(addr, &self.cache_name, self.topology_id).await?;
        if let Some(auth) = &self.auth {
            auth.authenticate(&mut conn).await?;
        }
        self.connections.insert(addr, conn);
        Ok(())
    }

    /// Applies whatever topology update the connection at `addr` parsed
    /// from its last response, if any.
    fn record_topology_update(&mut self, addr: SocketAddr) {
        let Some(conn) = self.connections.get_mut(&addr) else {
            return;
        };
        let Some(update) = conn.take_pending_topology_update() else {
            return;
        };
        self.topology_id = update.topology_id as i32;
        self.topology = Some(ClusterTopology {
            servers: update.servers,
            hash_function_version: update.hash_function_version,
            segment_owners: update.segment_owners,
        });
    }
}

async fn resolve_server_addr(server: &TopologyServer) -> Result<SocketAddr> {
    lookup_host((server.host.as_str(), server.port))
        .await?
        .next()
        .ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no address found for {}:{}", server.host, server.port),
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connect_fails_with_no_seed_addresses() {
        let result = HotRodCluster::connect(&[], "my-cache").await;
        assert!(matches!(result, Err(Error::Io(_))));
    }
}
