//! `TypedCache`: a typed façade over `RemoteCache`, built entirely on
//! `Marshaller` (`marshall.rs`) and the existing byte-oriented
//! `RemoteCache` methods (`docs/adr/0012-serialization-abstraction.md`).
//!
//! Every method here marshalls its typed arguments, delegates to the
//! matching byte-oriented `RemoteCache` method, then unmarshalls the
//! result. Routing (including backup owners), the retry chain and its
//! circuit breaker (ADR 0011), statistics and tracing (ADR 0010) all
//! keep working unchanged: the marshalled key is exactly what
//! `hash::segment` has always routed on, so none of that logic needs
//! to know a typed caller exists.
//!
//! Streaming, server-side iteration, client listeners and near caching
//! have no typed equivalent in this phase: `TypedCache` `Deref`s to
//! the wrapped `RemoteCache` for all of them, the same way
//! `NearCachedCache` leaves most of `RemoteCache` untouched rather
//! than reimplementing it.

use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;
use std::time::SystemTime;

use crate::connection::VersionedResult;
use crate::error::{Error, Result};
use crate::marshall::Marshaller;
use crate::remote_cache::RemoteCache;
use crate::wire::Expiration;

fn wrap<E: std::error::Error + Send + Sync + 'static>(err: E) -> Error {
    Error::Marshalling(Box::new(err))
}

/// `VersionedValue`'s typed equivalent: `get_with_version`'s metadata,
/// with `value: V` instead of `Vec<u8>`. A separate type rather than
/// a generic parameter added to `VersionedValue` itself, so the
/// existing public byte-oriented type never changes shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedVersionedValue<V> {
    pub value: V,
    pub version: u64,
    pub created: Option<SystemTime>,
    pub lifespan: Expiration,
    pub last_used: Option<SystemTime>,
    pub max_idle: Expiration,
}

/// A typed handle to one named cache, obtained from
/// `RemoteCache::typed`. Cheap to clone, like `RemoteCache` itself:
/// `key_marshaller`/`value_marshaller` are `Arc`-shared rather than
/// requiring `MK`/`MV` themselves to be `Clone`, so a marshaller can
/// hold state that is not cheap (or not possible) to clone, such as a
/// compiled schema.
#[derive(Clone)]
pub struct TypedCache<MK: Marshaller, MV: Marshaller> {
    cache: RemoteCache,
    key_marshaller: Arc<MK>,
    value_marshaller: Arc<MV>,
}

impl<MK: Marshaller, MV: Marshaller> Deref for TypedCache<MK, MV> {
    type Target = RemoteCache;

    fn deref(&self) -> &RemoteCache {
        &self.cache
    }
}

impl<MK: Marshaller, MV: Marshaller> TypedCache<MK, MV> {
    pub(crate) fn new(cache: RemoteCache, key_marshaller: MK, value_marshaller: MV) -> Self {
        Self {
            cache,
            key_marshaller: Arc::new(key_marshaller),
            value_marshaller: Arc::new(value_marshaller),
        }
    }

    pub async fn get(&self, key: &MK::Value) -> Result<Option<MV::Value>> {
        let key_bytes = self.key_marshaller.marshall(key).map_err(wrap)?;
        match self.cache.get(&key_bytes).await? {
            Some(value_bytes) => Ok(Some(
                self.value_marshaller
                    .unmarshall(&value_bytes)
                    .map_err(wrap)?,
            )),
            None => Ok(None),
        }
    }

    pub async fn put(
        &self,
        key: &MK::Value,
        value: &MV::Value,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        let key_bytes = self.key_marshaller.marshall(key).map_err(wrap)?;
        let value_bytes = self.value_marshaller.marshall(value).map_err(wrap)?;
        self.cache
            .put(&key_bytes, &value_bytes, lifespan, max_idle)
            .await
    }

    pub async fn put_if_absent(
        &self,
        key: &MK::Value,
        value: &MV::Value,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<bool> {
        let key_bytes = self.key_marshaller.marshall(key).map_err(wrap)?;
        let value_bytes = self.value_marshaller.marshall(value).map_err(wrap)?;
        self.cache
            .put_if_absent(&key_bytes, &value_bytes, lifespan, max_idle)
            .await
    }

    pub async fn replace(
        &self,
        key: &MK::Value,
        value: &MV::Value,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<bool> {
        let key_bytes = self.key_marshaller.marshall(key).map_err(wrap)?;
        let value_bytes = self.value_marshaller.marshall(value).map_err(wrap)?;
        self.cache
            .replace(&key_bytes, &value_bytes, lifespan, max_idle)
            .await
    }

    pub async fn remove(&self, key: &MK::Value) -> Result<bool> {
        let key_bytes = self.key_marshaller.marshall(key).map_err(wrap)?;
        self.cache.remove(&key_bytes).await
    }

    pub async fn get_with_version(
        &self,
        key: &MK::Value,
    ) -> Result<Option<TypedVersionedValue<MV::Value>>> {
        let key_bytes = self.key_marshaller.marshall(key).map_err(wrap)?;
        match self.cache.get_with_version(&key_bytes).await? {
            Some(versioned) => Ok(Some(TypedVersionedValue {
                value: self
                    .value_marshaller
                    .unmarshall(&versioned.value)
                    .map_err(wrap)?,
                version: versioned.version,
                created: versioned.created,
                lifespan: versioned.lifespan,
                last_used: versioned.last_used,
                max_idle: versioned.max_idle,
            })),
            None => Ok(None),
        }
    }

    pub async fn replace_if_unmodified(
        &self,
        key: &MK::Value,
        value: &MV::Value,
        version: u64,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<VersionedResult> {
        let key_bytes = self.key_marshaller.marshall(key).map_err(wrap)?;
        let value_bytes = self.value_marshaller.marshall(value).map_err(wrap)?;
        self.cache
            .replace_if_unmodified(&key_bytes, &value_bytes, version, lifespan, max_idle)
            .await
    }

    pub async fn remove_if_unmodified(
        &self,
        key: &MK::Value,
        version: u64,
    ) -> Result<VersionedResult> {
        let key_bytes = self.key_marshaller.marshall(key).map_err(wrap)?;
        self.cache.remove_if_unmodified(&key_bytes, version).await
    }

    pub async fn contains_key(&self, key: &MK::Value) -> Result<bool> {
        let key_bytes = self.key_marshaller.marshall(key).map_err(wrap)?;
        self.cache.contains_key(&key_bytes).await
    }

    /// Fetches every key in `keys` that exists. See
    /// `RemoteCache::get_all`'s own docs for the routing trade-off this
    /// inherits unchanged (always the seed connection, never split by
    /// owner).
    pub async fn get_all<'k>(
        &self,
        keys: impl IntoIterator<Item = &'k MK::Value>,
    ) -> Result<HashMap<MK::Value, MV::Value>>
    where
        MK::Value: Eq + Hash + 'k,
    {
        let mut key_bytes_list = Vec::new();
        for key in keys {
            key_bytes_list.push(self.key_marshaller.marshall(key).map_err(wrap)?);
        }
        let raw = self.cache.get_all(key_bytes_list).await?;
        let mut result = HashMap::with_capacity(raw.len());
        for (key_bytes, value_bytes) in raw {
            let key = self.key_marshaller.unmarshall(&key_bytes).map_err(wrap)?;
            let value = self
                .value_marshaller
                .unmarshall(&value_bytes)
                .map_err(wrap)?;
            result.insert(key, value);
        }
        Ok(result)
    }

    /// Writes every key/value pair in `entries`. See
    /// `RemoteCache::put_all`'s own docs for the routing trade-off this
    /// inherits unchanged.
    pub async fn put_all(
        &self,
        entries: impl IntoIterator<Item = (MK::Value, MV::Value)>,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        let mut entry_bytes_list = Vec::new();
        for (key, value) in entries {
            let key_bytes = self.key_marshaller.marshall(&key).map_err(wrap)?;
            let value_bytes = self.value_marshaller.marshall(&value).map_err(wrap)?;
            entry_bytes_list.push((key_bytes, value_bytes));
        }
        self.cache
            .put_all(entry_bytes_list, lifespan, max_idle)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;
    use std::sync::RwLock;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::client::tests::{read_request_opcode, response_header};
    use crate::client::ClusterTopology;
    use crate::marshall::{BytesMarshaller, Utf8Marshaller};
    use crate::remote_cache::tests::{client_with_seeds, set_topology};
    use crate::topology::TopologyServer;
    use crate::varint::{read_vint, write_vint};
    use crate::wire::{read_array, write_array};

    #[tokio::test]
    async fn get_and_put_round_trip_through_the_marshaller() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x01, "expected a Put request");
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, "saudação".as_bytes());
            let _time_units = stream.read_u8().await.unwrap();
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(value, "olá, mundo".as_bytes());
            let resp = response_header(id, 0x02, 0x00);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let _key = read_array(&mut stream).await.unwrap();
            let mut resp = response_header(id, 0x04, 0x00);
            write_array(&mut resp, "olá, mundo".as_bytes());
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let typed = client
            .cache("my-cache")
            .typed(Utf8Marshaller, Utf8Marshaller);

        typed
            .put(
                &"saudação".to_string(),
                &"olá, mundo".to_string(),
                Expiration::Default,
                Expiration::Default,
            )
            .await
            .expect("put should marshall key and value to UTF-8 bytes");

        let value = typed
            .get(&"saudação".to_string())
            .await
            .expect("get should unmarshall the response back into a String");

        assert_eq!(value, Some("olá, mundo".to_string()));

        seed_task.await.unwrap();
    }

    #[tokio::test]
    async fn get_all_and_put_all_round_trip_multiple_typed_entries() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x2D, "expected a PutAll request");
            let _time_units = stream.read_u8().await.unwrap();
            let count = read_vint(&mut stream).await.unwrap();
            assert_eq!(count, 2);
            for _ in 0..count {
                read_array(&mut stream).await.unwrap();
                read_array(&mut stream).await.unwrap();
            }
            let resp = response_header(id, 0x2E, 0x00);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x2F, "expected a GetAll request");
            let count = read_vint(&mut stream).await.unwrap();
            for _ in 0..count {
                read_array(&mut stream).await.unwrap();
            }
            let mut resp = response_header(id, 0x30, 0x00);
            write_vint(&mut resp, 2);
            write_array(&mut resp, b"one");
            write_array(&mut resp, b"1");
            write_array(&mut resp, b"two");
            write_array(&mut resp, b"2");
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let typed = client
            .cache("my-cache")
            .typed(Utf8Marshaller, Utf8Marshaller);

        typed
            .put_all(
                vec![
                    ("one".to_string(), "1".to_string()),
                    ("two".to_string(), "2".to_string()),
                ],
                Expiration::Default,
                Expiration::Default,
            )
            .await
            .expect("put_all should marshall every key and value");

        let keys = ["one".to_string(), "two".to_string()];
        let result = typed
            .get_all(keys.iter())
            .await
            .expect("get_all should unmarshall every returned key and value");

        assert_eq!(result.get("one"), Some(&"1".to_string()));
        assert_eq!(result.get("two"), Some(&"2".to_string()));

        seed_task.await.unwrap();
    }

    #[tokio::test]
    async fn get_with_version_and_replace_if_unmodified_round_trip_typed_values() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1B, "expected a GetWithMetadata request");
            let _key = read_array(&mut stream).await.unwrap();
            let mut resp = response_header(id, 0x1C, 0x00);
            resp.push(0x03); // immortal lifespan and max idle, no timestamps follow
            resp.extend_from_slice(&7u64.to_be_bytes()); // version
            write_array(&mut resp, b"1");
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x09, "expected a ReplaceIfUnmodified request");
            let _key = read_array(&mut stream).await.unwrap();
            let _version = {
                let mut buf = [0u8; 8];
                stream.read_exact(&mut buf).await.unwrap();
                u64::from_be_bytes(buf)
            };
            let _time_units = stream.read_u8().await.unwrap();
            let _value = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x0A, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let typed = client
            .cache("my-cache")
            .typed(Utf8Marshaller, Utf8Marshaller);

        let versioned = typed
            .get_with_version(&"counter".to_string())
            .await
            .expect("get_with_version should unmarshall the value")
            .expect("key exists");
        assert_eq!(versioned.value, "1".to_string());
        assert_eq!(versioned.version, 7);

        let outcome = typed
            .replace_if_unmodified(
                &"counter".to_string(),
                &"2".to_string(),
                versioned.version,
                Expiration::Default,
                Expiration::Default,
            )
            .await
            .expect("replace_if_unmodified should marshall key and value");
        assert_eq!(outcome, VersionedResult::Success);

        seed_task.await.unwrap();
    }

    #[derive(Debug)]
    struct AlwaysFails;

    impl std::fmt::Display for AlwaysFails {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "this marshaller always fails, on purpose")
        }
    }

    impl std::error::Error for AlwaysFails {}

    struct FailingMarshaller;

    impl Marshaller for FailingMarshaller {
        type Value = Vec<u8>;
        type Error = AlwaysFails;

        fn marshall(&self, _value: &Vec<u8>) -> std::result::Result<Vec<u8>, AlwaysFails> {
            Err(AlwaysFails)
        }

        fn unmarshall(&self, _bytes: &[u8]) -> std::result::Result<Vec<u8>, AlwaysFails> {
            Err(AlwaysFails)
        }
    }

    /// A marshalling failure never panics (`unreachable!` guards every
    /// `OperationResult` match in `RemoteCache`, so a bug here would be
    /// easy to trip accidentally): it surfaces as `Error::Marshalling`,
    /// and never even reaches the network, since marshalling the key
    /// happens before any connection is checked out.
    #[tokio::test]
    async fn a_failing_marshaller_surfaces_as_error_marshalling_without_touching_the_network() {
        // Never bound to anything: if this reached the network at all,
        // the test would hang instead of returning promptly.
        let unreachable: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
        let client = client_with_seeds(vec![unreachable], unreachable);
        let typed = client
            .cache("my-cache")
            .typed(FailingMarshaller, BytesMarshaller);

        let result = typed.get(&vec![1, 2, 3]).await;

        assert!(matches!(result, Err(Error::Marshalling(_))));
    }

    /// A typed key routes to the same owner a byte key would, since
    /// `TypedCache` marshals and then delegates to the same
    /// `RemoteCache::get` every byte-oriented caller uses: the segment
    /// hash this client has always routed on never changes shape.
    #[tokio::test]
    async fn a_typed_key_routes_to_the_same_owner_as_its_marshalled_bytes() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = seed_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let owner_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let owner_addr = owner_listener.local_addr().unwrap();
        let owner_task = tokio::spawn(async move {
            let (mut stream, _) = owner_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x03, "expected a Get request");
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, b"routed-key");
            let resp = response_header(id, 0x04, 0x02); // KEY_DOES_NOT_EXIST
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        set_topology(
            &client,
            ClusterTopology {
                topology_id: 9,
                servers: vec![TopologyServer {
                    host: owner_addr.ip().to_string(),
                    port: owner_addr.port(),
                }],
                hash_function_version: 3,
                segment_owners: vec![vec![0]],
                resolved_addrs: RwLock::new(StdHashMap::new()),
            },
        );

        let typed = client
            .cache("my-cache")
            .typed(BytesMarshaller, BytesMarshaller);

        let result = typed
            .get(&b"routed-key".to_vec())
            .await
            .expect("get should route to the owner the fake server above expects");

        assert_eq!(result, None);

        owner_task.await.unwrap();
    }
}
