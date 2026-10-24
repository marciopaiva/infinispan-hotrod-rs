//! `TypedMultimapCache`: a typed façade over `MultimapCache`, built
//! entirely on `Marshaller` (`marshall.rs`) and the existing
//! byte-oriented `MultimapCache` methods
//! (`docs/adr/0023-typed-remote-query-and-multimap.md`). Same shape
//! as `typed_cache.rs`'s own `TypedCache` over `RemoteCache`: every
//! method here marshalls its typed arguments, delegates to the
//! matching byte-oriented `MultimapCache` method, then unmarshalls
//! the result.
//!
//! `size`/`supports_duplicates` have nothing key- or value-shaped to
//! marshal, so they are reached through `Deref`, unmodified, the same
//! way `TypedCache` leaves untyped `RemoteCache` methods untouched.

use std::ops::Deref;
use std::sync::Arc;
use std::time::SystemTime;

use crate::error::{wrap_marshalling_error as wrap, MarshallingSide, Result};
use crate::marshall::Marshaller;
use crate::multimap::MultimapCache;
use crate::wire::Expiration;

/// `MultimapEntry`'s typed counterpart: `values: Vec<V>` instead of
/// `Vec<Vec<u8>>`. A separate type, not a generic parameter added to
/// `MultimapEntry` itself, the same reason `TypedVersionedValue` is
/// separate from `VersionedValue`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedMultimapEntry<V> {
    pub values: Vec<V>,
    pub version: u64,
    pub created: Option<SystemTime>,
    pub lifespan: Expiration,
    pub last_used: Option<SystemTime>,
    pub max_idle: Expiration,
}

/// A typed handle to one multimap cache, obtained from
/// `MultimapCache::typed`. Cheap to clone, like `MultimapCache`
/// itself: `key_marshaller`/`value_marshaller` are `Arc`-shared
/// rather than requiring `MK`/`MV` themselves to be `Clone`, the same
/// reason `TypedCache` shares them this way.
pub struct TypedMultimapCache<MK: Marshaller, MV: Marshaller> {
    cache: MultimapCache,
    key_marshaller: Arc<MK>,
    value_marshaller: Arc<MV>,
}

// Not `#[derive(Clone)]`: see `TypedCache`'s own identical comment on
// why that would wrongly require `MK: Clone`/`MV: Clone`.
impl<MK: Marshaller, MV: Marshaller> Clone for TypedMultimapCache<MK, MV> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
            key_marshaller: self.key_marshaller.clone(),
            value_marshaller: self.value_marshaller.clone(),
        }
    }
}

impl<MK: Marshaller, MV: Marshaller> Deref for TypedMultimapCache<MK, MV> {
    type Target = MultimapCache;

    fn deref(&self) -> &MultimapCache {
        &self.cache
    }
}

impl<MK: Marshaller, MV: Marshaller> TypedMultimapCache<MK, MV> {
    pub(crate) fn new(cache: MultimapCache, key_marshaller: MK, value_marshaller: MV) -> Self {
        Self {
            cache,
            key_marshaller: Arc::new(key_marshaller),
            value_marshaller: Arc::new(value_marshaller),
        }
    }

    fn marshall_key(&self, key: &MK::Value) -> Result<Vec<u8>> {
        self.key_marshaller
            .marshall(key)
            .map_err(|err| wrap(MarshallingSide::Key, err))
    }

    fn marshall_value(&self, value: &MV::Value) -> Result<Vec<u8>> {
        self.value_marshaller
            .marshall(value)
            .map_err(|err| wrap(MarshallingSide::Value, err))
    }

    fn unmarshall_value(&self, bytes: &[u8]) -> Result<MV::Value> {
        self.value_marshaller
            .unmarshall(bytes)
            .map_err(|err| wrap(MarshallingSide::Value, err))
    }

    /// Every value stored under `key`, empty if it has no entry: see
    /// `MultimapCache::get`'s own docs.
    pub async fn get(&self, key: &MK::Value) -> Result<Vec<MV::Value>> {
        let key_bytes = self.marshall_key(key)?;
        let raw = self.cache.get(&key_bytes).await?;
        raw.into_iter()
            .map(|value_bytes| self.unmarshall_value(&value_bytes))
            .collect()
    }

    /// Same as `get`, plus the entry's metadata. `None` if `key` has
    /// no entry.
    pub async fn get_with_metadata(
        &self,
        key: &MK::Value,
    ) -> Result<Option<TypedMultimapEntry<MV::Value>>> {
        let key_bytes = self.marshall_key(key)?;
        match self.cache.get_with_metadata(&key_bytes).await? {
            Some(entry) => {
                let mut values = Vec::with_capacity(entry.values.len());
                for value_bytes in entry.values {
                    values.push(self.unmarshall_value(&value_bytes)?);
                }
                Ok(Some(TypedMultimapEntry {
                    values,
                    version: entry.version,
                    created: entry.created,
                    lifespan: entry.lifespan,
                    last_used: entry.last_used,
                    max_idle: entry.max_idle,
                }))
            }
            None => Ok(None),
        }
    }

    /// Adds `value` to the collection stored under `key`, creating it
    /// if needed.
    pub async fn put(
        &self,
        key: &MK::Value,
        value: &MV::Value,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        let key_bytes = self.marshall_key(key)?;
        let value_bytes = self.marshall_value(value)?;
        self.cache
            .put(&key_bytes, &value_bytes, lifespan, max_idle)
            .await
    }

    /// Removes `key` and every value stored under it.
    pub async fn remove_key(&self, key: &MK::Value) -> Result<bool> {
        let key_bytes = self.marshall_key(key)?;
        self.cache.remove_key(&key_bytes).await
    }

    /// Removes just `value` from `key`'s collection, leaving any other
    /// values under it untouched.
    pub async fn remove_entry(&self, key: &MK::Value, value: &MV::Value) -> Result<bool> {
        let key_bytes = self.marshall_key(key)?;
        let value_bytes = self.marshall_value(value)?;
        self.cache.remove_entry(&key_bytes, &value_bytes).await
    }

    pub async fn contains_entry(&self, key: &MK::Value, value: &MV::Value) -> Result<bool> {
        let key_bytes = self.marshall_key(key)?;
        let value_bytes = self.marshall_value(value)?;
        self.cache.contains_entry(&key_bytes, &value_bytes).await
    }

    pub async fn contains_key(&self, key: &MK::Value) -> Result<bool> {
        let key_bytes = self.marshall_key(key)?;
        self.cache.contains_key(&key_bytes).await
    }

    /// Whether `value` is stored under any key in the whole cache,
    /// not just one.
    pub async fn contains_value(&self, value: &MV::Value) -> Result<bool> {
        let value_bytes = self.marshall_value(value)?;
        self.cache.contains_value(&value_bytes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::client::tests::{read_request_opcode, response_header};
    use crate::client::HotRodClient;
    use crate::marshall::{BytesMarshaller, Utf8Marshaller};
    use crate::remote_cache::tests::client_with_seeds;
    use crate::wire::{read_array, write_array};

    fn multimap_cache(client: &HotRodClient) -> MultimapCache {
        client.multimap_cache("my-multimap", false)
    }

    #[tokio::test]
    async fn get_and_put_round_trip_through_the_marshaller() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x6B, "expected a MultimapPut request");
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, b"fruits");
            let _time_units = stream.read_u8().await.unwrap();
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(value, b"apple");
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let resp = response_header(id, 0x6C, 0x00);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x67, "expected a MultimapGet request");
            let _key = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x68, 0x00);
            crate::varint::write_vint(&mut resp, 1);
            write_array(&mut resp, b"apple");
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let typed = multimap_cache(&client).typed(Utf8Marshaller, Utf8Marshaller);

        typed
            .put(
                &"fruits".to_string(),
                &"apple".to_string(),
                Expiration::Default,
                Expiration::Default,
            )
            .await
            .expect("put should marshall key and value to UTF-8 bytes");

        let values = typed
            .get(&"fruits".to_string())
            .await
            .expect("get should unmarshall every returned value");

        assert_eq!(values, vec!["apple".to_string()]);

        seed_task.await.unwrap();
    }

    #[tokio::test]
    async fn get_with_metadata_unmarshalls_every_value_and_the_metadata() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x69, "expected a MultimapGetWithMetadata request");
            let _key = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x6A, 0x00);
            resp.push(0x03); // immortal lifespan and max idle, no timestamps follow
            resp.extend_from_slice(&3u64.to_be_bytes()); // version
            crate::varint::write_vint(&mut resp, 2);
            write_array(&mut resp, b"apple");
            write_array(&mut resp, b"banana");
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let typed = multimap_cache(&client).typed(Utf8Marshaller, Utf8Marshaller);

        let entry = typed
            .get_with_metadata(&"fruits".to_string())
            .await
            .expect("get_with_metadata should unmarshall every value")
            .expect("key exists");

        assert_eq!(
            entry.values,
            vec!["apple".to_string(), "banana".to_string()]
        );
        assert_eq!(entry.version, 3);

        seed_task.await.unwrap();
    }

    #[tokio::test]
    async fn remove_key_and_remove_entry_marshall_their_arguments() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x6D, "expected a MultimapRemoveKey request");
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, b"fruits");
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x6E, 0x00);
            resp.push(1); // read_multimap_bool reads this byte unconditionally
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x6F, "expected a MultimapRemoveEntry request");
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, b"fruits");
            let _time_units = stream.read_u8().await.unwrap(); // hardcoded immortal, unused
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(value, b"apple");
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x70, 0x00);
            resp.push(1);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let typed = multimap_cache(&client).typed(Utf8Marshaller, Utf8Marshaller);

        let removed_key = typed
            .remove_key(&"fruits".to_string())
            .await
            .expect("remove_key should marshall the key");
        assert!(removed_key);

        let removed_entry = typed
            .remove_entry(&"fruits".to_string(), &"apple".to_string())
            .await
            .expect("remove_entry should marshall key and value");
        assert!(removed_entry);

        seed_task.await.unwrap();
    }

    #[tokio::test]
    async fn contains_entry_contains_key_and_contains_value_marshall_their_arguments() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x73, "expected a MultimapContainsEntry request");
            let _key = read_array(&mut stream).await.unwrap();
            let _time_units = stream.read_u8().await.unwrap(); // hardcoded immortal, unused
            let _value = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x74, 0x00);
            resp.push(1);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x75, "expected a MultimapContainsKey request");
            let _key = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x76, 0x00);
            resp.push(1);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x77, "expected a MultimapContainsValue request");
            let _time_units = stream.read_u8().await.unwrap(); // hardcoded immortal, unused
            let _value = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x78, 0x00);
            resp.push(1);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let typed = multimap_cache(&client).typed(Utf8Marshaller, Utf8Marshaller);

        assert!(typed
            .contains_entry(&"fruits".to_string(), &"apple".to_string())
            .await
            .expect("contains_entry should marshall key and value"));
        assert!(typed
            .contains_key(&"fruits".to_string())
            .await
            .expect("contains_key should marshall the key"));
        assert!(typed
            .contains_value(&"apple".to_string())
            .await
            .expect("contains_value should marshall the value"));

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

    /// A marshalling failure never reaches the network: the same
    /// guarantee `typed_cache.rs`'s own analogous test confirms for
    /// `TypedCache`.
    #[tokio::test]
    async fn a_failing_key_marshaller_surfaces_as_error_marshalling_without_touching_the_network() {
        let unreachable: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();
        let client = client_with_seeds(vec![unreachable], unreachable);
        let typed = multimap_cache(&client).typed(FailingMarshaller, BytesMarshaller);

        let result = typed.get(&vec![1, 2, 3]).await;

        assert!(matches!(
            result,
            Err(crate::error::Error::Marshalling {
                side: MarshallingSide::Key,
                ..
            })
        ));
    }
}
