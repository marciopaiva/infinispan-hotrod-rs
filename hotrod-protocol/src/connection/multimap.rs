//! Multimap cache wire operations (`docs/adr/0016-multimap-cache.md`).
//! Every body ends with the `supportsDuplicates` byte, confirmed
//! against the Java client's own codec to exist only from protocol
//! 4.0 onward; this crate always speaks 4.1, so every one of these
//! always sends it. A multimap cache is an ordinary cache
//! (`self.cache_name`), not a fixed reserved one like counters.
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): `impl HotRodConnection` blocks are
//! free to live in any module descended from `connection`, since
//! Rust's privacy rule makes every private field and method defined
//! at `connection.rs`'s root (`self.stream`, `self.cache_name`,
//! `self.timeout`, `write_and_read_header`, `begin_operation`/
//! `end_operation`, `with_timeout`, `key_value_body`) visible here
//! without any visibility change.

use crate::error::Result;
use crate::header::OpCode;
use crate::varint::{read_vint, read_vlong};
use crate::wire::{read_array, write_array, write_expiration_params, Expiration};

use super::{key_value_body, with_timeout, HotRodConnection};

impl HotRodConnection {
    /// Returns every value stored under `key`, empty if `key` has no
    /// entry (confirmed against `GetMultimapOperation`: a missing key
    /// is an empty collection, not a distinguishable third case).
    pub async fn multimap_get(
        &mut self,
        key: &[u8],
        supports_duplicates: bool,
    ) -> Result<Vec<Vec<u8>>> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            body.push(supports_duplicates as u8);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::MultimapGet, &body)
                .await?;
            if header.status.is_not_exist() {
                return Ok(Vec::new());
            }
            read_array_collection(&mut self.stream).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    pub async fn multimap_get_with_metadata(
        &mut self,
        key: &[u8],
        supports_duplicates: bool,
    ) -> Result<Option<crate::multimap::MultimapEntry>> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            body.push(supports_duplicates as u8);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::MultimapGetWithMetadata, &body)
                .await?;
            if header.status.is_not_exist() {
                return Ok(None);
            }
            let (created, lifespan, last_used, max_idle, version) =
                self.read_entry_metadata().await?;
            let values = read_array_collection(&mut self.stream).await?;
            Ok(Some(crate::multimap::MultimapEntry {
                values,
                version,
                created,
                lifespan,
                last_used,
                max_idle,
            }))
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Adds `value` to the collection stored under `key`, creating it
    /// if needed. `lifespan`/`max_idle` apply to the key's entry as a
    /// whole, the same as the main cache API's `put`.
    pub async fn multimap_put(
        &mut self,
        key: &[u8],
        value: &[u8],
        lifespan: Expiration,
        max_idle: Expiration,
        supports_duplicates: bool,
    ) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = key_value_body(key, value, lifespan, max_idle);
            body.push(supports_duplicates as u8);
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::MultimapPut, &body)
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Removes `key` and every value stored under it.
    pub async fn multimap_remove_key(
        &mut self,
        key: &[u8],
        supports_duplicates: bool,
    ) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            body.push(supports_duplicates as u8);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::MultimapRemoveKey, &body)
                .await?;
            read_multimap_bool(&mut self.stream, header.status).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Removes just `value` from `key`'s collection, leaving any
    /// other values under it untouched. `lifespan`/`max_idle` are
    /// sent but not meaningful for a removal: confirmed against the
    /// Java client's own `RemoveEntryMultimapOperation`, which
    /// hardcodes them to "immortal" itself (a known inconsistency in
    /// the wire format, not something this client invents).
    pub async fn multimap_remove_entry(
        &mut self,
        key: &[u8],
        value: &[u8],
        supports_duplicates: bool,
    ) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = key_value_body(key, value, Expiration::Immortal, Expiration::Immortal);
            body.push(supports_duplicates as u8);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::MultimapRemoveEntry, &body)
                .await?;
            read_multimap_bool(&mut self.stream, header.status).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// The number of key/value pairs in the whole multimap cache
    /// (every value under every key counts separately), confirmed
    /// against `SizeMultimapOperation` to be a plain `vLong`, read
    /// unconditionally, unlike every other multimap response above:
    /// there is no key for a "not exist" status to apply to.
    pub async fn multimap_size(&mut self, supports_duplicates: bool) -> Result<u64> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let body = vec![supports_duplicates as u8];
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::MultimapSize, &body)
                .await?;
            read_vlong(&mut self.stream).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Same hardcoded-expiration quirk as `multimap_remove_entry`,
    /// confirmed against `ContainsEntryMultimapOperation`.
    pub async fn multimap_contains_entry(
        &mut self,
        key: &[u8],
        value: &[u8],
        supports_duplicates: bool,
    ) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = key_value_body(key, value, Expiration::Immortal, Expiration::Immortal);
            body.push(supports_duplicates as u8);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::MultimapContainsEntry, &body)
                .await?;
            read_multimap_bool(&mut self.stream, header.status).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    pub async fn multimap_contains_key(
        &mut self,
        key: &[u8],
        supports_duplicates: bool,
    ) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, key);
            body.push(supports_duplicates as u8);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::MultimapContainsKey, &body)
                .await?;
            read_multimap_bool(&mut self.stream, header.status).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Whether `value` is stored under any key in the whole multimap
    /// cache, not just one. No key at all in the request body
    /// (confirmed against `ContainsValueMultimapOperation`): just the
    /// same hardcoded-expiration quirk, the value, and
    /// `supportsDuplicates`.
    pub async fn multimap_contains_value(
        &mut self,
        value: &[u8],
        supports_duplicates: bool,
    ) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_expiration_params(&mut body, Expiration::Immortal, Expiration::Immortal);
            write_array(&mut body, value);
            body.push(supports_duplicates as u8);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::MultimapContainsValue, &body)
                .await?;
            read_multimap_bool(&mut self.stream, header.status).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }
}

/// A multimap response's collection of values: a `vInt` count
/// followed by that many length-prefixed arrays, confirmed against
/// `GetMultimapOperation`/`GetWithMetadataMultimapOperation`. Reading
/// zero of them (an empty collection, not a missing key: `is_not_exist`
/// is checked separately, before this is ever called) is the normal,
/// successful shape of an empty entry.
async fn read_array_collection<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
) -> Result<Vec<Vec<u8>>> {
    let count = read_vint(stream).await?;
    // Not `Vec::with_capacity(count as usize)`: `count` is an
    // unchecked, server-controlled vInt, and sizing a single
    // allocation from it directly would let a corrupted or hostile
    // response abort the process instead of failing with a typed
    // error, the same hazard `wire.rs::read_array`'s own
    // `MAX_ARRAY_LEN` check exists to avoid. Growing the `Vec`
    // organically costs reallocations, not a single huge one; an
    // inflated count with no data behind it just times out on the
    // next `read_array`, the same as `wire.rs::read_string_map`
    // already does for its own count-prefixed collection.
    let mut values = Vec::new();
    for _ in 0..count {
        values.push(read_array(stream).await?);
    }
    Ok(values)
}

/// The boolean response shape every multimap `remove`/`contains`
/// operation but `size` shares: `KeyDoesNotExist` means `false`
/// without a body to read, any other (non-error, already checked by
/// `read_response_header`) status means read one byte, confirmed
/// against `RemoveKeyMultimapOperation`/`ContainsKeyMultimapOperation`.
async fn read_multimap_bool<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
    status: crate::status::Status,
) -> Result<bool> {
    if status.is_not_exist() {
        return Ok(false);
    }
    Ok(tokio::io::AsyncReadExt::read_u8(stream).await? != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::connection::tests::{read_request, response_header};
    use crate::varint::{write_vint, write_vlong};

    #[tokio::test]
    async fn multimap_put_sends_key_expiration_value_and_supports_duplicates() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x6B, "expected a MultimapPut request");
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, b"key");
            let time_units = stream.read_u8().await.unwrap();
            assert_eq!(
                time_units, 0x88,
                "both lifespan and max idle infinite (unit nibble 8 each)"
            );
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(value, b"value");
            let supports_duplicates = stream.read_u8().await.unwrap();
            assert_eq!(supports_duplicates, 1);
            let resp = response_header(id, 0x6C, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        conn.multimap_put(
            b"key",
            b"value",
            Expiration::Immortal,
            Expiration::Immortal,
            true,
        )
        .await
        .expect("multimap_put");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn multimap_get_returns_the_collection_of_values() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x67, "expected a MultimapGet request");
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, b"key");
            let supports_duplicates = stream.read_u8().await.unwrap();
            assert_eq!(supports_duplicates, 0);
            let mut resp = response_header(id, 0x68, 0x00);
            write_vint(&mut resp, 2);
            write_array(&mut resp, b"one");
            write_array(&mut resp, b"two");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let values = conn
            .multimap_get(b"key", false)
            .await
            .expect("multimap_get");
        assert_eq!(values, vec![b"one".to_vec(), b"two".to_vec()]);

        server.await.unwrap();
    }

    /// `KeyDoesNotExist` carries no body for this operation: reading
    /// a collection from it regardless (instead of short-circuiting
    /// to an empty one) would block waiting for bytes the server
    /// never sends, rather than fail fast. This is a regression test
    /// for exactly that bug, caught by live-server testing before
    /// merge.
    #[tokio::test]
    async fn multimap_get_returns_an_empty_collection_without_reading_a_body_when_key_is_missing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x67, "expected a MultimapGet request");
            let _key = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let resp = response_header(id, 0x68, 0x02); // KeyDoesNotExist, no body
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let values = conn
            .multimap_get(b"key", false)
            .await
            .expect("multimap_get");
        assert_eq!(values, Vec::<Vec<u8>>::new());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn multimap_get_with_metadata_returns_none_when_key_is_missing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x69, "expected a MultimapGetWithMetadata request");
            let _key = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let resp = response_header(id, 0x6A, 0x02); // KeyDoesNotExist
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let entry = conn
            .multimap_get_with_metadata(b"key", true)
            .await
            .expect("multimap_get_with_metadata");
        assert_eq!(entry, None);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn multimap_get_with_metadata_returns_values_and_metadata() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x69, "expected a MultimapGetWithMetadata request");
            let _key = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x6A, 0x00);
            resp.push(0x03); // both lifespan and max idle infinite
            resp.extend_from_slice(&42u64.to_be_bytes()); // version
            write_vint(&mut resp, 1);
            write_array(&mut resp, b"one");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let entry = conn
            .multimap_get_with_metadata(b"key", false)
            .await
            .expect("multimap_get_with_metadata")
            .expect("entry should be present");
        assert_eq!(entry.values, vec![b"one".to_vec()]);
        assert_eq!(entry.version, 42);
        assert_eq!(entry.lifespan, Expiration::Immortal);
        assert_eq!(entry.max_idle, Expiration::Immortal);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn multimap_remove_key_reads_a_boolean_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x6D, "expected a MultimapRemoveKey request");
            let _key = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x6E, 0x00);
            resp.push(1);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let removed = conn
            .multimap_remove_key(b"key", false)
            .await
            .expect("multimap_remove_key");
        assert!(removed);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn multimap_remove_key_is_false_without_reading_a_body_when_key_is_missing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x6D, "expected a MultimapRemoveKey request");
            let _key = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let resp = response_header(id, 0x6E, 0x02); // KeyDoesNotExist, no body
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let removed = conn
            .multimap_remove_key(b"key", false)
            .await
            .expect("multimap_remove_key");
        assert!(!removed);

        server.await.unwrap();
    }

    /// `removeEntry` sends `lifespan`/`max_idle` even though removing
    /// does not need them: confirmed against the Java client's own
    /// `RemoveEntryMultimapOperation`, a known inconsistency in the
    /// wire format, not something this client invents.
    #[tokio::test]
    async fn multimap_remove_entry_sends_hardcoded_immortal_expiration() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x6F, "expected a MultimapRemoveEntry request");
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, b"key");
            let time_units = stream.read_u8().await.unwrap();
            assert_eq!(
                time_units, 0x88,
                "both lifespan and max idle infinite (unit nibble 8 each)"
            );
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(value, b"value");
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x70, 0x00);
            resp.push(1);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let removed = conn
            .multimap_remove_entry(b"key", b"value", false)
            .await
            .expect("multimap_remove_entry");
        assert!(removed);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn multimap_size_reads_a_vlong_unconditionally() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x71, "expected a MultimapSize request");
            let supports_duplicates = stream.read_u8().await.unwrap();
            assert_eq!(supports_duplicates, 1);
            let mut resp = response_header(id, 0x72, 0x00);
            write_vlong(&mut resp, 1_000_000_000_000);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let size = conn.multimap_size(true).await.expect("multimap_size");
        assert_eq!(size, 1_000_000_000_000);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn multimap_contains_entry_sends_hardcoded_immortal_expiration() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x73, "expected a MultimapContainsEntry request");
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, b"key");
            let time_units = stream.read_u8().await.unwrap();
            assert_eq!(
                time_units, 0x88,
                "both lifespan and max idle infinite (unit nibble 8 each)"
            );
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(value, b"value");
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x74, 0x00);
            resp.push(0);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let contains = conn
            .multimap_contains_entry(b"key", b"value", false)
            .await
            .expect("multimap_contains_entry");
        assert!(!contains);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn multimap_contains_key_reads_a_boolean_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x75, "expected a MultimapContainsKey request");
            let _key = read_array(&mut stream).await.unwrap();
            let _supports_duplicates = stream.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x76, 0x00);
            resp.push(1);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let contains = conn
            .multimap_contains_key(b"key", false)
            .await
            .expect("multimap_contains_key");
        assert!(contains);

        server.await.unwrap();
    }

    /// `containsValue` has no key at all: confirmed against the Java
    /// client's own `ContainsValueMultimapOperation`, which writes
    /// the hardcoded-immortal expiration fields, the value, and
    /// `supportsDuplicates`, nothing else.
    #[tokio::test]
    async fn multimap_contains_value_has_no_key_in_the_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x77, "expected a MultimapContainsValue request");
            let time_units = stream.read_u8().await.unwrap();
            assert_eq!(
                time_units, 0x88,
                "both lifespan and max idle infinite (unit nibble 8 each)"
            );
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(value, b"value");
            let supports_duplicates = stream.read_u8().await.unwrap();
            assert_eq!(supports_duplicates, 1);
            let mut resp = response_header(id, 0x78, 0x00);
            resp.push(1);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-multimap")
            .await
            .expect("connect");
        let contains = conn
            .multimap_contains_value(b"value", true)
            .await
            .expect("multimap_contains_value");
        assert!(contains);

        server.await.unwrap();
    }
}
