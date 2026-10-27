//! `get_all`/`put_all`: multi-key requests in one round trip.
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): see `connection/multimap.rs`'s module
//! docs for the mechanism that lets this `impl HotRodConnection`
//! block reach every private field/method it needs with no
//! visibility change.

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::header::OpCode;
use crate::varint::{read_vint, write_vint};
use crate::wire::{read_array, write_array, write_expiration_params, Expiration};

use super::{with_timeout, HotRodConnection};

/// Safety ceiling on the number of entries `get_all`/`put_all` accept in
/// one call, checked before a single byte is written. This is not a
/// protocol limit either: a real Hot Rod server enforces its own maximum
/// frame size and rejects whatever does not fit, but that rejection would
/// arrive only after this client already built and sent a frame that could
/// be gigabytes long, for a request that was always going to fail. A
/// caller with more entries than this needs several smaller calls instead,
/// the same way `get`/`put` scale to many keys today.
pub const MAX_BULK_ENTRIES: usize = 100_000;

impl HotRodConnection {
    /// Fetches every key in `keys` that exists, in one request. A key with
    /// no entry is simply missing from the result map, the same as `get`
    /// returning `None` for it.
    ///
    /// Returns `Error::BatchTooLarge` without writing anything if `keys` has
    /// more than `MAX_BULK_ENTRIES` entries. A caller with more keys than
    /// that needs several smaller calls instead.
    pub async fn get_all(
        &mut self,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        let keys: Vec<Vec<u8>> = keys.into_iter().map(|key| key.as_ref().to_vec()).collect();
        if keys.len() > MAX_BULK_ENTRIES {
            return Err(Error::BatchTooLarge {
                what: "get_all",
                len: keys.len(),
                max: MAX_BULK_ENTRIES,
            });
        }
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_vint(&mut body, keys.len() as u32);
            for key in &keys {
                write_array(&mut body, key);
            }
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::GetAll, &body)
                .await?;

            let size = read_vint(&mut self.stream).await?;
            let mut result = HashMap::new();
            for _ in 0..size {
                let key = read_array(&mut self.stream).await?;
                let value = read_array(&mut self.stream).await?;
                result.insert(key, value);
            }
            Ok(result)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Writes every key/value pair in `entries` in one request, all sharing
    /// the same `lifespan`/`max_idle`.
    ///
    /// Same `MAX_BULK_ENTRIES` ceiling as `get_all`, checked the same way.
    pub async fn put_all(
        &mut self,
        entries: impl IntoIterator<Item = (impl AsRef<[u8]>, impl AsRef<[u8]>)>,
        lifespan: Expiration,
        max_idle: Expiration,
    ) -> Result<()> {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = entries
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_vec(), value.as_ref().to_vec()))
            .collect();
        if entries.len() > MAX_BULK_ENTRIES {
            return Err(Error::BatchTooLarge {
                what: "put_all",
                len: entries.len(),
                max: MAX_BULK_ENTRIES,
            });
        }
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_expiration_params(&mut body, lifespan, max_idle);
            write_vint(&mut body, entries.len() as u32);
            for (key, value) in &entries {
                write_array(&mut body, key);
                write_array(&mut body, value);
            }
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::PutAll, &body)
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::connection::tests::{read_request, response_header};
    use crate::error::Error;
    use crate::varint::write_vint;
    use crate::wire::{read_array, write_array, Expiration};

    use super::{HotRodConnection, MAX_BULK_ENTRIES};

    #[tokio::test]
    async fn get_all_returns_only_the_keys_the_server_found() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x2F, "expected a GetAll request");
            let count = crate::varint::read_vint(&mut stream).await.unwrap();
            let mut keys = Vec::new();
            for _ in 0..count {
                keys.push(read_array(&mut stream).await.unwrap());
            }
            assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

            let mut resp = response_header(id, 0x30, 0x00);
            write_vint(&mut resp, 1);
            write_array(&mut resp, b"a");
            write_array(&mut resp, b"value-a");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let result = conn
            .get_all([b"a".as_slice(), b"b".as_slice()])
            .await
            .expect("get_all");
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(b"a".as_slice()), Some(&b"value-a".to_vec()));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn put_all_completes_on_a_bare_success_status() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x2D, "expected a PutAll request");
            let _time_units = stream.read_u8().await.unwrap();
            let count = crate::varint::read_vint(&mut stream).await.unwrap();
            let mut entries = Vec::new();
            for _ in 0..count {
                let key = read_array(&mut stream).await.unwrap();
                let value = read_array(&mut stream).await.unwrap();
                entries.push((key, value));
            }
            assert_eq!(entries, vec![(b"a".to_vec(), b"1".to_vec())]);

            let resp = response_header(id, 0x2E, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        conn.put_all(
            [(b"a".as_slice(), b"1".as_slice())],
            Expiration::Default,
            Expiration::Default,
        )
        .await
        .expect("put_all");

        server.await.unwrap();
    }

    /// The oversized-batch check must reject the call before a single byte
    /// reaches the network: the listener below accepts a connection and then
    /// does nothing else, so this test would hang if `get_all` tried to read
    /// a response instead of failing fast on the length check.
    #[tokio::test]
    async fn get_all_rejects_a_batch_over_the_limit_without_touching_the_network() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");

        let keys = vec![b"key".to_vec(); MAX_BULK_ENTRIES + 1];
        let result = conn.get_all(keys).await;

        assert!(matches!(
            result,
            Err(Error::BatchTooLarge {
                what: "get_all",
                len,
                max: MAX_BULK_ENTRIES,
            }) if len == MAX_BULK_ENTRIES + 1
        ));
    }

    #[tokio::test]
    async fn put_all_rejects_a_batch_over_the_limit_without_touching_the_network() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");

        let entries = vec![(b"key".to_vec(), b"value".to_vec()); MAX_BULK_ENTRIES + 1];
        let result = conn
            .put_all(entries, Expiration::Default, Expiration::Default)
            .await;

        assert!(matches!(
            result,
            Err(Error::BatchTooLarge {
                what: "put_all",
                len,
                max: MAX_BULK_ENTRIES,
            }) if len == MAX_BULK_ENTRIES + 1
        ));
    }
}
