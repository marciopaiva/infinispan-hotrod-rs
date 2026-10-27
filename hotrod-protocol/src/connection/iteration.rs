//! Server-side iteration (`docs/adr/0009-server-side-iteration.md`).
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): see `connection/multimap.rs`'s module
//! docs for the mechanism that lets this `impl HotRodConnection`
//! block reach every private field/method it needs with no
//! visibility change. `read_entry_metadata` stays at `connection.rs`'s
//! root since the core `get_with_version` path and streaming also
//! depend on it.

use crate::error::{Error, Result};
use crate::header::OpCode;
use crate::listener::{write_factory_params, ServerFactory, MAX_FACTORY_PARAMS};
use crate::varint::{read_vint, write_signed_vint, write_vint};
use crate::wire::{read_array, write_array};

use super::{with_timeout, HotRodConnection, VersionedValue};

/// One `iteration_next` call's worth of a server-side iteration. An
/// empty `entries` is how the protocol signals the cursor is exhausted;
/// it carries no flag of its own for that. The response also carries
/// which segments were finished in this batch, but this phase never
/// retries a segment on another node (see
/// `docs/adr/0009-server-side-iteration.md`), so `iteration_next` reads
/// and discards those bytes rather than keeping a field nothing reads.
pub(crate) struct IterationBatch {
    pub entries: Vec<(Vec<u8>, VersionedValue)>,
}

impl HotRodConnection {
    /// Opens a server-side cursor over this connection's cache, scoped to
    /// `segments` (`None` for the whole cache, used for a single-node
    /// cache or a node with no topology yet) and, if given, evaluated
    /// through a deployed filter/converter factory (`ServerFactory`'s
    /// doc comment covers that this client never runs the logic itself).
    /// Always requests metadata for every entry, so `iteration_next` can
    /// always fill it in: `docs/adr/0009-server-side-iteration.md` covers
    /// why there is nothing worth saving by asking for less. Returns the
    /// iteration id `iteration_next`/`iteration_end` must reuse on this
    /// same connection.
    pub(crate) async fn iteration_start(
        &mut self,
        segments: Option<&[u32]>,
        filter: Option<&ServerFactory>,
        batch_size: u32,
    ) -> Result<Vec<u8>> {
        // Checked before `begin_operation` poisons the connection, the
        // same way `get_all`/`put_all` check `MAX_BULK_ENTRIES` before
        // writing anything: a caller-side mistake like this one must
        // not cost an otherwise healthy, never-touched connection.
        if let Some(factory) = filter {
            if factory.params.len() > MAX_FACTORY_PARAMS {
                return Err(Error::BatchTooLarge {
                    what: "a server-side iteration filter/converter factory's parameters",
                    len: factory.params.len(),
                    max: MAX_FACTORY_PARAMS,
                });
            }
        }
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            match segments {
                None => write_signed_vint(&mut body, -1),
                Some(segments) => {
                    let bitset = write_segment_bitset(segments);
                    write_signed_vint(&mut body, bitset.len() as i32);
                    body.extend_from_slice(&bitset);
                }
            }
            match filter {
                None => write_signed_vint(&mut body, -1),
                Some(factory) => {
                    let name_bytes = factory.name.as_bytes();
                    write_signed_vint(&mut body, name_bytes.len() as i32);
                    body.extend_from_slice(name_bytes);
                    write_factory_params(&mut body, &factory.params);
                }
            }
            write_vint(&mut body, batch_size);
            body.push(1); // metadata: always requested
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::IterationStart, &body)
                .await?;
            read_array(&mut self.stream).await
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Reads the next batch of entries from a cursor `iteration_start`
    /// opened on this same connection. An empty `entries` on the
    /// returned `IterationBatch` means the cursor is exhausted: the
    /// caller must still send `iteration_end`, since unlike
    /// `get_stream_end` the server does not clean up on its own once a
    /// cursor runs dry (confirmed against the Java client, which sends
    /// it immediately on seeing this). `Error::InvalidIteration` means
    /// the server no longer knows this cursor at all; this phase does
    /// not retry that, see the ADR.
    pub(crate) async fn iteration_next(&mut self, iteration_id: &[u8]) -> Result<IterationBatch> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, iteration_id);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::IterationNext, &body)
                .await?;
            // Finished segments: read to stay in sync with the wire, then
            // discarded. See `IterationBatch`'s doc comment for why.
            read_array(&mut self.stream).await?;
            let entries_count = read_vint(&mut self.stream).await?;
            let mut entries = Vec::new();
            if entries_count > 0 {
                let projections = read_vint(&mut self.stream).await?;
                if projections != 1 {
                    return Err(Error::MalformedIterationResponse(format!(
                        "server sent {projections} value projections per entry, expected 1"
                    )));
                }
                for _ in 0..entries_count {
                    let has_metadata = tokio::io::AsyncReadExt::read_u8(&mut self.stream).await?;
                    if has_metadata != 1 {
                        return Err(Error::MalformedIterationResponse(
                            "entry carries no metadata, even though IterationStart requested it"
                                .to_string(),
                        ));
                    }
                    let (created, lifespan, last_used, max_idle, version) =
                        self.read_entry_metadata().await?;
                    let key = read_array(&mut self.stream).await?;
                    let value = read_array(&mut self.stream).await?;
                    entries.push((
                        key,
                        VersionedValue {
                            value,
                            version,
                            created,
                            lifespan,
                            last_used,
                            max_idle,
                        },
                    ));
                }
            }
            if header.status.is_invalid_iteration() {
                return Err(Error::InvalidIteration);
            }
            Ok(IterationBatch { entries })
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Closes a cursor `iteration_start` opened on this same connection.
    /// Must always be sent, even after `iteration_next` reports the
    /// cursor exhausted: see `iteration_next`'s doc comment. A server
    /// reply of `INVALID_ITERATION` here just means the cursor is
    /// already gone, which is the outcome this call wants either way, so
    /// it is not surfaced as an error.
    pub(crate) async fn iteration_end(&mut self, iteration_id: &[u8]) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, iteration_id);
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::IterationEnd, &body)
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }
}

/// Encodes `segments` the way `java.util.BitSet.toByteArray()` would:
/// segment `n` set means byte `n / 8` has bit `n % 8` set, counted from
/// the byte's least significant bit, with the array only as long as the
/// highest segment given needs (empty if `segments` is empty). Both
/// `IterationStart`'s segment filter and `IterationNext`'s
/// finished-segments use this exact shape; confirmed against
/// `Codec30.writeIteratorStartOperation` and
/// `IterableIterationResult.segmentsToBytes`, since nothing in this
/// crate already encodes a `BitSet` this way.
fn write_segment_bitset(segments: &[u32]) -> Vec<u8> {
    let Some(&max) = segments.iter().max() else {
        return Vec::new();
    };
    let mut bytes = vec![0u8; (max / 8) as usize + 1];
    for &segment in segments {
        bytes[(segment / 8) as usize] |= 1 << (segment % 8);
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::connection::tests::{read_request, response_header};
    use crate::wire::Expiration;

    #[test]
    fn segment_bitset_encodes_scattered_and_adjacent_bits() {
        // Segment 0 and 3 set bits 0 and 3 of byte 0 (0x09); segment 9
        // sets bit 1 of byte 1 (0x02); segments 16 and 17 set bits 0
        // and 1 of byte 2 (0x03).
        let bytes = write_segment_bitset(&[0, 3, 9, 16, 17]);
        assert_eq!(bytes, vec![0x09, 0x02, 0x03]);
    }

    #[test]
    fn segment_bitset_of_an_empty_slice_is_an_empty_array() {
        assert_eq!(write_segment_bitset(&[]), Vec::<u8>::new());
    }

    #[tokio::test]
    async fn iteration_start_without_segments_or_a_filter_sends_both_sentinels() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x31, "expected an IterationStart request");
            let segments_sentinel = stream.read_u8().await.unwrap();
            assert_eq!(segments_sentinel, 0x01, "zigzag(-1): no segment filter");
            let filter_sentinel = stream.read_u8().await.unwrap();
            assert_eq!(filter_sentinel, 0x01, "zigzag(-1): no filter factory");
            let batch_size = read_vint(&mut stream).await.unwrap();
            assert_eq!(batch_size, 128);
            let metadata = stream.read_u8().await.unwrap();
            assert_eq!(metadata, 1, "metadata is always requested");

            let mut resp = response_header(id, 0x32, 0x00);
            write_array(&mut resp, b"iteration-id");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let iteration_id = conn
            .iteration_start(None, None, 128)
            .await
            .expect("iteration_start");
        assert_eq!(iteration_id, b"iteration-id");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn iteration_start_encodes_segments_and_a_filter_factory() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x31, "expected an IterationStart request");

            let bitset_len = read_vint(&mut stream).await.unwrap();
            assert_eq!(bitset_len, 4, "zigzag(2) == 4: a 2-byte bitset");
            let mut bitset = [0u8; 2];
            stream.read_exact(&mut bitset).await.unwrap();
            assert_eq!(bitset, [0x01, 0x02], "segments 0 and 9 set");

            let name_len = read_vint(&mut stream).await.unwrap();
            assert_eq!(name_len, 10, "zigzag(5) == 10: a 5-byte factory name");
            let mut name = vec![0u8; 5];
            stream.read_exact(&mut name).await.unwrap();
            assert_eq!(&name, b"my-fn");
            let param_count = stream.read_u8().await.unwrap();
            assert_eq!(param_count, 1);
            let param = read_array(&mut stream).await.unwrap();
            assert_eq!(param, b"param");

            let _batch_size = read_vint(&mut stream).await.unwrap();
            let _metadata = stream.read_u8().await.unwrap();

            let mut resp = response_header(id, 0x32, 0x00);
            write_array(&mut resp, b"iteration-id");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let filter = ServerFactory {
            name: "my-fn".to_string(),
            params: vec![b"param".to_vec()],
        };
        conn.iteration_start(Some(&[0, 9]), Some(&filter), 64)
            .await
            .expect("iteration_start");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn iteration_next_returns_entries_with_metadata() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x33, "expected an IterationNext request");
            let iteration_id = read_array(&mut stream).await.unwrap();
            assert_eq!(iteration_id, b"iteration-id");

            let mut resp = response_header(id, 0x34, 0x00);
            write_array(&mut resp, &write_segment_bitset(&[2])); // finished segments
            write_vint(&mut resp, 1); // entries count
            write_vint(&mut resp, 1); // value projections
            resp.push(1); // metadata present
            const INFINITE_MAXIDLE: u8 = 0x02;
            resp.push(INFINITE_MAXIDLE);
            resp.extend_from_slice(&1_700_000_000_000u64.to_be_bytes()); // creation
            write_vint(&mut resp, 100); // lifespan seconds
            resp.extend_from_slice(&42u64.to_be_bytes()); // version
            write_array(&mut resp, b"key");
            write_array(&mut resp, b"value");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let batch = conn
            .iteration_next(b"iteration-id")
            .await
            .expect("iteration_next");

        assert_eq!(batch.entries.len(), 1);
        let (key, value) = &batch.entries[0];
        assert_eq!(key, b"key");
        assert_eq!(value.value, b"value");
        assert_eq!(value.version, 42);
        assert_eq!(value.lifespan, Expiration::Seconds(100));
        assert_eq!(value.max_idle, Expiration::Immortal);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn iteration_next_returns_no_entries_on_exhaustion() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x33, "expected an IterationNext request");
            let _iteration_id = read_array(&mut stream).await.unwrap();

            let mut resp = response_header(id, 0x34, 0x00);
            write_array(&mut resp, &[]); // no finished segments
            write_vint(&mut resp, 0); // entries count: exhausted
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let batch = conn
            .iteration_next(b"iteration-id")
            .await
            .expect("iteration_next");

        assert!(batch.entries.is_empty());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn iteration_next_surfaces_invalid_iteration_as_a_typed_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x33, "expected an IterationNext request");
            let _iteration_id = read_array(&mut stream).await.unwrap();

            let mut resp = response_header(id, 0x34, 0x05); // INVALID_ITERATION
            write_array(&mut resp, &[]);
            write_vint(&mut resp, 0);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        let result = conn.iteration_next(b"iteration-id").await;
        assert!(matches!(result, Err(Error::InvalidIteration)));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn iteration_end_sends_the_iteration_id() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x35, "expected an IterationEnd request");
            let iteration_id = read_array(&mut stream).await.unwrap();
            assert_eq!(iteration_id, b"iteration-id");
            let resp = response_header(id, 0x36, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        conn.iteration_end(b"iteration-id")
            .await
            .expect("iteration_end");

        server.await.unwrap();
    }

    /// `IterationEnd` on a cursor the server already forgot about (already
    /// closed, or reaped) is not an error: the caller just wanted the
    /// cursor gone, which it already is.
    #[tokio::test]
    async fn iteration_end_tolerates_invalid_iteration_as_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x35, "expected an IterationEnd request");
            let _iteration_id = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x36, 0x05); // INVALID_ITERATION
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "my-cache")
            .await
            .expect("connect");
        conn.iteration_end(b"iteration-id")
            .await
            .expect("iteration_end treats INVALID_ITERATION as already closed");

        server.await.unwrap();
    }
}
