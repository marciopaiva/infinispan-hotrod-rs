//! Server-side iteration over a whole cache, per
//! `docs/adr/0009-server-side-iteration.md` (issue #53).
//!
//! A distributed cache has its segments spread across every node, so
//! covering the whole cache means opening one server-side cursor per
//! node that primary-owns at least one segment (confirmed against the
//! Java client's `RemoteCachePublisher`, which does the same, though
//! concurrently rather than one node at a time). `CacheIterator` walks
//! that list sequentially: it exhausts one node's cursor before opening
//! the next, never two at once. This is simpler than the Java client's
//! concurrent fan-out and still correct (no segment is skipped), at the
//! cost of being slower on a cluster with many nodes; see the ADR for
//! why that trade was made deliberately rather than discovered as a
//! limitation.
//!
//! Like `GetStream`/`PutStream` (#52), a cursor is pinned to the one
//! connection that opened it and is never retried or failed over: an
//! error partway through one node's cursor propagates to the caller as
//! is, leaving whatever that node owned unread. Unlike streaming's
//! `GetStreamEnd`, `IterationEnd` must always be sent, even when a
//! cursor ran dry on its own: the server does not clean up a finished
//! cursor by itself (confirmed against the Java client, which sends
//! `IterationEnd` the moment it sees an empty batch, not just when a
//! caller stops early). `NodeIterator` does exactly that, from inside
//! the call that detects exhaustion, rather than waiting for some
//! `close` the caller might never call.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::SystemTime;

use crate::error::Result;
use crate::listener::ServerFactory;
use crate::pool::PooledGuard;
use crate::remote_cache::RemoteCache;
use crate::topology::TopologyServer;
use crate::wire::Expiration;

/// Matches the Java client's `ConfigurationProperties.DEFAULT_BATCH_SIZE`:
/// how many entries the server packs into one `IterationNext` response
/// before this client has to ask for more.
const DEFAULT_BATCH_SIZE: u32 = 10_000;

/// One entry read from a `CacheIterator`. Carries the same metadata
/// `VersionedValue` does, flattened alongside the key and value instead
/// of nested, since every entry here already carries it (the iteration
/// was opened with metadata enabled, unconditionally).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IterationEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: u64,
    pub created: Option<SystemTime>,
    pub lifespan: Expiration,
    pub last_used: Option<SystemTime>,
    pub max_idle: Expiration,
}

/// Options for `RemoteCache::iter_with`. `RemoteCache::iter` is
/// `iter_with(IterationOptions::default())`: the server's own batch
/// size default, no server-side filter or converter.
#[derive(Debug, Clone, Default)]
pub struct IterationOptions {
    pub batch_size: u32,
    /// Evaluated server-side; see `ServerFactory`'s own doc comment for
    /// why this client never runs the filter or converter logic itself.
    pub filter_factory: Option<ServerFactory>,
}

impl IterationOptions {
    /// `batch_size`, or the server's own default if left at `0` (as
    /// `IterationOptions::default()` does): `0` is not a meaningful
    /// batch size to send on the wire, so there is no ambiguity in
    /// reserving it to mean "unset" instead of a dedicated `Option`.
    pub(crate) fn batch_size_or_default(&self) -> u32 {
        if self.batch_size == 0 {
            DEFAULT_BATCH_SIZE
        } else {
            self.batch_size
        }
    }
}

/// A server-side cursor over one node's primary-owned segments, pinned
/// to the connection that opened it. Buffers one `IterationNext`
/// response's worth of entries at a time (`pending`) instead of making
/// a round trip per entry.
pub(crate) struct NodeIterator {
    conn: PooledGuard,
    iteration_id: Vec<u8>,
    pending: VecDeque<IterationEntry>,
    /// Set once `IterationEnd` has been sent, successfully, for this
    /// cursor: either because it ran dry on its own (see the module
    /// docs on why that still requires sending `IterationEnd`) or
    /// because `Drop` needs to know whether sending it is still owed.
    ended: bool,
}

impl NodeIterator {
    pub(crate) fn new(conn: PooledGuard, iteration_id: Vec<u8>) -> Self {
        Self {
            conn,
            iteration_id,
            pending: VecDeque::new(),
            ended: false,
        }
    }

    async fn next_entry(&mut self) -> Result<Option<IterationEntry>> {
        if let Some(entry) = self.pending.pop_front() {
            return Ok(Some(entry));
        }
        if self.ended {
            return Ok(None);
        }
        let batch = self.conn.iteration_next(&self.iteration_id).await?;
        if batch.entries.is_empty() {
            self.conn.iteration_end(&self.iteration_id).await?;
            self.ended = true;
            return Ok(None);
        }
        self.pending = batch
            .entries
            .into_iter()
            .map(|(key, value)| IterationEntry {
                key,
                value: value.value,
                version: value.version,
                created: value.created,
                lifespan: value.lifespan,
                last_used: value.last_used,
                max_idle: value.max_idle,
            })
            .collect();
        Ok(self.pending.pop_front())
    }
}

impl Drop for NodeIterator {
    /// Poisons the connection unless `IterationEnd` already went out
    /// successfully (`ended`): anything else, including this cursor
    /// being abandoned mid-batch by `CacheIterator` itself being
    /// dropped, leaves server-side cursor state this client has no
    /// further chance to clean up, the same reasoning
    /// `streaming::GetStream`/`PutStream` already apply to their own
    /// server-side state.
    fn drop(&mut self) {
        if !self.ended {
            self.conn.mark_poisoned();
        }
    }
}

/// A cursor over an entire cache, obtained from `RemoteCache::iter`/
/// `iter_with`. Walks one node's worth of primary-owned segments at a
/// time (`current`), moving on to the next entry in `remaining_targets`
/// once a node's cursor is exhausted; see the module docs for why this
/// is sequential rather than concurrent, and why no node is retried or
/// failed over.
///
/// No `Drop` impl of its own: `current`'s own `Drop` already resolves
/// or poisons whichever connection is active when this is dropped, and
/// a target still sitting in `remaining_targets` was never opened, so
/// there is nothing else to clean up.
pub struct CacheIterator {
    cache: RemoteCache,
    remaining_targets: VecDeque<(SocketAddr, Option<TopologyServer>, Vec<u32>)>,
    current: Option<NodeIterator>,
    options: IterationOptions,
}

impl CacheIterator {
    pub(crate) fn new(
        cache: RemoteCache,
        current: Option<NodeIterator>,
        remaining_targets: VecDeque<(SocketAddr, Option<TopologyServer>, Vec<u32>)>,
        options: IterationOptions,
    ) -> Self {
        Self {
            cache,
            remaining_targets,
            current,
            options,
        }
    }

    /// Returns the next entry, opening the next node's cursor as each
    /// one runs dry, or `None` once every node has been fully read.
    ///
    /// On error, `current` is cleared first: the connection behind it is
    /// already poisoned (by whichever `iteration_next`/`iteration_end`
    /// call failed), so leaving it in place would just make a following
    /// call re-enter the same dead node and fail again with
    /// `Error::PoisonedConnection` instead of the error that actually
    /// happened. A target is only popped from `remaining_targets` once
    /// `open_node_iterator` for it has actually succeeded, so a caller
    /// that calls this again after an error opening a node retries that
    /// same node rather than silently skipping it.
    pub async fn next_entry(&mut self) -> Result<Option<IterationEntry>> {
        loop {
            if let Some(node) = self.current.as_mut() {
                match node.next_entry().await {
                    Ok(Some(entry)) => return Ok(Some(entry)),
                    Ok(None) => self.current = None,
                    Err(err) => {
                        self.current = None;
                        return Err(err);
                    }
                }
            }
            let Some((addr, origin, segments)) = self.remaining_targets.front().cloned() else {
                return Ok(None);
            };
            self.current = Some(
                self.cache
                    .open_node_iterator(addr, origin, segments, &self.options)
                    .await?,
            );
            self.remaining_targets.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::client::tests::{read_request_opcode, response_header, unreachable_addr};
    use crate::client::{ClusterTopology, HotRodClient};
    use crate::error::Error;
    use crate::remote_cache::tests::client_with_seeds_and_timeout;
    use crate::varint::{read_vint, write_vint};
    use crate::wire::{read_array, write_array};
    use std::time::Duration;

    fn client_with_seed(seed_addr: SocketAddr) -> HotRodClient {
        client_with_seeds_and_timeout(vec![seed_addr], seed_addr, Duration::from_secs(5))
    }

    #[tokio::test]
    async fn iter_reads_every_entry_from_a_single_node_without_poisoning_the_connection() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = tcp.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x31, "expected an IterationStart request");
            let segments_sentinel = sock.read_u8().await.unwrap();
            assert_eq!(segments_sentinel, 0x01, "no topology: no segment filter");
            let filter_sentinel = sock.read_u8().await.unwrap();
            assert_eq!(filter_sentinel, 0x01, "no filter factory");
            let _batch_size = read_vint(&mut sock).await.unwrap();
            let _metadata = sock.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x32, 0x00);
            write_array(&mut resp, b"iter-1");
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x33, "expected an IterationNext request");
            let iteration_id = read_array(&mut sock).await.unwrap();
            assert_eq!(iteration_id, b"iter-1");
            let mut resp = response_header(id, 0x34, 0x00);
            write_array(&mut resp, &[]); // finished segments
            write_vint(&mut resp, 1); // entries count
            write_vint(&mut resp, 1); // value projections
            resp.push(1); // metadata present
            resp.push(0x03); // immortal lifespan and max idle
            resp.extend_from_slice(&7u64.to_be_bytes()); // version
            write_array(&mut resp, b"key-1");
            write_array(&mut resp, b"value-1");
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x33, "expected a second IterationNext request");
            let _iteration_id = read_array(&mut sock).await.unwrap();
            let mut resp = response_header(id, 0x34, 0x00);
            write_array(&mut resp, &[]); // finished segments
            write_vint(&mut resp, 0); // entries count: exhausted
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(
                opcode, 0x35,
                "expected an IterationEnd request, sent automatically on exhaustion"
            );
            let iteration_id = read_array(&mut sock).await.unwrap();
            assert_eq!(iteration_id, b"iter-1");
            let resp = response_header(id, 0x36, 0x00);
            sock.write_all(&resp).await.unwrap();

            // Proves the connection was not poisoned: reused directly for
            // a Ping, no second accept anywhere in this test.
            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(
                opcode, 0x17,
                "expected a Ping request on the same connection"
            );
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            write_vint(&mut resp, 0);
            sock.write_all(&resp).await.unwrap();
        });

        let client = client_with_seed(addr);
        let cache = client.cache("my-cache");
        let mut iter = cache.iter().await.expect("iter");

        let entry = iter
            .next_entry()
            .await
            .expect("next_entry")
            .expect("one entry");
        assert_eq!(entry.key, b"key-1");
        assert_eq!(entry.value, b"value-1");
        assert_eq!(entry.version, 7);
        assert_eq!(entry.lifespan, Expiration::Immortal);

        assert!(iter.next_entry().await.expect("next_entry").is_none());
        drop(iter);

        cache
            .ping()
            .await
            .expect("ping should reuse the connection");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn cache_iterator_dropped_mid_scan_poisons_the_connection() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut first, _) = tcp.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut first).await;
            assert_eq!(opcode, 0x31, "expected an IterationStart request");
            let _segments_sentinel = first.read_u8().await.unwrap();
            let _filter_sentinel = first.read_u8().await.unwrap();
            let _batch_size = read_vint(&mut first).await.unwrap();
            let _metadata = first.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x32, 0x00);
            write_array(&mut resp, b"iter-1");
            first.write_all(&resp).await.unwrap();

            // Dropped here without reading to exhaustion or sending
            // IterationEnd: a following Ping must open a new connection.
            let (mut second, _) = tcp.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut second).await;
            assert_eq!(opcode, 0x17, "expected a Ping request on a new connection");
            let mut resp = response_header(id, 0x18, 0x00);
            resp.push(0);
            resp.push(0);
            resp.push(41);
            write_vint(&mut resp, 0);
            second.write_all(&resp).await.unwrap();
        });

        let client = client_with_seed(addr);
        let cache = client.cache("my-cache");
        let iter = cache.iter().await.expect("iter");
        drop(iter);

        cache
            .ping()
            .await
            .expect("ping should open a new connection, not reuse the poisoned one");

        server.await.unwrap();
    }

    /// Covers the central design decision of #53: a distributed cache's
    /// segments are spread across every node, so `CacheIterator` must
    /// open one cursor per owning node and must not stop once the
    /// first one is exhausted. Two synthetic nodes, each primary-owning
    /// one segment; `next_entry` must surface both nodes' entries
    /// before reporting exhaustion, moving on sequentially rather than
    /// concurrently (the fake servers below each only ever see one
    /// connection, in exactly the request order a sequential walk would
    /// produce).
    #[tokio::test]
    async fn cache_iterator_fans_out_sequentially_across_every_owning_node() {
        async fn serve_one_node_cursor(
            listener: TcpListener,
            iteration_id: &'static [u8],
            key: &'static [u8],
            value: &'static [u8],
        ) {
            let (mut sock, _) = listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x31, "expected an IterationStart request");
            let _segments_sentinel_or_len = read_vint(&mut sock).await.unwrap();
            let mut bitset = vec![0u8; 1];
            sock.read_exact(&mut bitset).await.unwrap();
            let _filter_sentinel = sock.read_u8().await.unwrap();
            let _batch_size = read_vint(&mut sock).await.unwrap();
            let _metadata = sock.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x32, 0x00);
            write_array(&mut resp, iteration_id);
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x33, "expected an IterationNext request");
            let got_id = read_array(&mut sock).await.unwrap();
            assert_eq!(got_id, iteration_id);
            let mut resp = response_header(id, 0x34, 0x00);
            write_array(&mut resp, &[]);
            write_vint(&mut resp, 1);
            write_vint(&mut resp, 1);
            resp.push(1);
            resp.push(0x03);
            resp.extend_from_slice(&1u64.to_be_bytes());
            write_array(&mut resp, key);
            write_array(&mut resp, value);
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x33, "expected a second IterationNext request");
            let _got_id = read_array(&mut sock).await.unwrap();
            let mut resp = response_header(id, 0x34, 0x00);
            write_array(&mut resp, &[]);
            write_vint(&mut resp, 0);
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x35, "expected an IterationEnd request");
            let got_id = read_array(&mut sock).await.unwrap();
            assert_eq!(got_id, iteration_id);
            let resp = response_header(id, 0x36, 0x00);
            sock.write_all(&resp).await.unwrap();
        }

        let node_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_a_addr = node_a.local_addr().unwrap();
        let node_b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_b_addr = node_b.local_addr().unwrap();

        let task_a = tokio::spawn(serve_one_node_cursor(
            node_a, b"iter-a", b"key-a", b"value-a",
        ));
        let task_b = tokio::spawn(serve_one_node_cursor(
            node_b, b"iter-b", b"key-b", b"value-b",
        ));

        let seed_addr = unreachable_addr().await;
        let client =
            client_with_seeds_and_timeout(vec![seed_addr], seed_addr, Duration::from_secs(5));
        *client.inner().topology.write().unwrap() = Some(Arc::new(ClusterTopology {
            topology_id: 9,
            servers: vec![
                TopologyServer {
                    host: node_a_addr.ip().to_string(),
                    port: node_a_addr.port(),
                },
                TopologyServer {
                    host: node_b_addr.ip().to_string(),
                    port: node_b_addr.port(),
                },
            ],
            hash_function_version: 3,
            segment_owners: vec![vec![0], vec![1]],
            resolved_addrs: RwLock::new(std::collections::HashMap::new()),
        }));

        let cache = client.cache("my-cache");
        let mut iter = cache.iter().await.expect("iter");

        let mut entries = Vec::new();
        while let Some(entry) = iter.next_entry().await.expect("next_entry") {
            entries.push((entry.key, entry.value));
        }
        entries.sort();

        assert_eq!(
            entries,
            vec![
                (b"key-a".to_vec(), b"value-a".to_vec()),
                (b"key-b".to_vec(), b"value-b".to_vec()),
            ]
        );

        task_a.await.unwrap();
        task_b.await.unwrap();
    }

    /// Regression test for a review finding: before the fix, an error
    /// from the active node left `current` set to that same (now
    /// poisoned) node, so a following call re-entered it and failed
    /// again with `Error::PoisonedConnection` instead of correctly
    /// reporting the iteration over (there was only ever this one
    /// target, and `open_node_iterator` for it already succeeded, so
    /// there is nothing left to retry once it errors mid-scan).
    #[tokio::test]
    async fn cache_iterator_clears_the_failed_node_instead_of_wedging_on_error() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = tcp.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x31, "expected an IterationStart request");
            let _segments_sentinel = sock.read_u8().await.unwrap();
            let _filter_sentinel = sock.read_u8().await.unwrap();
            let _batch_size = read_vint(&mut sock).await.unwrap();
            let _metadata = sock.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x32, 0x00);
            write_array(&mut resp, b"iter-1");
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x33, "expected an IterationNext request");
            let _iteration_id = read_array(&mut sock).await.unwrap();
            let mut resp = response_header(id, 0x34, 0x00);
            write_array(&mut resp, &[]); // finished segments
            write_vint(&mut resp, 1); // entries count
            write_vint(&mut resp, 2); // value projections: invalid, must be 1
            sock.write_all(&resp).await.unwrap();
        });

        let client = client_with_seed(addr);
        let cache = client.cache("my-cache");
        let mut iter = cache.iter().await.expect("iter");

        let err = iter
            .next_entry()
            .await
            .expect_err("a malformed response must surface as an error");
        assert!(matches!(err, Error::MalformedIterationResponse(_)));

        let result = iter
            .next_entry()
            .await
            .expect("the single target is already spent, not retried");
        assert!(result.is_none());

        server.await.unwrap();
    }

    /// Regression test for a review finding: before the fix, the next
    /// target was popped from `remaining_targets` before
    /// `open_node_iterator` ran, so a failure opening it (here, node
    /// b's address never accepts a connection) lost that target for
    /// good; a following call would have wrongly reported the whole
    /// iteration done instead of retrying the same node. Built by
    /// constructing `CacheIterator` directly instead of through
    /// `RemoteCache::iter`, so which node is "first" and "second" does
    /// not depend on `nodes_and_owned_segments`'s `HashMap` iteration
    /// order.
    #[tokio::test]
    async fn cache_iterator_retries_the_same_node_after_a_failed_open() {
        let node_a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let node_a_addr = node_a.local_addr().unwrap();
        let node_b_addr = unreachable_addr().await;

        let task_a = tokio::spawn(async move {
            let (mut sock, _) = node_a.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x31, "expected an IterationStart request");
            let _segments_sentinel = sock.read_u8().await.unwrap();
            let _filter_sentinel = sock.read_u8().await.unwrap();
            let _batch_size = read_vint(&mut sock).await.unwrap();
            let _metadata = sock.read_u8().await.unwrap();
            let mut resp = response_header(id, 0x32, 0x00);
            write_array(&mut resp, b"iter-a");
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x33, "expected an IterationNext request");
            let _iteration_id = read_array(&mut sock).await.unwrap();
            let mut resp = response_header(id, 0x34, 0x00);
            write_array(&mut resp, &[]); // finished segments
            write_vint(&mut resp, 0); // entries count: exhausted immediately
            sock.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut sock).await;
            assert_eq!(opcode, 0x35, "expected an IterationEnd request");
            let _iteration_id = read_array(&mut sock).await.unwrap();
            let resp = response_header(id, 0x36, 0x00);
            sock.write_all(&resp).await.unwrap();
        });

        let seed_addr = unreachable_addr().await;
        let client =
            client_with_seeds_and_timeout(vec![seed_addr], seed_addr, Duration::from_millis(300));
        let cache = client.cache("my-cache");

        let mut guard = client
            .checkout(node_a_addr, "my-cache", None)
            .await
            .expect("checkout node a");
        let iteration_id = guard
            .iteration_start(None, None, 10_000)
            .await
            .expect("iteration_start on node a");
        let node_a_iter = NodeIterator::new(guard, iteration_id);

        let mut iter = CacheIterator::new(
            cache,
            Some(node_a_iter),
            VecDeque::from(vec![(node_b_addr, None, Vec::new())]),
            IterationOptions::default(),
        );

        // Node a is already exhausted and ended; the loop inside
        // `next_entry` moves straight on to node b, which never
        // accepts a connection.
        let first_attempt = iter.next_entry().await;
        assert!(matches!(first_attempt, Err(Error::Io(_))));

        // If the target had been popped before the failed open (the
        // bug this test guards against), this second call would see
        // an empty `remaining_targets` and wrongly return `Ok(None)`
        // instead of failing the same way again.
        let second_attempt = iter.next_entry().await;
        assert!(matches!(second_attempt, Err(Error::Io(_))));

        task_a.await.unwrap();
    }
}
