//! Distributed counter listeners: value-change events pushed by the
//! server, the same dedicated-connection, no-reconnection shape
//! `listener.rs` already established for cache events (ADR 0006, #4).
//! See `docs/adr/0022-distributed-counter-listeners.md` for the
//! decision this module implements, and `docs/adr/0015-distributed-counters.md`
//! for the counter protocol this builds on.
//!
//! The counter listener protocol is simpler than the cache one in
//! every place it differs: no server-side filtering, so no
//! factories, no interests bitmask; no `isCustom`/`isRetried` bytes
//! on an event, since every event is the same shape (a value change).
//! What it adds that a cache event does not carry is the counter's
//! own name on the wire, confirmed against the Java client/server
//! source (`infinispan/infinispan`): `ClientCounterEvent.writeTo`
//! (server-side encoder) and `Codec30.readCounterEvent` (client-side
//! decoder) agree the event frame, after the generic magic/message-id/
//! opcode, is: status byte, topology marker (always `0`, same as a
//! cache event), `counterName` (string), `listenerId` (array), one
//! byte of packed old/new `CounterState`, `oldValue` (i64), `newValue`
//! (i64).

use std::io;
use std::time::Duration;

use rand::RngCore;
use tokio::io::{AsyncRead, AsyncReadExt, BufStream};
use tokio::sync::watch;

use crate::connection::{with_timeout, HotRodConnection, DEFAULT_TOPOLOGY_ID};
use crate::error::{Error, Result};
use crate::header::{write_and_read_header, OpCode, RESPONSE_MAGIC};
use crate::status::Status;
use crate::tls::Transport;
use crate::topology::ClientIntelligence;
use crate::varint::read_vlong;
use crate::wire::{read_array, read_string, write_array};

/// The pushed event opcode itself has no corresponding request, the
/// same reason `listener.rs`'s own `EVENT_CREATED` etc. are not in
/// `header::OpCode` either (see that enum's own docs on
/// `CounterAddListener`/`CounterRemoveListener`).
const COUNTER_EVENT: u8 = 0x66;

/// A counter's state at one side of a value change, packed into one
/// byte alongside its counterpart (bits 0-1 for the old state, bits
/// 2-3 for the new one): only meaningful for a bounded strong
/// counter, `Valid` for every other kind. Mirrors
/// `org.infinispan.counter.api.CounterState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterState {
    Valid,
    LowerBoundReached,
    UpperBoundReached,
}

fn decode_old_state(encoded: u8) -> Result<CounterState> {
    match encoded & 0x03 {
        0x00 => Ok(CounterState::Valid),
        0x01 => Ok(CounterState::LowerBoundReached),
        0x02 => Ok(CounterState::UpperBoundReached),
        other => Err(Error::MalformedEvent(format!(
            "unrecognized counter old-state bits: {other:#04x}"
        ))),
    }
}

fn decode_new_state(encoded: u8) -> Result<CounterState> {
    match encoded & 0x0C {
        0x00 => Ok(CounterState::Valid),
        0x04 => Ok(CounterState::LowerBoundReached),
        0x08 => Ok(CounterState::UpperBoundReached),
        other => Err(Error::MalformedEvent(format!(
            "unrecognized counter new-state bits: {other:#04x}"
        ))),
    }
}

/// One value-change event pushed by the server to a `CounterListener`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounterEvent {
    pub old_value: i64,
    pub old_state: CounterState,
    pub new_value: i64,
    pub new_state: CounterState,
}

/// A registered counter listener: a dedicated connection that does
/// nothing but receive value-change events from here on, for one
/// specific counter (see the module docs for why). Dropping this
/// without calling `close` just closes the socket; the server notices
/// the connection is gone and cleans up the registration on its own,
/// the same as `listener.rs`'s `CacheListener` already documents for
/// a dropped cache listener.
pub struct CounterListener {
    stream: BufStream<Transport>,
    listener_id: Vec<u8>,
    counter_name: String,
    cache_name: Vec<u8>,
    timeout: Duration,
    /// Set once a frame has started being read (the magic byte
    /// arrived), cleared only once that whole frame was parsed
    /// successfully. Same rule, for the same reason, as
    /// `listener.rs`'s `CacheListener::poisoned`.
    poisoned: bool,
    /// Watched the same way `CacheListener` does, so a cluster switch
    /// away from this listener's own cluster surfaces as a terminal
    /// `Error::ListenerClusterSwitched` instead of waiting forever on
    /// a connection that no longer has anything to do with the
    /// cluster this client now routes against. See
    /// `docs/adr/0019-multicluster-listener-and-pool-cleanup.md`.
    generation_rx: watch::Receiver<u64>,
}

impl CounterListener {
    /// Builds the `CounterAddListener` request body and registers it
    /// on `conn`, which must not be reused afterward: see
    /// `HotRodConnection::into_transport`.
    pub(crate) async fn register(
        mut conn: HotRodConnection,
        counter_name: &str,
        cache_name: &[u8],
        timeout: Duration,
        generation_rx: watch::Receiver<u64>,
    ) -> Result<Self> {
        let mut listener_id = vec![0u8; 16];
        rand::thread_rng().fill_bytes(&mut listener_id);

        let mut body = Vec::new();
        write_array(&mut body, counter_name.as_bytes());
        write_array(&mut body, &listener_id);

        conn.counter_add_listener(&body).await?;

        Ok(Self {
            stream: conn.into_transport(),
            listener_id,
            counter_name: counter_name.to_string(),
            cache_name: cache_name.to_vec(),
            timeout,
            poisoned: false,
            generation_rx,
        })
    }

    /// Waits for the next value-change event. Same semantics as
    /// `CacheListener::next`: `None` only for a clean close between
    /// frames, a terminal `Err` for anything else (including a
    /// cluster switch, via `generation_rx`), no reconnection. Not
    /// bounded by this listener's timeout, for the same reason
    /// `CacheListener::next` is not: waiting for the next event is
    /// this listener's normal state, not a hung request.
    pub async fn next(&mut self) -> Option<Result<CounterEvent>> {
        if self.poisoned {
            return Some(Err(Error::PoisonedConnection));
        }
        let magic = tokio::select! {
            biased;
            Ok(()) = self.generation_rx.changed() => {
                self.poisoned = true;
                return Some(Err(Error::ListenerClusterSwitched));
            }
            byte = self.stream.read_u8() => match byte {
                Ok(byte) => byte,
                Err(io_err) if io_err.kind() == io::ErrorKind::UnexpectedEof => return None,
                Err(io_err) => return Some(Err(Error::Io(io_err))),
            },
        };
        self.poisoned = true;
        let result = read_counter_event_after_magic(
            &mut self.stream,
            magic,
            &self.counter_name,
            &self.listener_id,
        )
        .await;
        self.poisoned = result.is_err();
        Some(result)
    }

    /// Unregisters the listener and waits for the server's
    /// confirmation, the counter equivalent of `CacheListener::close`.
    pub async fn close(mut self) -> Result<()> {
        let mut body = Vec::new();
        write_array(&mut body, self.counter_name.as_bytes());
        write_array(&mut body, &self.listener_id);

        let timeout = self.timeout;
        with_timeout(timeout, async {
            write_and_read_header(
                &mut self.stream,
                0,
                &self.cache_name,
                OpCode::CounterRemoveListener,
                ClientIntelligence::Basic,
                DEFAULT_TOPOLOGY_ID,
                &body,
                crate::header::RequestMediaType::None,
            )
            .await?;
            Ok(())
        })
        .await
    }
}

/// Reads the rest of one `COUNTER_EVENT` frame given its already-read
/// magic byte. Not built on `header::read_response_header`, for the
/// same reason `listener.rs`'s own `read_event_after_magic` is not:
/// an event frame has no pending request to validate its opcode
/// against, and the magic byte was already consumed by `next` to
/// tell a clean close between frames apart from one partway through.
async fn read_counter_event_after_magic<R: AsyncRead + Unpin>(
    stream: &mut R,
    magic: u8,
    expected_counter_name: &str,
    expected_listener_id: &[u8],
) -> Result<CounterEvent> {
    if magic != RESPONSE_MAGIC {
        return Err(Error::InvalidMagic(magic));
    }
    let _message_id = read_vlong(stream).await?;
    let opcode = stream.read_u8().await?;
    let status = Status(stream.read_u8().await?);
    let _topology_marker = stream.read_u8().await?; // always 0 for events

    if status.is_error() {
        let message = read_string(stream).await?;
        return Err(Error::Server {
            status: status.0,
            message,
        });
    }
    if !status.is_known() {
        return Err(Error::UnknownStatus(status.0));
    }
    if opcode != COUNTER_EVENT {
        return Err(Error::MalformedEvent(format!(
            "unrecognized counter event opcode {opcode:#04x}"
        )));
    }

    let counter_name = read_string(stream).await?;
    if counter_name != expected_counter_name {
        return Err(Error::MalformedEvent(
            "counter event's own counter name does not match this connection's own, on a \
             connection dedicated to a single counter listener"
                .to_string(),
        ));
    }
    let listener_id = read_array(stream).await?;
    if listener_id != expected_listener_id {
        return Err(Error::MalformedEvent(
            "counter event's own listener id does not match this connection's own, on a \
             connection dedicated to a single listener"
                .to_string(),
        ));
    }

    let encoded_state = stream.read_u8().await?;
    let old_value = stream.read_i64().await?;
    let new_value = stream.read_i64().await?;
    Ok(CounterEvent {
        old_value,
        old_state: decode_old_state(encoded_state)?,
        new_value,
        new_state: decode_new_state(encoded_state)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    use crate::client::tests::{read_request_opcode, response_header};
    use crate::varint::write_vlong;

    fn counter_event_frame(
        message_id: u64,
        counter_name: &str,
        listener_id: &[u8],
        old_value: i64,
        old_state: u8,
        new_value: i64,
        new_state: u8,
    ) -> Vec<u8> {
        let mut buf = vec![0xA1];
        write_vlong(&mut buf, message_id);
        buf.push(COUNTER_EVENT);
        buf.push(0x00); // status: success
        buf.push(0); // topology marker: always 0 for events
        write_array(&mut buf, counter_name.as_bytes());
        write_array(&mut buf, listener_id);
        buf.push(old_state | new_state);
        buf.extend_from_slice(&old_value.to_be_bytes());
        buf.extend_from_slice(&new_value.to_be_bytes());
        buf
    }

    /// Reads one `CounterAddListener` request far enough to pull out
    /// its message id, counter name and listener id, trusting the
    /// body's shape is otherwise covered by
    /// `register_sends_well_formed_counter_add_listener_request`.
    async fn read_counter_listener_id(stream: &mut TcpStream) -> (u64, String, Vec<u8>) {
        let (id, opcode) = read_request_opcode(stream).await;
        assert_eq!(opcode, 0x5A, "expected a CounterAddListener request");
        let counter_name = read_string(stream).await.unwrap();
        let listener_id = read_array(stream).await.unwrap();
        (id, counter_name, listener_id)
    }

    #[tokio::test]
    async fn register_sends_well_formed_counter_add_listener_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x5A, "expected a CounterAddListener request");

            let counter_name = read_string(&mut stream).await.unwrap();
            assert_eq!(counter_name, "my-counter");
            let listener_id = read_array(&mut stream).await.unwrap();
            assert_eq!(listener_id.len(), 16, "listener id should be 16 bytes");

            let resp = response_header(id, 0x5B, 0x00);
            stream.write_all(&resp).await.unwrap();
            stream
        });

        let conn = HotRodConnection::connect(addr, "org.infinispan.COUNTER")
            .await
            .unwrap();
        let listener = CounterListener::register(
            conn,
            "my-counter",
            b"org.infinispan.COUNTER",
            Duration::from_secs(5),
            watch::channel(0).1,
        )
        .await
        .expect("register");
        assert_eq!(listener.listener_id.len(), 16);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn next_parses_a_value_change_event() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, _name, listener_id) = read_counter_listener_id(&mut stream).await;
            let resp = response_header(id, 0x5B, 0x00);
            stream.write_all(&resp).await.unwrap();

            // valid -> valid
            let valid = counter_event_frame(0, "my-counter", &listener_id, 1, 0x00, 2, 0x00);
            // valid -> upper bound reached
            let upper = counter_event_frame(0, "my-counter", &listener_id, 9, 0x00, 10, 0x08);
            // lower bound reached -> valid
            let lower = counter_event_frame(0, "my-counter", &listener_id, 0, 0x01, 1, 0x00);
            stream.write_all(&valid).await.unwrap();
            stream.write_all(&upper).await.unwrap();
            stream.write_all(&lower).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "org.infinispan.COUNTER")
            .await
            .unwrap();
        let mut listener = CounterListener::register(
            conn,
            "my-counter",
            b"org.infinispan.COUNTER",
            Duration::from_secs(5),
            watch::channel(0).1,
        )
        .await
        .expect("register");

        assert_eq!(
            listener.next().await.unwrap().unwrap(),
            CounterEvent {
                old_value: 1,
                old_state: CounterState::Valid,
                new_value: 2,
                new_state: CounterState::Valid,
            }
        );
        assert_eq!(
            listener.next().await.unwrap().unwrap(),
            CounterEvent {
                old_value: 9,
                old_state: CounterState::Valid,
                new_value: 10,
                new_state: CounterState::UpperBoundReached,
            }
        );
        assert_eq!(
            listener.next().await.unwrap().unwrap(),
            CounterEvent {
                old_value: 0,
                old_state: CounterState::LowerBoundReached,
                new_value: 1,
                new_state: CounterState::Valid,
            }
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn next_returns_none_once_the_server_closes_cleanly() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, _name, _listener_id) = read_counter_listener_id(&mut stream).await;
            let resp = response_header(id, 0x5B, 0x00);
            stream.write_all(&resp).await.unwrap();
            // Dropping `stream` here closes the connection cleanly.
        });

        let conn = HotRodConnection::connect(addr, "org.infinispan.COUNTER")
            .await
            .unwrap();
        let mut listener = CounterListener::register(
            conn,
            "my-counter",
            b"org.infinispan.COUNTER",
            Duration::from_secs(5),
            watch::channel(0).1,
        )
        .await
        .expect("register");

        assert!(listener.next().await.is_none());

        server.await.unwrap();
    }

    /// `HotRodClient::switch_to_cluster` (or automatic failover) bumps
    /// the generation this listener watches; `next` must notice that
    /// promptly, the same guarantee `listener.rs`'s own analogous test
    /// confirms for `CacheListener`.
    #[tokio::test]
    async fn next_returns_a_terminal_error_once_the_cluster_generation_changes() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, _name, _listener_id) = read_counter_listener_id(&mut stream).await;
            let resp = response_header(id, 0x5B, 0x00);
            stream.write_all(&resp).await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let conn = HotRodConnection::connect(addr, "org.infinispan.COUNTER")
            .await
            .unwrap();
        let (generation_tx, generation_rx) = watch::channel(0u64);
        let mut listener = CounterListener::register(
            conn,
            "my-counter",
            b"org.infinispan.COUNTER",
            Duration::from_secs(5),
            generation_rx,
        )
        .await
        .expect("register");

        generation_tx.send_modify(|generation| *generation += 1);

        assert!(matches!(
            listener.next().await,
            Some(Err(Error::ListenerClusterSwitched))
        ));
        assert!(matches!(
            listener.next().await,
            Some(Err(Error::PoisonedConnection))
        ));
    }

    #[tokio::test]
    async fn next_surfaces_a_server_error_as_err() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, _name, _listener_id) = read_counter_listener_id(&mut stream).await;
            let resp = response_header(id, 0x5B, 0x00);
            stream.write_all(&resp).await.unwrap();

            let mut err_frame = vec![0xA1];
            write_vlong(&mut err_frame, 0);
            err_frame.push(0x50); // generic ERROR_RESPONSE opcode
            err_frame.push(0x85); // SERVER_ERROR status
            err_frame.push(0); // topology marker
            write_array(&mut err_frame, b"boom");
            stream.write_all(&err_frame).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "org.infinispan.COUNTER")
            .await
            .unwrap();
        let mut listener = CounterListener::register(
            conn,
            "my-counter",
            b"org.infinispan.COUNTER",
            Duration::from_secs(5),
            watch::channel(0).1,
        )
        .await
        .expect("register");

        let err = listener.next().await.unwrap().unwrap_err();
        match err {
            Error::Server { status, message } => {
                assert_eq!(status, 0x85);
                assert_eq!(message, "boom");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }

        server.await.unwrap();
    }

    #[tokio::test]
    async fn next_rejects_an_unrecognized_status_byte() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, _name, _listener_id) = read_counter_listener_id(&mut stream).await;
            let resp = response_header(id, 0x5B, 0x00);
            stream.write_all(&resp).await.unwrap();

            let mut frame = vec![0xA1];
            write_vlong(&mut frame, 0);
            frame.push(COUNTER_EVENT);
            frame.push(0x99); // unrecognized status
            frame.push(0); // topology marker
            stream.write_all(&frame).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "org.infinispan.COUNTER")
            .await
            .unwrap();
        let mut listener = CounterListener::register(
            conn,
            "my-counter",
            b"org.infinispan.COUNTER",
            Duration::from_secs(5),
            watch::channel(0).1,
        )
        .await
        .expect("register");

        let err = listener.next().await.unwrap().unwrap_err();
        assert!(
            matches!(err, Error::UnknownStatus(0x99)),
            "unexpected error variant: {err:?}"
        );

        server.await.unwrap();
    }

    /// A connection dropped partway through a frame must surface as a
    /// terminal `Err` and poison the listener, the same guarantee
    /// `listener.rs`'s own analogous test confirms for `CacheListener`.
    #[tokio::test]
    async fn next_errors_and_poisons_on_a_connection_dropped_mid_frame() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, _name, _listener_id) = read_counter_listener_id(&mut stream).await;
            let resp = response_header(id, 0x5B, 0x00);
            stream.write_all(&resp).await.unwrap();

            // Only the first few bytes of a frame: a real frame has
            // more after this. Dropping `stream` here closes the
            // connection with a frame already under way.
            let mut partial = vec![0xA1];
            write_vlong(&mut partial, 0);
            partial.push(COUNTER_EVENT);
            partial.push(0x00);
            partial.push(0);
            stream.write_all(&partial).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "org.infinispan.COUNTER")
            .await
            .unwrap();
        let mut listener = CounterListener::register(
            conn,
            "my-counter",
            b"org.infinispan.COUNTER",
            Duration::from_secs(5),
            watch::channel(0).1,
        )
        .await
        .expect("register");

        let first = listener.next().await;
        assert!(
            matches!(first, Some(Err(_))),
            "a connection dropped mid-frame must not look like a clean close: {first:?}"
        );

        let second = listener.next().await;
        assert!(
            matches!(second, Some(Err(Error::PoisonedConnection))),
            "a listener that errored mid-frame should fail fast on the next call: {second:?}"
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn close_sends_counter_remove_listener_and_waits_for_the_response() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, _name, listener_id) = read_counter_listener_id(&mut stream).await;
            let resp = response_header(id, 0x5B, 0x00);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x5C, "expected a CounterRemoveListener request");
            let removed_name = read_string(&mut stream).await.unwrap();
            assert_eq!(removed_name, "my-counter");
            let removed_listener_id = read_array(&mut stream).await.unwrap();
            assert_eq!(removed_listener_id, listener_id);
            let resp = response_header(id, 0x5D, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "org.infinispan.COUNTER")
            .await
            .unwrap();
        let listener = CounterListener::register(
            conn,
            "my-counter",
            b"org.infinispan.COUNTER",
            Duration::from_secs(5),
            watch::channel(0).1,
        )
        .await
        .expect("register");

        listener.close().await.expect("close");

        server.await.unwrap();
    }
}
