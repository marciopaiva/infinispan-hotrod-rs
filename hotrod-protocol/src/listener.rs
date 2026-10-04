//! Client listeners: a dedicated connection that receives cache events
//! the server pushes, instead of the usual one-response-per-request
//! shape. Phase 4 of ADR 0001; see
//! `docs/adr/0006-client-listeners.md` for the decision this module
//! implements.
//!
//! **A listener gets its own connection**, separate from
//! `HotRodClient`'s pool (`pool.rs`, #77) and never reused for ordinary
//! cache operations. Once `AddClientListener` succeeds, the server can
//! push an event frame at any time, not just right after a request Hot
//! Rod's `read_response_header` expects to pair it with: `connection.rs`'s
//! module docs explain why `HotRodConnection` itself only ever writes
//! one request and reads exactly one response before the next call may
//! proceed. Rather than teach that model (or the shared pool) to
//! demultiplex an unbounded, unsolicited stream of event frames, a
//! listener's connection is handed off (`HotRodConnection::into_transport`)
//! once registration succeeds, and `CacheListener` reads directly off it
//! from then on.
//!
//! **No reconnection.** If the connection drops, `CacheListener::next`
//! returns `None` (a clean close) or a terminal `Err` (anything else);
//! the caller decides whether to register a new listener. Automatic
//! reconnection is a documented limitation, not an oversight: see the
//! ADR.

use std::io;
use std::time::Duration;

use rand::RngCore;
use tokio::io::{AsyncRead, AsyncReadExt, BufStream};

use crate::connection::{with_timeout, HotRodConnection, DEFAULT_TOPOLOGY_ID};
use crate::error::{Error, Result};
use crate::header::{write_and_read_header, OpCode, RESPONSE_MAGIC};
use crate::status::Status;
use crate::tls::Transport;
use crate::topology::ClientIntelligence;
use crate::varint::{read_vlong, write_vint};
use crate::wire::{read_array, read_string, write_array};

const EVENT_CREATED: u8 = 0x60;
const EVENT_MODIFIED: u8 = 0x61;
const EVENT_REMOVED: u8 = 0x62;
const EVENT_EXPIRED: u8 = 0x63;

/// Safety ceiling on a filter/converter factory's parameter count: the
/// wire encodes it as a single byte (`Codec30.writeNamedFactory`,
/// `Codec30.writeIteratorStartOperation`), so `255` is the most this
/// protocol can express at all, not a limit this client chose. Checked
/// before writing anything, the same reasoning `MAX_BULK_ENTRIES` already
/// uses for `get_all`/`put_all`: silently truncating a larger count would
/// desync the request instead of rejecting it up front. `pub(crate)`: the
/// same `ServerFactory` is also accepted by `iteration.rs`'s server-side
/// iteration filter.
pub(crate) const MAX_FACTORY_PARAMS: usize = u8::MAX as usize;

/// Which event types a listener receives: a bitmask on the wire (`0x01`
/// created, `0x02` modified, `0x04` removed, `0x08` expired). Defaults to
/// every type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheEventInterests {
    pub created: bool,
    pub modified: bool,
    pub removed: bool,
    pub expired: bool,
}

impl CacheEventInterests {
    /// Every event type: what `ListenOptions::default` uses.
    pub fn all() -> Self {
        Self {
            created: true,
            modified: true,
            removed: true,
            expired: true,
        }
    }

    fn as_vint(self) -> u32 {
        let mut bits = 0u32;
        if self.created {
            bits |= 0x01;
        }
        if self.modified {
            bits |= 0x02;
        }
        if self.removed {
            bits |= 0x04;
        }
        if self.expired {
            bits |= 0x08;
        }
        bits
    }
}

impl Default for CacheEventInterests {
    fn default() -> Self {
        Self::all()
    }
}

/// A server-side filter or converter factory: a name already deployed on
/// the server (a Java class registered there), plus whatever raw
/// parameters it expects. `hotrod-protocol` only ever sends this
/// through; it never evaluates filter or converter logic itself, since
/// that logic runs server-side. See `docs/adr/0006-client-listeners.md`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerFactory {
    pub name: String,
    pub params: Vec<Vec<u8>>,
}

/// Options for `RemoteCache::listen_with`. `RemoteCache::listen` is
/// `listen_with(ListenOptions::default())`: every event type, no
/// server-side filtering, no replay of the cache's current contents.
#[derive(Debug, Clone, Default)]
pub struct ListenOptions {
    pub interests: CacheEventInterests,
    pub include_current_state: bool,
    pub filter_factory: Option<ServerFactory>,
    pub converter_factory: Option<ServerFactory>,
    /// Only meaningful with `converter_factory` set: ask the server for
    /// the converter's raw output bytes (`CacheEvent::Custom`) instead of
    /// a Java-serialized object. `hotrod-protocol` never unmarshals Java
    /// objects, with or without this flag, so leaving it `false` with a
    /// converter that returns one yields bytes this client cannot make
    /// sense of; `true` is almost always the right choice once a
    /// converter factory is in use at all.
    pub raw_data: bool,
}

/// One event pushed by the server to a `CacheListener`. `is_retried` is
/// `true` when the server is replaying an event it cannot confirm the
/// client already saw (the server failed over mid-notification).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheEvent {
    Created {
        key: Vec<u8>,
        version: u64,
        is_retried: bool,
    },
    Modified {
        key: Vec<u8>,
        version: u64,
        is_retried: bool,
    },
    Removed {
        key: Vec<u8>,
        is_retried: bool,
    },
    Expired {
        key: Vec<u8>,
        is_retried: bool,
    },
    /// What a `converter_factory` produced, exactly as sent: see
    /// `ListenOptions::raw_data`. Carries no key of its own; the
    /// converter decides what goes in `data`.
    Custom {
        data: Vec<u8>,
        is_retried: bool,
    },
}

/// A registered listener: a dedicated connection that does nothing but
/// receive event frames from here on (see the module docs for why).
/// Dropping this without calling `close` just closes the socket; the
/// server notices the connection is gone and cleans up the registration
/// on its own, the same as it would for any other dead Hot Rod
/// connection.
pub struct CacheListener {
    stream: BufStream<Transport>,
    listener_id: Vec<u8>,
    cache_name: Vec<u8>,
    timeout: Duration,
    /// Set once a frame has started being read (the magic byte arrived),
    /// cleared only once that whole frame was parsed successfully.
    /// Mirrors `HotRodConnection`'s own poisoning rule (see its module
    /// docs) for the same reason: `next`'s doc explicitly allows pairing
    /// it with `tokio::select!` or an external timeout, so a future
    /// dropped mid-frame is exactly as possible here as it is for any
    /// `HotRodConnection` operation, and leaves the stream in the same
    /// kind of desynced state if the next call just started reading a
    /// fresh frame from the middle of the abandoned one.
    poisoned: bool,
}

fn write_factory(body: &mut Vec<u8>, factory: Option<&ServerFactory>) -> Result<()> {
    match factory {
        None => write_array(body, b""),
        Some(factory) => {
            if factory.params.len() > MAX_FACTORY_PARAMS {
                return Err(Error::BatchTooLarge {
                    what: "a listener's filter/converter factory parameters",
                    len: factory.params.len(),
                    max: MAX_FACTORY_PARAMS,
                });
            }
            write_array(body, factory.name.as_bytes());
            // A named factory's parameter count is a single byte on the
            // wire (Codec30.writeNamedFactory), not a vInt like every
            // other length-prefixed count elsewhere in this protocol.
            body.push(factory.params.len() as u8);
            for param in &factory.params {
                write_array(body, param);
            }
        }
    }
    Ok(())
}

impl CacheListener {
    /// Builds the `AddClientListener` request body and registers it on
    /// `conn`, which must not be reused afterward: see `into_transport`.
    pub(crate) async fn register(
        mut conn: HotRodConnection,
        cache_name: &[u8],
        timeout: Duration,
        options: &ListenOptions,
    ) -> Result<Self> {
        let mut listener_id = vec![0u8; 16];
        rand::thread_rng().fill_bytes(&mut listener_id);

        let mut body = Vec::new();
        write_array(&mut body, &listener_id);
        body.push(options.include_current_state as u8);
        write_factory(&mut body, options.filter_factory.as_ref())?;
        write_factory(&mut body, options.converter_factory.as_ref())?;
        body.push(options.raw_data as u8);
        write_vint(&mut body, options.interests.as_vint());

        conn.add_client_listener(&body).await?;

        Ok(Self {
            stream: conn.into_transport(),
            listener_id,
            cache_name: cache_name.to_vec(),
            timeout,
            poisoned: false,
        })
    }

    /// Waits for the next event. Returns `None` once the server closes
    /// the connection cleanly *between* frames; a close or any other
    /// failure partway through one is a terminal `Err`, same as a
    /// `PoisonedConnection` error on every following call (no
    /// reconnection here: see the module docs; call
    /// `RemoteCache::listen` again instead). Not bounded by this
    /// listener's timeout: waiting for the next event is the normal
    /// state of a listener, not a hung request the way every other wait
    /// in this crate is.
    pub async fn next(&mut self) -> Option<Result<CacheEvent>> {
        if self.poisoned {
            return Some(Err(Error::PoisonedConnection));
        }
        // A clean close is only a `None` when it happens before any byte
        // of a new frame has arrived; once the magic byte is in, this
        // client is committed to that frame, and `poisoned` stays set
        // until it is parsed in full, so neither a later clean close nor
        // this call being dropped mid-frame is ever mistaken for one.
        let magic = match self.stream.read_u8().await {
            Ok(byte) => byte,
            Err(io_err) if io_err.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(io_err) => return Some(Err(Error::Io(io_err))),
        };
        self.poisoned = true;
        let result = read_event_after_magic(&mut self.stream, magic, &self.listener_id).await;
        self.poisoned = result.is_err();
        Some(result)
    }

    /// Unregisters the listener and waits for the server's confirmation,
    /// instead of just dropping the connection: the server notices a
    /// dropped connection on its own, but not immediately, so this is the
    /// quicker, explicit alternative for a caller that wants the server
    /// to stop tracking the listener right away.
    pub async fn close(mut self) -> Result<()> {
        let mut body = Vec::new();
        write_array(&mut body, &self.listener_id);

        let timeout = self.timeout;
        with_timeout(timeout, async {
            write_and_read_header(
                &mut self.stream,
                0,
                &self.cache_name,
                OpCode::RemoveClientListener,
                ClientIntelligence::Basic,
                DEFAULT_TOPOLOGY_ID,
                &body,
            )
            .await?;
            Ok(())
        })
        .await
    }
}

/// Reads the rest of one event frame given its already-read magic byte:
/// message id, opcode, status, topology marker (always `0` for an
/// event), then the event-specific body. Not built on
/// `header::read_response_header`: that function validates the opcode
/// against one specific request it expects a reply to, which does not
/// fit a frame the server pushed on its own, and reads the magic byte
/// itself, which `next` already needed to read on its own to tell a
/// clean close between frames apart from a dropped connection partway
/// through one.
async fn read_event_after_magic<R: AsyncRead + Unpin>(
    stream: &mut R,
    magic: u8,
    expected_listener_id: &[u8],
) -> Result<CacheEvent> {
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

    let listener_id = read_array(stream).await?;
    if listener_id != expected_listener_id {
        return Err(Error::MalformedEvent(
            "event listener id does not match this connection's own, on a connection dedicated \
             to a single listener"
                .to_string(),
        ));
    }
    let is_custom = stream.read_u8().await?;
    let is_retried = stream.read_u8().await? == 1;

    if is_custom != 0 {
        let data = read_array(stream).await?;
        return Ok(CacheEvent::Custom { data, is_retried });
    }

    match opcode {
        EVENT_CREATED | EVENT_MODIFIED => {
            let key = read_array(stream).await?;
            let version = stream.read_u64().await?;
            Ok(if opcode == EVENT_CREATED {
                CacheEvent::Created {
                    key,
                    version,
                    is_retried,
                }
            } else {
                CacheEvent::Modified {
                    key,
                    version,
                    is_retried,
                }
            })
        }
        EVENT_REMOVED | EVENT_EXPIRED => {
            let key = read_array(stream).await?;
            Ok(if opcode == EVENT_REMOVED {
                CacheEvent::Removed { key, is_retried }
            } else {
                CacheEvent::Expired { key, is_retried }
            })
        }
        other => Err(Error::MalformedEvent(format!(
            "unrecognized event opcode {other:#04x}"
        ))),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    use crate::client::tests::{read_request_opcode, response_header};
    use crate::connection::HotRodConnection;
    use crate::varint::write_vlong;
    use crate::wire::write_array;

    pub(crate) fn event_frame(
        message_id: u64,
        opcode: u8,
        listener_id: &[u8],
        is_custom: u8,
        is_retried: bool,
        key: &[u8],
        version: Option<u64>,
    ) -> Vec<u8> {
        let mut buf = vec![0xA1];
        write_vlong(&mut buf, message_id);
        buf.push(opcode);
        buf.push(0x00); // status: success
        buf.push(0); // topology marker: always 0 for events
        write_array(&mut buf, listener_id);
        buf.push(is_custom);
        buf.push(is_retried as u8);
        write_array(&mut buf, key);
        if let Some(version) = version {
            buf.extend_from_slice(&version.to_be_bytes());
        }
        buf
    }

    /// Reads one `AddClientListener` request far enough to pull out its
    /// message id (needed to answer with a matching response) and the
    /// listener id it carries (the one piece a test needs to echo back in
    /// synthetic event frames), trusting the rest of the body's shape is
    /// already covered by `register_sends_well_formed_add_client_listener_request`.
    pub(crate) async fn read_listener_id(stream: &mut TcpStream) -> (u64, Vec<u8>) {
        let (id, opcode) = read_request_opcode(stream).await;
        assert_eq!(opcode, 0x25, "expected an AddClientListener request");
        let listener_id = read_array(stream).await.unwrap();
        let _include_current_state = stream.read_u8().await.unwrap();
        read_factory(stream).await;
        read_factory(stream).await;
        let _raw_data = stream.read_u8().await.unwrap();
        let _interests = crate::varint::read_vint(stream).await.unwrap();
        (id, listener_id)
    }

    /// Drains one filter/converter factory field: a name, and only if
    /// non-empty, a single-byte parameter count followed by that many
    /// arrays. Leaving any of this unread would desync the stream for
    /// whatever a test reads next, the same way a real server's own
    /// parser must consume the whole `AddClientListener` body before
    /// responding.
    async fn read_factory(stream: &mut TcpStream) {
        let name = read_string(stream).await.unwrap();
        if !name.is_empty() {
            let param_count = stream.read_u8().await.unwrap();
            for _ in 0..param_count {
                let _ = read_array(stream).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn register_sends_well_formed_add_client_listener_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x25, "expected an AddClientListener request");

            let listener_id = read_array(&mut stream).await.unwrap();
            assert_eq!(listener_id.len(), 16, "listener id should be 16 bytes");
            let include_current_state = stream.read_u8().await.unwrap();
            assert_eq!(include_current_state, 0);
            let filter_factory = read_string(&mut stream).await.unwrap();
            assert_eq!(filter_factory, "");
            let converter_factory = read_string(&mut stream).await.unwrap();
            assert_eq!(converter_factory, "");
            let raw_data = stream.read_u8().await.unwrap();
            assert_eq!(raw_data, 0);
            let interests = crate::varint::read_vint(&mut stream).await.unwrap();
            assert_eq!(
                interests, 0x0F,
                "default interests should be all four types"
            );

            let resp = response_header(id, 0x26, 0x00);
            stream.write_all(&resp).await.unwrap();
            stream
        });

        let conn = HotRodConnection::connect(addr, "my-cache").await.unwrap();
        let listener = CacheListener::register(
            conn,
            b"my-cache",
            Duration::from_secs(5),
            &ListenOptions::default(),
        )
        .await
        .expect("register");
        assert_eq!(listener.listener_id.len(), 16);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn register_sends_filter_and_converter_factories_with_their_params() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, _opcode) = read_request_opcode(&mut stream).await;
            let _listener_id = read_array(&mut stream).await.unwrap();
            let _include_current_state = stream.read_u8().await.unwrap();

            let filter_factory = read_string(&mut stream).await.unwrap();
            assert_eq!(filter_factory, "my-filter");
            let filter_param_count = stream.read_u8().await.unwrap();
            assert_eq!(filter_param_count, 1);
            let filter_param = read_array(&mut stream).await.unwrap();
            assert_eq!(filter_param, b"param-a");

            let converter_factory = read_string(&mut stream).await.unwrap();
            assert_eq!(converter_factory, "my-converter");
            let converter_param_count = stream.read_u8().await.unwrap();
            assert_eq!(converter_param_count, 0);

            let raw_data = stream.read_u8().await.unwrap();
            assert_eq!(raw_data, 1);
            let interests = crate::varint::read_vint(&mut stream).await.unwrap();
            assert_eq!(interests, 0x04, "only removed events requested");

            let resp = response_header(id, 0x26, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "my-cache").await.unwrap();
        let options = ListenOptions {
            interests: CacheEventInterests {
                created: false,
                modified: false,
                removed: true,
                expired: false,
            },
            filter_factory: Some(ServerFactory {
                name: "my-filter".to_string(),
                params: vec![b"param-a".to_vec()],
            }),
            converter_factory: Some(ServerFactory {
                name: "my-converter".to_string(),
                params: vec![],
            }),
            raw_data: true,
            ..Default::default()
        };
        CacheListener::register(conn, b"my-cache", Duration::from_secs(5), &options)
            .await
            .expect("register");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn next_parses_created_modified_removed_expired_events_in_order() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, listener_id) = read_listener_id(&mut stream).await;
            let resp = response_header(id, 0x26, 0x00);
            stream.write_all(&resp).await.unwrap();

            let created = event_frame(0, 0x60, &listener_id, 0, false, b"key1", Some(7));
            let modified = event_frame(0, 0x61, &listener_id, 0, false, b"key1", Some(8));
            let removed = event_frame(0, 0x62, &listener_id, 0, true, b"key1", None);
            let expired = event_frame(0, 0x63, &listener_id, 0, false, b"key2", None);
            stream.write_all(&created).await.unwrap();
            stream.write_all(&modified).await.unwrap();
            stream.write_all(&removed).await.unwrap();
            stream.write_all(&expired).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "my-cache").await.unwrap();
        let mut listener = CacheListener::register(
            conn,
            b"my-cache",
            Duration::from_secs(5),
            &ListenOptions::default(),
        )
        .await
        .expect("register");

        assert_eq!(
            listener.next().await.unwrap().unwrap(),
            CacheEvent::Created {
                key: b"key1".to_vec(),
                version: 7,
                is_retried: false,
            }
        );
        assert_eq!(
            listener.next().await.unwrap().unwrap(),
            CacheEvent::Modified {
                key: b"key1".to_vec(),
                version: 8,
                is_retried: false,
            }
        );
        assert_eq!(
            listener.next().await.unwrap().unwrap(),
            CacheEvent::Removed {
                key: b"key1".to_vec(),
                is_retried: true,
            }
        );
        assert_eq!(
            listener.next().await.unwrap().unwrap(),
            CacheEvent::Expired {
                key: b"key2".to_vec(),
                is_retried: false,
            }
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn next_parses_a_custom_event_from_a_converter() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, listener_id) = read_listener_id(&mut stream).await;
            let resp = response_header(id, 0x26, 0x00);
            stream.write_all(&resp).await.unwrap();

            // isCustom=2 (raw), key field absent, "custom data" is what
            // read_array reads next instead of a key.
            let custom = event_frame(0, 0x60, &listener_id, 2, false, b"custom data", None);
            stream.write_all(&custom).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "my-cache").await.unwrap();
        let mut listener = CacheListener::register(
            conn,
            b"my-cache",
            Duration::from_secs(5),
            &ListenOptions::default(),
        )
        .await
        .expect("register");

        assert_eq!(
            listener.next().await.unwrap().unwrap(),
            CacheEvent::Custom {
                data: b"custom data".to_vec(),
                is_retried: false,
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
            let (id, _listener_id) = read_listener_id(&mut stream).await;
            let resp = response_header(id, 0x26, 0x00);
            stream.write_all(&resp).await.unwrap();
            // Dropping `stream` here closes the connection cleanly.
        });

        let conn = HotRodConnection::connect(addr, "my-cache").await.unwrap();
        let mut listener = CacheListener::register(
            conn,
            b"my-cache",
            Duration::from_secs(5),
            &ListenOptions::default(),
        )
        .await
        .expect("register");

        assert!(listener.next().await.is_none());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn next_surfaces_a_server_error_as_err() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, _listener_id) = read_listener_id(&mut stream).await;
            let resp = response_header(id, 0x26, 0x00);
            stream.write_all(&resp).await.unwrap();

            let mut err_frame = vec![0xA1];
            write_vlong(&mut err_frame, 0);
            err_frame.push(0x50); // generic ERROR_RESPONSE opcode
            err_frame.push(0x85); // SERVER_ERROR status
            err_frame.push(0); // topology marker
            write_array(&mut err_frame, b"boom");
            stream.write_all(&err_frame).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "my-cache").await.unwrap();
        let mut listener = CacheListener::register(
            conn,
            b"my-cache",
            Duration::from_secs(5),
            &ListenOptions::default(),
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

    /// A status byte that is neither a known success/not-executed/not-exist
    /// code nor a known error code must be rejected, the same way
    /// `header::read_response_header` already rejects it for an ordinary
    /// response (`0x99` matches `status::tests::unrecognized_byte_is_unknown`).
    #[tokio::test]
    async fn next_rejects_an_unrecognized_status_byte() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, listener_id) = read_listener_id(&mut stream).await;
            let resp = response_header(id, 0x26, 0x00);
            stream.write_all(&resp).await.unwrap();

            let mut frame = vec![0xA1];
            write_vlong(&mut frame, 0);
            frame.push(EVENT_CREATED);
            frame.push(0x99); // unrecognized status
            frame.push(0); // topology marker
            write_array(&mut frame, &listener_id);
            stream.write_all(&frame).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "my-cache").await.unwrap();
        let mut listener = CacheListener::register(
            conn,
            b"my-cache",
            Duration::from_secs(5),
            &ListenOptions::default(),
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

    /// A connection dropped partway through a frame (as opposed to
    /// cleanly between two frames) must surface as a terminal `Err`, and
    /// poison the listener the same way a `HotRodConnection` operation
    /// poisons itself on a partial read: the review that caught this drew
    /// the comparison directly to `connection.rs`'s own module docs on
    /// why a partial read can never be treated as if nothing happened.
    #[tokio::test]
    async fn next_errors_and_poisons_on_a_connection_dropped_mid_frame() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, _listener_id) = read_listener_id(&mut stream).await;
            let resp = response_header(id, 0x26, 0x00);
            stream.write_all(&resp).await.unwrap();

            // Only the first few bytes of a frame (magic, message id,
            // opcode, status, topology marker): a real frame has more
            // after this. Dropping `stream` here closes the connection
            // with a frame already under way.
            let mut partial = vec![0xA1];
            write_vlong(&mut partial, 0);
            partial.push(EVENT_CREATED);
            partial.push(0x00);
            partial.push(0);
            stream.write_all(&partial).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "my-cache").await.unwrap();
        let mut listener = CacheListener::register(
            conn,
            b"my-cache",
            Duration::from_secs(5),
            &ListenOptions::default(),
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
    async fn register_rejects_more_than_255_factory_parameters() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        // The fake server never needs to do anything: `register` must
        // reject the oversized parameter list locally, before writing
        // the `AddClientListener` request at all.
        let server = tokio::spawn(async move {
            let _ = tcp_listener.accept().await;
        });

        let conn = HotRodConnection::connect(addr, "my-cache").await.unwrap();
        let options = ListenOptions {
            filter_factory: Some(ServerFactory {
                name: "my-filter".to_string(),
                params: vec![Vec::new(); 256],
            }),
            ..Default::default()
        };

        let result =
            CacheListener::register(conn, b"my-cache", Duration::from_secs(5), &options).await;
        match result {
            Err(
                err @ Error::BatchTooLarge {
                    len: 256, max: 255, ..
                },
            ) => drop(err),
            Err(err) => panic!("unexpected error variant: {err:?}"),
            Ok(_) => panic!("256 parameters should be rejected"),
        }

        server.await.unwrap();
    }

    #[tokio::test]
    async fn close_sends_remove_client_listener_and_waits_for_the_response() {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = tcp_listener.accept().await.unwrap();
            let (id, listener_id) = read_listener_id(&mut stream).await;
            let resp = response_header(id, 0x26, 0x00);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x27, "expected a RemoveClientListener request");
            let removed_listener_id = read_array(&mut stream).await.unwrap();
            assert_eq!(removed_listener_id, listener_id);
            let resp = response_header(id, 0x28, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let conn = HotRodConnection::connect(addr, "my-cache").await.unwrap();
        let listener = CacheListener::register(
            conn,
            b"my-cache",
            Duration::from_secs(5),
            &ListenOptions::default(),
        )
        .await
        .expect("register");

        listener.close().await.expect("close");

        server.await.unwrap();
    }
}
