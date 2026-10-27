//! Distributed counters (`docs/adr/0015-distributed-counters.md`).
//! Every one of these targets `org.infinispan.COUNTER`, confirmed
//! against the Java client's own `CounterOperationFactory` to be a
//! real, ordinary cache name declared in the header like any other,
//! unlike `execute_task`'s empty cache name. By the time one of these
//! runs, `self.cache_name` is already that name (set when
//! `CounterManager` obtained this connection's `RemoteCache` via
//! `client.cache(COUNTER_CACHE_NAME)`), so none of these methods need
//! to override it.
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): see `connection/multimap.rs`'s module
//! docs for the mechanism that lets this `impl HotRodConnection`
//! block reach every private field/method it needs with no
//! visibility change.

use crate::error::{Error, Result};
use crate::header::OpCode;
use crate::varint::read_vint;
use crate::wire::{read_string, write_array};

use super::{with_timeout, HotRodConnection};

impl HotRodConnection {
    /// Defines a counter with `config` if it is not already defined.
    /// `COUNTER_CREATE` carries no response body either way: the
    /// returned `true`/`false` (created now vs. already defined,
    /// idempotent) comes entirely from the status byte, confirmed
    /// against `DefineCounterOperation.createResponse`.
    pub(crate) async fn counter_define(
        &mut self,
        name: &str,
        config: &crate::counter::CounterConfiguration,
    ) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, name.as_bytes());
            crate::counter::encode_configuration(&mut body, config);
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::CounterCreate, &body)
                .await?;
            Ok(!header.status.has_previous())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    pub(crate) async fn counter_get_configuration(
        &mut self,
        name: &str,
    ) -> Result<Option<crate::counter::CounterConfiguration>> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, name.as_bytes());
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::CounterGetConfiguration, &body)
                .await?;
            if header.status.is_not_exist() {
                return Ok(None);
            }
            crate::counter::decode_configuration(&mut self.stream)
                .await
                .map(Some)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Lists every counter name known to the cluster. The only
    /// counter operation that sends no name at all: it is a
    /// cluster-wide listing, not about one specific counter.
    pub(crate) async fn counter_names(&mut self) -> Result<Vec<String>> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::CounterGetNames, &[])
                .await?;
            let count = read_vint(&mut self.stream).await?;
            // Not sized from `count` directly: see
            // `read_array_collection`'s own comment, the same
            // unchecked-server-count hazard applies here.
            let mut names = Vec::new();
            for _ in 0..count {
                names.push(read_string(&mut self.stream).await?);
            }
            Ok(names)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// `true` if `name` is defined, confirmed against the Java
    /// client's own `IsDefinedOperation`: the boolean comes purely
    /// from the status byte (`NO_ERROR_STATUS` vs. a server-confirmed
    /// `OperationNotExecuted`, not `KeyDoesNotExist`, unlike every
    /// other counter operation below), with no response body either
    /// way.
    pub(crate) async fn counter_is_defined(&mut self, name: &str) -> Result<bool> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, name.as_bytes());
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::CounterIsDefined, &body)
                .await?;
            Ok(header.status.is_success())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    pub(crate) async fn counter_get(&mut self, name: &str) -> Result<i64> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, name.as_bytes());
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::CounterGet, &body)
                .await?;
            if header.status.is_not_exist() {
                return Err(Error::CounterNotFound(name.to_string()));
            }
            tokio::io::AsyncReadExt::read_i64(&mut self.stream)
                .await
                .map_err(Error::Io)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Adds `delta` (negative to subtract) and returns the counter's
    /// new value. Used by both `StrongCounter::add_and_get` and
    /// `WeakCounter::add` (which just discards the returned value),
    /// confirmed against the Java client's own `WeakCounterImpl.add`
    /// to call this exact same operation, not a different one.
    ///
    /// On a bounded strong counter, an update that would move the
    /// counter past its configured bound comes back as status
    /// `NOT_EXECUTED_WITH_PREVIOUS` with an empty body (confirmed
    /// against the server's own `CounterRequestProcessor`, not
    /// guessed: the body is genuinely empty, not a value this method
    /// failed to read), surfaced as `Error::CounterOutOfBounds`
    /// instead of a value.
    pub(crate) async fn counter_add_and_get(&mut self, name: &str, delta: i64) -> Result<i64> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, name.as_bytes());
            body.extend_from_slice(&delta.to_be_bytes());
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::CounterAddAndGet, &body)
                .await?;
            if header.status.is_not_exist() {
                return Err(Error::CounterNotFound(name.to_string()));
            }
            if header.status.is_not_executed_with_previous() {
                return Err(Error::CounterOutOfBounds);
            }
            tokio::io::AsyncReadExt::read_i64(&mut self.stream)
                .await
                .map_err(Error::Io)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Swaps the counter's value to `update` only if it is currently
    /// `expect`, returning the value it had **before** the call
    /// (confirmed against `CompareAndSwapOperation.createResponse`):
    /// the caller determines success as `previous == expect`, the
    /// same comparison the Java client makes itself.
    pub(crate) async fn counter_compare_and_swap(
        &mut self,
        name: &str,
        expect: i64,
        update: i64,
    ) -> Result<i64> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, name.as_bytes());
            body.extend_from_slice(&expect.to_be_bytes());
            body.extend_from_slice(&update.to_be_bytes());
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::CounterCompareAndSwap, &body)
                .await?;
            if header.status.is_not_exist() {
                return Err(Error::CounterNotFound(name.to_string()));
            }
            if header.status.is_not_executed_with_previous() {
                return Err(Error::CounterOutOfBounds);
            }
            tokio::io::AsyncReadExt::read_i64(&mut self.stream)
                .await
                .map_err(Error::Io)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Sets the counter's value to `value`, returning its previous
    /// value.
    pub(crate) async fn counter_get_and_set(&mut self, name: &str, value: i64) -> Result<i64> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, name.as_bytes());
            body.extend_from_slice(&value.to_be_bytes());
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::CounterGetAndSet, &body)
                .await?;
            if header.status.is_not_exist() {
                return Err(Error::CounterNotFound(name.to_string()));
            }
            if header.status.is_not_executed_with_previous() {
                return Err(Error::CounterOutOfBounds);
            }
            tokio::io::AsyncReadExt::read_i64(&mut self.stream)
                .await
                .map_err(Error::Io)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    pub(crate) async fn counter_reset(&mut self, name: &str) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, name.as_bytes());
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::CounterReset, &body)
                .await?;
            if header.status.is_not_exist() {
                return Err(Error::CounterNotFound(name.to_string()));
            }
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    pub(crate) async fn counter_remove(&mut self, name: &str) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, name.as_bytes());
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::CounterRemove, &body)
                .await?;
            if header.status.is_not_exist() {
                return Err(Error::CounterNotFound(name.to_string()));
            }
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use crate::connection::tests::{read_request, response_header};
    use crate::varint::write_vint;
    use crate::wire::read_array;

    /// `counter_define`'s own small helper: expects a `CounterCreate`
    /// request against `COUNTER_CACHE_NAME`, reads the name and
    /// configuration, then replies with `status`, returning whatever
    /// `counter_define` resolves to.
    async fn serve_counter_create(status: u8) -> (bool, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x4B, "expected a CounterCreate request");
            let name = read_array(&mut stream).await.unwrap();
            assert_eq!(name, b"my-counter");
            let flags = stream.read_u8().await.unwrap();
            assert_eq!(flags, 0x00, "unbounded strong, volatile");
            let initial = tokio::io::AsyncReadExt::read_i64(&mut stream)
                .await
                .unwrap();
            assert_eq!(initial, 7);
            let resp = response_header(id, 0x4C, status);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let config = crate::counter::CounterConfiguration {
            counter_type: crate::counter::CounterType::UnboundedStrong,
            initial_value: 7,
            storage: crate::counter::Storage::Volatile,
        };
        let created = conn
            .counter_define("my-counter", &config)
            .await
            .expect("counter_define");
        (created, server)
    }

    #[tokio::test]
    async fn counter_define_returns_true_when_created_now() {
        let (created, server) = serve_counter_create(0x00).await;
        assert!(created);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_define_returns_false_when_already_defined() {
        let (created, server) = serve_counter_create(0x03).await;
        assert!(!created);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_is_defined_is_true_on_plain_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x4F, "expected a CounterIsDefined request");
            let _name = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x51, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        assert!(conn
            .counter_is_defined("my-counter")
            .await
            .expect("counter_is_defined"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_is_defined_is_false_when_not_defined() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x4F, "expected a CounterIsDefined request");
            let _name = read_array(&mut stream).await.unwrap();
            // OperationNotExecuted (0x01), confirmed server-side: a
            // missing counter is not KeyDoesNotExist (0x02) for this
            // one operation, unlike every other counter operation.
            let resp = response_header(id, 0x51, 0x01);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        assert!(!conn
            .counter_is_defined("my-counter")
            .await
            .expect("counter_is_defined"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_get_configuration_returns_none_when_not_defined() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x4D, "expected a CounterGetConfiguration request");
            let _name = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x4E, 0x02); // KeyDoesNotExist
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let config = conn
            .counter_get_configuration("my-counter")
            .await
            .expect("counter_get_configuration");
        assert_eq!(config, None);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_get_configuration_returns_the_configuration() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x4D, "expected a CounterGetConfiguration request");
            let _name = read_array(&mut stream).await.unwrap();
            let mut resp = response_header(id, 0x4E, 0x00);
            resp.push(0x01); // weak, volatile
            write_vint(&mut resp, 16); // concurrency level
            resp.extend_from_slice(&42i64.to_be_bytes()); // initial value
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let config = conn
            .counter_get_configuration("my-counter")
            .await
            .expect("counter_get_configuration")
            .expect("configuration should be present");
        assert_eq!(
            config,
            crate::counter::CounterConfiguration {
                counter_type: crate::counter::CounterType::Weak {
                    concurrency_level: 16
                },
                initial_value: 42,
                storage: crate::counter::Storage::Volatile,
            }
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_names_returns_the_listed_names() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x64, "expected a CounterGetNames request");
            let mut resp = response_header(id, 0x65, 0x00);
            write_vint(&mut resp, 2);
            write_array(&mut resp, b"one");
            write_array(&mut resp, b"two");
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let names = conn.counter_names().await.expect("counter_names");
        assert_eq!(names, vec!["one".to_string(), "two".to_string()]);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_get_returns_the_value() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x56, "expected a CounterGet request");
            let _name = read_array(&mut stream).await.unwrap();
            let mut resp = response_header(id, 0x57, 0x00);
            resp.extend_from_slice(&99i64.to_be_bytes());
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let value = conn.counter_get("my-counter").await.expect("counter_get");
        assert_eq!(value, 99);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_get_fails_with_counter_not_found_when_missing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x56, "expected a CounterGet request");
            let _name = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x57, 0x02); // KeyDoesNotExist
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let err = conn
            .counter_get("my-counter")
            .await
            .expect_err("expected CounterNotFound");
        assert!(matches!(err, Error::CounterNotFound(name) if name == "my-counter"));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_add_and_get_returns_the_new_value() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x52, "expected a CounterAddAndGet request");
            let _name = read_array(&mut stream).await.unwrap();
            let delta = tokio::io::AsyncReadExt::read_i64(&mut stream)
                .await
                .unwrap();
            assert_eq!(delta, 5);
            let mut resp = response_header(id, 0x53, 0x00);
            resp.extend_from_slice(&15i64.to_be_bytes());
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let value = conn
            .counter_add_and_get("my-counter", 5)
            .await
            .expect("counter_add_and_get");
        assert_eq!(value, 15);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_add_and_get_fails_out_of_bounds_with_an_empty_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x52, "expected a CounterAddAndGet request");
            let _name = read_array(&mut stream).await.unwrap();
            let _delta = tokio::io::AsyncReadExt::read_i64(&mut stream)
                .await
                .unwrap();
            // NotExecutedWithPrevious, no body: confirmed against the
            // server's own emptyResponse call for this status.
            let resp = response_header(id, 0x53, 0x04);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let err = conn
            .counter_add_and_get("my-counter", 1000)
            .await
            .expect_err("expected CounterOutOfBounds");
        assert!(matches!(err, Error::CounterOutOfBounds));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_compare_and_swap_returns_the_previous_value() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x58, "expected a CounterCompareAndSwap request");
            let _name = read_array(&mut stream).await.unwrap();
            let expect = tokio::io::AsyncReadExt::read_i64(&mut stream)
                .await
                .unwrap();
            let update = tokio::io::AsyncReadExt::read_i64(&mut stream)
                .await
                .unwrap();
            assert_eq!(expect, 10);
            assert_eq!(update, 20);
            let mut resp = response_header(id, 0x59, 0x00);
            resp.extend_from_slice(&10i64.to_be_bytes()); // previous value
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let previous = conn
            .counter_compare_and_swap("my-counter", 10, 20)
            .await
            .expect("counter_compare_and_swap");
        assert_eq!(previous, 10);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_get_and_set_returns_the_previous_value() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x7F, "expected a CounterGetAndSet request");
            let _name = read_array(&mut stream).await.unwrap();
            let value = tokio::io::AsyncReadExt::read_i64(&mut stream)
                .await
                .unwrap();
            assert_eq!(value, 50);
            let mut resp = response_header(id, 0x80, 0x00);
            resp.extend_from_slice(&30i64.to_be_bytes()); // previous value
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let previous = conn
            .counter_get_and_set("my-counter", 50)
            .await
            .expect("counter_get_and_set");
        assert_eq!(previous, 30);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_reset_succeeds_on_a_bare_success_status() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x54, "expected a CounterReset request");
            let _name = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x55, 0x00);
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        conn.counter_reset("my-counter")
            .await
            .expect("counter_reset");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn counter_remove_fails_with_counter_not_found_when_missing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x5E, "expected a CounterRemove request");
            let _name = read_array(&mut stream).await.unwrap();
            let resp = response_header(id, 0x5F, 0x02); // KeyDoesNotExist
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, crate::counter::COUNTER_CACHE_NAME)
            .await
            .expect("connect");
        let err = conn
            .counter_remove("my-counter")
            .await
            .expect_err("expected CounterNotFound");
        assert!(matches!(err, Error::CounterNotFound(name) if name == "my-counter"));

        server.await.unwrap();
    }
}
