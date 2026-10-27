//! Remote query (Ickle), `docs/adr/0013-remote-query.md`: the
//! `RemoteCache` half of `Query`'s public API.
//!
//! Split out of `remote_cache.rs`
//! (`docs/adr/0026-split-connection-and-remote-cache-by-domain.md`):
//! see `remote_cache/multimap.rs`'s module docs for the mechanism
//! that lets this `impl RemoteCache` block reach `call_seed` with no
//! visibility change.

use crate::error::Result;
use crate::query::{Query, QueryResult};

use super::{Operation, OperationResult, RemoteCache};

impl RemoteCache {
    /// Runs `query` (Ickle) against this cache, see
    /// `docs/adr/0013-remote-query.md`. No key to route by, same as
    /// `get_all`/`put_all`/`size`/`clear`: always goes to the seed, which
    /// is free to run the query cluster-wide or forward it as needed.
    pub fn query(&self, query: impl Into<String>) -> Query<'_> {
        Query::new(self, query.into())
    }

    pub(crate) async fn run_query(&self, request_bytes: Vec<u8>) -> Result<QueryResult> {
        match self.call_seed(Operation::Query(request_bytes)).await? {
            OperationResult::Query(result) => Ok(result),
            _ => unreachable!("Operation::Query always yields OperationResult::Query"),
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    use crate::client::tests::{read_request_opcode, response_header};
    use crate::protobuf_wire::{read_length_delimited, read_tag, WIRE_TYPE_VARINT};
    use crate::query::test_support::{
        query_response_bytes, wrapped_entity_bytes, wrapped_scalar_bytes,
    };
    use crate::query::{QueryRow, QueryValue};
    use crate::remote_cache::tests::client_with_seeds;
    use crate::wire::{read_array, write_array};

    /// `docs/adr/0013-remote-query.md`: no key to route by, so `query`
    /// always targets the seed, same as `size`/`clear`/`ping`/`stats`.
    /// Without a projection, each result is a whole entity.
    #[tokio::test]
    async fn query_targets_the_seed_and_returns_whole_entities() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let request_body = read_array(&mut stream).await.unwrap();
            assert!(String::from_utf8_lossy(&request_body).contains("FROM org.example.User"));

            let response = query_response_bytes(
                0,
                &[wrapped_entity_bytes("org.example.User", b"entity-bytes")],
                1,
            );
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let result = client
            .cache("my-cache")
            .query("FROM org.example.User")
            .execute()
            .await
            .expect("query should succeed");

        assert_eq!(
            result.rows,
            vec![QueryRow::Entity(b"entity-bytes".to_vec())]
        );
        assert_eq!(result.hit_count, 1);

        seed_task.await.unwrap();
    }

    /// With a projection (`SELECT a, b`), results group into one row per
    /// `projection_size` consecutive `WrappedMessage`s.
    #[tokio::test]
    async fn query_with_projection_returns_columns() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let request_body = read_array(&mut stream).await.unwrap();
            assert!(String::from_utf8_lossy(&request_body).contains("SELECT name, age"));
            // Confirm the named parameter made it into the request too,
            // rather than only checking the response path.
            let mut pos = 0;
            let mut saw_named_parameters = false;
            while let Some((field, wire_type)) = read_tag(&request_body, &mut pos).unwrap() {
                if field == 5 {
                    saw_named_parameters = true;
                    read_length_delimited(&request_body, &mut pos).unwrap();
                } else if wire_type == WIRE_TYPE_VARINT {
                    crate::protobuf_wire::read_varint(&request_body, &mut pos).unwrap();
                } else {
                    read_length_delimited(&request_body, &mut pos).unwrap();
                }
            }
            assert!(
                saw_named_parameters,
                "expected namedParameters in the request"
            );

            let response = query_response_bytes(
                2,
                &[
                    wrapped_scalar_bytes(&QueryValue::String("Alice".to_string())),
                    wrapped_scalar_bytes(&QueryValue::Int32(30)),
                    wrapped_scalar_bytes(&QueryValue::String("Bob".to_string())),
                    wrapped_scalar_bytes(&QueryValue::Int32(40)),
                ],
                2,
            );
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let result = client
            .cache("my-cache")
            .query("SELECT name, age FROM org.example.User WHERE age >= :minAge")
            .param("minAge", QueryValue::Int32(18))
            .execute()
            .await
            .expect("query should succeed");

        assert_eq!(
            result.rows,
            vec![
                QueryRow::Columns(vec![
                    QueryValue::String("Alice".to_string()),
                    QueryValue::Int32(30)
                ]),
                QueryRow::Columns(vec![
                    QueryValue::String("Bob".to_string()),
                    QueryValue::Int32(40)
                ]),
            ]
        );
        assert_eq!(result.hit_count, 2);

        seed_task.await.unwrap();
    }

    /// `Query::stream` (issue #109) repeats the request with an
    /// advancing `start_offset`, `page_size` rows at a time, stopping
    /// at the first page shorter than requested.
    /// `docs/adr/0024-remote-query-streaming.md`.
    #[tokio::test]
    async fn query_stream_yields_rows_across_multiple_pages_then_exhausts() {
        use crate::protobuf_wire::{read_int32, read_int64, skip_field};

        fn parse_offset_and_max_results(body: &[u8]) -> (i64, Option<i32>) {
            let mut pos = 0;
            let mut start_offset = 0i64;
            let mut max_results = None;
            while let Some((field, wire_type)) = read_tag(body, &mut pos).unwrap() {
                match field {
                    3 => start_offset = read_int64(body, &mut pos).unwrap(),
                    4 => max_results = Some(read_int32(body, &mut pos).unwrap()),
                    _ => skip_field(body, &mut pos, wire_type).unwrap(),
                }
            }
            (start_offset, max_results)
        }

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            // Page 1: offset 0, a full page of 2.
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let body = read_array(&mut stream).await.unwrap();
            assert_eq!(parse_offset_and_max_results(&body), (0, Some(2)));
            let response = query_response_bytes(
                0,
                &[
                    wrapped_entity_bytes("org.example.User", b"one"),
                    wrapped_entity_bytes("org.example.User", b"two"),
                ],
                5,
            );
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();

            // Page 2: offset 2, another full page of 2.
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let body = read_array(&mut stream).await.unwrap();
            assert_eq!(parse_offset_and_max_results(&body), (2, Some(2)));
            let response = query_response_bytes(
                0,
                &[
                    wrapped_entity_bytes("org.example.User", b"three"),
                    wrapped_entity_bytes("org.example.User", b"four"),
                ],
                5,
            );
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();

            // Page 3: offset 4, a short final page of 1: no further
            // request follows.
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let body = read_array(&mut stream).await.unwrap();
            assert_eq!(parse_offset_and_max_results(&body), (4, Some(2)));
            let response =
                query_response_bytes(0, &[wrapped_entity_bytes("org.example.User", b"five")], 5);
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let cache = client.cache("my-cache");
        let mut rows = cache.query("FROM org.example.User").stream(2);

        let mut collected = Vec::new();
        while let Some(row) = rows.next_row().await.expect("next_row should succeed") {
            collected.push(row);
        }

        assert_eq!(
            collected,
            vec![
                QueryRow::Entity(b"one".to_vec()),
                QueryRow::Entity(b"two".to_vec()),
                QueryRow::Entity(b"three".to_vec()),
                QueryRow::Entity(b"four".to_vec()),
                QueryRow::Entity(b"five".to_vec()),
            ]
        );
        assert_eq!(rows.hit_count(), 5);

        seed_task.await.unwrap();
    }

    /// `page_size(0)` falls back to the built-in default rather than
    /// sending a literal `0` as `maxResults`.
    #[tokio::test]
    async fn query_stream_page_size_zero_uses_the_built_in_default() {
        use crate::protobuf_wire::{read_int32, read_int64, skip_field};

        fn parse_max_results(body: &[u8]) -> Option<i32> {
            let mut pos = 0;
            let mut max_results = None;
            while let Some((field, wire_type)) = read_tag(body, &mut pos).unwrap() {
                match field {
                    4 => max_results = Some(read_int32(body, &mut pos).unwrap()),
                    3 => {
                        read_int64(body, &mut pos).unwrap();
                    }
                    _ => skip_field(body, &mut pos, wire_type).unwrap(),
                }
            }
            max_results
        }

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let body = read_array(&mut stream).await.unwrap();
            assert_eq!(parse_max_results(&body), Some(100));
            let response = query_response_bytes(0, &[], 0);
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let cache = client.cache("my-cache");
        let mut rows = cache.query("FROM org.example.User").stream(0);

        assert_eq!(
            rows.next_row().await.expect("next_row should succeed"),
            None
        );

        seed_task.await.unwrap();
    }

    /// `max_results`, set on the builder before `.stream()`, caps the
    /// stream's total row count across every page rather than meaning
    /// page size: the final page's own request is shrunk to fit the
    /// remaining budget.
    #[tokio::test]
    async fn query_stream_shrinks_the_final_page_to_fit_max_results_budget() {
        use crate::protobuf_wire::{read_int32, read_int64, skip_field};

        fn parse_max_results(body: &[u8]) -> Option<i32> {
            let mut pos = 0;
            let mut max_results = None;
            while let Some((field, wire_type)) = read_tag(body, &mut pos).unwrap() {
                match field {
                    4 => max_results = Some(read_int32(body, &mut pos).unwrap()),
                    3 => {
                        read_int64(body, &mut pos).unwrap();
                    }
                    _ => skip_field(body, &mut pos, wire_type).unwrap(),
                }
            }
            max_results
        }

        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            // First page: the full page_size of 2, well within budget.
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let body = read_array(&mut stream).await.unwrap();
            assert_eq!(parse_max_results(&body), Some(2));
            let response = query_response_bytes(
                0,
                &[
                    wrapped_entity_bytes("org.example.User", b"one"),
                    wrapped_entity_bytes("org.example.User", b"two"),
                ],
                3,
            );
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();

            // Second page: only 1 remains in the budget, so the
            // request's own maxResults is shrunk from page_size (2)
            // down to 1, not sent as 2.
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let body = read_array(&mut stream).await.unwrap();
            assert_eq!(parse_max_results(&body), Some(1));
            let response =
                query_response_bytes(0, &[wrapped_entity_bytes("org.example.User", b"three")], 3);
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let cache = client.cache("my-cache");
        let mut rows = cache
            .query("FROM org.example.User")
            .max_results(3)
            .stream(2);

        let mut collected = Vec::new();
        while let Some(row) = rows.next_row().await.expect("next_row should succeed") {
            collected.push(row);
        }

        assert_eq!(
            collected,
            vec![
                QueryRow::Entity(b"one".to_vec()),
                QueryRow::Entity(b"two".to_vec()),
                QueryRow::Entity(b"three".to_vec()),
            ]
        );

        seed_task.await.unwrap();
    }

    /// An exactly-page-sized final page is indistinguishable from "a
    /// full page with more rows after it" until one more, empty round
    /// trip confirms exhaustion: the same ambiguity any offset/limit
    /// pagination scheme has.
    #[tokio::test]
    async fn query_stream_sends_one_more_request_after_an_exact_full_final_page() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let _body = read_array(&mut stream).await.unwrap();
            let response = query_response_bytes(
                0,
                &[
                    wrapped_entity_bytes("org.example.User", b"one"),
                    wrapped_entity_bytes("org.example.User", b"two"),
                ],
                2,
            );
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();

            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let _body = read_array(&mut stream).await.unwrap();
            let response = query_response_bytes(0, &[], 2);
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let cache = client.cache("my-cache");
        let mut rows = cache.query("FROM org.example.User").stream(2);

        assert_eq!(
            rows.next_row().await.unwrap(),
            Some(QueryRow::Entity(b"one".to_vec()))
        );
        assert_eq!(
            rows.next_row().await.unwrap(),
            Some(QueryRow::Entity(b"two".to_vec()))
        );
        assert_eq!(rows.next_row().await.unwrap(), None);

        seed_task.await.unwrap();
    }

    /// `execute_statement` (issue #108) runs the exact same
    /// `OpCode::Query` round trip as `execute`
    /// (`docs/adr/0013-remote-query.md`), with `hitCountAccuracy`
    /// forced to 1 and the affected-row count read back from
    /// `hit_count` rather than `rows`.
    #[tokio::test]
    async fn execute_statement_sets_hit_count_accuracy_and_returns_the_affected_count() {
        let seed_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = seed_listener.local_addr().unwrap();
        let seed_task = tokio::spawn(async move {
            let (mut stream, _) = seed_listener.accept().await.unwrap();
            let (id, opcode) = read_request_opcode(&mut stream).await;
            assert_eq!(opcode, 0x1F, "expected a Query request");
            let request_body = read_array(&mut stream).await.unwrap();
            assert!(String::from_utf8_lossy(&request_body).contains("DELETE FROM org.example.User"));

            let mut pos = 0;
            let mut hit_count_accuracy = None;
            while let Some((field, wire_type)) = read_tag(&request_body, &mut pos).unwrap() {
                if field == 7 {
                    hit_count_accuracy =
                        Some(crate::protobuf_wire::read_int32(&request_body, &mut pos).unwrap());
                } else if wire_type == WIRE_TYPE_VARINT {
                    crate::protobuf_wire::read_varint(&request_body, &mut pos).unwrap();
                } else {
                    read_length_delimited(&request_body, &mut pos).unwrap();
                }
            }
            assert_eq!(
                hit_count_accuracy,
                Some(1),
                "expected hitCountAccuracy = 1 in the request"
            );

            // The server leaves `results` empty for a statement, not a
            // SELECT; only `hit_count` (the affected-row count) matters.
            let response = query_response_bytes(0, &[], 3);
            let mut resp = response_header(id, 0x20, 0x00);
            write_array(&mut resp, &response);
            stream.write_all(&resp).await.unwrap();
        });

        let client = client_with_seeds(vec![seed_addr], seed_addr);
        let affected = client
            .cache("my-cache")
            .query("DELETE FROM org.example.User WHERE age < :maxAge")
            .param("maxAge", QueryValue::Int32(18))
            .execute_statement()
            .await
            .expect("execute_statement should succeed");

        assert_eq!(affected, 3);

        seed_task.await.unwrap();
    }
}
