//! Remote query (Ickle), `docs/adr/0013-remote-query.md`.
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): see `connection/multimap.rs`'s module
//! docs for the mechanism that lets this `impl HotRodConnection`
//! block reach every private field/method it needs with no
//! visibility change. No dedicated tests here: the wire-level query
//! round trip, including every projection and pagination case, is
//! already covered end to end by `remote_cache::query`'s own tests,
//! which exercise this method through `Query`'s public API.

use crate::error::Result;
use crate::header::OpCode;
use crate::query::{decode_query_response, QueryResult};
use crate::wire::{read_array, write_array};

use super::{with_timeout, HotRodConnection};

impl HotRodConnection {
    /// Runs a remote query (Ickle), `docs/adr/0013-remote-query.md`.
    /// `request_bytes` is already the Protobuf-encoded `QueryRequest`
    /// body (`query::encode_query_request`); wrapped here in a Hot Rod
    /// array the same way the Java client carries it
    /// (`QueryOperation.writeOperationRequest` calls `ByteBufUtil.
    /// writeArray`, never writing the Protobuf bytes to the Hot Rod
    /// buffer directly), and the response read back the same way.
    pub(crate) async fn query(&mut self, request_bytes: &[u8]) -> Result<QueryResult> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let mut body = Vec::new();
            write_array(&mut body, request_bytes);
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::Query, &body)
                .await?;
            let response_bytes = read_array(&mut self.stream).await?;
            decode_query_response(&response_bytes)
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }
}
