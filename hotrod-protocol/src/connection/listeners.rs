//! Registering client and counter listeners: the handoff into
//! `listener.rs`/`counter_listener.rs`'s own event-stream handling.
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): see `connection/multimap.rs`'s module
//! docs for the mechanism that lets this `impl HotRodConnection`
//! block reach every private field/method it needs with no
//! visibility change. No dedicated tests here: `listener.rs`'s and
//! `counter_listener.rs`'s own tests, plus the live-server listener
//! tests, already cover this registration end to end.

use crate::error::Result;
use crate::header::OpCode;

use super::{with_timeout, HotRodConnection};

impl HotRodConnection {
    /// Registers a client listener, `body` already encoded by
    /// `listener.rs` (listener id, include-current-state flag,
    /// filter/converter factory names and parameters, raw-data flag,
    /// event interests). Confirms the server accepted it; the connection
    /// is expected to be handed to `listener.rs` via `into_transport`
    /// right after, not reused for further operations here.
    pub(crate) async fn add_client_listener(&mut self, body: &[u8]) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::AddClientListener, body)
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }

    /// Registers a counter listener, `body` already encoded by
    /// `counter_listener.rs` (counter name, then listener id; no
    /// factories or interests, unlike `add_client_listener`'s fuller
    /// body: the counter listener protocol has no server-side
    /// filtering concept at all). Same handoff as `add_client_listener`:
    /// the connection is expected to go to `counter_listener.rs` via
    /// `into_transport` right after, not reused here.
    pub(crate) async fn counter_add_listener(&mut self, body: &[u8]) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let cache_name = self.cache_name.clone();
            self.write_and_read_header(&cache_name, OpCode::CounterAddListener, body)
                .await?;
            Ok(())
        })
        .await;
        self.end_operation(result.is_ok());
        result
    }
}
