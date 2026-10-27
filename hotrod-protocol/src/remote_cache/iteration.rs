//! Server-side iteration (`docs/adr/0009-server-side-iteration.md`).
//!
//! Split out of `remote_cache.rs`
//! (`docs/adr/0026-split-connection-and-remote-cache-by-domain.md`):
//! see `remote_cache/multimap.rs`'s module docs for the mechanism
//! that lets this `impl RemoteCache` block reach `RemoteCache`'s
//! otherwise-private `client`/`cache_name` fields with no visibility
//! change. No dedicated tests here: the wire-level shape is covered
//! by `connection::iteration`'s own tests, and `iteration.rs`'s own
//! tests cover `CacheIterator`'s cross-node fan-out.

use std::collections::VecDeque;
use std::net::SocketAddr;

use crate::error::Result;
use crate::iteration::{CacheIterator, IterationOptions, NodeIterator};
use crate::topology::TopologyServer;

use super::RemoteCache;

impl RemoteCache {
    /// Opens a cursor over every entry in this cache, with the
    /// server's own default batch size and no server-side filter. See
    /// `iter_with` for finer control, and
    /// `docs/adr/0009-server-side-iteration.md` for how the cluster-wide
    /// fan-out works.
    pub async fn iter(&self) -> Result<CacheIterator> {
        self.iter_with(IterationOptions::default()).await
    }

    /// Opens a cursor over every entry in this cache. On a cluster,
    /// this opens one server-side cursor per node that primary-owns at
    /// least one segment, read one node at a time rather than
    /// concurrently; see the module docs on `iteration.rs` for why.
    /// Opens the first node's cursor eagerly, the same as `get_stream`/
    /// `put_stream` open their own connection eagerly, so a failure to
    /// reach it surfaces here rather than on the first `next_entry`
    /// call.
    pub async fn iter_with(&self, options: IterationOptions) -> Result<CacheIterator> {
        let mut targets: VecDeque<_> = self.client.nodes_and_owned_segments().await?.into();
        let current = match targets.pop_front() {
            Some((addr, origin, segments)) => Some(
                self.open_node_iterator(addr, origin, segments, &options)
                    .await?,
            ),
            None => None,
        };
        Ok(CacheIterator::new(self.clone(), current, targets, options))
    }

    /// Shared by `iter_with` and `CacheIterator::next_entry`: checks
    /// out a connection to `addr` and opens a server-side cursor over
    /// `segments` on it (`None` when `segments` is empty, meaning no
    /// filter: the whole cache, from whichever node this is).
    pub(crate) async fn open_node_iterator(
        &self,
        addr: SocketAddr,
        origin: Option<TopologyServer>,
        segments: Vec<u32>,
        options: &IterationOptions,
    ) -> Result<NodeIterator> {
        let segments_arg = if segments.is_empty() {
            None
        } else {
            Some(segments.as_slice())
        };
        let mut guard = self.client.checkout(addr, &self.cache_name, origin).await?;
        let iteration_id = guard
            .iteration_start(
                segments_arg,
                options.filter_factory.as_ref(),
                options.batch_size_or_default(),
            )
            .await?;
        self.client.record_topology_update(&mut guard);
        Ok(NodeIterator::new(guard, iteration_id))
    }
}
