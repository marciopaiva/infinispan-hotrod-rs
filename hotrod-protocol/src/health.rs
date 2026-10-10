//! Per-node health tracking for the retry chain in `remote_cache.rs`
//! (`docs/adr/0011-retry-policy-and-node-health.md`): a lightweight,
//! TTL-based circuit breaker mirroring the Java client's
//! `OperationDispatcher.connectionFailedServers`, a cache with
//! `expireAfterWrite` that blacklists a node the moment it fails,
//! rather than counting consecutive failures first. A plain
//! `HashMap<SocketAddr, Instant>` covers the same role at this scale
//! (one client process, not thousands of tracked entries), so no new
//! dependency is needed for it.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Tracks when each node last failed, so a retry chain can skip a node
/// still inside its quarantine window instead of paying a full
/// connect-or-request timeout against it again. Nothing expires
/// entries on a timer: `is_quarantined` just compares the recorded
/// failure time against now, so a stale entry costs one `HashMap` slot
/// until the next failure or success touches the same address.
#[derive(Default)]
pub(crate) struct NodeHealth {
    failed_at: RwLock<HashMap<SocketAddr, Instant>>,
}

impl NodeHealth {
    /// Records `addr` as having just failed, overwriting any earlier
    /// failure time: only the most recent one matters for the
    /// quarantine window.
    pub(crate) fn mark_failed(&self, addr: SocketAddr) {
        self.failed_at
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(addr, Instant::now());
    }

    /// `true` if `addr` failed less than `quarantine` ago.
    pub(crate) fn is_quarantined(&self, addr: SocketAddr, quarantine: Duration) -> bool {
        self.failed_at
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(&addr)
            .is_some_and(|&failed_at| failed_at.elapsed() < quarantine)
    }

    /// Clears `addr`'s recorded failure, called after a successful
    /// attempt against it.
    pub(crate) fn clear(&self, addr: SocketAddr) {
        self.failed_at
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&addr);
    }

    /// Clears every recorded failure, called when a topology update
    /// actually changes the topology id: a new topology already means
    /// the cluster's membership view changed, so stale quarantine
    /// state should not outlive it.
    pub(crate) fn clear_all(&self) {
        self.failed_at
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    #[test]
    fn a_node_that_never_failed_is_not_quarantined() {
        let health = NodeHealth::default();
        assert!(!health.is_quarantined(addr(1), Duration::from_secs(30)));
    }

    #[test]
    fn a_node_marked_failed_is_quarantined_until_the_window_elapses() {
        let health = NodeHealth::default();
        health.mark_failed(addr(1));
        assert!(health.is_quarantined(addr(1), Duration::from_secs(30)));
        assert!(!health.is_quarantined(addr(1), Duration::from_millis(0)));
    }

    #[test]
    fn clear_lifts_the_quarantine_before_the_window_elapses() {
        let health = NodeHealth::default();
        health.mark_failed(addr(1));
        health.clear(addr(1));
        assert!(!health.is_quarantined(addr(1), Duration::from_secs(30)));
    }

    #[test]
    fn clearing_or_quarantining_one_address_does_not_affect_another() {
        let health = NodeHealth::default();
        health.mark_failed(addr(1));
        assert!(!health.is_quarantined(addr(2), Duration::from_secs(30)));
    }

    #[test]
    fn clear_all_lifts_every_quarantine_at_once() {
        let health = NodeHealth::default();
        health.mark_failed(addr(1));
        health.mark_failed(addr(2));
        health.clear_all();
        assert!(!health.is_quarantined(addr(1), Duration::from_secs(30)));
        assert!(!health.is_quarantined(addr(2), Duration::from_secs(30)));
    }
}
