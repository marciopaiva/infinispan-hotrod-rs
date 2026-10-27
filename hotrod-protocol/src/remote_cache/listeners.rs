//! Registering client and counter listeners on a dedicated connection.
//!
//! Split out of `remote_cache.rs`
//! (`docs/adr/0026-split-connection-and-remote-cache-by-domain.md`):
//! see `remote_cache/multimap.rs`'s module docs for the mechanism
//! that lets this `impl RemoteCache` block reach every private
//! field/method it needs with no visibility change. No dedicated
//! tests here: `listener.rs`'s and `counter_listener.rs`'s own
//! tests, plus the live-server listener tests, already cover this
//! registration end to end.

use crate::counter_listener::CounterListener;
use crate::error::Result;
use crate::listener::{CacheListener, ListenOptions};

use super::RemoteCache;

impl RemoteCache {
    /// Registers a listener for this cache's events, with every event
    /// type and no server-side filtering. See `listen_with` for finer
    /// control (selected event types, a server-side filter or converter
    /// factory, replaying the cache's current contents first).
    pub async fn listen(&self) -> Result<CacheListener> {
        self.listen_with(&ListenOptions::default()).await
    }

    /// Registers a listener on a connection dedicated to it alone (see
    /// `listener.rs`'s module docs for why), targeting the current active
    /// seed: events are cache-wide, not routed by key, so there is no
    /// segment owner to pick instead. Not retried against another seed if
    /// this one refuses the connection: unlike `call`/`run_seed_op`,
    /// which node ends up holding a long-lived registration is worth the
    /// caller seeing directly rather than this silently failing over.
    pub async fn listen_with(&self, options: &ListenOptions) -> Result<CacheListener> {
        // Read together, not as two separate calls around the dial: see
        // `active_seed_addr_and_generation`'s own docs for the race
        // that would otherwise reopen between a switch and this
        // registration.
        let (seed, generation_rx) = self.client.active_seed_addr_and_generation();
        let conn = self
            .client
            .open_and_authenticate(seed, &self.cache_name, None)
            .await?;
        CacheListener::register(
            conn,
            self.cache_name.as_bytes(),
            self.client.timeout(),
            options,
            generation_rx,
        )
        .await
    }

    /// `StrongCounter::listen`/`WeakCounter::listen`'s own entry
    /// point (`docs/adr/0022-distributed-counter-listeners.md`): same
    /// shape as `listen_with` above (the active seed, dedicated
    /// connection, race-free seed+generation read), for `counter_name`
    /// on this cache, which is always `org.infinispan.COUNTER` by the
    /// time a `StrongCounter`/`WeakCounter` reaches this. `pub(crate)`
    /// only: not a supported entry point through a bare `RemoteCache`,
    /// the same as counters' own CRUD operations.
    pub(crate) async fn listen_for_counter(&self, counter_name: &str) -> Result<CounterListener> {
        let (seed, generation_rx) = self.client.active_seed_addr_and_generation();
        let conn = self
            .client
            .open_and_authenticate(seed, &self.cache_name, None)
            .await?;
        CounterListener::register(
            conn,
            counter_name,
            self.cache_name.as_bytes(),
            self.client.timeout(),
            generation_rx,
        )
        .await
    }
}
