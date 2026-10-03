//! A bounded pool of `HotRodConnection`s to one node, for one cache.
//!
//! Knows nothing about Hot Rod beyond holding `HotRodConnection` and
//! respecting its poisoning rule (`is_poisoned`, see the `connection`
//! module docs) before a connection is reused. `HotRodClient` owns one of
//! these per `(SocketAddr, cache name)` pair it has talked to: see
//! `docs/adr/0005-connection-pooling-and-client-cache-split.md` for why
//! the cache name is part of the key, not just the address.
//!
//! `slots` holds exactly the connections (or empty markers, meaning "open
//! one") not currently checked out; `semaphore`'s permit count is kept
//! equal to `slots.len()` at all times, purely so `checkout` can wait
//! asynchronously instead of polling when the pool is fully checked out.
//! A permit is never held past the `pop` it guards (`forget`ten
//! immediately after), and every `push` is paired with `add_permits(1)`,
//! so the two always agree. `checkout` pops the most recently returned
//! slot (LIFO), not the oldest: this matters because an empty slot (a
//! connection that still needs opening) and an idle connection are both
//! just entries in the same vec, and popping in return order makes a
//! checkout right after a healthy return reuse that connection instead of
//! opening a redundant new one.
//!
//! This replaced an earlier two-part design (a `Semaphore` permit tied to
//! each connection's lifetime, plus a separate idle `Vec` of
//! connection+permit pairs) that deadlocked under concurrent load above
//! `max_size`: a checkout that found the `Vec` momentarily empty
//! committed to waiting on the semaphore for a *new* permit, but a
//! healthy return never freed one (the permit stayed bound to the
//! connection it travelled with), so that waiter could only ever be woken
//! by an actual close, not by the connection it was waiting for becoming
//! idle again. Caught by running the `cluster_*` live-server tests
//! concurrently against a real two-node cluster under
//! `ci/infinispan-kind/`: every checkout past the first `max_size` hung
//! until the operation timeout. A second attempt (a bounded `mpsc`
//! channel of slots) fixed the hang but introduced a different bug: the
//! channel's FIFO order meant a freshly returned, still-open connection
//! sat behind the pool's other, still-empty initial slots, so the very
//! next checkout opened a redundant new connection instead of reusing it,
//! which a mock single-accept test listener cannot tolerate. The
//! single-vec, LIFO, permit-is-just-a-counter design above has neither
//! problem.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Semaphore;

use crate::connection::HotRodConnection;

enum Slot {
    /// Boxed so an empty slot (the common case while a pool is lightly
    /// used) does not carry the full size of a `HotRodConnection` around
    /// for no reason (clippy's `large_enum_variant`).
    Connection(Box<HotRodConnection>),
    Empty,
}

/// What `ConnectionPool::checkout` hands back: a connection ready to use,
/// or nothing, meaning the caller must open and authenticate a new one
/// itself, since doing that needs state (auth credentials, TLS config,
/// the topology id) this module has no idea about.
pub(crate) enum Checkout {
    Idle(Box<HotRodConnection>),
    NeedsNew,
}

pub(crate) struct ConnectionPool {
    semaphore: Semaphore,
    slots: Mutex<Vec<Slot>>,
    /// Set once the node this pool talks to has left the topology.
    /// `PooledGuard::drop` checks this before returning a connection, so
    /// eviction never has to reach into a slot another task currently
    /// holds checked out: it turns into an empty slot whenever it is
    /// returned instead.
    closed: AtomicBool,
}

impl ConnectionPool {
    pub(crate) fn new(max_size: usize) -> Self {
        Self {
            semaphore: Semaphore::new(max_size),
            slots: Mutex::new((0..max_size).map(|_| Slot::Empty).collect()),
            closed: AtomicBool::new(false),
        }
    }

    /// Waits for the next available slot: an idle connection, or an empty
    /// slot meaning the caller should open one. Never returns early with
    /// neither; a caller past `max_size` outstanding checkouts waits here
    /// until another checkout returns its slot, healthy or not.
    pub(crate) async fn checkout(&self) -> Checkout {
        let permit = self
            .semaphore
            .acquire()
            .await
            .expect("this pool's semaphore is never closed");
        // The permit's only job was gating this pop against `slots.len()`;
        // once popped, `return_slot` is what re-grows the count, via its
        // own `add_permits(1)`, not this permit being dropped/returned.
        permit.forget();
        let slot = self
            .slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .pop()
            .expect("the semaphore count equals slots.len(), so an acquired permit guarantees one");
        match slot {
            Slot::Connection(conn) => Checkout::Idle(conn),
            Slot::Empty => Checkout::NeedsNew,
        }
    }

    /// Returns the slot a checkout took, as a reusable connection if it
    /// is healthy and this pool has not since closed, as an empty slot
    /// otherwise. Exactly one call per `checkout` keeps the total slot
    /// count fixed at `max_size`. `pub(crate)`, not just used by
    /// `PooledGuard::drop`: `HotRodClient::checkout` also calls this
    /// directly with `None` when it took a `Checkout::NeedsNew` slot but
    /// failed (or was cancelled by its own outer timeout) before ever
    /// producing a connection to wrap in a `PooledGuard`, so that slot is
    /// never otherwise returned. See `PendingSlot` in `client.rs`.
    pub(crate) fn return_slot(&self, conn: Option<HotRodConnection>) {
        let slot = match conn {
            Some(conn) if !conn.is_poisoned() && !self.is_closed() => {
                Slot::Connection(Box::new(conn))
            }
            _ => Slot::Empty,
        };
        self.slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(slot);
        self.semaphore.add_permits(1);
    }

    /// Used by `HotRodClient::failover_seed` to make a freshly opened and
    /// authenticated connection to a new seed available immediately,
    /// without a caller's next checkout having to open yet another one.
    /// Takes whatever slot is available first (a brand new pool starts
    /// full of empty ones) and returns the real connection in its place,
    /// the same net effect on the slot count as one ordinary
    /// `checkout`/return pair.
    pub(crate) async fn seed_idle(&self, conn: HotRodConnection) {
        let _ = self.checkout().await;
        self.return_slot(Some(conn));
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Marks this pool closed and drops every connection idle in it right
    /// now, replacing each with an empty slot. A connection checked out
    /// at this moment is left alone: `PooledGuard::drop` checks
    /// `is_closed` when it runs and returns an empty slot instead of the
    /// connection, so this never reaches into a slot another task
    /// currently holds. The slot count itself does not change, so the
    /// semaphore needs no adjustment here.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.invalidate_idle();
    }

    /// Drops every connection idle in this pool right now, replacing each
    /// with an empty slot, without closing the pool itself: a later
    /// checkout still opens a fresh connection for one of these slots
    /// instead of failing outright. Used by `HotRodClient::authenticate_with`
    /// so an already-pooled connection authenticated with a credential
    /// that was just replaced (a token refresh, a changed password) is
    /// not handed to a later caller still carrying the old one; a
    /// connection currently checked out is unaffected; it was opened
    /// with whatever credential was current then, same as always.
    pub(crate) fn invalidate_idle(&self) {
        for slot in self
            .slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter_mut()
        {
            *slot = Slot::Empty;
        }
    }

    /// Applies a new timeout to every connection idle in this pool right
    /// now. A connection checked out by another task at the moment this
    /// runs is not reachable here; it picks up the new timeout once it is
    /// returned and later checked out again. See `HotRodClient::set_timeout`.
    pub(crate) fn set_idle_timeouts(&self, timeout: std::time::Duration) {
        for slot in self
            .slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter_mut()
        {
            if let Slot::Connection(conn) = slot {
                conn.set_timeout(timeout);
            }
        }
    }
}

/// A connection checked out from a `ConnectionPool`, returned to it on
/// drop unless poisoned or the pool has since closed.
pub(crate) struct PooledGuard {
    pool: Arc<ConnectionPool>,
    conn: Option<HotRodConnection>,
}

impl PooledGuard {
    pub(crate) fn new(pool: Arc<ConnectionPool>, conn: HotRodConnection) -> Self {
        Self {
            pool,
            conn: Some(conn),
        }
    }
}

impl Deref for PooledGuard {
    type Target = HotRodConnection;

    fn deref(&self) -> &HotRodConnection {
        self.conn
            .as_ref()
            .expect("conn is only taken in Drop, after which the guard is not used again")
    }
}

impl DerefMut for PooledGuard {
    fn deref_mut(&mut self) -> &mut HotRodConnection {
        self.conn
            .as_mut()
            .expect("conn is only taken in Drop, after which the guard is not used again")
    }
}

impl Drop for PooledGuard {
    fn drop(&mut self) {
        self.pool.return_slot(self.conn.take());
    }
}
