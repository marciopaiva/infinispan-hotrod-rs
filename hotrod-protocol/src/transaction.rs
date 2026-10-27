//! Local, optimistic transactions against one cache, one-phase
//! commit only (`docs/adr/0018-transactions-non-xa-one-phase.md`).
//!
//! `get`/`put`/`remove` never touch the network for a write: every
//! modification stays in memory until `commit` sends the whole batch
//! in one `PREPARE_TX_2` request with `onePhaseCommit` set, so a
//! single round trip both validates (against each modified key's own
//! read version, if it was read first in this transaction) and
//! applies them. `rollback` is purely local for the same reason:
//! nothing was ever sent to the server to undo.
//!
//! The cache this runs against must already be configured as
//! transactional server-side (`<transaction mode="NON_XA"/>` or
//! similar); there is no opcode to check this ahead of time, so an
//! untransactional cache only reveals itself when `commit` fails.
//!
//! A `Transaction` is pinned to the cluster active when
//! `begin_transaction` created it (see the addendum in
//! `docs/adr/0018-transactions-non-xa-one-phase.md`): every `get` and
//! the final `commit` target that cluster for the transaction's whole
//! lifetime, never a different one a multi-cluster failover may have
//! switched to in the meantime. If that cluster stops responding, the
//! transaction fails with a connection error instead of silently
//! continuing against a cluster whose state it never actually read.
//! If a switch away and back is merely detected after the fact, `get`/
//! `commit` fail fast with `Error::TransactionClusterChanged`, without
//! attempting any network call at all.

use std::collections::HashMap;

use rand::RngCore;

use crate::connection::VersionedValue;
use crate::error::{Error, Result};
use crate::remote_cache::RemoteCache;
use crate::wire::Expiration;

/// `0x48525458`, "HRTX" in hex: this crate's own `Xid.formatId`,
/// confirmed against the Java client's `RemoteXid` to be an
/// arbitrary value the server never interprets, just echoes back (no
/// external JTA coordinator exists in this phase to need a
/// standardized one).
pub(crate) const XID_FORMAT_ID: i32 = 0x4852_5458;

/// Both `Xid` byte arrays are this many bytes, confirmed against the
/// Java client's own `RemoteXid`. The JTA `Xid` spec allows up to 64;
/// this crate's own generated ids do not need the extra room.
const XID_ARRAY_LEN: usize = 32;

/// A transaction identifier this crate generates itself: there is no
/// external transaction coordinator in this phase for one to come
/// from. Opaque to the server beyond being echoed back; its internal
/// structure (unlike the Java client's own `RemoteXid`, built from a
/// UUID, a timestamp and a counter) is just random bytes, since
/// nothing needs to parse it back out.
pub(crate) struct Xid {
    pub(crate) format_id: i32,
    pub(crate) global_transaction_id: Vec<u8>,
    pub(crate) branch_qualifier: Vec<u8>,
}

pub(crate) fn generate_xid() -> Xid {
    let mut global_transaction_id = vec![0u8; XID_ARRAY_LEN];
    rand::thread_rng().fill_bytes(&mut global_transaction_id);
    let mut branch_qualifier = vec![0u8; XID_ARRAY_LEN];
    rand::thread_rng().fill_bytes(&mut branch_qualifier);
    Xid {
        format_id: XID_FORMAT_ID,
        global_transaction_id,
        branch_qualifier,
    }
}

/// Bitmask confirmed against the Java client's own `ControlByte`:
/// combined with `|`, not a discrete enum. `NOT_READ` and
/// `NON_EXISTING` are mutually exclusive in practice (a key is either
/// never read in this transaction, or read and found to exist or
/// not), but nothing here enforces that beyond `Transaction` itself
/// only ever producing one or the other.
const NOT_READ: u8 = 0x1;
const NON_EXISTING: u8 = 0x2;
const REMOVE_OP: u8 = 0x4;

#[derive(Debug, Clone)]
pub(crate) enum ModificationWrite {
    Put {
        value: Vec<u8>,
        lifespan: Expiration,
        max_idle: Expiration,
    },
    Remove,
}

/// One accumulated write, ready to serialize into a `PREPARE_TX_2`
/// request. `version` is `Some` only when the key was read first in
/// this transaction and existed: confirmed against the Java client's
/// own `Modification.writeTo`, which writes the version if and only
/// if neither `NON_EXISTING` nor `NOT_READ` is set, i.e. exactly the
/// "read and existed" case.
pub(crate) struct Modification {
    pub(crate) key: Vec<u8>,
    pub(crate) version: Option<u64>,
    pub(crate) not_read: bool,
    pub(crate) write: ModificationWrite,
}

impl Modification {
    fn control_byte(&self) -> u8 {
        let mut control = 0u8;
        if self.not_read {
            control |= NOT_READ;
        } else if self.version.is_none() {
            control |= NON_EXISTING;
        }
        if matches!(self.write, ModificationWrite::Remove) {
            control |= REMOVE_OP;
        }
        control
    }
}

/// Builds a `PREPARE_TX_2` request body: `Xid`, `onePhaseCommit`
/// (always `true` in this phase), `recoverable` (always `false`:
/// this phase implements no recovery opcodes a recoverable
/// transaction would need), `timeoutMs` as a raw `i64` (not a
/// `vLong`), a `vInt` modification count, then each modification in
/// turn (confirmed field order against the Java client's own
/// `PrepareTransactionOperation.writeOperationRequest` and
/// `Modification.writeTo`).
pub(crate) fn encode_prepare_request(
    xid: &Xid,
    timeout_ms: i64,
    modifications: &[Modification],
) -> Vec<u8> {
    let mut buf = Vec::new();
    crate::varint::write_signed_vint(&mut buf, xid.format_id);
    crate::wire::write_array(&mut buf, &xid.global_transaction_id);
    crate::wire::write_array(&mut buf, &xid.branch_qualifier);
    buf.push(1); // onePhaseCommit: true
    buf.push(0); // recoverable: false
    buf.extend_from_slice(&timeout_ms.to_be_bytes());
    crate::varint::write_vint(&mut buf, modifications.len() as u32);
    for modification in modifications {
        crate::wire::write_array(&mut buf, &modification.key);
        buf.push(modification.control_byte());
        if let Some(version) = modification.version {
            buf.extend_from_slice(&version.to_be_bytes());
        }
        if let ModificationWrite::Put {
            value,
            lifespan,
            max_idle,
        } = &modification.write
        {
            crate::wire::write_expiration_params(&mut buf, *lifespan, *max_idle);
            crate::wire::write_array(&mut buf, value);
        }
    }
    buf
}

/// `javax.transaction.xa.XAResource`'s own result codes, confirmed
/// as what the 4-byte `i32` after a successful (`NO_ERROR_STATUS`)
/// `PREPARE_TX_2` response actually carries: the Hot Rod status byte
/// itself is almost always success, the real outcome is this int.
const XA_OK: i32 = 0;
const XA_RDONLY: i32 = 3;
const XA_RBROLLBACK: i32 = 100;
const XAER_RMERR: i32 = -3;

/// Maps a `PREPARE_TX_2` response's XA result code to this crate's
/// own `Result`. `XA_OK`/`XA_RDONLY` (the transaction had no writes
/// at all, so the server never needed to apply anything) both mean
/// every write committed. Anything else means none did; see
/// `Error::TransactionRolledBack`/`Error::TransactionCommitFailed`
/// for what little the wire distinguishes beyond that.
pub(crate) fn decode_prepare_response(xa_result: i32) -> Result<()> {
    match xa_result {
        XA_OK | XA_RDONLY => Ok(()),
        XA_RBROLLBACK => Err(Error::TransactionRolledBack),
        XAER_RMERR => Err(Error::TransactionCommitFailed),
        _ => Err(Error::TransactionCommitFailed),
    }
}

enum ReadOutcome {
    Existed { version: u64 },
    DidNotExist,
}

/// A local, optimistic transaction against one `RemoteCache`,
/// obtained from `RemoteCache::begin_transaction`. See the module
/// docs for what `get`/`put`/`remove` do and don't send over the
/// network before `commit`.
pub struct Transaction {
    cache: RemoteCache,
    pinned_cluster: String,
    pinned_generation: u64,
    reads: HashMap<Vec<u8>, ReadOutcome>,
    writes: HashMap<Vec<u8>, ModificationWrite>,
}

impl Transaction {
    pub(crate) fn new(cache: RemoteCache) -> Self {
        let (pinned_cluster, pinned_generation) = cache.active_cluster_name_and_generation();
        Self {
            cache,
            pinned_cluster,
            pinned_generation,
            reads: HashMap::new(),
            writes: HashMap::new(),
        }
    }

    /// Checks this transaction's own pending writes first
    /// (read-your-own-writes): a `put`/`remove` earlier in the same
    /// transaction is visible to a later `get` without a round trip.
    /// Otherwise reads the server directly (`get_with_version`,
    /// already used by the main cache API), recording the version
    /// seen, or that the key did not exist, so a later `put`/`remove`
    /// on the same key validates against it at `commit`.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(write) = self.writes.get(key) {
            return Ok(match write {
                ModificationWrite::Put { value, .. } => Some(value.clone()),
                ModificationWrite::Remove => None,
            });
        }
        match self
            .cache
            .get_with_version_pinned(key, &self.pinned_cluster, self.pinned_generation)
            .await?
        {
            Some(VersionedValue { value, version, .. }) => {
                self.reads
                    .insert(key.to_vec(), ReadOutcome::Existed { version });
                Ok(Some(value))
            }
            None => {
                self.reads.insert(key.to_vec(), ReadOutcome::DidNotExist);
                Ok(None)
            }
        }
    }

    /// Records a put against `key`. Local only: no network call
    /// until `commit`.
    pub fn put(&mut self, key: &[u8], value: &[u8], lifespan: Expiration, max_idle: Expiration) {
        self.writes.insert(
            key.to_vec(),
            ModificationWrite::Put {
                value: value.to_vec(),
                lifespan,
                max_idle,
            },
        );
    }

    /// Records a removal of `key`. Local only: no network call until
    /// `commit`.
    pub fn remove(&mut self, key: &[u8]) {
        self.writes.insert(key.to_vec(), ModificationWrite::Remove);
    }

    /// Sends every recorded write in one `PREPARE_TX_2` request.
    /// `Ok(())` means every write committed. `Err` means none of
    /// them did (see `Error::TransactionRolledBack`/
    /// `Error::TransactionCommitFailed`); the usual response to a
    /// rolled-back transaction is to retry the whole thing from
    /// scratch, not to inspect which individual write conflicted,
    /// since the wire does not say.
    pub async fn commit(self) -> Result<()> {
        let xid = generate_xid();
        let modifications = self
            .writes
            .into_iter()
            .map(|(key, write)| {
                let (version, not_read) = match self.reads.get(&key) {
                    Some(ReadOutcome::Existed { version }) => (Some(*version), false),
                    Some(ReadOutcome::DidNotExist) => (None, false),
                    None => (None, true),
                };
                Modification {
                    key,
                    version,
                    not_read,
                    write,
                }
            })
            .collect();
        self.cache
            .run_prepare_transaction(
                xid,
                modifications,
                &self.pinned_cluster,
                self.pinned_generation,
            )
            .await
    }

    /// Discards every recorded read and write. Local only: nothing
    /// was ever sent to the server before `commit` in this one-phase
    /// design, so there is nothing to undo remotely. Dropping a
    /// `Transaction` without calling `commit` or `rollback` has the
    /// exact same effect; this method exists for callers who want
    /// that intent to be explicit.
    pub fn rollback(self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::tests::unreachable_addr;
    use crate::remote_cache::tests::client_with_seeds;

    #[tokio::test]
    async fn get_after_a_local_put_returns_the_local_value_without_touching_the_network() {
        let addr = unreachable_addr().await;
        let client = client_with_seeds(vec![addr], addr);
        let mut tx = client.cache("my-cache").begin_transaction();

        tx.put(b"key", b"value", Expiration::Immortal, Expiration::Immortal);
        let value = tx
            .get(b"key")
            .await
            .expect("a local write must be visible without a network round trip");

        assert_eq!(value, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn get_after_a_local_remove_returns_none_without_touching_the_network() {
        let addr = unreachable_addr().await;
        let client = client_with_seeds(vec![addr], addr);
        let mut tx = client.cache("my-cache").begin_transaction();

        tx.put(b"key", b"value", Expiration::Immortal, Expiration::Immortal);
        tx.remove(b"key");
        let value = tx
            .get(b"key")
            .await
            .expect("a local removal must be visible without a network round trip");

        assert_eq!(value, None);
    }

    /// `rollback` never calls `RemoteCache`/the network at all, so an
    /// address that refuses every connection is enough to prove it:
    /// if `rollback` tried to send anything, that attempt would have
    /// nowhere to succeed, and this test would hang or error instead
    /// of returning.
    #[tokio::test]
    async fn rollback_discards_pending_writes_purely_locally() {
        let addr = unreachable_addr().await;
        let client = client_with_seeds(vec![addr], addr);
        let mut tx = client.cache("my-cache").begin_transaction();

        tx.put(b"key", b"value", Expiration::Immortal, Expiration::Immortal);
        tx.rollback();
    }
}
