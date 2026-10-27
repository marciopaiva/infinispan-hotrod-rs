//! Local, optimistic transactions, one-phase commit only
//! (`docs/adr/0018-transactions-non-xa-one-phase.md`), pinned to the
//! cluster active when they began (that ADR's addendum,
//! `docs/adr/0026-split-connection-and-remote-cache-by-domain.md`).
//!
//! Split out of `remote_cache.rs`: see `remote_cache/multimap.rs`'s
//! module docs for the mechanism that lets this `impl RemoteCache`
//! block reach `call`/`call_pinned`/`call_seed_pinned`, private
//! methods defined at `remote_cache.rs`'s root, with no visibility
//! change.

use crate::connection::VersionedValue;
use crate::error::Result;
use crate::transaction::{Modification, Transaction, Xid};

use super::{Operation, OperationResult, RemoteCache};

impl RemoteCache {
    /// Same as `get_with_version`, but for `Transaction::get`: pinned to
    /// the cluster active when the transaction began, see `call_pinned`.
    /// `Transaction`-only; the public `get_with_version` is unaffected.
    pub(crate) async fn get_with_version_pinned(
        &self,
        key: &[u8],
        pinned_cluster: &str,
        pinned_generation: u64,
    ) -> Result<Option<VersionedValue>> {
        let op = Operation::GetWithVersion(key.to_vec());
        match self
            .call_pinned(key, op, pinned_cluster, pinned_generation)
            .await?
        {
            OperationResult::GetWithVersion(value) => Ok(value),
            _ => unreachable!(
                "Operation::GetWithVersion always yields OperationResult::GetWithVersion"
            ),
        }
    }

    /// Starts a local, optimistic transaction against this cache
    /// (`docs/adr/0018-transactions-non-xa-one-phase.md`). No
    /// network call: see `Transaction`'s own docs.
    pub fn begin_transaction(&self) -> Transaction {
        Transaction::new(self.clone())
    }

    /// Sends `modifications` as one `PREPARE_TX_2` request under
    /// `xid`, generated once by `Transaction::commit` so the same id
    /// is replayed on every retry attempt `call_seed` makes. No key
    /// to route by: a transaction can span several keys with
    /// different owners, and the server resolves that internally
    /// regardless of which node receives the request (confirmed
    /// against the Java client: its own first-key-owner routing is a
    /// latency heuristic, not a protocol requirement). Always goes to
    /// the seed, the same as `query`/`size`/`clear`.
    /// `pinned_cluster`/`pinned_generation` are `Transaction::new`'s own
    /// capture of the cluster active when it began: this never lets the
    /// final `PREPARE_TX_2` reach a different cluster than the one the
    /// transaction's reads came from. See `call_seed_pinned`.
    pub(crate) async fn run_prepare_transaction(
        &self,
        xid: Xid,
        modifications: Vec<Modification>,
        pinned_cluster: &str,
        pinned_generation: u64,
    ) -> Result<()> {
        match self
            .call_seed_pinned(
                Operation::PrepareTransaction(xid, modifications),
                pinned_cluster,
                pinned_generation,
            )
            .await?
        {
            OperationResult::PrepareTransaction => Ok(()),
            _ => unreachable!(
                "Operation::PrepareTransaction always yields OperationResult::PrepareTransaction"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::net::TcpListener;

    use crate::client::tests::unreachable_addr;
    use crate::client::HotRodClient;
    use crate::error::Error;
    use crate::remote_cache::tests::client_with_seeds;
    use crate::wire::Expiration;

    /// A `Transaction` is pinned to the cluster active when it began
    /// (`docs/adr/0018-transactions-non-xa-one-phase.md`'s addendum):
    /// unlike a plain `get`
    /// (`automatic_cluster_failover_completes_the_operation_on_a_live_
    /// alternate` above), its `commit` must never cross over to a
    /// different, otherwise-live configured cluster just because the
    /// original one's every seed is down. The alternate cluster's
    /// listener below is deliberately never joined: if `commit`
    /// wrongly reached it, this test would still fail on the
    /// assertions that follow rather than hang, since nothing here
    /// waits on that connection ever happening.
    #[tokio::test]
    async fn transaction_commit_does_not_fail_over_to_another_cluster_when_pinned() {
        let dead_seed_addr = unreachable_addr().await;

        let dr_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dr_addr = dr_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = dr_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = client_with_seeds(vec![dead_seed_addr], dead_seed_addr);
        client
            .add_cluster("dr", vec![dr_addr])
            .expect("add_cluster should succeed");

        let mut tx = client.cache("my-cache").begin_transaction();
        tx.put(b"key", b"value", Expiration::Immortal, Expiration::Immortal);

        let result = tx.commit().await;

        assert!(
            matches!(result, Err(Error::Io(_)) | Err(Error::Timeout(_))),
            "a pinned commit must fail with the pinned cluster's own error, never reach the alternate cluster"
        );
        assert_eq!(
            client.active_cluster_name(),
            HotRodClient::DEFAULT_CLUSTER_NAME,
            "a pinned commit must never switch the active cluster"
        );
    }

    /// `Transaction::get` fails fast, before attempting any network
    /// call, once the active cluster has moved on from the one pinned
    /// at `begin_transaction`. The other cluster's listener below would
    /// answer correctly if dialed; it never is, which is what proves
    /// this is a local check against `cluster_generation`, not a
    /// request that happened to land on the wrong cluster and get
    /// rejected there.
    #[tokio::test]
    async fn transaction_get_fails_fast_when_the_active_cluster_already_changed() {
        let original_addr = unreachable_addr().await;

        let other_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let other_addr = other_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = other_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = client_with_seeds(vec![original_addr], original_addr);
        client
            .add_cluster("other", vec![other_addr])
            .expect("add_cluster should succeed");

        let mut tx = client.cache("my-cache").begin_transaction();

        client
            .switch_to_cluster("other")
            .expect("switch_to_cluster should succeed");

        let err = tx
            .get(b"key")
            .await
            .expect_err("get must fail once the active cluster no longer matches the pin");

        match err {
            Error::TransactionClusterChanged { pinned, current } => {
                assert_eq!(pinned, HotRodClient::DEFAULT_CLUSTER_NAME);
                assert_eq!(current, "other");
            }
            other => panic!("expected TransactionClusterChanged, got {other:?}"),
        }
    }

    /// Same as `transaction_get_fails_fast_when_the_active_cluster_
    /// already_changed`, but the switch happens between a local `put`
    /// and `commit`: the write was only ever recorded locally, so
    /// nothing was lost by failing here, but `commit` must still
    /// refuse rather than send it to the wrong cluster.
    #[tokio::test]
    async fn transaction_commit_fails_fast_when_the_active_cluster_changed_since_begin() {
        let original_addr = unreachable_addr().await;

        let other_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let other_addr = other_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = other_listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = client_with_seeds(vec![original_addr], original_addr);
        client
            .add_cluster("other", vec![other_addr])
            .expect("add_cluster should succeed");

        let mut tx = client.cache("my-cache").begin_transaction();
        tx.put(b"key", b"value", Expiration::Immortal, Expiration::Immortal);

        client
            .switch_to_cluster("other")
            .expect("switch_to_cluster should succeed");

        let err = tx
            .commit()
            .await
            .expect_err("commit must fail once the active cluster no longer matches the pin");

        match err {
            Error::TransactionClusterChanged { pinned, current } => {
                assert_eq!(pinned, HotRodClient::DEFAULT_CLUSTER_NAME);
                assert_eq!(current, "other");
            }
            other => panic!("expected TransactionClusterChanged, got {other:?}"),
        }
    }
}
