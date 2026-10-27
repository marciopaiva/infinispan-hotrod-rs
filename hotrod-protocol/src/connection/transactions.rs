//! `PREPARE_TX_2`, one-phase commit only
//! (`docs/adr/0018-transactions-non-xa-one-phase.md`).
//!
//! Split out of `connection.rs` (`docs/adr/0026-split-connection-and-
//! remote-cache-by-domain.md`): see `connection/multimap.rs`'s module
//! docs for the mechanism that lets this `impl HotRodConnection`
//! block reach every private field/method it needs with no
//! visibility change.

use crate::error::{Error, Result};
use crate::header::OpCode;

use super::{with_timeout, HotRodConnection};

impl HotRodConnection {
    /// Sends `modifications` as one `PREPARE_TX_2` request with
    /// `onePhaseCommit` set
    /// (`docs/adr/0018-transactions-non-xa-one-phase.md`). `xid` is
    /// generated once by the caller (`Transaction::commit`), not
    /// here: `dispatch`'s retry chain calls this again, against
    /// another node, on a transport failure, and must reuse the
    /// exact same id on every attempt, the same reason every other
    /// retried operation replays the same owned arguments rather
    /// than building new ones per attempt.
    ///
    /// `NOT_PUT_REMOVED_REPLACED` (confirmed against the server's own
    /// `PrepareTransactionOperation.createResponse`) means a
    /// concurrent prepare for the same transaction id conflicted with
    /// this one, with no body to read either way; mapped to
    /// `Error::TransactionRolledBack` the same as a real rollback,
    /// since neither leaves anything committed. Otherwise the status
    /// is success and the real outcome is the 4-byte XA result code
    /// that follows (`transaction::decode_prepare_response`).
    pub(crate) async fn prepare_transaction_one_phase(
        &mut self,
        xid: &crate::transaction::Xid,
        modifications: &[crate::transaction::Modification],
    ) -> Result<()> {
        self.begin_operation()?;
        let timeout = self.timeout;
        let result = with_timeout(timeout, async {
            let body = crate::transaction::encode_prepare_request(
                xid,
                timeout.as_millis() as i64,
                modifications,
            );
            let cache_name = self.cache_name.clone();
            let (_message_id, header) = self
                .write_and_read_header(&cache_name, OpCode::PrepareTx, &body)
                .await?;
            if header.status.is_not_put_removed_replaced() {
                return Err(Error::TransactionRolledBack);
            }
            let xa_result = tokio::io::AsyncReadExt::read_i32(&mut self.stream).await?;
            crate::transaction::decode_prepare_response(xa_result)
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
    use crate::wire::{read_array, Expiration};

    /// Confirms the full `PREPARE_TX_2` body against
    /// `docs/adr/0018-transactions-non-xa-one-phase.md`: a 32-byte Xid
    /// pair, `onePhaseCommit=true`, `recoverable=false`, the raw i64
    /// timeout, then one modification whose control byte is
    /// `NOT_READ` (`0x01`, the key was never read in this
    /// transaction) carrying no version, followed by its
    /// expiration-tagged value. A successful response is the status
    /// byte plus the 4-byte `XA_OK` result code.
    #[tokio::test]
    async fn prepare_transaction_sends_xid_flags_and_modifications() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x7D, "expected a PrepareTx request");

            let raw_format_id = crate::varint::read_vint(&mut stream).await.unwrap();
            let format_id = ((raw_format_id >> 1) as i32) ^ -((raw_format_id & 1) as i32);
            assert_eq!(format_id, crate::transaction::XID_FORMAT_ID);
            let global_transaction_id = read_array(&mut stream).await.unwrap();
            assert_eq!(global_transaction_id.len(), 32);
            let branch_qualifier = read_array(&mut stream).await.unwrap();
            assert_eq!(branch_qualifier.len(), 32);

            let one_phase_commit = stream.read_u8().await.unwrap();
            assert_eq!(one_phase_commit, 1);
            let recoverable = stream.read_u8().await.unwrap();
            assert_eq!(recoverable, 0);

            let mut timeout_bytes = [0u8; 8];
            stream.read_exact(&mut timeout_bytes).await.unwrap();
            assert_eq!(i64::from_be_bytes(timeout_bytes), 30_000);

            let count = crate::varint::read_vint(&mut stream).await.unwrap();
            assert_eq!(count, 1);
            let key = read_array(&mut stream).await.unwrap();
            assert_eq!(key, b"key");
            let control_byte = stream.read_u8().await.unwrap();
            assert_eq!(control_byte, 0x1, "NOT_READ: this key was never read first");
            let time_units = stream.read_u8().await.unwrap();
            assert_eq!(time_units, 0x88, "lifespan and max idle both immortal");
            let value = read_array(&mut stream).await.unwrap();
            assert_eq!(value, b"value");

            let mut resp = response_header(id, 0x7E, 0x00);
            resp.extend_from_slice(&0i32.to_be_bytes()); // XA_OK
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "transactional-test")
            .await
            .expect("connect");
        let xid = crate::transaction::generate_xid();
        let modifications = vec![crate::transaction::Modification {
            key: b"key".to_vec(),
            version: None,
            not_read: true,
            write: crate::transaction::ModificationWrite::Put {
                value: b"value".to_vec(),
                lifespan: Expiration::Immortal,
                max_idle: Expiration::Immortal,
            },
        }];
        conn.prepare_transaction_one_phase(&xid, &modifications)
            .await
            .expect("prepare_transaction_one_phase");

        server.await.unwrap();
    }

    /// Exercises every other `control_byte`/version/value combination
    /// `prepare_transaction_sends_xid_flags_and_modifications` leaves
    /// untested, across three modifications in one request: a key
    /// read and found to exist (control byte `0x0`, its version
    /// written, a `Put`), a key read and found absent (`NON_EXISTING`,
    /// `0x2`, no version, a `Put`), and a key read and found to exist
    /// then removed (version written even though the write is a
    /// `Remove`, `REMOVE_OP`, `0x4`, no expiration or value).
    #[tokio::test]
    async fn prepare_transaction_sends_version_non_existing_and_remove_control_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x7D, "expected a PrepareTx request");

            let _format_id = crate::varint::read_vint(&mut stream).await.unwrap();
            let _global_transaction_id = read_array(&mut stream).await.unwrap();
            let _branch_qualifier = read_array(&mut stream).await.unwrap();
            let _one_phase_commit = stream.read_u8().await.unwrap();
            let _recoverable = stream.read_u8().await.unwrap();
            let mut timeout_bytes = [0u8; 8];
            stream.read_exact(&mut timeout_bytes).await.unwrap();

            let count = crate::varint::read_vint(&mut stream).await.unwrap();
            assert_eq!(count, 3);

            let key1 = read_array(&mut stream).await.unwrap();
            assert_eq!(key1, b"key1");
            let control1 = stream.read_u8().await.unwrap();
            assert_eq!(
                control1, 0x0,
                "read and existed: neither NOT_READ nor NON_EXISTING"
            );
            let mut version1 = [0u8; 8];
            stream.read_exact(&mut version1).await.unwrap();
            assert_eq!(u64::from_be_bytes(version1), 7);
            let time_units1 = stream.read_u8().await.unwrap();
            assert_eq!(time_units1, 0x88);
            let value1 = read_array(&mut stream).await.unwrap();
            assert_eq!(value1, b"v1");

            let key2 = read_array(&mut stream).await.unwrap();
            assert_eq!(key2, b"key2");
            let control2 = stream.read_u8().await.unwrap();
            assert_eq!(control2, 0x2, "NON_EXISTING: read and did not exist");
            let time_units2 = stream.read_u8().await.unwrap();
            assert_eq!(time_units2, 0x88);
            let value2 = read_array(&mut stream).await.unwrap();
            assert_eq!(value2, b"v2");

            let key3 = read_array(&mut stream).await.unwrap();
            assert_eq!(key3, b"key3");
            let control3 = stream.read_u8().await.unwrap();
            assert_eq!(control3, 0x4, "REMOVE_OP: read and existed, then removed");
            let mut version3 = [0u8; 8];
            stream.read_exact(&mut version3).await.unwrap();
            assert_eq!(u64::from_be_bytes(version3), 9);

            let mut resp = response_header(id, 0x7E, 0x00);
            resp.extend_from_slice(&0i32.to_be_bytes()); // XA_OK
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "transactional-test")
            .await
            .expect("connect");
        let xid = crate::transaction::generate_xid();
        let modifications = vec![
            crate::transaction::Modification {
                key: b"key1".to_vec(),
                version: Some(7),
                not_read: false,
                write: crate::transaction::ModificationWrite::Put {
                    value: b"v1".to_vec(),
                    lifespan: Expiration::Immortal,
                    max_idle: Expiration::Immortal,
                },
            },
            crate::transaction::Modification {
                key: b"key2".to_vec(),
                version: None,
                not_read: false,
                write: crate::transaction::ModificationWrite::Put {
                    value: b"v2".to_vec(),
                    lifespan: Expiration::Immortal,
                    max_idle: Expiration::Immortal,
                },
            },
            crate::transaction::Modification {
                key: b"key3".to_vec(),
                version: Some(9),
                not_read: false,
                write: crate::transaction::ModificationWrite::Remove,
            },
        ];
        conn.prepare_transaction_one_phase(&xid, &modifications)
            .await
            .expect("prepare_transaction_one_phase");

        server.await.unwrap();
    }

    /// `NOT_PUT_REMOVED_REPLACED` with no body means a conflicting
    /// concurrent prepare for the same key: mapped to
    /// `Error::TransactionRolledBack` without reading an XA result
    /// code, since none was sent.
    #[tokio::test]
    async fn prepare_transaction_maps_not_executed_status_to_rolled_back() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x7D, "expected a PrepareTx request");
            let resp = response_header(id, 0x7E, 0x01); // NOT_PUT_REMOVED_REPLACED
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "transactional-test")
            .await
            .expect("connect");
        let xid = crate::transaction::generate_xid();
        let err = conn
            .prepare_transaction_one_phase(&xid, &[])
            .await
            .expect_err("expected TransactionRolledBack");
        assert!(matches!(err, Error::TransactionRolledBack));

        server.await.unwrap();
    }

    /// A successful status with an `XA_RBROLLBACK` result code is
    /// also a rollback: the Hot Rod status byte alone does not say
    /// whether the transaction actually committed, the XA result
    /// code that follows does.
    #[tokio::test]
    async fn prepare_transaction_maps_xa_rbrollback_to_rolled_back() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (id, opcode) = read_request(&mut stream).await;
            assert_eq!(opcode, 0x7D, "expected a PrepareTx request");
            let mut resp = response_header(id, 0x7E, 0x00);
            resp.extend_from_slice(&100i32.to_be_bytes()); // XA_RBROLLBACK
            stream.write_all(&resp).await.unwrap();
        });

        let mut conn = HotRodConnection::connect(addr, "transactional-test")
            .await
            .expect("connect");
        let xid = crate::transaction::generate_xid();
        let err = conn
            .prepare_transaction_one_phase(&xid, &[])
            .await
            .expect_err("expected TransactionRolledBack");
        assert!(matches!(err, Error::TransactionRolledBack));

        server.await.unwrap();
    }
}
