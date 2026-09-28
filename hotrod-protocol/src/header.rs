//! Request and response header framing for Hot Rod protocol 4.1.
//!
//! Field order and encoding are taken from `Codec30.writeHeader` (the
//! shared base, unchanged by 3.1/4.0/4.1) plus `Codec40.writeHeader`, which
//! appends the additional-parameters map introduced in protocol 4.0. Phase 1
//! never sends additional parameters, so that map is always empty.

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{Error, Result};
use crate::status::Status;
use crate::topology::{read_topology_update, ClientIntelligence, TopologyUpdate};
use crate::varint::{read_vlong, write_vint, write_vlong};
use crate::wire::{read_string, write_array, write_no_media_type_pair};

const REQUEST_MAGIC: u8 = 0xA0;
const RESPONSE_MAGIC: u8 = 0xA1;

/// Hot Rod protocol version 4.1, the version targeted by ADR 0001.
const PROTOCOL_VERSION: u8 = 41;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpCode {
    Put = 0x01,
    Get = 0x03,
    PutIfAbsent = 0x05,
    Replace = 0x07,
    ReplaceIfUnmodified = 0x09,
    Remove = 0x0B,
    RemoveIfUnmodified = 0x0D,
    GetWithMetadata = 0x1B,
    AuthMechList = 0x21,
    Auth = 0x23,
}

impl OpCode {
    /// Every response opcode observed in the Java client is the request
    /// opcode plus one.
    fn expected_response_opcode(self) -> u8 {
        self as u8 + 1
    }
}

pub(crate) fn write_request_header(
    buf: &mut Vec<u8>,
    message_id: u64,
    cache_name: &[u8],
    opcode: OpCode,
    intelligence: ClientIntelligence,
    topology_id: i32,
) {
    buf.push(REQUEST_MAGIC);
    write_vlong(buf, message_id);
    buf.push(PROTOCOL_VERSION);
    buf.push(opcode as u8);
    write_array(buf, cache_name);
    write_vint(buf, 0); // flags: none set in phase 1
    buf.push(intelligence.as_byte());
    write_vint(buf, topology_id as u32);
    write_no_media_type_pair(buf);
    write_vint(buf, 0); // additional params (protocol 4.0+): none
}

#[derive(Debug)]
pub(crate) struct ResponseHeader {
    pub status: Status,
    pub topology_update: Option<TopologyUpdate>,
}

/// Reads and validates a response header against the request that
/// triggered it, and turns a server-reported error status into `Err`
/// before returning: every error status's body is just a message string
/// (Codec30.checkForErrorsInResponseStatus), so it is safe to consume here
/// regardless of which operation sent the request.
pub(crate) async fn read_response_header<R: AsyncRead + Unpin>(
    reader: &mut R,
    request_message_id: u64,
    request_opcode: OpCode,
    intelligence: ClientIntelligence,
) -> Result<ResponseHeader> {
    let magic = reader.read_u8().await?;
    if magic != RESPONSE_MAGIC {
        return Err(Error::InvalidMagic(magic));
    }

    let message_id = read_vlong(reader).await?;
    if message_id != request_message_id {
        return Err(Error::MessageIdMismatch {
            expected: request_message_id,
            actual: message_id,
        });
    }

    let opcode = reader.read_u8().await?;
    let status = Status(reader.read_u8().await?);

    let topology_marker = reader.read_u8().await?;
    let topology_update = if topology_marker == 0 {
        None
    } else {
        match intelligence {
            // A compliant server never sends a topology update to a BASIC
            // client, so seeing one here means the server disagrees about
            // the intelligence just advertised, and the stream cannot be
            // trusted to stay in sync from this point on.
            ClientIntelligence::Basic => return Err(Error::UnsupportedTopologyUpdate),
            ClientIntelligence::HashDistributionAware => Some(read_topology_update(reader).await?),
        }
    };

    // The server reports a failure through the generic ERROR_RESPONSE opcode
    // (0x50) rather than the operation's own response opcode, so the error
    // status must be checked before the opcode is validated (Codec30's
    // checkForErrorsInResponseStatus runs ahead of HeaderDecoder's opcode
    // comparison for exactly this reason).
    if status.is_error() {
        let message = read_string(reader).await?;
        return Err(Error::Server {
            status: status.0,
            message,
        });
    }

    let expected_opcode = request_opcode.expected_response_opcode();
    if opcode != expected_opcode {
        return Err(Error::UnexpectedOpcode {
            expected: expected_opcode,
            actual: opcode,
        });
    }
    if !status.is_known() {
        return Err(Error::UnknownStatus(status.0));
    }

    Ok(ResponseHeader {
        status,
        topology_update,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::vec_init_then_push)]
    fn header_field_order_matches_codec30() {
        let mut buf = Vec::new();
        write_request_header(
            &mut buf,
            7,
            b"my-cache",
            OpCode::Get,
            ClientIntelligence::Basic,
            -1,
        );

        let mut expected = Vec::new();
        expected.push(0xA0); // magic
        expected.push(7); // message id (vLong, single byte)
        expected.push(41); // version
        expected.push(0x03); // opcode
        expected.push(8); // cache name length
        expected.extend_from_slice(b"my-cache");
        expected.push(0); // flags
        expected.push(1); // client intelligence: BASIC
        expected.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]); // topology id -1
        expected.push(0); // key media type
        expected.push(0); // value media type
        expected.push(0); // additional params count

        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn reads_success_header() {
        let mut resp = vec![0xA1]; // magic
        resp.push(7); // message id
        resp.push(0x04); // GET_RESPONSE
        resp.push(0x00); // status: success
        resp.push(0x00); // no topology change

        let header = read_response_header(
            &mut resp.as_slice(),
            7,
            OpCode::Get,
            ClientIntelligence::Basic,
        )
        .await
        .expect("read_response_header");
        assert!(header.status.is_success());
        assert!(header.topology_update.is_none());
    }

    #[tokio::test]
    async fn basic_client_rejects_topology_update() {
        let resp = vec![0xA1, 7, 0x04, 0x00, 0x01]; // topology marker set

        let err = read_response_header(
            &mut resp.as_slice(),
            7,
            OpCode::Get,
            ClientIntelligence::Basic,
        )
        .await
        .expect_err("expected UnsupportedTopologyUpdate");
        assert!(matches!(err, Error::UnsupportedTopologyUpdate));
    }

    #[tokio::test]
    async fn hash_distribution_aware_client_parses_topology_update() {
        let mut resp = vec![0xA1, 7, 0x04, 0x00, 0x01]; // topology marker set
        write_vint(&mut resp, 9); // topology id
        write_vint(&mut resp, 1); // num servers
        write_array(&mut resp, b"node1");
        resp.extend_from_slice(&7000u16.to_be_bytes());
        resp.push(3); // hash function version
        write_vint(&mut resp, 0); // num segments

        let header = read_response_header(
            &mut resp.as_slice(),
            7,
            OpCode::Get,
            ClientIntelligence::HashDistributionAware,
        )
        .await
        .expect("read_response_header");
        assert!(header.status.is_success());
        let update = header.topology_update.expect("topology update");
        assert_eq!(update.topology_id, 9);
        assert_eq!(update.servers.len(), 1);
        assert_eq!(update.servers[0].host, "node1");
        assert_eq!(update.servers[0].port, 7000);
        assert_eq!(update.hash_function_version, 3);
        assert!(update.segment_owners.is_empty());
    }

    #[tokio::test]
    async fn maps_server_error_status_to_err() {
        let mut resp = vec![0xA1, 7, 0x04, 0x85, 0x00];
        write_array(&mut resp, b"boom");

        let err = read_response_header(
            &mut resp.as_slice(),
            7,
            OpCode::Get,
            ClientIntelligence::Basic,
        )
        .await
        .expect_err("expected a Server error");
        match err {
            Error::Server { status, message } => {
                assert_eq!(status, 0x85);
                assert_eq!(message, "boom");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    /// The server reports failures through the generic ERROR_RESPONSE opcode
    /// (0x50), not the operation's own response opcode. Found by testing
    /// against a live server: an earlier version of this function checked
    /// the opcode before the status, so this case surfaced as a confusing
    /// `UnexpectedOpcode` instead of the actual server error.
    #[tokio::test]
    async fn maps_generic_error_response_opcode_to_server_error() {
        let mut resp = vec![0xA1, 7, 0x50, 0x84, 0x00];
        write_array(&mut resp, b"parse error");

        let err = read_response_header(
            &mut resp.as_slice(),
            7,
            OpCode::Put,
            ClientIntelligence::Basic,
        )
        .await
        .expect_err("expected a Server error");
        match err {
            Error::Server { status, message } => {
                assert_eq!(status, 0x84);
                assert_eq!(message, "parse error");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
