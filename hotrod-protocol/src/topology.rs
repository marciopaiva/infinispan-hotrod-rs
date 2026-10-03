//! Cluster topology and hash-aware routing types, and the parser for the
//! topology payload a Hot Rod server sends after a response header whose
//! topology marker byte is `1`.
//!
//! Field order and encoding are taken from `Encoder2x.writeTopologyUpdate`
//! and `Encoder2x.writeHashTopologyUpdate` (`server/hotrod`, Infinispan main
//! branch): topology id and server/segment counts are vInts
//! (`ExtendedByteBuf.writeUnsignedInt`), a server's host is a vInt-prefixed
//! string and its port is a raw big-endian `u16`
//! (`ExtendedByteBuf.writeUnsignedShort` calls `ByteBuf.writeShort`, which is
//! big-endian unless the buffer says otherwise, and Hot Rod's buffers never
//! do), and the hash function version and each segment's owner count are raw
//! single bytes.

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{Error, Result};
use crate::varint::read_vint;
use crate::wire::read_string;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClientIntelligence {
    /// No cluster or hash information. What `HotRodConnection` always sends.
    Basic,
    /// Cluster membership plus per-segment hash ownership. What
    /// `HotRodClient`'s per-node connections send.
    HashDistributionAware,
}

impl ClientIntelligence {
    pub(crate) fn as_byte(self) -> u8 {
        match self {
            ClientIntelligence::Basic => 1,
            ClientIntelligence::HashDistributionAware => 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopologyServer {
    pub host: String,
    pub port: u16,
}

/// A cluster topology and, when hash-aware, the per-segment ownership that
/// goes with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TopologyUpdate {
    pub topology_id: u32,
    pub servers: Vec<TopologyServer>,
    pub hash_function_version: u8,
    /// Index `n` is segment `n`'s owners, as indices into `servers`, primary
    /// owner first.
    pub segment_owners: Vec<Vec<u32>>,
}

/// Safety ceiling on a declared item count (servers or segments), checked
/// before allocating for it. Not a protocol limit: no real cluster comes
/// anywhere near this many nodes or segments. It exists so a corrupted or
/// hostile count gets a typed error instead of an allocation big enough to
/// abort the process.
const MAX_TOPOLOGY_COUNT: u32 = 1_000_000;

fn check_topology_count(what: &'static str, declared: u32) -> Result<()> {
    if declared > MAX_TOPOLOGY_COUNT {
        return Err(Error::DeclaredLengthTooLarge {
            what,
            declared,
            max: MAX_TOPOLOGY_COUNT,
        });
    }
    Ok(())
}

pub(crate) async fn read_topology_update<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<TopologyUpdate> {
    let topology_id = read_vint(reader).await?;

    let num_servers = read_vint(reader).await?;
    check_topology_count("a server count", num_servers)?;
    let mut servers = Vec::with_capacity(num_servers as usize);
    for _ in 0..num_servers {
        let host = read_string(reader).await?;
        let port = reader.read_u16().await?;
        servers.push(TopologyServer { host, port });
    }

    let hash_function_version = reader.read_u8().await?;

    let num_segments = read_vint(reader).await?;
    check_topology_count("a segment count", num_segments)?;
    let mut segment_owners = Vec::with_capacity(num_segments as usize);
    for _ in 0..num_segments {
        let num_owners = reader.read_u8().await?;
        let mut owners = Vec::with_capacity(num_owners as usize);
        for _ in 0..num_owners {
            owners.push(read_vint(reader).await?);
        }
        segment_owners.push(owners);
    }

    for owners in &segment_owners {
        for &index in owners {
            if index as usize >= servers.len() {
                return Err(Error::InvalidTopologyOwnerIndex {
                    index,
                    num_servers: servers.len(),
                });
            }
        }
    }

    Ok(TopologyUpdate {
        topology_id,
        servers,
        hash_function_version,
        segment_owners,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::varint::write_vint;
    use crate::wire::write_array;

    fn write_topology_update_payload(buf: &mut Vec<u8>) {
        write_vint(buf, 5); // topology id
        write_vint(buf, 2); // num servers
        write_array(buf, b"node1");
        buf.extend_from_slice(&7000u16.to_be_bytes());
        write_array(buf, b"node2");
        buf.extend_from_slice(&7001u16.to_be_bytes());
        buf.push(3); // hash function version
        write_vint(buf, 2); // num segments
        buf.push(1); // segment 0: one owner
        write_vint(buf, 0);
        buf.push(2); // segment 1: two owners
        write_vint(buf, 0);
        write_vint(buf, 1);
    }

    #[tokio::test]
    async fn parses_servers_hash_version_and_segment_owners() {
        let mut buf = Vec::new();
        write_topology_update_payload(&mut buf);

        let update = read_topology_update(&mut buf.as_slice())
            .await
            .expect("read_topology_update");

        assert_eq!(update.topology_id, 5);
        assert_eq!(
            update.servers,
            vec![
                TopologyServer {
                    host: "node1".to_string(),
                    port: 7000
                },
                TopologyServer {
                    host: "node2".to_string(),
                    port: 7001
                },
            ]
        );
        assert_eq!(update.hash_function_version, 3);
        assert_eq!(update.segment_owners, vec![vec![0], vec![0, 1]]);
    }

    #[tokio::test]
    async fn rejects_owner_index_out_of_range_for_the_server_list() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 1); // topology id
        write_vint(&mut buf, 1); // num servers
        write_array(&mut buf, b"node1");
        buf.extend_from_slice(&7000u16.to_be_bytes());
        buf.push(3); // hash function version
        write_vint(&mut buf, 1); // num segments
        buf.push(1); // segment 0: one owner
        write_vint(&mut buf, 1); // owner index 1, but only server 0 exists

        let result = read_topology_update(&mut buf.as_slice()).await;

        assert!(matches!(
            result,
            Err(Error::InvalidTopologyOwnerIndex {
                index: 1,
                num_servers: 1
            })
        ));
    }

    #[tokio::test]
    async fn rejects_a_server_count_over_the_safety_limit() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 1); // topology id
        write_vint(&mut buf, MAX_TOPOLOGY_COUNT + 1); // num servers

        let result = read_topology_update(&mut buf.as_slice()).await;

        assert!(matches!(
            result,
            Err(Error::DeclaredLengthTooLarge {
                declared,
                max: MAX_TOPOLOGY_COUNT,
                ..
            }) if declared == MAX_TOPOLOGY_COUNT + 1
        ));
    }

    #[tokio::test]
    async fn rejects_a_segment_count_over_the_safety_limit() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 1); // topology id
        write_vint(&mut buf, 1); // num servers
        write_array(&mut buf, b"node1");
        buf.extend_from_slice(&7000u16.to_be_bytes());
        buf.push(3); // hash function version
        write_vint(&mut buf, MAX_TOPOLOGY_COUNT + 1); // num segments

        let result = read_topology_update(&mut buf.as_slice()).await;

        assert!(matches!(
            result,
            Err(Error::DeclaredLengthTooLarge {
                declared,
                max: MAX_TOPOLOGY_COUNT,
                ..
            }) if declared == MAX_TOPOLOGY_COUNT + 1
        ));
    }

    #[test]
    fn client_intelligence_bytes_match_hot_rod_protocol_constants() {
        assert_eq!(ClientIntelligence::Basic.as_byte(), 1);
        assert_eq!(ClientIntelligence::HashDistributionAware.as_byte(), 3);
    }
}
