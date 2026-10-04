//! vInt and vLong encoding: unsigned LEB128, matching the Java client's
//! `ByteBufUtil.writeVInt`/`writeVLong` byte for byte.
//!
//! A negative `i32` (such as the default topology id, -1) is written by
//! reinterpreting its bits as `u32` and encoding that. The Java client does
//! the same: it passes negative ints through `writeVInt(int)`, which treats
//! them as unsigned during the shift. `read_vint` reconstructs the same bit
//! pattern, so casting the result back to `i32` recovers the original value.

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::{Error, Result};

pub(crate) fn write_vint(buf: &mut Vec<u8>, mut value: u32) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            buf.push(byte | 0x80);
        } else {
            buf.push(byte);
            break;
        }
    }
}

/// ZigZag-encodes `value` before writing it as a plain `vInt`: Hot Rod's
/// `ByteBufUtil.writeSignedVInt`/`SignedNumeric.encode`. Unlike the raw
/// bit-reinterpretation `write_vint` already uses for the topology id,
/// this maps `-1` to the single byte `0x01`, not five bytes of set bits,
/// so the two must not be confused. `IterationStart`'s "no filter"/"no
/// segment list" sentinels are the only callers.
pub(crate) fn write_signed_vint(buf: &mut Vec<u8>, value: i32) {
    write_vint(buf, ((value << 1) ^ (value >> 31)) as u32);
}

pub(crate) fn write_vlong(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            buf.push(byte | 0x80);
        } else {
            buf.push(byte);
            break;
        }
    }
}

/// A vInt never needs more than 5 continuation bytes to cover 32 bits at
/// 7 bits per byte; a longer run is malformed input, not a longer number.
const MAX_VINT_BYTES: u8 = 5;

/// Same reasoning as `MAX_VINT_BYTES`, for a vLong's 64 bits.
const MAX_VLONG_BYTES: u8 = 10;

pub(crate) async fn read_vint<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u32> {
    let mut result: u32 = 0;
    let mut shift = 0u32;
    for _ in 0..MAX_VINT_BYTES {
        let byte = reader.read_u8().await?;
        result |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(Error::MalformedVarint {
        max_bytes: MAX_VINT_BYTES,
    })
}

pub(crate) async fn read_vlong<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for _ in 0..MAX_VLONG_BYTES {
        let byte = reader.read_u8().await?;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(Error::MalformedVarint {
        max_bytes: MAX_VLONG_BYTES,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_vint_matches_single_byte_range() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 0x7F);
        assert_eq!(buf, vec![0x7F]);
    }

    #[test]
    fn write_vint_matches_multi_byte_range() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 300);
        assert_eq!(buf, vec![0xAC, 0x02]);
    }

    #[test]
    fn write_vint_of_negative_one_as_u32_takes_five_bytes() {
        let mut buf = Vec::new();
        write_vint(&mut buf, (-1i32) as u32);
        assert_eq!(buf, vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    }

    #[tokio::test]
    async fn roundtrip_vint() {
        let mut buf = Vec::new();
        write_vint(&mut buf, 123_456);
        let value = read_vint(&mut buf.as_slice()).await.expect("read_vint");
        assert_eq!(value, 123_456);
    }

    #[tokio::test]
    async fn roundtrip_vint_negative_topology_id() {
        let mut buf = Vec::new();
        write_vint(&mut buf, (-1i32) as u32);
        let value = read_vint(&mut buf.as_slice()).await.expect("read_vint");
        assert_eq!(value as i32, -1);
    }

    #[test]
    fn write_signed_vint_of_negative_one_is_a_single_byte() {
        let mut buf = Vec::new();
        write_signed_vint(&mut buf, -1);
        assert_eq!(buf, vec![0x01]);
    }

    #[test]
    fn write_signed_vint_of_a_non_negative_value_doubles_it() {
        let mut buf = Vec::new();
        write_signed_vint(&mut buf, 5);
        assert_eq!(buf, vec![0x0A]);
    }

    #[tokio::test]
    async fn roundtrip_vlong() {
        let mut buf = Vec::new();
        write_vlong(&mut buf, u64::MAX);
        let value = read_vlong(&mut buf.as_slice()).await.expect("read_vlong");
        assert_eq!(value, u64::MAX);
    }

    #[tokio::test]
    async fn read_vint_rejects_a_continuation_run_longer_than_five_bytes() {
        let buf = [0x80u8; 5];
        let err = read_vint(&mut buf.as_slice())
            .await
            .expect_err("expected MalformedVarint");
        assert!(matches!(err, Error::MalformedVarint { max_bytes: 5 }));
    }

    #[tokio::test]
    async fn read_vlong_rejects_a_continuation_run_longer_than_ten_bytes() {
        let buf = [0x80u8; 10];
        let err = read_vlong(&mut buf.as_slice())
            .await
            .expect_err("expected MalformedVarint");
        assert!(matches!(err, Error::MalformedVarint { max_bytes: 10 }));
    }
}
