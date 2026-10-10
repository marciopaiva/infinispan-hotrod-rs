//! Generic Protocol Buffers wire-format primitives: tags, varints,
//! zigzag, fixed32/fixed64 and length-delimited framing
//! (`docs/adr/0013-remote-query.md`). Used only to speak the small,
//! fixed envelope the query operation needs (`query.rs`'s
//! `QueryRequest`/`QueryResponse`/`WrappedMessage`), never to
//! interpret a caller's own domain schema.
//!
//! Everything here parses a fully-buffered `&[u8]` with an explicit
//! cursor, not `AsyncRead`: unlike every other wire format in this
//! crate, a query request/response body is always read whole first
//! (`wire::read_array`/`write_array` already handle the outer Hot Rod
//! framing), so parsing it is plain, synchronous, in-memory work, not
//! network I/O.
//!
//! The varint and zigzag encodings below happen to be byte-for-byte
//! identical to `varint.rs`'s own vInt/SignedVInt (both are the
//! standard Protocol Buffers LEB128 scheme), but are written again
//! here rather than shared: `varint.rs` streams directly off a
//! connection, while this module only ever sees an already-buffered
//! slice, and the two protocols (Hot Rod's own framing vs. the
//! Protobuf envelope it happens to carry for this one operation) are
//! only coincidentally related, not meant to stay coupled.

use crate::error::{Error, Result};

pub(crate) const WIRE_TYPE_VARINT: u8 = 0;
pub(crate) const WIRE_TYPE_FIXED64: u8 = 1;
pub(crate) const WIRE_TYPE_LENGTH_DELIMITED: u8 = 2;
pub(crate) const WIRE_TYPE_FIXED32: u8 = 5;

/// A varint never needs more than 10 continuation bytes to cover 64
/// bits at 7 bits per byte; a longer run is malformed input, not a
/// longer number. Same bound `varint.rs` uses for its own vLong.
const MAX_VARINT_BYTES: u8 = 10;

/// Safety ceiling on a declared length-delimited field's length,
/// checked before allocating for it, same reasoning as
/// `wire::MAX_ARRAY_LEN`.
const MAX_LENGTH_DELIMITED_LEN: u64 = 64 * 1024 * 1024;

pub(crate) fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
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

pub(crate) fn read_varint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for _ in 0..MAX_VARINT_BYTES {
        let byte = *buf.get(*pos).ok_or(Error::MalformedVarint {
            max_bytes: MAX_VARINT_BYTES,
        })?;
        *pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(Error::MalformedVarint {
        max_bytes: MAX_VARINT_BYTES,
    })
}

/// Protobuf's plain `int32`: unlike `sint32` (`zigzag_encode`), a
/// negative value is sign-extended to 64 bits before being written as
/// a varint, so it always takes the full 10 bytes on the wire. A
/// quirk of the format, not a choice made here: `QueryRequest`'s
/// `startOffset`/`maxResults`/`hitCountAccuracy` are declared exactly
/// this way (confirmed against the generated `proto.lock` schema, not
/// guessed), not as `sint32`/`sint64`.
pub(crate) fn write_int32(buf: &mut Vec<u8>, value: i32) {
    write_varint(buf, (value as i64) as u64);
}

pub(crate) fn read_int32(buf: &[u8], pos: &mut usize) -> Result<i32> {
    Ok(read_varint(buf, pos)? as i64 as i32)
}

pub(crate) fn write_int64(buf: &mut Vec<u8>, value: i64) {
    write_varint(buf, value as u64);
}

pub(crate) fn read_int64(buf: &[u8], pos: &mut usize) -> Result<i64> {
    Ok(read_varint(buf, pos)? as i64)
}

/// A field number and wire type, decoded from one leading varint:
/// `field << 3 | wire_type`. `field` fits in 29 bits per the Protobuf
/// spec (field numbers are capped well below `u32::MAX`), so the
/// shift below never loses bits that `field`'s own range would need.
pub(crate) fn write_tag(buf: &mut Vec<u8>, field: u32, wire_type: u8) {
    write_varint(buf, (u64::from(field) << 3) | u64::from(wire_type));
}

/// `None` exactly at a clean end of the buffer (no more fields to
/// read); any other short read is a real `Error::MalformedVarint`,
/// not a graceful end.
pub(crate) fn read_tag(buf: &[u8], pos: &mut usize) -> Result<Option<(u32, u8)>> {
    if *pos >= buf.len() {
        return Ok(None);
    }
    let tag = read_varint(buf, pos)?;
    Ok(Some(((tag >> 3) as u32, (tag & 0x7) as u8)))
}

/// Protobuf's zigzag: maps small-magnitude signed values to
/// small-magnitude unsigned ones (`-1 -> 1`, `1 -> 2`, `-2 -> 3`, ...)
/// so they still encode as a short varint, the same formula
/// `varint.rs::write_signed_vint` already uses for Hot Rod's own
/// `SignedVInt`, just over 64 bits here instead of 32.
///
/// Not called anywhere yet: `WrappedMessage`'s `sint32`/`sint64`
/// fields (`docs/adr/0013-remote-query.md`'s "left out of this phase"
/// list) are the only place this would apply, and `QueryValue` does
/// not expose them in v1. Kept, tested, ready for when that changes,
/// rather than rediscovering the same formula then.
#[allow(dead_code)]
pub(crate) fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

#[allow(dead_code)]
pub(crate) fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

pub(crate) fn write_fixed32(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn read_fixed32(buf: &[u8], pos: &mut usize) -> Result<u32> {
    let bytes: [u8; 4] = buf
        .get(*pos..*pos + 4)
        .ok_or_else(|| Error::MalformedQueryResponse("truncated fixed32 field".to_string()))?
        .try_into()
        .expect("slice of length 4");
    *pos += 4;
    Ok(u32::from_le_bytes(bytes))
}

pub(crate) fn write_fixed64(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn read_fixed64(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let bytes: [u8; 8] = buf
        .get(*pos..*pos + 8)
        .ok_or_else(|| Error::MalformedQueryResponse("truncated fixed64 field".to_string()))?
        .try_into()
        .expect("slice of length 8");
    *pos += 8;
    Ok(u64::from_le_bytes(bytes))
}

pub(crate) fn write_length_delimited(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_varint(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

pub(crate) fn read_length_delimited(buf: &[u8], pos: &mut usize) -> Result<Vec<u8>> {
    let len = read_varint(buf, pos)?;
    if len > MAX_LENGTH_DELIMITED_LEN {
        return Err(Error::DeclaredLengthTooLarge {
            what: "a Protobuf length-delimited field",
            declared: len as u32,
            max: MAX_LENGTH_DELIMITED_LEN as u32,
        });
    }
    let end = *pos + len as usize;
    let bytes = buf
        .get(*pos..end)
        .ok_or_else(|| {
            Error::MalformedQueryResponse("truncated length-delimited field".to_string())
        })?
        .to_vec();
    *pos = end;
    Ok(bytes)
}

/// Skips one field's value, having already read its wire type from
/// `read_tag`: used to tolerate an unknown field in a message this
/// crate only partially understands, the same forward-compatible
/// stance a real Protobuf implementation takes.
pub(crate) fn skip_field(buf: &[u8], pos: &mut usize, wire_type: u8) -> Result<()> {
    match wire_type {
        WIRE_TYPE_VARINT => {
            read_varint(buf, pos)?;
        }
        WIRE_TYPE_FIXED64 => {
            read_fixed64(buf, pos)?;
        }
        WIRE_TYPE_LENGTH_DELIMITED => {
            read_length_delimited(buf, pos)?;
        }
        WIRE_TYPE_FIXED32 => {
            read_fixed32(buf, pos)?;
        }
        other => {
            return Err(Error::MalformedQueryResponse(format!(
                "unknown Protobuf wire type {other}"
            )))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trips_small_and_large_values() {
        for value in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, value);
            let mut pos = 0;
            assert_eq!(read_varint(&buf, &mut pos).unwrap(), value);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn read_varint_rejects_an_unterminated_continuation_run() {
        let buf = vec![0x80; 11];
        let mut pos = 0;
        assert!(matches!(
            read_varint(&buf, &mut pos),
            Err(Error::MalformedVarint { .. })
        ));
    }

    #[test]
    fn read_varint_rejects_running_past_the_end_of_the_buffer() {
        let buf = vec![0x80, 0x80];
        let mut pos = 0;
        assert!(matches!(
            read_varint(&buf, &mut pos),
            Err(Error::MalformedVarint { .. })
        ));
    }

    #[test]
    fn tag_round_trips_field_and_wire_type() {
        let mut buf = Vec::new();
        write_tag(&mut buf, 19, WIRE_TYPE_VARINT);
        let mut pos = 0;
        assert_eq!(
            read_tag(&buf, &mut pos).unwrap(),
            Some((19, WIRE_TYPE_VARINT))
        );
    }

    #[test]
    fn read_tag_returns_none_at_a_clean_end_of_buffer() {
        let buf = Vec::new();
        let mut pos = 0;
        assert_eq!(read_tag(&buf, &mut pos).unwrap(), None);
    }

    #[test]
    fn int32_and_int64_round_trip_negative_values_as_a_full_width_varint() {
        let mut buf = Vec::new();
        write_int32(&mut buf, -1);
        // Sign-extended to 64 bits before varint-encoding, so a
        // negative int32 always takes the full 10 bytes, unlike a
        // zigzag-encoded sint32 (which would take 1 byte for -1).
        assert_eq!(buf.len(), 10);
        let mut pos = 0;
        assert_eq!(read_int32(&buf, &mut pos).unwrap(), -1);

        let mut buf = Vec::new();
        write_int64(&mut buf, -1);
        assert_eq!(buf.len(), 10);
        let mut pos = 0;
        assert_eq!(read_int64(&buf, &mut pos).unwrap(), -1);

        let mut buf = Vec::new();
        write_int32(&mut buf, 42);
        let mut pos = 0;
        assert_eq!(read_int32(&buf, &mut pos).unwrap(), 42);
    }

    #[test]
    fn zigzag_round_trips_negative_and_positive_values() {
        for value in [0i64, 1, -1, 2, -2, i64::MAX, i64::MIN] {
            assert_eq!(zigzag_decode(zigzag_encode(value)), value);
        }
        // Matches the documented small-magnitude mapping exactly.
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
    }

    #[test]
    fn fixed32_and_fixed64_round_trip_little_endian() {
        let mut buf = Vec::new();
        write_fixed32(&mut buf, 0x01020304);
        write_fixed64(&mut buf, 0x0102030405060708);
        let mut pos = 0;
        assert_eq!(read_fixed32(&buf, &mut pos).unwrap(), 0x01020304);
        assert_eq!(read_fixed64(&buf, &mut pos).unwrap(), 0x0102030405060708);
    }

    #[test]
    fn length_delimited_round_trips_bytes() {
        let mut buf = Vec::new();
        write_length_delimited(&mut buf, b"hello");
        let mut pos = 0;
        assert_eq!(read_length_delimited(&buf, &mut pos).unwrap(), b"hello");
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn read_length_delimited_rejects_a_length_over_the_safety_limit() {
        let mut buf = Vec::new();
        write_varint(&mut buf, MAX_LENGTH_DELIMITED_LEN + 1);
        let mut pos = 0;
        assert!(matches!(
            read_length_delimited(&buf, &mut pos),
            Err(Error::DeclaredLengthTooLarge { .. })
        ));
    }

    #[test]
    fn skip_field_advances_past_each_wire_type() {
        let mut buf = Vec::new();
        write_varint(&mut buf, 42);
        write_fixed64(&mut buf, 1);
        write_length_delimited(&mut buf, b"abc");
        write_fixed32(&mut buf, 1);

        let mut pos = 0;
        skip_field(&buf, &mut pos, WIRE_TYPE_VARINT).unwrap();
        skip_field(&buf, &mut pos, WIRE_TYPE_FIXED64).unwrap();
        skip_field(&buf, &mut pos, WIRE_TYPE_LENGTH_DELIMITED).unwrap();
        skip_field(&buf, &mut pos, WIRE_TYPE_FIXED32).unwrap();
        assert_eq!(pos, buf.len());
    }
}
