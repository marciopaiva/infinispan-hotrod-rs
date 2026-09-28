//! Length-prefixed byte arrays and strings, and the two fixed-size structures
//! that ride alongside every request: the media type pair and the
//! expiration parameters. All shapes are taken from the Java client's
//! `ByteBufUtil` and `TimeUnitParam`.

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::error::Result;
use crate::varint::{read_vint, write_vint, write_vlong};

pub(crate) fn write_array(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_vint(buf, bytes.len() as u32);
    buf.extend_from_slice(bytes);
}

pub(crate) async fn read_array<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    let len = read_vint(reader).await? as usize;
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes).await?;
    Ok(bytes)
}

pub(crate) async fn read_string<R: AsyncRead + Unpin>(reader: &mut R) -> Result<String> {
    let bytes = read_array(reader).await?;
    Ok(String::from_utf8(bytes)?)
}

/// No key/value media type is negotiated in phase 1: both key and value
/// travel as opaque bytes. Wire form is a single zero byte per Codec30's
/// `writeMediaType`: type 0 ("none") carries no id and no parameters.
pub(crate) fn write_no_media_type_pair(buf: &mut Vec<u8>) {
    buf.push(0); // key media type
    buf.push(0); // value media type
}

/// Time-to-live for an entry, as passed to `put`, `putIfAbsent`, `replace`
/// and the versioned variants.
///
/// Mirrors `TimeUnitParam.encodeDuration`: a zero duration means "use the
/// server's configured default", a value carries the count in seconds.
/// Sub-second and non-second units are not exposed in phase 1; the wire
/// format supports them (see `TimeUnitParam`), but nothing in the ADR 0001
/// phase 1 scope needs them yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Expiration {
    /// Use the cache's configured default.
    #[default]
    Default,
    /// The entry never expires.
    Immortal,
    /// The entry expires after this many seconds.
    Seconds(u64),
}

impl Expiration {
    fn unit_nibble(self) -> u8 {
        match self {
            Expiration::Default => 7,
            Expiration::Immortal => 8,
            Expiration::Seconds(_) => 0, // TimeUnit.SECONDS
        }
    }

    fn value(self) -> Option<u64> {
        match self {
            Expiration::Seconds(seconds) if seconds > 0 => Some(seconds),
            _ => None,
        }
    }
}

pub(crate) fn write_expiration_params(
    buf: &mut Vec<u8>,
    lifespan: Expiration,
    max_idle: Expiration,
) {
    let time_units = (lifespan.unit_nibble() << 4) | max_idle.unit_nibble();
    buf.push(time_units);
    if let Some(seconds) = lifespan.value() {
        write_vlong(buf, seconds);
    }
    if let Some(seconds) = max_idle.value() {
        write_vlong(buf, seconds);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn roundtrip_array() {
        let mut buf = Vec::new();
        write_array(&mut buf, b"hello");
        let bytes = read_array(&mut buf.as_slice()).await.expect("read_array");
        assert_eq!(bytes, b"hello");
    }

    #[tokio::test]
    async fn roundtrip_string() {
        let mut buf = Vec::new();
        write_array(&mut buf, "cache-name".as_bytes());
        let s = read_string(&mut buf.as_slice()).await.expect("read_string");
        assert_eq!(s, "cache-name");
    }

    #[test]
    fn no_media_type_pair_is_two_zero_bytes() {
        let mut buf = Vec::new();
        write_no_media_type_pair(&mut buf);
        assert_eq!(buf, vec![0, 0]);
    }

    #[test]
    fn expiration_default_writes_only_the_flag_byte() {
        let mut buf = Vec::new();
        write_expiration_params(&mut buf, Expiration::Default, Expiration::Default);
        assert_eq!(buf, vec![0x77]);
    }

    #[test]
    fn expiration_immortal_writes_only_the_flag_byte() {
        let mut buf = Vec::new();
        write_expiration_params(&mut buf, Expiration::Immortal, Expiration::Immortal);
        assert_eq!(buf, vec![0x88]);
    }

    #[test]
    fn expiration_seconds_writes_flag_then_values() {
        let mut buf = Vec::new();
        write_expiration_params(&mut buf, Expiration::Seconds(60), Expiration::Immortal);
        assert_eq!(buf, vec![0x08, 60]);
    }
}
