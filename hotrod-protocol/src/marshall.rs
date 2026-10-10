//! `Marshaller`: converts a typed value to and from the raw bytes
//! `RemoteCache` actually sends and receives
//! (`docs/adr/0012-serialization-abstraction.md`). `hotrod-protocol`
//! stays byte-oriented at its core; `TypedCache` (`typed_cache.rs`) is
//! what a caller reaches for typed values, built entirely on top of
//! this trait and the existing byte-oriented `RemoteCache`, with no
//! change to the wire protocol itself.
//!
//! Only two implementations ship here, both with no dependency beyond
//! `std`: `BytesMarshaller` and `Utf8Marshaller`. A caller who wants
//! JSON, Protobuf or anything else implements this trait themselves
//! against whatever crate they already depend on; `hotrod-protocol`
//! does not pick a serialization format for them.

use std::convert::Infallible;
use std::string::FromUtf8Error;

/// Converts a typed value to and from the raw bytes `RemoteCache`
/// sends and receives. One instance can serve as both a key and a
/// value marshaller on the same `TypedCache` when `K` and `V` share
/// the same representation (`Utf8Marshaller` for a cache of
/// `TypedCache<Utf8Marshaller, Utf8Marshaller>`, for instance).
///
/// `Value` is an associated type, not a type parameter on the trait
/// itself, so a `TypedCache<MK, MV>` only has to name the marshaller
/// types: `K`/`V` follow as `MK::Value`/`MV::Value`, the same way
/// `Iterator::Item` saves an iterator from also being generic over
/// what it yields.
pub trait Marshaller: Send + Sync {
    /// The typed value this marshaller converts.
    type Value;

    /// The error `marshall`/`unmarshall` can fail with. `TypedCache`
    /// wraps it into `Error::Marshalling`, so it only needs to
    /// implement `std::error::Error`, not this crate's own `Error`.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Converts `value` into the bytes a `RemoteCache` operation
    /// sends as a key or a value.
    fn marshall(&self, value: &Self::Value) -> Result<Vec<u8>, Self::Error>;

    /// Converts bytes a `RemoteCache` operation received back as a
    /// key or a value into a typed value.
    fn unmarshall(&self, bytes: &[u8]) -> Result<Self::Value, Self::Error>;
}

/// Passes bytes through unchanged. Mirrors the Java client's
/// `IdentityMarshaller`/`BytesOnlyMarshaller`: for a `TypedCache` that
/// only wants the typed surface's own ergonomics (an owned
/// `HashMap<Vec<u8>, Vec<u8>>` from `get_all`, for instance) without
/// actually changing representation.
#[derive(Debug, Default, Clone, Copy)]
pub struct BytesMarshaller;

impl Marshaller for BytesMarshaller {
    type Value = Vec<u8>;
    type Error = Infallible;

    fn marshall(&self, value: &Vec<u8>) -> Result<Vec<u8>, Infallible> {
        Ok(value.clone())
    }

    fn unmarshall(&self, bytes: &[u8]) -> Result<Vec<u8>, Infallible> {
        Ok(bytes.to_vec())
    }
}

/// UTF-8 text, rejecting anything that is not valid UTF-8 on the way
/// back. Mirrors the Java client's `UTF8StringMarshaller`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Utf8Marshaller;

impl Marshaller for Utf8Marshaller {
    type Value = String;
    type Error = FromUtf8Error;

    fn marshall(&self, value: &String) -> Result<Vec<u8>, FromUtf8Error> {
        Ok(value.clone().into_bytes())
    }

    fn unmarshall(&self, bytes: &[u8]) -> Result<String, FromUtf8Error> {
        String::from_utf8(bytes.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_marshaller_round_trips_unchanged() {
        let marshaller = BytesMarshaller;
        let original = vec![1u8, 2, 3, 0, 255];
        let bytes = marshaller.marshall(&original).unwrap();
        assert_eq!(bytes, original);
        assert_eq!(marshaller.unmarshall(&bytes).unwrap(), original);
    }

    #[test]
    fn utf8_marshaller_round_trips_valid_text() {
        let marshaller = Utf8Marshaller;
        let original = "héllo wörld".to_string();
        let bytes = marshaller.marshall(&original).unwrap();
        assert_eq!(bytes, original.as_bytes());
        assert_eq!(marshaller.unmarshall(&bytes).unwrap(), original);
    }

    #[test]
    fn utf8_marshaller_rejects_invalid_utf8_on_unmarshall() {
        let marshaller = Utf8Marshaller;
        let invalid = vec![0xFF, 0xFE];
        assert!(marshaller.unmarshall(&invalid).is_err());
    }
}
