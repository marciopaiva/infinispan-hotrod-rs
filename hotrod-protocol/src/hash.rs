//! Infinispan's own `MurmurHash3` variant and Hot Rod segment computation.
//!
//! This is not the canonical MurmurHash3. `org.infinispan.commons.hash.MurmurHash3`
//! is ported from a pre-release draft of the algorithm (circa late 2010): h1/h2
//! are seeded by XORing with fixed constants rather than the seed itself, c1/c2
//! mutate every round, the rotation amounts are 23/23/41 instead of the final
//! algorithm's 31/33/27/31, and the h1/h2 update multiplies by 3 instead of 5.
//! Infinispan's own docs call this permanent: the hash function is part of the
//! wire-level consistent hashing protocol, and changing it would break data
//! distribution with every existing cluster.
//!
//! The tail byte handling also has a quirk worth flagging explicitly, because
//! it is easy to "fix" by accident when porting: each tail byte is sign
//! extended before being XORed into `k1`/`k2` (a plain Java `byte` is signed,
//! and widening it to `long` sign extends), not zero extended as an unsigned
//! byte would be. Both forms behave identically once shifted since the
//! extended bits are discarded, except for the top byte of each of `k1` and
//! `k2` (tail lengths of 8 and, if a 9th+ byte is present, 15), where the
//! extended bits land inside the 64 bit word instead of being shifted out.
//! Getting this wrong would not fail loudly: a Hot Rod server always still
//! serves a request sent to the wrong node, just less efficiently, so a
//! subtly wrong hash silently misroutes instead of erroring. `sext` below
//! exists specifically to replicate this, and the constants and vectors in
//! this file's tests are cross validated against real Java output captured
//! from this exact class (see `docs/adr/0003-hash-aware-routing-scope.md`),
//! not derived from memory or from the canonical MurmurHash3 reference.

// Not wired up yet: `segment` starts being called once `cluster.rs` lands
// later in this phase (see docs/adr/0003-hash-aware-routing-scope.md).
#![allow(dead_code)]

use crate::error::{Error, Result};

const H1_SEED_XOR: u64 = 0x9368e53c2f6af274;
const H2_SEED_XOR: u64 = 0x586dcd208f7cd3fd;
const C1_INIT: u64 = 0x87c37b91114253d5;
const C2_INIT: u64 = 0x4cf5ad432745937f;

struct State {
    h1: u64,
    h2: u64,
    k1: u64,
    k2: u64,
    c1: u64,
    c2: u64,
}

fn bmix(s: &mut State) {
    s.k1 = s.k1.wrapping_mul(s.c1).rotate_left(23).wrapping_mul(s.c2);
    s.h1 ^= s.k1;
    s.h1 = s.h1.wrapping_add(s.h2);

    s.h2 = s.h2.rotate_left(41);

    s.k2 = s.k2.wrapping_mul(s.c2).rotate_left(23).wrapping_mul(s.c1);
    s.h2 ^= s.k2;
    s.h2 = s.h2.wrapping_add(s.h1);

    s.h1 = s.h1.wrapping_mul(3).wrapping_add(0x52dce729);
    s.h2 = s.h2.wrapping_mul(3).wrapping_add(0x38495ab5);

    s.c1 = s.c1.wrapping_mul(5).wrapping_add(0x7b7d159c);
    s.c2 = s.c2.wrapping_mul(5).wrapping_add(0x6bce6396);
}

fn fmix(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    k
}

fn get_block(key: &[u8], offset: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key[offset..offset + 8]);
    u64::from_le_bytes(bytes)
}

/// Widens a byte the way a Java `long ^= byte` does: sign extended, not zero
/// extended. See this module's doc comment for why that distinction matters.
fn sext(b: u8) -> u64 {
    (b as i8 as i64) as u64
}

fn murmur_hash3_x64_64(key: &[u8], seed: i32) -> u64 {
    let seed = (seed as i64) as u64;
    let mut state = State {
        h1: H1_SEED_XOR ^ seed,
        h2: H2_SEED_XOR ^ seed,
        k1: 0,
        k2: 0,
        c1: C1_INIT,
        c2: C2_INIT,
    };

    let nblocks = key.len() / 16;
    for i in 0..nblocks {
        state.k1 = get_block(key, i * 16);
        state.k2 = get_block(key, i * 16 + 8);
        bmix(&mut state);
    }

    let tail = nblocks * 16;
    let tail_len = key.len() & 15;
    if tail_len > 0 {
        state.k1 = 0;
        state.k2 = 0;

        if tail_len >= 15 {
            state.k2 ^= sext(key[tail + 14]) << 48;
        }
        if tail_len >= 14 {
            state.k2 ^= sext(key[tail + 13]) << 40;
        }
        if tail_len >= 13 {
            state.k2 ^= sext(key[tail + 12]) << 32;
        }
        if tail_len >= 12 {
            state.k2 ^= sext(key[tail + 11]) << 24;
        }
        if tail_len >= 11 {
            state.k2 ^= sext(key[tail + 10]) << 16;
        }
        if tail_len >= 10 {
            state.k2 ^= sext(key[tail + 9]) << 8;
        }
        if tail_len >= 9 {
            state.k2 ^= sext(key[tail + 8]);
        }
        if tail_len >= 8 {
            state.k1 ^= sext(key[tail + 7]) << 56;
        }
        if tail_len >= 7 {
            state.k1 ^= sext(key[tail + 6]) << 48;
        }
        if tail_len >= 6 {
            state.k1 ^= sext(key[tail + 5]) << 40;
        }
        if tail_len >= 5 {
            state.k1 ^= sext(key[tail + 4]) << 32;
        }
        if tail_len >= 4 {
            state.k1 ^= sext(key[tail + 3]) << 24;
        }
        if tail_len >= 3 {
            state.k1 ^= sext(key[tail + 2]) << 16;
        }
        if tail_len >= 2 {
            state.k1 ^= sext(key[tail + 1]) << 8;
        }
        state.k1 ^= sext(key[tail]);
        bmix(&mut state);
    }

    state.h2 ^= key.len() as u64;

    state.h1 = state.h1.wrapping_add(state.h2);
    state.h2 = state.h2.wrapping_add(state.h1);

    state.h1 = fmix(state.h1);
    state.h2 = fmix(state.h2);

    state.h1 = state.h1.wrapping_add(state.h2);
    state.h2 = state.h2.wrapping_add(state.h1);

    state.h1
}

/// Mirrors `MurmurHash3.hash(byte[])`: the fixed seed `9001`, truncated to
/// the upper 32 bits of the x64 64 bit variant.
fn murmur_hash3(key: &[u8]) -> i32 {
    (murmur_hash3_x64_64(key, 9001) >> 32) as i32
}

/// The Hot Rod hash function version this client implements. Mirrors
/// `MurmurHash3` as used by `SyncConsistentHashFactory` since Infinispan 7.0,
/// the current default.
pub(crate) const SUPPORTED_HASH_FUNCTION_VERSION: u8 = 3;

/// Maps a key to its owning segment, the way `KeyPartitioner.getSegment`
/// does: `(hash & Integer.MAX_VALUE) % numSegments`.
///
/// Only hash function version 3 is implemented. An unrecognized version is
/// reported rather than silently guessed, since a wrong hash function would
/// misroute every key computed with it.
pub(crate) fn segment(key: &[u8], num_segments: u32, hash_function_version: u8) -> Result<u32> {
    if hash_function_version != SUPPORTED_HASH_FUNCTION_VERSION {
        return Err(Error::UnsupportedHashFunctionVersion(hash_function_version));
    }
    let hash = (murmur_hash3(key) as u32) & 0x7FFF_FFFF;
    Ok(hash % num_segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Captured from a throwaway Java harness running the exact algorithm
    // copied from org.infinispan.commons.hash.MurmurHash3 (Infinispan main
    // branch), not hand derived. See docs/adr/0003-hash-aware-routing-scope.md.
    const VECTORS: &[(&str, i32, u32)] = &[
        ("", 89125410, 34),
        ("a", -1119243492, 28),
        ("ab", -2035972426, 182),
        ("abc", 1809745788, 124),
        ("hello", 1671093224, 232),
        ("hello-world", -1451473778, 142),
        ("key", -1316802850, 222),
        ("key1", -1758694083, 61),
        ("key2", -2048737143, 137),
        (
            "the quick brown fox jumps over the lazy dog",
            1670566563,
            163,
        ),
        ("0123456789012345", 1137680597, 213), // 16 bytes: one full block, no tail
        ("01234567890123456", 1107742961, 241), // 17 bytes: one block + 1 tail byte
        ("012345678901234567890123456789012", -317775152, 208), // 33 bytes: two blocks + 1 tail byte
    ];

    #[test]
    fn matches_real_java_hash_output() {
        for (input, expected_hash, _) in VECTORS {
            let actual = murmur_hash3(input.as_bytes());
            assert_eq!(actual, *expected_hash, "hash mismatch for {input:?}");
        }
    }

    #[test]
    fn matches_real_java_segment_output() {
        for (input, _, expected_segment) in VECTORS {
            let actual = segment(input.as_bytes(), 256, SUPPORTED_HASH_FUNCTION_VERSION)
                .expect("supported hash function version");
            assert_eq!(actual, *expected_segment, "segment mismatch for {input:?}");
        }
    }

    #[test]
    fn unsupported_hash_function_version_is_rejected() {
        let err = segment(b"key", 256, 1).unwrap_err();
        assert!(matches!(err, Error::UnsupportedHashFunctionVersion(1)));
    }
}
