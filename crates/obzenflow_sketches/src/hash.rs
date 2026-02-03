// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stable hashing utilities for sketches.
//!
//! Determinism constraints:
//! - no per-process random seeding
//! - stable across runs (and preferably across platforms)
//! - explicit `seed` is part of configuration and snapshots

/// Deterministic 64-bit hash for sketch semantics.
///
/// Notes:
/// - This is not a cryptographic hash.
/// - The exact algorithm is considered part of the sketch ABI: changing it
///   changes sketch results and would break replay equivalence. Prefer adding a
///   new algorithm/version rather than “upgrading” in-place.
pub fn stable_hash64(seed: u64, bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS ^ seed;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    fmix64(hash)
}

#[inline]
fn fmix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_hash64_is_deterministic() {
        let seed = 0xdead_beef_u64;
        let bytes = b"obzenflow";
        assert_eq!(stable_hash64(seed, bytes), stable_hash64(seed, bytes));
    }

    #[test]
    fn stable_hash64_has_golden_vectors() {
        assert_eq!(stable_hash64(0, b""), 0xefd01f60ba992926);
        assert_eq!(stable_hash64(0, b"obzenflow"), 0xfa063b03c448eb2d);
        assert_eq!(stable_hash64(0xdead_beef, b"obzenflow"), 0xcccd99b70edff180);
    }

    #[test]
    fn stable_hash64_respects_seed() {
        let bytes = b"same-input";
        assert_ne!(stable_hash64(1, bytes), stable_hash64(2, bytes));
    }
}
