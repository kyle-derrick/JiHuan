use digest::Digest;
use md5::Md5;
use sha1::Sha1;
use sha2::Sha256;

use crate::config::HashAlgorithm;
use crate::utils::to_hex;

/// v0.4.8: canonical in-memory / on-wire representation of a chunk hash.
///
/// We standardise on 32 raw bytes to fit SHA-256 (the default and
/// recommended algorithm) exactly, while still accommodating the legacy
/// MD5 (16 B) and SHA-1 (20 B) options by left-aligning the raw digest
/// and zero-padding the remainder. The padding costs 12-16 wasted bytes
/// per chunk under the legacy algorithms, but they are rarely used in
/// practice — the upside of a fixed-size representation is that it
/// serialises as exactly 32 bytes under bincode (vs 65 bytes for the
/// hex-string form: 1-byte length prefix + 64 ASCII chars), saving
/// ~32 B per `ChunkMeta`.
pub const HASH_BYTES: usize = 32;

/// Alias for a fixed-size chunk hash. Callers that need `Option` semantics
/// (to represent "dedup disabled, no hash computed") use
/// `Option<HashBytes>`; places that only ever run with dedup on (e.g.
/// `DedupEntry.hash`) use the bare form.
pub type HashBytes = [u8; HASH_BYTES];

/// Format a `HashBytes` for logging / human consumption.
///
/// Returns the full 64-character lowercase hex representation. Callers
/// that work with legacy MD5/SHA-1 algorithms are expected to know the
/// active algorithm and trim the trailing zero-padding themselves if
/// they want to surface just the meaningful prefix.
#[inline]
pub fn hash_to_hex(h: &HashBytes) -> String {
    to_hex(h)
}

/// Parse a block-file `ChunkEntry`'s hex-ASCII hash back into a
/// `HashBytes`. Short digests (MD5/SHA-1) are left-aligned with the
/// trailing bytes zero-filled, matching the on-wire convention used by
/// [`hash_chunk_bytes`]. Returns `None` for an empty input (the
/// `HashAlgorithm::None` case) or when the hex string is malformed.
pub fn hash_from_hex(hex_str: &str) -> Option<HashBytes> {
    if hex_str.is_empty() {
        return None;
    }
    let raw = hex::decode(hex_str).ok()?;
    if raw.len() > HASH_BYTES {
        return None;
    }
    let mut out = [0u8; HASH_BYTES];
    out[..raw.len()].copy_from_slice(&raw);
    Some(out)
}

/// Compute a content hash as a fixed-size byte array, left-aligned and
/// zero-padded when the algorithm's native digest is shorter than 32 B.
/// Returns `None` when dedup is disabled (`HashAlgorithm::None`).
pub fn hash_chunk_bytes(data: &[u8], algo: HashAlgorithm) -> Option<HashBytes> {
    let mut out = [0u8; HASH_BYTES];
    match algo {
        HashAlgorithm::None => return None,
        HashAlgorithm::Md5 => {
            let mut h = Md5::new();
            h.update(data);
            let d = h.finalize();
            out[..d.len()].copy_from_slice(&d);
        }
        HashAlgorithm::Sha1 => {
            let mut h = Sha1::new();
            h.update(data);
            let d = h.finalize();
            out[..d.len()].copy_from_slice(&d);
        }
        HashAlgorithm::Sha256 => {
            let mut h = Sha256::new();
            h.update(data);
            let d = h.finalize();
            // d.len() == 32, so this copies the full array
            out.copy_from_slice(&d);
        }
    }
    Some(out)
}

/// Compute a content hash for the given data using the specified algorithm.
/// Returns an empty string when the algorithm is None (dedup disabled).
///
/// v0.4.8: retained for block-file format (`ChunkEntry.hash: [u8; 64]`
/// still uses hex ASCII for backwards compatibility with existing block
/// files) and for log / audit output. New in-memory code paths should
/// prefer [`hash_chunk_bytes`] to avoid the allocation + hex conversion.
pub fn hash_chunk(data: &[u8], algo: HashAlgorithm) -> String {
    match algo {
        HashAlgorithm::None => String::new(),
        HashAlgorithm::Md5 => {
            let mut h = Md5::new();
            h.update(data);
            to_hex(&h.finalize())
        }
        HashAlgorithm::Sha1 => {
            let mut h = Sha1::new();
            h.update(data);
            to_hex(&h.finalize())
        }
        HashAlgorithm::Sha256 => {
            let mut h = Sha256::new();
            h.update(data);
            to_hex(&h.finalize())
        }
    }
}

/// Compute CRC32 checksum for integrity verification
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Verify CRC32 checksum
pub fn verify_crc32(data: &[u8], expected: u32) -> bool {
    crc32(data) == expected
}

/// A combined integrity digest: CRC32 for fast corruption detection + content hash for dedup.
///
/// v0.4.8: the `hash` field is now `Option<HashBytes>` (32 raw bytes,
/// left-aligned with zero-padding for MD5/SHA-1). `None` indicates
/// dedup is disabled (`HashAlgorithm::None`). Downstream call sites
/// check the `Option` rather than comparing against the legacy
/// "empty string means disabled" sentinel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkDigest {
    /// Content hash for deduplication; `None` iff `HashAlgorithm::None`.
    pub hash: Option<HashBytes>,
    /// CRC32 for fast integrity check
    pub crc32: u32,
}

impl ChunkDigest {
    pub fn compute(data: &[u8], algo: HashAlgorithm) -> Self {
        Self {
            hash: hash_chunk_bytes(data, algo),
            crc32: crc32(data),
        }
    }

    pub fn verify(&self, data: &[u8]) -> bool {
        verify_crc32(data, self.crc32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn test_sha256_known_value() {
        let hash = hash_chunk(b"hello", HashAlgorithm::Sha256);
        assert_eq!(
            hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_md5_known_value() {
        let hash = hash_chunk(b"hello", HashAlgorithm::Md5);
        assert_eq!(hash, "5d41402abc4b2a76b9719d911017c592");
    }

    #[test]
    fn test_sha1_known_value() {
        let hash = hash_chunk(b"hello", HashAlgorithm::Sha1);
        assert_eq!(hash, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
    }

    #[test]
    fn test_none_returns_empty() {
        assert_eq!(hash_chunk(b"anything", HashAlgorithm::None), "");
    }

    #[test]
    fn test_crc32_deterministic() {
        let a = crc32(b"test data");
        let b = crc32(b"test data");
        assert_eq!(a, b);
    }

    #[test]
    fn test_crc32_different_data() {
        let a = crc32(b"test data");
        let b = crc32(b"test datb");
        assert_ne!(a, b);
    }

    #[test]
    fn test_chunk_digest_compute_and_verify() {
        let data = b"hello world";
        let digest = ChunkDigest::compute(data, HashAlgorithm::Sha256);
        assert!(digest.verify(data));
        let corrupt = b"hello world!";
        assert!(!digest.verify(corrupt));
    }

    proptest! {
        #[test]
        fn prop_sha256_deterministic(data in proptest::collection::vec(any::<u8>(), 0..1024)) {
            let a = hash_chunk(&data, HashAlgorithm::Sha256);
            let b = hash_chunk(&data, HashAlgorithm::Sha256);
            prop_assert_eq!(a, b);
        }

        #[test]
        fn prop_crc32_verify_ok(data in proptest::collection::vec(any::<u8>(), 0..1024)) {
            let c = crc32(&data);
            prop_assert!(verify_crc32(&data, c));
        }
    }
}
