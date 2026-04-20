use digest::Digest;
use md5::Md5;
use sha1::Sha1;
use sha2::Sha256;

use crate::config::HashAlgorithm;
use crate::utils::to_hex;

/// Compute a content hash for the given data using the specified algorithm.
/// Returns an empty string when the algorithm is None (dedup disabled).
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

/// A combined integrity digest: CRC32 for fast corruption detection + content hash for dedup
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkDigest {
    /// Content hash for deduplication (empty if HashAlgorithm::None)
    pub hash: String,
    /// CRC32 for fast integrity check
    pub crc32: u32,
}

impl ChunkDigest {
    pub fn compute(data: &[u8], algo: HashAlgorithm) -> Self {
        Self {
            hash: hash_chunk(data, algo),
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
