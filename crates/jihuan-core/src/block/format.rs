use bincode::{Decode, Encode};

/// Magic bytes at the start of every block file: "JIHUAN\0\0"
pub const BLOCK_MAGIC: [u8; 8] = *b"JIHUAN\x00\x00";
/// Current block file format version
pub const BLOCK_VERSION: u32 = 1;
/// Magic for the footer sentinel
pub const FOOTER_MAGIC: [u8; 8] = *b"JIHUANFT";

/// On-disk block file layout:
///
/// ```text
/// [ BlockHeader (fixed, 64 bytes) ]
/// [ Chunk 1 compressed data       ]
/// [ Chunk 2 compressed data       ]
/// ...
/// [ ChunkEntry × n (index table)  ]
/// [ BlockFooter (fixed, 32 bytes) ]
/// ```
///
/// The index table offset is stored in the footer so we can quickly
/// locate it after a sequential write, and also survive partial writes
/// (footer not written == block is incomplete).
/// Fixed-size block file header (serialised with bincode, 64-byte reserved)
#[derive(Debug, Clone, Encode, Decode)]
pub struct BlockHeader {
    /// Magic bytes
    pub magic: [u8; 8],
    /// Format version
    pub version: u32,
    /// Unique block ID (UUID simple, 32 ascii bytes stored as fixed array)
    pub block_id: [u8; 32],
    /// Unix timestamp (seconds) when this block was created
    pub create_time: u64,
    /// Number of chunk entries stored in this block
    pub chunk_count: u32,
    /// CRC32 of the header fields above (excluding this field)
    pub header_crc32: u32,
    /// Reserved for future use
    pub _reserved: [u8; 8],
}

impl BlockHeader {
    pub const SERIALIZED_SIZE: usize = 72; // bincode fixed size

    pub fn new(block_id: &str, create_time: u64) -> Self {
        let mut id_bytes = [0u8; 32];
        let src = block_id.as_bytes();
        let len = src.len().min(32);
        id_bytes[..len].copy_from_slice(&src[..len]);

        let mut hdr = Self {
            magic: BLOCK_MAGIC,
            version: BLOCK_VERSION,
            block_id: id_bytes,
            create_time,
            chunk_count: 0,
            header_crc32: 0,
            _reserved: [0u8; 8],
        };
        hdr.header_crc32 = hdr.compute_crc();
        hdr
    }

    /// Re-compute the CRC over all fields except `header_crc32`
    pub fn compute_crc(&self) -> u32 {
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(&self.magic);
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&self.block_id);
        buf.extend_from_slice(&self.create_time.to_le_bytes());
        buf.extend_from_slice(&self.chunk_count.to_le_bytes());
        buf.extend_from_slice(&self._reserved);
        crc32fast::hash(&buf)
    }

    pub fn verify(&self) -> bool {
        self.magic == BLOCK_MAGIC
            && self.version == BLOCK_VERSION
            && self.header_crc32 == self.compute_crc()
    }

    pub fn block_id_str(&self) -> String {
        let end = self.block_id.iter().position(|&b| b == 0).unwrap_or(32);
        String::from_utf8_lossy(&self.block_id[..end]).into_owned()
    }
}

/// Per-chunk index entry stored in the index table section of the block file
#[derive(Debug, Clone, Encode, Decode)]
pub struct ChunkEntry {
    /// Content hash (hex string, up to 64 bytes for sha256)
    pub hash: [u8; 64],
    /// Hash length (actual bytes used in `hash` array)
    pub hash_len: u8,
    /// Byte offset of the compressed chunk data within the block file
    pub data_offset: u64,
    /// Length of the compressed data in bytes
    pub compressed_size: u32,
    /// Original (uncompressed) size in bytes
    pub original_size: u32,
    /// CRC32 of the *compressed* data
    pub data_crc32: u32,
    /// Compression algorithm used (0=none, 1=lz4, 2=zstd)
    pub compression: u8,
    /// Reserved
    pub _reserved: [u8; 2],
}

impl ChunkEntry {
    pub fn new(
        hash: &str,
        data_offset: u64,
        compressed_size: u32,
        original_size: u32,
        data_crc32: u32,
        compression: u8,
    ) -> Self {
        let mut hash_bytes = [0u8; 64];
        let src = hash.as_bytes();
        let len = src.len().min(64);
        hash_bytes[..len].copy_from_slice(&src[..len]);
        Self {
            hash: hash_bytes,
            hash_len: len as u8,
            data_offset,
            compressed_size,
            original_size,
            data_crc32,
            compression,
            _reserved: [0u8; 2],
        }
    }

    pub fn hash_str(&self) -> String {
        let len = self.hash_len as usize;
        String::from_utf8_lossy(&self.hash[..len]).into_owned()
    }
}

/// Fixed-size block file footer (written last; absence means incomplete block)
#[derive(Debug, Clone, Encode, Decode)]
pub struct BlockFooter {
    /// Footer magic
    pub magic: [u8; 8],
    /// Byte offset where the index table starts
    pub index_offset: u64,
    /// Number of chunk entries
    pub chunk_count: u32,
    /// CRC32 of all chunk compressed data concatenated
    pub data_crc32: u32,
    /// CRC32 of the footer fields above (excluding this field)
    pub footer_crc32: u32,
    /// Reserved
    pub _reserved: [u8; 4],
}

impl BlockFooter {
    pub const SERIALIZED_SIZE: usize = 36;

    pub fn new(index_offset: u64, chunk_count: u32, data_crc32: u32) -> Self {
        let mut f = Self {
            magic: FOOTER_MAGIC,
            index_offset,
            chunk_count,
            data_crc32,
            footer_crc32: 0,
            _reserved: [0u8; 4],
        };
        f.footer_crc32 = f.compute_crc();
        f
    }

    pub fn compute_crc(&self) -> u32 {
        let mut buf = Vec::with_capacity(28);
        buf.extend_from_slice(&self.magic);
        buf.extend_from_slice(&self.index_offset.to_le_bytes());
        buf.extend_from_slice(&self.chunk_count.to_le_bytes());
        buf.extend_from_slice(&self.data_crc32.to_le_bytes());
        buf.extend_from_slice(&self._reserved);
        crc32fast::hash(&buf)
    }

    pub fn verify(&self) -> bool {
        self.magic == FOOTER_MAGIC && self.footer_crc32 == self.compute_crc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_header_verify() {
        let hdr = BlockHeader::new("test-block-id-0000000000000000", 12345678);
        assert!(hdr.verify());
    }

    #[test]
    fn test_block_header_tamper_detected() {
        let mut hdr = BlockHeader::new("abc", 0);
        hdr.version = 99;
        assert!(!hdr.verify());
    }

    #[test]
    fn test_footer_verify() {
        let f = BlockFooter::new(1024, 5, 0xdeadbeef);
        assert!(f.verify());
    }

    #[test]
    fn test_footer_tamper_detected() {
        let mut f = BlockFooter::new(1024, 5, 0xdeadbeef);
        f.chunk_count = 6;
        assert!(!f.verify());
    }

    #[test]
    fn test_chunk_entry_hash_roundtrip() {
        let hash = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let entry = ChunkEntry::new(hash, 64, 100, 200, 0xaabb, 2);
        assert_eq!(entry.hash_str(), hash);
    }
}
