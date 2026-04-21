use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::block::format::{BlockFooter, BlockHeader, ChunkEntry};
use crate::compression::compress;
use crate::config::{CompressionAlgorithm, HashAlgorithm};
use crate::dedup::crc32;
use crate::error::{JiHuanError, Result};
use crate::utils::now_secs;

/// Writes a single block file sequentially.
///
/// Usage:
/// 1. `BlockWriter::create(path, block_id, ...)`
/// 2. Call `write_chunk()` for each chunk
/// 3. Call `finish()` to write the index table and footer
pub struct BlockWriter {
    path: PathBuf,
    writer: BufWriter<File>,
    block_id: String,
    compression: CompressionAlgorithm,
    compression_level: i32,
    _hash_algo: HashAlgorithm,
    entries: Vec<ChunkEntry>,
    current_offset: u64,
    cumulative_crc: crc32fast::Hasher,
    max_size: u64,
    finished: bool,
}

impl BlockWriter {
    pub fn create<P: AsRef<Path>>(
        path: P,
        block_id: &str,
        compression: CompressionAlgorithm,
        compression_level: i32,
        _hash_algo: HashAlgorithm,
        max_size: u64,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        // Create parent dirs if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(JiHuanError::Io)?;
        }

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(JiHuanError::Io)?;

        let mut writer = BufWriter::new(file);

        // Write placeholder header (we will NOT rewrite it; chunk_count is in footer)
        let header = BlockHeader::new(block_id, now_secs());
        let header_bytes = bincode::encode_to_vec(&header, bincode::config::standard())
            .map_err(|e| JiHuanError::Serialization(e.to_string()))?;
        writer.write_all(&header_bytes).map_err(JiHuanError::Io)?;

        let header_size = header_bytes.len() as u64;

        Ok(Self {
            path,
            writer,
            block_id: block_id.to_string(),
            compression,
            compression_level,
            _hash_algo,
            entries: Vec::new(),
            current_offset: header_size,
            cumulative_crc: crc32fast::Hasher::new(),
            max_size,
            finished: false,
        })
    }

    /// How many bytes have been written so far (header + chunk data)
    pub fn current_size(&self) -> u64 {
        self.current_offset
    }

    /// Returns true if the block cannot accept any more chunks without exceeding `max_size`
    pub fn is_full(&self, next_chunk_uncompressed: usize) -> bool {
        // Conservative estimate: assume no compression
        self.current_offset + next_chunk_uncompressed as u64 >= self.max_size
    }

    /// Write a chunk into the block.
    /// Returns the `ChunkEntry` that was appended.
    pub fn write_chunk(&mut self, data: &[u8], hash: &str) -> Result<ChunkEntry> {
        if self.finished {
            return Err(JiHuanError::Block(
                "write_chunk called on a finished BlockWriter".into(),
            ));
        }

        let original_size = data.len() as u32;

        // Compress the chunk
        let compressed = compress(data, self.compression, self.compression_level)?;
        let compressed_size = compressed.len() as u32;

        // CRC32 of the compressed data
        let data_crc32 = crc32(&compressed);
        self.cumulative_crc.update(&compressed);

        // Write compressed data
        self.writer
            .write_all(&compressed)
            .map_err(JiHuanError::Io)?;

        let algo_byte = match self.compression {
            crate::config::CompressionAlgorithm::None => 0u8,
            crate::config::CompressionAlgorithm::Lz4 => 1u8,
            crate::config::CompressionAlgorithm::Zstd => 2u8,
        };

        let entry = ChunkEntry::new(
            hash,
            self.current_offset,
            compressed_size,
            original_size,
            data_crc32,
            algo_byte,
        );

        self.current_offset += compressed_size as u64;
        self.entries.push(entry.clone());

        Ok(entry)
    }

    /// Flush, write the index table and footer, then close the file.
    /// After this call the block file is immutable.
    pub fn finish(mut self) -> Result<BlockSummary> {
        if self.finished {
            return Err(JiHuanError::Block("BlockWriter already finished".into()));
        }
        self.finished = true;

        // Flush buffered writes so far
        self.writer.flush().map_err(JiHuanError::Io)?;

        let index_offset = self.current_offset;
        let chunk_count = self.entries.len() as u32;

        // Write index table (one ChunkEntry per chunk)
        for entry in &self.entries {
            let bytes = bincode::encode_to_vec(entry, bincode::config::standard())
                .map_err(|e| JiHuanError::Serialization(e.to_string()))?;
            self.writer.write_all(&bytes).map_err(JiHuanError::Io)?;
        }

        // Cumulative CRC over all compressed data
        let data_crc32 = self.cumulative_crc.finalize();

        // Write footer
        let footer = BlockFooter::new(index_offset, chunk_count, data_crc32);
        let footer_bytes = bincode::encode_to_vec(&footer, bincode::config::standard())
            .map_err(|e| JiHuanError::Serialization(e.to_string()))?;
        self.writer
            .write_all(&footer_bytes)
            .map_err(JiHuanError::Io)?;

        // Final flush + sync to disk
        self.writer.flush().map_err(JiHuanError::Io)?;
        self.writer.get_ref().sync_all().map_err(JiHuanError::Io)?;

        Ok(BlockSummary {
            block_id: self.block_id.clone(),
            path: self.path.clone(),
            chunk_count,
            total_size: self.current_offset,
        })
    }

    pub fn block_id(&self) -> &str {
        &self.block_id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Number of chunks written so far
    pub fn chunk_count(&self) -> usize {
        self.entries.len()
    }

    /// Flush the internal BufWriter and sync file data to disk.
    /// Does NOT write the index table or footer; the block remains "open".
    ///
    /// We call `sync_data()` here (rather than only BufWriter::flush) so that
    /// `std::fs::metadata(path).len()` reflects the bytes written — this is
    /// required for accurate disk-usage stats while a block is still active.
    pub fn flush(&mut self) -> crate::error::Result<()> {
        use std::io::Write as _;
        self.writer.flush().map_err(crate::error::JiHuanError::Io)?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(crate::error::JiHuanError::Io)
    }
}

/// Summary returned after `finish()`
#[derive(Debug, Clone)]
pub struct BlockSummary {
    pub block_id: String,
    pub path: PathBuf,
    pub chunk_count: u32,
    pub total_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompressionAlgorithm, HashAlgorithm};
    use crate::dedup::hash_chunk;
    use tempfile::tempdir;

    fn make_writer(dir: &Path, id: &str) -> BlockWriter {
        BlockWriter::create(
            dir.join(format!("{}.blk", id)),
            id,
            CompressionAlgorithm::Zstd,
            1,
            HashAlgorithm::Sha256,
            1024 * 1024 * 1024,
        )
        .unwrap()
    }

    #[test]
    fn test_write_single_chunk() {
        let tmp = tempdir().unwrap();
        let mut w = make_writer(tmp.path(), "block001");
        let data = b"hello, world! This is test data.";
        let hash = hash_chunk(data, HashAlgorithm::Sha256);
        let entry = w.write_chunk(data, &hash).unwrap();
        assert_eq!(entry.original_size, data.len() as u32);
        assert_eq!(entry.hash_str(), hash);
        let summary = w.finish().unwrap();
        assert_eq!(summary.chunk_count, 1);
        assert!(summary.path.exists());
    }

    #[test]
    fn test_write_multiple_chunks() {
        let tmp = tempdir().unwrap();
        let mut w = make_writer(tmp.path(), "block002");
        for i in 0..5u8 {
            let data: Vec<u8> = vec![i; 1024];
            let hash = hash_chunk(&data, HashAlgorithm::Sha256);
            w.write_chunk(&data, &hash).unwrap();
        }
        let summary = w.finish().unwrap();
        assert_eq!(summary.chunk_count, 5);
        assert!(summary.total_size > 0);
    }

    #[test]
    fn test_block_file_magic_at_start() {
        let tmp = tempdir().unwrap();
        let mut w = make_writer(tmp.path(), "block003");
        w.write_chunk(b"x", "fakehash").unwrap();
        let summary = w.finish().unwrap();

        let bytes = std::fs::read(&summary.path).unwrap();
        assert_eq!(&bytes[..8], &crate::block::format::BLOCK_MAGIC);
    }

    #[test]
    fn test_write_after_finish_errors() {
        let tmp = tempdir().unwrap();
        let mut w = make_writer(tmp.path(), "block004");
        w.write_chunk(b"data", "h").unwrap();
        let _summary = w.finish().unwrap();
        // Can't write after finish - writer is consumed, this is enforced by type system
    }
}
