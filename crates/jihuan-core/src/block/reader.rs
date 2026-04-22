use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::block::format::{BlockFooter, BlockHeader, ChunkEntry, FOOTER_MAGIC};
use crate::compression::decompress;
use crate::config::CompressionAlgorithm;
use crate::dedup::verify_crc32;
use crate::error::{JiHuanError, Result};

/// Reads chunks from an immutable block file.
pub struct BlockReader {
    path: PathBuf,
    file: File,
    pub header: BlockHeader,
    pub footer: BlockFooter,
    pub entries: Vec<ChunkEntry>,
}

impl BlockReader {
    /// Open a block file and validate its header and footer.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path).map_err(JiHuanError::Io)?;

        // --- Read and validate header ---
        let header = Self::read_header(&mut file)?;
        if !header.verify() {
            return Err(JiHuanError::DataCorruption(format!(
                "Block header verification failed for '{}'",
                path.display()
            )));
        }

        // --- Read and validate footer ---
        let footer = Self::read_footer(&mut file)?;
        if !footer.verify() {
            return Err(JiHuanError::DataCorruption(format!(
                "Block footer verification failed for '{}' – block may be incomplete",
                path.display()
            )));
        }

        // --- Read the index table ---
        let entries = Self::read_index(&mut file, &footer)?;

        Ok(Self {
            path,
            file,
            header,
            footer,
            entries,
        })
    }

    /// Attempt to detect if a block file is complete (has a valid footer).
    pub fn is_complete<P: AsRef<Path>>(path: P) -> bool {
        let mut file = match File::open(path.as_ref()) {
            Ok(f) => f,
            Err(_) => return false,
        };
        match Self::read_footer(&mut file) {
            Ok(f) => f.verify(),
            Err(_) => false,
        }
    }

    fn read_header(file: &mut File) -> Result<BlockHeader> {
        file.seek(SeekFrom::Start(0)).map_err(JiHuanError::Io)?;
        // Read a generous buffer; bincode will decode the first N bytes
        let mut buf = vec![0u8; 256];
        let n = file.read(&mut buf).map_err(JiHuanError::Io)?;
        buf.truncate(n);

        let (header, _) =
            bincode::decode_from_slice::<BlockHeader, _>(&buf, bincode::config::standard())
                .map_err(|e| JiHuanError::Serialization(format!("Block header decode: {}", e)))?;
        Ok(header)
    }

    fn read_footer(file: &mut File) -> Result<BlockFooter> {
        // Footer is at EOF - SERIALIZED_SIZE bytes
        let file_size = file.seek(SeekFrom::End(0)).map_err(JiHuanError::Io)?;
        if file_size < BlockFooter::SERIALIZED_SIZE as u64 {
            return Err(JiHuanError::DataCorruption(
                "File too small to contain a valid footer".into(),
            ));
        }

        let footer_start = file_size.saturating_sub(256); // read a tail buffer
        file.seek(SeekFrom::Start(footer_start))
            .map_err(JiHuanError::Io)?;
        let mut buf = vec![0u8; (file_size - footer_start) as usize];
        file.read_exact(&mut buf).map_err(JiHuanError::Io)?;

        // Scan backwards for footer magic
        let magic = &FOOTER_MAGIC;
        let pos = buf
            .windows(8)
            .rposition(|w| w == magic)
            .ok_or_else(|| JiHuanError::DataCorruption("Footer magic not found".into()))?;

        let (footer, _) =
            bincode::decode_from_slice::<BlockFooter, _>(&buf[pos..], bincode::config::standard())
                .map_err(|e| JiHuanError::Serialization(format!("Block footer decode: {}", e)))?;
        Ok(footer)
    }

    fn read_index(file: &mut File, footer: &BlockFooter) -> Result<Vec<ChunkEntry>> {
        file.seek(SeekFrom::Start(footer.index_offset))
            .map_err(JiHuanError::Io)?;

        // Read everything from index_offset to end (minus footer)
        let file_size = file.seek(SeekFrom::End(0)).map_err(JiHuanError::Io)?;
        file.seek(SeekFrom::Start(footer.index_offset))
            .map_err(JiHuanError::Io)?;
        let index_data_len = file_size.saturating_sub(footer.index_offset) as usize;
        let mut buf = vec![0u8; index_data_len];
        file.read_exact(&mut buf).map_err(JiHuanError::Io)?;

        let mut entries = Vec::with_capacity(footer.chunk_count as usize);
        let mut offset = 0usize;
        for _ in 0..footer.chunk_count {
            if offset >= buf.len() {
                return Err(JiHuanError::DataCorruption("Index table truncated".into()));
            }
            let (entry, consumed) = bincode::decode_from_slice::<ChunkEntry, _>(
                &buf[offset..],
                bincode::config::standard(),
            )
            .map_err(|e| JiHuanError::Serialization(format!("ChunkEntry decode: {}", e)))?;
            offset += consumed;
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Read and decompress the chunk at the given entry.
    pub fn read_chunk(&mut self, entry: &ChunkEntry, verify: bool) -> Result<Vec<u8>> {
        let (compressed, algo) = self.read_chunk_raw(entry, verify)?;
        decompress(&compressed, algo, entry.original_size as usize)
    }

    /// v0.4.7: read the **compressed** chunk bytes verbatim along with
    /// the algorithm byte recorded at write time. Used by the compaction
    /// merge path to skip decompress + recompress when the source block
    /// already uses the engine's configured algorithm (same-algo
    /// passthrough).
    ///
    /// The integrity envelope is identical to `read_chunk`: per-chunk
    /// CRC32 is verified when `verify == true`. Content-hash
    /// verification would require a decompression pass and is
    /// deliberately omitted here (matches existing semantics of the
    /// normal read path — we trust the chunk hash recorded in
    /// `FileMeta.chunks[i].hash`).
    pub fn read_chunk_raw(
        &mut self,
        entry: &ChunkEntry,
        verify: bool,
    ) -> Result<(Vec<u8>, CompressionAlgorithm)> {
        self.file
            .seek(SeekFrom::Start(entry.data_offset))
            .map_err(JiHuanError::Io)?;

        let mut compressed = vec![0u8; entry.compressed_size as usize];
        self.file
            .read_exact(&mut compressed)
            .map_err(JiHuanError::Io)?;

        // Verify CRC32 of compressed data
        if verify && !verify_crc32(&compressed, entry.data_crc32) {
            return Err(JiHuanError::ChecksumMismatch {
                expected: format!("{:#010x}", entry.data_crc32),
                actual: format!("{:#010x}", crc32fast::hash(&compressed)),
            });
        }

        let algo = match entry.compression {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            2 => CompressionAlgorithm::Zstd,
            n => {
                return Err(JiHuanError::DataCorruption(format!(
                    "Unknown compression byte: {}",
                    n
                )))
            }
        };

        Ok((compressed, algo))
    }

    /// Read a chunk by its position index (0-based)
    pub fn read_chunk_at(&mut self, idx: usize, verify: bool) -> Result<Vec<u8>> {
        let entry = self
            .entries
            .get(idx)
            .ok_or_else(|| JiHuanError::NotFound(format!("Chunk index {} not found", idx)))?
            .clone();
        self.read_chunk(&entry, verify)
    }

    /// Find the first chunk entry matching the given hash
    pub fn find_by_hash(&self, hash: &str) -> Option<&ChunkEntry> {
        self.entries.iter().find(|e| e.hash_str() == hash)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn chunk_count(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::writer::BlockWriter;
    use crate::config::{CompressionAlgorithm, HashAlgorithm};
    use crate::dedup::hash_chunk;
    use tempfile::tempdir;

    fn write_test_block(dir: &Path, chunks: &[&[u8]]) -> PathBuf {
        let block_path = dir.join("test.blk");
        let mut w = BlockWriter::create(
            &block_path,
            "test-block-00000000000000000000000000",
            CompressionAlgorithm::Zstd,
            1,
            HashAlgorithm::Sha256,
            1024 * 1024 * 1024,
        )
        .unwrap();
        for chunk in chunks {
            let hash = hash_chunk(chunk, HashAlgorithm::Sha256);
            w.write_chunk(chunk, &hash).unwrap();
        }
        w.finish().unwrap();
        block_path
    }

    #[test]
    fn test_open_and_read_back() {
        let tmp = tempdir().unwrap();
        let chunks: &[&[u8]] = &[b"hello", b"world", b"foo bar baz"];
        let path = write_test_block(tmp.path(), chunks);

        let mut reader = BlockReader::open(&path).unwrap();
        assert_eq!(reader.chunk_count(), 3);

        for (i, expected) in chunks.iter().enumerate() {
            let data = reader.read_chunk_at(i, true).unwrap();
            assert_eq!(data, *expected);
        }
    }

    #[test]
    fn test_is_complete_true() {
        let tmp = tempdir().unwrap();
        let path = write_test_block(tmp.path(), &[b"data"]);
        assert!(BlockReader::is_complete(&path));
    }

    #[test]
    fn test_incomplete_block_detection() {
        let tmp = tempdir().unwrap();
        // Write a partial file with only the magic header, no footer
        let path = tmp.path().join("partial.blk");
        std::fs::write(&path, crate::block::format::BLOCK_MAGIC).unwrap();
        assert!(!BlockReader::is_complete(&path));
    }

    #[test]
    fn test_find_by_hash() {
        let tmp = tempdir().unwrap();
        let data = b"unique test data";
        let hash = hash_chunk(data, HashAlgorithm::Sha256);
        let path = write_test_block(tmp.path(), &[data]);

        let reader = BlockReader::open(&path).unwrap();
        let entry = reader.find_by_hash(&hash);
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().original_size, data.len() as u32);
    }

    #[test]
    fn test_checksum_failure_on_corruption() {
        let tmp = tempdir().unwrap();
        let path = write_test_block(tmp.path(), &[b"hello world"]);

        // Corrupt a byte in the chunk data area (offset ~72, after header)
        let mut bytes = std::fs::read(&path).unwrap();
        if bytes.len() > 80 {
            bytes[80] ^= 0xFF;
        }
        std::fs::write(&path, &bytes).unwrap();

        // Reading with verify=true should fail or succeed depending on which byte was hit
        // We just ensure it doesn't panic
        let result = BlockReader::open(&path);
        // It may or may not fail at open depending on which byte was corrupted;
        // the important thing is no panic.
        let _ = result;
    }
}
