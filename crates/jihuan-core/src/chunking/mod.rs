use std::io::{Read, Seek, SeekFrom};

use crate::config::HashAlgorithm;
use crate::dedup::ChunkDigest;
use crate::error::{JiHuanError, Result};

/// Raw data of a single chunk along with its digest
#[derive(Debug)]
pub struct RawChunk {
    /// Index of this chunk within the file (0-based)
    pub index: u32,
    /// Raw (uncompressed) data
    pub data: bytes::Bytes,
    /// Integrity and dedup digest
    pub digest: ChunkDigest,
}

/// Split a byte slice into fixed-size chunks
pub fn chunk_data(data: &[u8], chunk_size: usize, algo: HashAlgorithm) -> Vec<RawChunk> {
    if data.is_empty() {
        return vec![RawChunk {
            index: 0,
            data: bytes::Bytes::new(),
            digest: ChunkDigest::compute(&[], algo),
        }];
    }

    let mut chunks = Vec::with_capacity(data.len().div_ceil(chunk_size));
    for (index, piece) in data.chunks(chunk_size).enumerate() {
        chunks.push(RawChunk {
            index: index as u32,
            data: bytes::Bytes::copy_from_slice(piece),
            digest: ChunkDigest::compute(piece, algo),
        });
    }
    chunks
}

/// Stream-based chunker that reads from any `Read + Seek` source
pub struct StreamChunker<R: Read + Seek> {
    reader: R,
    chunk_size: usize,
    algo: HashAlgorithm,
    index: u32,
    done: bool,
}

impl<R: Read + Seek> StreamChunker<R> {
    pub fn new(reader: R, chunk_size: usize, algo: HashAlgorithm) -> Self {
        Self {
            reader,
            chunk_size,
            algo,
            index: 0,
            done: false,
        }
    }

    /// Get the total file size by seeking
    pub fn file_size(&mut self) -> Result<u64> {
        let pos = self.reader.stream_position().map_err(JiHuanError::Io)?;
        let end = self
            .reader
            .seek(SeekFrom::End(0))
            .map_err(JiHuanError::Io)?;
        self.reader
            .seek(SeekFrom::Start(pos))
            .map_err(JiHuanError::Io)?;
        Ok(end)
    }
}

impl<R: Read + Seek> Iterator for StreamChunker<R> {
    type Item = Result<RawChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut buf = vec![0u8; self.chunk_size];
        let mut total_read = 0;

        loop {
            match self.reader.read(&mut buf[total_read..]) {
                Ok(0) => break,
                Ok(n) => {
                    total_read += n;
                    if total_read == self.chunk_size {
                        break;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Some(Err(JiHuanError::Io(e))),
            }
        }

        if total_read == 0 {
            // Emit an empty chunk for a completely empty stream (first call only)
            if self.index == 0 {
                self.done = true;
                let data = bytes::Bytes::new();
                let digest = ChunkDigest::compute(&[], self.algo);
                return Some(Ok(RawChunk {
                    index: 0,
                    data,
                    digest,
                }));
            }
            self.done = true;
            return None;
        }

        if total_read < self.chunk_size {
            self.done = true;
        }

        buf.truncate(total_read);
        let digest = ChunkDigest::compute(&buf, self.algo);
        let chunk = RawChunk {
            index: self.index,
            data: bytes::Bytes::from(buf),
            digest,
        };
        self.index += 1;
        Some(Ok(chunk))
    }
}

/// Calculate the number of chunks for a given file size
pub fn chunk_count(file_size: u64, chunk_size: u64) -> u32 {
    if file_size == 0 {
        return 1; // even empty files have one (empty) chunk
    }
    file_size.div_ceil(chunk_size) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::io::Cursor;

    #[test]
    fn test_chunk_exact_multiple() {
        let data = vec![0u8; 8 * 1024]; // 8KB
        let chunks = chunk_data(&data, 4 * 1024, HashAlgorithm::Sha256);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].data.len(), 4096);
        assert_eq!(chunks[1].data.len(), 4096);
        assert_eq!(chunks[0].index, 0);
        assert_eq!(chunks[1].index, 1);
    }

    #[test]
    fn test_chunk_not_multiple() {
        let data = vec![0u8; 5 * 1024]; // 5KB
        let chunks = chunk_data(&data, 4 * 1024, HashAlgorithm::Sha256);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].data.len(), 4096);
        assert_eq!(chunks[1].data.len(), 1024);
    }

    #[test]
    fn test_chunk_smaller_than_chunk_size() {
        let data = vec![1u8; 100]; // 100 bytes
        let chunks = chunk_data(&data, 4096, HashAlgorithm::Sha256);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data.len(), 100);
    }

    #[test]
    fn test_chunk_empty_data() {
        let chunks = chunk_data(&[], 4096, HashAlgorithm::Sha256);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data.len(), 0);
    }

    #[test]
    fn test_stream_chunker_reads_all_data() {
        let data: Vec<u8> = (0u8..=255).cycle().take(10 * 1024).collect();
        let cursor = Cursor::new(data.clone());
        let chunks: Vec<_> = StreamChunker::new(cursor, 4096, HashAlgorithm::Sha256)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        let reassembled: Vec<u8> = chunks.iter().flat_map(|c| c.data.iter().copied()).collect();
        assert_eq!(reassembled, data);
    }

    #[test]
    fn test_stream_chunker_empty_input() {
        let cursor = Cursor::new(vec![]);
        let chunks: Vec<_> = StreamChunker::new(cursor, 4096, HashAlgorithm::Sha256)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data.len(), 0);
    }

    #[test]
    fn test_chunk_digest_verifies() {
        let data = vec![42u8; 1024];
        let chunks = chunk_data(&data, 512, HashAlgorithm::Sha256);
        for chunk in &chunks {
            assert!(chunk.digest.verify(&chunk.data));
        }
    }

    #[test]
    fn test_chunk_count() {
        assert_eq!(chunk_count(0, 4096), 1);
        assert_eq!(chunk_count(4096, 4096), 1);
        assert_eq!(chunk_count(4097, 4096), 2);
        assert_eq!(chunk_count(8192, 4096), 2);
    }

    proptest! {
        #[test]
        fn prop_chunks_reassemble_correctly(
            data in proptest::collection::vec(any::<u8>(), 0..16384),
            chunk_size in 512usize..=8192usize,
        ) {
            let chunks = chunk_data(&data, chunk_size, HashAlgorithm::Sha256);
            let reassembled: Vec<u8> = chunks.iter().flat_map(|c| c.data.iter().copied()).collect();
            prop_assert_eq!(reassembled, data);
        }
    }
}
