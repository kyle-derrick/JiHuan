use std::io::Read;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

use crate::block::format::ChunkEntry;
use crate::block::reader::BlockReader;
use crate::block::writer::{BlockSummary, BlockWriter};
use crate::chunking::{chunk_data, RawChunk};
use crate::config::AppConfig;
use crate::dedup::hash_chunk;
use crate::error::{JiHuanError, Result};
use crate::gc::{cleanup_incomplete_blocks, GcConfig, GcService};
use crate::metadata::store::MetadataStore;
use crate::metadata::types::{BlockMeta, ChunkMeta, DedupEntry, FileMeta};
use crate::utils::{current_partition_id, ensure_dir, new_id, now_secs};
use crate::wal::{WalOperation, WriteAheadLog};

/// The central storage engine.
///
/// All public methods are synchronous (internally they may hold locks).
/// Wrap in an `Arc` and use from async code via `tokio::task::spawn_blocking`.
pub struct Engine {
    config: AppConfig,
    meta: Arc<MetadataStore>,
    wal: Arc<Mutex<WriteAheadLog>>,
    /// The currently active block writer (if any).
    active_writer: Arc<Mutex<Option<ActiveBlock>>>,
    /// LRU cache of recently opened BlockReaders (keyed by block path string).
    /// Avoids re-parsing block headers on every read of a recently-accessed block.
    reader_cache: Arc<Mutex<LruCache<String, BlockReader>>>,
    gc: Arc<GcService>,
}

/// Cached chunk data stored in memory while a block is still being written.
/// Serves reads of the active block without requiring a seal (which would
/// stop all concurrent writes).
#[derive(Clone)]
struct CachedChunk {
    /// Raw uncompressed data
    data: Vec<u8>,
}

struct ActiveBlock {
    writer: BlockWriter,
    /// In-memory copy of every chunk written to this block so far.
    /// Keyed by `data_offset` (same key used in ChunkMeta).
    chunk_cache: std::collections::HashMap<u64, CachedChunk>,
}

impl Engine {
    /// Open (or create) an engine with the given configuration.
    /// Performs crash recovery on startup.
    pub fn open(config: AppConfig) -> Result<Self> {
        // Ensure all directories exist
        ensure_dir(&config.storage.data_dir)?;
        ensure_dir(&config.storage.meta_dir)?;
        ensure_dir(&config.storage.wal_dir)?;

        // Open metadata store
        let meta_path = config.storage.meta_dir.join("meta.db");
        let meta = Arc::new(MetadataStore::open(&meta_path)?);

        // Open WAL
        let wal_path = config.storage.wal_dir.join("jihuan.wal");
        let wal = Arc::new(Mutex::new(WriteAheadLog::open(&wal_path)?));

        // Crash recovery: remove any incomplete block files
        let removed = cleanup_incomplete_blocks(&config.storage.data_dir)?;
        if removed > 0 {
            tracing::warn!(removed, "Crash recovery: removed incomplete block files");
        }

        let gc_config = GcConfig {
            gc_threshold: config.storage.gc_threshold,
            gc_interval_secs: config.storage.gc_interval_secs,
            time_partition_hours: config.storage.time_partition_hours,
            data_dir: config.storage.data_dir.clone(),
        };
        let gc = Arc::new(GcService::new(meta.clone(), gc_config));

        // LRU reader cache: keep up to max_open_block_files readers open
        let cache_cap = NonZeroUsize::new(config.storage.max_open_block_files.max(4)).unwrap();
        let reader_cache = Arc::new(Mutex::new(LruCache::new(cache_cap)));

        // Register metrics descriptors (no-op if no recorder is installed)
        metrics::describe_counter!("jihuan_puts_total", "Total number of file put operations");
        metrics::describe_counter!("jihuan_gets_total", "Total number of file get operations");
        metrics::describe_counter!(
            "jihuan_deletes_total",
            "Total number of file delete operations"
        );
        metrics::describe_counter!(
            "jihuan_dedup_hits_total",
            "Total number of chunk deduplication hits"
        );
        metrics::describe_counter!(
            "jihuan_bytes_written_total",
            "Total bytes stored (before dedup/compression)"
        );
        metrics::describe_counter!(
            "jihuan_bytes_read_total",
            "Total bytes returned from get operations"
        );
        metrics::describe_histogram!(
            "jihuan_put_duration_seconds",
            "Latency of put_bytes operations in seconds"
        );
        metrics::describe_histogram!(
            "jihuan_get_duration_seconds",
            "Latency of get_bytes operations in seconds"
        );

        tracing::info!("JiHuan engine opened successfully");

        Ok(Self {
            config,
            meta,
            wal,
            active_writer: Arc::new(Mutex::new(None)),
            reader_cache,
            gc,
        })
    }

    /// Start the background GC service
    pub fn start_gc(&self) -> tokio::task::JoinHandle<()> {
        self.gc.clone().start()
    }

    /// Trigger GC manually (synchronous, blocking)
    pub async fn trigger_gc(&self) -> Result<crate::gc::GcStats> {
        self.gc.run_once().await
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Write path
    // ─────────────────────────────────────────────────────────────────────────

    /// Store a file from a byte slice. Returns the assigned file_id.
    pub fn put_bytes(
        &self,
        data: &[u8],
        file_name: &str,
        content_type: Option<&str>,
    ) -> Result<String> {
        self.put_reader(
            &mut std::io::Cursor::new(data),
            data.len() as u64,
            file_name,
            content_type,
        )
    }

    /// Store a file from a reader. Returns the assigned file_id.
    pub fn put_reader<R: Read>(
        &self,
        reader: &mut R,
        file_size: u64,
        file_name: &str,
        content_type: Option<&str>,
    ) -> Result<String> {
        let t0 = std::time::Instant::now();

        // Read entire content into memory.
        let mut buf = Vec::with_capacity(file_size.min(64 * 1024 * 1024) as usize);
        reader.read_to_end(&mut buf).map_err(JiHuanError::Io)?;
        let buf_len = buf.len() as u64;

        let file_id = new_id();
        let create_time = now_secs();
        let partition_id = current_partition_id(self.config.storage.time_partition_hours);

        let chunks = chunk_data(
            &buf,
            self.config.storage.chunk_size as usize,
            self.config.storage.hash_algorithm,
        );

        let mut chunk_metas = Vec::with_capacity(chunks.len());
        for raw_chunk in &chunks {
            let cm = self.store_chunk(raw_chunk)?;
            chunk_metas.push(cm);
        }

        let file_meta = FileMeta {
            file_id: file_id.clone(),
            file_name: file_name.to_string(),
            file_size: buf_len,
            create_time,
            partition_id,
            chunks: chunk_metas,
            content_type: content_type.map(|s| s.to_string()),
        };

        // WAL before metadata commit
        self.wal.lock().append(WalOperation::InsertFile {
            file_id: file_id.clone(),
        })?;
        self.meta.insert_file(&file_meta)?;

        // Metrics
        metrics::counter!("jihuan_puts_total").increment(1);
        metrics::counter!("jihuan_bytes_written_total").increment(buf_len);
        metrics::histogram!("jihuan_put_duration_seconds").record(t0.elapsed().as_secs_f64());

        Ok(file_id)
    }

    fn store_chunk(&self, raw: &RawChunk) -> Result<ChunkMeta> {
        let hash = &raw.digest.hash;
        let use_dedup = !hash.is_empty();

        // Check for existing dedup entry
        if use_dedup {
            if let Some(dedup) = self.meta.get_dedup_entry(hash)? {
                // Reuse existing chunk – increment block ref count
                self.meta
                    .update_block_ref_count(&dedup.block_id, 1)
                    .unwrap_or(0);
                metrics::counter!("jihuan_dedup_hits_total").increment(1);
                return Ok(ChunkMeta {
                    block_id: dedup.block_id,
                    offset: dedup.offset,
                    original_size: dedup.original_size,
                    compressed_size: dedup.compressed_size,
                    hash: hash.clone(),
                    index: raw.index,
                });
            }
        }

        // Write new chunk to active block
        let (block_id, entry) = self.write_chunk_to_block(&raw.data, hash)?;

        let chunk_meta = ChunkMeta {
            block_id: block_id.clone(),
            offset: entry.data_offset,
            original_size: entry.original_size as u64,
            compressed_size: entry.compressed_size as u64,
            hash: hash.clone(),
            index: raw.index,
        };

        // Register dedup entry
        if use_dedup {
            let dedup = DedupEntry {
                hash: hash.clone(),
                block_id: block_id.clone(),
                offset: entry.data_offset,
                original_size: entry.original_size as u64,
                compressed_size: entry.compressed_size as u64,
            };
            self.meta.insert_dedup_entry(&dedup)?;
        }

        Ok(chunk_meta)
    }

    fn write_chunk_to_block(&self, data: &[u8], hash: &str) -> Result<(String, ChunkEntry)> {
        let mut guard = self.active_writer.lock();

        // Check if we need a new block
        let need_new_block = match guard.as_ref() {
            None => true,
            Some(ab) => ab.writer.is_full(data.len()),
        };

        if need_new_block {
            // Finish the previous block (if any) and move it into the reader cache
            if let Some(ab) = guard.take() {
                let summary = ab.writer.finish()?;
                self.register_block(&summary)?;
                // Populate the reader cache with the just-sealed block
                if let Ok(reader) = BlockReader::open(&summary.path) {
                    let mut cache = self.reader_cache.lock();
                    cache.put(summary.path.to_string_lossy().into_owned(), reader);
                }
            }
            // Create a new block
            let block_id = new_id();
            let block_path = self.block_path(&block_id);
            let writer = BlockWriter::create(
                &block_path,
                &block_id,
                self.config.storage.compression_algorithm,
                self.config.storage.compression_level,
                self.config.storage.hash_algorithm,
                self.config.storage.block_file_size,
            )?;
            // Register block metadata immediately (ref_count starts at 0)
            let block_meta = BlockMeta::new(
                &block_id,
                block_path.to_str().unwrap_or_default(),
                0,
                now_secs(),
            );
            self.wal.lock().append(WalOperation::InsertBlock {
                block_id: block_id.clone(),
            })?;
            self.meta.insert_block(&block_meta)?;

            *guard = Some(ActiveBlock {
                writer,
                chunk_cache: std::collections::HashMap::new(),
            });
        }

        let ab = guard.as_mut().unwrap();
        let entry = ab.writer.write_chunk(data, hash)?;
        let block_id = ab.writer.block_id().to_string();

        // Store a copy of the raw data for in-memory reads of the active block
        ab.chunk_cache.insert(
            entry.data_offset,
            CachedChunk {
                data: data.to_vec(),
            },
        );

        // Increment ref count for the block
        self.meta.update_block_ref_count(&block_id, 1)?;

        Ok((block_id, entry))
    }

    fn register_block(&self, summary: &BlockSummary) -> Result<()> {
        // Update block size in metadata
        if let Some(mut block) = self.meta.get_block(&summary.block_id)? {
            block.size = summary.total_size;
            self.meta.delete_block(&summary.block_id)?;
            self.meta.insert_block(&block)?;
        }
        Ok(())
    }

    /// Flush the BufWriter of the active block to the OS page cache.
    pub fn flush_active_block(&self) -> Result<()> {
        let mut guard = self.active_writer.lock();
        if let Some(ab) = guard.as_mut() {
            ab.writer.flush()?;
        }
        Ok(())
    }

    /// Seal (finish) the active block writer, making it accessible to `BlockReader`.
    /// After sealing, the block is added to the reader cache.
    pub fn seal_active_block(&self) -> Result<()> {
        let mut guard = self.active_writer.lock();
        if let Some(ab) = guard.take() {
            let summary = ab.writer.finish()?;
            self.register_block(&summary)?;
            // Add the freshly-sealed block to the reader cache
            if let Ok(reader) = BlockReader::open(&summary.path) {
                let mut cache = self.reader_cache.lock();
                cache.put(summary.path.to_string_lossy().into_owned(), reader);
            }
        }
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Read path
    // ─────────────────────────────────────────────────────────────────────────

    /// Retrieve a file by its ID. Returns the reassembled bytes.
    ///
    /// Hot path: chunks that still reside in the *active* (unsealed) block are
    /// served directly from the in-memory `chunk_cache` without sealing the block.
    /// Sealed blocks are served via the LRU `BlockReader` cache.
    pub fn get_bytes(&self, file_id: &str) -> Result<Vec<u8>> {
        let t0 = std::time::Instant::now();

        let file_meta = self
            .meta
            .get_file(file_id)?
            .ok_or_else(|| JiHuanError::NotFound(format!("File '{}' not found", file_id)))?;

        let mut result = Vec::with_capacity(file_meta.file_size as usize);
        let verify = self.config.storage.verify_on_read;

        // Chunks are ordered by index; read them in order
        let mut sorted_chunks = file_meta.chunks.clone();
        sorted_chunks.sort_by_key(|c| c.index);

        for chunk_meta in &sorted_chunks {
            // ── Fast path: chunk is in the active (unsealed) block ──────────
            {
                let guard = self.active_writer.lock();
                if let Some(ab) = guard.as_ref() {
                    if ab.writer.block_id() == chunk_meta.block_id {
                        if let Some(cached) = ab.chunk_cache.get(&chunk_meta.offset) {
                            // Verify hash if needed
                            if verify && !chunk_meta.hash.is_empty() {
                                let actual =
                                    hash_chunk(&cached.data, self.config.storage.hash_algorithm);
                                if actual != chunk_meta.hash {
                                    return Err(JiHuanError::ChecksumMismatch {
                                        expected: chunk_meta.hash.clone(),
                                        actual,
                                    });
                                }
                            }
                            result.extend_from_slice(&cached.data);
                            continue; // next chunk
                        }
                    }
                }
            } // drop active_writer lock before taking reader_cache lock

            // ── Slow path: chunk is in a sealed block ────────────────────────
            let block_info = self.meta.get_block(&chunk_meta.block_id)?.ok_or_else(|| {
                JiHuanError::NotFound(format!(
                    "Block '{}' not found for file '{}'",
                    chunk_meta.block_id, file_id
                ))
            })?;

            let chunk_data = self.read_chunk_from_block(
                std::path::Path::new(&block_info.path),
                chunk_meta.offset,
                verify,
            )?;

            // Verify content hash
            if verify && !chunk_meta.hash.is_empty() {
                let actual_hash = hash_chunk(&chunk_data, self.config.storage.hash_algorithm);
                if actual_hash != chunk_meta.hash {
                    return Err(JiHuanError::ChecksumMismatch {
                        expected: chunk_meta.hash.clone(),
                        actual: actual_hash,
                    });
                }
            }

            result.extend_from_slice(&chunk_data);
        }

        // Metrics
        metrics::counter!("jihuan_gets_total").increment(1);
        metrics::counter!("jihuan_bytes_read_total").increment(result.len() as u64);
        metrics::histogram!("jihuan_get_duration_seconds").record(t0.elapsed().as_secs_f64());

        Ok(result)
    }

    /// Read a single chunk from a sealed block file, using the LRU reader cache.
    fn read_chunk_from_block(
        &self,
        block_path: &std::path::Path,
        offset: u64,
        verify: bool,
    ) -> Result<Vec<u8>> {
        let path_key = block_path.to_string_lossy().into_owned();

        // Try cache first (lock scope kept short)
        {
            let mut cache = self.reader_cache.lock();
            if let Some(reader) = cache.get_mut(&path_key) {
                let entry = reader
                    .entries
                    .iter()
                    .find(|e| e.data_offset == offset)
                    .cloned()
                    .ok_or_else(|| {
                        JiHuanError::NotFound(format!(
                            "Chunk at offset {} not found in cached block '{}'",
                            offset, path_key
                        ))
                    })?;
                return reader.read_chunk(&entry, verify);
            }
        }

        // Cache miss: open the block file, read the chunk, insert into cache
        let mut reader = BlockReader::open(block_path)?;
        let entry = reader
            .entries
            .iter()
            .find(|e| e.data_offset == offset)
            .cloned()
            .ok_or_else(|| {
                JiHuanError::NotFound(format!(
                    "Chunk at offset {} not found in block '{}'",
                    offset, path_key
                ))
            })?;
        let data = reader.read_chunk(&entry, verify)?;

        // Insert the reader into cache for future accesses
        {
            let mut cache = self.reader_cache.lock();
            cache.put(path_key, reader);
        }

        Ok(data)
    }

    /// Get file metadata without reading the data
    pub fn get_file_meta(&self, file_id: &str) -> Result<Option<FileMeta>> {
        self.meta.get_file(file_id)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Delete path
    // ─────────────────────────────────────────────────────────────────────────

    /// Delete a file. Decrements block ref counts; blocks are reclaimed by GC.
    pub fn delete_file(&self, file_id: &str) -> Result<()> {
        let file_meta = match self.meta.delete_file(file_id)? {
            Some(f) => f,
            None => {
                return Err(JiHuanError::NotFound(format!(
                    "File '{}' not found",
                    file_id
                )))
            }
        };

        self.wal.lock().append(WalOperation::DeleteFile {
            file_id: file_id.to_string(),
        })?;

        // Decrement ref counts for each chunk's block.
        // Each chunk reference was counted independently (one increment per stored chunk),
        // so we must decrement once per chunk even if multiple chunks share the same block.
        for chunk in &file_meta.chunks {
            match self.meta.update_block_ref_count(&chunk.block_id, -1) {
                Ok(_) | Err(JiHuanError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }

        metrics::counter!("jihuan_deletes_total").increment(1);
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Stats / Admin
    // ─────────────────────────────────────────────────────────────────────────

    pub fn file_count(&self) -> Result<u64> {
        self.meta.file_count()
    }

    pub fn block_count(&self) -> Result<u64> {
        self.meta.block_count()
    }

    /// Collect a lightweight stats snapshot without any Prometheus dependency.
    pub fn stats(&self) -> Result<crate::metrics::EngineStats> {
        let file_count = self.meta.file_count()?;
        let block_count = self.meta.block_count()?;
        let disk_usage_bytes = crate::utils::disk_usage_bytes(&self.config.storage.data_dir);

        // Rough dedup ratio: if disk is 0 treat as 1x
        let dedup_ratio = if disk_usage_bytes == 0 {
            1.0
        } else {
            // Sum original sizes of all blocks as a proxy for logical bytes
            let logical: u64 = self.meta.list_all_blocks()?.iter().map(|b| b.size).sum();
            (logical as f64 / disk_usage_bytes as f64).max(1.0)
        };

        Ok(crate::metrics::EngineStats {
            file_count,
            block_count,
            disk_usage_bytes,
            dedup_ratio,
        })
    }

    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    pub fn metadata(&self) -> &MetadataStore {
        &self.meta
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    fn block_path(&self, block_id: &str) -> PathBuf {
        // Use a 2-level directory prefix from the block_id to avoid too many files in one dir
        let prefix = &block_id[..2];
        let sub = &block_id[2..4];
        self.config
            .storage
            .data_dir
            .join(prefix)
            .join(sub)
            .join(format!("{}.blk", block_id))
    }
}

/// Graceful shutdown: seal the active block
impl Drop for Engine {
    fn drop(&mut self) {
        if let Err(e) = self.seal_active_block() {
            tracing::error!(error = %e, "Engine::drop: failed to seal active block");
        }
        if let Err(e) = self.wal.lock().sync() {
            tracing::error!(error = %e, "Engine::drop: failed to sync WAL");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigTemplate;
    use tempfile::tempdir;

    fn open_engine(tmp: &tempfile::TempDir) -> Engine {
        let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        Engine::open(cfg).unwrap()
    }

    #[test]
    fn test_put_and_get_small_file() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        let data = b"Hello, JiHuan! This is a test file.";
        let file_id = engine.put_bytes(data, "test.txt", None).unwrap();
        assert!(!file_id.is_empty());

        let got = engine.get_bytes(&file_id).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn test_put_and_get_empty_file() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        let file_id = engine.put_bytes(b"", "empty.txt", None).unwrap();
        let got = engine.get_bytes(&file_id).unwrap();
        assert_eq!(got, b"");
    }

    #[test]
    fn test_dedup_same_content() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        let data = b"Duplicate content that should be deduplicated";
        let id1 = engine.put_bytes(data, "file1.txt", None).unwrap();
        let id2 = engine.put_bytes(data, "file2.txt", None).unwrap();

        // Both files should be readable
        assert_eq!(engine.get_bytes(&id1).unwrap(), data);
        assert_eq!(engine.get_bytes(&id2).unwrap(), data);

        // Only one block should have been created (dedup)
        assert_eq!(engine.block_count().unwrap(), 1);
    }

    #[test]
    fn test_delete_file() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        let data = b"File to be deleted";
        let file_id = engine.put_bytes(data, "del.txt", None).unwrap();
        engine.delete_file(&file_id).unwrap();

        let result = engine.get_bytes(&file_id);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_nonexistent_file_errors() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        assert!(engine.delete_file("nonexistent-id").is_err());
    }

    #[test]
    fn test_get_nonexistent_file_errors() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        let result = engine.get_bytes("no-such-file");
        assert!(matches!(result, Err(JiHuanError::NotFound(_))));
    }

    #[test]
    fn test_put_large_file_spanning_multiple_chunks() {
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 1024; // 1KB chunks for testing
        let engine = Engine::open(cfg).unwrap();

        // 10KB file → 10 chunks
        let data: Vec<u8> = (0u8..=255).cycle().take(10 * 1024).collect();
        let file_id = engine.put_bytes(&data, "large.bin", None).unwrap();
        let got = engine.get_bytes(&file_id).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn test_file_count() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        assert_eq!(engine.file_count().unwrap(), 0);
        engine.put_bytes(b"a", "a.txt", None).unwrap();
        engine.put_bytes(b"b", "b.txt", None).unwrap();
        assert_eq!(engine.file_count().unwrap(), 2);
    }

    #[test]
    fn test_engine_reopen_crash_recovery() {
        let tmp = tempdir().unwrap();
        let file_id;
        {
            let engine = open_engine(&tmp);
            file_id = engine
                .put_bytes(b"survive crash", "crash.txt", None)
                .unwrap();
            // Don't call seal_active_block / Drop will be called
        }
        // Reopen (simulates crash recovery)
        let engine2 = open_engine(&tmp);
        let got = engine2.get_bytes(&file_id);
        // After graceful drop the block is sealed; data should be readable
        assert!(got.is_ok());
        assert_eq!(got.unwrap(), b"survive crash");
    }
}
