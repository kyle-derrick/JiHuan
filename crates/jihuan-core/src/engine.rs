use std::io::Read;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use lru::LruCache;
use parking_lot::Mutex;

use crate::block::format::ChunkEntry;
use crate::block::reader::BlockReader;
use crate::block::writer::{BlockSummary, BlockWriter};
use crate::chunking::RawChunk;
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
    /// Cached `stats()` snapshot (Phase 6b-P4).
    ///
    /// `stats()` used to walk `data_dir` and sum every file's size on every
    /// call — O(files + blocks) I/O per Dashboard refresh. We cache the result
    /// with a short TTL so that a tight polling UI, `/api/status`,
    /// `/api/metrics`, and any operator script each pay the scan cost at most
    /// once per `STATS_CACHE_TTL_MS`. Writers / deletes don't invalidate the
    /// cache directly — they just let it expire — which is acceptable because
    /// the absolute numbers are inherently approximate (active block bytes,
    /// OS page cache, filesystem metadata overhead).
    stats_cache: Arc<Mutex<Option<CachedStats>>>,
}

/// TTL for the `stats()` cache. Five seconds balances Dashboard freshness
/// against scan cost: a 1 M-file deployment walks `data_dir` in ~1 s, so
/// 5 s keeps amortised CPU at ~20 % of a core even under a 1 Hz poll.
const STATS_CACHE_TTL_MS: u128 = 5_000;

struct CachedStats {
    at: Instant,
    snapshot: crate::metrics::EngineStats,
}

/// Counts returned by [`Engine::repair`]. Exposed for tests and for the
/// eventual admin RPC that invokes repair on-demand.
#[derive(Debug, Default, Clone, Copy, serde::Serialize)]
pub struct RepairSummary {
    pub files_removed: u32,
    pub blocks_removed: u32,
    pub dedup_removed: u32,
}

/// Cached chunk data stored in memory while a block is still being written.
/// Serves reads of the active block without requiring a seal (which would
/// stop all concurrent writes).
#[derive(Clone)]
struct CachedChunk {
    /// Raw uncompressed data
    data: Vec<u8>,
}

/// Default byte budget for the per-active-block chunk cache. Phase 6b-P6:
/// previously this was unbounded, which meant a 1 GB active block held
/// 1 GB of uncompressed chunk data in RAM until seal. 64 MB keeps the
/// common "read-your-write" case O(1) while capping worst-case RSS.
const DEFAULT_ACTIVE_CHUNK_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Resolve the active chunk-cache budget. Tests set `JIHUAN_ACTIVE_CHUNK_CACHE_BYTES`
/// to a small value to exercise the eviction + pread-fallback path without
/// having to write tens of MB per assertion. Production callers never set it.
fn active_chunk_cache_budget() -> usize {
    std::env::var("JIHUAN_ACTIVE_CHUNK_CACHE_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_ACTIVE_CHUNK_CACHE_BYTES)
}

/// LRU-ordered, byte-budgeted cache of recently-written chunks for the
/// active (unsealed) block. On eviction the chunk data is **not** lost —
/// reads just fall through to a direct pread against the active block
/// file (see [`Engine::read_active_chunk_from_disk`]).
struct BoundedChunkCache {
    inner: lru::LruCache<u64, CachedChunk>,
    bytes_used: usize,
    max_bytes: usize,
}

impl BoundedChunkCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            inner: lru::LruCache::unbounded(),
            bytes_used: 0,
            max_bytes: max_bytes.max(1),
        }
    }

    fn insert(&mut self, offset: u64, chunk: CachedChunk) {
        let sz = chunk.data.len();
        if sz == 0 {
            // Empty-chunk case (empty files) — record presence without
            // consuming byte budget. Useful for the `put_stream` empty-file
            // path which still emits one zero-byte chunk.
            self.inner.put(offset, chunk);
            return;
        }
        if sz > self.max_bytes {
            // A single chunk bigger than the whole budget — we refuse to
            // cache it rather than evict everything. Reads will fall back
            // to pread on the active block file.
            return;
        }
        // Evict until there's room for the new entry.
        while self.bytes_used + sz > self.max_bytes {
            match self.inner.pop_lru() {
                Some((_, c)) => {
                    self.bytes_used = self.bytes_used.saturating_sub(c.data.len());
                }
                None => break,
            }
        }
        if let Some(old) = self.inner.put(offset, chunk) {
            self.bytes_used = self.bytes_used.saturating_sub(old.data.len());
        }
        self.bytes_used += sz;
    }

    fn get(&mut self, offset: &u64) -> Option<&CachedChunk> {
        self.inner.get(offset)
    }

    #[cfg(test)]
    fn bytes_used(&self) -> usize {
        self.bytes_used
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

struct ActiveBlock {
    writer: BlockWriter,
    /// Byte-budgeted cache of chunks written to this block. Eviction
    /// falls through to a pread on the block file — see
    /// [`Engine::read_active_chunk_from_disk`].
    chunk_cache: BoundedChunkCache,
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

        // Crash recovery: sweep incomplete block files. Never deletes a
        // block that metadata still references — those get quarantined to
        // `.blk.orphan` so data is preserved for recovery.
        let report = cleanup_incomplete_blocks(&config.storage.data_dir, &meta)?;
        if report.deleted_orphans > 0 || report.quarantined_referenced > 0 {
            tracing::warn!(
                deleted_orphans = report.deleted_orphans,
                quarantined_referenced = report.quarantined_referenced,
                "Crash recovery: block-file cleanup"
            );
        }

        // Optional repair pass: `JIHUAN_REPAIR=1` on startup strips metadata
        // rows whose block file is missing (or was just quarantined). This
        // is destructive — files that referenced the missing block will
        // disappear from `/api/v1/files` — but it restores consistency so
        // reads of *other* files stop failing mysteriously with 500.
        if std::env::var("JIHUAN_REPAIR").ok().as_deref() == Some("1") {
            let summary = Self::repair_static(&meta, &config.storage.data_dir)?;
            tracing::warn!(
                files_removed = summary.files_removed,
                blocks_removed = summary.blocks_removed,
                dedup_removed = summary.dedup_removed,
                "JIHUAN_REPAIR=1: purged dangling metadata"
            );
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
            stats_cache: Arc::new(Mutex::new(None)),
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
    ///
    /// Phase 6b-P1: this now delegates to [`put_stream`] so the in-RAM
    /// footprint is bounded to one `chunk_size` regardless of `data.len()`,
    /// unifying the historical `put_reader` / `put_stream` paths that both
    /// used to chunk-then-write. The byte-slice variant keeps its precise
    /// quota check (we know the exact size, unlike a live `Read`).
    pub fn put_bytes(
        &self,
        data: &[u8],
        file_name: &str,
        content_type: Option<&str>,
    ) -> Result<String> {
        // Precise quota check — we know the exact payload size here.
        if let Some(cap) = self.config.storage.max_storage_bytes {
            let used = crate::utils::disk_usage_bytes(&self.config.storage.data_dir);
            let needed = data.len() as u64;
            if used.saturating_add(needed) > cap {
                return Err(JiHuanError::StorageFull {
                    available: cap.saturating_sub(used),
                    needed,
                });
            }
        }
        self.put_stream(std::io::Cursor::new(data), file_name, content_type)
    }

    /// Store a file from a streaming reader. Memory usage is bounded to
    /// `storage.chunk_size` (default 4 MB) regardless of file size.
    /// This is the preferred entry-point for large uploads.
    pub fn put_stream<R: Read>(
        &self,
        mut reader: R,
        file_name: &str,
        content_type: Option<&str>,
    ) -> Result<String> {
        let t0 = std::time::Instant::now();
        let chunk_size = self.config.storage.chunk_size as usize;
        let algo = self.config.storage.hash_algorithm;

        // Pre-upload quota check. Performed once per call: we measure current
        // on-disk bytes and reject when already at/over the configured cap.
        // We don't know the incoming stream length here (it may come from an
        // axum body), so "needed" is reported as 0 — the `available` figure
        // still gives the operator actionable context in the error.
        //
        // Known limitation: concurrent uploads can each observe the same
        // `available` and all proceed. This is acceptable for a single-tenant
        // deployment; a precise enforcer would use an atomic reservation.
        if let Some(cap) = self.config.storage.max_storage_bytes {
            let used = crate::utils::disk_usage_bytes(&self.config.storage.data_dir);
            if used >= cap {
                return Err(JiHuanError::StorageFull {
                    available: 0,
                    needed: 0,
                });
            }
        }

        let file_id = new_id();
        let create_time = now_secs();
        let partition_id = current_partition_id(self.config.storage.time_partition_hours);

        let mut chunk_metas: Vec<ChunkMeta> = Vec::new();
        let mut file_size: u64 = 0;
        let mut index: u32 = 0;
        let mut buf = vec![0u8; chunk_size];

        loop {
            let mut total_read = 0usize;
            loop {
                match reader.read(&mut buf[total_read..]) {
                    Ok(0) => break,
                    Ok(n) => {
                        total_read += n;
                        if total_read == chunk_size {
                            break;
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(JiHuanError::Io(e)),
                }
            }

            if total_read == 0 {
                if index == 0 {
                    // Empty file: emit a single empty chunk for consistency with chunk_data()
                    let digest = crate::dedup::ChunkDigest::compute(&[], algo);
                    let raw = RawChunk {
                        index: 0,
                        data: bytes::Bytes::new(),
                        digest,
                    };
                    let cm = self.store_chunk(&raw)?;
                    chunk_metas.push(cm);
                }
                break;
            }

            file_size += total_read as u64;
            let slice = &buf[..total_read];
            let digest = crate::dedup::ChunkDigest::compute(slice, algo);
            let raw = RawChunk {
                index,
                data: bytes::Bytes::copy_from_slice(slice),
                digest,
            };
            let cm = self.store_chunk(&raw)?;
            chunk_metas.push(cm);

            index += 1;
            if total_read < chunk_size {
                break;
            }
        }

        let file_meta = FileMeta {
            file_id: file_id.clone(),
            file_name: file_name.to_string(),
            file_size,
            create_time,
            partition_id,
            chunks: chunk_metas,
            content_type: content_type.map(|s| s.to_string()),
        };

        self.wal.lock().append(WalOperation::InsertFile {
            file_id: file_id.clone(),
        })?;
        self.meta.insert_file(&file_meta)?;

        metrics::counter!("jihuan_puts_total").increment(1);
        metrics::counter!("jihuan_bytes_written_total").increment(file_size);
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
                chunk_cache: BoundedChunkCache::new(active_chunk_cache_budget()),
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

    /// Graceful shutdown: seal the active block and fsync the WAL.
    ///
    /// Call this **before** the process exits. The `Drop` impl does the
    /// same work, but is unreliable in practice because signals (SIGINT /
    /// Ctrl-C on Windows) terminate the process without running destructors
    /// of values held in long-lived `Arc`s — exactly the kind of shutdown
    /// that used to destroy the active block's data.
    ///
    /// Errors are logged but never propagated: partial success is better
    /// than aborting shutdown and leaving the process hanging.
    pub fn shutdown(&self) {
        tracing::info!("Engine::shutdown — sealing active block and syncing WAL");
        if let Err(e) = self.seal_active_block() {
            tracing::error!(error = %e, "shutdown: failed to seal active block");
        }
        if let Err(e) = self.wal.lock().sync() {
            tracing::error!(error = %e, "shutdown: failed to sync WAL");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Repair (Phase 2.7 data-loss recovery)
    // ─────────────────────────────────────────────────────────────────────────

    /// Static flavour of [`Engine::repair`] so it can run during startup
    /// before the `Engine` itself is fully constructed. Takes an already-
    /// opened [`MetadataStore`] and the configured data directory.
    ///
    /// Purges three classes of dangling metadata:
    ///
    /// 1. **Files** whose chunks reference a block whose `.blk` file is
    ///    missing from disk (typically because a prior version of this
    ///    codebase deleted an unsealed active block during startup).
    /// 2. **Block metadata rows** whose `.blk` file is missing.
    /// 3. **Dedup entries** that pointed at those missing blocks.
    ///
    /// Guarantees:
    /// * A file is removed only if **at least one** of its chunks lives in
    ///   a missing block — it is un-readable anyway, so removing it
    ///   restores list consistency without further data loss.
    /// * A block row is removed only when the file is missing from disk.
    ///   Quarantined `.blk.orphan` files keep their metadata row so a
    ///   future manual recovery can reattach them.
    /// * Dedup entries to missing blocks are removed unconditionally —
    ///   leaving them would make subsequent uploads silently dedupe
    ///   against a block that can never be read.
    pub fn repair_static(
        meta: &MetadataStore,
        data_dir: &std::path::Path,
    ) -> Result<RepairSummary> {
        let mut summary = RepairSummary::default();

        // Build the set of on-disk block_ids (sealed only — .blk files).
        // We tolerate missing data_dir gracefully so repair is idempotent.
        let mut present: std::collections::HashSet<String> = std::collections::HashSet::new();
        if data_dir.exists() {
            for entry in walkdir::WalkDir::new(data_dir)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_type().is_file()
                        && e.path().extension().map(|x| x == "blk").unwrap_or(false)
                })
            {
                if let Some(stem) = entry.path().file_stem().and_then(|s| s.to_str()) {
                    present.insert(stem.to_string());
                }
            }
        }

        // Build the set of block_ids we consider "missing": has a metadata
        // row but no `.blk` file on disk. These are the dangling references.
        let blocks = meta.list_all_blocks()?;
        let mut missing: std::collections::HashSet<String> = std::collections::HashSet::new();
        for b in &blocks {
            if !present.contains(&b.block_id) {
                missing.insert(b.block_id.clone());
            }
        }

        if missing.is_empty() {
            return Ok(summary);
        }

        tracing::warn!(
            missing_blocks = missing.len(),
            "Repair: found block metadata rows with no .blk file on disk"
        );

        // 1) Remove files whose chunks reference any missing block.
        for f in meta.list_all_files()? {
            let references_missing = f.chunks.iter().any(|c| missing.contains(&c.block_id));
            if references_missing {
                if let Err(e) = meta.delete_file(&f.file_id) {
                    tracing::error!(file_id=%f.file_id, error=%e, "Repair: delete_file failed");
                } else {
                    tracing::warn!(
                        file_id = %f.file_id,
                        file_name = %f.file_name,
                        "Repair: removed file referencing missing block"
                    );
                    summary.files_removed += 1;
                }
            }
        }

        // 2) Remove dedup entries pointing at missing blocks.
        for (hash, block_id) in meta.list_dedup_hash_block_pairs()? {
            if missing.contains(&block_id) {
                if let Err(e) = meta.remove_dedup_entry(&hash) {
                    tracing::error!(hash=%hash, error=%e, "Repair: remove_dedup_entry failed");
                } else {
                    summary.dedup_removed += 1;
                }
            }
        }

        // 3) Remove block metadata rows for the missing blocks. We defer
        //    this to last so step (1) can still recognise missing blocks.
        for block_id in &missing {
            if let Err(e) = meta.delete_block(block_id) {
                tracing::error!(block_id=%block_id, error=%e, "Repair: delete_block failed");
            } else {
                summary.blocks_removed += 1;
            }
        }

        Ok(summary)
    }

    /// Instance-level wrapper around [`Engine::repair_static`] for callers
    /// that already hold a live engine. Can be driven from an admin RPC
    /// in the future; currently invoked only via `JIHUAN_REPAIR=1` on
    /// startup.
    pub fn repair(&self) -> Result<RepairSummary> {
        Self::repair_static(&self.meta, &self.config.storage.data_dir)
    }

    /// Test-only: wipe the active block's chunk cache to force the next read
    /// through the disk-fallback path. Used by Phase 6b-P6 eviction tests so
    /// we don't need to write tens of MB to trigger eviction organically.
    #[cfg(test)]
    pub(crate) fn clear_active_chunk_cache_for_test(&self) {
        let mut guard = self.active_writer.lock();
        if let Some(ab) = guard.as_mut() {
            ab.chunk_cache = BoundedChunkCache::new(
                ab.chunk_cache.max_bytes.max(DEFAULT_ACTIVE_CHUNK_CACHE_BYTES),
            );
        }
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

        // Chunks are ordered by index; read them in order.
        let mut sorted_chunks = file_meta.chunks.clone();
        sorted_chunks.sort_by_key(|c| c.index);

        for chunk_meta in &sorted_chunks {
            let data = self.read_chunk_bytes(chunk_meta, verify)?;
            result.extend_from_slice(&data);
        }

        metrics::counter!("jihuan_gets_total").increment(1);
        metrics::counter!("jihuan_bytes_read_total").increment(result.len() as u64);
        metrics::histogram!("jihuan_get_duration_seconds").record(t0.elapsed().as_secs_f64());

        Ok(result)
    }

    /// Read a single chunk directly from the **active (unsealed)** block file.
    ///
    /// Used when the in-memory chunk cache has evicted this chunk to respect
    /// the byte budget (Phase 6b-P6). Because the block has no footer yet we
    /// can't go through `BlockReader`; instead we open a fresh read handle,
    /// seek to the known offset, read exactly `compressed_size` bytes, and
    /// decompress using the engine's configured algorithm.
    ///
    /// The writer must have been flushed before this runs — the caller in
    /// `read_chunk_bytes` takes care of that under the `active_writer` lock.
    fn read_active_chunk_from_disk(
        &self,
        block_path: &std::path::Path,
        chunk_meta: &crate::metadata::types::ChunkMeta,
    ) -> Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(block_path).map_err(JiHuanError::Io)?;
        f.seek(SeekFrom::Start(chunk_meta.offset))
            .map_err(JiHuanError::Io)?;
        let mut compressed = vec![0u8; chunk_meta.compressed_size as usize];
        f.read_exact(&mut compressed).map_err(JiHuanError::Io)?;
        crate::compression::decompress(
            &compressed,
            self.config.storage.compression_algorithm,
            chunk_meta.original_size as usize,
        )
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

    /// Retrieve a byte range `[start, end]` (inclusive on both ends) of a file.
    ///
    /// Only the chunks that overlap the requested range are fetched and
    /// decompressed; chunks outside the range are skipped entirely. This is
    /// the primitive backing HTTP `Range: bytes=…` / `curl -C -` / `aria2c -s`.
    ///
    /// Returns `InvalidArgument` if the range is unsatisfiable (`start > end`,
    /// or `start >= file_size`).
    pub fn get_range(&self, file_id: &str, start: u64, end: u64) -> Result<Vec<u8>> {
        if start > end {
            return Err(JiHuanError::InvalidArgument(format!(
                "range start {} > end {}",
                start, end
            )));
        }

        let t0 = std::time::Instant::now();

        let file_meta = self
            .meta
            .get_file(file_id)?
            .ok_or_else(|| JiHuanError::NotFound(format!("File '{}' not found", file_id)))?;

        if file_meta.file_size == 0 || start >= file_meta.file_size {
            return Err(JiHuanError::InvalidArgument(format!(
                "range start {} is beyond file size {}",
                start, file_meta.file_size
            )));
        }
        // Clamp end to last valid byte
        let end = end.min(file_meta.file_size - 1);
        let wanted_len = (end - start + 1) as usize;

        let verify = self.config.storage.verify_on_read;

        // Chunks are ordered by index
        let mut sorted_chunks = file_meta.chunks.clone();
        sorted_chunks.sort_by_key(|c| c.index);

        let mut result = Vec::with_capacity(wanted_len);
        let mut cursor: u64 = 0; // byte position of the first byte in the current chunk

        for chunk_meta in &sorted_chunks {
            let chunk_start = cursor;
            let chunk_end = cursor + chunk_meta.original_size; // exclusive
            cursor = chunk_end;

            if chunk_end <= start {
                continue; // entirely before requested range
            }
            if chunk_start > end {
                break; // entirely after requested range
            }

            // Fetch the full chunk bytes (active cache or sealed block)
            let chunk_data = self.read_chunk_bytes(chunk_meta, verify)?;

            // Slice the overlap
            let lo = start.saturating_sub(chunk_start) as usize;
            let hi = ((end + 1).min(chunk_end) - chunk_start) as usize;
            result.extend_from_slice(&chunk_data[lo..hi]);
        }

        metrics::counter!("jihuan_gets_total").increment(1);
        metrics::counter!("jihuan_bytes_read_total").increment(result.len() as u64);
        metrics::histogram!("jihuan_get_duration_seconds").record(t0.elapsed().as_secs_f64());

        Ok(result)
    }

    /// Helper: fetch the full bytes of a single chunk, honoring the active-block
    /// chunk cache and the sealed-block reader cache. Used by both `get_bytes`
    /// and `get_range`.
    fn read_chunk_bytes(
        &self,
        chunk_meta: &crate::metadata::types::ChunkMeta,
        verify: bool,
    ) -> Result<Vec<u8>> {
        // ── Fast path: active (unsealed) block's in-memory cache ────────────
        //
        // We also capture the on-disk path under the same lock so that, on a
        // cache miss, we can fall through to a direct pread without sealing
        // the block (Phase 6b-P6). Capturing the path + flushing the writer
        // under the same lock is critical: it guarantees every cached byte
        // is durable on disk by the time we open a fresh read handle.
        let active_miss: Option<std::path::PathBuf> = {
            let mut guard = self.active_writer.lock();
            match guard.as_mut() {
                Some(ab) if ab.writer.block_id() == chunk_meta.block_id => {
                    if let Some(cached) = ab.chunk_cache.get(&chunk_meta.offset) {
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
                        return Ok(cached.data.clone());
                    }
                    // Cache miss: flush and hand back the path for a pread.
                    ab.writer.flush()?;
                    Some(ab.writer.path().to_path_buf())
                }
                _ => None,
            }
        };

        if let Some(active_path) = active_miss {
            let data = self.read_active_chunk_from_disk(&active_path, chunk_meta)?;
            if verify && !chunk_meta.hash.is_empty() {
                let actual = hash_chunk(&data, self.config.storage.hash_algorithm);
                if actual != chunk_meta.hash {
                    return Err(JiHuanError::ChecksumMismatch {
                        expected: chunk_meta.hash.clone(),
                        actual,
                    });
                }
            }
            return Ok(data);
        }

        // ── Slow path: sealed block via reader cache ────────────────────────
        let block_info = self.meta.get_block(&chunk_meta.block_id)?.ok_or_else(|| {
            JiHuanError::NotFound(format!(
                "Block '{}' not found for chunk at offset {}",
                chunk_meta.block_id, chunk_meta.offset
            ))
        })?;

        let data = self.read_chunk_from_block(
            std::path::Path::new(&block_info.path),
            chunk_meta.offset,
            verify,
        )?;

        if verify && !chunk_meta.hash.is_empty() {
            let actual = hash_chunk(&data, self.config.storage.hash_algorithm);
            if actual != chunk_meta.hash {
                return Err(JiHuanError::ChecksumMismatch {
                    expected: chunk_meta.hash.clone(),
                    actual,
                });
            }
        }
        Ok(data)
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

    /// List all files (for admin/UI use). Sorted newest-first.
    pub fn list_all_files(&self) -> Result<Vec<crate::metadata::types::FileMeta>> {
        self.meta.list_all_files()
    }

    /// Collect a lightweight stats snapshot without any Prometheus dependency.
    ///
    /// Results are cached for [`STATS_CACHE_TTL_MS`] milliseconds so that
    /// polling consumers (`/api/status`, `/api/metrics`, the Dashboard) pay
    /// the directory-walk + file-list cost at most once per TTL. The cache
    /// is invalidated implicitly by expiry, not by writers — acceptable
    /// because the snapshot is already approximate (active block, OS page
    /// cache, FS overhead).
    pub fn stats(&self) -> Result<crate::metrics::EngineStats> {
        // Fast path: return a still-fresh cached snapshot.
        {
            let guard = self.stats_cache.lock();
            if let Some(cached) = guard.as_ref() {
                if cached.at.elapsed().as_millis() < STATS_CACHE_TTL_MS {
                    return Ok(cached.snapshot.clone());
                }
            }
        }

        let file_count = self.meta.file_count()?;
        let block_count = self.meta.block_count()?;

        // Flush the active block's BufWriter so on-disk sizes reflect buffered bytes.
        // This is safe: flush() only pushes the buffered bytes through to the File
        // without sealing the block.
        let _ = self.flush_active_block();

        let disk_usage_bytes = crate::utils::disk_usage_bytes(&self.config.storage.data_dir);

        // Logical bytes = sum of user-visible file sizes. Using file metadata
        // (not block sizes) because that's the number operators expect to see
        // — "how much data have I uploaded" — and it's also the correct
        // numerator for the dedup ratio: identical files contribute their
        // full size to `logical` but share a single set of on-disk chunks.
        let logical_bytes: u64 = self
            .meta
            .list_all_files()?
            .iter()
            .map(|f| f.file_size)
            .sum();

        let dedup_ratio = if disk_usage_bytes == 0 {
            1.0
        } else {
            (logical_bytes as f64 / disk_usage_bytes as f64).max(1.0)
        };

        let snapshot = crate::metrics::EngineStats {
            file_count,
            block_count,
            logical_bytes,
            disk_usage_bytes,
            dedup_ratio,
        };

        // Store for subsequent fast-path reads. Cloning is cheap — EngineStats
        // is a handful of u64 / f64.
        *self.stats_cache.lock() = Some(CachedStats {
            at: Instant::now(),
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    /// Invalidate the `stats()` cache. Called from writer paths that make a
    /// material change (large upload / delete / GC) and want the next stats
    /// call to reflect reality immediately instead of waiting for TTL.
    pub fn invalidate_stats_cache(&self) {
        *self.stats_cache.lock() = None;
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
    fn test_stats_cache_returns_same_snapshot_within_ttl() {
        // Phase 6b-P4: two consecutive stats() calls inside the TTL must be
        // byte-identical without the second call re-walking `data_dir`.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        engine.put_bytes(b"abc", "a.txt", None).unwrap();

        let first = engine.stats().unwrap();
        let second = engine.stats().unwrap();
        assert_eq!(first.file_count, second.file_count);
        assert_eq!(first.disk_usage_bytes, second.disk_usage_bytes);
        assert_eq!(first.logical_bytes, second.logical_bytes);
    }

    // ── Phase 6b-P6: bounded active-block chunk cache ─────────────────────

    #[test]
    fn test_bounded_cache_respects_byte_budget() {
        let mut cache = BoundedChunkCache::new(100);
        cache.insert(0, CachedChunk { data: vec![1u8; 40] });
        cache.insert(40, CachedChunk { data: vec![2u8; 40] });
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.bytes_used(), 80);

        // This pushes over 100 → oldest (offset 0) must be evicted.
        cache.insert(80, CachedChunk { data: vec![3u8; 40] });
        assert_eq!(cache.len(), 2);
        assert!(cache.bytes_used() <= 100);
        assert!(cache.get(&0).is_none(), "LRU should have evicted offset 0");
        assert!(cache.get(&40).is_some());
        assert!(cache.get(&80).is_some());
    }

    #[test]
    fn test_bounded_cache_skips_oversized_chunk() {
        // A single chunk bigger than the whole budget: never cached,
        // existing entries preserved.
        let mut cache = BoundedChunkCache::new(50);
        cache.insert(0, CachedChunk { data: vec![1u8; 30] });
        cache.insert(30, CachedChunk { data: vec![2u8; 200] }); // oversized
        assert_eq!(cache.len(), 1);
        assert!(cache.get(&0).is_some());
        assert!(cache.get(&30).is_none());
    }

    #[test]
    fn test_read_active_block_via_disk_fallback_after_cache_eviction() {
        // Phase 6b-P6 regression: a chunk evicted from the in-memory cache
        // must still be readable from the unsealed on-disk block via the
        // pread fallback. The file we query is still in the active block
        // (never sealed), so this exercises `read_active_chunk_from_disk`.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        let payload: Vec<u8> = (0..4096u16).flat_map(|i| i.to_le_bytes()).collect();
        let file_id = engine.put_bytes(&payload, "evicted.bin", None).unwrap();

        // Simulate eviction without actually pushing past the 64 MB budget.
        engine.clear_active_chunk_cache_for_test();

        // get_bytes() must now round-trip via pread, not the cache.
        let got = engine.get_bytes(&file_id).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn test_repair_purges_files_referencing_missing_blocks() {
        // Simulate the historical data-loss bug: metadata references a
        // block whose .blk file is missing from disk. Repair must:
        //   * remove the file record (it's unreadable anyway)
        //   * remove the dangling block metadata row
        //   * remove any dedup entry pointing at the gone block
        // but must leave OTHER (still-valid) files alone.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        // (a) upload two files. They end up in the active block.
        let good_id = engine.put_bytes(b"good-data", "good.txt", None).unwrap();
        // Seal so this file's block lives on disk independently.
        engine.seal_active_block().unwrap();

        let lost_id = engine.put_bytes(b"lost-data", "lost.txt", None).unwrap();
        engine.seal_active_block().unwrap();

        // (b) identify the block backing `lost_id` and delete its .blk file
        //     to simulate the historical bug.
        let lost_meta = engine.metadata().get_file(&lost_id).unwrap().unwrap();
        let lost_block_id = lost_meta.chunks[0].block_id.clone();
        let lost_block_row = engine
            .metadata()
            .get_block(&lost_block_id)
            .unwrap()
            .unwrap();
        std::fs::remove_file(&lost_block_row.path).unwrap();

        // Sanity: the metadata row still exists, the .blk file does not.
        assert!(engine.metadata().get_block(&lost_block_id).unwrap().is_some());
        assert!(!std::path::Path::new(&lost_block_row.path).exists());

        // (c) run repair.
        let summary = engine.repair().unwrap();
        assert!(summary.files_removed >= 1);
        assert!(summary.blocks_removed >= 1);

        // (d) verify: lost file is gone, good file still works.
        assert!(engine.metadata().get_file(&lost_id).unwrap().is_none());
        assert!(engine.metadata().get_block(&lost_block_id).unwrap().is_none());
        let got = engine.get_bytes(&good_id).unwrap();
        assert_eq!(got, b"good-data");
    }

    #[test]
    fn test_repair_noop_on_healthy_store() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        engine.put_bytes(b"x", "a.txt", None).unwrap();
        engine.seal_active_block().unwrap();

        let summary = engine.repair().unwrap();
        assert_eq!(summary.files_removed, 0);
        assert_eq!(summary.blocks_removed, 0);
        assert_eq!(summary.dedup_removed, 0);
    }

    #[test]
    fn test_stats_cache_invalidate_picks_up_changes() {
        // Explicit invalidation must let the next stats() reflect writes
        // that happened since the last snapshot.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        let empty = engine.stats().unwrap();
        assert_eq!(empty.file_count, 0);

        engine.put_bytes(b"hello", "h.txt", None).unwrap();
        engine.invalidate_stats_cache();

        let after = engine.stats().unwrap();
        assert_eq!(after.file_count, 1);
        assert!(after.logical_bytes >= 5);
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
    fn test_put_stream_small_file() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        let data = b"Streaming upload produces identical content";
        let file_id = engine
            .put_stream(std::io::Cursor::new(data), "stream.txt", Some("text/plain"))
            .unwrap();
        let got = engine.get_bytes(&file_id).unwrap();
        assert_eq!(got, data);

        let meta = engine.get_file_meta(&file_id).unwrap().unwrap();
        assert_eq!(meta.file_size, data.len() as u64);
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
    }

    #[test]
    fn test_put_stream_empty_file() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        let file_id = engine
            .put_stream(std::io::Cursor::new(b""), "empty.txt", None)
            .unwrap();
        assert_eq!(engine.get_bytes(&file_id).unwrap(), b"");
    }

    #[test]
    fn test_put_stream_multi_chunk_matches_put_bytes() {
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 512; // small chunks
        let engine = Engine::open(cfg).unwrap();

        // 5 KB of data
        let data: Vec<u8> = (0u8..=255).cycle().take(5 * 1024).collect();

        // Upload via stream
        let id_stream = engine
            .put_stream(std::io::Cursor::new(&data), "via_stream.bin", None)
            .unwrap();
        // Upload via bytes
        let id_bytes = engine.put_bytes(&data, "via_bytes.bin", None).unwrap();

        assert_eq!(engine.get_bytes(&id_stream).unwrap(), data);
        assert_eq!(engine.get_bytes(&id_bytes).unwrap(), data);

        let m_stream = engine.get_file_meta(&id_stream).unwrap().unwrap();
        let m_bytes = engine.get_file_meta(&id_bytes).unwrap().unwrap();
        // Same number of chunks and identical hashes at each index (content-addressed).
        assert_eq!(m_stream.chunks.len(), m_bytes.chunks.len());
        for (a, b) in m_stream.chunks.iter().zip(m_bytes.chunks.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.original_size, b.original_size);
        }
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
