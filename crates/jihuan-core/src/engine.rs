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
use crate::utils::{
    current_partition_id, ensure_dir, new_id, normalize_and_validate_file_id, now_secs,
};
use crate::wal::{WalOperation, WriteAheadLog};

/// v0.4.6: Policy for resolving a client-supplied `file_id` that already
/// exists in the store. Selected by HTTP multipart `on_conflict` field,
/// gRPC `PutConflictPolicy` enum, or CLI `--on-conflict`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// Reject the upload with [`JiHuanError::AlreadyExists`]. HTTP layer
    /// maps this to `409 Conflict`. **Default** — explicit is safer.
    Error,
    /// Keep the existing file and discard the new upload silently.
    /// Returns the pre-existing `file_id` and [`PutOutcome::Skipped`].
    /// Useful for retry idempotency when the client can't tell whether
    /// a previous attempt succeeded.
    Skip,
    /// Replace the existing file's content with the uploaded bytes.
    /// Old block ref-counts are released atomically; if the new content
    /// is identical, dedup makes the whole thing a no-op on the block
    /// layer.
    Overwrite,
}

impl Default for ConflictPolicy {
    fn default() -> Self {
        Self::Error
    }
}

/// v0.4.6: outcome tag returned alongside the `file_id` from
/// [`Engine::put_stream_with`] / [`Engine::put_bytes_with`]. Clients use
/// it to distinguish "newly created" from "returned the pre-existing
/// record" without having to compare the `file_id` to whatever they
/// supplied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    Skipped,
    Overwritten,
}

impl PutOutcome {
    /// Stable snake-case rendering used by the HTTP / gRPC / CLI layers.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Skipped => "skipped",
            Self::Overwritten => "overwritten",
        }
    }
}

/// v0.4.6: optional upload parameters carried through the put path.
///
/// The legacy `put_bytes(data, name, content_type)` / `put_stream(reader,
/// name, content_type)` entry points remain for callers that don't care
/// about custom ids; they delegate to `put_*_with` with `file_id = None`
/// and `on_conflict = Error`.
#[derive(Debug, Clone)]
pub struct PutOptions<'a> {
    pub file_name: &'a str,
    pub content_type: Option<&'a str>,
    /// If `Some`, use this id instead of generating a UUID. Subject to
    /// [`normalize_and_validate_file_id`] rules.
    pub file_id: Option<&'a str>,
    pub on_conflict: ConflictPolicy,
}

impl<'a> PutOptions<'a> {
    /// Shorthand for the legacy "always generate a new id, fail on collision"
    /// behaviour. Used by the thin-wrapper `put_bytes` / `put_stream` impls.
    pub fn new(file_name: &'a str, content_type: Option<&'a str>) -> Self {
        Self {
            file_name,
            content_type,
            file_id: None,
            on_conflict: ConflictPolicy::Error,
        }
    }
}

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
    /// Block IDs currently being written to. Shared with the GC service so
    /// that GC skips these even when their ref_count momentarily sits at 0
    /// (between `insert_block` and the first chunk's ref-count increment).
    /// See [`crate::gc::PinnedBlocks`] for the full rationale.
    pinned_blocks: crate::gc::PinnedBlocks,
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

/// v0.4.2 P5 scratch space carried through a single file upload.
///
/// Every chunk written or dedup-hit during a `put_stream` call appends
/// into these collections; the final `MetadataStore::commit_file_batch`
/// drains them in one redb write transaction. The net effect on a
/// file-with-N-chunks upload is 1 metadata fsync instead of ~2N+1.
#[derive(Default)]
struct FileBatch {
    /// `block_id → net ref-count delta`. Summed so multiple chunks
    /// pointing at the same block collapse into a single map entry
    /// with `delta = #chunks`.
    ref_count_deltas: std::collections::HashMap<String, i64>,
    /// New dedup rows that still need to be persisted. Caller guarantees
    /// none are already in the on-disk DEDUP_TABLE — this matches the
    /// `commit_file_batch` contract.
    new_dedup_entries: Vec<DedupEntry>,
    /// In-flight dedup overlay: content-hash → already-written location
    /// within **this** upload. Needed because `new_dedup_entries` aren't
    /// visible via `get_dedup_entry` until commit.
    overlay: std::collections::HashMap<String, OverlayEntry>,
}

#[derive(Clone)]
struct OverlayEntry {
    block_id: String,
    offset: u64,
    original_size: u64,
    compressed_size: u64,
}

/// Info about a block that was just sealed by [`Engine::seal_active_block`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SealedBlockInfo {
    pub block_id: String,
    pub size: u64,
}

/// Per-block statistics returned by [`Engine::compact_block`].
///
/// Serialisable so that the HTTP admin endpoint can round-trip the value
/// straight back to the operator/UI.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompactionBlockStats {
    pub old_block_id: String,
    /// `None` when the old block had no live chunks (i.e. it was already
    /// a GC candidate and we skipped creating a new block).
    pub new_block_id: Option<String>,
    pub old_size_bytes: u64,
    pub new_size_bytes: u64,
    /// Signed because a tiny block with incompressible content can grow
    /// slightly after re-encode; the operator should still see the real
    /// delta.
    pub bytes_saved: i64,
    pub live_chunks: u64,
    pub dropped_chunks: u64,
}

/// v0.4.5: per-block live-byte / live-hash view used by the compaction
/// scanner and by the admin block-listing API.
///
/// Deliberately *not* serialisable — callers project the `live_bytes` scalar
/// into their own DTOs (`BlockInfoDto` / stats structs). Keeping the
/// `live_hashes` set internal avoids shipping potentially large hash lists
/// over the wire on every `/api/block/list` poll.
#[derive(Debug, Clone)]
pub struct BlockLiveInfo {
    /// Sum of `compressed_size` over **unique** chunk hashes referenced by
    /// any live file. See [`Engine::compute_block_live_info`] for dedup
    /// rationale.
    pub live_bytes: u64,
    /// The set of unique hashes referenced. Consumed by
    /// `compact_merge_group` to decide which chunks to copy over.
    ///
    /// Stored as `Arc<str>` rather than `String` so the downstream
    /// `compact_low_utilization` scanner can `.clone()` this set into the
    /// candidate tuple without re-allocating every hash — a cheap refcount
    /// bump instead of N String allocations. Matters on dedup-heavy tiers
    /// where a single candidate block can easily hold 10k+ unique hashes.
    pub live_hashes: std::collections::HashSet<std::sync::Arc<str>>,
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

/// v0.4.5: `(source_block_meta, live_bytes, live_hashes)` triple used by
/// the cross-block compaction merger. Named to appease clippy's
/// `type_complexity` lint and to make the many `Vec<...>` / `&[...]`
/// signatures in `engine.rs` readable.
type CompactSource = (BlockMeta, u64, std::collections::HashSet<std::sync::Arc<str>>);

/// v0.4.7: why the scanner selected a block as a compaction candidate.
/// Carried alongside each [`CompactSource`] in a parallel map so the
/// group commit logic can decide whether Strategy A (low-util),
/// Strategy B (undersized), or both justified the merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateReason {
    LowUtil,
    Undersized,
    Both,
}

impl CandidateReason {
    fn as_label(self) -> &'static str {
        match self {
            CandidateReason::LowUtil => "low_util",
            CandidateReason::Undersized => "undersized",
            CandidateReason::Both => "both",
        }
    }

    fn has_low_util(self) -> bool {
        matches!(self, CandidateReason::LowUtil | CandidateReason::Both)
    }
}

/// v0.4.7: knobs that drive a compaction pass. Built from
/// [`AppConfig`](crate::config::AppConfig) for the background loop via
/// [`CompactionOptions::from_config`], and patched per-call by the
/// `/api/admin/compact` admin endpoint.
#[derive(Debug, Clone)]
pub struct CompactionOptions {
    /// Strategy A candidate gate: `util = live/size < threshold`
    /// makes a block eligible. A group containing any A-candidate is
    /// always committed (Strategy A has no separate commit threshold;
    /// tune aggressiveness via this single knob).
    pub threshold: f64,
    /// Strategy B candidate gate: `size < block_file_size * ratio`
    /// makes a block eligible even at full utilisation.
    pub undersize_ratio: f64,
    /// Strategy B commit gate: a group must reduce the sealed-block
    /// count by at least this many files. Zero disables the gate
    /// (useful for legacy low-util-only tests).
    pub min_file_saved: u64,
    /// Anti-thrash filter: blocks younger than this are skipped by
    /// both candidate predicates. Zero disables.
    pub min_block_age_secs: u64,
    /// Minimum free disk bytes required above group live-sum before
    /// a merge is allowed to write. Zero disables. Always respected,
    /// even under `force = true`.
    pub disk_headroom_bytes: u64,
    /// Bypass both Strategy A and Strategy B commit rules, committing
    /// any non-empty packed group. Intended for operator use via the
    /// admin endpoint during maintenance windows. Does NOT bypass
    /// disk-headroom or age filters.
    pub force: bool,
}

impl CompactionOptions {
    /// Read defaults straight from the engine config. This is what
    /// the background loop uses every tick.
    pub fn from_config(cfg: &crate::config::AppConfig) -> Self {
        let s = &cfg.storage;
        Self {
            threshold: s.auto_compact_threshold,
            undersize_ratio: s.auto_compact_undersize_ratio,
            min_file_saved: s.auto_compact_min_file_saved,
            min_block_age_secs: s.auto_compact_min_block_age_secs,
            disk_headroom_bytes: s.auto_compact_disk_headroom_bytes,
            force: false,
        }
    }
}

/// Result of [`Engine::reseal_orphan_block_static`].
///
/// `chunks_restored` is the number of **unique** chunk hashes whose
/// `ChunkEntry` rows we rebuilt in the new index table. Files that dedup
/// against this block will all point back at those same entries after
/// the rename, so `chunks_restored` may be less than the number of
/// `FileMeta`-level chunk references that the operator expects.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct ResealSummary {
    pub block_id: String,
    pub chunks_restored: u64,
    /// Size of the resealed `.blk` after truncation + footer write.
    pub final_size: u64,
    /// Bytes dropped from the end of the orphan file because they were
    /// part of a chunk that never made it into metadata (partial write
    /// at crash time). Non-zero indicates those bytes are permanently
    /// gone; all metadata-referenced bytes survived.
    pub trailing_bytes_truncated: u64,
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
            audit_retention_days: config.auth.audit_retention_days,
        };
        let pinned_blocks = crate::gc::new_pinned_blocks();
        let gc = Arc::new(GcService::new(meta.clone(), gc_config, pinned_blocks.clone()));

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

        // v0.4.7 compaction metrics
        metrics::describe_counter!(
            "jihuan_compactions_total",
            "Total compaction groups committed (labelled by strategy: low_util/undersized/mixed)"
        );
        metrics::describe_counter!(
            "jihuan_compactions_skipped_total",
            "Total groups skipped during compaction (labelled by reason: \
             below_file_benefit/disk_headroom/block_too_young)"
        );
        metrics::describe_counter!(
            "jihuan_compactions_bytes_saved_total",
            "Total on-disk bytes reclaimed by compaction"
        );
        metrics::describe_counter!(
            "jihuan_compactions_files_reduced_total",
            "Total net reduction in sealed block files produced by compaction"
        );
        metrics::describe_counter!(
            "jihuan_compactions_passthrough_chunks_total",
            "Chunks copied verbatim during compaction (same-algorithm passthrough)"
        );
        metrics::describe_counter!(
            "jihuan_compactions_recompressed_chunks_total",
            "Chunks that required decompress+recompress during compaction \
             (cross-algorithm migration)"
        );

        tracing::info!("JiHuan engine opened successfully");

        Ok(Self {
            config,
            meta,
            wal,
            active_writer: Arc::new(Mutex::new(None)),
            reader_cache,
            gc,
            pinned_blocks,
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

    /// v0.4.7: start the background auto-compaction loop, if enabled in
    /// config. Returns `None` when `storage.auto_compact_enabled = false`.
    ///
    /// The cadence is `gc_interval_secs * auto_compact_every_gc_ticks`, so
    /// the default shipped config (300 s × 12) fires once an hour. Each
    /// tick calls `compact_with(CompactionOptions::from_config(cfg))` to
    /// run the dual-strategy scanner. Errors are logged but never break
    /// the loop.
    pub fn start_auto_compaction(
        self: &Arc<Self>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let s = &self.config.storage;
        if !s.auto_compact_enabled {
            return None;
        }
        let period_secs = s
            .gc_interval_secs
            .saturating_mul(s.auto_compact_every_gc_ticks.max(1) as u64);
        let base_opts = CompactionOptions::from_config(&self.config);
        let engine = self.clone();
        Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(period_secs));
            // First tick fires immediately in tokio; we want to delay past
            // first GC so startup doesn't pay the compaction cost before
            // the server is even handling requests.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let e = engine.clone();
                let opts = base_opts.clone();
                let res = tokio::task::spawn_blocking(move || e.compact_with(opts)).await;
                match res {
                    Ok(Ok(stats)) => {
                        if !stats.is_empty() {
                            let total_saved: i64 = stats.iter().map(|s| s.bytes_saved).sum();
                            tracing::info!(
                                blocks_compacted = stats.len(),
                                total_bytes_saved = total_saved,
                                "auto-compaction tick completed"
                            );
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "auto-compaction tick failed");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "auto-compaction tick panicked");
                    }
                }
            }
        }))
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Write path
    // ─────────────────────────────────────────────────────────────────────────

    /// Store a file from a byte slice. Returns the assigned file_id.
    ///
    /// Phase 6b-P1: delegates to [`put_stream`] so the in-RAM footprint
    /// is bounded to one `chunk_size` regardless of `data.len()`. The
    /// byte-slice variant keeps its precise quota check (we know the
    /// exact size, unlike a live `Read`).
    ///
    /// **v0.4.6:** legacy signature preserved. For custom `file_id` or
    /// conflict-policy control, call [`Self::put_bytes_with`] instead.
    pub fn put_bytes(
        &self,
        data: &[u8],
        file_name: &str,
        content_type: Option<&str>,
    ) -> Result<String> {
        self.put_bytes_with(data, PutOptions::new(file_name, content_type))
            .map(|(id, _)| id)
    }

    /// Store a file from a streaming reader. Memory usage is bounded to
    /// `storage.chunk_size` (default 4 MB) regardless of file size.
    /// This is the preferred entry-point for large uploads.
    ///
    /// **v0.4.6:** legacy signature preserved. For custom `file_id` or
    /// conflict-policy control, call [`Self::put_stream_with`] instead.
    pub fn put_stream<R: Read>(
        &self,
        reader: R,
        file_name: &str,
        content_type: Option<&str>,
    ) -> Result<String> {
        self.put_stream_with(reader, PutOptions::new(file_name, content_type))
            .map(|(id, _)| id)
    }

    /// v0.4.6: byte-slice variant of [`Self::put_stream_with`]. Performs
    /// a precise pre-upload quota check before streaming into the engine.
    pub fn put_bytes_with(
        &self,
        data: &[u8],
        opts: PutOptions<'_>,
    ) -> Result<(String, PutOutcome)> {
        // Precise quota check — we know the exact payload size here.
        // Only applies to insert / overwrite; skip paths return early in
        // put_stream_with Phase 0 without writing.
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
        self.put_stream_with(std::io::Cursor::new(data), opts)
    }

    /// v0.4.6: streaming upload with optional caller-supplied `file_id`
    /// and conflict-resolution policy. See [`PutOptions`] /
    /// [`ConflictPolicy`].
    ///
    /// Three phases — crash safety relies on their separation:
    ///
    /// **Phase 0** — pre-flight. Normalise and validate `file_id`,
    /// probe [`MetadataStore::get_file`] for a collision, and short-
    /// circuit on `Error` / `Skip` **without writing any bytes to
    /// disk**. Overwrite proceeds but remembers the old `FileMeta`.
    ///
    /// **Phase 1** — stream the incoming reader into the active block
    /// via [`Self::store_chunk_batched`], accumulating deferred ref-
    /// count / dedup changes in `FileBatch`. Identical to the
    /// pre-v0.4.6 path.
    ///
    /// **Phase 2** — commit atomically. Insert path uses
    /// [`MetadataStore::commit_file_batch`]; overwrite path uses
    /// [`MetadataStore::commit_file_batch_swap`], which folds the old
    /// file's `-1` decrements into the new deltas in a single redb
    /// write transaction. A mid-stream crash therefore always leaves
    /// the old FileMeta intact — any bytes written in Phase 1 are just
    /// orphan slack that compaction reaps later.
    pub fn put_stream_with<R: Read>(
        &self,
        mut reader: R,
        opts: PutOptions<'_>,
    ) -> Result<(String, PutOutcome)> {
        let t0 = std::time::Instant::now();
        let chunk_size = self.config.storage.chunk_size as usize;
        let algo = self.config.storage.hash_algorithm;

        // ── Phase 0: resolve effective id + conflict policy ──────────────
        let effective_id = match opts.file_id {
            Some(raw) => normalize_and_validate_file_id(raw)?,
            None => new_id(),
        };

        // Pre-existing-row probe. When the caller supplied a custom id,
        // collisions can actually happen; when we generated a UUID we
        // still do the probe (in the astronomically unlikely case of a
        // collision) but the normal path just returns None and falls
        // through to the insert branch.
        let existing = self.meta.get_file(&effective_id)?;
        let overwrite_target: Option<FileMeta> = match (existing, opts.on_conflict) {
            (None, _) => None,
            (Some(_), ConflictPolicy::Error) => {
                metrics::counter!("jihuan_puts_conflict_total", "policy" => "error").increment(1);
                return Err(JiHuanError::AlreadyExists(format!(
                    "File '{}' already exists",
                    effective_id
                )));
            }
            (Some(_), ConflictPolicy::Skip) => {
                metrics::counter!("jihuan_puts_conflict_total", "policy" => "skip").increment(1);
                tracing::info!(
                    file_id = %effective_id,
                    "put_stream_with: conflict + skip \u{2192} zero-byte short-circuit"
                );
                return Ok((effective_id, PutOutcome::Skipped));
            }
            (Some(old), ConflictPolicy::Overwrite) => Some(old),
        };

        // Pre-upload quota check. Concurrent uploads can each observe
        // the same "available" and all proceed — acceptable for a
        // single-tenant deployment. Only runs on insert / overwrite
        // paths; skip / error returned above.
        if let Some(cap) = self.config.storage.max_storage_bytes {
            let used = crate::utils::disk_usage_bytes(&self.config.storage.data_dir);
            if used >= cap {
                return Err(JiHuanError::StorageFull {
                    available: 0,
                    needed: 0,
                });
            }
        }

        // ── Phase 1: stream the reader into the active block ────────────
        let create_time = now_secs();
        let partition_id = current_partition_id(self.config.storage.time_partition_hours);

        let mut chunk_metas: Vec<ChunkMeta> = Vec::new();
        let mut file_size: u64 = 0;
        let mut index: u32 = 0;
        let mut buf = vec![0u8; chunk_size];
        let mut batch = FileBatch::default();

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
                    let cm = self.store_chunk_batched(&raw, &mut batch)?;
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
            let cm = self.store_chunk_batched(&raw, &mut batch)?;
            chunk_metas.push(cm);

            index += 1;
            if total_read < chunk_size {
                break;
            }
        }

        let file_meta = FileMeta {
            file_id: effective_id.clone(),
            file_name: opts.file_name.to_string(),
            file_size,
            create_time,
            partition_id,
            chunks: chunk_metas,
            content_type: opts.content_type.map(|s| s.to_string()),
        };

        // ── Phase 2: atomic commit ───────────────────────────────────────
        let outcome = match overwrite_target {
            None => {
                self.wal.lock().append(WalOperation::InsertFile {
                    file_id: effective_id.clone(),
                })?;
                self.meta.commit_file_batch(
                    &file_meta,
                    &batch.ref_count_deltas,
                    &batch.new_dedup_entries,
                )?;
                PutOutcome::Created
            }
            Some(old) => {
                // WAL: emit delete+insert so crash replay is a no-op
                // superset of what the committed redb tx already did.
                // Order matters only for human grep; redb is the source
                // of truth.
                {
                    let mut w = self.wal.lock();
                    w.append(WalOperation::DeleteFile {
                        file_id: effective_id.clone(),
                    })?;
                    w.append(WalOperation::InsertFile {
                        file_id: effective_id.clone(),
                    })?;
                }
                let old_size = old.file_size;
                // Move the deltas into the swap commit so it can fold
                // old.chunks' -1 decrements in without re-hashing.
                self.meta.commit_file_batch_swap(
                    &file_meta,
                    &old,
                    batch.ref_count_deltas.clone(),
                    &batch.new_dedup_entries,
                )?;
                tracing::info!(
                    file_id = %effective_id,
                    old_size,
                    new_size = file_size,
                    "put_stream_with: overwrite committed"
                );
                metrics::counter!("jihuan_puts_overwrite_total").increment(1);
                PutOutcome::Overwritten
            }
        };

        metrics::counter!("jihuan_puts_total").increment(1);
        metrics::counter!("jihuan_bytes_written_total").increment(file_size);
        metrics::histogram!("jihuan_put_duration_seconds").record(t0.elapsed().as_secs_f64());

        Ok((effective_id, outcome))
    }

    /// v0.4.2 P5: chunk-processing helper that accumulates pending metadata
    /// writes into `batch` instead of committing each inline. The single
    /// end-of-file `commit_file_batch` call collapses N+1 redb transactions
    /// into exactly one, dramatically reducing fsync pressure on uploads.
    fn store_chunk_batched(
        &self,
        raw: &RawChunk,
        batch: &mut FileBatch,
    ) -> Result<ChunkMeta> {
        let hash = &raw.digest.hash;
        let use_dedup = !hash.is_empty();

        // 1) Same-upload overlay dedup. A file with repeated chunks (e.g.
        //    identical 4 MB blocks of zeros) reuses the first-seen location
        //    without needing a committed dedup entry, because the batch
        //    hasn't been committed yet so `get_dedup_entry` would miss.
        if use_dedup {
            if let Some(o) = batch.overlay.get(hash).cloned() {
                *batch.ref_count_deltas.entry(o.block_id.clone()).or_insert(0) += 1;
                metrics::counter!("jihuan_dedup_hits_total").increment(1);
                return Ok(ChunkMeta {
                    block_id: o.block_id,
                    offset: o.offset,
                    original_size: o.original_size,
                    compressed_size: o.compressed_size,
                    hash: hash.clone(),
                    index: raw.index,
                });
            }
        }

        // 2) Persisted dedup index (hits across uploads).
        if use_dedup {
            if let Some(dedup) = self.meta.get_dedup_entry(hash)? {
                *batch
                    .ref_count_deltas
                    .entry(dedup.block_id.clone())
                    .or_insert(0) += 1;
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

        // 3) New chunk. Compress outside the active_writer lock (Phase 6b-P3)
        //    then append. ref-count and dedup-entry bookkeeping is *deferred*
        //    to the end-of-file batch commit.
        let compressed = crate::compression::compress(
            &raw.data,
            self.config.storage.compression_algorithm,
            self.config.storage.compression_level,
        )?;
        let (block_id, entry) = self.write_chunk_to_block(&raw.data, &compressed, hash)?;

        let chunk_meta = ChunkMeta {
            block_id: block_id.clone(),
            offset: entry.data_offset,
            original_size: entry.original_size as u64,
            compressed_size: entry.compressed_size as u64,
            hash: hash.clone(),
            index: raw.index,
        };

        *batch.ref_count_deltas.entry(block_id.clone()).or_insert(0) += 1;
        if use_dedup {
            batch.new_dedup_entries.push(DedupEntry {
                hash: hash.clone(),
                block_id: block_id.clone(),
                offset: entry.data_offset,
                original_size: entry.original_size as u64,
                compressed_size: entry.compressed_size as u64,
            });
            batch.overlay.insert(
                hash.clone(),
                OverlayEntry {
                    block_id: block_id.clone(),
                    offset: entry.data_offset,
                    original_size: entry.original_size as u64,
                    compressed_size: entry.compressed_size as u64,
                },
            );
        }

        Ok(chunk_meta)
    }

    /// Append an already-compressed chunk to the active block.
    ///
    /// `data` is the uncompressed payload — retained only so it can seed the
    /// bounded chunk cache for read-your-write latency. `compressed` is what
    /// actually gets written to disk. The caller (see `store_chunk`) does the
    /// compression outside this critical section so concurrent uploads don't
    /// serialise on a single-threaded compressor (Phase 6b-P3).
    fn write_chunk_to_block(
        &self,
        data: &[u8],
        compressed: &[u8],
        hash: &str,
    ) -> Result<(String, ChunkEntry)> {
        let mut guard = self.active_writer.lock();

        // Check if we need a new block. Rollover uses uncompressed size as
        // a conservative upper bound — same as the old path.
        let need_new_block = match guard.as_ref() {
            None => true,
            Some(ab) => ab.writer.is_full(data.len()),
        };

        if need_new_block {
            // Finish the previous block (if any) and move it into the reader cache
            if let Some(ab) = guard.take() {
                let sealed_id = ab.writer.block_id().to_string();
                let summary = ab.writer.finish()?;
                self.register_block(&summary)?;
                // Populate the reader cache with the just-sealed block
                if let Ok(reader) = BlockReader::open(&summary.path) {
                    let mut cache = self.reader_cache.lock();
                    cache.put(summary.path.to_string_lossy().into_owned(), reader);
                }
                // Unpin the sealed block — it's either referenced by at least
                // one chunk (ref_count > 0 → naturally GC-safe) or was never
                // written to and will be reaped cleanly on the next tick.
                self.pinned_blocks.lock().remove(&sealed_id);
            }
            // Create a new block. **Pin before any metadata commit** so a
            // concurrent GC tick that snapshots between `insert_block` and
            // the first ref-count increment still sees this block as active.
            let block_id = new_id();
            self.pinned_blocks.lock().insert(block_id.clone());
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
        let entry = ab
            .writer
            .append_precompressed_chunk(compressed, data.len() as u32, hash)?;
        let block_id = ab.writer.block_id().to_string();

        // Store a copy of the raw data for in-memory reads of the active block
        ab.chunk_cache.insert(
            entry.data_offset,
            CachedChunk {
                data: data.to_vec(),
            },
        );

        // v0.4.2 P5: ref-count bookkeeping is now deferred to the
        // end-of-file batch commit in `put_stream`. The pin in
        // `pinned_blocks` (inserted above when this block was created)
        // keeps GC away until the commit lands.
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

    /// v0.4.5: Seal the active block **iff** it currently has zero live
    /// references (i.e. every chunk in it has already been dereferenced by
    /// `delete_file`). This is the single trigger that unblocks reclaim of
    /// dead data sitting in the active writer — without it, GC can't touch
    /// the block (it's pinned) and `compact_block` refuses it outright, so
    /// deleted data in the active block would remain on disk until the
    /// block organically filled up via new writes.
    ///
    /// Called opportunistically from the admin GC and compact endpoints so
    /// an explicit operator action reclaims *all* reclaimable space, not
    /// just the portion sitting in already-sealed blocks.
    ///
    /// Safety: reading `ref_count` and acquiring the `active_writer` lock
    /// are separate steps, but `seal_active_block` internally re-locks and
    /// seals whichever writer is current — so a concurrent upload that
    /// rolled over during the window simply seals an empty/fresh block
    /// (harmless, reaped on next GC) instead of the one we checked. The
    /// only invariant that matters is "don't seal a block that has live
    /// references"; that's enforced by the ref_count guard below.
    pub fn seal_if_dead(&self) -> Result<Option<SealedBlockInfo>> {
        // Snapshot the current active block's id under its own lock.
        let active_id = {
            let guard = self.active_writer.lock();
            match guard.as_ref() {
                None => return Ok(None),
                Some(ab) => ab.writer.block_id().to_string(),
            }
        };

        // Cheap metadata check: only seal when ref_count is literally 0.
        // This is the safe, narrow trigger; richer util-based auto-seal is
        // intentionally deferred to keep behaviour predictable.
        match self.meta.get_block(&active_id)? {
            Some(b) if b.ref_count == 0 => {
                tracing::info!(
                    block_id = %active_id,
                    "seal_if_dead: active block has ref_count=0, sealing for reclaim"
                );
                self.seal_active_block()
            }
            _ => Ok(None),
        }
    }

    /// Seal (finish) the active block writer, making it accessible to `BlockReader`.
    /// After sealing, the block is added to the reader cache.
    ///
    /// Returns `Ok(Some(SealedBlockInfo))` when a block was actually sealed,
    /// or `Ok(None)` when the active writer was empty (nothing to seal).
    /// The info lets the admin `/api/admin/seal` endpoint surface which
    /// block was just sealed so operators can immediately target it for
    /// compaction.
    pub fn seal_active_block(&self) -> Result<Option<SealedBlockInfo>> {
        let mut guard = self.active_writer.lock();
        if let Some(ab) = guard.take() {
            let sealed_id = ab.writer.block_id().to_string();
            let summary = ab.writer.finish()?;
            let info = SealedBlockInfo {
                block_id: sealed_id.clone(),
                size: summary.total_size,
            };
            self.register_block(&summary)?;
            // Add the freshly-sealed block to the reader cache
            if let Ok(reader) = BlockReader::open(&summary.path) {
                let mut cache = self.reader_cache.lock();
                cache.put(summary.path.to_string_lossy().into_owned(), reader);
            }
            // Unpin the block now that it's sealed; ref_count is already >0
            // on any block that had at least one chunk written.
            self.pinned_blocks.lock().remove(&sealed_id);
            return Ok(Some(info));
        }
        Ok(None)
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

    /// Recovery helper: rebuild a valid footer + index table for a
    /// quarantined `.blk.orphan` so the block becomes readable again.
    ///
    /// **Must be run while the server is stopped** — we open the data
    /// file in write mode and truncate any trailing partial chunk. The
    /// orphan is produced by `cleanup_incomplete_blocks` on startup when
    /// an unsealed active block still has metadata references; by the
    /// time this function runs, all metadata for the block is intact
    /// (if it weren't, the operator would have chosen `JIHUAN_REPAIR=1`
    /// instead, which *removes* the metadata).
    ///
    /// Procedure:
    /// 1. Look up `BlockMeta` to find the expected `.blk` path; derive
    ///    `.blk.orphan` from it.
    /// 2. Stream `FileMeta` rows, collect unique `(hash, offset,
    ///    compressed_size, original_size)` pointing at this block.
    /// 3. Sort ascending by offset; truncate the orphan file to the
    ///    max `offset + compressed_size` (drops bytes from a partial
    ///    chunk that crashed before its metadata was committed).
    /// 4. For every unique chunk, read its compressed bytes back off
    ///    disk, compute its CRC32, and build a `ChunkEntry`. Accumulate
    ///    the whole-block `data_crc32` as the concatenation of those
    ///    bytes in offset order, matching `BlockWriter::finish`.
    /// 5. Serialise the index table and a fresh `BlockFooter`, fsync,
    ///    then rename `.blk.orphan` → `.blk`.
    /// 6. Update `BlockMeta.size` so the UI's utilisation math agrees
    ///    with the new file size.
    ///
    /// All steps run before any rename, so a crash partway through
    /// leaves the orphan file in place and the metadata untouched —
    /// the operator can simply re-run the command.
    pub fn reseal_orphan_block_static(
        meta: &MetadataStore,
        data_dir: &std::path::Path,
        block_id: &str,
        compression: crate::config::CompressionAlgorithm,
    ) -> Result<ResealSummary> {
        use crate::block::format::{BlockFooter, ChunkEntry};
        use std::collections::HashMap;
        use std::fs::OpenOptions;
        use std::io::{Read, Seek, SeekFrom, Write};

        // ── 1. Locate the orphan file ────────────────────────────────────
        let mut block_meta = meta.get_block(block_id)?.ok_or_else(|| {
            JiHuanError::NotFound(format!(
                "reseal: no BlockMeta row for block_id {}. \
                 If the metadata is truly gone, run JIHUAN_REPAIR=1 to purge \
                 any dangling references and then delete the .blk.orphan manually.",
                block_id
            ))
        })?;

        let sealed_path = std::path::PathBuf::from(&block_meta.path);
        let orphan_path = sealed_path.with_extension("blk.orphan");

        if sealed_path.exists() && !orphan_path.exists() {
            return Err(JiHuanError::InvalidArgument(format!(
                "reseal: {} is already sealed (no .blk.orphan next to it); nothing to do",
                block_id
            )));
        }
        if !orphan_path.exists() {
            return Err(JiHuanError::NotFound(format!(
                "reseal: orphan file {} does not exist",
                orphan_path.display()
            )));
        }
        let _ = data_dir; // reserved for future subdir validation

        // ── 2. Collect unique chunks pointing at this block ──────────────
        // Dedup by hash: multiple FileMeta rows may reference the same
        // physical (offset, compressed_size) tuple. Take the first.
        let mut unique: HashMap<String, (u64, u32, u32)> = HashMap::new();
        meta.for_each_file(|f| {
            for c in &f.chunks {
                if c.block_id == block_id {
                    unique
                        .entry(c.hash.clone())
                        .or_insert((c.offset, c.compressed_size as u32, c.original_size as u32));
                }
            }
            Ok(())
        })?;
        if unique.is_empty() {
            return Err(JiHuanError::InvalidArgument(format!(
                "reseal: no FileMeta rows reference block {}; \
                 run JIHUAN_REPAIR=1 to clean dangling metadata and delete the orphan file",
                block_id
            )));
        }

        // ── 3. Sort by offset & verify the orphan file covers them all ──
        let mut sorted: Vec<(String, u64, u32, u32)> = unique
            .into_iter()
            .map(|(h, (off, cs, os))| (h, off, cs, os))
            .collect();
        sorted.sort_by_key(|x| x.1);
        let data_end: u64 = sorted
            .iter()
            .map(|(_, off, cs, _)| *off + *cs as u64)
            .max()
            .unwrap_or(0);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&orphan_path)
            .map_err(JiHuanError::Io)?;
        let orig_size = file.metadata().map_err(JiHuanError::Io)?.len();
        if orig_size < data_end {
            return Err(JiHuanError::DataCorruption(format!(
                "reseal: orphan file is {} bytes but metadata requires at least {} — \
                 the last chunk write never hit disk. Aborting to avoid silent data loss.",
                orig_size, data_end
            )));
        }
        let trailing = orig_size.saturating_sub(data_end);

        // ── 4. Build ChunkEntry rows and accumulate block data_crc32 ────
        let algo_byte: u8 = match compression {
            crate::config::CompressionAlgorithm::None => 0,
            crate::config::CompressionAlgorithm::Lz4 => 1,
            crate::config::CompressionAlgorithm::Zstd => 2,
        };
        let mut entries: Vec<ChunkEntry> = Vec::with_capacity(sorted.len());
        let mut block_hasher = crc32fast::Hasher::new();
        for (hash, offset, cs, os) in &sorted {
            file.seek(SeekFrom::Start(*offset)).map_err(JiHuanError::Io)?;
            let mut buf = vec![0u8; *cs as usize];
            file.read_exact(&mut buf).map_err(JiHuanError::Io)?;
            let data_crc = crc32fast::hash(&buf);
            block_hasher.update(&buf);
            entries.push(ChunkEntry::new(hash, *offset, *cs, *os, data_crc, algo_byte));
        }
        let block_data_crc = block_hasher.finalize();

        // ── 5. Truncate partial tail, append index + footer ─────────────
        file.set_len(data_end).map_err(JiHuanError::Io)?;
        file.seek(SeekFrom::Start(data_end)).map_err(JiHuanError::Io)?;
        for e in &entries {
            let buf = bincode::encode_to_vec(e, bincode::config::standard()).map_err(|err| {
                JiHuanError::Serialization(format!("ChunkEntry encode: {}", err))
            })?;
            file.write_all(&buf).map_err(JiHuanError::Io)?;
        }
        let chunk_count = entries.len() as u32;
        let footer = BlockFooter::new(data_end, chunk_count, block_data_crc);
        let footer_buf = bincode::encode_to_vec(&footer, bincode::config::standard())
            .map_err(|err| JiHuanError::Serialization(format!("BlockFooter encode: {}", err)))?;
        file.write_all(&footer_buf).map_err(JiHuanError::Io)?;
        file.sync_all().map_err(JiHuanError::Io)?;
        drop(file);

        // ── 6. Rename orphan → sealed, update BlockMeta.size ────────────
        std::fs::rename(&orphan_path, &sealed_path).map_err(JiHuanError::Io)?;
        let final_size = std::fs::metadata(&sealed_path)
            .map_err(JiHuanError::Io)?
            .len();
        block_meta.size = final_size;
        meta.insert_block(&block_meta)?;

        Ok(ResealSummary {
            block_id: block_id.to_string(),
            chunks_restored: chunk_count as u64,
            final_size,
            trailing_bytes_truncated: trailing,
        })
    }

    /// Test-only: flush the active block's BufWriter to the file so a
    /// simulated kill (drop the engine without `shutdown()`) reliably
    /// leaves all committed chunk data on disk. Mirrors the kernel-level
    /// fsync that a clean shutdown would perform, minus the footer write.
    #[cfg(test)]
    pub(crate) fn flush_active_writer_for_test(&self) -> Result<()> {
        let mut guard = self.active_writer.lock();
        if let Some(ab) = guard.as_mut() {
            ab.writer.flush()?;
        }
        Ok(())
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
    // v0.5.1 Batch wrappers
    //
    // These exist primarily so that callers (e.g. `jihuan-client::EmbeddedClient`)
    // can amortise per-call overhead: one `spawn_blocking` for N files instead
    // of N. The internal hot path still uses the existing per-file code so the
    // atomicity guarantees (per-file one redb commit) are preserved exactly.
    //
    // We intentionally return a `Vec<Result<_>>` rather than short-circuiting
    // on the first error: batch users generally want to know *which* items
    // failed so they can retry selectively. Callers that prefer all-or-none
    // semantics can collapse the Vec themselves with `.into_iter().collect()`.
    // ─────────────────────────────────────────────────────────────────────────

    /// Store a list of byte payloads. See [`put_bytes`] for per-item semantics.
    pub fn put_bytes_batch(
        &self,
        items: &[(Vec<u8>, String, Option<String>)],
    ) -> Vec<Result<String>> {
        items
            .iter()
            .map(|(data, name, ct)| self.put_bytes(data, name, ct.as_deref()))
            .collect()
    }

    /// Fetch a list of files by id. Each entry is an independent [`get_bytes`]
    /// call; failures do not abort the batch.
    pub fn get_bytes_batch(&self, ids: &[String]) -> Vec<Result<Vec<u8>>> {
        ids.iter().map(|id| self.get_bytes(id)).collect()
    }

    /// Delete a list of files. Each entry is an independent [`delete_file`]
    /// call.
    pub fn delete_file_batch(&self, ids: &[String]) -> Vec<Result<()>> {
        ids.iter().map(|id| self.delete_file(id)).collect()
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

    /// v0.4.5: compute per-block *unique* live-bytes and live-hash sets by
    /// scanning every `FileMeta.chunks[]` entry once.
    ///
    /// `live_bytes` for a block is defined as the sum of `compressed_size`
    /// over the **unique** chunk hashes referenced from at least one live
    /// file. Deduplicated references (same hash referenced by multiple
    /// files, or multiple times within one file) contribute **once**, since
    /// the chunk physically occupies the block exactly once. This is the
    /// canonical utilisation metric used by both:
    ///   * the compaction scanner (deciding which blocks to rewrite);
    ///   * the `/api/block/list` + `/api/block/:id` API surface (so the UI
    ///     shows the same ratio the scanner uses).
    ///
    /// Prior to v0.4.5 the scanner naively summed `compressed_size` per
    /// `ChunkMeta` reference, which could overshoot the physical block size
    /// on dedup-heavy datasets (utilisation > 100% reported) and starve
    /// compaction of genuinely low-util blocks. Fixed by the HashSet-
    /// per-block aggregation below.
    pub fn compute_block_live_info(
        &self,
    ) -> Result<std::collections::HashMap<String, BlockLiveInfo>> {
        use std::collections::{HashMap, HashSet};
        use std::sync::Arc;

        // Streaming aggregation: we never hold more than one FileMeta in
        // heap at a time (beyond the growing per-block HashSets). For a
        // 1 M-file deployment this replaces an ~O(files × avg_chunks)
        // Vec<FileMeta> allocation (~hundreds of MB) with O(unique_hashes)
        // Arc<str> buffers (~dozens of MB).
        let mut live_hashes_per_block: HashMap<String, HashSet<Arc<str>>> = HashMap::new();
        // Hash-string intern pool: dedup-heavy tiers see the same hash
        // referenced by many files; this keeps only one `Arc<str>` alive.
        let mut intern: HashMap<String, Arc<str>> = HashMap::new();
        // `compressed_size` per (block_id, hash) pair. All references to
        // the same (block, hash) agree on the size; we take the first.
        let mut size_by_block_hash: HashMap<(String, Arc<str>), u64> = HashMap::new();

        self.meta.for_each_file(|f| {
            for c in &f.chunks {
                let h: Arc<str> = match intern.get(&c.hash) {
                    Some(a) => a.clone(),
                    None => {
                        let a: Arc<str> = Arc::from(c.hash.as_str());
                        intern.insert(c.hash.clone(), a.clone());
                        a
                    }
                };
                live_hashes_per_block
                    .entry(c.block_id.clone())
                    .or_default()
                    .insert(h.clone());
                size_by_block_hash
                    .entry((c.block_id.clone(), h))
                    .or_insert(c.compressed_size);
            }
            Ok(())
        })?;
        // intern is no longer needed now that every Arc<str> has been
        // cloned into the sets; drop it early to free memory before the
        // live_bytes reduction below.
        drop(intern);

        let mut out: HashMap<String, BlockLiveInfo> = HashMap::new();
        for (block_id, hashes) in live_hashes_per_block {
            let live_bytes: u64 = hashes
                .iter()
                .map(|h| {
                    size_by_block_hash
                        .get(&(block_id.clone(), h.clone()))
                        .copied()
                        .unwrap_or(0)
                })
                .sum();
            out.insert(
                block_id,
                BlockLiveInfo {
                    live_bytes,
                    live_hashes: hashes,
                },
            );
        }
        Ok(out)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    // ─────────────────────────────────────────────────────────────────────────
    // v0.4.3: Block compaction
    // ─────────────────────────────────────────────────────────────────────────

    /// Compact a single sealed block: read its live chunks, rewrite them into
    /// a fresh block file, and atomically flip every `FileMeta` / `DedupEntry`
    /// reference from the old block to the new one. Finally delete the old
    /// block file from disk.
    ///
    /// "Live" is defined as *referenced by at least one `FileMeta.chunks[]`
    /// entry whose `block_id == old_block_id`*. Chunks whose hash is no
    /// longer referenced by any file are dropped — this is where the disk
    /// savings come from.
    ///
    /// ## Scope: single block (targeted API)
    ///
    /// This function rewrites ONE explicitly-identified block into ONE new
    /// block. It never reads from, writes to, or merges with any other
    /// block. This is the right semantics for `/api/admin/compact?block_id=X`
    /// — the caller named a specific target.
    ///
    /// For the unscoped "reclaim everywhere" path use
    /// [`Engine::compact_low_utilization`], which v0.4.5+ bin-packs multiple
    /// low-util blocks into shared new blocks (cross-block merge) to keep
    /// the block count from drifting up over time.
    ///
    /// Returns the per-block statistics. Callers should treat this as an
    /// administrative maintenance operation: it's safe to run alongside
    /// normal traffic but holds a read tx (scans `FILES_TABLE` twice) and
    /// re-compresses the live chunks, so throughput is non-trivial.
    ///
    /// Refuses to compact:
    ///   - a non-existent block (`NotFound`);
    ///   - the currently-active (pinned) block (`InvalidArgument`);
    ///   - a block whose on-disk file is missing or incomplete.
    pub fn compact_block(&self, old_block_id: &str) -> Result<CompactionBlockStats> {
        use std::collections::HashMap;

        // Refuse to compact an active/unsealed block.
        if self.pinned_blocks.lock().contains(old_block_id) {
            return Err(JiHuanError::InvalidArgument(format!(
                "block {} is currently active — cannot compact",
                old_block_id
            )));
        }

        let old_meta = self.meta.get_block(old_block_id)?.ok_or_else(|| {
            JiHuanError::NotFound(format!("Block '{}' not found", old_block_id))
        })?;
        let old_path = PathBuf::from(&old_meta.path);
        if !old_path.exists() {
            return Err(JiHuanError::DataCorruption(format!(
                "block {} metadata exists but file {} is missing",
                old_block_id,
                old_path.display()
            )));
        }
        if !BlockReader::is_complete(&old_path) {
            return Err(JiHuanError::DataCorruption(format!(
                "block {} is not sealed — refusing to compact",
                old_block_id
            )));
        }

        // Collect the set of live hashes by scanning FILES_TABLE.
        //
        // Pre-scan's only job is to decide which chunks to *copy* into the
        // new block. The authoritative ref-count is computed inside
        // `rewrite_block_references` at commit time, so a concurrent upload
        // that slips in between pre-scan and commit is still counted
        // correctly.
        let live_hashes: std::collections::HashSet<String> = {
            let all_files = self.meta.list_all_files()?;
            let mut set = std::collections::HashSet::new();
            for f in &all_files {
                for c in &f.chunks {
                    if c.block_id == old_block_id {
                        set.insert(c.hash.clone());
                    }
                }
            }
            set
        };

        // Open the old block and build a hash → entry map.
        let mut old_reader = BlockReader::open(&old_path)?;
        let hash_to_entry: HashMap<String, ChunkEntry> = old_reader
            .entries
            .iter()
            .map(|e| (e.hash_str(), e.clone()))
            .collect();
        let old_chunks = old_reader.entries.len() as u64;

        // Short-circuit: block has no live chunks → skip straight to GC.
        // This is exactly the state a ref_count==0 block already covers and
        // the normal GC path will reclaim it on the next tick. We don't try
        // to do it here to avoid racing with GC.
        if live_hashes.is_empty() {
            return Ok(CompactionBlockStats {
                old_block_id: old_block_id.to_string(),
                new_block_id: None,
                old_size_bytes: old_meta.size,
                new_size_bytes: 0,
                bytes_saved: old_meta.size as i64,
                live_chunks: 0,
                dropped_chunks: old_chunks,
            });
        }

        // Allocate a new block and pin it for the rewrite window.
        let new_block_id = new_id();
        self.pinned_blocks.lock().insert(new_block_id.clone());
        // Guard that always unpins on early return.
        struct PinGuard<'a> {
            pins: &'a crate::gc::PinnedBlocks,
            id: String,
        }
        impl<'a> Drop for PinGuard<'a> {
            fn drop(&mut self) {
                self.pins.lock().remove(&self.id);
            }
        }
        let _pin_guard = PinGuard {
            pins: &self.pinned_blocks,
            id: new_block_id.clone(),
        };

        let new_path = self.block_path(&new_block_id);
        if let Some(parent) = new_path.parent() {
            ensure_dir(parent)?;
        }
        let mut writer = BlockWriter::create(
            &new_path,
            &new_block_id,
            self.config.storage.compression_algorithm,
            self.config.storage.compression_level,
            self.config.storage.hash_algorithm,
            // Give the compaction writer a generous cap so a full block can
            // fit even if the current block_file_size was shrunk after the
            // old block was sealed.
            self.config.storage.block_file_size.max(old_meta.size + 4096),
        )?;

        // Copy each live chunk. Re-compress on write so the new entry's
        // compressed_size / offset reflect the current algorithm+level.
        let mut chunk_remap: HashMap<String, (String, u64, u64, u64)> = HashMap::new();
        let mut live_chunks: u64 = 0;
        let verify = self.config.storage.verify_on_read;
        for hash in &live_hashes {
            let entry = hash_to_entry.get(hash).ok_or_else(|| {
                JiHuanError::DataCorruption(format!(
                    "compact: live hash {} missing from block {}'s index",
                    hash, old_block_id
                ))
            })?;
            let data = old_reader.read_chunk(entry, verify)?;
            let compressed = crate::compression::compress(
                &data,
                self.config.storage.compression_algorithm,
                self.config.storage.compression_level,
            )?;
            let new_entry =
                writer.append_precompressed_chunk(&compressed, data.len() as u32, hash)?;
            chunk_remap.insert(
                hash.clone(),
                (
                    new_block_id.clone(),
                    new_entry.data_offset,
                    new_entry.original_size as u64,
                    new_entry.compressed_size as u64,
                ),
            );
            live_chunks += 1;
        }

        // Seal the new block.
        let summary = writer.finish()?;

        let new_block_meta = BlockMeta {
            block_id: new_block_id.clone(),
            ref_count: 0, // set by rewrite_block_references
            create_time: now_secs(),
            path: summary.path.to_string_lossy().into_owned(),
            size: summary.total_size,
        };

        // Atomic metadata flip. Returns the ref_count that was actually
        // persisted; a mismatch with `live_chunks` is evidence of a
        // concurrent upload that landed extra dedup-hit references during
        // the rewrite window (still correct, just informational).
        let actual_ref_count = self
            .meta
            .rewrite_block_references(old_block_id, &new_block_meta, &chunk_remap)?;

        if actual_ref_count as u64 != live_chunks {
            tracing::info!(
                old = %old_block_id,
                new = %new_block_id,
                pre_scan_live = live_chunks,
                committed_ref_count = actual_ref_count,
                "compact_block: additional references landed during rewrite"
            );
        }

        // Evict old block from the reader cache so subsequent reads miss
        // into the (now-redirected) metadata and pick up the new block.
        {
            let mut cache = self.reader_cache.lock();
            let key = old_path.to_string_lossy().into_owned();
            cache.pop(&key);
        }
        // Drop our own old_reader handle before attempting delete (Windows).
        drop(old_reader);

        // Best-effort delete of the old block file. If this fails, metadata
        // is already consistent — on the next GC tick or restart the
        // `cleanup_incomplete_blocks` pass can sweep it.
        match std::fs::remove_file(&old_path) {
            Ok(()) => tracing::info!(
                old = %old_block_id,
                old_path = %old_path.display(),
                "compact_block: deleted old block file"
            ),
            Err(e) => tracing::warn!(
                old = %old_block_id,
                error = %e,
                "compact_block: could not delete old block file; will be reaped later"
            ),
        }

        let dropped = old_chunks.saturating_sub(live_chunks);
        let bytes_saved = (old_meta.size as i64) - (summary.total_size as i64);

        Ok(CompactionBlockStats {
            old_block_id: old_block_id.to_string(),
            new_block_id: Some(new_block_id),
            old_size_bytes: old_meta.size,
            new_size_bytes: summary.total_size,
            bytes_saved,
            live_chunks,
            dropped_chunks: dropped,
        })
    }

    /// v0.4.7: the unified dual-strategy compaction pass.
    ///
    /// **Strategy A** (low-utilisation): a block is a candidate when
    /// `live_bytes / size < opts.threshold`. Value function is
    /// bytes-saved; any group containing at least one A-candidate is
    /// committed.
    ///
    /// **Strategy B** (undersized): a block is a candidate when
    /// `size < block_file_size × opts.undersize_ratio`. Value function
    /// is file-count-saved; a group commits when
    /// `source_count − ceil(live_sum / block_file_size) ≥ opts.min_file_saved`.
    ///
    /// Both strategies share one scan → pack → commit pipeline. The
    /// two commit rules are joined with **OR**: a group commits if
    /// either rule (or `opts.force`) fires. Blocks younger than
    /// `opts.min_block_age_secs` are excluded from both candidate
    /// predicates to prevent thrash on freshly-sealed or freshly-merged
    /// blocks.
    ///
    /// The active (unsealed) block is always skipped regardless of
    /// options.
    pub fn compact_with(&self, opts: CompactionOptions) -> Result<Vec<CompactionBlockStats>> {
        use std::collections::{HashMap, HashSet};

        let threshold = opts.threshold.clamp(0.0, 1.0);
        let undersize_ratio = opts.undersize_ratio.clamp(0.0, 1.0);
        let block_file_size = self.config.storage.block_file_size;
        let undersize_bytes = ((block_file_size as f64) * undersize_ratio) as u64;
        let pinned = self.pinned_blocks.lock().clone();
        let all_blocks = self.meta.list_all_blocks()?;
        let now = now_secs();

        // v0.4.5: use the canonical unique-hash live-bytes computation so
        // the scanner and `/api/block/list` never disagree, and dedup-heavy
        // datasets don't falsely report >100% utilisation.
        let live_info = self.compute_block_live_info()?;

        // v0.4.7: disjunctive candidate predicate (low_util OR undersized)
        // with an age filter. We collect a parallel `reasons` map keyed
        // by block_id so the group commit logic can tell Strategy A
        // participants from Strategy B participants.
        let mut candidates: Vec<CompactSource> = Vec::new();
        let mut reasons: HashMap<String, CandidateReason> = HashMap::new();
        for b in all_blocks {
            if pinned.contains(&b.block_id) {
                continue;
            }
            // Anti-thrash: exclude blocks younger than the cooldown.
            // `create_time` is seconds-since-epoch; use saturating_sub
            // so a clock skew where `create_time > now` doesn't
            // accidentally short-circuit the candidate.
            let age_secs = now.saturating_sub(b.create_time);
            if opts.min_block_age_secs > 0 && age_secs < opts.min_block_age_secs {
                tracing::debug!(
                    block_id = %b.block_id,
                    age_secs,
                    min_age = opts.min_block_age_secs,
                    "compact: candidate scan skipped (too young)"
                );
                metrics::counter!("jihuan_compactions_skipped_total", "reason" => "block_too_young")
                    .increment(1);
                continue;
            }
            let (live, hashes) = match live_info.get(&b.block_id) {
                Some(info) => (info.live_bytes, info.live_hashes.clone()),
                None => (0, HashSet::new()),
            };
            let util = if b.size == 0 {
                1.0
            } else {
                live as f64 / b.size as f64
            };
            let is_low_util = util < threshold;
            let is_undersized = undersize_ratio > 0.0 && b.size < undersize_bytes;
            let reason = match (is_low_util, is_undersized) {
                (true, true) => Some(CandidateReason::Both),
                (true, false) => Some(CandidateReason::LowUtil),
                (false, true) => Some(CandidateReason::Undersized),
                (false, false) => None,
            };
            // Per-candidate scan trace. `reason` is present when the
            // block is kept; absent when rejected by both predicates.
            tracing::debug!(
                block_id = %b.block_id,
                size = b.size,
                live_bytes = live,
                utilization = %format_args!("{:.3}", util),
                threshold = %format_args!("{:.3}", threshold),
                undersize_bytes,
                reason = reason.map(|r| r.as_label()).unwrap_or("none"),
                "compact: candidate scan"
            );
            if let Some(r) = reason {
                reasons.insert(b.block_id.clone(), r);
                candidates.push((b, live, hashes));
            }
        }

        if candidates.is_empty() {
            tracing::debug!(
                threshold = %format_args!("{:.3}", threshold),
                undersize_ratio = %format_args!("{:.3}", undersize_ratio),
                "compact: no candidates; nothing to do"
            );
            return Ok(vec![]);
        }

        // Greedy bin-pack candidates into groups whose combined live
        // bytes fit within one block_file_size. Block-granular: a
        // candidate either goes fully into the current group or starts
        // the next one. This preserves the single-tx atomic swap
        // invariant in `compact_merge_group`.
        candidates.sort_by_key(|(_, live, _)| *live);

        let mut groups: Vec<Vec<CompactSource>> = Vec::new();
        let mut current: Vec<CompactSource> = Vec::new();
        let mut current_size: u64 = 0;
        for cand in candidates {
            let cand_live = cand.1;
            if !current.is_empty() && current_size + cand_live > block_file_size {
                groups.push(std::mem::take(&mut current));
                current_size = 0;
            }
            current_size += cand_live;
            current.push(cand);
        }
        if !current.is_empty() {
            groups.push(current);
        }

        tracing::info!(
            candidate_count = groups.iter().map(|g| g.len()).sum::<usize>(),
            group_count = groups.len(),
            "compact: packed candidates into groups"
        );

        // v0.4.7 dual-gate commit rule. For each group compute:
        //   has_a_candidate   — did Strategy A flag any member?
        //   file_count_saved  — Strategy B value function
        // Group commits when (A) OR (B) OR (force). Skipped groups
        // emit a metric labelled with the binding reason.
        let mut committed_groups: Vec<Vec<CompactSource>> = Vec::new();
        let mut committed_reasons: Vec<(&'static str, &'static str)> = Vec::new();
        for group in groups {
            let has_a = group
                .iter()
                .any(|(b, _, _)| reasons.get(&b.block_id).is_some_and(|r| r.has_low_util()));
            let live_sum: u64 = group.iter().map(|(_, live, _)| *live).sum();
            // ceil(live_sum / block_file_size). Safe because
            // block_file_size > 0 by validator guarantee.
            let output_block_estimate = live_sum.div_ceil(block_file_size.max(1));
            let file_count_saved = (group.len() as u64).saturating_sub(output_block_estimate);

            let passes_b = opts.min_file_saved > 0 && file_count_saved >= opts.min_file_saved;
            let commit = opts.force || has_a || passes_b;

            let strategy_label = if has_a && group.iter().any(|(b, _, _)| {
                matches!(
                    reasons.get(&b.block_id),
                    Some(CandidateReason::Undersized) | Some(CandidateReason::Both)
                )
            }) {
                "mixed"
            } else if has_a {
                "low_util"
            } else {
                "undersized"
            };
            let commit_reason = if opts.force {
                "forced"
            } else if has_a {
                "a_candidate"
            } else if passes_b {
                "file_benefit"
            } else {
                "none"
            };

            tracing::debug!(
                group_size = group.len(),
                live_sum,
                file_count_saved,
                min_file_saved = opts.min_file_saved,
                has_a_candidate = has_a,
                strategy = strategy_label,
                commit,
                commit_reason,
                "compact: group commit decision"
            );

            if commit {
                committed_groups.push(group);
                committed_reasons.push((strategy_label, commit_reason));
            } else {
                metrics::counter!("jihuan_compactions_skipped_total", "reason" => "below_file_benefit")
                    .increment(1);
            }
        }

        if committed_groups.is_empty() {
            tracing::debug!("compact: no group passed the commit gates");
            return Ok(vec![]);
        }

        // Verify the filesystem has enough free space to hold each
        // group's new block in addition to the still-present source
        // blocks (they're only unlinked after the redb commit). We
        // check once per group, greedily accounting for groups we've
        // already accepted in this pass. Always respected — even
        // `force=true` goes through this check.
        let headroom = opts.disk_headroom_bytes;
        let (committed_groups, committed_reasons) = if headroom > 0 {
            let data_dir = self.config.storage.data_dir.clone();
            match fs2::available_space(&data_dir) {
                Ok(mut avail) => {
                    let mut kept_groups: Vec<Vec<CompactSource>> =
                        Vec::with_capacity(committed_groups.len());
                    let mut kept_reasons: Vec<(&'static str, &'static str)> =
                        Vec::with_capacity(committed_reasons.len());
                    for (g, r) in committed_groups.into_iter().zip(committed_reasons.into_iter())
                    {
                        let live_sum: u64 = g.iter().map(|(_, live, _)| *live).sum();
                        let required = live_sum.saturating_add(headroom);
                        if avail < required {
                            tracing::warn!(
                                group_size = g.len(),
                                live_sum,
                                required,
                                available = avail,
                                "compact: group skipped (insufficient disk headroom)"
                            );
                            metrics::counter!(
                                "jihuan_compactions_skipped_total",
                                "reason" => "disk_headroom"
                            )
                            .increment(1);
                            continue;
                        }
                        avail = avail.saturating_sub(live_sum);
                        kept_groups.push(g);
                        kept_reasons.push(r);
                    }
                    (kept_groups, kept_reasons)
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        data_dir = %data_dir.display(),
                        "compact: could not query filesystem free space; skipping disk-headroom check"
                    );
                    (committed_groups, committed_reasons)
                }
            }
        } else {
            (committed_groups, committed_reasons)
        };

        let mut results = Vec::new();
        for (group, (strategy, commit_reason)) in
            committed_groups.into_iter().zip(committed_reasons.into_iter())
        {
            tracing::info!(
                group_size = group.len(),
                strategy,
                commit_reason,
                "compact: committing group"
            );
            metrics::counter!("jihuan_compactions_total", "strategy" => strategy).increment(1);
            match self.compact_merge_group(&group) {
                Ok(mut stats) => {
                    let bytes_saved: i64 = stats.iter().map(|s| s.bytes_saved).sum();
                    if bytes_saved > 0 {
                        metrics::counter!("jihuan_compactions_bytes_saved_total")
                            .increment(bytes_saved as u64);
                    }
                    let live_sum: u64 = group.iter().map(|(_, live, _)| *live).sum();
                    let files_reduced = (group.len() as u64)
                        .saturating_sub(live_sum.div_ceil(block_file_size.max(1)));
                    metrics::counter!("jihuan_compactions_files_reduced_total")
                        .increment(files_reduced);
                    results.append(&mut stats);
                }
                Err(e) => tracing::warn!(
                    group_size = group.len(),
                    error = %e,
                    "compact: group skipped due to error"
                ),
            }
        }
        Ok(results)
    }

    /// v0.4.5: merge `group` (one or more sealed, non-pinned, low-util
    /// blocks) into a **single** new block. Writes live chunks from every
    /// source sequentially into one new block file, then atomically swaps
    /// all references (FileMeta chunks, DedupEntry rows, BlockMeta rows) in
    /// a single redb tx via `rewrite_block_references_group`.
    ///
    /// Emits one `CompactionBlockStats` **per source block** so callers
    /// still see per-block attribution; all members of the same group share
    /// the same `new_block_id`. `new_size_bytes` on each stat is that
    /// source's live contribution (sum of compressed chunk sizes it
    /// provided), and `bytes_saved = old_size - contribution`; summed
    /// across the group this approximately equals the group's net savings
    /// (minus the single shared block header/trailer of ~500-1000 B).
    fn compact_merge_group(
        &self,
        group: &[CompactSource],
    ) -> Result<Vec<CompactionBlockStats>> {
        use std::collections::HashMap;

        if group.is_empty() {
            return Ok(vec![]);
        }

        // Partition: sources with zero live chunks are GC candidates — emit
        // their stats directly without copying. Everyone else goes into the
        // merge writer.
        let mut stats: Vec<CompactionBlockStats> = Vec::new();
        let mut effective: Vec<&CompactSource> = Vec::new();
        for entry in group {
            if entry.2.is_empty() {
                stats.push(CompactionBlockStats {
                    old_block_id: entry.0.block_id.clone(),
                    new_block_id: None,
                    old_size_bytes: entry.0.size,
                    new_size_bytes: 0,
                    bytes_saved: entry.0.size as i64,
                    live_chunks: 0,
                    dropped_chunks: 0, // unknown without opening the reader
                });
            } else {
                effective.push(entry);
            }
        }
        if effective.is_empty() {
            return Ok(stats);
        }

        // Allocate a single new block for the whole group.
        let new_block_id = new_id();
        self.pinned_blocks.lock().insert(new_block_id.clone());
        struct PinGuard<'a> {
            pins: &'a crate::gc::PinnedBlocks,
            id: String,
        }
        impl<'a> Drop for PinGuard<'a> {
            fn drop(&mut self) {
                self.pins.lock().remove(&self.id);
            }
        }
        let _pin_guard = PinGuard {
            pins: &self.pinned_blocks,
            id: new_block_id.clone(),
        };

        let new_path = self.block_path(&new_block_id);
        if let Some(parent) = new_path.parent() {
            ensure_dir(parent)?;
        }

        // Cap the writer at the larger of configured block_file_size or the
        // sum of source live bytes + slack, so we never reject mid-merge on
        // a packed group that fits by construction.
        let group_live_sum: u64 = effective.iter().map(|e| e.1).sum();
        let mut writer = BlockWriter::create(
            &new_path,
            &new_block_id,
            self.config.storage.compression_algorithm,
            self.config.storage.compression_level,
            self.config.storage.hash_algorithm,
            self.config
                .storage
                .block_file_size
                .max(group_live_sum + 65_536),
        )?;

        // Per-source bookkeeping for attribution in the returned stats.
        struct SourceOutcome {
            block_id: String,
            old_size: u64,
            old_chunks: u64,
            live_chunks: u64,
            contributed_bytes: u64,
        }
        let mut outcomes: Vec<SourceOutcome> = Vec::new();
        let mut chunk_remap: HashMap<String, (String, u64, u64, u64)> = HashMap::new();
        let verify = self.config.storage.verify_on_read;

        // Stream each source's live chunks into the shared writer.
        for entry in &effective {
            let (old_meta, _live_bytes_hint, live_hashes) = *entry;
            let old_path = PathBuf::from(&old_meta.path);
            if !old_path.exists() {
                return Err(JiHuanError::DataCorruption(format!(
                    "block {} metadata exists but file {} is missing",
                    old_meta.block_id,
                    old_path.display()
                )));
            }
            if !BlockReader::is_complete(&old_path) {
                return Err(JiHuanError::DataCorruption(format!(
                    "block {} is not sealed — refusing to compact",
                    old_meta.block_id
                )));
            }

            let mut old_reader = BlockReader::open(&old_path)?;
            let hash_to_entry: HashMap<String, crate::block::format::ChunkEntry> = old_reader
                .entries
                .iter()
                .map(|e| (e.hash_str(), e.clone()))
                .collect();
            let old_chunks = old_reader.entries.len() as u64;

            let mut live_count: u64 = 0;
            let mut contributed: u64 = 0;
            let target_algo = self.config.storage.compression_algorithm;
            let target_level = self.config.storage.compression_level;
            for hash in live_hashes {
                // hash is &Arc<str>; HashMap<String, _>::get wants &str via
                // String: Borrow<str>. as_ref() yields the &str we need.
                let hash_str: &str = hash.as_ref();
                let src_entry = hash_to_entry.get(hash_str).ok_or_else(|| {
                    JiHuanError::DataCorruption(format!(
                        "compact_merge_group: live hash {} missing from block {}'s index",
                        hash_str, old_meta.block_id
                    ))
                })?;
                // v0.4.7: read the source's compressed bytes + recorded
                // algorithm. When they match the engine's configured
                // target, write verbatim (no decompress → no recompress).
                // Cross-algorithm sources pay the full round-trip.
                let (src_compressed, src_algo) = old_reader.read_chunk_raw(src_entry, verify)?;
                let original_size = src_entry.original_size;
                let new_entry = if src_algo == target_algo {
                    metrics::counter!("jihuan_compactions_passthrough_chunks_total").increment(1);
                    writer.append_precompressed_chunk(
                        &src_compressed,
                        original_size,
                        hash_str,
                    )?
                } else {
                    metrics::counter!("jihuan_compactions_recompressed_chunks_total")
                        .increment(1);
                    let data = crate::compression::decompress(
                        &src_compressed,
                        src_algo,
                        original_size as usize,
                    )?;
                    let compressed =
                        crate::compression::compress(&data, target_algo, target_level)?;
                    writer.append_precompressed_chunk(
                        &compressed,
                        data.len() as u32,
                        hash_str,
                    )?
                };
                chunk_remap.insert(
                    // rewrite_block_references_group's tx layer expects
                    // owned String keys, so pay one allocation here. The
                    // Arc<str> savings are on the live_hashes fan-out,
                    // not on this inner per-live-chunk map.
                    hash_str.to_string(),
                    (
                        new_block_id.clone(),
                        new_entry.data_offset,
                        new_entry.original_size as u64,
                        new_entry.compressed_size as u64,
                    ),
                );
                live_count += 1;
                contributed += new_entry.compressed_size as u64;
            }

            outcomes.push(SourceOutcome {
                block_id: old_meta.block_id.clone(),
                old_size: old_meta.size,
                old_chunks,
                live_chunks: live_count,
                contributed_bytes: contributed,
            });
        }

        // Seal the merged new block.
        let summary = writer.finish()?;
        let new_block_meta = BlockMeta {
            block_id: new_block_id.clone(),
            ref_count: 0, // filled by rewrite_block_references_group
            create_time: now_secs(),
            path: summary.path.to_string_lossy().into_owned(),
            size: summary.total_size,
        };

        // Single-tx atomic swap for the whole group.
        let old_ids: Vec<String> = outcomes.iter().map(|o| o.block_id.clone()).collect();
        let actual_ref_count =
            self.meta
                .rewrite_block_references_group(&old_ids, &new_block_meta, &chunk_remap)?;
        let committed_live: u64 = outcomes.iter().map(|o| o.live_chunks).sum();
        if actual_ref_count != committed_live {
            tracing::info!(
                group_size = effective.len(),
                new = %new_block_id,
                pre_scan_live = committed_live,
                committed_ref_count = actual_ref_count,
                "compact_merge_group: additional references landed during rewrite"
            );
        }

        // Evict old readers and delete old files (best-effort; orphans get
        // reaped later).
        {
            let mut cache = self.reader_cache.lock();
            for outcome in &outcomes {
                let old_path = self.block_path(&outcome.block_id);
                let key = old_path.to_string_lossy().into_owned();
                cache.pop(&key);
            }
        }
        for outcome in &outcomes {
            let old_path = self.block_path(&outcome.block_id);
            match std::fs::remove_file(&old_path) {
                Ok(()) => tracing::info!(
                    old = %outcome.block_id,
                    new = %new_block_id,
                    "compact_merge_group: deleted old block file"
                ),
                Err(e) => tracing::warn!(
                    old = %outcome.block_id,
                    error = %e,
                    "compact_merge_group: could not delete old block file; will be reaped later"
                ),
            }
        }

        // Emit per-source stats.
        for outcome in outcomes {
            let dropped = outcome.old_chunks.saturating_sub(outcome.live_chunks);
            let bytes_saved = (outcome.old_size as i64) - (outcome.contributed_bytes as i64);
            stats.push(CompactionBlockStats {
                old_block_id: outcome.block_id,
                new_block_id: Some(new_block_id.clone()),
                old_size_bytes: outcome.old_size,
                new_size_bytes: outcome.contributed_bytes,
                bytes_saved,
                live_chunks: outcome.live_chunks,
                dropped_chunks: dropped,
            });
        }

        Ok(stats)
    }

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

    /// v0.4.7 test helper: `CompactionOptions` that exercises only
    /// Strategy A (low utilisation). All Strategy B gates are
    /// disabled so legacy regression tests continue to assert the
    /// expected low-util-only behaviour.
    fn low_util_only(threshold: f64) -> CompactionOptions {
        CompactionOptions {
            threshold,
            undersize_ratio: 0.0,
            min_file_saved: 0,
            min_block_age_secs: 0,
            disk_headroom_bytes: 0,
            force: false,
        }
    }

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

    // ── v0.4.4: Auto-compaction regressions ──────────────────────────────

    #[test]
    fn test_start_auto_compaction_returns_none_when_disabled() {
        // Default config → auto_compact_enabled = false. Must not spawn a
        // task; returning None keeps startup free of unnecessary background
        // work for the common case.
        let tmp = tempdir().unwrap();
        let engine = Arc::new(open_engine(&tmp));
        assert!(engine.start_auto_compaction().is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_start_auto_compaction_runs_when_enabled() {
        // Enable auto-compaction at a 1 s cadence (gc_interval_secs=1 ×
        // every_gc_ticks=1). Upload two files sharing one block, delete
        // one to create a compaction candidate, then wait for one tick
        // past the first (which is consumed on startup). Verify the
        // low-util block got rewritten.
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 16 * 1024 * 1024;
        cfg.storage.gc_interval_secs = 1;
        cfg.storage.auto_compact_enabled = true;
        cfg.storage.auto_compact_threshold = 0.75;
        cfg.storage.auto_compact_every_gc_ticks = 1;
        cfg.storage.auto_compact_undersize_ratio = 0.0;
        cfg.storage.auto_compact_min_file_saved = 0;
        cfg.storage.auto_compact_min_block_age_secs = 0;
        cfg.storage.auto_compact_disk_headroom_bytes = 0;
        let engine = Arc::new(Engine::open(cfg).unwrap());

        let p_a: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect();
        let p_b: Vec<u8> = (0..150_000u32).map(|i| ((i * 11) % 251) as u8).collect();
        let id_a = engine.put_bytes(&p_a, "a.bin", None).unwrap();
        let id_b = engine.put_bytes(&p_b, "b.bin", None).unwrap();
        engine.seal_active_block().unwrap();
        let old_block = engine.meta.get_file(&id_a).unwrap().unwrap().chunks[0]
            .block_id
            .clone();
        engine.delete_file(&id_b).unwrap();

        let _handle = engine.start_auto_compaction().expect("handle");
        // Wait past two ticks: first (init) + one real tick.
        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

        // Old block metadata should be gone; new (compacted) block exists.
        assert!(
            engine.meta.get_block(&old_block).unwrap().is_none(),
            "auto-compaction should have rewritten the low-util block"
        );
        // And file A still round-trips.
        assert_eq!(engine.get_bytes(&id_a).unwrap(), p_a);
    }

    // ── v0.4.3: Block compaction regressions ─────────────────────────────

    #[test]
    fn test_compact_block_reclaims_space_from_deleted_file() {
        // Upload two distinct small files → they share the same active block.
        // Seal, delete one, compact → new block is smaller AND the surviving
        // file still reads back byte-identical.
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 16 * 1024 * 1024;
        let engine = Engine::open(cfg).unwrap();

        // Distinct random-ish payloads so compression can't collapse them.
        let payload_a: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let payload_b: Vec<u8> = (0..200_000u32).map(|i| ((i * 7) % 251) as u8).collect();
        let id_a = engine.put_bytes(&payload_a, "a.bin", None).unwrap();
        let id_b = engine.put_bytes(&payload_b, "b.bin", None).unwrap();

        // Seal the active block so it becomes a compactable sealed block.
        engine.seal_active_block().unwrap();

        // Pick the block id that holds A's first chunk.
        let file_a = engine.meta.get_file(&id_a).unwrap().unwrap();
        let block_id = file_a.chunks[0].block_id.clone();
        let old_size = engine.meta.get_block(&block_id).unwrap().unwrap().size;

        // Delete file B.
        engine.delete_file(&id_b).unwrap();

        // Compact the (now half-populated) block.
        let stats = engine.compact_block(&block_id).unwrap();
        assert!(
            stats.new_block_id.is_some(),
            "compaction should produce a new block when live chunks remain"
        );
        assert!(
            stats.bytes_saved > 0,
            "expected compaction to save bytes (old={} new={})",
            stats.old_size_bytes,
            stats.new_size_bytes
        );
        assert!(
            stats.dropped_chunks >= 1,
            "at least one chunk should have been dropped"
        );
        assert!(
            stats.new_size_bytes < old_size,
            "new block must be smaller than the old one"
        );

        // Old block must be gone from metadata.
        assert!(engine.meta.get_block(&block_id).unwrap().is_none());
        // File A must still read back byte-identical.
        let got = engine.get_bytes(&id_a).unwrap();
        assert_eq!(got, payload_a);
        // File B must still be 404.
        assert!(matches!(
            engine.get_bytes(&id_b).unwrap_err(),
            JiHuanError::NotFound(_)
        ));
    }

    #[test]
    fn test_compact_block_preserves_cross_block_dedup() {
        // After compaction, a second upload of identical content should
        // dedup-hit the new block (not the old one that's been reaped).
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 16 * 1024 * 1024;
        let engine = Engine::open(cfg).unwrap();

        let payload_a: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect();
        let payload_b: Vec<u8> = (0..150_000u32).map(|i| ((i * 3) % 251) as u8).collect();
        let id_a = engine.put_bytes(&payload_a, "a.bin", None).unwrap();
        let id_b = engine.put_bytes(&payload_b, "b.bin", None).unwrap();
        engine.seal_active_block().unwrap();

        let file_a = engine.meta.get_file(&id_a).unwrap().unwrap();
        let old_block = file_a.chunks[0].block_id.clone();
        engine.delete_file(&id_b).unwrap();
        let stats = engine.compact_block(&old_block).unwrap();
        let new_block = stats.new_block_id.clone().unwrap();

        // Upload A's payload again under a different file name — should
        // dedup-hit into the NEW block, not try to open the deleted old file.
        let id_a2 = engine.put_bytes(&payload_a, "a2.bin", None).unwrap();
        let file_a2 = engine.meta.get_file(&id_a2).unwrap().unwrap();
        for c in &file_a2.chunks {
            assert_eq!(
                c.block_id, new_block,
                "dedup must follow the compaction remap, not point at the purged old block"
            );
        }

        // And both copies must still round-trip.
        assert_eq!(engine.get_bytes(&id_a).unwrap(), payload_a);
        assert_eq!(engine.get_bytes(&id_a2).unwrap(), payload_a);
    }

    #[test]
    fn test_compact_block_rejects_active_block() {
        // Must refuse to compact the block we're currently writing to.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        let id = engine.put_bytes(b"something", "x.txt", None).unwrap();
        let file = engine.meta.get_file(&id).unwrap().unwrap();
        let active_block = file.chunks[0].block_id.clone();

        let err = engine.compact_block(&active_block).unwrap_err();
        assert!(matches!(err, JiHuanError::InvalidArgument(_)), "got {:?}", err);
    }

    #[test]
    fn test_compact_low_utilization_scanner() {
        // Three sealed blocks, only one below threshold → only it is compacted.
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 16 * 1024 * 1024;
        // v0.4.7: tests use sub-MB payloads; disable headroom so we
        // actually exercise the scanner path.
        cfg.storage.auto_compact_disk_headroom_bytes = 0;
        let engine = Engine::open(cfg).unwrap();

        // Block 1: 2 files alive → high utilization.
        // Use varied bytes so chunks within one file DON'T self-dedup — this
        // models realistic workloads. (Uniform `vec![n; N]` payloads would
        // collapse intra-file chunks to one physical write and make the
        // utilisation math pathological due to block overhead.)
        let p1: Vec<u8> = (0..150_000u32).map(|j| (j % 251) as u8).collect();
        let p2: Vec<u8> = (0..150_000u32).map(|j| ((j * 7) % 251) as u8).collect();
        let id1 = engine.put_bytes(&p1, "k1.bin", None).unwrap();
        let _id2 = engine.put_bytes(&p2, "k2.bin", None).unwrap();
        engine.seal_active_block().unwrap();
        let block_hi = engine.meta.get_file(&id1).unwrap().unwrap().chunks[0]
            .block_id
            .clone();

        // Block 2: 2 files, delete 1 → low utilization.
        let p3: Vec<u8> = (0..150_000u32).map(|j| ((j * 11) % 251) as u8).collect();
        let p4: Vec<u8> = (0..150_000u32).map(|j| ((j * 13) % 251) as u8).collect();
        let _id3 = engine.put_bytes(&p3, "k3.bin", None).unwrap();
        let id4 = engine.put_bytes(&p4, "k4.bin", None).unwrap();
        engine.seal_active_block().unwrap();
        let block_lo = engine.meta.get_file(&id4).unwrap().unwrap().chunks[0]
            .block_id
            .clone();
        engine.delete_file(&id4).unwrap();

        // Threshold: compact anything under 75% utilization.
        let results = engine.compact_with(low_util_only(0.75)).unwrap();

        // Exactly the low-util block was compacted.
        assert_eq!(results.len(), 1, "only block_lo should be compacted");
        assert_eq!(results[0].old_block_id, block_lo);
        // And block_hi was untouched.
        assert!(engine.meta.get_block(&block_hi).unwrap().is_some());
    }

    #[test]
    fn test_compact_low_utilization_merges_multiple_blocks_into_one() {
        // v0.4.5 cross-block merge regression: three sealed blocks each
        // below utilization threshold should bin-pack into ONE new block,
        // not three. All surviving files remain byte-identical afterwards.
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        // Pick a block_file_size large enough to fit all three sources'
        // live chunks combined. 4 MiB easily holds ~6 * 100 KB payloads.
        cfg.storage.block_file_size = 4 * 1024 * 1024;
        // Disable compression so the 50% util math below is deterministic
        // (otherwise zstd collapses the test's patterned payloads and the
        // candidates end up above threshold).
        cfg.storage.compression_algorithm = crate::config::CompressionAlgorithm::None;
        cfg.storage.compression_level = 0;
        cfg.storage.auto_compact_disk_headroom_bytes = 0;
        let engine = Engine::open(cfg).unwrap();

        // Build 3 blocks, each with 2 files, delete 1 per block → each
        // lands at ~50% utilization.
        let mut keepers: Vec<(String, Vec<u8>)> = Vec::new();
        let mut old_block_ids: Vec<String> = Vec::new();
        for i in 0..3u8 {
            let keep_payload: Vec<u8> = (0..100_000u32).map(|j| ((j + i as u32) % 251) as u8).collect();
            let drop_payload: Vec<u8> = vec![i.wrapping_add(128); 100_000];
            let keep_name = format!("keep_{i}.bin");
            let drop_name = format!("drop_{i}.bin");
            let keep_id = engine.put_bytes(&keep_payload, &keep_name, None).unwrap();
            let drop_id = engine.put_bytes(&drop_payload, &drop_name, None).unwrap();
            engine.seal_active_block().unwrap();
            let block_id = engine
                .meta
                .get_file(&keep_id)
                .unwrap()
                .unwrap()
                .chunks[0]
                .block_id
                .clone();
            engine.delete_file(&drop_id).unwrap();
            keepers.push((keep_id, keep_payload));
            old_block_ids.push(block_id);
        }

        let before = engine.meta.list_all_blocks().unwrap();
        let sealed_before: Vec<_> = before.iter().filter(|b| b.size > 0).collect();
        assert_eq!(
            sealed_before.len(),
            3,
            "setup precondition: 3 sealed candidate blocks"
        );

        // Compact with threshold=0.75 so each ~50% block qualifies.
        let stats = engine.compact_with(low_util_only(0.75)).unwrap();

        // All three sources report the SAME new_block_id → proves merge.
        let new_ids: std::collections::HashSet<_> = stats
            .iter()
            .filter_map(|s| s.new_block_id.as_ref())
            .collect();
        assert_eq!(
            new_ids.len(),
            1,
            "expected a single merged new block id, got {:?}",
            new_ids
        );

        // And the block list now has exactly one sealed block (the merged one).
        let after = engine.meta.list_all_blocks().unwrap();
        let sealed_after: Vec<_> = after.iter().filter(|b| b.size > 0).collect();
        assert_eq!(sealed_after.len(), 1, "all three merged into one sealed block");

        // None of the original block ids should still exist.
        for old in &old_block_ids {
            assert!(
                engine.meta.get_block(old).unwrap().is_none(),
                "old block {} should have been replaced by merge",
                old
            );
        }

        // Every surviving file remains byte-identical.
        for (id, expected) in &keepers {
            let got = engine.get_bytes(id).unwrap();
            assert_eq!(got, *expected, "file {} corrupted after merge compact", id);
        }
    }

    #[test]
    fn test_reseal_orphan_roundtrip_restores_file_reads() {
        // Regression for v0.4.5 recovery path:
        //   1. Upload files into the active block (not sealed).
        //   2. Simulate a hard-kill crash: drop the engine without calling
        //      shutdown(), then manually rename the active .blk → .blk.orphan
        //      (this is exactly what cleanup_incomplete_blocks does at next
        //       startup when it sees a referenced but footerless block).
        //   3. Call reseal_orphan_block_static.
        //   4. Re-open engine, verify both files read back byte-identical.
        use std::sync::Arc;

        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 16 * 1024 * 1024;
        let engine = Engine::open(cfg.clone()).unwrap();

        let p_a: Vec<u8> = (0..250_000u32).map(|i| (i % 251) as u8).collect();
        let p_b: Vec<u8> = (0..180_000u32).map(|i| ((i * 7) % 251) as u8).collect();
        let id_a = engine.put_bytes(&p_a, "a.bin", None).unwrap();
        let id_b = engine.put_bytes(&p_b, "b.bin", None).unwrap();

        // NOTE: deliberately do NOT call seal_active_block — this mirrors the
        // kill -9 failure mode. The block file currently has a valid header
        // and chunk data but no footer / index table.
        let block_id = engine.meta.get_file(&id_a).unwrap().unwrap().chunks[0]
            .block_id
            .clone();
        let block_meta = engine.meta.get_block(&block_id).unwrap().unwrap();
        let sealed_path = std::path::PathBuf::from(&block_meta.path);
        let orphan_path = sealed_path.with_extension("blk.orphan");

        // Flush the BufWriter so every committed chunk hits disk before we
        // simulate the kill. Without this hook the tail < 8 KB of
        // compressed data stays in the BufWriter and is lost on drop, and
        // the resealer (correctly) refuses to seal an orphan whose metadata
        // references bytes that aren't there.
        engine.flush_active_writer_for_test().unwrap();

        // Drop engine WITHOUT shutdown() to mimic the crash, then rename.
        drop(engine);
        std::fs::rename(&sealed_path, &orphan_path).unwrap();
        assert!(!sealed_path.exists());
        assert!(orphan_path.exists());

        // Open the metadata store directly (no engine) and reseal.
        let meta = Arc::new(
            crate::metadata::store::MetadataStore::open(
                cfg.storage.meta_dir.join("meta.db"),
            )
            .unwrap(),
        );
        let summary = Engine::reseal_orphan_block_static(
            &meta,
            &cfg.storage.data_dir,
            &block_id,
            cfg.storage.compression_algorithm,
        )
        .unwrap();
        assert!(summary.chunks_restored > 0);
        assert!(!orphan_path.exists(), "orphan should be renamed away");
        assert!(sealed_path.exists(), "sealed .blk must exist after reseal");
        drop(meta); // release redb lock so Engine::open can reopen it

        // Reopen the engine; BlockReader::open must succeed now, and both
        // files must round-trip byte-identically.
        let engine = Engine::open(cfg).unwrap();
        assert_eq!(engine.get_bytes(&id_a).unwrap(), p_a);
        assert_eq!(engine.get_bytes(&id_b).unwrap(), p_b);
    }

    #[test]
    fn test_compute_block_live_info_dedups_across_files() {
        // v0.4.5 regression: dedup-heavy datasets used to report
        // `live_bytes > size` because the old scanner summed
        // compressed_size per ChunkMeta reference instead of per unique
        // hash. The fix uses the canonical unique-hash aggregation.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        // Two uploads with identical contents → should fully dedup.
        let payload = vec![9u8; 200_000];
        let id1 = engine.put_bytes(&payload, "a.bin", None).unwrap();
        let _id2 = engine.put_bytes(&payload, "b.bin", None).unwrap();
        engine.seal_active_block().unwrap();

        let block_id = engine.meta.get_file(&id1).unwrap().unwrap().chunks[0]
            .block_id
            .clone();
        let block_meta = engine.meta.get_block(&block_id).unwrap().unwrap();

        let info = engine.compute_block_live_info().unwrap();
        let per_block = info.get(&block_id).expect("block has live info");

        // Sanity: dedup kept it to one physical copy → live_bytes <= size.
        assert!(
            per_block.live_bytes <= block_meta.size,
            "live_bytes {} must not exceed block size {} after dedup",
            per_block.live_bytes,
            block_meta.size
        );
    }

    // ── v0.4.7: dual-strategy compaction regressions ─────────────────────

    /// Strategy B candidate gate: sealed blocks whose size is below
    /// `block_file_size * undersize_ratio` must be picked up by the
    /// scanner even when their utilisation is ≥ threshold. This is
    /// the anti-small-block-drift axis that Strategy A alone cannot
    /// address.
    #[test]
    fn test_compact_undersized_strategy_b_picks_up_high_util_small_blocks() {
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        // Large block_file_size so our ~150 KB sealed blocks are *way*
        // below the undersize threshold regardless of content.
        cfg.storage.block_file_size = 4 * 1024 * 1024;
        cfg.storage.compression_algorithm = crate::config::CompressionAlgorithm::None;
        cfg.storage.compression_level = 0;
        let engine = Engine::open(cfg).unwrap();

        // Build four *fully utilised* sealed blocks (no deletions) —
        // Strategy A would skip all of these.
        let mut block_ids = Vec::new();
        for i in 0..4u8 {
            let payload: Vec<u8> =
                (0..150_000u32).map(|j| ((j + i as u32 * 31) % 251) as u8).collect();
            let id = engine.put_bytes(&payload, &format!("k{}.bin", i), None).unwrap();
            engine.seal_active_block().unwrap();
            let bid = engine.meta.get_file(&id).unwrap().unwrap().chunks[0]
                .block_id
                .clone();
            block_ids.push(bid);
        }

        // Strategy A only (undersize disabled) → no candidates.
        let none = engine.compact_with(low_util_only(0.9)).unwrap();
        assert!(
            none.is_empty(),
            "pure-A with full blocks should find nothing, got {:?}",
            none
        );

        // Strategy B: ratio 0.5 → any block under 2 MiB qualifies.
        // min_file_saved=2 → a 4→1 merge saves 3, passes.
        let opts = CompactionOptions {
            threshold: 0.0,          // disable A
            undersize_ratio: 0.5,
            min_file_saved: 2,
            min_block_age_secs: 0,
            disk_headroom_bytes: 0,
            force: false,
        };
        let stats = engine.compact_with(opts).unwrap();
        assert_eq!(
            stats.len(),
            4,
            "all four undersized blocks should be merged, got {:?}",
            stats
        );
        let new_ids: std::collections::HashSet<_> =
            stats.iter().filter_map(|s| s.new_block_id.as_ref()).collect();
        assert_eq!(
            new_ids.len(),
            1,
            "undersized merge should collapse into a single new block"
        );
    }

    /// Strategy B commit gate: when `min_file_saved > file_count_saved`
    /// the group must be skipped. Single-block merges produce
    /// `file_count_saved = 0` and therefore never pass gate B.
    #[test]
    fn test_compact_undersized_respects_min_file_saved() {
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 4 * 1024 * 1024;
        cfg.storage.compression_algorithm = crate::config::CompressionAlgorithm::None;
        cfg.storage.compression_level = 0;
        let engine = Engine::open(cfg).unwrap();

        // One undersized high-util block.
        let payload: Vec<u8> = (0..150_000u32).map(|j| (j % 251) as u8).collect();
        let _id = engine.put_bytes(&payload, "solo.bin", None).unwrap();
        engine.seal_active_block().unwrap();

        // Strategy B only, require 3 files saved → group of size 1
        // yields file_count_saved=0 → no commit.
        let opts = CompactionOptions {
            threshold: 0.0,
            undersize_ratio: 0.5,
            min_file_saved: 3,
            min_block_age_secs: 0,
            disk_headroom_bytes: 0,
            force: false,
        };
        let stats = engine.compact_with(opts).unwrap();
        assert!(
            stats.is_empty(),
            "single-block undersized group should fail gate B, got {:?}",
            stats
        );
    }

    /// `force = true` bypasses both commit gates, allowing single-block
    /// merges to rewrite. The disk-headroom safety check is NOT
    /// bypassed — we don't test that here (would require filesystem
    /// manipulation), but the code path is a simple > comparison.
    #[test]
    fn test_compact_force_bypasses_commit_gates() {
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 4 * 1024 * 1024;
        cfg.storage.compression_algorithm = crate::config::CompressionAlgorithm::None;
        cfg.storage.compression_level = 0;
        let engine = Engine::open(cfg).unwrap();

        let payload: Vec<u8> = (0..150_000u32).map(|j| (j % 251) as u8).collect();
        let id = engine.put_bytes(&payload, "solo.bin", None).unwrap();
        engine.seal_active_block().unwrap();
        let old_bid = engine.meta.get_file(&id).unwrap().unwrap().chunks[0]
            .block_id
            .clone();

        // Same gate-B-failing config as above, but with force=true.
        let opts = CompactionOptions {
            threshold: 0.0,
            undersize_ratio: 0.5,
            min_file_saved: 3,
            min_block_age_secs: 0,
            disk_headroom_bytes: 0,
            force: true,
        };
        let stats = engine.compact_with(opts).unwrap();
        assert_eq!(
            stats.len(),
            1,
            "force=true must commit the single-block group"
        );
        assert_eq!(stats[0].old_block_id, old_bid);
        // File still reads back byte-identical.
        assert_eq!(engine.get_bytes(&id).unwrap(), payload);
    }

    /// Anti-thrash filter: blocks whose age is below
    /// `min_block_age_secs` must be excluded from both candidate
    /// predicates. The block we just sealed has age ≈ 0, so a high
    /// cooldown should make the pass a no-op even when the block
    /// would otherwise be a Strategy A candidate.
    #[test]
    fn test_compact_age_filter_excludes_fresh_blocks() {
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 16 * 1024 * 1024;
        let engine = Engine::open(cfg).unwrap();

        // Build a low-util block: upload two files, delete one.
        let p1: Vec<u8> = (0..150_000u32).map(|j| (j % 251) as u8).collect();
        let p2: Vec<u8> = (0..150_000u32).map(|j| ((j * 7) % 251) as u8).collect();
        let _keep = engine.put_bytes(&p1, "k.bin", None).unwrap();
        let drop_id = engine.put_bytes(&p2, "d.bin", None).unwrap();
        engine.seal_active_block().unwrap();
        engine.delete_file(&drop_id).unwrap();

        // High cooldown — the just-sealed block is younger than 1h.
        let opts = CompactionOptions {
            threshold: 0.9,
            undersize_ratio: 0.0,
            min_file_saved: 0,
            min_block_age_secs: 3600,
            disk_headroom_bytes: 0,
            force: false,
        };
        let stats = engine.compact_with(opts.clone()).unwrap();
        assert!(
            stats.is_empty(),
            "age filter should have excluded the fresh block, got {:?}",
            stats
        );

        // Same options with cooldown disabled → the low-util block now
        // compacts, proving the previous skip was driven by the age
        // filter and not by some other predicate.
        let opts_no_age = CompactionOptions {
            min_block_age_secs: 0,
            ..opts
        };
        let stats = engine.compact_with(opts_no_age).unwrap();
        assert_eq!(
            stats.len(),
            1,
            "without age filter the low-util block must compact, got {:?}",
            stats
        );
    }

    /// Deprecated-key detection: parsing a TOML file that still
    /// contains either of the two v0.4.6 keys must fail with a
    /// migration-hint error. Belongs-and-fails is better than
    /// silently-ignored-and-wrong-behavior.
    #[test]
    fn test_config_rejects_deprecated_v047_keys() {
        use crate::config::AppConfig;
        let base = r#"
[storage]
data_dir = "/tmp/x/data"
meta_dir = "/tmp/x/meta"
wal_dir  = "/tmp/x/wal"
block_file_size = 1073741824
chunk_size = 4194304
hash_algorithm = "sha256"
compression_algorithm = "zstd"
compression_level = 1
gc_threshold = 0.7
"#;
        let with_min_size = format!(
            "{}auto_compact_min_size_bytes = 65536\n\n[server]\nhttp_addr=\"0.0.0.0:8080\"\ngrpc_addr=\"0.0.0.0:8081\"\nmetrics_addr=\"0.0.0.0:9090\"\n",
            base
        );
        let err = AppConfig::from_toml_str(&with_min_size).unwrap_err();
        assert!(
            err.to_string().contains("auto_compact_min_size_bytes"),
            "expected a migration-hint error for auto_compact_min_size_bytes, got: {}",
            err
        );

        let with_min_benefit = format!(
            "{}auto_compact_min_benefit_bytes = 8388608\n\n[server]\nhttp_addr=\"0.0.0.0:8080\"\ngrpc_addr=\"0.0.0.0:8081\"\nmetrics_addr=\"0.0.0.0:9090\"\n",
            base
        );
        let err = AppConfig::from_toml_str(&with_min_benefit).unwrap_err();
        assert!(
            err.to_string().contains("auto_compact_min_benefit_bytes"),
            "expected a migration-hint error for auto_compact_min_benefit_bytes, got: {}",
            err
        );
    }

    #[test]
    fn test_seal_if_dead_unblocks_reclaim_of_active_block() {
        // Regression for v0.4.5 bug: user deletes the only file referencing
        // the active block → ref_count==0, but GC skipped it (pinned) and
        // compact_block refused it (active), so its disk bytes stuck around
        // forever. `seal_if_dead` is the narrow trigger that flips it to
        // sealed so the normal reclaim paths can touch it.
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 16 * 1024 * 1024;
        let engine = Engine::open(cfg).unwrap();

        // Upload one file, don't seal — active block has ref_count=1.
        let payload = vec![7u8; 100_000];
        let id = engine.put_bytes(&payload, "only.bin", None).unwrap();
        let active_id = engine.meta.get_file(&id).unwrap().unwrap().chunks[0]
            .block_id
            .clone();

        // seal_if_dead is a no-op while the file still references the block.
        assert!(
            engine.seal_if_dead().unwrap().is_none(),
            "must not seal a block with live references"
        );

        // Delete the file → ref_count goes to 0 on the active block.
        engine.delete_file(&id).unwrap();
        assert_eq!(
            engine.meta.get_block(&active_id).unwrap().unwrap().ref_count,
            0
        );

        // Now seal_if_dead should seal it.
        let sealed = engine.seal_if_dead().unwrap();
        assert!(sealed.is_some(), "active block with ref_count=0 must seal");
        assert_eq!(sealed.unwrap().block_id, active_id);

        // And a subsequent GC pass physically reclaims the newly-sealed block.
        let stats = rt.block_on(async { engine.trigger_gc().await.unwrap() });
        assert_eq!(stats.blocks_deleted, 1, "GC must reclaim the sealed block");
        assert!(engine.meta.get_block(&active_id).unwrap().is_none());
    }

    #[test]
    fn test_gc_purges_stale_dedup_for_deleted_block() {
        // Regression for the v0.4.3 bonus fix: after GC deletes a block
        // (ref_count → 0), its dedup entries must also be purged or the
        // next upload of identical content will point at non-existent data.
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 16 * 1024 * 1024;
        let engine = Engine::open(cfg).unwrap();

        // Single-chunk file so we can read the one chunk hash straight out
        // of FileMeta without second-guessing the chunker.
        let payload: Vec<u8> = (0..30_000u32).map(|i| (i % 251) as u8).collect();
        let id = engine.put_bytes(&payload, "once.bin", None).unwrap();
        let file = engine.meta.get_file(&id).unwrap().unwrap();
        assert_eq!(file.chunks.len(), 1);
        let hash = file.chunks[0].hash.clone();

        engine.seal_active_block().unwrap();
        engine.delete_file(&id).unwrap();

        // After delete, block has ref_count==0 but dedup row lingers until GC.
        assert!(engine.meta.get_dedup_entry(&hash).unwrap().is_some());

        // GC runs → ref_count==0 block deleted AND its dedup purged.
        rt.block_on(async { engine.trigger_gc().await.unwrap() });

        assert!(
            engine.meta.get_dedup_entry(&hash).unwrap().is_none(),
            "dedup entry must be purged after its block is GC'd"
        );
    }

    // ── v0.4.2 P5: per-file batch commit regressions ─────────────────────

    #[test]
    fn test_same_upload_chunk_dedup_via_overlay() {
        // Phase 6b-P1 sets chunk_size to 4 MiB by default. Construct a file
        // that is exactly two identical chunks. With P5's in-flight overlay,
        // the second chunk should reuse the first's (block_id, offset) — so
        // the resulting FileMeta has two chunks pointing at the same
        // (block_id, offset) pair, and dedup hit counter went up by one.
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        // Small chunks for a fast test.
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 4 * 1024 * 1024;
        let engine = Engine::open(cfg).unwrap();

        // 128 KiB of the same byte → exactly 2 × 64 KiB chunks, identical.
        let payload = vec![0xABu8; 128 * 1024];
        let file_id = engine.put_bytes(&payload, "twins.bin", None).unwrap();

        let file = engine.meta.get_file(&file_id).unwrap().unwrap();
        assert_eq!(file.chunks.len(), 2, "expected exactly 2 chunks");
        assert_eq!(
            (&file.chunks[0].block_id, file.chunks[0].offset),
            (&file.chunks[1].block_id, file.chunks[1].offset),
            "same-content chunks must point at the same block+offset \
             after P5 overlay dedup",
        );

        // Round-trip: the file must still read back byte-identical.
        let got = engine.get_bytes(&file_id).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn test_ref_count_matches_chunk_count_after_batch_commit() {
        // P5 invariant: after put_stream completes, the block's ref_count
        // equals the number of chunks (new or deduped) that landed on it.
        // Regression guard against off-by-one errors in the delta aggregation.
        let tmp = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        cfg.storage.chunk_size = 64 * 1024;
        cfg.storage.block_file_size = 4 * 1024 * 1024;
        let engine = Engine::open(cfg).unwrap();

        // 3 distinct 64 KiB chunks → 3 ref-count increments on the one block.
        let mut payload = Vec::with_capacity(3 * 64 * 1024);
        payload.extend(std::iter::repeat(1u8).take(64 * 1024));
        payload.extend(std::iter::repeat(2u8).take(64 * 1024));
        payload.extend(std::iter::repeat(3u8).take(64 * 1024));
        let file_id = engine.put_bytes(&payload, "triple.bin", None).unwrap();

        let file = engine.meta.get_file(&file_id).unwrap().unwrap();
        assert_eq!(file.chunks.len(), 3);
        let block_id = &file.chunks[0].block_id;
        // All 3 chunks landed on the same (currently active) block.
        assert!(file.chunks.iter().all(|c| c.block_id == *block_id));

        let block = engine.meta.get_block(block_id).unwrap().unwrap();
        assert_eq!(
            block.ref_count, 3,
            "block ref_count must equal chunk count after batched commit"
        );

        // Delete: decrements 3 times → ref_count back to 0.
        engine.delete_file(&file_id).unwrap();
        let block = engine.meta.get_block(block_id).unwrap().unwrap();
        assert_eq!(block.ref_count, 0, "delete must bring ref_count back to 0");
    }

    // ── v0.4.2: GC active-block protection regression ─────────────────────

    #[tokio::test]
    async fn test_gc_does_not_delete_pinned_active_block() {
        // Before the pin: GC running while a chunk was just inserted but
        // `update_block_ref_count(+1)` hadn't landed yet would delete the
        // active block file — corrupting every chunk written before seal.
        //
        // After the pin: the engine registers the block_id in
        // `pinned_blocks` before any metadata commit, so GC skips it even
        // when `list_unreferenced_blocks` returns it with ref_count == 0.
        //
        // Construction: write a chunk, then manually zero the block's
        // ref_count in redb to simulate the narrow race window where GC
        // snapshots mid-transaction. Run GC. The block must survive and
        // the chunk must still be readable.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        let file_id = engine
            .put_bytes(b"protected by pin", "guard.txt", None)
            .unwrap();

        // Grab the block_id the file points at and force its ref_count
        // back to 0. This mimics the "just-inserted, not yet incremented"
        // window the pin is designed to cover.
        let file = engine.meta.get_file(&file_id).unwrap().unwrap();
        let block_id = file.chunks[0].block_id.clone();
        let mut b = engine.meta.get_block(&block_id).unwrap().unwrap();
        b.ref_count = 0;
        engine.meta.delete_block(&block_id).unwrap();
        engine.meta.insert_block(&b).unwrap();
        // Sanity: the block IS in list_unreferenced_blocks now.
        assert!(engine
            .meta
            .list_unreferenced_blocks()
            .unwrap()
            .iter()
            .any(|x| x.block_id == block_id));

        // The engine pinned this block when it was created. GC must skip.
        let stats = engine.trigger_gc().await.unwrap();
        assert_eq!(
            stats.blocks_deleted, 0,
            "pinned active block must not be deleted by GC"
        );

        // And the data must still be readable.
        let got = engine.get_bytes(&file_id).unwrap();
        assert_eq!(got, b"protected by pin");
    }

    #[tokio::test]
    async fn test_gc_reaps_unpinned_unreferenced_block() {
        // The sibling of the test above: after the block is sealed the
        // engine unpins it. At that point a 0-ref_count block is *correctly*
        // a GC candidate — the pin must not linger forever.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);

        let file_id = engine
            .put_bytes(b"reap me", "reap.txt", None)
            .unwrap();
        // Seal → unpin.
        engine.seal_active_block().unwrap();

        let file = engine.meta.get_file(&file_id).unwrap().unwrap();
        let block_id = file.chunks[0].block_id.clone();
        // Force ref_count to 0 again; this time the block is unpinned.
        let mut b = engine.meta.get_block(&block_id).unwrap().unwrap();
        b.ref_count = 0;
        engine.meta.delete_block(&block_id).unwrap();
        engine.meta.insert_block(&b).unwrap();

        let stats = engine.trigger_gc().await.unwrap();
        assert_eq!(
            stats.blocks_deleted, 1,
            "sealed + unreferenced block must be reaped"
        );
    }

    // ── Phase 6b-P3: compression-outside-lock regression ──────────────────

    #[test]
    fn test_concurrent_puts_roundtrip_after_compression_offload() {
        // Phase 6b-P3 moved compress() out of the active_writer critical
        // section. Regression: 8 threads each uploading a distinct 256 KB
        // payload must all succeed and read back byte-identical. A single
        // mis-ordered bookkeeping step (wrong offset, wrong compressed len,
        // etc.) would surface here as a CRC failure during download.
        use std::sync::Arc as StdArc;
        let tmp = tempdir().unwrap();
        let engine = StdArc::new(open_engine(&tmp));

        let thread_count = 8;
        let payloads: Vec<Vec<u8>> = (0..thread_count)
            .map(|seed| {
                // Deterministic but distinct per thread. zstd must produce
                // different compressed output for each, so any cross-thread
                // data mixing would be detectable.
                let mut v = vec![0u8; 256 * 1024];
                for (i, b) in v.iter_mut().enumerate() {
                    *b = ((i as u32).wrapping_mul(1315423911u32).wrapping_add(seed as u32)
                        & 0xff) as u8;
                }
                v
            })
            .collect();

        let mut handles = Vec::new();
        for (i, data) in payloads.iter().cloned().enumerate() {
            let eng = engine.clone();
            handles.push(std::thread::spawn(move || {
                eng.put_bytes(&data, &format!("concurrent-{i}.bin"), None)
                    .unwrap()
            }));
        }
        let ids: Vec<String> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        for (id, expected) in ids.iter().zip(payloads.iter()) {
            let got = engine.get_bytes(id).unwrap();
            assert_eq!(got, *expected, "payload mismatch for {id}");
        }
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

    // ── v0.4.6: Custom file_id + conflict policy ─────────────────────────

    fn opts<'a>(name: &'a str, id: &'a str, policy: ConflictPolicy) -> PutOptions<'a> {
        PutOptions {
            file_name: name,
            content_type: None,
            file_id: Some(id),
            on_conflict: policy,
        }
    }

    #[test]
    fn test_put_with_custom_id_created() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        let (id, outcome) = engine
            .put_bytes_with(b"hello", opts("a.txt", "orders/1001", ConflictPolicy::Error))
            .unwrap();
        assert_eq!(id, "orders/1001");
        assert_eq!(outcome, PutOutcome::Created);
        assert_eq!(engine.get_bytes(&id).unwrap(), b"hello");
    }

    #[test]
    fn test_put_with_custom_id_conflict_error() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        engine
            .put_bytes_with(b"first", opts("a.txt", "same-id", ConflictPolicy::Error))
            .unwrap();

        // Second put with same id must reject.
        let err = engine
            .put_bytes_with(b"second", opts("a.txt", "same-id", ConflictPolicy::Error))
            .unwrap_err();
        assert!(matches!(err, JiHuanError::AlreadyExists(_)));

        // And the original content must be untouched.
        assert_eq!(engine.get_bytes("same-id").unwrap(), b"first");
    }

    #[test]
    fn test_put_with_custom_id_conflict_skip() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        engine
            .put_bytes_with(b"keep-me", opts("a.txt", "sticky", ConflictPolicy::Error))
            .unwrap();

        // Skip must return the existing id + Skipped, content unchanged.
        let (id, outcome) = engine
            .put_bytes_with(b"ignored", opts("a.txt", "sticky", ConflictPolicy::Skip))
            .unwrap();
        assert_eq!(id, "sticky");
        assert_eq!(outcome, PutOutcome::Skipped);
        assert_eq!(engine.get_bytes("sticky").unwrap(), b"keep-me");
    }

    #[test]
    fn test_put_with_custom_id_conflict_overwrite_different_content() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        engine
            .put_bytes_with(
                b"old content",
                opts("a.txt", "mutable", ConflictPolicy::Error),
            )
            .unwrap();

        let new_payload = vec![0xABu8; 16 * 1024];
        let (id, outcome) = engine
            .put_bytes_with(
                &new_payload,
                opts("a.txt", "mutable", ConflictPolicy::Overwrite),
            )
            .unwrap();
        assert_eq!(id, "mutable");
        assert_eq!(outcome, PutOutcome::Overwritten);
        assert_eq!(engine.get_bytes("mutable").unwrap(), new_payload);
    }

    #[test]
    fn test_put_with_custom_id_overwrite_same_content_is_idempotent() {
        // Hot path: uploading identical bytes to the same file_id must
        // succeed, produce PutOutcome::Overwritten, and leave the block
        // layer untouched beyond at most a trivial ref-count increment
        // + decrement that net to zero.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        let payload = vec![0x42u8; 8 * 1024];
        engine
            .put_bytes_with(
                &payload,
                opts("a.txt", "idempotent", ConflictPolicy::Error),
            )
            .unwrap();

        let blocks_before = engine.meta.block_count().unwrap();

        let (_id, outcome) = engine
            .put_bytes_with(
                &payload,
                opts("a.txt", "idempotent", ConflictPolicy::Overwrite),
            )
            .unwrap();
        assert_eq!(outcome, PutOutcome::Overwritten);
        let blocks_after = engine.meta.block_count().unwrap();
        assert_eq!(
            blocks_before, blocks_after,
            "idempotent overwrite must not spawn extra blocks"
        );
        assert_eq!(engine.get_bytes("idempotent").unwrap(), payload);
    }

    #[test]
    fn test_put_with_invalid_file_id_rejected() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        for bad in ["", "bad\nid", "/leading", "trailing/", "a//b", "..", "a/../b"] {
            let err = engine
                .put_bytes_with(b"x", opts("a.txt", bad, ConflictPolicy::Error))
                .unwrap_err();
            assert!(
                matches!(err, JiHuanError::InvalidFileId(_)),
                "expected InvalidFileId for {:?}, got {:?}",
                bad,
                err
            );
        }
    }

    #[test]
    fn test_put_with_nfc_normalisation() {
        // NFD input ("cafe\u{0301}.txt") must resolve to the same id as
        // NFC input ("caf\u{00E9}.txt") — second call with the NFD form
        // hits the already-stored record.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        let nfc_id = "caf\u{00E9}.txt";
        let nfd_id = "cafe\u{0301}.txt";

        let (id1, o1) = engine
            .put_bytes_with(b"first", opts("a.txt", nfc_id, ConflictPolicy::Error))
            .unwrap();
        assert_eq!(id1, nfc_id);
        assert_eq!(o1, PutOutcome::Created);

        // NFD input must collide with the existing NFC row.
        let err = engine
            .put_bytes_with(b"second", opts("a.txt", nfd_id, ConflictPolicy::Error))
            .unwrap_err();
        assert!(matches!(err, JiHuanError::AlreadyExists(_)));

        // …and Skip via NFD must return the NFC id.
        let (id2, o2) = engine
            .put_bytes_with(b"ignored", opts("a.txt", nfd_id, ConflictPolicy::Skip))
            .unwrap();
        assert_eq!(id2, nfc_id);
        assert_eq!(o2, PutOutcome::Skipped);
    }

    #[test]
    fn test_put_with_chinese_file_id() {
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        let (id, o) = engine
            .put_bytes_with(
                b"hello world",
                opts("\u{53d1}\u{7968}.pdf", "\u{8ba2}\u{5355}-2024/\u{53d1}\u{7968}.pdf", ConflictPolicy::Error),
            )
            .unwrap();
        assert_eq!(id, "\u{8ba2}\u{5355}-2024/\u{53d1}\u{7968}.pdf");
        assert_eq!(o, PutOutcome::Created);
        assert_eq!(engine.get_bytes(&id).unwrap(), b"hello world");
    }

    #[test]
    fn test_put_auto_id_when_no_file_id_supplied() {
        // When file_id is None, the engine mints a UUID and returns Created.
        let tmp = tempdir().unwrap();
        let engine = open_engine(&tmp);
        let (id, o) = engine
            .put_bytes_with(
                b"auto",
                PutOptions::new("x.txt", None),
            )
            .unwrap();
        assert_eq!(o, PutOutcome::Created);
        assert_eq!(id.len(), 32, "UUID simple form is 32 hex chars");
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
