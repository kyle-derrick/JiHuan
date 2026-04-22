use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::interval;

use crate::error::{JiHuanError, Result};
use crate::metadata::store::MetadataStore;
use crate::utils::now_secs;

/// Statistics from a single GC run
#[derive(Debug, Clone, Default)]
pub struct GcStats {
    pub blocks_deleted: u64,
    pub bytes_reclaimed: u64,
    pub partitions_deleted: u64,
    pub files_deleted: u64,
    /// Audit-log rows purged because they were older than the configured
    /// retention window. `0` when `audit_retention_days == 0` (disabled).
    pub audit_events_purged: u64,
    pub duration_ms: u64,
}

/// GC configuration subset (used directly by the GC runner)
#[derive(Debug, Clone)]
pub struct GcConfig {
    pub gc_threshold: f64,
    pub gc_interval_secs: u64,
    pub time_partition_hours: u32,
    pub data_dir: PathBuf,
    /// Audit-log retention in days. `0` disables the purge step entirely
    /// (events are retained forever). Default 90 via `AuthConfig`.
    pub audit_retention_days: u64,
}

/// A concurrent set of block IDs that the engine has declared "in use".
///
/// Phase v0.4.2: solves a latent data-loss race. Between `insert_block`
/// (ref_count = 0) and the first `update_block_ref_count(+1)` — or in the
/// P5 world, between `insert_block` and the end-of-file batch commit — a
/// concurrent GC tick that scans `list_unreferenced_blocks` will see the
/// fresh block with ref_count == 0 and happily delete it.
///
/// The engine registers every block it's actively writing to into this
/// set before calling `insert_block`, and only removes it once the block
/// is sealed (or the upload commits). GC consults the set and unconditionally
/// skips pinned blocks regardless of their ref_count.
///
/// `parking_lot::Mutex` keeps the critical section microsecond-scoped; the
/// set is expected to hold at most 1–2 entries in typical workloads.
pub type PinnedBlocks = Arc<parking_lot::Mutex<std::collections::HashSet<String>>>;

/// Construct a fresh empty pin set. Callers share a single instance between
/// the `Engine` and the `GcService` so both observe the same membership.
pub fn new_pinned_blocks() -> PinnedBlocks {
    Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()))
}

/// Background GC service
pub struct GcService {
    meta: Arc<MetadataStore>,
    config: GcConfig,
    running: Arc<AtomicBool>,
    last_stats: Arc<Mutex<Option<GcStats>>>,
    pinned_blocks: PinnedBlocks,
}

impl GcService {
    /// Construct a GC service that shares `pinned_blocks` with the engine.
    pub fn new(
        meta: Arc<MetadataStore>,
        config: GcConfig,
        pinned_blocks: PinnedBlocks,
    ) -> Self {
        Self {
            meta,
            config,
            running: Arc::new(AtomicBool::new(false)),
            last_stats: Arc::new(Mutex::new(None)),
            pinned_blocks,
        }
    }

    /// Start the background GC loop (runs in a spawned tokio task)
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        self.running.store(true, Ordering::Relaxed);
        let svc = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(svc.config.gc_interval_secs));
            let mut tick_seq: u64 = 0;
            loop {
                ticker.tick().await;
                if !svc.running.load(Ordering::Relaxed) {
                    break;
                }
                tick_seq = tick_seq.wrapping_add(1);
                // Debug-only preamble so `RUST_LOG=jihuan_core::gc=debug`
                // yields a numbered start/end pair per tick. At info level
                // the existing completion log is enough.
                tracing::debug!(
                    tick = tick_seq,
                    interval_secs = svc.config.gc_interval_secs,
                    "GC: tick starting"
                );
                match svc.run_once().await {
                    Ok(stats) => {
                        tracing::info!(
                            tick = tick_seq,
                            blocks_deleted = stats.blocks_deleted,
                            bytes_reclaimed = stats.bytes_reclaimed,
                            duration_ms = stats.duration_ms,
                            "GC run completed"
                        );
                        *svc.last_stats.lock().await = Some(stats);
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "GC run failed");
                    }
                }
            }
        })
    }

    /// Stop the background loop
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Run a single GC pass (can also be called manually)
    pub async fn run_once(&self) -> Result<GcStats> {
        let _start = now_secs();
        let t0 = std::time::Instant::now();
        let mut stats = GcStats::default();

        // Step 1: Delete expired time partitions
        let current_partition =
            crate::utils::current_partition_id(self.config.time_partition_hours);
        // We intentionally do NOT delete the current or the immediately prior partition
        // to give in-flight writes time to complete.
        let cutoff_partition = current_partition.saturating_sub(1);

        // Collect all partition IDs with files
        // (We use a simple scan: list all files and group by partition_id)
        // For production efficiency this could maintain an explicit partition registry.
        let expired_partitions = self.find_expired_partitions(cutoff_partition).await?;

        for pid in expired_partitions {
            let deleted_files = self.meta.delete_partition(pid)?;
            stats.files_deleted += deleted_files.len() as u64;
            stats.partitions_deleted += 1;

            // Decrement ref counts for all chunks in deleted files
            for file in &deleted_files {
                for chunk in &file.chunks {
                    match self.meta.update_block_ref_count(&chunk.block_id, -1) {
                        Ok(_) => {}
                        Err(JiHuanError::NotFound(_)) => {
                            tracing::warn!(
                                block_id = %chunk.block_id,
                                "GC: block not found when decrementing ref count"
                            );
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        // Step 2: Delete block files with ref_count == 0
        //
        // v0.4.2: skip blocks the engine is currently writing to. Without
        // this guard a fresh block whose metadata has been inserted but
        // whose first chunk hasn't yet incremented ref_count would be
        // eligible for deletion — and we'd lose every chunk written
        // before the first ref_count commit.
        let unreferenced = self.meta.list_unreferenced_blocks()?;
        // Clone the pin set once per tick so the Mutex is only held for
        // microseconds. HashSet::clone is O(n) but n ≤ active writer count.
        let pinned: std::collections::HashSet<String> = self.pinned_blocks.lock().clone();
        // Top-line summary so `RUST_LOG=jihuan_core::gc=debug` yields one
        // log line per tick summarising how much work the GC pass has to
        // do before it starts deleting. The per-block decisions below
        // then explain *which* blocks got reclaimed vs. skipped.
        tracing::debug!(
            unreferenced = unreferenced.len(),
            pinned_count = pinned.len(),
            "GC: beginning reclaim pass"
        );
        for block in unreferenced {
            if pinned.contains(&block.block_id) {
                tracing::debug!(
                    block_id = %block.block_id,
                    "GC: skipping pinned active block"
                );
                continue;
            }
            let path = PathBuf::from(&block.path);
            if path.exists() {
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        tracing::info!(
                            block_id = %block.block_id,
                            path = %block.path,
                            bytes = block.size,
                            "GC: deleted block file"
                        );
                        stats.blocks_deleted += 1;
                        stats.bytes_reclaimed += block.size;
                    }
                    Err(e) => {
                        tracing::warn!(
                            block_id = %block.block_id,
                            error = %e,
                            "GC: failed to delete block file"
                        );
                    }
                }
            }
            // Remove block metadata regardless of file existence
            self.meta.delete_block(&block.block_id)?;
            // v0.4.3: purge dedup entries that pointed at this now-gone block.
            // Otherwise a subsequent upload of the same content would short-
            // circuit to a non-existent (block_id, offset) and break reads.
            match self.meta.purge_dedup_for_block(&block.block_id) {
                Ok(n) if n > 0 => tracing::info!(
                    block_id = %block.block_id,
                    dedup_entries_removed = n,
                    "GC: purged stale dedup entries"
                ),
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    block_id = %block.block_id,
                    error = %e,
                    "GC: failed to purge dedup entries for deleted block"
                ),
            }
        }

        // Step 3: Purge audit-log rows older than the retention window.
        // Runs inline because it's a single bounded redb scan + delete and
        // piggybacking on the GC tick keeps the scheduler simple.
        if self.config.audit_retention_days > 0 {
            let retention_secs = self.config.audit_retention_days.saturating_mul(86_400);
            let cutoff = now_secs().saturating_sub(retention_secs);
            match self.meta.purge_audit_events_before(cutoff) {
                Ok(n) => {
                    if n > 0 {
                        tracing::info!(
                            audit_events_purged = n,
                            cutoff_secs = cutoff,
                            retention_days = self.config.audit_retention_days,
                            "GC: purged expired audit events"
                        );
                    }
                    stats.audit_events_purged = n;
                }
                Err(e) => {
                    // Audit retention failures never block reclaim — log and move on.
                    tracing::warn!(error = %e, "GC: audit event purge failed");
                }
            }
        }

        stats.duration_ms = t0.elapsed().as_millis() as u64;
        Ok(stats)
    }

    async fn find_expired_partitions(&self, cutoff: u64) -> Result<Vec<u64>> {
        // Scan all blocks to find referenced partition IDs
        // This is a lightweight approach for MVP; can be optimised with a partition registry
        let all_blocks = self.meta.list_all_blocks()?;

        // A partition is expired if ALL its files have been deleted (ref_count == 0 on blocks)
        // and the partition_id < cutoff.
        // We track which partitions have any live files by scanning the partition file lists.
        // Simple approach: any partition with id < cutoff is a candidate.
        let mut expired = Vec::new();

        // We detect partitions by scanning the PARTITION_FILES table indirectly via block creation times
        // Simplified: derive partition IDs from known block create times
        let partition_hours = self.config.time_partition_hours;
        let mut seen_partitions: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for block in &all_blocks {
            let pid = crate::utils::partition_id_from_ts(block.create_time, partition_hours);
            seen_partitions.insert(pid);
        }

        for pid in seen_partitions {
            if pid < cutoff {
                let file_ids = self.meta.list_files_in_partition(pid)?;
                if file_ids.is_empty() {
                    // All files already deleted from this partition
                    expired.push(pid);
                }
            }
        }

        Ok(expired)
    }

    pub async fn last_stats(&self) -> Option<GcStats> {
        self.last_stats.lock().await.clone()
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

/// Summary of the startup block cleanup. The caller logs this so operators
/// see exactly what happened on a recovery restart.
#[derive(Debug, Default, Clone, Copy)]
pub struct CleanupReport {
    /// Incomplete block files with **no** metadata row — safe to delete
    /// (they were never committed; the crash happened before any chunk
    /// metadata was persisted).
    pub deleted_orphans: u32,
    /// Incomplete block files that **are still referenced** by the metadata
    /// store. We NEVER delete these — they hold the only copy of the
    /// chunks for already-committed file metadata. Instead we rename them
    /// to `<block>.blk.orphan` so an operator can later run the recovery
    /// tool (or simply restore them manually once they've sealed).
    pub quarantined_referenced: u32,
}

/// Startup cleanup for block files that lack a valid footer.
///
/// Prior to the Phase-2.7 fix this function blindly deleted any incomplete
/// `.blk` — which destroyed data whenever the server was killed without
/// running `Engine::seal_active_block()` because the unsealed active
/// block's metadata rows were already committed in redb. The new behaviour:
///
/// * `.blk` has valid footer               → leave alone.
/// * `.blk` has no footer AND metadata row → **rename to `.blk.orphan`**.
///   Preserves data; caller can reseal or strip dangling metadata via
///   [`Engine::repair`](crate::Engine::repair).
/// * `.blk` has no footer AND no metadata row → **delete**. True crash
///   before any chunk commit; the file cannot possibly be referenced.
///
/// Returns a [`CleanupReport`] so the caller can log exactly what moved
/// where. Never returns an error on individual filesystem failures —
/// those are logged at `error!` and skipped so one bad file doesn't
/// prevent the server from starting.
pub fn cleanup_incomplete_blocks(
    data_dir: &PathBuf,
    meta: &crate::metadata::store::MetadataStore,
) -> Result<CleanupReport> {
    use crate::block::reader::BlockReader;
    let mut report = CleanupReport::default();
    if !data_dir.exists() {
        return Ok(report);
    }
    for entry in walkdir::WalkDir::new(data_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file() && e.path().extension().map(|x| x == "blk").unwrap_or(false)
        })
    {
        let path = entry.path();
        if BlockReader::is_complete(path) {
            continue;
        }

        // Block id is the file stem (see `Engine::block_path`).
        let block_id = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s.to_string(),
            None => {
                tracing::error!(path=%path.display(), "Startup: cannot derive block_id from file name, skipping");
                continue;
            }
        };

        // Does metadata still reference this block? We can't know ref_count
        // precisely because GC might be pending, but presence of a BlockMeta
        // row means dedup entries / ChunkMeta *might* be pointing here —
        // that's enough to refuse deletion.
        let meta_row = match meta.get_block(&block_id) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(block_id=%block_id, error=%e, "Startup: metadata lookup failed; skipping to be safe");
                continue;
            }
        };

        if meta_row.is_some() {
            // QUARANTINE: rename to .blk.orphan so data is preserved.
            let orphan_path = path.with_extension("blk.orphan");
            match std::fs::rename(path, &orphan_path) {
                Ok(()) => {
                    tracing::error!(
                        path = %path.display(),
                        orphan = %orphan_path.display(),
                        block_id = %block_id,
                        "Startup: unsealed block referenced by metadata — QUARANTINED to .blk.orphan. \
                         Files whose chunks live in this block will return 500 until you either (a) \
                         reseal the .blk.orphan and rename it back to .blk, or (b) set JIHUAN_REPAIR=1 \
                         to purge the dangling metadata."
                    );
                    report.quarantined_referenced += 1;
                }
                Err(e) => {
                    tracing::error!(path=%path.display(), error=%e, "Startup: failed to rename incomplete block");
                }
            }
        } else {
            // True orphan — no metadata row, safe to delete.
            tracing::warn!(path=%path.display(), block_id=%block_id, "Startup: deleting orphan incomplete block (no metadata reference)");
            if let Err(e) = std::fs::remove_file(path) {
                tracing::error!(path=%path.display(), error=%e, "Startup: failed to remove orphan block");
            } else {
                report.deleted_orphans += 1;
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::BlockMeta;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_store(tmp: &tempfile::TempDir) -> Arc<MetadataStore> {
        Arc::new(MetadataStore::open(tmp.path().join("meta.db")).unwrap())
    }

    #[tokio::test]
    async fn test_gc_deletes_unreferenced_block_file() {
        let tmp = tempdir().unwrap();
        let meta = make_store(&tmp);

        // Create a block file
        let block_path = tmp.path().join("data").join("old.blk");
        std::fs::create_dir_all(block_path.parent().unwrap()).unwrap();
        std::fs::write(&block_path, b"fake block data").unwrap();

        // Register block with ref_count = 0
        let block = BlockMeta {
            block_id: "blk_old".to_string(),
            ref_count: 0,
            create_time: 1000,
            path: block_path.to_str().unwrap().to_string(),
            size: 15,
        };
        meta.insert_block(&block).unwrap();

        let config = GcConfig {
            gc_threshold: 0.7,
            gc_interval_secs: 60,
            time_partition_hours: 24,
            data_dir: tmp.path().join("data"),
            audit_retention_days: 0,
        };
        let svc = GcService::new(meta.clone(), config, new_pinned_blocks());
        let stats = svc.run_once().await.unwrap();

        assert_eq!(stats.blocks_deleted, 1);
        assert!(!block_path.exists(), "Block file should have been deleted");
        assert!(meta.get_block("blk_old").unwrap().is_none());
    }

    #[tokio::test]
    async fn test_gc_skips_referenced_blocks() {
        let tmp = tempdir().unwrap();
        let meta = make_store(&tmp);

        let block_path = tmp.path().join("active.blk");
        std::fs::write(&block_path, b"active block").unwrap();

        let mut block = BlockMeta::new("blk_active", block_path.to_str().unwrap(), 12, 1000);
        block.ref_count = 3;
        meta.insert_block(&block).unwrap();

        let config = GcConfig {
            gc_threshold: 0.7,
            gc_interval_secs: 60,
            time_partition_hours: 24,
            data_dir: tmp.path().to_path_buf(),
            audit_retention_days: 0,
        };
        let svc = GcService::new(meta.clone(), config, new_pinned_blocks());
        let stats = svc.run_once().await.unwrap();

        assert_eq!(stats.blocks_deleted, 0);
        assert!(block_path.exists(), "Active block should NOT be deleted");
    }

    #[tokio::test]
    async fn test_gc_purges_expired_audit_events() {
        // Phase 2.6 retention: a GC tick with audit_retention_days = 1 must
        // delete events older than 86_400 seconds and leave fresher ones alone.
        use crate::metadata::types::{AuditEvent, AuditResult};
        let tmp = tempdir().unwrap();
        let meta = make_store(&tmp);

        let now = now_secs();
        // Old event: 10 days ago, must be purged.
        meta.insert_audit_event(&AuditEvent {
            ts: now.saturating_sub(10 * 86_400),
            actor_key_id: Some("k_old".into()),
            actor_ip: None,
            action: "auth.login".into(),
            target: None,
            result: AuditResult::Ok,
            http_status: Some(200),
        })
        .unwrap();
        // Fresh event: 5 minutes ago, must survive.
        meta.insert_audit_event(&AuditEvent {
            ts: now.saturating_sub(300),
            actor_key_id: Some("k_new".into()),
            actor_ip: None,
            action: "auth.login".into(),
            target: None,
            result: AuditResult::Ok,
            http_status: Some(200),
        })
        .unwrap();

        let config = GcConfig {
            gc_threshold: 0.7,
            gc_interval_secs: 60,
            time_partition_hours: 24,
            data_dir: tmp.path().to_path_buf(),
            audit_retention_days: 1,
        };
        let svc = GcService::new(meta.clone(), config, new_pinned_blocks());
        let stats = svc.run_once().await.unwrap();

        assert_eq!(stats.audit_events_purged, 1, "the 10-day-old event should be purged");
        let remaining = meta.list_audit_events(None, None, None, None, 10).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].actor_key_id.as_deref(), Some("k_new"));
    }

    #[tokio::test]
    async fn test_gc_skips_audit_purge_when_retention_disabled() {
        // `audit_retention_days = 0` is the explicit "keep forever" sentinel.
        use crate::metadata::types::{AuditEvent, AuditResult};
        let tmp = tempdir().unwrap();
        let meta = make_store(&tmp);
        meta.insert_audit_event(&AuditEvent {
            ts: 1, // Ancient
            actor_key_id: None,
            actor_ip: None,
            action: "auth.login".into(),
            target: None,
            result: AuditResult::Ok,
            http_status: None,
        })
        .unwrap();

        let config = GcConfig {
            gc_threshold: 0.7,
            gc_interval_secs: 60,
            time_partition_hours: 24,
            data_dir: tmp.path().to_path_buf(),
            audit_retention_days: 0,
        };
        let svc = GcService::new(meta.clone(), config, new_pinned_blocks());
        let stats = svc.run_once().await.unwrap();

        assert_eq!(stats.audit_events_purged, 0);
        assert_eq!(meta.list_audit_events(None, None, None, None, 10).unwrap().len(), 1);
    }

    #[test]
    fn test_cleanup_deletes_orphan_incomplete_blocks() {
        // No metadata row → true orphan, should be deleted.
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let store_path = tmp.path().join("m.db");
        let store = MetadataStore::open(&store_path).unwrap();

        let incomplete = data_dir.join("inc.blk");
        std::fs::write(&incomplete, crate::block::format::BLOCK_MAGIC).unwrap();

        let report = cleanup_incomplete_blocks(&data_dir, &store).unwrap();
        assert_eq!(report.deleted_orphans, 1);
        assert_eq!(report.quarantined_referenced, 0);
        assert!(!incomplete.exists());
    }

    #[test]
    fn test_cleanup_quarantines_referenced_incomplete_blocks() {
        // Metadata row exists → block was the active writer when we crashed.
        // Must NOT be deleted; must be renamed to .blk.orphan.
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let store_path = tmp.path().join("m.db");
        let store = MetadataStore::open(&store_path).unwrap();

        let block_id = "active-block-00000000000000000000000";
        let block_path = data_dir.join(format!("{}.blk", block_id));
        std::fs::write(&block_path, crate::block::format::BLOCK_MAGIC).unwrap();

        store
            .insert_block(&BlockMeta::new(
                block_id,
                block_path.to_str().unwrap(),
                0,
                0,
            ))
            .unwrap();

        let report = cleanup_incomplete_blocks(&data_dir, &store).unwrap();
        assert_eq!(report.deleted_orphans, 0);
        assert_eq!(report.quarantined_referenced, 1);
        assert!(!block_path.exists(), "original .blk must be renamed, not deleted");
        let orphan = data_dir.join(format!("{}.blk.orphan", block_id));
        assert!(orphan.exists(), "expected .blk.orphan to be created");
    }

    #[test]
    fn test_cleanup_leaves_complete_blocks_alone() {
        use crate::block::writer::BlockWriter;
        use crate::config::{CompressionAlgorithm, HashAlgorithm};
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();
        let store_path = tmp.path().join("m.db");
        let store = MetadataStore::open(&store_path).unwrap();

        let complete = data_dir.join("complete.blk");
        let mut w = BlockWriter::create(
            &complete,
            "complete-00000000000000000000000000000",
            CompressionAlgorithm::None,
            0,
            HashAlgorithm::None,
            1024 * 1024,
        )
        .unwrap();
        w.write_chunk(b"data", "").unwrap();
        w.finish().unwrap();

        let report = cleanup_incomplete_blocks(&data_dir, &store).unwrap();
        assert_eq!(report.deleted_orphans, 0);
        assert_eq!(report.quarantined_referenced, 0);
        assert!(complete.exists());
    }
}
