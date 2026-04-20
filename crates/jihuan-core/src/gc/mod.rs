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
    pub duration_ms: u64,
}

/// GC configuration subset (used directly by the GC runner)
#[derive(Debug, Clone)]
pub struct GcConfig {
    pub gc_threshold: f64,
    pub gc_interval_secs: u64,
    pub time_partition_hours: u32,
    pub data_dir: PathBuf,
}

/// Background GC service
pub struct GcService {
    meta: Arc<MetadataStore>,
    config: GcConfig,
    running: Arc<AtomicBool>,
    last_stats: Arc<Mutex<Option<GcStats>>>,
}

impl GcService {
    pub fn new(meta: Arc<MetadataStore>, config: GcConfig) -> Self {
        Self {
            meta,
            config,
            running: Arc::new(AtomicBool::new(false)),
            last_stats: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the background GC loop (runs in a spawned tokio task)
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        self.running.store(true, Ordering::Relaxed);
        let svc = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(svc.config.gc_interval_secs));
            loop {
                ticker.tick().await;
                if !svc.running.load(Ordering::Relaxed) {
                    break;
                }
                match svc.run_once().await {
                    Ok(stats) => {
                        tracing::info!(
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
        let unreferenced = self.meta.list_unreferenced_blocks()?;
        for block in unreferenced {
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

/// Delete incomplete (no valid footer) block files in the data directory.
/// Called during startup crash recovery.
pub fn cleanup_incomplete_blocks(data_dir: &PathBuf) -> Result<u32> {
    use crate::block::reader::BlockReader;
    let mut removed = 0u32;
    if !data_dir.exists() {
        return Ok(0);
    }
    for entry in walkdir::WalkDir::new(data_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file() && e.path().extension().map(|x| x == "blk").unwrap_or(false)
        })
    {
        let path = entry.path();
        if !BlockReader::is_complete(path) {
            tracing::warn!(
                path = %path.display(),
                "Crash recovery: removing incomplete block file"
            );
            if let Err(e) = std::fs::remove_file(path) {
                tracing::error!(path=%path.display(), error=%e, "Failed to remove incomplete block");
            } else {
                removed += 1;
            }
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::{BlockMeta, ChunkMeta, FileMeta};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_store(tmp: &tempfile::TempDir) -> Arc<MetadataStore> {
        Arc::new(MetadataStore::open(tmp.path().join("meta.db")).unwrap())
    }

    fn make_file(id: &str, partition_id: u64, block_id: &str) -> FileMeta {
        FileMeta {
            file_id: id.to_string(),
            file_name: format!("{}.txt", id),
            file_size: 1024,
            create_time: partition_id * 86400,
            partition_id,
            chunks: vec![ChunkMeta {
                block_id: block_id.to_string(),
                offset: 0,
                original_size: 1024,
                compressed_size: 512,
                hash: "abc".to_string(),
                index: 0,
            }],
            content_type: None,
        }
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
        };
        let svc = GcService::new(meta.clone(), config);
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
        };
        let svc = GcService::new(meta.clone(), config);
        let stats = svc.run_once().await.unwrap();

        assert_eq!(stats.blocks_deleted, 0);
        assert!(block_path.exists(), "Active block should NOT be deleted");
    }

    #[test]
    fn test_cleanup_incomplete_blocks() {
        let tmp = tempdir().unwrap();
        let data_dir = tmp.path().to_path_buf();

        // Create a fake incomplete block (just magic, no footer)
        let incomplete = data_dir.join("inc.blk");
        std::fs::write(&incomplete, &crate::block::format::BLOCK_MAGIC).unwrap();

        // Create a valid complete block
        use crate::block::writer::BlockWriter;
        use crate::config::{CompressionAlgorithm, HashAlgorithm};
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

        let removed = cleanup_incomplete_blocks(&data_dir).unwrap();
        assert_eq!(removed, 1);
        assert!(!incomplete.exists());
        assert!(complete.exists());
    }
}
