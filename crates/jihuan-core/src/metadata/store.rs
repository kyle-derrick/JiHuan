use std::path::Path;
use std::sync::Arc;

use redb::{
    Database, MultimapTableDefinition, ReadableTable, ReadableTableMetadata, TableDefinition,
    TableError,
};

use crate::error::{JiHuanError, Result};
use crate::metadata::types::{ApiKeyMeta, AuditEvent, BlockMeta, DedupEntry, FileMeta};

// Table definitions: key → value (v0.4.8: values bincode-encoded blobs).
const FILES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("files");
const BLOCKS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("blocks");
// v0.4.8: key is now a raw 32-byte hash (was 64-char ASCII hex) to
// match the in-memory `ChunkMeta.hash: Option<[u8; 32]>` representation
// — saves ~32 B/key in the redb index and avoids a hex-encode on every
// lookup/insert.
const DEDUP_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("dedup");
const PARTITIONS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("partitions");
/// Maps partition_id →→ file_id (one-to-many). v0.4.6: switched from
/// `TableDefinition<&str, Vec<String>>` to multimap so that a single
/// partition holding 10k+ files no longer rewrites a multi-MB row on
/// every insert/delete. The key space is unchanged; only the value
/// representation flips from "encoded vec" to "independent entries".
const PARTITION_FILES_TABLE: MultimapTableDefinition<&str, &str> =
    MultimapTableDefinition::new("partition_files");
/// Maps key_id → ApiKeyMeta (bincode-encoded)
const APIKEYS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("apikeys");
/// Maps key_hash → key_id (for fast lookup by raw key hash)
const APIKEY_HASH_TABLE: TableDefinition<&str, &str> = TableDefinition::new("apikey_hash");
/// Audit log (Phase 2.6). Keyed by 16-byte BE composite `[ts_secs:u64][seq:u64]`
/// so that table iteration returns events in chronological order. The
/// `seq` component is monotonically increased per process so that two
/// events landing in the same second still have a stable ordering.
const AUDIT_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("audit");

// v0.4.8: metadata persistence switched from `serde_json` to bincode
// via its serde compatibility shim. Benefits:
//   * Field names disappear from the wire → ~50% smaller rows for the
//     typical `FileMeta.chunks[]` payload, which is the hot spot on
//     small-file workloads.
//   * Numeric fields become fixed-width little-endian → faster decode
//     and no string allocation for number parsing.
//   * `[u8; N]` serializes as `N` raw bytes instead of a JSON array of
//     decimal digits, which unlocks the hash = `[u8; 32]` change.
//
// bincode 2.x uses a configurable encoding; we stick with
// `bincode::config::standard()` (little-endian, variable-length integer
// encoding) everywhere to keep one canonical byte layout across redb
// values and the WAL frame.
fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>> {
    bincode::serde::encode_to_vec(v, bincode::config::standard())
        .map_err(|e| JiHuanError::Serialization(e.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let (v, _consumed) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|e| JiHuanError::Serialization(e.to_string()))?;
    Ok(v)
}

/// Thread-safe metadata store backed by redb
pub struct MetadataStore {
    db: Arc<Database>,
    /// Monotonic sequence counter for audit events — disambiguates events
    /// that share the same second timestamp. (Phase 2.6)
    audit_seq: std::sync::atomic::AtomicU64,
}

impl MetadataStore {
    /// Open (or create) the metadata database at the given path
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(JiHuanError::Io)?;
        }

        let db = Database::create(path).map_err(|e| JiHuanError::Database(e.to_string()))?;

        // Ensure all tables exist
        {
            let tx = db
                .begin_write()
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            tx.open_table(FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            tx.open_table(BLOCKS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            tx.open_table(DEDUP_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            tx.open_table(PARTITIONS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            // v0.4.6: partition_files is now a MultimapTable. If the
            // on-disk database was produced by an earlier build its
            // partition_files table has a scalar value type — redb
            // surfaces this as `TableTypeMismatch`. Translate that into
            // a human-actionable error so operators don't have to grep
            // through redb source.
            match tx.open_multimap_table(PARTITION_FILES_TABLE) {
                Ok(_) => (),
                Err(TableError::TableTypeMismatch { .. }) => {
                    return Err(JiHuanError::Database(format!(
                        "redb schema mismatch — table `partition_files` has an \
                         incompatible definition (pre-v0.4.6 layout detected).\n\n\
                         RESOLUTION (development only):\n  \
                         1. Stop jihuan-server.\n  \
                         2. Delete the data directory:  rm -rf {}\n  \
                         3. Restart the server; a fresh database will be created.\n\n\
                         IMPORTANT: this WILL DISCARD ALL DATA. Do not run this in \
                         production — a proper migration tool is not yet shipped.",
                        path.display()
                    )));
                }
                Err(e) => return Err(JiHuanError::Database(e.to_string())),
            }
            tx.open_table(APIKEYS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            tx.open_table(APIKEY_HASH_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            tx.open_table(AUDIT_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            tx.commit()
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }

        Ok(Self {
            db: Arc::new(db),
            audit_seq: std::sync::atomic::AtomicU64::new(0),
        })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // File operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Atomic per-file commit (v0.4.2 P5).
    ///
    /// Commits the file record, its partition-index update, a batch of
    /// block ref-count deltas, and a batch of new dedup entries in **one**
    /// redb write transaction. This replaces the old pattern of N inline
    /// `update_block_ref_count` + N `insert_dedup_entry` + 1 `insert_file`
    /// commits (≈ 2N+1 fsyncs for an N-chunk file) with exactly one fsync.
    ///
    /// Semantics:
    ///  - `ref_count_deltas` contains `(block_id, delta)` pairs. Missing
    ///    blocks are ignored with a warning trace; this matches
    ///    `update_block_ref_count`'s historical tolerance for the
    ///    `NotFound` case.
    ///  - `new_dedup_entries` are inserted unconditionally; callers must
    ///    not include entries that are already present.
    ///  - The file record is inserted with duplicate-detection (errors
    ///    on collision, same as `insert_file`).
    ///
    /// Atomicity guarantee: on commit success every caller-visible state
    /// change lands together; on failure, none does.
    pub fn commit_file_batch(
        &self,
        file: &FileMeta,
        ref_count_deltas: &std::collections::HashMap<String, i64>,
        new_dedup_entries: &[DedupEntry],
    ) -> Result<()> {
        tracing::debug!(
            file_id = %file.file_id,
            deltas = ref_count_deltas.len(),
            new_dedups = new_dedup_entries.len(),
            "MetadataStore::commit_file_batch"
        );
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        {
            // 1) Insert the file record (error on duplicate)
            let mut files = tx
                .open_table(FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            if files
                .get(file.file_id.as_str())
                .map_err(|e| JiHuanError::Database(e.to_string()))?
                .is_some()
            {
                return Err(JiHuanError::AlreadyExists(format!(
                    "File '{}' already exists",
                    file.file_id
                )));
            }
            let file_bytes = encode(file)?;
            files
                .insert(file.file_id.as_str(), file_bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        {
            // 2) Partition →→ file_id multimap (v0.4.6): single O(log n)
            // insert per file, no decode/encode of a growing Vec.
            let mut pt = tx
                .open_multimap_table(PARTITION_FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let pid = file.partition_id.to_string();
            pt.insert(pid.as_str(), file.file_id.as_str())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        {
            // 3) Block ref-count deltas
            let mut blocks = tx
                .open_table(BLOCKS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            for (block_id, delta) in ref_count_deltas {
                // Decode-then-drop the read guard before re-inserting so
                // redb's borrow checker sees sequential borrows.
                let current: Option<BlockMeta> = {
                    let guard = blocks
                        .get(block_id.as_str())
                        .map_err(|e| JiHuanError::Database(e.to_string()))?;
                    match guard {
                        Some(v) => Some(decode(v.value())?),
                        None => None,
                    }
                };
                match current {
                    Some(mut meta) => {
                        let new_count = (meta.ref_count as i64 + delta).max(0) as u64;
                        meta.ref_count = new_count;
                        let bytes = encode(&meta)?;
                        blocks
                            .insert(block_id.as_str(), bytes.as_slice())
                            .map_err(|e| JiHuanError::Database(e.to_string()))?;
                    }
                    None => {
                        tracing::warn!(
                            block_id = %block_id,
                            delta = delta,
                            "commit_file_batch: block missing for ref-count delta"
                        );
                    }
                }
            }
        }
        {
            // 4) Dedup entries (caller filters out duplicates beforehand)
            let mut dedup = tx
                .open_table(DEDUP_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            for entry in new_dedup_entries {
                let bytes = encode(entry)?;
                // v0.4.8: DEDUP_TABLE key is raw bytes (&[u8]), not hex.
                dedup
                    .insert(&entry.hash[..], bytes.as_slice())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
        }
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(())
    }

    /// Atomic replace-in-place for an existing `file_id` (v0.4.6).
    ///
    /// Used by the `on_conflict = Overwrite` branch of
    /// [`crate::Engine::put_stream_with`]. Semantics:
    ///
    /// 1. `old.chunks` contribute `-1` ref-count deltas that are folded
    ///    into `ref_count_deltas` (same block appearing on both sides
    ///    can net to zero, which is the hot path for a truly idempotent
    ///    overwrite).
    /// 2. `FILES_TABLE[new.file_id]` is overwritten with the new row
    ///    (same key as `old.file_id` — overwrites are identity-preserving
    ///    on `file_id`).
    /// 3. If `new.partition_id != old.partition_id` the partition index
    ///    is migrated: remove from the old partition's multimap entry,
    ///    insert into the new one.
    /// 4. The merged ref-count deltas are applied to `BLOCKS_TABLE`
    ///    exactly once each — after the merge in (1) no block appears
    ///    twice in the delta map.
    /// 5. New dedup entries are inserted (caller must already have
    ///    filtered out ones that were present before this call).
    ///
    /// All five steps land in one redb write transaction: one fsync,
    /// all-or-nothing visibility. If the caller crashes after step (1)
    /// in the engine (i.e. bytes were streamed into the active block
    /// but this function never ran) the old FileMeta remains fully
    /// intact and the orphan slack bytes are compacted out later.
    pub fn commit_file_batch_swap(
        &self,
        new_file: &FileMeta,
        old_file: &FileMeta,
        mut ref_count_deltas: std::collections::HashMap<String, i64>,
        new_dedup_entries: &[DedupEntry],
    ) -> Result<()> {
        debug_assert_eq!(
            new_file.file_id, old_file.file_id,
            "swap requires identical file_id"
        );
        tracing::debug!(
            file_id = %new_file.file_id,
            old_size = old_file.file_size,
            new_size = new_file.file_size,
            deltas = ref_count_deltas.len(),
            new_dedups = new_dedup_entries.len(),
            "MetadataStore::commit_file_batch_swap"
        );

        // Merge old.chunks -1 decrements — each chunk reference in old
        // must be released. The hot path is the idempotent overwrite
        // where new chunks dedup onto the same blocks, so +1 − 1 nets
        // to 0 for that block and we skip the whole redb rewrite.
        // v0.4.8: `chunk.block_id` is now resolved via the owning
        // file's pool. Resolve once per chunk — cheap Vec index.
        for chunk in &old_file.chunks {
            *ref_count_deltas
                .entry(chunk.block_id(old_file).to_string())
                .or_insert(0) -= 1;
        }

        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;

        {
            // 1) Overwrite FILES_TABLE[file_id] with the new row. `insert`
            //    on the same key replaces the value.
            let mut files = tx
                .open_table(FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let file_bytes = encode(new_file)?;
            files
                .insert(new_file.file_id.as_str(), file_bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }

        // 2) Partition index migration — only if the partition changed.
        if new_file.partition_id != old_file.partition_id {
            let mut pt = tx
                .open_multimap_table(PARTITION_FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let old_pid = old_file.partition_id.to_string();
            let new_pid = new_file.partition_id.to_string();
            pt.remove(old_pid.as_str(), old_file.file_id.as_str())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            pt.insert(new_pid.as_str(), new_file.file_id.as_str())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }

        {
            // 3) Apply merged ref-count deltas. Zero-net entries are
            //    skipped to avoid touching blocks that don't need
            //    updating — this is what makes idempotent overwrite
            //    free.
            let mut blocks = tx
                .open_table(BLOCKS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            for (block_id, delta) in &ref_count_deltas {
                if *delta == 0 {
                    continue;
                }
                let current: Option<BlockMeta> = {
                    let guard = blocks
                        .get(block_id.as_str())
                        .map_err(|e| JiHuanError::Database(e.to_string()))?;
                    match guard {
                        Some(v) => Some(decode(v.value())?),
                        None => None,
                    }
                };
                match current {
                    Some(mut meta) => {
                        let new_count = (meta.ref_count as i64 + *delta).max(0) as u64;
                        meta.ref_count = new_count;
                        let bytes = encode(&meta)?;
                        blocks
                            .insert(block_id.as_str(), bytes.as_slice())
                            .map_err(|e| JiHuanError::Database(e.to_string()))?;
                    }
                    None => {
                        tracing::warn!(
                            block_id = %block_id,
                            delta,
                            "commit_file_batch_swap: block missing for ref-count delta"
                        );
                    }
                }
            }
        }

        {
            // 4) New dedup entries (caller already filtered existing ones).
            let mut dedup = tx
                .open_table(DEDUP_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            for entry in new_dedup_entries {
                let bytes = encode(entry)?;
                // v0.4.8: DEDUP_TABLE key is raw bytes (&[u8]), not hex.
                dedup
                    .insert(&entry.hash[..], bytes.as_slice())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
        }

        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(())
    }

    /// Get a file by its ID
    pub fn get_file(&self, file_id: &str) -> Result<Option<FileMeta>> {
        tracing::debug!(file_id = %file_id, "MetadataStore::get_file");
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(FILES_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        match table
            .get(file_id)
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            Some(v) => Ok(Some(decode(v.value())?)),
            None => Ok(None),
        }
    }

    /// Delete a file record. Returns the deleted FileMeta if it existed.
    pub fn delete_file(&self, file_id: &str) -> Result<Option<FileMeta>> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;

        let file_opt: Option<FileMeta> = {
            let mut table = tx
                .open_table(FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let raw = table
                .remove(file_id)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            match raw {
                Some(v) => {
                    let bytes = v.value().to_vec();
                    Some(decode(&bytes)?)
                }
                None => None,
            }
        };

        if let Some(ref file) = file_opt {
            // v0.4.6 multimap: O(log n) single-entry removal, no Vec roundtrip.
            let mut pt = tx
                .open_multimap_table(PARTITION_FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let pid = file.partition_id.to_string();
            pt.remove(pid.as_str(), file_id)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }

        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(file_opt)
    }

    /// List all files (full scan, for admin/UI use).
    ///
    /// Materialises every `FileMeta` into a `Vec`, so at ~1 M files this
    /// reaches ~hundreds of MB of heap. Aggregation-only callers (e.g. the
    /// compaction scanner) should prefer [`Self::for_each_file`] which
    /// streams one row at a time.
    pub fn list_all_files(&self) -> Result<Vec<FileMeta>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(FILES_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let mut result = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            let (_, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
            let file: FileMeta = decode(v.value())?;
            result.push(file);
        }
        // Sort newest first
        result.sort_by(|a, b| b.create_time.cmp(&a.create_time));
        Ok(result)
    }

    /// Streaming variant of [`Self::list_all_files`]: invokes `f` for every
    /// `FileMeta` row without materialising the whole table in memory. The
    /// read transaction is held for the entire walk, so concurrent writers
    /// stall at commit time — keep the callback cheap.
    ///
    /// v0.4.5: added to let `compute_block_live_info` scale to million-file
    /// deployments. The row ordering follows redb's B-tree order, **not**
    /// `create_time`; callers that need chronological order should still
    /// use `list_all_files`.
    pub fn for_each_file<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(FileMeta) -> Result<()>,
    {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(FILES_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        for entry in table
            .iter()
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            let (_, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
            let file: FileMeta = decode(v.value())?;
            f(file)?;
        }
        Ok(())
    }

    /// List all file IDs in a partition.
    ///
    /// v0.4.6: backed by a `MultimapTable`, so this streams entries from
    /// the B-tree without decoding a growing `Vec<String>` blob. The
    /// returned `Vec` is materialised for caller convenience; callers
    /// expecting very large partitions should prefer
    /// [`Self::count_files_in_partition`] when they only need the count.
    pub fn list_files_in_partition(&self, partition_id: u64) -> Result<Vec<String>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_multimap_table(PARTITION_FILES_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let pid = partition_id.to_string();
        let iter = table
            .get(pid.as_str())
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let mut out = Vec::new();
        for entry in iter {
            let v = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
            out.push(v.value().to_string());
        }
        Ok(out)
    }

    /// Count files in a partition without materialising the id list.
    pub fn count_files_in_partition(&self, partition_id: u64) -> Result<u64> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_multimap_table(PARTITION_FILES_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let pid = partition_id.to_string();
        let iter = table
            .get(pid.as_str())
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(iter.count() as u64)
    }

    /// Delete all files in a partition. Returns the list of deleted FileMeta.
    pub fn delete_partition(&self, partition_id: u64) -> Result<Vec<FileMeta>> {
        let file_ids = self.list_files_in_partition(partition_id)?;
        let mut deleted = Vec::with_capacity(file_ids.len());
        for fid in &file_ids {
            if let Some(f) = self.delete_file(fid)? {
                deleted.push(f);
            }
        }
        Ok(deleted)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Block operations
    // ─────────────────────────────────────────────────────────────────────────

    pub fn insert_block(&self, block: &BlockMeta) -> Result<()> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        {
            let mut table = tx
                .open_table(BLOCKS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let bytes = encode(block)?;
            table
                .insert(block.block_id.as_str(), bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn get_block(&self, block_id: &str) -> Result<Option<BlockMeta>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(BLOCKS_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        match table
            .get(block_id)
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            Some(v) => Ok(Some(decode(v.value())?)),
            None => Ok(None),
        }
    }

    pub fn update_block_ref_count(&self, block_id: &str, delta: i64) -> Result<u64> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let new_ref_count = {
            let mut table = tx
                .open_table(BLOCKS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let mut block: BlockMeta = match table
                .get(block_id)
                .map_err(|e| JiHuanError::Database(e.to_string()))?
            {
                Some(v) => decode(v.value())?,
                None => {
                    return Err(JiHuanError::NotFound(format!(
                        "Block '{}' not found",
                        block_id
                    )))
                }
            };
            let new_count = (block.ref_count as i64 + delta).max(0) as u64;
            block.ref_count = new_count;
            let bytes = encode(&block)?;
            table
                .insert(block_id, bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            new_count
        };
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(new_ref_count)
    }

    pub fn delete_block(&self, block_id: &str) -> Result<Option<BlockMeta>> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let result = {
            let mut table = tx
                .open_table(BLOCKS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let raw = table
                .remove(block_id)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            match raw {
                Some(v) => {
                    let bytes = v.value().to_vec();
                    Some(decode(&bytes)?)
                }
                None => None,
            }
        };
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(result)
    }

    /// List all block IDs with ref_count == 0
    pub fn list_unreferenced_blocks(&self) -> Result<Vec<BlockMeta>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(BLOCKS_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let mut result = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            let (_, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
            let block: BlockMeta = decode(v.value())?;
            if block.ref_count == 0 {
                result.push(block);
            }
        }
        Ok(result)
    }

    /// List all blocks
    pub fn list_all_blocks(&self) -> Result<Vec<BlockMeta>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(BLOCKS_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let mut result = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            let (_, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
            let block: BlockMeta = decode(v.value())?;
            result.push(block);
        }
        Ok(result)
    }

    /// v0.4.3 compaction: atomically rewrite every reference to `old_block_id`.
    ///
    /// This is the metadata-side half of block compaction. The caller has
    /// already built a new block file containing the *live* chunks copied
    /// from the old one; this function flips every on-disk reference from
    /// the old location to the new in a single redb transaction:
    ///
    ///   1. Delete the old `BlockMeta` row.
    ///   2. Insert `new_block` (its `ref_count` must already equal the total
    ///      number of live chunk references — caller's responsibility).
    ///   3. Scan `FILES_TABLE`. For each `FileMeta` that has any chunk in
    ///      `old_block_id`, remap those chunks to the new `(block_id,
    ///      offset, compressed_size, original_size)` from `chunk_remap`
    ///      (keyed by content hash). Unaffected chunks are left alone.
    ///   4. Scan `DEDUP_TABLE`. Entries whose `block_id == old_block_id`
    ///      are either rewritten (if their hash is in `chunk_remap`) or
    ///      dropped (if they'd otherwise point at a chunk no longer on
    ///      disk — this also cleans up the stale-dedup class of bug).
    ///
    /// Atomicity guarantee: the caller sees the world before the call
    /// **or** the world after; never an in-between.
    pub fn rewrite_block_references(
        &self,
        old_block_id: &str,
        new_block: &BlockMeta,
        // v0.4.8: keyed by raw 32-byte hash (was hex String).
        chunk_remap: &std::collections::HashMap<
            crate::dedup::HashBytes,
            (String, u64, u64, u64),
        >,
    ) -> Result<u64> {
        // chunk_remap value = (new_block_id, new_offset, new_original_size, new_compressed_size)
        // Returns: the number of chunk references that were rewritten (i.e. the
        // ref_count that was actually persisted onto the new block).
        tracing::info!(
            old = %old_block_id,
            new = %new_block.block_id,
            remap_entries = chunk_remap.len(),
            "MetadataStore::rewrite_block_references"
        );
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;

        // 1) Rewrite affected FileMeta rows and count references in one pass.
        //
        // Counting inside the same write tx (instead of trusting a pre-scan
        // passed in by the caller) avoids a race window: a dedup-hit upload
        // that slipped in between a pre-scan and this commit would otherwise
        // yield a ref_count that under-counts the true references.
        let mut new_ref_count: u64 = 0;
        {
            let mut files = tx
                .open_table(FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let mut to_rewrite: Vec<(String, FileMeta)> = Vec::new();
            {
                for entry in files
                    .iter()
                    .map_err(|e| JiHuanError::Database(e.to_string()))?
                {
                    let (k, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
                    let file: FileMeta = decode(v.value())?;
                    // v0.4.8: pool-level membership check is O(pool_size)
                    // and skips resolving every chunk's index on the
                    // ~99 % of files that don't touch this block.
                    if file.block_ids.iter().any(|b| b == old_block_id) {
                        to_rewrite.push((k.value().to_string(), file));
                    }
                }
            }
            for (k, mut file) in to_rewrite {
                let file_chunk_size = file.chunk_size;
                // v0.4.8: locate the pool slot that held `old_block_id`
                // so we can (a) filter chunks pointing at it by
                // `block_idx` instead of allocating a resolved string
                // per chunk, and (b) replace the pool entry in-place
                // with `new_block.block_id`. Each `ChunkMeta.block_idx`
                // keeps its numeric value — the pool now resolves it
                // to the new block — so we don't touch every chunk's
                // bytes to remap the reference itself.
                let old_pool_idx = match file
                    .block_ids
                    .iter()
                    .position(|b| b == old_block_id)
                {
                    Some(i) => i as u16,
                    None => continue, // pool check above should have excluded this
                };
                for chunk in &mut file.chunks {
                    if chunk.block_idx == old_pool_idx {
                        // v0.4.8: chunks on a live block always have
                        // `hash = Some(_)` — dedup-disabled uploads
                        // never share a block with others anyway, but
                        // we guard here to avoid a silent data loss if
                        // that invariant regresses.
                        let hash_bytes = chunk.hash.as_ref().ok_or_else(|| {
                            JiHuanError::Internal(format!(
                                "rewrite_block_references: chunk in file {} points at block {} but has no hash",
                                file.file_id, old_block_id
                            ))
                        })?;
                        if let Some((_nb, no, norig, ncomp)) = chunk_remap.get(hash_bytes) {
                            chunk.offset = *no;
                            // v0.4.8: re-apply the default-elided encoding
                            // against the owning file's canonical size.
                            // `norig` is always the resolved byte length.
                            chunk.original_size = if *norig == file_chunk_size {
                                None
                            } else {
                                Some(*norig)
                            };
                            chunk.compressed_size = *ncomp;
                            new_ref_count += 1;
                        } else {
                            return Err(JiHuanError::Internal(format!(
                                "rewrite_block_references: hash {} not in remap for file {}",
                                crate::dedup::hash_to_hex(hash_bytes),
                                file.file_id
                            )));
                        }
                    }
                }
                // Pool swap: every chunk that previously pointed at
                // `old_block_id` now transparently points at
                // `new_block.block_id` via the same `block_idx`.
                file.block_ids[old_pool_idx as usize] = new_block.block_id.clone();
                let bytes = encode(&file)?;
                files
                    .insert(k.as_str(), bytes.as_slice())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
        }

        // 2) Block metadata flip. ref_count comes from the scan above.
        {
            let mut blocks = tx
                .open_table(BLOCKS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            blocks
                .remove(old_block_id)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let mut persisted = new_block.clone();
            persisted.ref_count = new_ref_count;
            let bytes = encode(&persisted)?;
            blocks
                .insert(persisted.block_id.as_str(), bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }

        // 4) Rewrite / drop affected DedupEntry rows.
        {
            let mut dedup = tx
                .open_table(DEDUP_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            // v0.4.8: keys are raw 32-byte hashes (not hex strings).
            let mut to_update: Vec<(crate::dedup::HashBytes, DedupEntry)> = Vec::new();
            let mut to_drop: Vec<crate::dedup::HashBytes> = Vec::new();
            {
                for entry in dedup
                    .iter()
                    .map_err(|e| JiHuanError::Database(e.to_string()))?
                {
                    let (k, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
                    let de: DedupEntry = decode(v.value())?;
                    if de.block_id == old_block_id {
                        // Key is `&[u8]`; convert to the fixed-size array.
                        let hash: crate::dedup::HashBytes =
                            <[u8; crate::dedup::HASH_BYTES]>::try_from(k.value())
                                .map_err(|_| {
                                    JiHuanError::DataCorruption(format!(
                                        "DEDUP_TABLE key length {} != {}",
                                        k.value().len(),
                                        crate::dedup::HASH_BYTES
                                    ))
                                })?;
                        if let Some((nb, no, norig, ncomp)) = chunk_remap.get(&hash) {
                            let mut updated = de;
                            updated.block_id = nb.clone();
                            updated.offset = *no;
                            updated.original_size = *norig;
                            updated.compressed_size = *ncomp;
                            to_update.push((hash, updated));
                        } else {
                            // Dedup entry points at a hash no longer on disk —
                            // drop it to prevent future reads from following
                            // a stale pointer.
                            to_drop.push(hash);
                        }
                    }
                }
            }
            for (hash, updated) in to_update {
                let bytes = encode(&updated)?;
                dedup
                    .insert(&hash[..], bytes.as_slice())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
            for hash in to_drop {
                dedup
                    .remove(&hash[..])
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
        }

        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(new_ref_count)
    }

    /// v0.4.5: cross-block merge variant of [`rewrite_block_references`].
    ///
    /// Same atomicity guarantees, but accepts a **set** of source block ids
    /// that all get collapsed into one `new_block`. Every chunk in the remap
    /// must have originated from one of `old_block_ids`. On commit:
    ///   * every `FileMeta.chunks[*].block_id` pointing at any of the old
    ///     ids is remapped to the single `new_block.block_id` (with the new
    ///     offset/sizes from `chunk_remap`);
    ///   * every `DedupEntry` whose `block_id` is in the old set is likewise
    ///     rewritten (or dropped if its hash isn't in the remap — same
    ///     stale-entry cleanup as the single-source variant);
    ///   * **all** old BlockMeta rows are deleted and the single new one
    ///     inserted with `ref_count` = total remapped references.
    ///
    /// Returns the ref_count that was persisted on the new block.
    pub fn rewrite_block_references_group(
        &self,
        old_block_ids: &[String],
        new_block: &BlockMeta,
        // v0.4.8: keyed by raw 32-byte hash (was hex String).
        chunk_remap: &std::collections::HashMap<
            crate::dedup::HashBytes,
            (String, u64, u64, u64),
        >,
    ) -> Result<u64> {
        // Build a set for fast membership. Empty input is a no-op (caller
        // bug, but we won't blow up; just commit an empty tx).
        use std::collections::HashSet;
        let old_set: HashSet<&str> = old_block_ids.iter().map(|s| s.as_str()).collect();
        tracing::info!(
            old_count = old_set.len(),
            new = %new_block.block_id,
            remap_entries = chunk_remap.len(),
            "MetadataStore::rewrite_block_references_group"
        );

        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;

        // 1) FileMeta: any chunk whose block_id is in old_set gets remapped.
        let mut new_ref_count: u64 = 0;
        {
            let mut files = tx
                .open_table(FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let mut to_rewrite: Vec<(String, FileMeta)> = Vec::new();
            {
                for entry in files
                    .iter()
                    .map_err(|e| JiHuanError::Database(e.to_string()))?
                {
                    let (k, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
                    let file: FileMeta = decode(v.value())?;
                    // v0.4.8: pool-level membership check avoids
                    // resolving every chunk's index when the file
                    // doesn't touch any block in the merge group.
                    if file
                        .block_ids
                        .iter()
                        .any(|b| old_set.contains(b.as_str()))
                    {
                        to_rewrite.push((k.value().to_string(), file));
                    }
                }
            }
            for (k, mut file) in to_rewrite {
                let file_chunk_size = file.chunk_size;
                // v0.4.8: collect the set of pool indices whose slot
                // holds one of the merged-away block ids. Every
                // `ChunkMeta.block_idx` in this set needs its offset/
                // size re-bound via `chunk_remap`; the pool slot gets
                // replaced with `new_block.block_id` in-place so the
                // numeric `block_idx` stays stable.
                let mut old_pool_idxs: std::collections::HashSet<u16> =
                    std::collections::HashSet::new();
                for (i, b) in file.block_ids.iter().enumerate() {
                    if old_set.contains(b.as_str()) {
                        old_pool_idxs.insert(i as u16);
                    }
                }
                for chunk in &mut file.chunks {
                    if old_pool_idxs.contains(&chunk.block_idx) {
                        let hash_bytes = chunk.hash.as_ref().ok_or_else(|| {
                            JiHuanError::Internal(format!(
                                "rewrite_block_references_group: chunk in file {} has no hash",
                                file.file_id
                            ))
                        })?;
                        if let Some((_nb, no, norig, ncomp)) = chunk_remap.get(hash_bytes) {
                            chunk.offset = *no;
                            // v0.4.8: re-apply the default-elided encoding
                            // against the owning file's canonical size.
                            chunk.original_size = if *norig == file_chunk_size {
                                None
                            } else {
                                Some(*norig)
                            };
                            chunk.compressed_size = *ncomp;
                            new_ref_count += 1;
                        } else {
                            return Err(JiHuanError::Internal(format!(
                                "rewrite_block_references_group: hash {} not in remap for file {}",
                                crate::dedup::hash_to_hex(hash_bytes),
                                file.file_id
                            )));
                        }
                    }
                }
                // Pool swap: every old slot now resolves to the single
                // merged destination block. Entries for old blocks
                // this file doesn't actually reference anymore (all of
                // them, after the swap) collapse to duplicate
                // strings, which is functionally harmless — they waste
                // ~36 B each until the file is rewritten again.
                for idx in &old_pool_idxs {
                    file.block_ids[*idx as usize] = new_block.block_id.clone();
                }
                let bytes = encode(&file)?;
                files
                    .insert(k.as_str(), bytes.as_slice())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
        }

        // 2) BlockMeta: remove all old rows, insert the single new one.
        {
            let mut blocks = tx
                .open_table(BLOCKS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            for old_id in old_block_ids {
                blocks
                    .remove(old_id.as_str())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
            let mut persisted = new_block.clone();
            persisted.ref_count = new_ref_count;
            let bytes = encode(&persisted)?;
            blocks
                .insert(persisted.block_id.as_str(), bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }

        // 3) DedupEntry: rewrite in-remap entries, drop stale ones.
        {
            let mut dedup = tx
                .open_table(DEDUP_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            // v0.4.8: keys are raw 32-byte hashes (not hex strings).
            let mut to_update: Vec<(crate::dedup::HashBytes, DedupEntry)> = Vec::new();
            let mut to_drop: Vec<crate::dedup::HashBytes> = Vec::new();
            {
                for entry in dedup
                    .iter()
                    .map_err(|e| JiHuanError::Database(e.to_string()))?
                {
                    let (k, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
                    let de: DedupEntry = decode(v.value())?;
                    if old_set.contains(de.block_id.as_str()) {
                        let hash: crate::dedup::HashBytes =
                            <[u8; crate::dedup::HASH_BYTES]>::try_from(k.value())
                                .map_err(|_| {
                                    JiHuanError::DataCorruption(format!(
                                        "DEDUP_TABLE key length {} != {}",
                                        k.value().len(),
                                        crate::dedup::HASH_BYTES
                                    ))
                                })?;
                        if let Some((nb, no, norig, ncomp)) = chunk_remap.get(&hash) {
                            let mut updated = de;
                            updated.block_id = nb.clone();
                            updated.offset = *no;
                            updated.original_size = *norig;
                            updated.compressed_size = *ncomp;
                            to_update.push((hash, updated));
                        } else {
                            to_drop.push(hash);
                        }
                    }
                }
            }
            for (hash, updated) in to_update {
                let bytes = encode(&updated)?;
                dedup
                    .insert(&hash[..], bytes.as_slice())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
            for hash in to_drop {
                dedup
                    .remove(&hash[..])
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
        }

        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(new_ref_count)
    }

    /// Remove every dedup entry whose `block_id` equals the given id.
    ///
    /// Called by GC after it reaps a ref_count == 0 block: otherwise the
    /// dedup index retains a pointer at disk space that's been freed and
    /// the next upload of an identical chunk would short-circuit into a
    /// non-existent location.
    pub fn purge_dedup_for_block(&self, block_id: &str) -> Result<u64> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let mut count: u64 = 0;
        {
            let mut dedup = tx
                .open_table(DEDUP_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            // v0.4.8: DEDUP_TABLE keys are raw bytes (was hex String).
            // We buffer them as owned `Vec<u8>` (not the fixed-size
            // array) so a corrupted row with the wrong key length still
            // gets purged along with the rest.
            let mut to_drop: Vec<Vec<u8>> = Vec::new();
            {
                for entry in dedup
                    .iter()
                    .map_err(|e| JiHuanError::Database(e.to_string()))?
                {
                    let (k, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
                    let de: DedupEntry = decode(v.value())?;
                    if de.block_id == block_id {
                        to_drop.push(k.value().to_vec());
                    }
                }
            }
            for hash in to_drop {
                dedup
                    .remove(hash.as_slice())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
                count += 1;
            }
        }
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(count)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Dedup index operations
    // ─────────────────────────────────────────────────────────────────────────

    /// v0.4.8: lookup key is raw bytes (32-byte hash) rather than hex
    /// string. Callers that have a `ChunkMeta.hash: Option<HashBytes>`
    /// should pass `hash.as_ref().map(|h| &h[..]).unwrap_or(&[])` — an
    /// empty slice short-circuits to `None` because no real row is
    /// keyed by zero bytes.
    pub fn get_dedup_entry(&self, hash: &[u8]) -> Result<Option<DedupEntry>> {
        if hash.is_empty() {
            return Ok(None);
        }
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(DEDUP_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        match table
            .get(hash)
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            Some(v) => Ok(Some(decode(v.value())?)),
            None => Ok(None),
        }
    }

    /// Enumerate every dedup entry (hash → block_id pairs only).
    ///
    /// Used by [`Engine::repair`](crate::Engine::repair) to find dedup
    /// entries that point at block files missing from disk. Kept minimal
    /// (no DedupEntry clone) so a large dedup table doesn't spike memory
    /// during a repair pass — we only need the hash + block_id to decide
    /// whether to delete.
    /// v0.4.8: hash pairs are surfaced as `(HashBytes, block_id)`
    /// tuples. Callers that need to log them should go through
    /// [`crate::dedup::hash_to_hex`] to format the bytes.
    pub fn list_dedup_hash_block_pairs(
        &self,
    ) -> Result<Vec<(crate::dedup::HashBytes, String)>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(DEDUP_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let mut out = Vec::new();
        for entry in table
            .iter()
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            let (k, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
            let de: DedupEntry = decode(v.value())?;
            let hash: crate::dedup::HashBytes =
                <[u8; crate::dedup::HASH_BYTES]>::try_from(k.value()).map_err(|_| {
                    JiHuanError::DataCorruption(format!(
                        "DEDUP_TABLE key length {} != {}",
                        k.value().len(),
                        crate::dedup::HASH_BYTES
                    ))
                })?;
            out.push((hash, de.block_id));
        }
        Ok(out)
    }

    /// v0.4.8: key is raw bytes (32-byte hash) rather than hex string.
    pub fn remove_dedup_entry(&self, hash: &[u8]) -> Result<()> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        {
            let mut table = tx
                .open_table(DEDUP_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            table
                .remove(hash)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────────
    // API Key operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Insert a new API key record.
    pub fn insert_api_key(&self, key: &ApiKeyMeta) -> Result<()> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        {
            let bytes = encode(key)?;
            let mut table = tx
                .open_table(APIKEYS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            table
                .insert(key.key_id.as_str(), bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let mut hash_table = tx
                .open_table(APIKEY_HASH_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            hash_table
                .insert(key.key_hash.as_str(), key.key_id.as_str())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(())
    }

    /// Look up an API key by its SHA-256 hash. Updates `last_used_at` on success.
    pub fn get_api_key_by_hash(&self, key_hash: &str) -> Result<Option<ApiKeyMeta>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let hash_table = tx
            .open_table(APIKEY_HASH_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let key_id = match hash_table
            .get(key_hash)
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            Some(v) => v.value().to_string(),
            None => return Ok(None),
        };
        let table = tx
            .open_table(APIKEYS_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        match table
            .get(key_id.as_str())
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            Some(v) => Ok(Some(decode(v.value())?)),
            None => Ok(None),
        }
    }

    /// Get an API key by its ID.
    pub fn get_api_key(&self, key_id: &str) -> Result<Option<ApiKeyMeta>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(APIKEYS_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        match table
            .get(key_id)
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            Some(v) => Ok(Some(decode(v.value())?)),
            None => Ok(None),
        }
    }

    /// List all API keys.
    pub fn list_api_keys(&self) -> Result<Vec<ApiKeyMeta>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(APIKEYS_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let mut keys = Vec::new();
        for entry in table.iter().map_err(|e| JiHuanError::Database(e.to_string()))? {
            let (_, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
            keys.push(decode(v.value())?);
        }
        Ok(keys)
    }

    /// Update the `last_used_at` timestamp for an API key.
    pub fn touch_api_key(&self, key_id: &str, now: u64) -> Result<()> {
        // Read phase: extract current value without holding a write transaction
        let updated_bytes: Option<Vec<u8>> = {
            let rtx = self
                .db
                .begin_read()
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let table = rtx
                .open_table(APIKEYS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            match table
                .get(key_id)
                .map_err(|e| JiHuanError::Database(e.to_string()))?
            {
                Some(r) => {
                    let mut meta: ApiKeyMeta = decode(r.value())?;
                    meta.last_used_at = now;
                    Some(encode(&meta)?)
                }
                None => None,
            }
        };
        // Write phase: only open write transaction after read transaction is dropped
        if let Some(bytes) = updated_bytes {
            let tx = self
                .db
                .begin_write()
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            {
                let mut table = tx
                    .open_table(APIKEYS_TABLE)
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
                table
                    .insert(key_id, bytes.as_slice())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
            tx.commit()
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        Ok(())
    }

    /// Delete (revoke) an API key by ID. Returns the removed key if found.
    pub fn delete_api_key(&self, key_id: &str) -> Result<Option<ApiKeyMeta>> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        // Phase 1: remove from apikeys table, extract owned bytes before dropping AccessGuard
        let removed_raw: Option<Vec<u8>> = {
            let mut table = tx
                .open_table(APIKEYS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let result = table
                .remove(key_id)
                .map_err(|e| JiHuanError::Database(e.to_string()));
            // Eagerly copy bytes to owned Vec so AccessGuard (and table borrow) is released
            match result {
                Ok(Some(guard)) => Some(guard.value().to_vec()),
                Ok(None) => None,
                Err(e) => return Err(e),
            }
        };
        let removed_meta: Option<ApiKeyMeta> = match removed_raw {
            Some(ref b) => Some(decode(b)?),
            None => None,
        };
        // Phase 2: remove from hash index (table borrow dropped above)
        if let Some(ref meta) = removed_meta {
            let mut hash_table = tx
                .open_table(APIKEY_HASH_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            hash_table
                .remove(meta.key_hash.as_str())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        let removed = removed_meta;
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(removed)
    }

    /// Rotate an API key's credential: overwrite `key_hash` + `key_prefix`
    /// in place, leaving `key_id`, `name`, `scopes`, `created_at`, and
    /// `enabled` unchanged. The old hash entry is removed from the secondary
    /// index so the old plaintext can no longer authenticate.
    ///
    /// Returns `Ok(true)` when the row existed and was updated, `Ok(false)`
    /// when no key matched `key_id`.
    pub fn update_api_key_hash(
        &self,
        key_id: &str,
        new_hash: &str,
        new_prefix: &str,
    ) -> Result<bool> {
        // Read-modify phase: fetch the existing meta and compute the new
        // encoded payload without holding a write transaction.
        let (updated_bytes, old_hash) = {
            let rtx = self
                .db
                .begin_read()
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let table = rtx
                .open_table(APIKEYS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            match table
                .get(key_id)
                .map_err(|e| JiHuanError::Database(e.to_string()))?
            {
                Some(r) => {
                    let mut meta: ApiKeyMeta = decode(r.value())?;
                    let old = meta.key_hash.clone();
                    meta.key_hash = new_hash.to_string();
                    meta.key_prefix = new_prefix.to_string();
                    (Some(encode(&meta)?), old)
                }
                None => return Ok(false),
            }
        };

        let bytes = match updated_bytes {
            Some(b) => b,
            None => return Ok(false),
        };

        // Write phase: atomically rewrite both tables so an interrupted update
        // cannot leave a stale hash → key_id mapping pointing at the wrong row.
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        {
            let mut table = tx
                .open_table(APIKEYS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            table
                .insert(key_id, bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        {
            let mut hash_table = tx
                .open_table(APIKEY_HASH_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            if !old_hash.is_empty() {
                hash_table
                    .remove(old_hash.as_str())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?;
            }
            hash_table
                .insert(new_hash, key_id)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(true)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Audit log (Phase 2.6)
    // ─────────────────────────────────────────────────────────────────────────

    /// Append a single audit event. Key is `[ts_secs:u64_be][seq:u64_be]`.
    /// Returns the composed key bytes (useful for correlation / tests).
    pub fn insert_audit_event(&self, event: &AuditEvent) -> Result<[u8; 16]> {
        let seq = self
            .audit_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut key = [0u8; 16];
        key[0..8].copy_from_slice(&event.ts.to_be_bytes());
        key[8..16].copy_from_slice(&seq.to_be_bytes());
        let bytes = encode(event)?;
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        {
            let mut table = tx
                .open_table(AUDIT_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            table
                .insert(key.as_slice(), bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(key)
    }

    /// Return the most recent audit events (newest first), optionally
    /// filtered by actor/action and bounded by timestamp range. `limit`
    /// caps the returned set. All filters are applied in-memory after the
    /// range scan — acceptable because the expected volume is moderate
    /// (<10⁵ events/day). Heavy-traffic deployments can add secondary
    /// indices later without touching callers.
    pub fn list_audit_events(
        &self,
        since_secs: Option<u64>,
        until_secs: Option<u64>,
        actor_key_id: Option<&str>,
        action_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AuditEvent>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(AUDIT_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;

        // Build a range `[lo, hi)` over the 16-byte composite key. The seq
        // component is zeroed for `lo` and max for `hi` so the filter is
        // strictly by the timestamp half.
        let lo = {
            let mut k = [0u8; 16];
            k[0..8].copy_from_slice(&since_secs.unwrap_or(0).to_be_bytes());
            k
        };
        let hi = {
            let mut k = [0xFFu8; 16];
            k[0..8].copy_from_slice(&until_secs.unwrap_or(u64::MAX).to_be_bytes());
            k
        };

        let mut out = Vec::new();
        let range = table
            .range::<&[u8]>(lo.as_slice()..=hi.as_slice())
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        // Iterate newest-first by reversing the range iterator.
        for entry in range.rev() {
            let (_, v) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
            let ev: AuditEvent = decode(v.value())?;
            if let Some(a) = actor_key_id {
                if ev.actor_key_id.as_deref() != Some(a) {
                    continue;
                }
            }
            if let Some(prefix) = action_prefix {
                if !ev.action.starts_with(prefix) {
                    continue;
                }
            }
            out.push(ev);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Remove audit events strictly older than `cutoff_secs`. Used by the
    /// retention policy (default 90 days). Returns the number of rows
    /// deleted.
    pub fn purge_audit_events_before(&self, cutoff_secs: u64) -> Result<u64> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let mut deleted = 0u64;
        {
            let mut table = tx
                .open_table(AUDIT_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let mut hi = [0u8; 16];
            hi[0..8].copy_from_slice(&cutoff_secs.to_be_bytes());
            // Collect keys first (can't mutate while iterating redb ranges).
            let mut keys: Vec<[u8; 16]> = Vec::new();
            for entry in table
                .range::<&[u8]>(&[][..]..hi.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?
            {
                let (k, _) = entry.map_err(|e| JiHuanError::Database(e.to_string()))?;
                let raw = k.value();
                if raw.len() == 16 {
                    let mut arr = [0u8; 16];
                    arr.copy_from_slice(raw);
                    keys.push(arr);
                }
            }
            for k in keys {
                if table
                    .remove(k.as_slice())
                    .map_err(|e| JiHuanError::Database(e.to_string()))?
                    .is_some()
                {
                    deleted += 1;
                }
            }
        }
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(deleted)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Stats
    // ─────────────────────────────────────────────────────────────────────────

    pub fn file_count(&self) -> Result<u64> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(FILES_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        table
            .len()
            .map_err(|e| JiHuanError::Database(e.to_string()))
    }

    pub fn block_count(&self) -> Result<u64> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(BLOCKS_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        table
            .len()
            .map_err(|e| JiHuanError::Database(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::types::{ChunkMeta, FileMeta};
    use tempfile::tempdir;

    fn make_store() -> (MetadataStore, tempfile::TempDir) {
        let tmp = tempdir().unwrap();
        let store = MetadataStore::open(tmp.path().join("meta.db")).unwrap();
        (store, tmp)
    }

    fn make_file(id: &str, partition_id: u64) -> FileMeta {
        FileMeta {
            file_id: id.to_string(),
            file_name: format!("{}.txt", id),
            file_size: 1024,
            create_time: 1000000,
            partition_id,
            // v0.4.8: tests use an artificial file_chunk_size of 1024 so
            // the single chunk below can sit at the default-elided size
            // (`original_size = None`) and still round-trip correctly.
            chunk_size: 1024,
            // v0.4.8: block_id pool — the single chunk points at slot 0
            // which resolves to "blk1".
            block_ids: vec!["blk1".to_string()],
            chunks: vec![ChunkMeta {
                block_idx: 0,
                offset: 0,
                original_size: None,
                compressed_size: 512,
                // v0.4.8: hash is now a fixed-size byte array. The
                // value here is a deterministic sentinel for tests.
                hash: Some({
                    let mut h = [0u8; crate::dedup::HASH_BYTES];
                    h[..6].copy_from_slice(b"abc123");
                    h
                }),
            }],
            content_type: None,
        }
    }

    /// Test-only helper: insert a file via `commit_file_batch` with empty
    /// deltas and dedups. Mirrors the semantics of the removed
    /// `insert_file` method (duplicate → `AlreadyExists`).
    fn insert_file_for_test(store: &MetadataStore, file: &FileMeta) -> Result<()> {
        store.commit_file_batch(file, &std::collections::HashMap::new(), &[])
    }

    #[test]
    fn test_insert_and_get_file() {
        let (store, _tmp) = make_store();
        let file = make_file("file1", 0);
        insert_file_for_test(&store, &file).unwrap();
        let got = store.get_file("file1").unwrap();
        assert_eq!(got, Some(file));
    }

    #[test]
    fn test_insert_duplicate_file_errors() {
        let (store, _tmp) = make_store();
        let file = make_file("dup", 0);
        insert_file_for_test(&store, &file).unwrap();
        assert!(insert_file_for_test(&store, &file).is_err());
    }

    #[test]
    fn test_delete_file() {
        let (store, _tmp) = make_store();
        insert_file_for_test(&store, &make_file("f1", 0)).unwrap();
        let deleted = store.delete_file("f1").unwrap();
        assert!(deleted.is_some());
        assert_eq!(store.get_file("f1").unwrap(), None);
    }

    #[test]
    fn test_partition_file_listing() {
        let (store, _tmp) = make_store();
        insert_file_for_test(&store, &make_file("f1", 5)).unwrap();
        insert_file_for_test(&store, &make_file("f2", 5)).unwrap();
        insert_file_for_test(&store, &make_file("f3", 6)).unwrap();

        let p5 = store.list_files_in_partition(5).unwrap();
        assert_eq!(p5.len(), 2);
        let p6 = store.list_files_in_partition(6).unwrap();
        assert_eq!(p6.len(), 1);
        assert_eq!(store.count_files_in_partition(5).unwrap(), 2);
        assert_eq!(store.count_files_in_partition(6).unwrap(), 1);
        assert_eq!(store.count_files_in_partition(999).unwrap(), 0);
    }

    /// v0.4.6 regression: the multimap partition_files schema must hold
    /// up under thousands of entries without per-insert amplification
    /// (pre-v0.4.6 this would rewrite a multi-MB bincode Vec on every
    /// insert). We don't assert timing here — just that CRUD stays
    /// correct at 1k entries in a single partition.
    #[test]
    fn test_partition_large_multimap() {
        let (store, _tmp) = make_store();
        const N: usize = 1_000;
        for i in 0..N {
            insert_file_for_test(&store, &make_file(&format!("big_{i}"), 42)).unwrap();
        }
        assert_eq!(store.count_files_in_partition(42).unwrap(), N as u64);
        let ids = store.list_files_in_partition(42).unwrap();
        assert_eq!(ids.len(), N);

        // Remove half, confirm count tracks.
        for i in 0..N / 2 {
            store.delete_file(&format!("big_{i}")).unwrap();
        }
        assert_eq!(store.count_files_in_partition(42).unwrap(), (N / 2) as u64);
    }

    #[test]
    fn test_delete_partition() {
        let (store, _tmp) = make_store();
        insert_file_for_test(&store, &make_file("f1", 3)).unwrap();
        insert_file_for_test(&store, &make_file("f2", 3)).unwrap();
        let deleted = store.delete_partition(3).unwrap();
        assert_eq!(deleted.len(), 2);
        assert_eq!(store.list_files_in_partition(3).unwrap().len(), 0);
    }

    #[test]
    fn test_block_ref_count() {
        let (store, _tmp) = make_store();
        let block = BlockMeta::new("blk1", "/data/blk1.blk", 1024 * 1024, 1000);
        store.insert_block(&block).unwrap();

        let rc = store.update_block_ref_count("blk1", 3).unwrap();
        assert_eq!(rc, 3);
        let rc = store.update_block_ref_count("blk1", -1).unwrap();
        assert_eq!(rc, 2);
        let rc = store.update_block_ref_count("blk1", -10).unwrap(); // clamps to 0
        assert_eq!(rc, 0);
    }

    #[test]
    fn test_list_unreferenced_blocks() {
        let (store, _tmp) = make_store();
        store
            .insert_block(&BlockMeta::new("b1", "/b1.blk", 100, 0))
            .unwrap();
        store
            .insert_block(&BlockMeta::new("b2", "/b2.blk", 100, 0))
            .unwrap();
        store.update_block_ref_count("b1", 1).unwrap();

        let unreferenced = store.list_unreferenced_blocks().unwrap();
        assert_eq!(unreferenced.len(), 1);
        assert_eq!(unreferenced[0].block_id, "b2");
    }

    #[test]
    fn test_dedup_entry_crud() {
        let (store, _tmp) = make_store();
        // v0.4.8: DedupEntry.hash is now `[u8; 32]`. We build a
        // deterministic sentinel from an ASCII seed so the assertions
        // stay readable.
        let mut hash = [0u8; crate::dedup::HASH_BYTES];
        hash[..9].copy_from_slice(b"sha256abc");
        let entry = DedupEntry {
            hash,
            block_id: "blk1".to_string(),
            offset: 64,
            original_size: 4096,
            compressed_size: 2000,
        };
        // Insert via commit_file_batch (the production path; supersedes the
        // removed standalone `insert_dedup_entry`).
        let dummy_file = make_file("_dedup_test_anchor", 0);
        store
            .commit_file_batch(&dummy_file, &std::collections::HashMap::new(), std::slice::from_ref(&entry))
            .unwrap();
        let got = store.get_dedup_entry(&hash[..]).unwrap();
        assert!(got.is_some());
        store.remove_dedup_entry(&hash[..]).unwrap();
        assert!(store.get_dedup_entry(&hash[..]).unwrap().is_none());
    }

    // ─── Phase 2.6 audit log ─────────────────────────────────────────────────

    fn mk_audit(ts: u64, action: &str, actor: Option<&str>) -> AuditEvent {
        AuditEvent {
            ts,
            actor_key_id: actor.map(String::from),
            actor_ip: None,
            action: action.to_string(),
            target: None,
            result: crate::metadata::types::AuditResult::Ok,
            http_status: Some(200),
        }
    }

    #[test]
    fn test_audit_insert_and_list_newest_first() {
        let (store, _tmp) = make_store();
        store.insert_audit_event(&mk_audit(100, "auth.login", Some("k1"))).unwrap();
        store.insert_audit_event(&mk_audit(200, "auth.logout", Some("k1"))).unwrap();
        store.insert_audit_event(&mk_audit(150, "key.create", Some("k2"))).unwrap();

        let events = store.list_audit_events(None, None, None, None, 10).unwrap();
        assert_eq!(events.len(), 3);
        // Newest first
        assert_eq!(events[0].ts, 200);
        assert_eq!(events[2].ts, 100);
    }

    #[test]
    fn test_audit_filters() {
        let (store, _tmp) = make_store();
        store.insert_audit_event(&mk_audit(100, "auth.login", Some("k1"))).unwrap();
        store.insert_audit_event(&mk_audit(200, "auth.logout", Some("k1"))).unwrap();
        store.insert_audit_event(&mk_audit(150, "key.create", Some("k2"))).unwrap();

        let only_k1 = store.list_audit_events(None, None, Some("k1"), None, 10).unwrap();
        assert_eq!(only_k1.len(), 2);
        assert!(only_k1.iter().all(|e| e.actor_key_id.as_deref() == Some("k1")));

        let auth_prefix = store.list_audit_events(None, None, None, Some("auth."), 10).unwrap();
        assert_eq!(auth_prefix.len(), 2);
        assert!(auth_prefix.iter().all(|e| e.action.starts_with("auth.")));

        let bounded = store.list_audit_events(Some(120), Some(180), None, None, 10).unwrap();
        assert_eq!(bounded.len(), 1);
        assert_eq!(bounded[0].ts, 150);
    }

    #[test]
    fn test_audit_purge_before() {
        let (store, _tmp) = make_store();
        for ts in [10u64, 20, 30, 40] {
            store.insert_audit_event(&mk_audit(ts, "x", None)).unwrap();
        }
        let deleted = store.purge_audit_events_before(25).unwrap();
        assert_eq!(deleted, 2);
        let remaining = store.list_audit_events(None, None, None, None, 10).unwrap();
        assert_eq!(remaining.len(), 2);
        assert!(remaining.iter().all(|e| e.ts >= 25));
    }

    #[test]
    fn test_audit_seq_disambiguates_same_second() {
        // Two events with identical ts must still land and both be
        // retrievable — the sequence counter prevents key collisions.
        let (store, _tmp) = make_store();
        let k1 = store.insert_audit_event(&mk_audit(500, "a", None)).unwrap();
        let k2 = store.insert_audit_event(&mk_audit(500, "b", None)).unwrap();
        assert_ne!(k1, k2);
        let all = store.list_audit_events(None, None, None, None, 10).unwrap();
        assert_eq!(all.len(), 2);
    }
}
