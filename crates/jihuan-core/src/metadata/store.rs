use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};

use crate::error::{JiHuanError, Result};
use crate::metadata::types::{ApiKeyMeta, BlockMeta, DedupEntry, FileMeta};

// Table definitions: key type → value type (both &[u8] for bincode-encoded blobs)
const FILES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("files");
const BLOCKS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("blocks");
const DEDUP_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("dedup");
const PARTITIONS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("partitions");
/// Maps partition_id → list of file_ids (stored as bincode Vec<String>)
const PARTITION_FILES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("partition_files");
/// Maps key_id → ApiKeyMeta (JSON-encoded)
const APIKEYS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("apikeys");
/// Maps key_hash → key_id (for fast lookup by raw key hash)
const APIKEY_HASH_TABLE: TableDefinition<&str, &str> = TableDefinition::new("apikey_hash");

fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(v).map_err(|e| JiHuanError::Serialization(e.to_string()))
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| JiHuanError::Serialization(e.to_string()))
}

/// Thread-safe metadata store backed by redb
pub struct MetadataStore {
    db: Arc<Database>,
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
            tx.open_table(PARTITION_FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            tx.open_table(APIKEYS_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            tx.open_table(APIKEY_HASH_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            tx.commit()
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }

        Ok(Self { db: Arc::new(db) })
    }

    // ─────────────────────────────────────────────────────────────────────────
    // File operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Insert a new file record. Errors if the file_id already exists.
    pub fn insert_file(&self, file: &FileMeta) -> Result<()> {
        tracing::debug!(file_id = %file.file_id, "MetadataStore::insert_file");
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        {
            let mut table = tx
                .open_table(FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;

            if table
                .get(file.file_id.as_str())
                .map_err(|e| JiHuanError::Database(e.to_string()))?
                .is_some()
            {
                return Err(JiHuanError::AlreadyExists(format!(
                    "File '{}' already exists",
                    file.file_id
                )));
            }

            let bytes = encode(file)?;
            table
                .insert(file.file_id.as_str(), bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        {
            // Update partition → file list
            let mut pt = tx
                .open_table(PARTITION_FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let pid = file.partition_id.to_string();
            let mut ids: Vec<String> = pt
                .get(pid.as_str())
                .map_err(|e| JiHuanError::Database(e.to_string()))?
                .map(|v| decode::<Vec<String>>(v.value()).unwrap_or_default())
                .unwrap_or_default();
            ids.push(file.file_id.clone());
            let bytes = encode(&ids)?;
            pt.insert(pid.as_str(), bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
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
            let mut pt = tx
                .open_table(PARTITION_FILES_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let pid = file.partition_id.to_string();
            let mut ids: Vec<String> = pt
                .get(pid.as_str())
                .map_err(|e| JiHuanError::Database(e.to_string()))?
                .map(|v| decode::<Vec<String>>(v.value()).unwrap_or_default())
                .unwrap_or_default();
            ids.retain(|id| id != file_id);
            let bytes = encode(&ids)?;
            pt.insert(pid.as_str(), bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }

        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(file_opt)
    }

    /// List all files (full scan, for admin/UI use)
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

    /// List all file IDs in a partition
    pub fn list_files_in_partition(&self, partition_id: u64) -> Result<Vec<String>> {
        let tx = self
            .db
            .begin_read()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let table = tx
            .open_table(PARTITION_FILES_TABLE)
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        let pid = partition_id.to_string();
        match table
            .get(pid.as_str())
            .map_err(|e| JiHuanError::Database(e.to_string()))?
        {
            Some(v) => decode(v.value()),
            None => Ok(vec![]),
        }
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

    // ─────────────────────────────────────────────────────────────────────────
    // Dedup index operations
    // ─────────────────────────────────────────────────────────────────────────

    pub fn get_dedup_entry(&self, hash: &str) -> Result<Option<DedupEntry>> {
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

    pub fn insert_dedup_entry(&self, entry: &DedupEntry) -> Result<()> {
        let tx = self
            .db
            .begin_write()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        {
            let mut table = tx
                .open_table(DEDUP_TABLE)
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
            let bytes = encode(entry)?;
            table
                .insert(entry.hash.as_str(), bytes.as_slice())
                .map_err(|e| JiHuanError::Database(e.to_string()))?;
        }
        tx.commit()
            .map_err(|e| JiHuanError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn remove_dedup_entry(&self, hash: &str) -> Result<()> {
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
            chunks: vec![ChunkMeta {
                block_id: "blk1".to_string(),
                offset: 0,
                original_size: 1024,
                compressed_size: 512,
                hash: "abc123".to_string(),
                index: 0,
            }],
            content_type: None,
        }
    }

    #[test]
    fn test_insert_and_get_file() {
        let (store, _tmp) = make_store();
        let file = make_file("file1", 0);
        store.insert_file(&file).unwrap();
        let got = store.get_file("file1").unwrap();
        assert_eq!(got, Some(file));
    }

    #[test]
    fn test_insert_duplicate_file_errors() {
        let (store, _tmp) = make_store();
        let file = make_file("dup", 0);
        store.insert_file(&file).unwrap();
        assert!(store.insert_file(&file).is_err());
    }

    #[test]
    fn test_delete_file() {
        let (store, _tmp) = make_store();
        store.insert_file(&make_file("f1", 0)).unwrap();
        let deleted = store.delete_file("f1").unwrap();
        assert!(deleted.is_some());
        assert_eq!(store.get_file("f1").unwrap(), None);
    }

    #[test]
    fn test_partition_file_listing() {
        let (store, _tmp) = make_store();
        store.insert_file(&make_file("f1", 5)).unwrap();
        store.insert_file(&make_file("f2", 5)).unwrap();
        store.insert_file(&make_file("f3", 6)).unwrap();

        let p5 = store.list_files_in_partition(5).unwrap();
        assert_eq!(p5.len(), 2);
        let p6 = store.list_files_in_partition(6).unwrap();
        assert_eq!(p6.len(), 1);
    }

    #[test]
    fn test_delete_partition() {
        let (store, _tmp) = make_store();
        store.insert_file(&make_file("f1", 3)).unwrap();
        store.insert_file(&make_file("f2", 3)).unwrap();
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
        let entry = DedupEntry {
            hash: "sha256abc".to_string(),
            block_id: "blk1".to_string(),
            offset: 64,
            original_size: 4096,
            compressed_size: 2000,
        };
        store.insert_dedup_entry(&entry).unwrap();
        let got = store.get_dedup_entry("sha256abc").unwrap();
        assert!(got.is_some());
        store.remove_dedup_entry("sha256abc").unwrap();
        assert!(store.get_dedup_entry("sha256abc").unwrap().is_none());
    }
}
