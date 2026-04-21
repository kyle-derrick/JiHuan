use serde::{Deserialize, Serialize};

/// Metadata for a logical file stored in JiHuan
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileMeta {
    /// Unique file identifier (UUID simple string)
    pub file_id: String,
    /// Original file name (user-provided)
    pub file_name: String,
    /// Total original file size in bytes
    pub file_size: u64,
    /// Unix timestamp (seconds) when file was stored
    pub create_time: u64,
    /// Time partition ID this file belongs to
    pub partition_id: u64,
    /// Ordered list of chunk references
    pub chunks: Vec<ChunkMeta>,
    /// MIME type hint (optional)
    pub content_type: Option<String>,
}

/// Reference to a stored chunk within a block file
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkMeta {
    /// ID of the block file containing this chunk
    pub block_id: String,
    /// Byte offset of this chunk within the block file
    pub offset: u64,
    /// Original (uncompressed) size
    pub original_size: u64,
    /// Compressed size on disk
    pub compressed_size: u64,
    /// Content hash (used as dedup key)
    pub hash: String,
    /// Index of this chunk within the file (0-based)
    pub index: u32,
}

/// Metadata for a block file
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockMeta {
    /// Unique block identifier
    pub block_id: String,
    /// Number of logical chunk references pointing into this block
    pub ref_count: u64,
    /// Unix timestamp when this block was created
    pub create_time: u64,
    /// Filesystem path to the block file
    pub path: String,
    /// Total size of the block file in bytes
    pub size: u64,
}

impl BlockMeta {
    pub fn new(block_id: &str, path: &str, size: u64, create_time: u64) -> Self {
        Self {
            block_id: block_id.to_string(),
            ref_count: 0,
            create_time,
            path: path.to_string(),
            size,
        }
    }
}

/// A time partition groups all files created within a time window
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMeta {
    /// Partition ID (= floor(create_time / partition_window_secs))
    pub partition_id: u64,
    /// Unix timestamp of the start of this partition window
    pub start_time: u64,
    /// Unix timestamp of the end of this partition window
    pub end_time: u64,
    /// Number of files in this partition
    pub file_count: u64,
}

/// Metadata for an API key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyMeta {
    /// Unique identifier (UUID simple string)
    pub key_id: String,
    /// Human-readable label
    pub name: String,
    /// SHA-256 hash of the raw key (stored; never store the raw key)
    pub key_hash: String,
    /// Key prefix for display (first 8 chars of raw key + "...")
    pub key_prefix: String,
    /// Unix timestamp when this key was created
    pub created_at: u64,
    /// Unix timestamp of the last successful use (0 = never used)
    pub last_used_at: u64,
    /// Whether this key is currently active
    pub enabled: bool,
    /// Granted permission scopes. Known values: "read" / "write" / "admin".
    /// Missing in records from older builds — default to full access for
    /// backward compatibility via [`default_legacy_scopes`].
    #[serde(default = "default_legacy_scopes")]
    pub scopes: Vec<String>,
}

/// Legacy-compat default for [`ApiKeyMeta::scopes`]: pre-scopes records had
/// no restriction, so we reconstitute them as full-access keys.
fn default_legacy_scopes() -> Vec<String> {
    vec!["read".to_string(), "write".to_string(), "admin".to_string()]
}

/// A dedup index entry: maps a content hash → block location
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupEntry {
    pub hash: String,
    pub block_id: String,
    pub offset: u64,
    pub original_size: u64,
    pub compressed_size: u64,
}
