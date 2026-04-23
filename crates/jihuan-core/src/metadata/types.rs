use serde::{Deserialize, Serialize};

use crate::dedup::HashBytes;

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
    /// v0.4.8: canonical chunk size used when this file was uploaded.
    /// The first N-1 chunks of a typical file are exactly this size; only
    /// the tail chunk is usually smaller. We store it once at the file
    /// level so `ChunkMeta.original_size` can be `None` (implied = this
    /// value) for every non-tail chunk, saving ~5 bytes × (N-1) on the
    /// wire for large files.
    pub chunk_size: u64,
    /// v0.4.8: deduplicated pool of block IDs referenced by this file's
    /// chunks. Each `ChunkMeta.block_idx` is an index into this vector.
    ///
    /// A typical upload puts every chunk into one or two blocks, so this
    /// pool holds at most a handful of 36-byte UUIDs and every chunk
    /// pays 2 bytes (`u16`) instead of ~37 bytes for an inlined string.
    /// On a 100-chunk, single-block file that saves ~3.5 KB of metadata.
    pub block_ids: Vec<String>,
    /// Ordered list of chunk references
    pub chunks: Vec<ChunkMeta>,
    /// MIME type hint (optional)
    pub content_type: Option<String>,
}

/// Reference to a stored chunk within a block file.
///
/// v0.4.8: removed the explicit `index: u32` field. A chunk's position
/// in the owning `FileMeta.chunks` vector is its index by definition
/// — carrying a redundant integer inside every chunk cost 4 B of
/// metadata per chunk and opened the door to inconsistencies (two
/// chunks with the same `index`, or gaps in the sequence) that the
/// engine code had no way to notice. Callers who need the ordinal
/// position use `.iter().enumerate()` or index the slice directly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkMeta {
    /// v0.4.8: index into `FileMeta.block_ids` identifying which block
    /// file holds this chunk. Replaces the previous inlined
    /// `block_id: String` (36-char UUID), paid exactly 2 bytes on the
    /// wire regardless of file size or dedup pattern.
    ///
    /// Callers that need the actual block id string use
    /// [`ChunkMeta::block_id`] with the owning `FileMeta` in scope.
    pub block_idx: u16,
    /// Byte offset of this chunk within the block file
    pub offset: u64,
    /// v0.4.8: elided-default original (uncompressed) size.
    ///
    /// `None` means "this chunk's size equals the owning file's
    /// `FileMeta.chunk_size`" — the overwhelmingly common case for
    /// every non-tail chunk. `Some(n)` stores the explicit size
    /// (used for the tail, and for cross-file dedup hits where the
    /// originating file had a different `chunk_size`).
    ///
    /// Callers should resolve the effective size via
    /// `ChunkMeta::effective_original_size(file_chunk_size)` rather
    /// than reading this field directly.
    pub original_size: Option<u64>,
    /// Compressed size on disk
    pub compressed_size: u64,
    /// Content hash (used as dedup key).
    ///
    /// v0.4.8: 32 raw bytes (was 64-char hex string). `None` means the
    /// owning file was uploaded with dedup disabled
    /// (`HashAlgorithm::None`) — such chunks are never entered into
    /// `DEDUP_TABLE` and never match a future dedup lookup. For
    /// MD5/SHA-1 algorithms the digest is left-aligned and the trailing
    /// bytes are zero-padded; SHA-256 fills all 32 bytes.
    pub hash: Option<HashBytes>,
}

impl ChunkMeta {
    /// Resolve the chunk's original (uncompressed) size, falling back
    /// to the owning file's canonical `chunk_size` when
    /// [`ChunkMeta::original_size`] is `None` (the default-elided case
    /// for non-tail chunks). Callers that have the file context in
    /// scope should always prefer this accessor over touching the
    /// field directly.
    #[inline]
    pub fn effective_original_size(&self, file_chunk_size: u64) -> u64 {
        self.original_size.unwrap_or(file_chunk_size)
    }

    /// Resolve `block_idx` back to a block id string by looking it up
    /// in the owning file's `block_ids` pool (v0.4.8). Panics on
    /// out-of-range indices, which would indicate a corrupted row —
    /// production callers that want graceful degradation should use
    /// [`ChunkMeta::try_block_id`] instead.
    #[inline]
    pub fn block_id<'a>(&self, file: &'a FileMeta) -> &'a str {
        &file.block_ids[self.block_idx as usize]
    }

    /// Fallible variant of [`ChunkMeta::block_id`] — returns `None`
    /// when the index is out of range (indicates metadata corruption).
    #[inline]
    pub fn try_block_id<'a>(&self, file: &'a FileMeta) -> Option<&'a str> {
        file.block_ids
            .get(self.block_idx as usize)
            .map(String::as_str)
    }
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

/// Metadata for an API key (v0.5.0-iam: aka ServiceAccount).
///
/// Type name kept as `ApiKeyMeta` for backward compatibility with existing
/// callers and table definitions; new fields (`parent_user`,
/// `allowed_partitions`, `expires_at`) give the record the semantics of
/// a MinIO-style ServiceAccount attached to a `User`. Legacy records
/// persisted before v0.5.0 lack these fields — serde `default` keeps them
/// readable (parent_user = "", no partition restriction, no expiry).
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
    /// Always populated by `AuthEngine::create_key` — never empty in records
    /// written by this codebase.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// v0.5.0-iam: parent User that owns this service account. Empty string
    /// means "no owner" — only used by legacy pre-IAM keys and by the
    /// session-cookie virtual SA that login mints for a User.
    #[serde(default)]
    pub parent_user: String,
    /// v0.5.0-iam: optional partition allow-list. `None` = full access
    /// (default for all pre-IAM keys and for admin SAs). `Some(vec![])` =
    /// explicitly no partitions (effectively denies upload/download).
    #[serde(default)]
    pub allowed_partitions: Option<Vec<String>>,
    /// v0.5.0-iam: unix timestamp at which the SA auto-disables. `None` =
    /// never expires. Checked on every auth; expired SAs return 401 with
    /// audit `auth.sa_expired`.
    #[serde(default)]
    pub expires_at: Option<u64>,
}

/// v0.5.0-iam: a human user account. Distinct from a ServiceAccount
/// (`ApiKeyMeta`) — users authenticate with username+password to obtain
/// a session cookie; service accounts authenticate with a raw bearer key.
/// One User owns 0..N ServiceAccounts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserMeta {
    /// Unique username (primary key), 2-64 bytes of `[a-zA-Z0-9_.-]`.
    pub username: String,
    /// SHA-256 hash of `password || salt`.
    pub password_hash: [u8; 32],
    /// Per-user random salt mixed into the password hash.
    pub salt: [u8; 16],
    /// Permission scopes. Same vocabulary as [`ApiKeyMeta::scopes`] —
    /// `"admin"`, `"operator"`, `"viewer"`, `"read"`, `"write"`.
    /// Convention: `admin` elevates to `read`+`write`; `operator` = `read+write`;
    /// `viewer` = `read`.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// Optional partition allow-list. `None` = all partitions.
    #[serde(default)]
    pub allowed_partitions: Option<Vec<String>>,
    /// Unix timestamp when this user was created.
    pub created_at: u64,
    /// Whether this user can currently log in. Disabling a user does NOT
    /// automatically revoke its service accounts.
    pub enabled: bool,
    /// True only for the single root account. Guarded against deletion
    /// and against `enabled = false` transitions.
    #[serde(default)]
    pub is_root: bool,
}

/// A dedup index entry: maps a content hash → block location.
///
/// v0.4.8: `hash` is 32 raw bytes (was 64-char hex). Dedup entries only
/// exist when dedup is enabled, so there's no `Option` wrapper — if
/// `ChunkMeta.hash == None` the chunk simply isn't registered here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupEntry {
    pub hash: HashBytes,
    pub block_id: String,
    pub offset: u64,
    pub original_size: u64,
    pub compressed_size: u64,
}

/// Result of an audited action (Phase 2.6).
///
/// Kept out-of-band from HTTP status because an audit consumer often cares
/// about semantic outcome (e.g. a 404 on a purge attempt is still "success"
/// from an audit-trail standpoint; a 401 is always a denial worth recording).
/// v0.4.8: removed `#[serde(tag = "kind")]` — internally-tagged enums
/// rely on `Deserializer::deserialize_any`, which bincode's
/// non-self-describing format can't provide. The default external
/// tagging encodes as a compact variant-index + payload pair, which is
/// both smaller on the wire and supported by every serde backend the
/// project uses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AuditResult {
    Ok,
    Denied { reason: String },
    Error { message: String },
}

/// A structured audit event recording a security- or data-relevant action.
/// Stored in the `audit` table keyed by `(ts_nanos, seq)` so scans return
/// events in chronological order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Unix timestamp in seconds. Duplicated from the storage key for
    /// convenience so consumers don't have to decode the raw key.
    pub ts: u64,
    /// `key_id` of the caller, or `None` for un-authenticated events
    /// (e.g. a failed login).
    pub actor_key_id: Option<String>,
    /// Caller IP as best-effort extracted from the connecting socket or the
    /// `X-Forwarded-For` header when behind a proxy.
    pub actor_ip: Option<String>,
    /// Canonical action name, snake_case. Examples:
    ///   `auth.login`, `auth.login_failed`, `auth.logout`, `auth.change_password`,
    ///   `key.create`, `key.delete`, `file.delete`, `gc.trigger`, `config.update`.
    pub action: String,
    /// Optional target identifier (file_id / key_id / block_id / ...).
    pub target: Option<String>,
    /// Outcome classification (see [`AuditResult`]).
    pub result: AuditResult,
    /// Associated HTTP status, if the event was produced from an HTTP handler.
    pub http_status: Option<u16>,
}
