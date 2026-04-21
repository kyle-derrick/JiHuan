//! Shared DTOs exposed to callers of [`crate::StorageClient`].
//!
//! These mirror `jihuan_core` types but are re-declared here so that the
//! client crate has a stable, backend-agnostic surface. HTTP serialisation
//! for the remote backend happens in `http.rs` using these same structs.

use serde::{Deserialize, Serialize};

/// Opaque file identifier. Currently a hex-encoded 128-bit value, but
/// callers must treat it as an opaque string — we explicitly forbid
/// parsing or comparing anything but equality.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FileId(pub String);

impl FileId {
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for FileId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Optional knobs for [`crate::StorageClient::put_with_options`].
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    /// MIME type hint stored alongside the file. When `None`, backends
    /// apply their default (`application/octet-stream` for embedded).
    pub content_type: Option<String>,
}

/// Metadata snapshot of a single stored file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub file_id: FileId,
    pub file_name: String,
    pub file_size: u64,
    pub content_type: Option<String>,
    pub create_time: u64,
    pub chunk_count: usize,
}

/// System-wide snapshot matching [`jihuan_core::metrics::EngineStats`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    pub file_count: u64,
    pub block_count: u64,
    /// Total logical bytes (pre-dedup, pre-compression, pre-block-overhead).
    pub logical_bytes: u64,
    /// Actual on-disk bytes under `data_dir`.
    pub disk_usage_bytes: u64,
    pub dedup_ratio: f64,
}
