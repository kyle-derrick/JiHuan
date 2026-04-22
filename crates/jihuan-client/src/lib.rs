//! # jihuan-client
//!
//! Unified client abstraction for JiHuan storage, supporting multiple
//! deployment topologies via a single [`StorageClient`] trait.
//!
//! ## Goals (v0.5.0 milestone)
//!
//! - **Single surface for three backends**: embedded (in-process), HTTP (remote
//!   server), and — in a future release — `ClusterClient` that talks to a
//!   distributed JiHuan cluster. Application code binds against the trait and
//!   swaps backend via a single line at startup.
//! - **Extreme standalone performance**: [`EmbeddedClient`] wraps
//!   [`jihuan_core::Engine`] directly. No HTTP hop, no JSON (de)serialisation,
//!   no Tokio spawn_blocking on the uploader hot path.
//! - **Zero behavioural drift**: every implementation passes the same
//!   `storage_client_contract` test suite (see `tests/contract.rs`).
//!
//! ## Quick start
//!
//! ```no_run
//! # #[tokio::main] async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use jihuan_client::{EmbeddedClient, StorageClient};
//! use jihuan_core::config::ConfigTemplate;
//!
//! let cfg = ConfigTemplate::general("./jihuan-data".into());
//! let client = EmbeddedClient::open(cfg)?;
//!
//! let id = client.put(b"hello".to_vec(), "greet.txt").await?;
//! let got = client.get(&id).await?;
//! assert_eq!(got, b"hello");
//! # Ok(()) }
//! ```

mod embedded;
mod error;
mod http;
mod types;

pub use embedded::EmbeddedClient;
pub use error::{ClientError, ClientResult};
pub use http::HttpClient;
pub use types::{FileId, FileInfo, PutOptions, StorageStats};

use async_trait::async_trait;

/// Unified storage API abstraction.
///
/// All methods are `async` so that the HTTP and future cluster
/// implementations don't force their callers onto blocking threads; the
/// embedded backend satisfies them by dispatching blocking engine work onto
/// the Tokio blocking pool. Callers that want the absolute lowest latency
/// on embedded can reach for the inherent sync methods on
/// [`EmbeddedClient`] directly.
#[async_trait]
pub trait StorageClient: Send + Sync {
    /// Store a file from an owned byte buffer. Returns the assigned file id.
    ///
    /// The ownership transfer is intentional: both backends need the data
    /// to outlive the caller's stack frame (embedded hands it to a blocking
    /// worker; HTTP uploads it via multipart). Cloning is the caller's
    /// decision, not ours.
    async fn put(&self, data: Vec<u8>, file_name: &str) -> ClientResult<FileId>;

    /// Same as [`put`] but with additional per-call options (content-type,
    /// explicit file id override, etc.). Most users want [`put`].
    async fn put_with_options(
        &self,
        data: Vec<u8>,
        file_name: &str,
        opts: PutOptions,
    ) -> ClientResult<FileId>;

    /// Read a file back into memory.
    ///
    /// For large files prefer a future `get_stream` variant (tracked as
    /// v0.5.3 in the roadmap). This method is intentionally simple — it's
    /// the common case for small-file workloads which is JiHuan's primary
    /// target.
    async fn get(&self, file_id: &FileId) -> ClientResult<Vec<u8>>;

    /// Fetch file metadata without the body.
    async fn stat(&self, file_id: &FileId) -> ClientResult<FileInfo>;

    /// Delete a file. Idempotent: deleting a missing file returns
    /// [`ClientError::NotFound`], which callers may choose to swallow.
    async fn delete(&self, file_id: &FileId) -> ClientResult<()>;

    /// List all files currently stored.
    ///
    /// For large deployments this is still a full scan — pagination is on
    /// the roadmap but not in v0.5.0 since the primary use case is
    /// admin/UI tooling.
    async fn list_files(&self) -> ClientResult<Vec<FileInfo>>;

    /// Return system-wide storage statistics.
    async fn stats(&self) -> ClientResult<StorageStats>;

    // ── v0.5.1 Batch API ──────────────────────────────────────────────────
    //
    // Default implementations fall back to the per-item async methods so
    // that any `StorageClient` gains batch support for free. Backends with
    // cheaper bulk paths (embedded: single `spawn_blocking`; future HTTP
    // backend: one multi-part POST) should override these defaults.
    //
    // Results are returned as `Vec<ClientResult<_>>` rather than
    // `ClientResult<Vec<_>>` so that partial failures remain visible to
    // the caller — matching the underlying `Engine::*_batch` contract.

    /// Batch-store N files. Returns per-item results in input order.
    async fn put_batch(&self, items: Vec<(Vec<u8>, String)>) -> Vec<ClientResult<FileId>> {
        let mut out = Vec::with_capacity(items.len());
        for (data, name) in items {
            out.push(self.put(data, &name).await);
        }
        out
    }

    /// Batch-fetch N files. Returns per-item results in input order.
    async fn get_batch(&self, ids: &[FileId]) -> Vec<ClientResult<Vec<u8>>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(self.get(id).await);
        }
        out
    }

    /// Batch-delete N files. Returns per-item results in input order.
    async fn delete_batch(&self, ids: &[FileId]) -> Vec<ClientResult<()>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(self.delete(id).await);
        }
        out
    }
}
