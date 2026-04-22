//! Embedded (in-process) implementation of [`crate::StorageClient`].
//!
//! This backend links [`jihuan_core::Engine`] directly into the caller's
//! address space. Reads and writes skip every layer that the HTTP stack
//! imposes: no JSON serialisation, no socket, no framing, no retry loop.
//! The async surface is kept for API parity; underneath, engine work that
//! would block the Tokio runtime is dispatched through
//! [`tokio::task::spawn_blocking`].
//!
//! Design notes:
//!
//! - The underlying [`Engine`] is wrapped in [`Arc`] so that clones of
//!   `EmbeddedClient` share a single engine instance. Cheap cloning
//!   matters because a common pattern is to stamp a client into every
//!   request handler of an embedding service.
//! - [`spawn_blocking`] is used for every operation even though
//!   `get_bytes` etc. are only mildly blocking. The deterministic cost
//!   (one context switch) is far preferable to the unpredictable hazard
//!   of stalling a shared Tokio runtime under high load.
//! - Future work (roadmap v0.5.4) will expose inherent sync methods that
//!   skip `spawn_blocking` for callers that run the engine on a dedicated
//!   thread.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use jihuan_core::config::AppConfig;
use jihuan_core::Engine;

use crate::error::{ClientError, ClientResult};
use crate::types::{FileId, FileInfo, PutOptions, StorageStats};
use crate::StorageClient;

/// In-process storage client backed by a [`jihuan_core::Engine`].
///
/// Cloning is O(1) — the wrapped engine is reference-counted. Dropping
/// the last clone seals the active block via [`Engine::shutdown`] as part
/// of the engine's own `Drop` impl.
#[derive(Clone)]
pub struct EmbeddedClient {
    engine: Arc<Engine>,
}

impl EmbeddedClient {
    /// Open (or create) an engine at `config.storage.data_dir` and wrap it
    /// as a client. Performs full crash recovery + WAL replay before
    /// returning.
    pub fn open(config: AppConfig) -> ClientResult<Self> {
        let engine =
            Engine::open(config).map_err(|e| ClientError::Backend(format!("open: {e}")))?;
        Ok(Self {
            engine: Arc::new(engine),
        })
    }

    /// Convenience: open with the bundled "general" profile at the given
    /// directory. Equivalent to
    /// `EmbeddedClient::open(ConfigTemplate::general(dir.into()))`.
    pub fn open_with_defaults(data_dir: impl AsRef<Path>) -> ClientResult<Self> {
        let cfg = jihuan_core::config::ConfigTemplate::general(data_dir.as_ref().to_path_buf());
        Self::open(cfg)
    }

    /// Escape hatch for callers that need direct access to the engine —
    /// used by the server to share a single engine between its HTTP layer
    /// and future in-process management tools. The returned `Arc` keeps
    /// the engine alive independently of this client.
    pub fn engine(&self) -> Arc<Engine> {
        self.engine.clone()
    }

    /// Start the background GC + auto-compaction loops attached to this
    /// engine. Matches what `jihuan-server` does at boot. Returns
    /// `(gc_handle, Option<compact_handle>)`; compact handle is `None`
    /// when `storage.auto_compact_enabled = false`.
    pub fn start_background_tasks(
        &self,
    ) -> (
        tokio::task::JoinHandle<()>,
        Option<tokio::task::JoinHandle<()>>,
    ) {
        let gc = self.engine.start_gc();
        let compact = self.engine.start_auto_compaction();
        (gc, compact)
    }
}

#[async_trait]
impl StorageClient for EmbeddedClient {
    async fn put(&self, data: Vec<u8>, file_name: &str) -> ClientResult<FileId> {
        self.put_with_options(data, file_name, PutOptions::default())
            .await
    }

    async fn put_with_options(
        &self,
        data: Vec<u8>,
        file_name: &str,
        opts: PutOptions,
    ) -> ClientResult<FileId> {
        // Validate up-front: engine accepts empty names but users almost
        // certainly don't want that; surface it as InvalidArgument so HTTP
        // + embedded agree on this behaviour.
        if file_name.is_empty() {
            return Err(ClientError::InvalidArgument(
                "file_name must not be empty".into(),
            ));
        }
        let engine = self.engine.clone();
        let name = file_name.to_string();
        let id = tokio::task::spawn_blocking(move || {
            engine.put_bytes(&data, &name, opts.content_type.as_deref())
        })
        .await
        .map_err(|e| ClientError::Backend(format!("put join: {e}")))??;
        Ok(FileId(id))
    }

    async fn get(&self, file_id: &FileId) -> ClientResult<Vec<u8>> {
        let engine = self.engine.clone();
        let id = file_id.0.clone();
        let bytes = tokio::task::spawn_blocking(move || engine.get_bytes(&id))
            .await
            .map_err(|e| ClientError::Backend(format!("get join: {e}")))??;
        Ok(bytes)
    }

    async fn stat(&self, file_id: &FileId) -> ClientResult<FileInfo> {
        let engine = self.engine.clone();
        let id = file_id.0.clone();
        let file = tokio::task::spawn_blocking(move || engine.metadata().get_file(&id))
            .await
            .map_err(|e| ClientError::Backend(format!("stat join: {e}")))??
            .ok_or_else(|| ClientError::NotFound(file_id.0.clone()))?;
        Ok(FileInfo {
            file_id: FileId(file.file_id),
            file_name: file.file_name,
            file_size: file.file_size,
            content_type: file.content_type,
            create_time: file.create_time,
            chunk_count: file.chunks.len(),
        })
    }

    async fn delete(&self, file_id: &FileId) -> ClientResult<()> {
        let engine = self.engine.clone();
        let id = file_id.0.clone();
        tokio::task::spawn_blocking(move || engine.delete_file(&id))
            .await
            .map_err(|e| ClientError::Backend(format!("delete join: {e}")))??;
        Ok(())
    }

    async fn list_files(&self) -> ClientResult<Vec<FileInfo>> {
        let engine = self.engine.clone();
        let metas = tokio::task::spawn_blocking(move || engine.metadata().list_all_files())
            .await
            .map_err(|e| ClientError::Backend(format!("list join: {e}")))??;
        Ok(metas
            .into_iter()
            .map(|m| FileInfo {
                file_id: FileId(m.file_id),
                file_name: m.file_name,
                file_size: m.file_size,
                content_type: m.content_type,
                create_time: m.create_time,
                chunk_count: m.chunks.len(),
            })
            .collect())
    }

    /// Override: one `spawn_blocking` covers the entire batch, so the
    /// Tokio → engine → Tokio dispatch overhead is paid once instead of
    /// N times. Internally this still delegates to `Engine::put_bytes`
    /// per item, preserving per-file atomicity.
    async fn put_batch(&self, items: Vec<(Vec<u8>, String)>) -> Vec<ClientResult<FileId>> {
        // Cheap upfront validation — stays consistent with single `put`.
        if items.iter().any(|(_, n)| n.is_empty()) {
            return items
                .into_iter()
                .map(|(_, n)| {
                    if n.is_empty() {
                        Err(ClientError::InvalidArgument(
                            "file_name must not be empty".into(),
                        ))
                    } else {
                        // Not reached — we only hit this branch if at least
                        // one name is empty, but we still have to return the
                        // right shape. Match engine semantics: reject the
                        // entire batch atomically on any empty name.
                        Err(ClientError::InvalidArgument(
                            "batch rejected: another item had empty file_name".into(),
                        ))
                    }
                })
                .collect();
        }
        let engine = self.engine.clone();
        let prepared: Vec<(Vec<u8>, String, Option<String>)> =
            items.into_iter().map(|(d, n)| (d, n, None)).collect();
        let results = tokio::task::spawn_blocking(move || engine.put_bytes_batch(&prepared))
            .await
            .unwrap_or_else(|e| {
                vec![Err(jihuan_core::JiHuanError::Internal(format!(
                    "put_batch join: {e}"
                )))]
            });
        results
            .into_iter()
            .map(|r| r.map(FileId).map_err(ClientError::from))
            .collect()
    }

    async fn get_batch(&self, ids: &[FileId]) -> Vec<ClientResult<Vec<u8>>> {
        let engine = self.engine.clone();
        let id_strings: Vec<String> = ids.iter().map(|f| f.0.clone()).collect();
        let results = tokio::task::spawn_blocking(move || engine.get_bytes_batch(&id_strings))
            .await
            .unwrap_or_else(|e| {
                vec![Err(jihuan_core::JiHuanError::Internal(format!(
                    "get_batch join: {e}"
                )))]
            });
        results
            .into_iter()
            .map(|r| r.map_err(ClientError::from))
            .collect()
    }

    async fn delete_batch(&self, ids: &[FileId]) -> Vec<ClientResult<()>> {
        let engine = self.engine.clone();
        let id_strings: Vec<String> = ids.iter().map(|f| f.0.clone()).collect();
        let results = tokio::task::spawn_blocking(move || engine.delete_file_batch(&id_strings))
            .await
            .unwrap_or_else(|e| {
                vec![Err(jihuan_core::JiHuanError::Internal(format!(
                    "delete_batch join: {e}"
                )))]
            });
        results
            .into_iter()
            .map(|r| r.map_err(ClientError::from))
            .collect()
    }

    async fn stats(&self) -> ClientResult<StorageStats> {
        let engine = self.engine.clone();
        let s = tokio::task::spawn_blocking(move || engine.stats())
            .await
            .map_err(|e| ClientError::Backend(format!("stats join: {e}")))??;
        Ok(StorageStats {
            file_count: s.file_count,
            block_count: s.block_count,
            logical_bytes: s.logical_bytes,
            disk_usage_bytes: s.disk_usage_bytes,
            dedup_ratio: s.dedup_ratio,
        })
    }
}
