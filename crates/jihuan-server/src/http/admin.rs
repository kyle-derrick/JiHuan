use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use serde_json::Value;
use std::sync::OnceLock;

use jihuan_core::Engine;

use crate::http::auth::{require_scope, AuthedKey};
use crate::http::files::AppError;

/// Tuple-struct wrapper so the handle can be inserted as an axum `Extension`
/// even when Prometheus recorder installation failed.
#[derive(Clone)]
pub struct MetricsHandle(pub Option<PrometheusHandle>);

static START_TIME: OnceLock<Instant> = OnceLock::new();

fn start_time() -> Instant {
    *START_TIME.get_or_init(Instant::now)
}

/// Call once from main() to pin the process start time.
pub fn init_start_time() {
    let _ = start_time();
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub file_count: u64,
    pub block_count: u64,
    /// Sum of all user-uploaded file sizes, pre-dedup / pre-compression.
    pub logical_bytes: u64,
    /// Actual on-disk bytes under `data_dir` (compressed, deduplicated, plus
    /// block headers/footers). May temporarily exceed `logical_bytes` on
    /// tiny datasets due to block overhead.
    pub disk_usage_bytes: u64,
    /// Configured hard storage cap; `None` = unlimited. Mirrors
    /// `storage.max_storage_bytes` so the UI can draw a usage gauge.
    pub max_storage_bytes: Option<u64>,
    pub dedup_ratio: f64,
    pub uptime_secs: u64,
    pub version: String,
    pub hash_algorithm: String,
    pub compression_algorithm: String,
    pub compression_level: i32,
}

#[derive(Debug, Serialize)]
pub struct GcResponse {
    pub blocks_deleted: u64,
    pub bytes_reclaimed: u64,
    pub partitions_deleted: u64,
    pub files_deleted: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct BlockListResponse {
    pub blocks: Vec<BlockInfoDto>,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct BlockInfoDto {
    pub block_id: String,
    pub size: u64,
    pub ref_count: u64,
    pub create_time: u64,
    pub path: String,
    /// true if block file is sealed (size>0 means we've recorded final size)
    pub sealed: bool,
}

#[derive(Debug, Serialize)]
pub struct BlockDetailResponse {
    pub block_id: String,
    pub size: u64,
    pub ref_count: u64,
    pub create_time: u64,
    pub path: String,
    pub sealed: bool,
    pub referencing_files: Vec<ReferencingFileDto>,
}

#[derive(Debug, Serialize)]
pub struct ReferencingFileDto {
    pub file_id: String,
    pub file_name: String,
    pub file_size: u64,
    pub chunks_in_block: usize,
}

/// GET /api/status
pub async fn get_status(State(engine): State<Arc<Engine>>) -> Result<Json<StatusResponse>, AppError> {
    let e = engine.clone();
    let stats = tokio::task::spawn_blocking(move || e.stats())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(|e| AppError::internal(e.to_string()))?;

    let cfg = engine.config();
    let uptime_secs = start_time().elapsed().as_secs();

    Ok(Json(StatusResponse {
        file_count: stats.file_count,
        block_count: stats.block_count,
        logical_bytes: stats.logical_bytes,
        disk_usage_bytes: stats.disk_usage_bytes,
        max_storage_bytes: cfg.storage.max_storage_bytes,
        dedup_ratio: stats.dedup_ratio,
        uptime_secs,
        version: env!("CARGO_PKG_VERSION").to_string(),
        hash_algorithm: cfg.storage.hash_algorithm.to_string(),
        compression_algorithm: cfg.storage.compression_algorithm.to_string(),
        compression_level: cfg.storage.compression_level,
    }))
}

/// POST /api/gc/trigger
pub async fn trigger_gc(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
) -> Result<Json<GcResponse>, AppError> {
    require_scope(&caller, "admin")?;
    let stats = engine
        .trigger_gc()
        .await
        .map_err(|e| AppError::internal(e.to_string()))?;

    Ok(Json(GcResponse {
        blocks_deleted: stats.blocks_deleted,
        bytes_reclaimed: stats.bytes_reclaimed,
        partitions_deleted: stats.partitions_deleted,
        files_deleted: stats.files_deleted,
        duration_ms: stats.duration_ms,
    }))
}

/// GET /api/block/list
pub async fn list_blocks(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
) -> Result<Json<BlockListResponse>, AppError> {
    require_scope(&caller, "read")?;
    let blocks = tokio::task::spawn_blocking(move || engine.metadata().list_all_blocks())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(|e| AppError::internal(e.to_string()))?;

    // Compute display size for each block:
    //   • sealed (meta.size > 0) → trust the metadata
    //   • unsealed (meta.size == 0, e.g. the active writer) → fall back to
    //     `fs::metadata(path).len()` so the UI/CLI doesn't show 0 B while the
    //     block is still being filled.
    let dtos: Vec<BlockInfoDto> = tokio::task::spawn_blocking(move || {
        blocks
            .into_iter()
            .map(|b| {
                let sealed = b.size > 0;
                let size = if sealed {
                    b.size
                } else {
                    std::fs::metadata(&b.path).map(|m| m.len()).unwrap_or(0)
                };
                BlockInfoDto {
                    sealed,
                    block_id: b.block_id,
                    size,
                    ref_count: b.ref_count,
                    create_time: b.create_time,
                    path: b.path,
                }
            })
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;

    let count = dtos.len();
    Ok(Json(BlockListResponse {
        blocks: dtos,
        count,
    }))
}

/// GET /api/block/:id
pub async fn get_block_detail(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(block_id): Path<String>,
) -> Result<Json<BlockDetailResponse>, AppError> {
    require_scope(&caller, "read")?;
    let e = engine.clone();
    let bid = block_id.clone();
    let (block, files) = tokio::task::spawn_blocking(move || -> jihuan_core::error::Result<_> {
        let block = e.metadata().get_block(&bid)?;
        let files = e.list_all_files()?;
        Ok((block, files))
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(|e| AppError::internal(e.to_string()))?;

    let block = block.ok_or_else(|| AppError::not_found(&block_id))?;

    // Resolve display size the same way as list_blocks: stat the file when the
    // block is still unsealed (meta.size == 0).
    let sealed = block.size > 0;
    let size = if sealed {
        block.size
    } else {
        let path = block.path.clone();
        tokio::task::spawn_blocking(move || std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0))
            .await
            .map_err(|e| AppError::internal(e.to_string()))?
    };

    let referencing_files: Vec<ReferencingFileDto> = files
        .into_iter()
        .filter_map(|f| {
            let n = f.chunks.iter().filter(|c| c.block_id == block_id).count();
            if n > 0 {
                Some(ReferencingFileDto {
                    file_id: f.file_id,
                    file_name: f.file_name,
                    file_size: f.file_size,
                    chunks_in_block: n,
                })
            } else {
                None
            }
        })
        .collect();

    Ok(Json(BlockDetailResponse {
        sealed,
        block_id: block.block_id,
        size,
        ref_count: block.ref_count,
        create_time: block.create_time,
        path: block.path,
        referencing_files,
    }))
}

/// DELETE /api/block/:id
/// Only allowed when ref_count == 0. Removes the block file and metadata entry.
/// admin only.
pub async fn delete_block(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(block_id): Path<String>,
) -> Result<StatusCode, AppError> {
    require_scope(&caller, "admin")?;
    let e = engine.clone();
    let bid = block_id.clone();
    let result = tokio::task::spawn_blocking(move || -> jihuan_core::error::Result<Result<(), String>> {
        let block = match e.metadata().get_block(&bid)? {
            Some(b) => b,
            None => return Ok(Err("not_found".into())),
        };
        if block.ref_count > 0 {
            return Ok(Err(format!(
                "block is still referenced (ref_count={})",
                block.ref_count
            )));
        }
        // Delete file on disk (best-effort)
        let _ = std::fs::remove_file(&block.path);
        e.metadata().delete_block(&bid)?;
        Ok(Ok(()))
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(|e| AppError::internal(e.to_string()))?;

    match result {
        Ok(()) => Ok(StatusCode::NO_CONTENT),
        Err(ref m) if m == "not_found" => Err(AppError::not_found(&block_id)),
        Err(m) => Err(AppError::conflict(m)),
    }
}

/// GET /api/config
/// Returns the currently-loaded AppConfig as JSON (read-only view).
pub async fn get_config(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
) -> Result<Json<Value>, AppError> {
    require_scope(&caller, "read")?;
    let cfg = engine.config();
    let v = serde_json::to_value(cfg).map_err(|e| AppError::internal(e.to_string()))?;
    Ok(Json(v))
}

/// GET /api/metrics
/// Same-origin proxy for the Prometheus exposition format. Returns the text
/// produced by the in-process recorder handle — no cross-origin call to
/// `:9090/metrics` is required. Returns 503 if the recorder failed to install.
pub async fn render_metrics(Extension(handle): Extension<MetricsHandle>) -> Response {
    let Some(h) = handle.0 else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "metrics recorder not installed",
        )
            .into_response();
    };
    let body = h.render();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "text/plain; version=0.0.4; charset=utf-8".parse().unwrap(),
    );
    (StatusCode::OK, headers, body).into_response()
}
