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
    /// v0.4.5: sum of compressed_size over *unique* chunk hashes in this
    /// block that are still referenced by at least one live file. Matches
    /// the live-bytes definition used by the compaction scanner.
    pub live_bytes: u64,
    /// v0.4.5: `live_bytes / size` in `[0.0, 1.0]`. `None` when the block
    /// is unsealed and its file size is still 0 (utilisation is
    /// ill-defined in that state).
    pub utilization: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct BlockDetailResponse {
    pub block_id: String,
    pub size: u64,
    pub ref_count: u64,
    pub create_time: u64,
    pub path: String,
    pub sealed: bool,
    pub live_bytes: u64,
    pub utilization: Option<f64>,
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

    // v0.4.5: an explicit GC trigger is the operator asking for
    // "reclaim everything you can right now". If the active block is
    // currently holding only dead data (ref_count==0, e.g. the user
    // deleted the file whose chunks lived there), seal it first so
    // the GC pass below can physically delete it. Without this step
    // that dead data would stick around until new writes organically
    // filled the block past the rollover threshold.
    let engine_for_seal = engine.clone();
    let sealed = tokio::task::spawn_blocking(move || engine_for_seal.seal_if_dead())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(|e| AppError::internal(e.to_string()))?;
    if let Some(info) = sealed {
        tracing::info!(
            block_id = %info.block_id,
            size = info.size,
            "/api/gc/trigger: auto-sealed dead active block before GC"
        );
    }

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

/// POST `/api/admin/compact` — dual-strategy block compaction (v0.4.7).
///
/// Body is a JSON object with the following optional fields. All values
/// default to the engine's configured values; supply a field to override
/// it for this one call.
///
/// * `block_id` — compact exactly this one block. Takes priority over
///   the dual-strategy scan when present; `threshold`,
///   `undersize_ratio`, `min_file_saved`, and `force` are ignored.
/// * `threshold` — Strategy A candidate gate (`[0.0, 1.0]`). Blocks
///   whose `live_bytes / size` is below this ratio are low-util
///   candidates.
/// * `undersize_ratio` — Strategy B candidate gate (`[0.0, 1.0]`).
///   Blocks whose absolute size is below
///   `block_file_size × undersize_ratio` are undersized candidates
///   regardless of utilisation.
/// * `min_file_saved` — Strategy B commit gate. A packed group
///   commits when `source_count − ceil(live_sum / block_file_size)
///   ≥ min_file_saved`. Zero disables gate B.
/// * `force` — when `true`, bypass both Strategy A and Strategy B
///   commit rules and commit every packed candidate group. Does NOT
///   bypass `auto_compact_disk_headroom_bytes` (safety check) or
///   `auto_compact_min_block_age_secs` (age filter). Use during
///   maintenance windows to clean up orphan small blocks.
///
/// v0.4.7 removes the legacy `min_size_bytes` and `min_benefit_bytes`
/// fields; requests containing them are ignored (no error, so old
/// scripts still run).
///
/// Returns an array of per-block stats. The active (unsealed) block
/// is never compacted regardless of parameters.
#[derive(Debug, serde::Deserialize, Default)]
#[serde(default)]
pub struct CompactRequest {
    pub block_id: Option<String>,
    pub threshold: Option<f64>,
    pub undersize_ratio: Option<f64>,
    pub min_file_saved: Option<u64>,
    pub force: Option<bool>,
    /// Deprecated in v0.4.7 — accepted-and-ignored so existing admin
    /// scripts keep working for one release.
    pub min_size_bytes: Option<u64>,
    /// Deprecated in v0.4.7 — accepted-and-ignored.
    pub min_benefit_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct CompactResponse {
    pub compacted: Vec<jihuan_core::CompactionBlockStats>,
    pub total_bytes_saved: i64,
}

pub async fn compact(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Json(req): Json<CompactRequest>,
) -> Result<Json<CompactResponse>, AppError> {
    require_scope(&caller, "admin")?;

    if req.min_size_bytes.is_some() || req.min_benefit_bytes.is_some() {
        tracing::warn!(
            "/api/admin/compact: ignoring deprecated fields \
             min_size_bytes / min_benefit_bytes (removed in v0.4.7)"
        );
    }

    // Mirror the behaviour of /api/gc/trigger — when the operator is
    // explicitly asking to reclaim space, seal an active block that's
    // already fully dereferenced so the normal compaction loop picks
    // it up. Only runs for the dual-strategy scan path; targeted
    // `block_id` requests are left alone.
    if req.block_id.is_none() {
        let engine_for_seal = engine.clone();
        let sealed = tokio::task::spawn_blocking(move || engine_for_seal.seal_if_dead())
            .await
            .map_err(|e| AppError::internal(e.to_string()))?
            .map_err(|e| AppError::internal(e.to_string()))?;
        if let Some(info) = sealed {
            tracing::info!(
                block_id = %info.block_id,
                size = info.size,
                "/api/admin/compact: auto-sealed dead active block before scan"
            );
        }
    }

    // Build compaction options: config defaults, patched by whatever
    // fields the caller supplied.
    let mut opts = jihuan_core::CompactionOptions::from_config(engine.config());
    if let Some(t) = req.threshold {
        opts.threshold = t;
    }
    if let Some(r) = req.undersize_ratio {
        opts.undersize_ratio = r;
    }
    if let Some(n) = req.min_file_saved {
        opts.min_file_saved = n;
    }
    if let Some(f) = req.force {
        opts.force = f;
    }

    let compacted = tokio::task::spawn_blocking(move || -> Result<Vec<_>, _> {
        if let Some(id) = req.block_id {
            engine.compact_block(&id).map(|s| vec![s])
        } else {
            engine.compact_with(opts)
        }
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(|e| AppError::internal(e.to_string()))?;

    let total_bytes_saved = compacted.iter().map(|s| s.bytes_saved).sum();
    Ok(Json(CompactResponse {
        compacted,
        total_bytes_saved,
    }))
}

/// POST `/api/admin/seal` — v0.4.4: force-seal the currently active block.
///
/// Normally a block is sealed automatically when its on-disk size reaches
/// `storage.block_file_size` (default 1 GiB). Operators who want to
/// *immediately* benefit from compaction / GC on a low-utilisation block
/// need a manual trigger — otherwise the block sits as `active` and
/// `compact_block` correctly refuses it.
///
/// Returns the sealed block's id + final size (`size: 0` when the active
/// writer was already empty and no block was sealed).
#[derive(Debug, Serialize)]
pub struct SealResponse {
    pub sealed_block_id: Option<String>,
    pub size: u64,
}

pub async fn seal(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
) -> Result<Json<SealResponse>, AppError> {
    require_scope(&caller, "admin")?;
    let result = tokio::task::spawn_blocking(move || engine.seal_active_block())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(|e| AppError::internal(e.to_string()))?;
    Ok(Json(SealResponse {
        sealed_block_id: result.as_ref().map(|r| r.block_id.clone()),
        size: result.map(|r| r.size).unwrap_or(0),
    }))
}

/// GET /api/block/list
pub async fn list_blocks(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
) -> Result<Json<BlockListResponse>, AppError> {
    require_scope(&caller, "read")?;
    let e_for_blocks = engine.clone();
    let blocks = tokio::task::spawn_blocking(move || e_for_blocks.metadata().list_all_blocks())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(|e| AppError::internal(e.to_string()))?;

    // v0.4.5: compute canonical per-block live_bytes via the engine helper
    // so the UI's utilisation reading matches the compaction scanner's.
    let e_for_live = engine.clone();
    let live_info = tokio::task::spawn_blocking(move || e_for_live.compute_block_live_info())
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
                let live_bytes = live_info
                    .get(&b.block_id)
                    .map(|i| i.live_bytes)
                    .unwrap_or(0);
                let utilization = if size == 0 {
                    None
                } else {
                    Some((live_bytes as f64 / size as f64).clamp(0.0, 1.0))
                };
                BlockInfoDto {
                    sealed,
                    block_id: b.block_id,
                    size,
                    ref_count: b.ref_count,
                    create_time: b.create_time,
                    path: b.path,
                    live_bytes,
                    utilization,
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
    let (block, files, live_info) =
        tokio::task::spawn_blocking(move || -> jihuan_core::error::Result<_> {
            let block = e.metadata().get_block(&bid)?;
            let files = e.list_all_files()?;
            let live_info = e.compute_block_live_info()?;
            Ok((block, files, live_info))
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

    let live_bytes = live_info
        .get(&block_id)
        .map(|i| i.live_bytes)
        .unwrap_or(0);
    let utilization = if size == 0 {
        None
    } else {
        Some((live_bytes as f64 / size as f64).clamp(0.0, 1.0))
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
        live_bytes,
        utilization,
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
