use std::sync::Arc;

use axum::{
    extract::State,
    Json,
};
use serde::Serialize;

use jihuan_core::Engine;

use crate::http::files::AppError;


#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub file_count: u64,
    pub block_count: u64,
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
}

/// GET /api/status
pub async fn get_status(State(engine): State<Arc<Engine>>) -> Result<Json<StatusResponse>, AppError> {
    let e = engine.clone();
    let (fc, bc) = tokio::task::spawn_blocking(move || {
        let fc = e.file_count().unwrap_or(0);
        let bc = e.block_count().unwrap_or(0);
        (fc, bc)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?;

    let cfg = engine.config();

    Ok(Json(StatusResponse {
        file_count: fc,
        block_count: bc,
        version: env!("CARGO_PKG_VERSION").to_string(),
        hash_algorithm: cfg.storage.hash_algorithm.to_string(),
        compression_algorithm: cfg.storage.compression_algorithm.to_string(),
        compression_level: cfg.storage.compression_level,
    }))
}

/// POST /api/gc/trigger
pub async fn trigger_gc(State(engine): State<Arc<Engine>>) -> Result<Json<GcResponse>, AppError> {
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
) -> Result<Json<BlockListResponse>, AppError> {
    let blocks = tokio::task::spawn_blocking(move || engine.metadata().list_all_blocks())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(|e| AppError::internal(e.to_string()))?;

    let dtos: Vec<BlockInfoDto> = blocks
        .into_iter()
        .map(|b| BlockInfoDto {
            block_id: b.block_id,
            size: b.size,
            ref_count: b.ref_count,
            create_time: b.create_time,
            path: b.path,
        })
        .collect();

    let count = dtos.len();
    Ok(Json(BlockListResponse {
        blocks: dtos,
        count,
    }))
}
