use std::sync::Arc;

use tonic::{Request, Response, Status};

use jihuan_core::Engine;

use crate::grpc::pb::{
    admin_service_server::AdminService, BlockInfo, GetStatusRequest, GetStatusResponse,
    ListBlocksRequest, ListBlocksResponse, TriggerGcRequest, TriggerGcResponse,
};

pub struct AdminServiceImpl {
    engine: Arc<Engine>,
}

impl AdminServiceImpl {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl AdminService for AdminServiceImpl {
    async fn get_status(
        &self,
        _request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, Status> {
        let engine = self.engine.clone();
        let (file_count, block_count) = tokio::task::spawn_blocking(move || {
            let fc = engine.file_count().unwrap_or(0);
            let bc = engine.block_count().unwrap_or(0);
            (fc, bc)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let cfg = self.engine.config();

        Ok(Response::new(GetStatusResponse {
            file_count,
            block_count,
            total_bytes: 0, // Could sum block sizes from metadata
            dedup_ratio: 0.0,
            compression_ratio: 0.0,
            version: env!("CARGO_PKG_VERSION").to_string(),
            hash_algorithm: cfg.storage.hash_algorithm.to_string(),
            compression_algorithm: cfg.storage.compression_algorithm.to_string(),
            compression_level: cfg.storage.compression_level as u32,
        }))
    }

    async fn trigger_gc(
        &self,
        _request: Request<TriggerGcRequest>,
    ) -> Result<Response<TriggerGcResponse>, Status> {
        let stats = self
            .engine
            .trigger_gc()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(TriggerGcResponse {
            blocks_deleted: stats.blocks_deleted,
            bytes_reclaimed: stats.bytes_reclaimed,
            partitions_deleted: stats.partitions_deleted,
            files_deleted: stats.files_deleted,
            duration_ms: stats.duration_ms,
        }))
    }

    async fn list_blocks(
        &self,
        _request: Request<ListBlocksRequest>,
    ) -> Result<Response<ListBlocksResponse>, Status> {
        let engine = self.engine.clone();
        let blocks = tokio::task::spawn_blocking(move || engine.metadata().list_all_blocks())
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))?;

        let block_infos: Vec<BlockInfo> = blocks
            .into_iter()
            .map(|b| BlockInfo {
                block_id: b.block_id,
                size: b.size,
                ref_count: b.ref_count,
                create_time: b.create_time,
                path: b.path,
            })
            .collect();

        Ok(Response::new(ListBlocksResponse {
            blocks: block_infos,
            next_page_token: String::new(),
        }))
    }
}
