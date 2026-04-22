use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use jihuan_core::{ConflictPolicy, Engine, JiHuanError, PutOptions};

use crate::grpc::auth_interceptor::require_scope_grpc;
use crate::grpc::pb::{
    file_service_server::FileService, get_file_response, put_file_request, DeleteFileRequest,
    DeleteFileResponse, GetFileMetaRequest, GetFileMetaResponse, GetFileRequest, GetFileResponse,
    PutConflictPolicy, PutFileRequest, PutFileResponse,
};

/// v0.4.6: map a `JiHuanError` to a `tonic::Status` with the right code.
/// Mirrors `http::AppError::from_jihuan` so HTTP and gRPC return
/// semantically equivalent errors for the same upload scenarios.
fn jihuan_error_to_status(e: JiHuanError) -> Status {
    match e {
        JiHuanError::NotFound(msg) => Status::not_found(msg),
        JiHuanError::AlreadyExists(msg) => Status::already_exists(msg),
        JiHuanError::InvalidArgument(msg) => Status::invalid_argument(msg),
        JiHuanError::InvalidFileId(msg) => {
            Status::invalid_argument(format!("Invalid file_id: {}", msg))
        }
        e @ JiHuanError::StorageFull { .. } => Status::resource_exhausted(e.to_string()),
        other => Status::internal(other.to_string()),
    }
}

pub struct FileServiceImpl {
    engine: Arc<Engine>,
}

impl FileServiceImpl {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

#[tonic::async_trait]
impl FileService for FileServiceImpl {
    type GetFileStream = ReceiverStream<Result<GetFileResponse, Status>>;

    async fn put_file(
        &self,
        request: Request<Streaming<PutFileRequest>>,
    ) -> Result<Response<PutFileResponse>, Status> {
        require_scope_grpc(&request, "write")?;
        let mut stream = request.into_inner();
        let mut file_name = String::new();
        let mut _file_size = 0u64;
        let mut content_type = String::new();
        let mut custom_file_id: Option<String> = None;
        let mut on_conflict: ConflictPolicy = ConflictPolicy::Error;
        let mut data_buf: Vec<u8> = Vec::new();

        while let Some(msg) = stream.message().await? {
            match msg.payload {
                Some(put_file_request::Payload::Info(info)) => {
                    file_name = info.file_name;
                    _file_size = info.file_size;
                    content_type = info.content_type;
                    data_buf.reserve(_file_size as usize);
                    // v0.4.6: Info may carry a caller-supplied id and conflict
                    // policy. Proto3 string defaults to "" — treat that as "no id".
                    let fid_trimmed = info.file_id.trim();
                    if !fid_trimmed.is_empty() {
                        custom_file_id = Some(fid_trimmed.to_string());
                    }
                    // Enum decodes from i32; unknown values become the default
                    // Error variant (0), which matches the HTTP default.
                    on_conflict = match PutConflictPolicy::try_from(info.on_conflict) {
                        Ok(PutConflictPolicy::Skip) => ConflictPolicy::Skip,
                        Ok(PutConflictPolicy::Overwrite) => ConflictPolicy::Overwrite,
                        Ok(PutConflictPolicy::Error) | Err(_) => ConflictPolicy::Error,
                    };
                }
                Some(put_file_request::Payload::ChunkData(chunk)) => {
                    data_buf.extend_from_slice(&chunk);
                }
                None => {}
            }
        }

        if file_name.is_empty() {
            return Err(Status::invalid_argument("file_name is required"));
        }

        let engine = self.engine.clone();
        let ct = if content_type.is_empty() {
            None
        } else {
            Some(content_type.as_str().to_string())
        };
        let fn_clone = file_name.clone();
        let id_clone = custom_file_id.clone();
        let stored_size = data_buf.len() as u64;

        let (file_id, outcome) = tokio::task::spawn_blocking(move || {
            let opts = PutOptions {
                file_name: &fn_clone,
                content_type: ct.as_deref(),
                file_id: id_clone.as_deref(),
                on_conflict,
            };
            engine.put_bytes_with(&data_buf, opts)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(jihuan_error_to_status)?;

        Ok(Response::new(PutFileResponse {
            file_id,
            stored_size,
            deduplicated: false,
            outcome: outcome.as_str().to_string(),
        }))
    }

    async fn get_file(
        &self,
        request: Request<GetFileRequest>,
    ) -> Result<Response<Self::GetFileStream>, Status> {
        require_scope_grpc(&request, "read")?;
        let file_id = request.into_inner().file_id;
        let engine = self.engine.clone();
        let fid = file_id.clone();

        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            // Get metadata first
            let meta_result = {
                let e = engine.clone();
                let fid2 = fid.clone();
                tokio::task::spawn_blocking(move || e.get_file_meta(&fid2))
                    .await
                    .map_err(|e| Status::internal(e.to_string()))
            };

            let meta = match meta_result {
                Ok(Ok(Some(m))) => m,
                Ok(Ok(None)) => {
                    let _ = tx
                        .send(Err(Status::not_found(format!("File '{}' not found", fid))))
                        .await;
                    return;
                }
                Ok(Err(e)) => {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };

            // Send metadata first
            let meta_msg = GetFileResponse {
                payload: Some(get_file_response::Payload::Meta(
                    crate::grpc::pb::GetFileMeta {
                        file_id: meta.file_id.clone(),
                        file_name: meta.file_name.clone(),
                        file_size: meta.file_size,
                        content_type: meta.content_type.unwrap_or_default(),
                        create_time: meta.create_time,
                    },
                )),
            };
            if tx.send(Ok(meta_msg)).await.is_err() {
                return;
            }

            // Read and stream file data in 1MB chunks
            let data_result = {
                let e = engine.clone();
                let fid2 = fid.clone();
                tokio::task::spawn_blocking(move || e.get_bytes(&fid2))
                    .await
                    .map_err(|e| Status::internal(e.to_string()))
            };

            let data = match data_result {
                Ok(Ok(d)) => d,
                Ok(Err(e)) => {
                    let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };

            const STREAM_CHUNK: usize = 1024 * 1024; // 1MB per stream message
            for chunk in data.chunks(STREAM_CHUNK) {
                let msg = GetFileResponse {
                    payload: Some(get_file_response::Payload::ChunkData(
                        Bytes::copy_from_slice(chunk).to_vec(),
                    )),
                };
                if tx.send(Ok(msg)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn delete_file(
        &self,
        request: Request<DeleteFileRequest>,
    ) -> Result<Response<DeleteFileResponse>, Status> {
        require_scope_grpc(&request, "write")?;
        let file_id = request.into_inner().file_id;
        let engine = self.engine.clone();

        let result = tokio::task::spawn_blocking(move || engine.delete_file(&file_id))
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Ok(()) => Ok(Response::new(DeleteFileResponse { success: true })),
            Err(jihuan_core::JiHuanError::NotFound(_)) => Err(Status::not_found("File not found")),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_file_meta(
        &self,
        request: Request<GetFileMetaRequest>,
    ) -> Result<Response<GetFileMetaResponse>, Status> {
        require_scope_grpc(&request, "read")?;
        let file_id = request.into_inner().file_id;
        let engine = self.engine.clone();

        let result = tokio::task::spawn_blocking(move || engine.get_file_meta(&file_id))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))?;

        match result {
            Some(meta) => Ok(Response::new(GetFileMetaResponse {
                meta: Some(crate::grpc::pb::GetFileMeta {
                    file_id: meta.file_id,
                    file_name: meta.file_name,
                    file_size: meta.file_size,
                    content_type: meta.content_type.unwrap_or_default(),
                    create_time: meta.create_time,
                }),
                found: true,
            })),
            None => Ok(Response::new(GetFileMetaResponse {
                meta: None,
                found: false,
            })),
        }
    }
}
