use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Multipart, Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::Serialize;

use jihuan_core::{Engine, JiHuanError};

#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub file_id: String,
    pub file_name: String,
    pub file_size: u64,
}

#[derive(Debug, Serialize)]
pub struct FileMetaResponse {
    pub file_id: String,
    pub file_name: String,
    pub file_size: u64,
    pub create_time: u64,
    pub content_type: Option<String>,
    pub chunk_count: usize,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: u16,
}

pub fn router(engine: Arc<Engine>) -> Router {
    Router::new()
        .route("/", post(upload_file))
        .route("/{file_id}", get(download_file))
        .route("/{file_id}", delete(delete_file))
        .route("/{file_id}/meta", get(get_file_meta))
        .with_state(engine)
}

/// POST /api/v1/files
/// Upload a file via multipart form data
async fn upload_file(
    State(engine): State<Arc<Engine>>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, AppError> {
    let mut file_data: Option<Vec<u8>> = None;
    let mut file_name = String::from("unknown");
    let mut content_type: Option<String> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::bad_request(format!("Multipart error: {}", e)))?
    {
        let name = field.name().unwrap_or("").to_string();
        let field_content_type = field.content_type().map(|s| s.to_string());

        if name == "file" || file_data.is_none() {
            if let Some(fname) = field.file_name() {
                file_name = fname.to_string();
            }
            content_type = field_content_type;
            let data = field
                .bytes()
                .await
                .map_err(|e| AppError::bad_request(format!("Failed to read field: {}", e)))?;
            file_data = Some(data.to_vec());
        }
    }

    let data = file_data.ok_or_else(|| AppError::bad_request("No file field found"))?;
    let file_size = data.len() as u64;
    let fn_clone = file_name.clone();
    let ct = content_type.clone();

    let file_id =
        tokio::task::spawn_blocking(move || engine.put_bytes(&data, &fn_clone, ct.as_deref()))
            .await
            .map_err(|e| AppError::internal(e.to_string()))?
            .map_err(AppError::from_jihuan)?;

    Ok(Json(UploadResponse {
        file_id,
        file_name,
        file_size,
    }))
}

/// GET /api/v1/files/:file_id
/// Download a file
async fn download_file(
    State(engine): State<Arc<Engine>>,
    Path(file_id): Path<String>,
) -> Result<Response, AppError> {
    // First fetch metadata for filename and content-type
    let meta = {
        let e = engine.clone();
        let fid = file_id.clone();
        tokio::task::spawn_blocking(move || e.get_file_meta(&fid))
            .await
            .map_err(|e| AppError::internal(e.to_string()))?
            .map_err(AppError::from_jihuan)?
    };

    let meta = meta.ok_or_else(|| AppError::not_found(&file_id))?;

    let e = engine.clone();
    let fid = file_id.clone();
    let data = tokio::task::spawn_blocking(move || e.get_bytes(&fid))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    let content_type = meta
        .content_type
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        content_type
            .parse()
            .unwrap_or("application/octet-stream".parse().unwrap()),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        format!("attachment; filename=\"{}\"", meta.file_name)
            .parse()
            .unwrap(),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        data.len().to_string().parse().unwrap(),
    );

    Ok((StatusCode::OK, headers, Body::from(data)).into_response())
}

/// DELETE /api/v1/files/:file_id
async fn delete_file(
    State(engine): State<Arc<Engine>>,
    Path(file_id): Path<String>,
) -> Result<StatusCode, AppError> {
    tokio::task::spawn_blocking(move || engine.delete_file(&file_id))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/v1/files/:file_id/meta
async fn get_file_meta(
    State(engine): State<Arc<Engine>>,
    Path(file_id): Path<String>,
) -> Result<Json<FileMetaResponse>, AppError> {
    let fid = file_id.clone();
    let meta = tokio::task::spawn_blocking(move || engine.get_file_meta(&fid))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?
        .ok_or_else(|| AppError::not_found(&file_id))?;

    Ok(Json(FileMetaResponse {
        file_id: meta.file_id,
        file_name: meta.file_name,
        file_size: meta.file_size,
        create_time: meta.create_time,
        content_type: meta.content_type,
        chunk_count: meta.chunks.len(),
    }))
}

// ─── Error Handling ───────────────────────────────────────────────────────────

pub struct AppError {
    pub status: StatusCode,
    pub message: String,
}

impl AppError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }

    pub fn not_found(id: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: format!("File '{}' not found", id),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }

    pub fn from_jihuan(e: JiHuanError) -> Self {
        match e {
            JiHuanError::NotFound(msg) => Self {
                status: StatusCode::NOT_FOUND,
                message: msg,
            },
            JiHuanError::AlreadyExists(msg) => Self {
                status: StatusCode::CONFLICT,
                message: msg,
            },
            JiHuanError::InvalidArgument(msg) => Self {
                status: StatusCode::BAD_REQUEST,
                message: msg,
            },
            other => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: other.to_string(),
            },
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let code = self.status.as_u16();
        let body = Json(ErrorResponse {
            error: self.message,
            code,
        });
        (self.status, body).into_response()
    }
}
