use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use jihuan_core::{
    config::AuthConfig,
    metadata::types::ApiKeyMeta,
    Engine,
};

use crate::http::files::AppError;

// ─── Auth state shared via axum Extension ─────────────────────────────────────

#[derive(Clone)]
pub struct AuthState {
    pub engine: Arc<Engine>,
    pub config: AuthConfig,
}

// ─── Key hashing helper ───────────────────────────────────────────────────────

pub fn hash_key(raw: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    hex::encode(hasher.finalize())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ─── Auth middleware ───────────────────────────────────────────────────────────

pub async fn auth_middleware(
    State(auth): State<AuthState>,
    req: Request,
    next: Next,
) -> Response {
    if !auth.config.enabled {
        return next.run(req).await;
    }

    let path = req.uri().path().to_string();

    // Check exempt routes (prefix match)
    for exempt in &auth.config.exempt_routes {
        if path.starts_with(exempt.as_str()) {
            return next.run(req).await;
        }
    }

    // Extract API key from headers
    let raw_key = extract_api_key(req.headers());
    let raw_key = match raw_key {
        Some(k) => k,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({
                    "error": "Missing API key. Provide via Authorization: Bearer <key> or X-API-Key: <key>",
                    "code": 401
                })),
            )
                .into_response();
        }
    };

    let key_hash = hash_key(&raw_key);
    let engine = auth.engine.clone();

    let key_result = tokio::task::spawn_blocking(move || {
        engine.metadata().get_api_key_by_hash(&key_hash)
    })
    .await;

    match key_result {
        Ok(Ok(Some(key))) if key.enabled => {
            // Touch last_used_at asynchronously (fire-and-forget)
            let engine2 = auth.engine.clone();
            let key_id = key.key_id.clone();
            tokio::task::spawn_blocking(move || {
                let _ = engine2.metadata().touch_api_key(&key_id, now_secs());
            });
            next.run(req).await
        }
        Ok(Ok(Some(_))) => {
            // Key exists but is disabled
            (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "API key is disabled",
                    "code": 403
                })),
            )
                .into_response()
        }
        _ => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "Invalid API key",
                "code": 401
            })),
        )
            .into_response(),
    }
}

fn extract_api_key(headers: &axum::http::HeaderMap) -> Option<String> {
    // Try Authorization: Bearer <key>
    if let Some(val) = headers.get("authorization") {
        if let Ok(s) = val.to_str() {
            if let Some(key) = s.strip_prefix("Bearer ") {
                return Some(key.to_string());
            }
        }
    }
    // Try X-API-Key: <key>
    if let Some(val) = headers.get("x-api-key") {
        if let Ok(s) = val.to_str() {
            return Some(s.to_string());
        }
    }
    None
}

// ─── /api/keys handlers ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateKeyRequest {
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct CreateKeyResponse {
    pub key_id: String,
    pub name: String,
    pub key: String,
    pub key_prefix: String,
    pub created_at: u64,
}

#[derive(Debug, Serialize)]
pub struct KeyInfoResponse {
    pub key_id: String,
    pub name: String,
    pub key_prefix: String,
    pub created_at: u64,
    pub last_used_at: u64,
    pub enabled: bool,
}

#[derive(Debug, Serialize)]
pub struct KeyListResponse {
    pub keys: Vec<KeyInfoResponse>,
    pub count: usize,
}

fn key_to_info(k: ApiKeyMeta) -> KeyInfoResponse {
    KeyInfoResponse {
        key_id: k.key_id,
        name: k.name,
        key_prefix: k.key_prefix,
        created_at: k.created_at,
        last_used_at: k.last_used_at,
        enabled: k.enabled,
    }
}

/// POST /api/keys  — Create a new API key
pub async fn create_key(
    State(engine): State<Arc<Engine>>,
    Json(req): Json<CreateKeyRequest>,
) -> Result<Json<CreateKeyResponse>, AppError> {
    if req.name.trim().is_empty() {
        return Err(AppError::bad_request("name must not be empty"));
    }

    // Generate a key: "jh_" + 48 hex chars derived from two independent UUIDs
    // UUID v4 uses OS CSPRNG, so two UUIDs give 256 bits of entropy
    let raw_bytes: [u8; 24] = {
        let id1 = uuid::Uuid::new_v4();
        let id2 = uuid::Uuid::new_v4();
        let mut sha = Sha256::new();
        sha.update(id1.as_bytes());
        sha.update(id2.as_bytes());
        sha.update(&now_secs().to_le_bytes());
        let digest = sha.finalize();
        let mut buf = [0u8; 24];
        buf.copy_from_slice(&digest[..24]);
        buf
    };

    let raw_key = format!("jh_{}", hex::encode(raw_bytes));
    let key_hash = hash_key(&raw_key);
    let key_prefix = format!("{}...", &raw_key[..10]);
    let key_id = uuid::Uuid::new_v4().simple().to_string();
    let now = now_secs();

    let meta = ApiKeyMeta {
        key_id: key_id.clone(),
        name: req.name.clone(),
        key_hash,
        key_prefix: key_prefix.clone(),
        created_at: now,
        last_used_at: 0,
        enabled: true,
    };

    tokio::task::spawn_blocking(move || engine.metadata().insert_api_key(&meta))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    Ok(Json(CreateKeyResponse {
        key_id,
        name: req.name,
        key: raw_key,
        key_prefix,
        created_at: now,
    }))
}

/// GET /api/keys  — List all API keys (never returns raw key values)
pub async fn list_keys(
    State(engine): State<Arc<Engine>>,
) -> Result<Json<KeyListResponse>, AppError> {
    let keys = tokio::task::spawn_blocking(move || engine.metadata().list_api_keys())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    let infos: Vec<KeyInfoResponse> = keys.into_iter().map(key_to_info).collect();
    let count = infos.len();
    Ok(Json(KeyListResponse { keys: infos, count }))
}

/// DELETE /api/keys/:key_id  — Revoke an API key
pub async fn delete_key(
    State(engine): State<Arc<Engine>>,
    axum::extract::Path(key_id): axum::extract::Path<String>,
) -> Result<StatusCode, AppError> {
    let removed = tokio::task::spawn_blocking(move || engine.metadata().delete_api_key(&key_id))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    if removed.is_none() {
        return Err(AppError {
            status: StatusCode::NOT_FOUND,
            message: format!("Key not found"),
        });
    }
    Ok(StatusCode::NO_CONTENT)
}
