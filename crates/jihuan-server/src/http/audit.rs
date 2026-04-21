//! HTTP-side audit logging helpers (Phase 2.6).
//!
//! Provides a single `record(...)` entry point that is cheap to call from
//! handlers and dispatches the synchronous redb write onto a blocking task.
//! Failures are logged but never propagated: the audit log is a
//! best-effort sidecar, not a transactional co-commit — we never want a
//! storage hiccup in the audit table to fail an otherwise-good user
//! request.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use serde::{Deserialize, Serialize};

use jihuan_core::{
    metadata::types::{AuditEvent, AuditResult},
    Engine,
};

use crate::http::auth::{require_scope, AuthedKey};
use crate::http::files::AppError;

/// Record an audit event asynchronously. Never panics, never propagates
/// storage errors to the caller. The `engine` handle is cloned cheaply; the
/// actual redb write happens on a blocking pool.
pub fn record(
    engine: Arc<Engine>,
    actor_key_id: Option<String>,
    actor_ip: Option<String>,
    action: impl Into<String>,
    target: Option<String>,
    result: AuditResult,
    http_status: Option<u16>,
) {
    let ev = AuditEvent {
        ts: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default(),
        actor_key_id,
        actor_ip,
        action: action.into(),
        target,
        result,
        http_status,
    };
    tokio::task::spawn_blocking(move || {
        if let Err(e) = engine.metadata().insert_audit_event(&ev) {
            tracing::warn!(error = %e, action = %ev.action, "audit insert failed");
        }
    });
}

/// Best-effort IP extractor: prefers `X-Forwarded-For` (first value), falls
/// back to the tcp socket. Returns `None` if neither is usable.
pub fn client_ip(headers: &HeaderMap, sock: Option<std::net::SocketAddr>) -> Option<String> {
    if let Some(v) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(first) = v.split(',').next() {
            let ip = first.trim();
            if !ip.is_empty() {
                return Some(ip.to_string());
            }
        }
    }
    sock.map(|s| s.ip().to_string())
}

// ─── Admin endpoint: GET /api/admin/audit ─────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct AuditQuery {
    pub since: Option<u64>,
    pub until: Option<u64>,
    pub actor: Option<String>,
    pub action: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct AuditListResponse {
    pub events: Vec<AuditEvent>,
    pub count: usize,
}

/// `GET /api/admin/audit` — paginate the audit log, newest first.
///
/// Query parameters:
///   * `since` / `until`   — Unix seconds bounds (inclusive).
///   * `actor`             — exact match against `actor_key_id`.
///   * `action`            — prefix match (e.g. `auth.` returns all auth events).
///   * `limit`             — cap (default 200, max 1000).
pub async fn list_audit(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Query(q): Query<AuditQuery>,
) -> Result<Json<AuditListResponse>, AppError> {
    require_scope(&caller, "admin")?;
    let limit = q.limit.unwrap_or(200).min(1000);
    let engine2 = engine.clone();
    let events = tokio::task::spawn_blocking(move || {
        engine2.metadata().list_audit_events(
            q.since,
            q.until,
            q.actor.as_deref(),
            q.action.as_deref(),
            limit,
        )
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    let count = events.len();
    Ok(Json(AuditListResponse { events, count }))
}

/// Extract the caller's socket addr (when the route was registered with
/// `.into_make_service_with_connect_info::<SocketAddr>()`). Returns `None`
/// if unavailable — callers fall back to header extraction.
#[allow(dead_code)]
pub fn caller_sock(conn: Option<&ConnectInfo<std::net::SocketAddr>>) -> Option<std::net::SocketAddr> {
    conn.map(|c| c.0)
}

/// Convenience: convert an `AppError` into an audit `Denied`/`Error`
/// classification. 4xx → Denied, 5xx → Error.
#[allow(dead_code)] // reserved for the forthcoming generic audit middleware
pub fn classify(err: &AppError) -> AuditResult {
    let code = err.status.as_u16();
    if (400..500).contains(&code) {
        AuditResult::Denied {
            reason: err.message.clone(),
        }
    } else {
        AuditResult::Error {
            message: err.message.clone(),
        }
    }
}

/// Convenience wrapper when the caller wants to translate a bare status code.
#[allow(dead_code)]
pub fn status_to_result(code: StatusCode) -> AuditResult {
    let c = code.as_u16();
    if c < 400 {
        AuditResult::Ok
    } else if c < 500 {
        AuditResult::Denied {
            reason: code.canonical_reason().unwrap_or("denied").to_string(),
        }
    } else {
        AuditResult::Error {
            message: code.canonical_reason().unwrap_or("error").to_string(),
        }
    }
}
