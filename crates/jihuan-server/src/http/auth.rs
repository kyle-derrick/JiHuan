use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{FromRequestParts, Request, State},
    http::{header, request::Parts, HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use jihuan_core::{
    config::AuthConfig,
    metadata::types::{ApiKeyMeta, AuditResult},
    Engine,
};

use crate::http::audit;
use crate::http::files::AppError;

// ─── Session store (in-memory, cookie-backed) ─────────────────────────────────

/// Name of the session cookie set by `/api/auth/login` and cleared by
/// `/api/auth/logout`. Kept short so it's not confused with third-party cookies.
pub const SESSION_COOKIE: &str = "jh_session";

/// How long a login session is valid (7 days). Refreshed on every request
/// that successfully authenticates via the cookie.
const SESSION_TTL_SECS: u64 = 7 * 24 * 3600;

/// A resolved session principal. In v0.5.0-iam a session can be minted
/// from one of two sources:
///   * A User who logged in with username+password. `key_id` is the
///     synthetic id `"user:<username>"`; the corresponding `ApiKeyMeta`
///     is never persisted — it is reconstructed on every request from
///     the current `UserMeta` record.
///   * Legacy path: a raw API key was exchanged for a cookie. Here
///     `key_id` points at an actual `APIKEYS_TABLE` row. v0.5.0-iam no
///     longer mints these (login refuses raw keys) but outstanding
///     cookies from previous versions keep working until they expire.
#[derive(Clone, Debug)]
struct Session {
    key_id: String,
    expires_at: u64,
}

/// Compose the synthetic key_id for a session tied to a [`UserMeta`].
pub fn user_session_key_id(username: &str) -> String {
    format!("user:{}", username)
}

/// Extract the username from a user-session key_id, or `None` when the
/// id does not follow the `user:<name>` convention (pre-IAM SA sessions).
pub fn username_from_session(key_id: &str) -> Option<&str> {
    key_id.strip_prefix("user:")
}

/// In-memory session map. `String` = opaque random token stored in the cookie.
/// Value caches the `key_id` so the cookie path does not re-scan the store.
#[derive(Default)]
pub struct SessionStore {
    inner: RwLock<HashMap<String, Session>>,
}

impl SessionStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    async fn insert(&self, token: String, key_id: String) {
        let mut g = self.inner.write().await;
        g.insert(
            token,
            Session {
                key_id,
                expires_at: now_secs() + SESSION_TTL_SECS,
            },
        );
    }

    /// Look up a token; returns the associated `key_id` if valid, else `None`.
    /// Expired entries are purged opportunistically on access.
    async fn lookup(&self, token: &str) -> Option<String> {
        let now = now_secs();
        {
            let g = self.inner.read().await;
            if let Some(s) = g.get(token) {
                if s.expires_at > now {
                    return Some(s.key_id.clone());
                }
            } else {
                return None;
            }
        }
        // Fall-through: entry exists but is expired → upgrade to a write lock
        // and drop it.
        let mut g = self.inner.write().await;
        g.remove(token);
        None
    }

    async fn remove(&self, token: &str) {
        let mut g = self.inner.write().await;
        g.remove(token);
    }
}

/// Generate a 128-bit opaque session token (hex-encoded → 32 chars). Backed
/// by the OS CSPRNG via two independent UUID v4s.
fn new_session_token() -> String {
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    let mut out = String::with_capacity(64);
    out.push_str(&hex::encode(a.as_bytes()));
    out.push_str(&hex::encode(b.as_bytes()));
    out
}

/// Parse the `Cookie` header for a single named cookie. Returns the raw value
/// (percent-decoding is not required since our tokens are `[0-9a-f]+`).
fn cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    for pair in raw.split(';') {
        let pair = pair.trim();
        if let Some(rest) = pair.strip_prefix(&format!("{}=", name)) {
            return Some(rest);
        }
    }
    None
}

/// The authenticated principal attached to every request that passed the
/// auth middleware. Handlers use [`require_scope`] to enforce permission
/// constraints against [`ApiKeyMeta::scopes`].
#[derive(Clone, Debug)]
pub struct AuthedKey(pub ApiKeyMeta);

impl AuthedKey {
    pub fn has_scope(&self, scope: &str) -> bool {
        self.0
            .scopes
            .iter()
            .any(|s| s == scope || s == "admin" && (scope == "read" || scope == "write"))
    }
}

/// Extractor used by handlers that want to inspect the authenticated key.
/// Returns 401 when the middleware did not attach a principal, which can
/// only happen on exempt routes — a programming error for scoped handlers.
#[axum::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for AuthedKey {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<AuthedKey>()
            .cloned()
            .ok_or_else(|| AppError {
                status: StatusCode::UNAUTHORIZED,
                message: "not authenticated".to_string(),
            })
    }
}

/// Verify the authenticated key carries `required` (or an elevating scope).
/// Returns a 403 `AppError` when the check fails, with a helpful message.
pub fn require_scope(key: &AuthedKey, required: &str) -> Result<(), AppError> {
    if key.has_scope(required) {
        Ok(())
    } else {
        Err(AppError {
            status: StatusCode::FORBIDDEN,
            message: format!("insufficient scope: need '{}'", required),
        })
    }
}

/// Phase 7.2 — verify the caller is allowed to operate on the time
/// partition `partition_id`.
///
/// Semantics (matches `metadata/store.rs` multimap key encoding):
/// * `allowed_partitions == None`  ⇒ no restriction (legacy behaviour).
/// * `allowed_partitions == Some(list)` ⇒ `partition_id.to_string()`
///   must appear verbatim in `list`. An empty `list` therefore denies
///   every partition, including the current one — this is by design
///   (matches the MinIO-style "ServiceAccount pinned to no bucket"
///   model).
///
/// Used by upload (target partition = current), download / stat /
/// delete (target partition = file's own `partition_id`).
pub fn require_partition(key: &AuthedKey, partition_id: u64) -> Result<(), AppError> {
    let Some(ref allowed) = key.0.allowed_partitions else {
        return Ok(());
    };
    if allowed.iter().any(|p| p == &partition_id.to_string()) {
        Ok(())
    } else {
        Err(AppError {
            status: StatusCode::FORBIDDEN,
            message: format!(
                "partition {} is not in caller's allow-list",
                partition_id
            ),
        })
    }
}

/// Phase 7.2 — return `true` iff the caller is allowed to see files
/// in `partition_id`. Intended for silent filtering of list results
/// (where 403 on the whole request would be too noisy).
pub fn partition_visible(key: &AuthedKey, partition_id: u64) -> bool {
    match &key.0.allowed_partitions {
        None => true,
        Some(list) => list.iter().any(|p| p == &partition_id.to_string()),
    }
}

// ─── Auth state shared via axum Extension ─────────────────────────────────────

#[derive(Clone)]
pub struct AuthState {
    pub engine: Arc<Engine>,
    pub config: AuthConfig,
    pub sessions: Arc<SessionStore>,
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

/// Synthetic principal used when auth is disabled or the route is exempt.
/// Carries all three scopes so that `require_scope(..)` checks always pass.
/// The key_hash is deliberately empty — this record is never persisted.
pub(crate) fn disabled_auth_principal() -> ApiKeyMeta {
    ApiKeyMeta {
        key_id: "__auth_disabled__".to_string(),
        name: "auth-disabled".to_string(),
        key_hash: String::new(),
        key_prefix: String::new(),
        created_at: 0,
        last_used_at: 0,
        enabled: true,
        scopes: vec!["read".to_string(), "write".to_string(), "admin".to_string()],
        parent_user: String::new(),
        allowed_partitions: None,
        expires_at: None,
    }
}

// ─── Auth middleware ───────────────────────────────────────────────────────────

pub async fn auth_middleware(
    State(auth): State<AuthState>,
    mut req: Request,
    next: Next,
) -> Response {
    // When auth is globally disabled, inject a synthetic full-scope principal
    // so that downstream `AuthedKey` extractors / `require_scope` calls see a
    // caller. Without this, every scoped handler returns 401 even though
    // authentication was turned off by configuration.
    if !auth.config.enabled {
        req.extensions_mut().insert(AuthedKey(disabled_auth_principal()));
        return next.run(req).await;
    }

    let path = req.uri().path().to_string();

    // Hard-coded always-exempt routes. These cannot be removed via config
    // because doing so would make the system unrecoverable:
    //   • `/api/auth/login` is how a browser obtains a session cookie — it
    //     must be reachable with no credentials, by definition.
    // Other endpoints (e.g. `/api/status`, `/ui/*`) stay configurable via
    // `auth.exempt_routes` below.
    const ALWAYS_EXEMPT: &[&str] = &["/api/auth/login"];
    let is_exempt = ALWAYS_EXEMPT.iter().any(|p| path.starts_with(p))
        || auth
            .config
            .exempt_routes
            .iter()
            .any(|p| path.starts_with(p.as_str()));
    if is_exempt {
        // Exempt routes can still be reached by handlers that optionally
        // inspect `AuthedKey` — give them a benign default.
        req.extensions_mut()
            .insert(AuthedKey(disabled_auth_principal()));
        return next.run(req).await;
    }

    // Resolve credential → `key_id`. Two accepted channels:
    //   1. `Authorization: Bearer <raw>` / `X-API-Key: <raw>` — hash and look
    //      up in the persistent key table. This is what CLIs and server-to-
    //      server callers use.
    //   2. `Cookie: jh_session=<token>` — opaque session token minted by
    //      `POST /api/auth/login`. This is what the UI uses so the raw API
    //      key is never exposed to JavaScript.
    let resolved_key_id: Option<String> = match extract_api_key(req.headers()) {
        Some(raw_key) => {
            let key_hash = hash_key(&raw_key);
            let engine = auth.engine.clone();
            match tokio::task::spawn_blocking(move || {
                engine.metadata().get_api_key_by_hash(&key_hash)
            })
            .await
            {
                Ok(Ok(Some(k))) if k.enabled => Some(k.key_id),
                Ok(Ok(Some(_))) => {
                    return (
                        StatusCode::FORBIDDEN,
                        Json(serde_json::json!({
                            "error": "API key is disabled",
                            "code": 403
                        })),
                    )
                        .into_response();
                }
                _ => None,
            }
        }
        None => {
            // No API key header — try the session cookie.
            match cookie_value(req.headers(), SESSION_COOKIE) {
                Some(tok) => auth.sessions.lookup(tok).await,
                None => None,
            }
        }
    };

    let key_id = match resolved_key_id {
        Some(id) => id,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({
                    "error": "Missing or invalid credentials. Provide one of: Authorization: Bearer <key>, X-API-Key: <key>, or a valid jh_session cookie.",
                    "code": 401
                })),
            )
                .into_response();
        }
    };

    // Resolve the principal. Two cases:
    //   * `user:<username>` → reconstruct an ephemeral `ApiKeyMeta` from
    //     the current `UserMeta` row. This is what cookie logins produce
    //     in v0.5.0-iam.
    //   * anything else → real `APIKEYS_TABLE` row for a ServiceAccount
    //     (or a pre-IAM key).
    if let Some(username) = username_from_session(&key_id).map(str::to_string) {
        let engine = auth.engine.clone();
        let user_res = tokio::task::spawn_blocking(move || {
            engine.metadata().get_user(&username)
        })
        .await;
        return match user_res {
            Ok(Ok(Some(u))) if u.enabled => {
                let meta = user_to_principal(&u);
                req.extensions_mut().insert(AuthedKey(meta));
                next.run(req).await
            }
            Ok(Ok(Some(_))) => (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "user is disabled",
                    "code": 403
                })),
            )
                .into_response(),
            _ => (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({
                    "error": "session user no longer exists",
                    "code": 401
                })),
            )
                .into_response(),
        };
    }

    // Fetch the full `ApiKeyMeta` for scope enforcement. The header path
    // already retrieved it once; re-reading here keeps the code uniform and
    // also picks up the latest `enabled` flag for cookie sessions.
    let engine = auth.engine.clone();
    let key_id_for_task = key_id.clone();
    let meta = tokio::task::spawn_blocking(move || {
        engine.metadata().get_api_key(&key_id_for_task)
    })
    .await;

    match meta {
        Ok(Ok(Some(key))) if key.enabled => {
            // v0.5.0-iam: enforce SA expiry.
            if let Some(exp) = key.expires_at {
                if now_secs() >= exp {
                    return (
                        StatusCode::UNAUTHORIZED,
                        Json(serde_json::json!({
                            "error": "service account expired",
                            "code": 401
                        })),
                    )
                        .into_response();
                }
            }
            // Touch last_used_at asynchronously (fire-and-forget)
            let engine2 = auth.engine.clone();
            let key_id2 = key.key_id.clone();
            tokio::task::spawn_blocking(move || {
                let _ = engine2.metadata().touch_api_key(&key_id2, now_secs());
            });
            req.extensions_mut().insert(AuthedKey(key));
            next.run(req).await
        }
        Ok(Ok(Some(_))) => (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "API key is disabled",
                "code": 403
            })),
        )
            .into_response(),
        _ => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "error": "Invalid or expired credentials",
                "code": 401
            })),
        )
            .into_response(),
    }
}

/// Build an ephemeral `ApiKeyMeta` principal from a `UserMeta`, used by
/// the session-cookie path so `AuthedKey`/`require_scope` can treat User
/// logins and ServiceAccount bearer tokens uniformly.
fn user_to_principal(u: &jihuan_core::metadata::types::UserMeta) -> ApiKeyMeta {
    ApiKeyMeta {
        key_id: user_session_key_id(&u.username),
        name: u.username.clone(),
        key_hash: String::new(),
        key_prefix: String::new(),
        created_at: u.created_at,
        last_used_at: 0,
        enabled: u.enabled,
        scopes: u.scopes.clone(),
        parent_user: u.username.clone(),
        allowed_partitions: u.allowed_partitions.clone(),
        expires_at: None,
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
    /// Optional scope list. Defaults to `["read", "write"]` — admin-level
    /// keys must be requested explicitly by callers that already hold admin.
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct CreateKeyResponse {
    pub key_id: String,
    pub name: String,
    pub key: String,
    pub key_prefix: String,
    pub created_at: u64,
    pub scopes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct KeyInfoResponse {
    pub key_id: String,
    pub name: String,
    pub key_prefix: String,
    pub created_at: u64,
    pub last_used_at: u64,
    pub enabled: bool,
    pub scopes: Vec<String>,
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
        scopes: k.scopes,
    }
}

/// Normalize and validate requested scopes. Unknown scope strings are rejected.
fn sanitize_scopes(requested: Option<Vec<String>>) -> Result<Vec<String>, AppError> {
    let scopes = requested.unwrap_or_else(|| vec!["read".to_string(), "write".to_string()]);
    let allowed = ["read", "write", "admin"];
    for s in &scopes {
        if !allowed.contains(&s.as_str()) {
            return Err(AppError::bad_request(format!(
                "unknown scope '{}'; allowed: {:?}",
                s, allowed
            )));
        }
    }
    if scopes.is_empty() {
        return Err(AppError::bad_request("scopes must not be empty"));
    }
    Ok(scopes)
}

/// POST /api/keys  — Create a new API key. Caller must hold the `admin` scope.
pub async fn create_key(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Json(req): Json<CreateKeyRequest>,
) -> Result<Json<CreateKeyResponse>, AppError> {
    require_scope(&caller, "admin")?;
    if req.name.trim().is_empty() {
        return Err(AppError::bad_request("name must not be empty"));
    }
    let scopes = sanitize_scopes(req.scopes)?;

    let (raw_key, meta, now) = build_new_key(&req.name, scopes.clone());
    let key_id = meta.key_id.clone();
    let key_prefix = meta.key_prefix.clone();

    let engine_for_audit = engine.clone();
    tokio::task::spawn_blocking(move || engine.metadata().insert_api_key(&meta))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    audit::record(
        engine_for_audit,
        Some(caller.0.key_id.clone()),
        None,
        "key.create",
        Some(key_id.clone()),
        AuditResult::Ok,
        Some(200),
    );

    Ok(Json(CreateKeyResponse {
        key_id,
        name: req.name,
        key: raw_key,
        key_prefix,
        created_at: now,
        scopes,
    }))
}

/// Helper shared by the HTTP handler and the startup bootstrap path. Returns
/// `(raw_key, meta, created_at)`. The raw key is the only place the plaintext
/// ever leaves this function — callers must surface it to the user and then
/// drop it.
pub fn build_new_key(name: &str, scopes: Vec<String>) -> (String, ApiKeyMeta, u64) {
    // Generate a key: "jh_" + 48 hex chars derived from two independent UUIDs.
    // UUID v4 uses OS CSPRNG, so two UUIDs give 256 bits of entropy.
    let raw_bytes: [u8; 24] = {
        let id1 = uuid::Uuid::new_v4();
        let id2 = uuid::Uuid::new_v4();
        let mut sha = Sha256::new();
        sha.update(id1.as_bytes());
        sha.update(id2.as_bytes());
        sha.update(now_secs().to_le_bytes());
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
        key_id,
        name: name.to_string(),
        key_hash,
        key_prefix,
        created_at: now,
        last_used_at: 0,
        enabled: true,
        scopes,
        parent_user: String::new(),
        allowed_partitions: None,
        expires_at: None,
    };
    (raw_key, meta, now)
}

/// GET /api/keys  — List all API keys (never returns raw key values). admin only.
pub async fn list_keys(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
) -> Result<Json<KeyListResponse>, AppError> {
    require_scope(&caller, "admin")?;
    let keys = tokio::task::spawn_blocking(move || engine.metadata().list_api_keys())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    let infos: Vec<KeyInfoResponse> = keys.into_iter().map(key_to_info).collect();
    let count = infos.len();
    Ok(Json(KeyListResponse { keys: infos, count }))
}

/// DELETE /api/keys/:key_id  — Revoke an API key. admin only.
///
/// v0.4.5 safety interlocks:
///
/// 1. Callers cannot delete their **own** key mid-session — that would
///    instantly 401 every follow-up request and force a restart to
///    recover via the bootstrap path.
/// 2. Callers cannot delete the **last admin** key — the system would
///    have no way to mint new keys and the UI admin views would all go
///    dark. This mirrors the "can't remove the last root user" rule
///    from most auth systems.
pub async fn delete_key(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    axum::extract::Path(key_id): axum::extract::Path<String>,
) -> Result<StatusCode, AppError> {
    require_scope(&caller, "admin")?;

    // ── Interlock #1: refuse self-delete ────────────────────────────────
    if caller.0.key_id == key_id {
        audit::record(
            engine.clone(),
            Some(caller.0.key_id.clone()),
            None,
            "key.delete",
            Some(key_id.clone()),
            AuditResult::Denied {
                reason: "cannot delete your own key".to_string(),
            },
            Some(409),
        );
        return Err(AppError {
            status: StatusCode::CONFLICT,
            message: "Refusing to delete the key that issued this request. Use another admin key, or delete this key from the CLI after revoking scopes.".to_string(),
        });
    }

    // ── Interlock #2: refuse deletion of the last admin key ────────────
    let engine_probe = engine.clone();
    let key_id_probe = key_id.clone();
    let is_sole_admin = tokio::task::spawn_blocking(move || {
        let keys = engine_probe.metadata().list_api_keys()?;
        let admins: Vec<_> = keys
            .iter()
            .filter(|k| k.scopes.iter().any(|s| s == "admin"))
            .collect();
        // "Last admin" = exactly one admin exists, and it is the target.
        let sole = admins.len() == 1 && admins[0].key_id == key_id_probe;
        Ok::<bool, jihuan_core::JiHuanError>(sole)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    if is_sole_admin {
        audit::record(
            engine.clone(),
            Some(caller.0.key_id.clone()),
            None,
            "key.delete",
            Some(key_id.clone()),
            AuditResult::Denied {
                reason: "would remove the last admin key".to_string(),
            },
            Some(409),
        );
        return Err(AppError {
            status: StatusCode::CONFLICT,
            message: "Refusing to delete the last admin key. Create another admin key first.".to_string(),
        });
    }

    let engine_task = engine.clone();
    let key_id_task = key_id.clone();
    let removed = tokio::task::spawn_blocking(move || engine_task.metadata().delete_api_key(&key_id_task))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    if removed.is_none() {
        audit::record(
            engine,
            Some(caller.0.key_id.clone()),
            None,
            "key.delete",
            Some(key_id),
            AuditResult::Denied {
                reason: "not found".to_string(),
            },
            Some(404),
        );
        return Err(AppError {
            status: StatusCode::NOT_FOUND,
            message: "Key not found".to_string(),
        });
    }
    audit::record(
        engine,
        Some(caller.0.key_id.clone()),
        None,
        "key.delete",
        Some(key_id),
        AuditResult::Ok,
        Some(204),
    );
    Ok(StatusCode::NO_CONTENT)
}

// ─── /api/auth/{login,logout,me} ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub key_id: String,
    pub name: String,
    pub scopes: Vec<String>,
    /// Session lifetime, in seconds, echoed back for UI display.
    pub expires_in: u64,
}

/// POST /api/auth/login — v0.5.0-iam: exchange a username+password for a
/// session cookie.
///
/// Accepts `{ "username": "...", "password": "..." }`. On success sets an
/// `HttpOnly`, `SameSite=Strict` cookie `jh_session=<token>` and returns
/// the authenticated user's identity. Raw API keys are no longer accepted
/// here — service-account callers must send `Authorization: Bearer` on
/// every request instead.
pub async fn login(
    State(auth): State<AuthState>,
    headers: HeaderMap,
    Json(req): Json<LoginRequest>,
) -> Result<Response, AppError> {
    let username = req.username.trim().to_string();
    let password = req.password;
    if username.is_empty() || password.is_empty() {
        return Err(AppError::bad_request(
            "username and password must both be provided",
        ));
    }

    // Fetch the user record on the blocking pool.
    let engine = auth.engine.clone();
    let username_for_task = username.clone();
    let user_opt = tokio::task::spawn_blocking(move || {
        engine.metadata().get_user(&username_for_task)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;

    let actor_ip = audit::client_ip(&headers, None);
    let user = match user_opt {
        Some(u) if u.enabled => u,
        Some(_) => {
            audit::record(
                auth.engine.clone(),
                None,
                actor_ip.clone(),
                "auth.login_failed",
                Some(username.clone()),
                AuditResult::Denied {
                    reason: "user disabled".to_string(),
                },
                Some(403),
            );
            return Err(AppError {
                status: StatusCode::FORBIDDEN,
                message: "user is disabled".to_string(),
            });
        }
        None => {
            audit::record(
                auth.engine.clone(),
                None,
                actor_ip.clone(),
                "auth.login_failed",
                Some(username.clone()),
                AuditResult::Denied {
                    reason: "unknown user".to_string(),
                },
                Some(401),
            );
            // Constant-ish 401 regardless of whether the user existed to
            // avoid username-enumeration oracles.
            return Err(AppError {
                status: StatusCode::UNAUTHORIZED,
                message: "invalid username or password".to_string(),
            });
        }
    };

    // Constant-time-ish password check. SHA-256 comparisons against 32
    // bytes are short enough that the side-channel risk is negligible in
    // the single-node admin UI threat model.
    let expected = crate::http::iam::hash_password(&password, &user.salt);
    if expected != user.password_hash {
        audit::record(
            auth.engine.clone(),
            None,
            actor_ip.clone(),
            "auth.login_failed",
            Some(username.clone()),
            AuditResult::Denied {
                reason: "bad password".to_string(),
            },
            Some(401),
        );
        return Err(AppError {
            status: StatusCode::UNAUTHORIZED,
            message: "invalid username or password".to_string(),
        });
    }

    // Mint + persist the session. `user:<username>` is the synthetic
    // key_id the middleware recognises and re-resolves to a principal
    // built from the current UserMeta row.
    let session_id = user_session_key_id(&username);
    let token = new_session_token();
    auth.sessions.insert(token.clone(), session_id.clone()).await;

    audit::record(
        auth.engine.clone(),
        Some(session_id.clone()),
        actor_ip,
        "auth.login",
        Some(username.clone()),
        AuditResult::Ok,
        Some(200),
    );
    let meta = ApiKeyMeta {
        key_id: session_id,
        name: user.username.clone(),
        key_hash: String::new(),
        key_prefix: String::new(),
        created_at: user.created_at,
        last_used_at: 0,
        enabled: true,
        scopes: user.scopes.clone(),
        parent_user: user.username.clone(),
        allowed_partitions: user.allowed_partitions.clone(),
        expires_at: None,
    };

    // Build the Set-Cookie header. `Secure` is gated on
    // `auth.cookie_secure` so plain-HTTP dev deployments still work
    // (browsers drop `Secure` cookies over HTTP). Operators behind TLS
    // must flip the flag on, otherwise a MITM could strip HTTPS and
    // exfiltrate the session token.
    let secure_attr = if auth.config.cookie_secure { "; Secure" } else { "" };
    let cookie = format!(
        "{name}={val}; Path=/; Max-Age={ttl}; HttpOnly; SameSite=Strict{sec}",
        name = SESSION_COOKIE,
        val = token,
        ttl = SESSION_TTL_SECS,
        sec = secure_attr,
    );

    let body = Json(LoginResponse {
        key_id: meta.key_id,
        name: meta.name,
        scopes: meta.scopes,
        expires_in: SESSION_TTL_SECS,
    });

    let mut resp = body.into_response();
    resp.headers_mut().insert(
        header::SET_COOKIE,
        cookie.parse().expect("cookie is valid ASCII"),
    );
    Ok(resp)
}

/// POST /api/auth/logout — invalidate the current session and clear the cookie.
/// Idempotent: always returns 204 even if there was no valid session.
pub async fn logout(
    State(auth): State<AuthState>,
    req_headers: HeaderMap,
) -> Response {
    let mut actor_key_id: Option<String> = None;
    if let Some(tok) = cookie_value(&req_headers, SESSION_COOKIE) {
        actor_key_id = auth.sessions.lookup(tok).await;
        auth.sessions.remove(tok).await;
    }
    audit::record(
        auth.engine.clone(),
        actor_key_id,
        audit::client_ip(&req_headers, None),
        "auth.logout",
        None,
        AuditResult::Ok,
        Some(204),
    );
    // Overwrite the cookie with an immediately-expired value. Match the
    // `Secure` attribute used at login so browsers don't treat the
    // clearing cookie as a different one and skip it.
    let secure_attr = if auth.config.cookie_secure { "; Secure" } else { "" };
    let clear = format!(
        "{name}=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict{sec}",
        name = SESSION_COOKIE,
        sec = secure_attr,
    );
    let mut resp = StatusCode::NO_CONTENT.into_response();
    resp.headers_mut()
        .insert(header::SET_COOKIE, clear.parse().unwrap());
    resp
}

/// GET /api/auth/me — return the caller's identity, or 401 if unauthenticated.
/// The UI uses this at mount time to decide whether to redirect to /ui/login.
pub async fn me(caller: AuthedKey) -> Json<KeyInfoResponse> {
    Json(key_to_info(caller.0))
}

#[derive(Debug, Deserialize)]
pub struct ChangePasswordRequest {
    /// The new plaintext password. Becomes the caller's new API key — any
    /// string works, but the UI encourages something memorable.
    pub new_password: String,
}

/// POST /api/auth/change-password — rotate the caller's own password.
///
/// v0.5.0-iam: when the caller is a User (session cookie), replace the
/// hash+salt on the corresponding `UserMeta` row. Legacy SA-backed
/// sessions cannot change their "password" — SAs rotate their secret
/// via the admin API instead — and return 400.
///
/// Requires the caller to already be authenticated; no extra scope check,
/// since changing your own password is a baseline capability.
pub async fn change_password(
    State(auth): State<AuthState>,
    caller: AuthedKey,
    Json(req): Json<ChangePasswordRequest>,
) -> Result<StatusCode, AppError> {
    let pw = req.new_password;
    if pw.chars().count() < 8 {
        return Err(AppError::bad_request(
            "new_password must be at least 8 characters",
        ));
    }
    if pw.chars().any(|c| c.is_control()) {
        return Err(AppError::bad_request(
            "new_password must not contain control characters",
        ));
    }

    let username = match username_from_session(&caller.0.key_id).map(str::to_string) {
        Some(u) => u,
        None => {
            return Err(AppError::bad_request(
                "service accounts cannot change a password; rotate the key via the admin API",
            ));
        }
    };

    let engine = auth.engine.clone();
    let username_probe = username.clone();
    let user_opt = tokio::task::spawn_blocking(move || {
        engine.metadata().get_user(&username_probe)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    let mut user = match user_opt {
        Some(u) => u,
        None => {
            return Err(AppError {
                status: StatusCode::NOT_FOUND,
                message: "authenticated user no longer exists".to_string(),
            })
        }
    };

    let salt = crate::http::iam::random_salt();
    user.password_hash = crate::http::iam::hash_password(&pw, &salt);
    user.salt = salt;

    let engine_task = auth.engine.clone();
    tokio::task::spawn_blocking(move || engine_task.metadata().insert_user(&user))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    tracing::info!(%username, "User password rotated via change-password");
    audit::record(
        auth.engine.clone(),
        Some(caller.0.key_id.clone()),
        None,
        "auth.change_password",
        Some(username),
        AuditResult::Ok,
        Some(204),
    );
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jihuan_core::metadata::types::ApiKeyMeta;

    fn make_key(allowed: Option<Vec<String>>) -> AuthedKey {
        AuthedKey(ApiKeyMeta {
            key_id: "k".into(),
            name: "n".into(),
            key_hash: "h".into(),
            key_prefix: "p".into(),
            created_at: 0,
            last_used_at: 0,
            enabled: true,
            scopes: vec!["read".into(), "write".into()],
            parent_user: String::new(),
            allowed_partitions: allowed,
            expires_at: None,
        })
    }

    #[test]
    fn test_require_partition_none_is_unrestricted() {
        // Phase 7.2: legacy pre-IAM keys have allowed_partitions=None
        // and must pass through unchanged — this is the backwards-
        // compatible fast path.
        let key = make_key(None);
        assert!(require_partition(&key, 0).is_ok());
        assert!(require_partition(&key, 42).is_ok());
        assert!(require_partition(&key, u64::MAX).is_ok());
    }

    #[test]
    fn test_require_partition_allows_listed_ids() {
        let key = make_key(Some(vec!["42".into(), "1000".into()]));
        assert!(require_partition(&key, 42).is_ok());
        assert!(require_partition(&key, 1000).is_ok());
    }

    #[test]
    fn test_require_partition_rejects_unlisted() {
        let key = make_key(Some(vec!["42".into()]));
        let err = require_partition(&key, 7).unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN);
        assert!(err.message.contains("partition 7"), "got: {}", err.message);
    }

    #[test]
    fn test_require_partition_empty_vec_denies_everything() {
        // Explicit empty list = MinIO-style "SA pinned to no bucket".
        // Must still deny — not a bug, a documented feature.
        let key = make_key(Some(Vec::new()));
        assert!(require_partition(&key, 0).is_err());
        assert!(require_partition(&key, 42).is_err());
    }

    #[test]
    fn test_partition_visible_agrees_with_require() {
        for allowed in [None, Some(vec![]), Some(vec!["5".into(), "10".into()])] {
            let key = make_key(allowed);
            for pid in 0u64..20 {
                assert_eq!(
                    partition_visible(&key, pid),
                    require_partition(&key, pid).is_ok(),
                    "pid={pid} allowed={:?}",
                    key.0.allowed_partitions
                );
            }
        }
    }
}
