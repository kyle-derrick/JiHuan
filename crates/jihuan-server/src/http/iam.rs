//! IAM HTTP handlers for v0.5.0-iam Phase 2B.
//!
//! Covers:
//!   * User CRUD (`/api/admin/users`, `/api/admin/users/:u`)
//!   * Password reset (`/api/admin/users/:u/password`)
//!   * ServiceAccount CRUD scoped to a user
//!     (`/api/admin/users/:u/sa`, `/api/admin/users/:u/sa/:key_id`)
//!   * SA secret rotation (`/api/admin/users/:u/sa/:key_id/rotate`)
//!
//! Password authentication (the login endpoint itself) lives next door in
//! [`crate::http::auth`]. This module focuses on the *administration* of
//! identities, not the runtime cookie handshake.
//!
//! See also: [`root_bootstrap`] which is invoked once from `main.rs` at
//! startup to ensure a usable admin credential always exists.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use jihuan_core::{
    metadata::types::{ApiKeyMeta, AuditResult, UserMeta},
    Engine,
};

use crate::http::audit;
use crate::http::auth::{build_new_key, require_scope, AuthedKey};
use crate::http::files::AppError;

// ─── Password hashing ─────────────────────────────────────────────────────────

/// Compute `SHA-256(password || salt)` — matches the scheme committed to in
/// `UserMeta.password_hash`. We deliberately do *not* use bcrypt/argon2
/// here because the threat model for a single-node admin UI does not
/// justify the dependency weight and the salt+SHA-256 combo defeats
/// rainbow-table attacks on the stored digest. Rotate this function's
/// body if a future phase needs a slower KDF.
pub fn hash_password(password: &str, salt: &[u8; 16]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(password.as_bytes());
    h.update(salt);
    let d = h.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&d);
    out
}

pub fn random_salt() -> [u8; 16] {
    // uuid v4 is backed by the OS CSPRNG — good enough for a per-user salt.
    let u = uuid::Uuid::new_v4();
    let mut out = [0u8; 16];
    out.copy_from_slice(u.as_bytes());
    out
}

/// Generate a user-friendly random password — 24 hex chars from the OS
/// CSPRNG via two concatenated UUID v4s. Used by the root-bootstrap path
/// when `JIHUAN_ROOT_PASSWORD` is not supplied.
pub fn random_password() -> String {
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    let mut buf = Vec::with_capacity(32);
    buf.extend_from_slice(a.as_bytes());
    buf.extend_from_slice(b.as_bytes());
    // Trim to 12 bytes → 24 hex chars, plenty of entropy and easy to
    // type out of a terminal once.
    hex::encode(&buf[..12])
}

pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ─── Username validation ──────────────────────────────────────────────────────

/// Accept `[a-zA-Z0-9_.-]{2,64}`. Matches the MinIO-style policy called
/// out in the roadmap and keeps usernames safe to drop into audit rows,
/// URLs, and log lines without escaping.
pub fn validate_username(u: &str) -> Result<(), AppError> {
    let len = u.len();
    if !(2..=64).contains(&len) {
        return Err(AppError::bad_request(
            "username must be 2-64 bytes long",
        ));
    }
    if !u
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    {
        return Err(AppError::bad_request(
            "username may only contain [a-zA-Z0-9_.-]",
        ));
    }
    Ok(())
}

fn validate_password(pw: &str) -> Result<(), AppError> {
    if pw.chars().count() < 8 {
        return Err(AppError::bad_request(
            "password must be at least 8 characters",
        ));
    }
    if pw.chars().any(|c| c.is_control()) {
        return Err(AppError::bad_request(
            "password must not contain control characters",
        ));
    }
    Ok(())
}

fn validate_scopes(scopes: &[String]) -> Result<(), AppError> {
    let allowed = ["read", "write", "admin", "operator", "viewer"];
    if scopes.is_empty() {
        return Err(AppError::bad_request("scopes must not be empty"));
    }
    for s in scopes {
        if !allowed.contains(&s.as_str()) {
            return Err(AppError::bad_request(format!(
                "unknown scope '{}'; allowed: {:?}",
                s, allowed
            )));
        }
    }
    Ok(())
}

/// Expand shorthand scopes (`operator`, `viewer`) to canonical atoms
/// (`read`/`write`/`admin`) so the `require_scope` checks remain uniform.
pub fn expand_scopes(scopes: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for s in scopes {
        match s.as_str() {
            "operator" => {
                out.push("read".into());
                out.push("write".into());
            }
            "viewer" => out.push("read".into()),
            _ => out.push(s.clone()),
        }
    }
    out.sort();
    out.dedup();
    out
}

// ─── Root bootstrap ───────────────────────────────────────────────────────────

/// Bootstrap error returned by [`root_bootstrap`] so the caller can
/// surface a friendly message on stderr and abort startup.
pub enum BootstrapOutcome {
    /// A fresh root user was created. The raw password must be printed
    /// *once* to stderr so the operator can save it.
    Created {
        username: String,
        raw_password: String,
    },
    /// The root user already existed — nothing was done.
    Existing,
    /// Migration required: pre-IAM apikeys exist but no users. Caller
    /// should print the embedded message and abort.
    NeedsMigration(String),
}

pub fn root_bootstrap(engine: &Engine, data_dir: &std::path::Path) -> anyhow::Result<BootstrapOutcome> {
    let meta = engine.metadata();

    // If any users exist, we're already past bootstrap.
    let users = meta.list_users()?;
    if !users.is_empty() {
        return Ok(BootstrapOutcome::Existing);
    }

    // Hard-fail when pre-IAM records exist but the users table is empty.
    // This matches the v1 roadmap migration plan: dev-phase only, operator
    // must wipe the data directory and restart.
    //
    // We tolerate a decode error from `list_api_keys` as well: bincode is
    // positional, so a pre-v0.5.0 ApiKeyMeta row missing the new
    // `parent_user`/`allowed_partitions`/`expires_at` fields cannot be
    // round-tripped. That's precisely the migration case we need to flag.
    let keys_count = match meta.list_api_keys() {
        Ok(keys) => keys.len(),
        Err(e) => {
            let msg = format!(
                "identity schema changed (v0.4.x → v0.5.0-iam).\n\n\
                 Failed to decode existing APIKEYS rows: {}\n\n\
                 RESOLUTION (development only — destroys data):\n  \
                 1. Stop jihuan-server.\n  \
                 2. Delete the data directory:  rm -rf {}\n  \
                 3. Set JIHUAN_ROOT_PASSWORD (optional; else an auto-generated one is printed).\n  \
                 4. Restart jihuan-server.",
                e,
                data_dir.display()
            );
            return Ok(BootstrapOutcome::NeedsMigration(msg));
        }
    };
    if keys_count > 0 {
        let keys = keys_count;
        let msg = format!(
            "identity schema changed (v0.4.x → v0.5.0-iam).\n\n\
             The server now uses User + ServiceAccount identities instead of\n\
             standalone API keys for interactive login. Existing apikey records\n\
             ({} found) are not auto-migrated in this dev build.\n\n\
             RESOLUTION (development only — destroys data):\n  \
             1. Stop jihuan-server.\n  \
             2. Delete the data directory:  rm -rf {}\n  \
             3. Set JIHUAN_ROOT_PASSWORD (optional; else an auto-generated one is printed).\n  \
             4. Restart jihuan-server.",
            keys,
            data_dir.display()
        );
        return Ok(BootstrapOutcome::NeedsMigration(msg));
    }

    // Create the root user. Username defaults to "root"; password defaults
    // to a generated value printed once.
    let username = std::env::var("JIHUAN_ROOT_USER").unwrap_or_else(|_| "root".to_string());
    validate_username(&username).map_err(|e| anyhow::anyhow!(e.message))?;
    let (raw_password, generated) = match std::env::var("JIHUAN_ROOT_PASSWORD") {
        Ok(p) if !p.is_empty() => (p, false),
        _ => (random_password(), true),
    };
    validate_password(&raw_password).map_err(|e| anyhow::anyhow!(e.message))?;
    let salt = random_salt();
    let user = UserMeta {
        username: username.clone(),
        password_hash: hash_password(&raw_password, &salt),
        salt,
        scopes: vec!["admin".to_string()],
        allowed_partitions: None,
        created_at: now_secs(),
        enabled: true,
        is_root: true,
    };
    meta.insert_user(&user)?;

    // Drop a sentinel file next to the metadata DB so subsequent boots can
    // tell they've already finished bootstrap even if someone manually
    // purges the USERS table.
    let lock_path = data_dir.join("root.lock");
    if let Some(parent) = lock_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&lock_path, username.as_bytes());

    Ok(BootstrapOutcome::Created {
        username,
        raw_password: if generated { raw_password } else { String::new() },
    })
}

// ─── HTTP DTOs ────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct UserInfo {
    pub username: String,
    pub scopes: Vec<String>,
    pub allowed_partitions: Option<Vec<String>>,
    pub created_at: u64,
    pub enabled: bool,
    pub is_root: bool,
}

impl From<UserMeta> for UserInfo {
    fn from(u: UserMeta) -> Self {
        UserInfo {
            username: u.username,
            scopes: u.scopes,
            allowed_partitions: u.allowed_partitions,
            created_at: u.created_at,
            enabled: u.enabled,
            is_root: u.is_root,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct UserListResponse {
    pub users: Vec<UserInfo>,
    pub count: usize,
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
    #[serde(default)]
    pub allowed_partitions: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateUserRequest {
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
    #[serde(default)]
    pub allowed_partitions: Option<Option<Vec<String>>>, // nested Option to distinguish "unset" from "clear"
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct SetPasswordRequest {
    pub password: String,
}

// ─── User CRUD handlers ───────────────────────────────────────────────────────

/// GET /api/admin/users — admin only.
pub async fn list_users(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
) -> Result<Json<UserListResponse>, AppError> {
    require_scope(&caller, "admin")?;
    let users = tokio::task::spawn_blocking(move || engine.metadata().list_users())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;
    let infos: Vec<UserInfo> = users.into_iter().map(UserInfo::from).collect();
    let count = infos.len();
    Ok(Json(UserListResponse { users: infos, count }))
}

/// POST /api/admin/users — admin only. Creates a new user.
pub async fn create_user(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Json(req): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<UserInfo>), AppError> {
    require_scope(&caller, "admin")?;
    validate_username(&req.username)?;
    validate_password(&req.password)?;
    let scopes = req
        .scopes
        .unwrap_or_else(|| vec!["viewer".to_string()]);
    validate_scopes(&scopes)?;

    let engine_probe = engine.clone();
    let username_probe = req.username.clone();
    let existing = tokio::task::spawn_blocking(move || {
        engine_probe.metadata().get_user(&username_probe)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    if existing.is_some() {
        return Err(AppError {
            status: StatusCode::CONFLICT,
            message: format!("user '{}' already exists", req.username),
        });
    }

    let salt = random_salt();
    let user = UserMeta {
        username: req.username.clone(),
        password_hash: hash_password(&req.password, &salt),
        salt,
        scopes: expand_scopes(&scopes),
        allowed_partitions: req.allowed_partitions,
        created_at: now_secs(),
        enabled: true,
        is_root: false,
    };
    let u_for_audit = user.clone();
    let engine_for_insert = engine.clone();
    tokio::task::spawn_blocking(move || engine_for_insert.metadata().insert_user(&user))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    audit::record(
        engine,
        Some(caller.0.key_id.clone()),
        None,
        "user.create",
        Some(u_for_audit.username.clone()),
        AuditResult::Ok,
        Some(201),
    );

    Ok((StatusCode::CREATED, Json(UserInfo::from(u_for_audit))))
}

/// GET /api/admin/users/:username — admin only.
pub async fn get_user(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(username): Path<String>,
) -> Result<Json<UserInfo>, AppError> {
    require_scope(&caller, "admin")?;
    let u = tokio::task::spawn_blocking(move || engine.metadata().get_user(&username))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;
    match u {
        Some(user) => Ok(Json(UserInfo::from(user))),
        None => Err(AppError {
            status: StatusCode::NOT_FOUND,
            message: "user not found".to_string(),
        }),
    }
}

/// PATCH /api/admin/users/:username — admin only. Updates scopes/
/// allowed_partitions/enabled. Root user cannot be disabled.
pub async fn update_user(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(username): Path<String>,
    Json(req): Json<UpdateUserRequest>,
) -> Result<Json<UserInfo>, AppError> {
    require_scope(&caller, "admin")?;

    let engine_probe = engine.clone();
    let username_probe = username.clone();
    let user = tokio::task::spawn_blocking(move || {
        engine_probe.metadata().get_user(&username_probe)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    let mut user = match user {
        Some(u) => u,
        None => {
            return Err(AppError {
                status: StatusCode::NOT_FOUND,
                message: "user not found".to_string(),
            })
        }
    };

    if let Some(scopes) = req.scopes {
        validate_scopes(&scopes)?;
        user.scopes = expand_scopes(&scopes);
    }
    if let Some(ap) = req.allowed_partitions {
        user.allowed_partitions = ap;
    }
    if let Some(enabled) = req.enabled {
        if user.is_root && !enabled {
            return Err(AppError {
                status: StatusCode::FORBIDDEN,
                message: "cannot disable the root user".to_string(),
            });
        }
        user.enabled = enabled;
    }

    let updated = user.clone();
    let engine_task = engine.clone();
    tokio::task::spawn_blocking(move || engine_task.metadata().insert_user(&user))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    audit::record(
        engine,
        Some(caller.0.key_id.clone()),
        None,
        "user.update",
        Some(username),
        AuditResult::Ok,
        Some(200),
    );
    Ok(Json(UserInfo::from(updated)))
}

/// DELETE /api/admin/users/:username — admin only. Cascades SAs.
/// Root user cannot be deleted.
pub async fn delete_user(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(username): Path<String>,
) -> Result<StatusCode, AppError> {
    require_scope(&caller, "admin")?;

    // Probe for root before the destructive call.
    let engine_probe = engine.clone();
    let username_probe = username.clone();
    let existing = tokio::task::spawn_blocking(move || {
        engine_probe.metadata().get_user(&username_probe)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    let target = match existing {
        Some(u) => u,
        None => {
            return Err(AppError {
                status: StatusCode::NOT_FOUND,
                message: "user not found".to_string(),
            })
        }
    };
    if target.is_root {
        audit::record(
            engine,
            Some(caller.0.key_id.clone()),
            None,
            "user.delete",
            Some(username),
            AuditResult::Denied {
                reason: "root user cannot be deleted".to_string(),
            },
            Some(403),
        );
        return Err(AppError {
            status: StatusCode::FORBIDDEN,
            message: "cannot delete the root user".to_string(),
        });
    }

    let engine_task = engine.clone();
    let username_task = username.clone();
    tokio::task::spawn_blocking(move || {
        engine_task.metadata().delete_user_cascade(&username_task)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;

    audit::record(
        engine,
        Some(caller.0.key_id.clone()),
        None,
        "user.delete",
        Some(username),
        AuditResult::Ok,
        Some(204),
    );
    Ok(StatusCode::NO_CONTENT)
}

/// POST /api/admin/users/:username/password — admin forces a password
/// reset on another account. The caller's own password change goes
/// through `POST /api/auth/change-password`.
pub async fn reset_password(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(username): Path<String>,
    Json(req): Json<SetPasswordRequest>,
) -> Result<StatusCode, AppError> {
    require_scope(&caller, "admin")?;
    validate_password(&req.password)?;

    let engine_probe = engine.clone();
    let username_probe = username.clone();
    let user = tokio::task::spawn_blocking(move || {
        engine_probe.metadata().get_user(&username_probe)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    let mut user = match user {
        Some(u) => u,
        None => {
            return Err(AppError {
                status: StatusCode::NOT_FOUND,
                message: "user not found".to_string(),
            })
        }
    };

    let salt = random_salt();
    user.password_hash = hash_password(&req.password, &salt);
    user.salt = salt;

    let engine_task = engine.clone();
    tokio::task::spawn_blocking(move || engine_task.metadata().insert_user(&user))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    audit::record(
        engine,
        Some(caller.0.key_id.clone()),
        None,
        "user.reset_password",
        Some(username),
        AuditResult::Ok,
        Some(204),
    );
    Ok(StatusCode::NO_CONTENT)
}

// ─── ServiceAccount (per-user) handlers ───────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct SaInfo {
    pub key_id: String,
    pub name: String,
    pub key_prefix: String,
    pub parent_user: String,
    pub scopes: Vec<String>,
    pub allowed_partitions: Option<Vec<String>>,
    pub created_at: u64,
    pub last_used_at: u64,
    pub expires_at: Option<u64>,
    pub enabled: bool,
}

impl From<ApiKeyMeta> for SaInfo {
    fn from(k: ApiKeyMeta) -> Self {
        SaInfo {
            key_id: k.key_id,
            name: k.name,
            key_prefix: k.key_prefix,
            parent_user: k.parent_user,
            scopes: k.scopes,
            allowed_partitions: k.allowed_partitions,
            created_at: k.created_at,
            last_used_at: k.last_used_at,
            expires_at: k.expires_at,
            enabled: k.enabled,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct SaListResponse {
    pub service_accounts: Vec<SaInfo>,
    pub count: usize,
}

#[derive(Debug, Deserialize)]
pub struct CreateSaRequest {
    pub name: String,
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
    #[serde(default)]
    pub allowed_partitions: Option<Vec<String>>,
    #[serde(default)]
    pub expires_at: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct CreateSaResponse {
    pub key_id: String,
    pub name: String,
    pub key: String,
    pub key_prefix: String,
    pub parent_user: String,
    pub scopes: Vec<String>,
    pub allowed_partitions: Option<Vec<String>>,
    pub expires_at: Option<u64>,
    pub created_at: u64,
}

/// Check that the caller can act on `target_user`'s service accounts.
/// Admins always pass; non-admins may only act on their own account.
fn check_sa_owner(caller: &AuthedKey, target_user: &str) -> Result<(), AppError> {
    if caller.has_scope("admin") {
        return Ok(());
    }
    if caller.0.parent_user == target_user && !target_user.is_empty() {
        return Ok(());
    }
    Err(AppError {
        status: StatusCode::FORBIDDEN,
        message: "only admin or account owner may manage these service accounts".to_string(),
    })
}

/// GET /api/admin/users/:username/sa — list SAs owned by a user.
pub async fn list_sa(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(username): Path<String>,
) -> Result<Json<SaListResponse>, AppError> {
    check_sa_owner(&caller, &username)?;
    let sas = tokio::task::spawn_blocking(move || {
        engine.metadata().list_service_accounts_by_user(&username)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    let infos: Vec<SaInfo> = sas.into_iter().map(SaInfo::from).collect();
    let count = infos.len();
    Ok(Json(SaListResponse {
        service_accounts: infos,
        count,
    }))
}

/// POST /api/admin/users/:username/sa — create a new SA owned by this user.
/// Scope intersection: requested scopes must be a subset of the user's.
pub async fn create_sa(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(username): Path<String>,
    Json(req): Json<CreateSaRequest>,
) -> Result<(StatusCode, Json<CreateSaResponse>), AppError> {
    check_sa_owner(&caller, &username)?;
    if req.name.trim().is_empty() {
        return Err(AppError::bad_request("name must not be empty"));
    }

    // Load the parent user to constrain scopes and partitions.
    let engine_probe = engine.clone();
    let username_probe = username.clone();
    let user_opt = tokio::task::spawn_blocking(move || {
        engine_probe.metadata().get_user(&username_probe)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    let user = match user_opt {
        Some(u) if u.enabled => u,
        Some(_) => {
            return Err(AppError {
                status: StatusCode::FORBIDDEN,
                message: "parent user is disabled".to_string(),
            })
        }
        None => {
            return Err(AppError {
                status: StatusCode::NOT_FOUND,
                message: "parent user not found".to_string(),
            })
        }
    };

    let requested = req.scopes.unwrap_or_else(|| user.scopes.clone());
    validate_scopes(&requested)?;
    let expanded = expand_scopes(&requested);

    // Every requested scope must already be present on the parent user —
    // the intersection must equal the request. `read`/`write` are also
    // implicitly granted by the parent's `admin` or `operator` scope.
    let parent_elevated = user
        .scopes
        .iter()
        .any(|u| u == "admin" || u == "operator");
    for s in &expanded {
        let granted = user.scopes.contains(s)
            || ((s == "read" || s == "write") && parent_elevated);
        if !granted {
            return Err(AppError {
                status: StatusCode::FORBIDDEN,
                message: format!(
                    "scope '{}' is not granted on parent user '{}'",
                    s, user.username
                ),
            });
        }
    }

    // Intersect allowed_partitions with parent's.
    let allowed_partitions = match (&user.allowed_partitions, &req.allowed_partitions) {
        (None, x) => x.clone(),
        (Some(parent), None) => Some(parent.clone()),
        (Some(parent), Some(requested)) => {
            let narrowed: Vec<String> = requested
                .iter()
                .filter(|p| parent.contains(p))
                .cloned()
                .collect();
            if narrowed.len() != requested.len() {
                return Err(AppError {
                    status: StatusCode::FORBIDDEN,
                    message: "requested partitions exceed parent user's allow-list".to_string(),
                });
            }
            Some(narrowed)
        }
    };

    let (raw_key, mut meta, _now) = build_new_key(&req.name, expanded.clone());
    meta.parent_user = username.clone();
    meta.allowed_partitions = allowed_partitions.clone();
    meta.expires_at = req.expires_at;
    let response = CreateSaResponse {
        key_id: meta.key_id.clone(),
        name: meta.name.clone(),
        key: raw_key,
        key_prefix: meta.key_prefix.clone(),
        parent_user: meta.parent_user.clone(),
        scopes: meta.scopes.clone(),
        allowed_partitions: meta.allowed_partitions.clone(),
        expires_at: meta.expires_at,
        created_at: meta.created_at,
    };
    let key_id_for_audit = meta.key_id.clone();

    let engine_task = engine.clone();
    tokio::task::spawn_blocking(move || engine_task.metadata().insert_api_key(&meta))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    audit::record(
        engine,
        Some(caller.0.key_id.clone()),
        None,
        "sa.create",
        Some(key_id_for_audit),
        AuditResult::Ok,
        Some(201),
    );

    Ok((StatusCode::CREATED, Json(response)))
}

/// DELETE /api/admin/users/:username/sa/:key_id — revoke a SA.
pub async fn delete_sa(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path((username, key_id)): Path<(String, String)>,
) -> Result<StatusCode, AppError> {
    check_sa_owner(&caller, &username)?;

    let engine_probe = engine.clone();
    let key_id_probe = key_id.clone();
    let sa = tokio::task::spawn_blocking(move || {
        engine_probe.metadata().get_api_key(&key_id_probe)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    let sa = match sa {
        Some(s) => s,
        None => {
            return Err(AppError {
                status: StatusCode::NOT_FOUND,
                message: "service account not found".to_string(),
            })
        }
    };
    if sa.parent_user != username {
        return Err(AppError {
            status: StatusCode::NOT_FOUND,
            message: "service account not found on this user".to_string(),
        });
    }

    let engine_task = engine.clone();
    let key_id_task = key_id.clone();
    tokio::task::spawn_blocking(move || engine_task.metadata().delete_api_key(&key_id_task))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    audit::record(
        engine,
        Some(caller.0.key_id.clone()),
        None,
        "sa.delete",
        Some(key_id),
        AuditResult::Ok,
        Some(204),
    );
    Ok(StatusCode::NO_CONTENT)
}

/// POST /api/admin/users/:username/sa/:key_id/rotate — issue a new secret
/// in place. Returns the new raw key once.
pub async fn rotate_sa(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path((username, key_id)): Path<(String, String)>,
) -> Result<Json<CreateSaResponse>, AppError> {
    check_sa_owner(&caller, &username)?;

    let engine_probe = engine.clone();
    let key_id_probe = key_id.clone();
    let sa = tokio::task::spawn_blocking(move || {
        engine_probe.metadata().get_api_key(&key_id_probe)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    let existing = match sa {
        Some(s) if s.parent_user == username => s,
        _ => {
            return Err(AppError {
                status: StatusCode::NOT_FOUND,
                message: "service account not found on this user".to_string(),
            })
        }
    };

    // Mint a new raw secret and overwrite hash/prefix in place. Keep all
    // other fields (scopes/allowed_partitions/expiry) untouched so rotation
    // is truly drop-in.
    let (new_raw, new_meta, _now) =
        build_new_key(&existing.name, existing.scopes.clone());
    let engine_task = engine.clone();
    let key_id_task = key_id.clone();
    let new_hash = new_meta.key_hash.clone();
    let new_prefix = new_meta.key_prefix.clone();
    let new_prefix_task = new_prefix.clone();
    let updated = tokio::task::spawn_blocking(move || {
        engine_task
            .metadata()
            .update_api_key_hash(&key_id_task, &new_hash, &new_prefix_task)
    })
    .await
    .map_err(|e| AppError::internal(e.to_string()))?
    .map_err(AppError::from_jihuan)?;
    if !updated {
        return Err(AppError {
            status: StatusCode::NOT_FOUND,
            message: "service account disappeared during rotation".to_string(),
        });
    }

    audit::record(
        engine,
        Some(caller.0.key_id.clone()),
        None,
        "sa.rotate",
        Some(key_id.clone()),
        AuditResult::Ok,
        Some(200),
    );

    Ok(Json(CreateSaResponse {
        key_id,
        name: existing.name,
        key: new_raw,
        key_prefix: new_prefix,
        parent_user: existing.parent_user,
        scopes: existing.scopes,
        allowed_partitions: existing.allowed_partitions,
        expires_at: existing.expires_at,
        created_at: existing.created_at,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_password_deterministic() {
        let salt = [9u8; 16];
        let a = hash_password("correct horse battery staple", &salt);
        let b = hash_password("correct horse battery staple", &salt);
        assert_eq!(a, b);
        // Different salt → different hash
        let c = hash_password("correct horse battery staple", &[1u8; 16]);
        assert_ne!(a, c);
        // Different password → different hash
        let d = hash_password("wrong", &salt);
        assert_ne!(a, d);
    }

    #[test]
    fn test_expand_scopes_operator_viewer() {
        let out = expand_scopes(&["operator".to_string()]);
        assert_eq!(out, vec!["read".to_string(), "write".to_string()]);
        let out = expand_scopes(&["viewer".to_string()]);
        assert_eq!(out, vec!["read".to_string()]);
        let out = expand_scopes(&["admin".to_string(), "operator".to_string()]);
        assert_eq!(
            out,
            vec!["admin".to_string(), "read".to_string(), "write".to_string()]
        );
    }

    #[test]
    fn test_validate_username_boundaries() {
        assert!(validate_username("ab").is_ok());
        assert!(validate_username("alice.bob_01-test").is_ok());
        // Too short / too long
        assert!(validate_username("a").is_err());
        assert!(validate_username(&"a".repeat(65)).is_err());
        // Invalid chars
        assert!(validate_username("al ice").is_err());
        assert!(validate_username("alice$").is_err());
    }

    #[test]
    fn test_random_salt_differs_between_calls() {
        let a = random_salt();
        let b = random_salt();
        assert_ne!(a, b, "two random salts must not collide");
    }
}
