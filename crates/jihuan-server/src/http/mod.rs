use std::sync::Arc;

use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, post},
    Extension, Router,
};
use metrics_exporter_prometheus::PrometheusHandle;

use jihuan_core::Engine;

pub mod admin;
pub mod audit;
pub mod auth;
pub mod files;
pub mod ui;

pub fn router(engine: Arc<Engine>, metrics_handle: Option<PrometheusHandle>) -> Router {
    let auth_state = auth::AuthState {
        engine: engine.clone(),
        config: engine.config().auth.clone(),
        sessions: auth::SessionStore::new(),
    };

    // Body limit for uploads: None in config ⇒ unlimited (disable).
    let upload_limit_layer = match engine.config().server.max_body_size {
        Some(n) => DefaultBodyLimit::max(n as usize),
        None => DefaultBodyLimit::disable(),
    };

    // Smaller default for all other routes (protect against oversized JSON etc.)
    let default_limit_layer = DefaultBodyLimit::max(16 * 1024 * 1024); // 16 MB

    let upload_router = Router::new()
        .route(
            "/api/v1/files",
            get(files::list_files).post(files::upload_file),
        )
        .layer(upload_limit_layer);

    // `/api/auth/*` endpoints use `State<AuthState>`, unlike the rest of the
    // app which uses `State<Arc<Engine>>`. Build them as a self-contained
    // router (middleware applied, state reduced to `Router<()>`) and merge at
    // the top level alongside the main engine-stated router.
    let auth_router: Router = Router::new()
        .route("/api/auth/login", post(auth::login))
        .route("/api/auth/logout", post(auth::logout))
        .route("/api/auth/me", get(auth::me))
        .route("/api/auth/change-password", post(auth::change_password))
        .layer(middleware::from_fn_with_state(
            auth_state.clone(),
            auth::auth_middleware,
        ))
        .with_state(auth_state.clone());

    let main_router: Router = Router::new()
        .merge(upload_router)
        .route("/api/v1/files/:file_id/meta", get(files::get_file_meta))
        .route(
            "/api/v1/files/:file_id",
            get(files::download_file).delete(files::delete_file),
        )
        // Admin routes
        .route("/api/status", get(admin::get_status))
        .route("/api/gc/trigger", post(admin::trigger_gc))
        .route("/api/admin/compact", post(admin::compact))
        .route("/api/block/list", get(admin::list_blocks))
        .route(
            "/api/block/:block_id",
            get(admin::get_block_detail).delete(admin::delete_block),
        )
        .route("/api/config", get(admin::get_config))
        // Audit log (Phase 2.6)
        .route("/api/admin/audit", get(audit::list_audit))
        // Same-origin Prometheus metrics proxy for UI and integrations that
        // cannot reach the standalone `:9090/metrics` port (CORS / firewall).
        .route("/api/metrics", get(admin::render_metrics))
        // Key management routes
        .route("/api/keys", post(auth::create_key).get(auth::list_keys))
        .route("/api/keys/:key_id", delete(auth::delete_key))
        // Web UI (SPA)
        .route("/ui/", get(ui::serve_index))
        .route("/ui/*path", get(ui::serve_asset))
        // Apply a default body limit to non-upload routes
        .layer(default_limit_layer)
        // Auth middleware (runs after routing, before handlers)
        .layer(middleware::from_fn_with_state(
            auth_state,
            auth::auth_middleware,
        ))
        // Share the Prometheus handle (if available) with handlers via Extension
        .layer(Extension(admin::MetricsHandle(metrics_handle)))
        .with_state(engine);

    // Liveness / readiness probes (k8s-compatible). Served outside the auth
    // and CORS layers so orchestrators can hit them without a credential
    // and without worrying about preflight headers. They must stay cheap
    // — no engine access, no allocations beyond the static body.
    let probe_router: Router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(|| async { "ready" }));

    Router::new()
        .merge(probe_router)
        .merge(auth_router)
        .merge(main_router)
}
