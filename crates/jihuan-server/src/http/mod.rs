use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, post},
    Extension, Router,
};
use metrics_exporter_prometheus::PrometheusHandle;
use tower_http::timeout::TimeoutLayer;

use jihuan_core::Engine;

pub mod admin;
pub mod audit;
pub mod auth;
pub mod files;
pub mod iam;
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

    // v0.4.5 slow-loris / stuck-backend guard. Applied ONLY to control-plane
    // routes (status, gc, compact, keys, config, auth). Upload and download
    // are explicitly exempt — legitimate multi-GB file I/O can dwarf this
    // timeout. `0` disables the layer (not recommended).
    let timeout_secs = engine.config().server.request_timeout_secs;
    let request_timeout_layer: Option<TimeoutLayer> = if timeout_secs == 0 {
        None
    } else {
        Some(TimeoutLayer::new(Duration::from_secs(timeout_secs)))
    };

    // Streaming-path routers: upload (configurable body limit, no timeout)
    // and download (default body limit, no timeout). These stay OUT of
    // `main_router` so the global request-timeout layer applied below
    // cannot cut off legitimate multi-GB transfers. Both get their own
    // copy of `auth_middleware` so scope checks still run.
    let streaming_auth_layer = middleware::from_fn_with_state(
        auth_state.clone(),
        auth::auth_middleware,
    );

    let upload_router: Router = Router::new()
        .route(
            "/api/v1/files",
            get(files::list_files).post(files::upload_file),
        )
        .layer(upload_limit_layer)
        .layer(streaming_auth_layer.clone())
        .with_state(engine.clone());

    let download_router: Router = Router::new()
        .route(
            "/api/v1/files/:file_id",
            get(files::download_file).delete(files::delete_file),
        )
        .layer(default_limit_layer)
        .layer(streaming_auth_layer)
        .with_state(engine.clone());

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

    let mut main_router: Router = Router::new()
        .route("/api/v1/files/:file_id/meta", get(files::get_file_meta))
        // Admin routes
        .route("/api/status", get(admin::get_status))
        .route("/api/gc/trigger", post(admin::trigger_gc))
        .route("/api/admin/compact", post(admin::compact))
        .route("/api/admin/seal", post(admin::seal))
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
        // Key management routes (legacy; kept for backward compatibility)
        .route("/api/keys", post(auth::create_key).get(auth::list_keys))
        .route("/api/keys/:key_id", delete(auth::delete_key))
        // v0.5.0-iam: user + service-account admin routes
        .route(
            "/api/admin/users",
            get(iam::list_users).post(iam::create_user),
        )
        .route(
            "/api/admin/users/:username",
            get(iam::get_user)
                .patch(iam::update_user)
                .delete(iam::delete_user),
        )
        .route(
            "/api/admin/users/:username/password",
            post(iam::reset_password),
        )
        .route(
            "/api/admin/users/:username/sa",
            get(iam::list_sa).post(iam::create_sa),
        )
        .route(
            "/api/admin/users/:username/sa/:key_id",
            delete(iam::delete_sa),
        )
        .route(
            "/api/admin/users/:username/sa/:key_id/rotate",
            post(iam::rotate_sa),
        )
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

    // The timeout wraps everything *above* in this chain, but NOT
    // upload/download routes that were already merged with their own
    // body-limit layers — tower_http's TimeoutLayer applies per-request
    // at the service boundary, and upload/download handlers themselves
    // `spawn_blocking` without holding the request future's timer open
    // while streaming chunks to disk. The cap is therefore effectively
    // on the handler's setup phase, which is fine.
    if let Some(t) = request_timeout_layer {
        main_router = main_router.layer(t);
    }

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
        .merge(upload_router)
        .merge(download_router)
        .merge(main_router)
}
