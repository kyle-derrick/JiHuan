use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::{delete, get, post},
    Extension, Json, Router,
};
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::json;
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};
use tower_http::timeout::TimeoutLayer;

use jihuan_core::Engine;

pub mod admin;
pub mod audit;
pub mod auth;
pub mod files;
pub mod iam;
pub mod ui;

/// Shared liveness/readiness flag. Cloned into the probe router so
/// `/readyz` can return 503 during the narrow window between process
/// start and engine bootstrap completion. Operators (k8s, Nomad,
/// Docker healthchecks) should use `/healthz` for liveness (always 200
/// while the HTTP task is accepting connections) and `/readyz` for
/// readiness (only 200 once the engine is servicing requests).
#[derive(Clone, Default)]
pub struct ReadyFlag(pub Arc<AtomicBool>);

impl ReadyFlag {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
    pub fn set_ready(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

async fn handle_healthz() -> impl IntoResponse {
    // Liveness: the HTTP task is alive enough to run this handler.
    // Always 200 — if k8s sees a non-200 here something is very wrong.
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn handle_readyz(State(ready): State<ReadyFlag>) -> impl IntoResponse {
    if ready.is_ready() {
        (
            StatusCode::OK,
            Json(json!({ "status": "ready" })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "starting",
                "reason": "engine bootstrap in progress"
            })),
        )
    }
}

pub fn router(
    engine: Arc<Engine>,
    metrics_handle: Option<PrometheusHandle>,
    ready: ReadyFlag,
) -> Router {
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

    // Phase 4.3 — per-IP token-bucket limiter. We build the shared
    // `GovernorConfig` once and wrap it in `Arc`; each sub-router that
    // opts in constructs its own lightweight `GovernorLayer` that
    // points at the same config (the token buckets live inside the
    // config's internal state keyed by peer IP). Probe routes, login,
    // and the metrics endpoint are merged *without* this layer so a
    // locked-out operator or orchestrator can always reach them.
    let rate_cfg = &engine.config().server.rate_limit;
    let governor_config = if rate_cfg.enabled {
        let per_second = rate_cfg.per_ip_per_sec.max(1) as u64;
        let burst = rate_cfg.burst.max(1);
        match GovernorConfigBuilder::default()
            .per_second(per_second)
            .burst_size(burst)
            .finish()
        {
            Some(conf) => Some(Arc::new(conf)),
            None => {
                // Defensive degrade — validate() + the clamps above
                // should guarantee `Some(_)`, but if tower_governor's
                // internal invariants change we prefer plaintext over
                // a crash loop.
                tracing::warn!(
                    "tower_governor rejected rate_limit config ({} per second, burst {}), \
                     rate limiting disabled",
                    per_second,
                    burst
                );
                None
            }
        }
    } else {
        None
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
        .route("/api/admin/scrub", post(admin::scrub))
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

    // Apply the rate-limit layer to the three request-serving routers
    // but *not* to the auth or probe routers. Each `GovernorLayer`
    // instance points at the shared `Arc<GovernorConfig>` so all
    // sub-routers hit the same per-IP buckets.
    let (upload_router, download_router) = if let Some(cfg) = governor_config.as_ref() {
        let u = upload_router.layer(GovernorLayer {
            config: cfg.clone(),
        });
        let d = download_router.layer(GovernorLayer {
            config: cfg.clone(),
        });
        (u, d)
    } else {
        (upload_router, download_router)
    };
    if let Some(cfg) = governor_config.as_ref() {
        main_router = main_router.layer(GovernorLayer {
            config: cfg.clone(),
        });
    }

    // Liveness / readiness probes (k8s-compatible). Served outside the
    // auth and CORS layers so orchestrators can hit them without a
    // credential and without worrying about preflight headers. Must
    // stay cheap — no engine access, no allocations beyond a tiny JSON
    // body. `/readyz` returns 503 with `{status:"starting"}` until the
    // engine reports ready; this is what gates Kubernetes traffic to
    // new pods during rolling deploys.
    let probe_router: Router = Router::new()
        .route("/healthz", get(handle_healthz))
        .route("/readyz", get(handle_readyz).with_state(ready));

    Router::new()
        .merge(probe_router)
        .merge(auth_router)
        .merge(upload_router)
        .merge(download_router)
        .merge(main_router)
}

#[cfg(test)]
mod probe_tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    fn probe_only_router(ready: ReadyFlag) -> Router {
        Router::new()
            .route("/healthz", get(handle_healthz))
            .route("/readyz", get(handle_readyz).with_state(ready))
    }

    #[tokio::test]
    async fn test_healthz_always_ok() {
        let app = probe_only_router(ReadyFlag::new()); // not-ready shouldn't matter
        let resp = app
            .oneshot(Request::builder().uri("/healthz").body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
        assert!(json["version"].is_string());
    }

    #[tokio::test]
    async fn test_readyz_503_before_set_ready() {
        let flag = ReadyFlag::new();
        let app = probe_only_router(flag);
        let resp = app
            .oneshot(Request::builder().uri("/readyz").body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "starting");
    }

    #[tokio::test]
    async fn test_governor_config_builds_for_valid_rate_limit() {
        // Phase 4.3 sanity: the production code path
        // (`GovernorConfigBuilder::per_second + burst_size`) used in
        // `router()` must yield `Some(_)` for the default config
        // values. An E2E 429 test requires a live socket + ConnectInfo
        // (tower_governor's peer-IP extractor needs it), covered by
        // the `tests/perf/` smoke scripts; here we just lock in that
        // the builder accepts our config surface.
        use tower_governor::governor::GovernorConfigBuilder;
        let conf = GovernorConfigBuilder::default()
            .per_second(50)
            .burst_size(100)
            .finish();
        assert!(conf.is_some());
    }

    #[tokio::test]
    async fn test_readyz_200_after_set_ready() {
        let flag = ReadyFlag::new();
        flag.set_ready();
        let app = probe_only_router(flag);
        let resp = app
            .oneshot(Request::builder().uri("/readyz").body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ready");
    }
}
