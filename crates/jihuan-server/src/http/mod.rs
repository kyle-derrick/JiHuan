use std::sync::Arc;

use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, post},
    Router,
};

use jihuan_core::Engine;

pub mod admin;
pub mod auth;
pub mod files;
pub mod ui;

pub fn router(engine: Arc<Engine>) -> Router {
    let auth_state = auth::AuthState {
        engine: engine.clone(),
        config: engine.config().auth.clone(),
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

    Router::new()
        .merge(upload_router)
        .route("/api/v1/files/:file_id/meta", get(files::get_file_meta))
        .route(
            "/api/v1/files/:file_id",
            get(files::download_file).delete(files::delete_file),
        )
        // Admin routes
        .route("/api/status", get(admin::get_status))
        .route("/api/gc/trigger", post(admin::trigger_gc))
        .route("/api/block/list", get(admin::list_blocks))
        .route(
            "/api/block/:block_id",
            get(admin::get_block_detail).delete(admin::delete_block),
        )
        .route("/api/config", get(admin::get_config))
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
        .with_state(engine)
}
