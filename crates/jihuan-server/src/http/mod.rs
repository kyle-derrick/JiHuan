use std::sync::Arc;

use axum::{
    Router,
    middleware,
    routing::{delete, get, post},
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

    Router::new()
        // File routes - specific paths before wildcard
        .route("/api/v1/files", post(files::upload_file))
        .route("/api/v1/files/:file_id/meta", get(files::get_file_meta))
        .route("/api/v1/files/:file_id", get(files::download_file).delete(files::delete_file))
        // Admin routes
        .route("/api/status", get(admin::get_status))
        .route("/api/gc/trigger", post(admin::trigger_gc))
        .route("/api/block/list", get(admin::list_blocks))
        // Key management routes
        .route("/api/keys", post(auth::create_key).get(auth::list_keys))
        .route("/api/keys/:key_id", delete(auth::delete_key))
        // Web UI (SPA)
        .route("/ui/", get(ui::serve_index))
        .route("/ui/*path", get(ui::serve_asset))
        // Auth middleware (runs after routing, before handlers)
        .layer(middleware::from_fn_with_state(
            auth_state,
            auth::auth_middleware,
        ))
        .with_state(engine)
}
