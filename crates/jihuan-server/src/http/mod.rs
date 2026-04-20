use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};

use jihuan_core::Engine;

pub mod admin;
pub mod files;

pub fn router(engine: Arc<Engine>) -> Router {
    Router::new()
        // File routes - specific paths before wildcard
        .route("/api/v1/files", post(files::upload_file))
        .route("/api/v1/files/:file_id/meta", get(files::get_file_meta))
        .route("/api/v1/files/:file_id", get(files::download_file).delete(files::delete_file))
        // Admin routes
        .route("/api/status", get(admin::get_status))
        .route("/api/gc/trigger", post(admin::trigger_gc))
        .route("/api/block/list", get(admin::list_blocks))
        .with_state(engine)
}
