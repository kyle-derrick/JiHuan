use std::sync::Arc;

use axum::Router;

use jihuan_core::Engine;

pub mod admin;
pub mod files;

pub fn router(engine: Arc<Engine>) -> Router {
    Router::new()
        .nest("/api/v1/files", files::router(engine.clone()))
        .nest("/api", admin::router(engine))
}
