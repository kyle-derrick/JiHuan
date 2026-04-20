use axum::{
    body::Body,
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use include_dir::{include_dir, Dir};

static UI_DIR: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/ui/dist");

fn mime_for(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "json" => "application/json; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// Serve the SPA index.html for the root `/ui/` path
pub async fn serve_index() -> Response {
    match UI_DIR.get_file("index.html") {
        Some(f) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(Body::from(f.contents()))
            .unwrap(),
        None => (StatusCode::NOT_FOUND, "UI not built").into_response(),
    }
}

/// Serve static assets under `/ui/:path`
pub async fn serve_asset(Path(path): Path<String>) -> Response {
    match UI_DIR.get_file(&path) {
        Some(f) => Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, mime_for(&path))
            .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable")
            .body(Body::from(f.contents()))
            .unwrap(),
        None => {
            // Fall back to index.html for client-side routing
            match UI_DIR.get_file("index.html") {
                Some(f) => Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
                    .body(Body::from(f.contents()))
                    .unwrap(),
                None => (StatusCode::NOT_FOUND, "Not found").into_response(),
            }
        }
    }
}
