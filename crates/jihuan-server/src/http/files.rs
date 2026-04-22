use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Multipart, Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde::Serialize;

use jihuan_core::{Engine, JiHuanError};

use crate::http::auth::{require_scope, AuthedKey};

/// Build an RFC 6266-compliant `Content-Disposition: attachment` header
/// value from an arbitrary user-supplied filename. Guarantees the result
/// never contains bytes that are illegal in HTTP header values (CR, LF,
/// NUL, DEL, control chars) — those would otherwise either make the
/// header unparsable (→ 500) or, worse, allow response splitting.
///
/// We emit both forms: a quoted ASCII fallback (`filename="…"`, with
/// unsafe bytes and backslash/quote replaced by `_`) for old clients, and
/// the percent-encoded UTF-8 variant (`filename*=UTF-8''…`) that every
/// modern browser prefers and which preserves full Unicode fidelity.
fn content_disposition_attachment(name: &str) -> axum::http::HeaderValue {
    // ── Percent-encode UTF-8 per RFC 5987 §3.2.1 ─────────────────────────
    // Keep the "attr-char" set (alnum + a few symbols); everything else
    // — including spaces, quotes and control bytes — is %HH-escaped.
    fn is_attr_char(b: u8) -> bool {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'&'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            )
    }
    let mut encoded = String::with_capacity(name.len() * 2);
    for b in name.as_bytes() {
        if is_attr_char(*b) {
            encoded.push(*b as char);
        } else {
            encoded.push_str(&format!("%{:02X}", b));
        }
    }

    // ── ASCII fallback: replace everything outside printable-ASCII or
    // the few quoted-string special chars with `_` ─────────────────────
    let mut ascii = String::with_capacity(name.len());
    for b in name.as_bytes() {
        match *b {
            b'"' | b'\\' => ascii.push('_'),
            0x20..=0x7E => ascii.push(*b as char),
            _ => ascii.push('_'), // control bytes / non-ASCII
        }
    }
    if ascii.is_empty() {
        ascii.push_str("download");
    }

    // HeaderValue::from_str accepts only visible ASCII + space + HT, so
    // after the sanitisation above this never fails; falling through to
    // a hardcoded default keeps the type signature infallible.
    let v = format!(
        "attachment; filename=\"{}\"; filename*=UTF-8''{}",
        ascii, encoded
    );
    axum::http::HeaderValue::from_str(&v).unwrap_or_else(|_| {
        axum::http::HeaderValue::from_static("attachment; filename=\"download\"")
    })
}

#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub file_id: String,
    pub file_name: String,
    pub file_size: u64,
}

#[derive(Debug, Serialize)]
pub struct FileMetaResponse {
    pub file_id: String,
    pub file_name: String,
    pub file_size: u64,
    pub create_time: u64,
    pub content_type: Option<String>,
    pub chunk_count: usize,
    /// Detailed chunk layout (block_id, offset, sizes, hash). Only populated by /meta endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunks: Option<Vec<ChunkInfoDto>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_id: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ChunkInfoDto {
    pub index: u32,
    pub block_id: String,
    pub offset: u64,
    pub original_size: u64,
    pub compressed_size: u64,
    pub hash: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: u16,
}


#[derive(Debug, Serialize)]
pub struct FileListResponse {
    pub files: Vec<FileMetaResponse>,
    pub count: usize,
    pub total: usize,
}

#[derive(Debug, Deserialize)]
pub struct ListFilesQuery {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// Case-insensitive substring match against file_name or file_id
    pub q: Option<String>,
    /// Sort key: "create_time" (default) | "file_name" | "file_size"
    pub sort: Option<String>,
    /// Sort order: "desc" (default) | "asc"
    pub order: Option<String>,
}

/// GET /api/v1/files
/// List all files with optional limit/offset pagination
pub async fn list_files(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Query(q): Query<ListFilesQuery>,
) -> Result<Json<FileListResponse>, AppError> {
    require_scope(&caller, "read")?;
    let all = tokio::task::spawn_blocking(move || engine.list_all_files())
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(|e| AppError::internal(e.to_string()))?;

    let total = all.len();
    let offset = q.offset.unwrap_or(0);
    let limit = q.limit.unwrap_or(200);
    // keep original total for no-filter case
    let _ = total;

    // Filter
    let mut filtered: Vec<_> = if let Some(query) = q.q.as_ref().map(|s| s.to_lowercase()) {
        all.into_iter()
            .filter(|m| m.file_name.to_lowercase().contains(&query) || m.file_id.contains(&query))
            .collect()
    } else {
        all
    };

    // Sort
    let sort_key = q.sort.as_deref().unwrap_or("create_time");
    let descending = q.order.as_deref().unwrap_or("desc") == "desc";
    match sort_key {
        "file_name" => filtered.sort_by(|a, b| a.file_name.cmp(&b.file_name)),
        "file_size" => filtered.sort_by_key(|m| m.file_size),
        _ => filtered.sort_by_key(|m| m.create_time),
    }
    if descending {
        filtered.reverse();
    }

    let total_after_filter = filtered.len();

    let files: Vec<FileMetaResponse> = filtered
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|m| FileMetaResponse {
            file_id: m.file_id,
            file_name: m.file_name,
            file_size: m.file_size,
            create_time: m.create_time,
            content_type: m.content_type,
            chunk_count: m.chunks.len(),
            chunks: None,
            partition_id: None,
        })
        .collect();

    let count = files.len();
    Ok(Json(FileListResponse { files, count, total: total_after_filter.max(total) }))
}

/// POST /api/v1/files
/// Upload a file via multipart form data — streams the body directly into the
/// engine without buffering the whole file in memory. Memory usage stays bounded
/// to roughly one chunk (`storage.chunk_size`) plus a small mpsc queue.
pub async fn upload_file(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, AppError> {
    require_scope(&caller, "write")?;
    // Find the first file field
    let mut field = loop {
        let f = multipart
            .next_field()
            .await
            .map_err(|e| AppError::bad_request(format!("Multipart error: {}", e)))?;
        match f {
            Some(f) => {
                let name = f.name().unwrap_or("");
                if name == "file" || f.file_name().is_some() {
                    break f;
                }
                // skip non-file fields
                let _ = f.bytes().await;
            }
            None => return Err(AppError::bad_request("No file field found")),
        }
    };

    let file_name = field
        .file_name()
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let content_type = field.content_type().map(|s| s.to_string());

    // Bridge async → sync via bounded mpsc of Bytes; blocking worker reads via StreamReader.
    let (tx, rx) = std::sync::mpsc::sync_channel::<std::io::Result<bytes::Bytes>>(8);

    let fn_clone = file_name.clone();
    let ct_clone = content_type.clone();
    let engine_clone = engine.clone();
    let worker = tokio::task::spawn_blocking(move || {
        let reader = ChannelReader::new(rx);
        engine_clone.put_stream(reader, &fn_clone, ct_clone.as_deref())
    });

    // Pump bytes from the multipart field into the channel.
    let mut total_bytes: u64 = 0;
    let pump_result: Result<(), AppError> = async {
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|e| AppError::bad_request(format!("Failed to read field: {}", e)))?
        {
            total_bytes += chunk.len() as u64;
            if tx.send(Ok(chunk)).is_err() {
                // Worker dropped (probably errored); stop pumping.
                break;
            }
        }
        Ok(())
    }
    .await;
    // Closing tx signals EOF to the worker.
    drop(tx);

    // Propagate multipart error only if the worker didn't already fail with a clearer error.
    let file_id = worker
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;
    pump_result?;

    Ok(Json(UploadResponse {
        file_id,
        file_name,
        file_size: total_bytes,
    }))
}

/// Blocking adapter: `std::io::Read` over a `sync_channel` receiver of `Bytes`.
struct ChannelReader {
    rx: std::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>,
    residual: bytes::Bytes,
}

impl ChannelReader {
    fn new(rx: std::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>) -> Self {
        Self {
            rx,
            residual: bytes::Bytes::new(),
        }
    }
}

impl std::io::Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.residual.is_empty() {
            match self.rx.recv() {
                Ok(Ok(b)) => self.residual = b,
                Ok(Err(e)) => return Err(e),
                Err(_) => return Ok(0), // sender dropped → EOF
            }
        }
        let n = self.residual.len().min(buf.len());
        buf[..n].copy_from_slice(&self.residual[..n]);
        self.residual = self.residual.slice(n..);
        Ok(n)
    }
}

/// GET /api/v1/files/:file_id
/// Download a file. Supports a single-range `Range: bytes=start-end` request
/// (responds 206 Partial Content). Multi-range and otherwise unsatisfiable
/// ranges return 416.
pub async fn download_file(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(file_id): Path<String>,
    req_headers: HeaderMap,
) -> Result<Response, AppError> {
    require_scope(&caller, "read")?;
    // First fetch metadata for filename, content-type and size
    let meta = {
        let e = engine.clone();
        let fid = file_id.clone();
        tokio::task::spawn_blocking(move || e.get_file_meta(&fid))
            .await
            .map_err(|e| AppError::internal(e.to_string()))?
            .map_err(AppError::from_jihuan)?
    };
    let meta = meta.ok_or_else(|| AppError::not_found(&file_id))?;

    let content_type = meta
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let file_size = meta.file_size;

    // Parse the Range header (single-range only; reject multi-range)
    let range_req = match req_headers.get(header::RANGE) {
        Some(v) => match parse_single_byte_range(v.to_str().unwrap_or(""), file_size) {
            Ok(r) => Some(r),
            Err(RangeParseError::Multi) | Err(RangeParseError::Unsatisfiable) => {
                // RFC 9110 §15.5.17: Content-Range: bytes */<complete-length>
                let mut headers = HeaderMap::new();
                headers.insert(header::ACCEPT_RANGES, "bytes".parse().unwrap());
                headers.insert(
                    header::CONTENT_RANGE,
                    format!("bytes */{}", file_size).parse().unwrap(),
                );
                return Ok((StatusCode::RANGE_NOT_SATISFIABLE, headers).into_response());
            }
            Err(RangeParseError::Malformed) => None, // ignore malformed → full response
        },
        None => None,
    };

    // Build headers using infallible `HeaderValue` constructors where
    // possible. `content_type` is user-influenced (multipart part header
    // at upload time), so parse it defensively and fall back to the
    // static octet-stream default when it contains bytes that are
    // illegal in a response header.
    let mut headers = HeaderMap::new();
    let ct_value = content_type
        .parse()
        .unwrap_or_else(|_| axum::http::HeaderValue::from_static("application/octet-stream"));
    headers.insert(header::CONTENT_TYPE, ct_value);
    headers.insert(
        header::CONTENT_DISPOSITION,
        content_disposition_attachment(&meta.file_name),
    );
    headers.insert(
        header::ACCEPT_RANGES,
        axum::http::HeaderValue::from_static("bytes"),
    );

    if let Some((start, end)) = range_req {
        // 206 Partial Content
        let e = engine.clone();
        let fid = file_id.clone();
        let data = tokio::task::spawn_blocking(move || e.get_range(&fid, start, end))
            .await
            .map_err(|e| AppError::internal(e.to_string()))?
            .map_err(AppError::from_jihuan)?;

        headers.insert(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", start, end, file_size)
                .parse()
                .unwrap(),
        );
        headers.insert(
            header::CONTENT_LENGTH,
            data.len().to_string().parse().unwrap(),
        );
        Ok((StatusCode::PARTIAL_CONTENT, headers, Body::from(data)).into_response())
    } else {
        // 200 OK — whole file
        let e = engine.clone();
        let fid = file_id.clone();
        let data = tokio::task::spawn_blocking(move || e.get_bytes(&fid))
            .await
            .map_err(|e| AppError::internal(e.to_string()))?
            .map_err(AppError::from_jihuan)?;

        headers.insert(
            header::CONTENT_LENGTH,
            data.len().to_string().parse().unwrap(),
        );
        Ok((StatusCode::OK, headers, Body::from(data)).into_response())
    }
}

#[derive(Debug)]
enum RangeParseError {
    /// `Range: bytes=...,...` — multi-range is not supported
    Multi,
    /// Range is syntactically valid but cannot be satisfied (e.g. start>=size)
    Unsatisfiable,
    /// Header does not match `bytes=...` grammar — treat as "no range".
    Malformed,
}

/// Parse a single-range `Range` header against `file_size`.
///
/// Returns `(start, end)` where both ends are inclusive byte offsets.
/// Supports:
///   • `bytes=a-b`  →  [a, b]
///   • `bytes=a-`   →  [a, file_size-1]
///   • `bytes=-n`   →  last `n` bytes (suffix)
fn parse_single_byte_range(header: &str, file_size: u64) -> Result<(u64, u64), RangeParseError> {
    let spec = header
        .trim()
        .strip_prefix("bytes=")
        .ok_or(RangeParseError::Malformed)?
        .trim();

    if spec.contains(',') {
        return Err(RangeParseError::Multi);
    }

    let (a, b) = spec.split_once('-').ok_or(RangeParseError::Malformed)?;
    let a = a.trim();
    let b = b.trim();

    if file_size == 0 {
        return Err(RangeParseError::Unsatisfiable);
    }

    let (start, end) = match (a.is_empty(), b.is_empty()) {
        (true, true) => return Err(RangeParseError::Malformed),
        (true, false) => {
            // Suffix: last N bytes
            let n: u64 = b.parse().map_err(|_| RangeParseError::Malformed)?;
            if n == 0 {
                return Err(RangeParseError::Unsatisfiable);
            }
            let start = file_size.saturating_sub(n);
            (start, file_size - 1)
        }
        (false, true) => {
            let start: u64 = a.parse().map_err(|_| RangeParseError::Malformed)?;
            (start, file_size - 1)
        }
        (false, false) => {
            let start: u64 = a.parse().map_err(|_| RangeParseError::Malformed)?;
            let end: u64 = b.parse().map_err(|_| RangeParseError::Malformed)?;
            (start, end)
        }
    };

    if start > end || start >= file_size {
        return Err(RangeParseError::Unsatisfiable);
    }
    // Clamp end to last byte
    Ok((start, end.min(file_size - 1)))
}

/// DELETE /api/v1/files/:file_id
pub async fn delete_file(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(file_id): Path<String>,
) -> Result<StatusCode, AppError> {
    require_scope(&caller, "write")?;
    tokio::task::spawn_blocking(move || engine.delete_file(&file_id))
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?;

    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/v1/files/:file_id/meta
pub async fn get_file_meta(
    State(engine): State<Arc<Engine>>,
    caller: AuthedKey,
    Path(file_id): Path<String>,
) -> Result<Json<FileMetaResponse>, AppError> {
    require_scope(&caller, "read")?;
    tracing::debug!(file_id = %file_id, "get_file_meta called");
    let fid = file_id.clone();
    let meta = tokio::task::spawn_blocking(move || {
        tracing::debug!(fid = %fid, "querying engine.get_file_meta");
        engine.get_file_meta(&fid)
    })
        .await
        .map_err(|e| AppError::internal(e.to_string()))?
        .map_err(AppError::from_jihuan)?
        .ok_or_else(|| AppError::not_found(&file_id))?;

    let chunks: Vec<ChunkInfoDto> = meta
        .chunks
        .iter()
        .map(|c| ChunkInfoDto {
            index: c.index,
            block_id: c.block_id.clone(),
            offset: c.offset,
            original_size: c.original_size,
            compressed_size: c.compressed_size,
            hash: c.hash.clone(),
        })
        .collect();

    Ok(Json(FileMetaResponse {
        file_id: meta.file_id,
        file_name: meta.file_name,
        file_size: meta.file_size,
        create_time: meta.create_time,
        content_type: meta.content_type,
        chunk_count: meta.chunks.len(),
        chunks: Some(chunks),
        partition_id: Some(meta.partition_id),
    }))
}

// ─── Error Handling ───────────────────────────────────────────────────────────

pub struct AppError {
    pub status: StatusCode,
    pub message: String,
}

impl AppError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }

    pub fn not_found(id: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: format!("File '{}' not found", id),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }

    pub fn conflict(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: msg.into(),
        }
    }

    pub fn from_jihuan(e: JiHuanError) -> Self {
        match e {
            JiHuanError::NotFound(msg) => Self {
                status: StatusCode::NOT_FOUND,
                message: msg,
            },
            JiHuanError::AlreadyExists(msg) => Self {
                status: StatusCode::CONFLICT,
                message: msg,
            },
            JiHuanError::InvalidArgument(msg) => Self {
                status: StatusCode::BAD_REQUEST,
                message: msg,
            },
            // 507 Insufficient Storage is the standard code for "server knows
            // the request is valid but cannot store the representation". Maps
            // cleanly onto our configured `max_storage_bytes` cap.
            e @ JiHuanError::StorageFull { .. } => Self {
                status: StatusCode::INSUFFICIENT_STORAGE,
                message: e.to_string(),
            },
            other => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: other.to_string(),
            },
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let code = self.status.as_u16();
        let body = Json(ErrorResponse {
            error: self.message,
            code,
        });
        (self.status, body).into_response()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────
// Kept at the very bottom of the file to satisfy clippy's
// `items_after_test_module`: any `pub fn` / `impl` block declared *after*
// a `#[cfg(test)] mod` would otherwise live in a compilation unit the
// test harness ignores.

#[cfg(test)]
mod range_tests {
    use super::*;

    #[test]
    fn parse_basic() {
        assert_eq!(parse_single_byte_range("bytes=0-99", 1000).unwrap(), (0, 99));
        assert_eq!(
            parse_single_byte_range("bytes=500-", 1000).unwrap(),
            (500, 999)
        );
        assert_eq!(parse_single_byte_range("bytes=-100", 1000).unwrap(), (900, 999));
        // Clamps end
        assert_eq!(
            parse_single_byte_range("bytes=0-9999", 1000).unwrap(),
            (0, 999)
        );
    }

    /// Security regression: malicious filenames (CR/LF for response
    /// splitting, quotes for `Content-Disposition` quoted-string
    /// injection, Unicode for header byte-range violations) must never
    /// propagate into the response header unchanged. We also verify the
    /// RFC 5987 `filename*=UTF-8''` extended form is attached so modern
    /// browsers render the original Unicode.
    #[test]
    fn content_disposition_sanitises_hostile_filenames() {
        // Plain ASCII passes through.
        let v = content_disposition_attachment("hello.txt");
        assert_eq!(
            v.to_str().unwrap(),
            "attachment; filename=\"hello.txt\"; filename*=UTF-8''hello.txt"
        );

        // CR/LF must be dropped before the header value is constructed.
        let v = content_disposition_attachment("evil\r\nX-Injected: pwn\r\n");
        let s = v.to_str().unwrap();
        assert!(!s.contains('\r'), "carriage return leaked: {:?}", s);
        assert!(!s.contains('\n'), "newline leaked: {:?}", s);

        // Embedded quote / backslash are downgraded to `_` in the ASCII
        // fallback; the extended form percent-encodes them.
        let v = content_disposition_attachment("a\"b\\c");
        let s = v.to_str().unwrap();
        assert!(s.starts_with("attachment; filename=\"a_b_c\";"));
        assert!(s.contains("filename*=UTF-8''a%22b%5Cc"));

        // Non-ASCII Unicode survives via the extended form.
        let v = content_disposition_attachment("中文.pdf");
        let s = v.to_str().unwrap();
        assert!(s.contains("filename*=UTF-8''%E4%B8%AD%E6%96%87.pdf"));
    }

    #[test]
    fn parse_rejects_invalid() {
        assert!(matches!(
            parse_single_byte_range("bytes=0-100,200-300", 1000),
            Err(RangeParseError::Multi)
        ));
        assert!(matches!(
            parse_single_byte_range("bytes=1000-2000", 1000),
            Err(RangeParseError::Unsatisfiable)
        ));
        assert!(matches!(
            parse_single_byte_range("items=0-10", 1000),
            Err(RangeParseError::Malformed)
        ));
        assert!(matches!(
            parse_single_byte_range("bytes=5-3", 1000),
            Err(RangeParseError::Unsatisfiable)
        ));
    }
}
