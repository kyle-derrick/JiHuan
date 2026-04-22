//! HTTP-backed implementation of [`crate::StorageClient`].
//!
//! Talks to a running `jihuan-server` instance over REST. Serves as the
//! canonical remote backend and — until `ClusterClient` lands in v0.6 — is
//! also what production single-node deployments use from outside the
//! server process.
//!
//! Design notes:
//!
//! * The client holds a single `reqwest::Client` for connection pooling.
//! * Authentication is via `X-API-Key` header (see `jihuan-server`'s
//!   `AuthedKey` extractor). When the server was started with
//!   `auth.enabled = false` the key can be any value — pass an empty
//!   string.
//! * Error mapping converts transport failures (DNS, TLS, timeouts) into
//!   [`ClientError::Transport`], HTTP 404 into [`ClientError::NotFound`],
//!   HTTP 400 into [`ClientError::InvalidArgument`], and everything else
//!   into [`ClientError::Backend`]. This keeps the surface identical to
//!   [`EmbeddedClient`] so the contract tests pass unchanged.

use async_trait::async_trait;
use reqwest::{multipart, Client, StatusCode};
use serde::Deserialize;

use crate::error::{ClientError, ClientResult};
use crate::types::{FileId, FileInfo, PutOptions, StorageStats};
use crate::StorageClient;

/// HTTP backend talking to a `jihuan-server` REST API.
#[derive(Clone)]
pub struct HttpClient {
    base: String,
    api_key: Option<String>,
    http: Client,
}

impl HttpClient {
    /// Build a new client.
    ///
    /// `base_url` must include scheme and authority (e.g.
    /// `http://127.0.0.1:8080`). A trailing slash is stripped for you so
    /// both `.../` and `...` work. Pass `api_key = None` only when the
    /// remote server has `auth.enabled = false`.
    pub fn new(base_url: impl Into<String>, api_key: Option<String>) -> ClientResult<Self> {
        let base = base_url.into().trim_end_matches('/').to_string();
        if base.is_empty() {
            return Err(ClientError::InvalidArgument(
                "base_url must be a non-empty absolute URL".into(),
            ));
        }
        let http = Client::builder()
            .build()
            .map_err(|e| ClientError::Transport(format!("reqwest build: {e}")))?;
        Ok(Self {
            base,
            api_key,
            http,
        })
    }

    /// Attach auth header if we have a key. Kept as a helper so every
    /// call site looks identical.
    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.api_key {
            Some(k) => req.header("X-API-Key", k),
            None => req,
        }
    }

    /// Centralised status → ClientError conversion. `ctx` is a human
    /// description of the operation (e.g. `"get"`) for diagnostic
    /// surfacing only; it never leaks into machine-consumable variants.
    async fn map_status(
        resp: reqwest::Response,
        id_for_404: Option<&str>,
        ctx: &str,
    ) -> ClientResult<reqwest::Response> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp.text().await.unwrap_or_default();
        Err(match status {
            StatusCode::NOT_FOUND => {
                ClientError::NotFound(id_for_404.unwrap_or("(not specified)").to_string())
            }
            StatusCode::BAD_REQUEST => ClientError::InvalidArgument(body),
            s if s.is_client_error() || s.is_server_error() => {
                ClientError::Backend(format!("{ctx}: HTTP {status} — {}", body.trim()))
            }
            _ => ClientError::Backend(format!("{ctx}: unexpected HTTP {status}")),
        })
    }
}

// ── Response DTOs — subset of `jihuan-server::http::files` ───────────────────
//
// We deliberately redeclare these instead of importing from `jihuan-server`
// to keep `jihuan-client` free of the server binary's axum/tower tree.

#[derive(Debug, Deserialize)]
struct UploadResp {
    file_id: String,
    #[allow(dead_code)]
    file_name: String,
    #[allow(dead_code)]
    file_size: u64,
}

#[derive(Debug, Deserialize)]
struct FileMetaResp {
    file_id: String,
    file_name: String,
    file_size: u64,
    create_time: u64,
    content_type: Option<String>,
    chunk_count: usize,
}

#[derive(Debug, Deserialize)]
struct FileListResp {
    files: Vec<FileMetaResp>,
    #[allow(dead_code)]
    count: usize,
    #[allow(dead_code)]
    total: usize,
}

#[derive(Debug, Deserialize)]
struct StatusResp {
    file_count: u64,
    block_count: u64,
    #[serde(default)]
    logical_bytes: u64,
    #[serde(default)]
    disk_usage_bytes: u64,
    #[serde(default)]
    dedup_ratio: f64,
}

impl From<FileMetaResp> for FileInfo {
    fn from(m: FileMetaResp) -> Self {
        Self {
            file_id: FileId(m.file_id),
            file_name: m.file_name,
            file_size: m.file_size,
            content_type: m.content_type,
            create_time: m.create_time,
            chunk_count: m.chunk_count,
        }
    }
}

#[async_trait]
impl StorageClient for HttpClient {
    async fn put(&self, data: Vec<u8>, file_name: &str) -> ClientResult<FileId> {
        self.put_with_options(data, file_name, PutOptions::default())
            .await
    }

    async fn put_with_options(
        &self,
        data: Vec<u8>,
        file_name: &str,
        opts: PutOptions,
    ) -> ClientResult<FileId> {
        if file_name.is_empty() {
            return Err(ClientError::InvalidArgument(
                "file_name must not be empty".into(),
            ));
        }
        let mut part = multipart::Part::bytes(data).file_name(file_name.to_string());
        if let Some(ct) = &opts.content_type {
            part = part
                .mime_str(ct)
                .map_err(|e| ClientError::InvalidArgument(format!("content_type: {e}")))?;
        }
        let form = multipart::Form::new().part("file", part);

        let resp = self
            .auth(self.http.post(format!("{}/api/v1/files", self.base)))
            .multipart(form)
            .send()
            .await
            .map_err(|e| ClientError::Transport(format!("put: {e}")))?;
        let resp = Self::map_status(resp, None, "put").await?;
        let body: UploadResp = resp
            .json()
            .await
            .map_err(|e| ClientError::Backend(format!("put: decode response: {e}")))?;
        Ok(FileId(body.file_id))
    }

    async fn get(&self, file_id: &FileId) -> ClientResult<Vec<u8>> {
        let resp = self
            .auth(
                self.http
                    .get(format!("{}/api/v1/files/{}", self.base, file_id.0)),
            )
            .send()
            .await
            .map_err(|e| ClientError::Transport(format!("get: {e}")))?;
        let resp = Self::map_status(resp, Some(&file_id.0), "get").await?;
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ClientError::Backend(format!("get: read body: {e}")))?;
        Ok(bytes.to_vec())
    }

    async fn stat(&self, file_id: &FileId) -> ClientResult<FileInfo> {
        let resp = self
            .auth(
                self.http
                    .get(format!("{}/api/v1/files/{}/meta", self.base, file_id.0)),
            )
            .send()
            .await
            .map_err(|e| ClientError::Transport(format!("stat: {e}")))?;
        let resp = Self::map_status(resp, Some(&file_id.0), "stat").await?;
        let meta: FileMetaResp = resp
            .json()
            .await
            .map_err(|e| ClientError::Backend(format!("stat: decode response: {e}")))?;
        Ok(meta.into())
    }

    async fn delete(&self, file_id: &FileId) -> ClientResult<()> {
        let resp = self
            .auth(
                self.http
                    .delete(format!("{}/api/v1/files/{}", self.base, file_id.0)),
            )
            .send()
            .await
            .map_err(|e| ClientError::Transport(format!("delete: {e}")))?;
        Self::map_status(resp, Some(&file_id.0), "delete").await?;
        Ok(())
    }

    async fn list_files(&self) -> ClientResult<Vec<FileInfo>> {
        let resp = self
            .auth(self.http.get(format!("{}/api/v1/files", self.base)))
            .send()
            .await
            .map_err(|e| ClientError::Transport(format!("list: {e}")))?;
        let resp = Self::map_status(resp, None, "list").await?;
        let body: FileListResp = resp
            .json()
            .await
            .map_err(|e| ClientError::Backend(format!("list: decode response: {e}")))?;
        Ok(body.files.into_iter().map(FileInfo::from).collect())
    }

    async fn stats(&self) -> ClientResult<StorageStats> {
        // `/api/status` is exempt from auth, but sending the key doesn't hurt.
        let resp = self
            .auth(self.http.get(format!("{}/api/status", self.base)))
            .send()
            .await
            .map_err(|e| ClientError::Transport(format!("stats: {e}")))?;
        let resp = Self::map_status(resp, None, "stats").await?;
        let body: StatusResp = resp
            .json()
            .await
            .map_err(|e| ClientError::Backend(format!("stats: decode response: {e}")))?;
        Ok(StorageStats {
            file_count: body.file_count,
            block_count: body.block_count,
            logical_bytes: body.logical_bytes,
            disk_usage_bytes: body.disk_usage_bytes,
            dedup_ratio: body.dedup_ratio,
        })
    }
}
