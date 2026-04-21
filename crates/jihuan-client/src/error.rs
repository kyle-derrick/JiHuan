//! Client error surface. Intentionally smaller than
//! [`jihuan_core::JiHuanError`] so that HTTP / cluster backends can map
//! their transport errors into the same shape without leaking internals.

use thiserror::Error;

pub type ClientResult<T> = Result<T, ClientError>;

#[derive(Debug, Error)]
pub enum ClientError {
    /// The requested file id does not exist.
    #[error("file not found: {0}")]
    NotFound(String),

    /// The underlying storage backend rejected the request for reasons
    /// other than "missing" — e.g. validation failure, quota exceeded,
    /// I/O error. The string is human-readable; machine-parseable
    /// variants may be added in v0.5.1+.
    #[error("storage backend error: {0}")]
    Backend(String),

    /// Transport-level failure (network timeout, TLS handshake, etc.).
    /// Never produced by [`crate::EmbeddedClient`] — reserved for HTTP /
    /// cluster backends. Keeping this variant visible in the embedded
    /// path lets callers write one match arm that covers every backend.
    #[error("transport error: {0}")]
    Transport(String),

    /// A supplied argument was rejected up-front. Typically this means
    /// the caller passed an empty file name, an invalid file id format,
    /// or a content-type that failed validation.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
}

impl From<jihuan_core::JiHuanError> for ClientError {
    fn from(e: jihuan_core::JiHuanError) -> Self {
        use jihuan_core::JiHuanError as E;
        match e {
            E::NotFound(m) => Self::NotFound(m),
            E::InvalidArgument(m) => Self::InvalidArgument(m),
            other => Self::Backend(other.to_string()),
        }
    }
}
