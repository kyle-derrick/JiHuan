use thiserror::Error;

#[derive(Debug, Error)]
pub enum JiHuanError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Metadata error: {0}")]
    Metadata(String),

    #[error("Block error: {0}")]
    Block(String),

    #[error("Chunk error: {0}")]
    Chunk(String),

    #[error("Dedup error: {0}")]
    Dedup(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("GC error: {0}")]
    Gc(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("Data corruption: {0}")]
    DataCorruption(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Storage full: available {available} bytes, needed {needed} bytes")]
    StorageFull { available: u64, needed: u64 },

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Concurrent modification: {0}")]
    ConcurrentModification(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<redb::Error> for JiHuanError {
    fn from(e: redb::Error) -> Self {
        JiHuanError::Database(e.to_string())
    }
}

impl From<redb::DatabaseError> for JiHuanError {
    fn from(e: redb::DatabaseError) -> Self {
        JiHuanError::Database(e.to_string())
    }
}

impl From<redb::TableError> for JiHuanError {
    fn from(e: redb::TableError) -> Self {
        JiHuanError::Database(e.to_string())
    }
}

impl From<redb::TransactionError> for JiHuanError {
    fn from(e: redb::TransactionError) -> Self {
        JiHuanError::Database(e.to_string())
    }
}

impl From<redb::CommitError> for JiHuanError {
    fn from(e: redb::CommitError) -> Self {
        JiHuanError::Database(e.to_string())
    }
}

impl From<redb::StorageError> for JiHuanError {
    fn from(e: redb::StorageError) -> Self {
        JiHuanError::Database(e.to_string())
    }
}

impl From<bincode::error::EncodeError> for JiHuanError {
    fn from(e: bincode::error::EncodeError) -> Self {
        JiHuanError::Serialization(e.to_string())
    }
}

impl From<bincode::error::DecodeError> for JiHuanError {
    fn from(e: bincode::error::DecodeError) -> Self {
        JiHuanError::Serialization(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, JiHuanError>;

/// Retry configuration for retryable operations
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 50,
            max_delay_ms: 2000,
            backoff_factor: 2.0,
        }
    }
}

impl RetryConfig {
    pub fn delay_for_attempt(&self, attempt: u32) -> u64 {
        let delay = self.initial_delay_ms as f64 * self.backoff_factor.powi(attempt as i32);
        delay.min(self.max_delay_ms as f64) as u64
    }
}

/// Determines if an error is retryable
pub fn is_retryable(err: &JiHuanError) -> bool {
    matches!(
        err,
        JiHuanError::Io(_) | JiHuanError::Timeout(_) | JiHuanError::ConcurrentModification(_)
    )
}

/// Retry an async operation with exponential backoff
pub async fn retry_async<F, Fut, T>(config: &RetryConfig, mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut last_err = None;
    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(v) => return Ok(v),
            Err(e) if is_retryable(&e) && attempt < config.max_retries => {
                let delay = config.delay_for_attempt(attempt);
                tracing::warn!(
                    attempt = attempt + 1,
                    max = config.max_retries,
                    delay_ms = delay,
                    error = %e,
                    "Retryable error, retrying after delay"
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                last_err = Some(e);
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_retry_config_delay() {
        let cfg = RetryConfig {
            initial_delay_ms: 50,
            backoff_factor: 2.0,
            max_delay_ms: 1000,
            max_retries: 3,
        };
        assert_eq!(cfg.delay_for_attempt(0), 50);
        assert_eq!(cfg.delay_for_attempt(1), 100);
        assert_eq!(cfg.delay_for_attempt(2), 200);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_retries() {
        let counter = Arc::new(AtomicU32::new(0));
        let cfg = RetryConfig {
            max_retries: 3,
            initial_delay_ms: 1,
            ..Default::default()
        };
        let result = retry_async(&cfg, || {
            let c = counter.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(JiHuanError::Io(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "transient",
                    )))
                } else {
                    Ok(n)
                }
            }
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);
    }

    #[test]
    fn test_is_retryable() {
        assert!(is_retryable(&JiHuanError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "broken"
        ))));
        assert!(!is_retryable(&JiHuanError::NotFound("x".into())));
    }
}
