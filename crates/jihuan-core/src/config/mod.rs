use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::{JiHuanError, Result};

/// Hash algorithm for chunk deduplication
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum HashAlgorithm {
    Md5,
    Sha1,
    #[default]
    Sha256,
    None,
}

impl std::fmt::Display for HashAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HashAlgorithm::Md5 => write!(f, "md5"),
            HashAlgorithm::Sha1 => write!(f, "sha1"),
            HashAlgorithm::Sha256 => write!(f, "sha256"),
            HashAlgorithm::None => write!(f, "none"),
        }
    }
}

/// Compression algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CompressionAlgorithm {
    None,
    Lz4,
    #[default]
    Zstd,
}

impl std::fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionAlgorithm::None => write!(f, "none"),
            CompressionAlgorithm::Lz4 => write!(f, "lz4"),
            CompressionAlgorithm::Zstd => write!(f, "zstd"),
        }
    }
}

fn default_block_file_size() -> u64 {
    1024 * 1024 * 1024 // 1GB
}

fn default_chunk_size() -> u64 {
    4 * 1024 * 1024 // 4MB
}

fn default_compression_level() -> i32 {
    1
}

fn default_time_partition_hours() -> u32 {
    24
}

fn default_gc_threshold() -> f64 {
    0.7
}

fn default_gc_interval_secs() -> u64 {
    300 // 5 minutes
}

fn default_max_open_block_files() -> usize {
    64
}

fn default_worker_threads() -> usize {
    num_cpus::get()
}

fn default_http_addr() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_grpc_addr() -> String {
    "0.0.0.0:8081".to_string()
}

fn default_metrics_addr() -> String {
    "0.0.0.0:9090".to_string()
}

fn default_true() -> bool {
    true
}

/// Core storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Data directory for block files
    pub data_dir: PathBuf,

    /// Metadata directory for redb database
    pub meta_dir: PathBuf,

    /// WAL directory
    pub wal_dir: PathBuf,

    /// Maximum size per block file in bytes
    #[serde(default = "default_block_file_size")]
    pub block_file_size: u64,

    /// Chunk size in bytes
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u64,

    /// Hash algorithm for deduplication
    #[serde(default)]
    pub hash_algorithm: HashAlgorithm,

    /// Compression algorithm
    #[serde(default)]
    pub compression_algorithm: CompressionAlgorithm,

    /// Compression level (1-22 for zstd, ignored for lz4/none)
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,

    /// Time partition granularity in hours
    #[serde(default = "default_time_partition_hours")]
    pub time_partition_hours: u32,

    /// Disk usage threshold to trigger auto GC (0.0 - 1.0)
    #[serde(default = "default_gc_threshold")]
    pub gc_threshold: f64,

    /// GC background scan interval in seconds
    #[serde(default = "default_gc_interval_secs")]
    pub gc_interval_secs: u64,

    /// Maximum number of concurrently open block files
    #[serde(default = "default_max_open_block_files")]
    pub max_open_block_files: usize,

    /// Enable data integrity verification on read
    #[serde(default = "default_true")]
    pub verify_on_read: bool,

    /// Hard quota on total bytes stored under `data_dir`. `None` (the TOML
    /// default when the key is omitted) means unlimited — the engine will
    /// keep accepting uploads until the underlying filesystem runs out of
    /// space. When set, uploads that would push the current disk usage
    /// above this value are rejected up-front with `StorageFull`.
    ///
    /// Tip: this is a soft cap — the check runs once at the start of each
    /// upload. Concurrent uploads can each see the same "available" number
    /// and all succeed, so leave headroom below the real filesystem limit.
    #[serde(default)]
    pub max_storage_bytes: Option<u64>,
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// HTTP listen address
    #[serde(default = "default_http_addr")]
    pub http_addr: String,

    /// gRPC listen address
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String,

    /// Prometheus metrics listen address
    #[serde(default = "default_metrics_addr")]
    pub metrics_addr: String,

    /// Number of worker threads (defaults to CPU count)
    #[serde(default = "default_worker_threads")]
    pub worker_threads: usize,

    /// Maximum request body size in bytes (default: 2GB)
    #[serde(default)]
    pub max_body_size: Option<u64>,

    /// Enable request/response logging
    #[serde(default = "default_true")]
    pub enable_access_log: bool,

    /// Allowed CORS origins. Empty (default) = same-origin only; in that
    /// mode no `Access-Control-Allow-Origin` header is emitted. Use
    /// explicit origins like `https://ui.example.com` for cross-site UIs.
    /// The sentinel `"*"` enables `Any` (dev only — never combine with
    /// credentials).
    #[serde(default = "default_cors_origins")]
    pub cors_origins: Vec<String>,
}

fn default_audit_retention_days() -> u64 {
    90
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Whether API key authentication is required.
    /// Default: **true**. On first boot with no keys, the server generates a
    /// bootstrap admin key and prints it once to stdout.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Routes that are exempt from authentication (prefix match).
    /// Defaults to the endpoints strictly needed before the user can
    /// authenticate: `/healthz`, `/readyz`, `/api/auth/login`, `/api/metrics`,
    /// plus the SPA bundle at `/ui` and the public status page.
    #[serde(default = "default_exempt_routes")]
    pub exempt_routes: Vec<String>,

    /// How long to retain audit-log rows before the background GC purges
    /// them. Default 90 days. Set to 0 to disable purging (keep everything
    /// forever — useful in compliance-heavy environments that ship audit
    /// events off-box before pruning).
    #[serde(default = "default_audit_retention_days")]
    pub audit_retention_days: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            exempt_routes: default_exempt_routes(),
            audit_retention_days: default_audit_retention_days(),
        }
    }
}

fn default_exempt_routes() -> Vec<String> {
    vec![
        "/healthz".to_string(),
        "/readyz".to_string(),
        "/api/auth/login".to_string(),
        "/api/metrics".to_string(),
        "/api/status".to_string(),
        "/ui".to_string(),
    ]
}

/// Default for [`ServerConfig::cors_origins`]: empty = same-origin only.
fn default_cors_origins() -> Vec<String> {
    Vec::new()
}

/// Top-level application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub storage: StorageConfig,
    pub server: ServerConfig,
    #[serde(default)]
    pub auth: AuthConfig,
}

impl AppConfig {
    /// Load configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            JiHuanError::Config(format!(
                "Failed to read config file '{}': {}",
                path.as_ref().display(),
                e
            ))
        })?;
        Self::from_toml_str(&content)
    }

    /// Parse configuration from a TOML string
    pub fn from_toml_str(content: &str) -> Result<Self> {
        let cfg: AppConfig = toml::from_str(content)
            .map_err(|e| JiHuanError::Config(format!("Failed to parse config: {}", e)))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<()> {
        let s = &self.storage;

        if s.chunk_size == 0 {
            return Err(JiHuanError::Config("chunk_size must be > 0".into()));
        }
        if s.block_file_size < s.chunk_size {
            return Err(JiHuanError::Config(
                "block_file_size must be >= chunk_size".into(),
            ));
        }
        if !(0.0..=1.0).contains(&s.gc_threshold) {
            return Err(JiHuanError::Config(
                "gc_threshold must be between 0.0 and 1.0".into(),
            ));
        }
        if s.compression_level < 0 || s.compression_level > 22 {
            return Err(JiHuanError::Config(
                "compression_level must be between 0 and 22".into(),
            ));
        }
        Ok(())
    }

    /// Default general-purpose configuration (recommended)
    pub fn default_general() -> Self {
        ConfigTemplate::general(PathBuf::from("./jihuan-data"))
    }
}

/// Pre-built configuration templates for common scenarios
pub struct ConfigTemplate;

impl ConfigTemplate {
    /// General purpose (balanced safety, speed, space)
    pub fn general(data_dir: PathBuf) -> AppConfig {
        let mut cfg = Self::base(data_dir);
        cfg.storage.block_file_size = 1024 * 1024 * 1024;
        cfg.storage.chunk_size = 4 * 1024 * 1024;
        cfg.storage.hash_algorithm = HashAlgorithm::Sha256;
        cfg.storage.compression_algorithm = CompressionAlgorithm::Zstd;
        cfg.storage.compression_level = 1;
        cfg.storage.time_partition_hours = 24;
        cfg.storage.gc_threshold = 0.7;
        cfg
    }

    /// Extreme speed (hot data, low latency, don't care about space)
    pub fn speed(data_dir: PathBuf) -> AppConfig {
        let mut cfg = Self::base(data_dir);
        cfg.storage.block_file_size = 512 * 1024 * 1024;
        cfg.storage.chunk_size = 8 * 1024 * 1024;
        cfg.storage.hash_algorithm = HashAlgorithm::Md5;
        cfg.storage.compression_algorithm = CompressionAlgorithm::None;
        cfg.storage.compression_level = 0;
        cfg.storage.time_partition_hours = 12;
        cfg.storage.gc_threshold = 0.8;
        cfg
    }

    /// Extreme space (cold archival data, max compression)
    pub fn space(data_dir: PathBuf) -> AppConfig {
        let mut cfg = Self::base(data_dir);
        cfg.storage.block_file_size = 2 * 1024 * 1024 * 1024;
        cfg.storage.chunk_size = 4 * 1024 * 1024;
        cfg.storage.hash_algorithm = HashAlgorithm::Sha256;
        cfg.storage.compression_algorithm = CompressionAlgorithm::Zstd;
        cfg.storage.compression_level = 9;
        cfg.storage.time_partition_hours = 72;
        cfg.storage.gc_threshold = 0.6;
        cfg
    }

    /// Small files (90%+ files under 10KB)
    pub fn small_files(data_dir: PathBuf) -> AppConfig {
        let mut cfg = Self::base(data_dir);
        cfg.storage.block_file_size = 512 * 1024 * 1024;
        cfg.storage.chunk_size = 2 * 1024 * 1024;
        cfg.storage.hash_algorithm = HashAlgorithm::Sha256;
        cfg.storage.compression_algorithm = CompressionAlgorithm::Zstd;
        cfg.storage.compression_level = 3;
        cfg.storage.time_partition_hours = 24;
        cfg.storage.gc_threshold = 0.7;
        cfg
    }

    /// Large files (90%+ files over 100MB)
    pub fn large_files(data_dir: PathBuf) -> AppConfig {
        let mut cfg = Self::base(data_dir);
        cfg.storage.block_file_size = 2 * 1024 * 1024 * 1024;
        cfg.storage.chunk_size = 8 * 1024 * 1024;
        cfg.storage.hash_algorithm = HashAlgorithm::Sha256;
        cfg.storage.compression_algorithm = CompressionAlgorithm::Zstd;
        cfg.storage.compression_level = 1;
        cfg.storage.time_partition_hours = 72;
        cfg.storage.gc_threshold = 0.8;
        cfg
    }

    fn base(data_dir: PathBuf) -> AppConfig {
        AppConfig {
            storage: StorageConfig {
                data_dir: data_dir.join("data"),
                meta_dir: data_dir.join("meta"),
                wal_dir: data_dir.join("wal"),
                block_file_size: default_block_file_size(),
                chunk_size: default_chunk_size(),
                hash_algorithm: HashAlgorithm::Sha256,
                compression_algorithm: CompressionAlgorithm::Zstd,
                compression_level: 1,
                time_partition_hours: 24,
                gc_threshold: 0.7,
                gc_interval_secs: 300,
                max_open_block_files: 64,
                verify_on_read: true,
                max_storage_bytes: None,
            },
            server: ServerConfig {
                http_addr: default_http_addr(),
                grpc_addr: default_grpc_addr(),
                metrics_addr: default_metrics_addr(),
                worker_threads: default_worker_threads(),
                max_body_size: None,
                enable_access_log: true,
                cors_origins: default_cors_origins(),
            },
            auth: AuthConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_config_template_general() {
        let dir = tempdir().unwrap();
        let cfg = ConfigTemplate::general(dir.path().to_path_buf());
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.storage.chunk_size, 4 * 1024 * 1024);
        assert_eq!(cfg.storage.hash_algorithm, HashAlgorithm::Sha256);
    }

    #[test]
    fn test_config_template_speed() {
        let dir = tempdir().unwrap();
        let cfg = ConfigTemplate::speed(dir.path().to_path_buf());
        assert!(cfg.validate().is_ok());
        assert_eq!(
            cfg.storage.compression_algorithm,
            CompressionAlgorithm::None
        );
        assert_eq!(cfg.storage.hash_algorithm, HashAlgorithm::Md5);
    }

    #[test]
    fn test_config_validation_fails_invalid_gc_threshold() {
        let dir = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(dir.path().to_path_buf());
        cfg.storage.gc_threshold = 1.5;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_validation_fails_chunk_larger_than_block() {
        let dir = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(dir.path().to_path_buf());
        cfg.storage.chunk_size = cfg.storage.block_file_size + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_config_from_toml_str() {
        let dir = tempdir().unwrap();
        let data_path = dir.path().display().to_string().replace('\\', "/");
        let toml = format!(
            r#"
[storage]
data_dir = "{}/data"
meta_dir = "{}/meta"
wal_dir = "{}/wal"
block_file_size = 1073741824
chunk_size = 4194304
hash_algorithm = "sha256"
compression_algorithm = "zstd"
compression_level = 1
time_partition_hours = 24
gc_threshold = 0.7

[server]
http_addr = "0.0.0.0:8080"
grpc_addr = "0.0.0.0:8081"
metrics_addr = "0.0.0.0:9090"
"#,
            data_path, data_path, data_path
        );
        let cfg = AppConfig::from_toml_str(&toml);
        assert!(cfg.is_ok(), "{:?}", cfg.err());
    }
}
