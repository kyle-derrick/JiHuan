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

fn default_scrub_interval_hours() -> u64 {
    168 // weekly; 0 disables the scheduler
}

fn default_auto_compact_threshold() -> f64 {
    0.5
}

fn default_auto_compact_every_gc_ticks() -> u32 {
    12 // with 5-min GC tick → once per hour
}

fn default_auto_compact_undersize_ratio() -> f64 {
    // v0.4.7: Strategy B candidate gate. A sealed block whose absolute
    // size is less than `block_file_size * undersize_ratio` is eligible
    // for compaction even when its utilisation is high. Drives the
    // "merge many small sealed blocks" axis that the low-utilisation
    // scanner cannot see. 0.25 means "blocks below 25% of the
    // configured block_file_size are candidates".
    0.25
}

fn default_auto_compact_min_file_saved() -> u64 {
    // v0.4.7: Strategy B commit gate. A group is committed when it
    // reduces the on-disk sealed-block count by at least this many
    // files (file_count_saved = source_count − ceil(live_sum /
    // block_file_size)). 3 means "only commit if at least 4 sources
    // collapse into 1 (or equivalent)". Zero disables the gate
    // entirely.
    3
}

fn default_auto_compact_min_block_age_secs() -> u64 {
    // v0.4.7: anti-thrash filter. Blocks younger than this are
    // excluded from both Strategy A and Strategy B candidate
    // predicates. Prevents (a) freshly-sealed blocks from being
    // compacted before they accumulate content, and (b) a merge
    // output that's itself small from being re-selected on the next
    // tick → infinite thrash loop. 3600 s = one scheduling cycle with
    // the default gc_interval * every_gc_ticks (300 s * 12).
    3600
}

fn default_auto_compact_disk_headroom_bytes() -> u64 {
    // v0.4.5: before writing a merge group we check that the filesystem
    // has at least (group_live_bytes + this much) free. Protects against
    // the edge case where compaction would tip the disk over the edge
    // while the old blocks are still on disk (they're only deleted after
    // the new block is committed). 512 MiB default — enough for one
    // block_file_size plus a little slack on the 1 GiB default.
    512 * 1024 * 1024
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

    /// Phase 4.5 follow-up — run [`Engine::scrub`] automatically every
    /// N hours. `0` disables the scheduler (operators must `POST
    /// /api/admin/scrub` manually). The scan is I/O-bound (~1 GB/s on
    /// warm SSD); pick a cadence that finishes well inside one
    /// interval for your data size. Default: **168** (weekly).
    #[serde(default = "default_scrub_interval_hours")]
    pub scrub_interval_hours: u64,

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

    /// Auto-compaction master switch. When enabled, every
    /// `auto_compact_every_gc_ticks` GC passes runs the dual-strategy
    /// scanner to reclaim fragmented space (Strategy A:
    /// low-utilisation) and merge undersized sealed blocks
    /// (Strategy B: undersized) behind the scenes. Disabled by default
    /// because compaction can touch large amounts of data — operators
    /// opt in when they care more about disk efficiency than peak CPU.
    #[serde(default)]
    pub auto_compact_enabled: bool,

    /// **Strategy A gate** — compact any sealed block whose `live_bytes
    /// / size` is below this ratio. `0.5` means "compact any block
    /// more than half-empty". This is the *only* knob for Strategy A:
    /// a group containing any A-candidate is always committed; tune
    /// this down (e.g. to 0.2) to require a higher dead-byte ratio
    /// before a block is deemed worth rewriting.
    #[serde(default = "default_auto_compact_threshold")]
    pub auto_compact_threshold: f64,

    /// Run auto-compaction every N GC ticks. `1` = every tick
    /// (aggressive); larger values amortise the I/O cost. Ignored
    /// when `auto_compact_enabled` is false.
    #[serde(default = "default_auto_compact_every_gc_ticks")]
    pub auto_compact_every_gc_ticks: u32,

    /// **Strategy B candidate gate** (v0.4.7) — sealed blocks whose
    /// absolute size is below `block_file_size * undersize_ratio` are
    /// candidates regardless of utilisation. Addresses the
    /// "many small sealed blocks" drift that Strategy A cannot see.
    /// `0.25` is a good middle-of-the-road default.
    #[serde(default = "default_auto_compact_undersize_ratio")]
    pub auto_compact_undersize_ratio: f64,

    /// **Strategy B commit gate** (v0.4.7) — a group must reduce the
    /// sealed-block count by at least this many files to be
    /// committed. Formally:
    /// `file_count_saved = source_count − ceil(live_sum / block_file_size)`.
    /// Zero disables the gate. `3` = at least 4 sources collapse to 1.
    #[serde(default = "default_auto_compact_min_file_saved")]
    pub auto_compact_min_file_saved: u64,

    /// **Anti-thrash filter** (v0.4.7) — blocks younger than this
    /// are excluded from *both* Strategy A and Strategy B candidate
    /// predicates. Prevents freshly-sealed blocks and merge outputs
    /// from being immediately re-selected. Zero disables the check
    /// (not recommended in production).
    #[serde(default = "default_auto_compact_min_block_age_secs")]
    pub auto_compact_min_block_age_secs: u64,

    /// Minimum filesystem free-space headroom (bytes) required to
    /// start a compaction group. Checked as `available(data_dir) >=
    /// group_live_bytes + auto_compact_disk_headroom_bytes`. Groups
    /// that don't fit are skipped — individual compactions don't
    /// partially apply. Zero disables the check. This safety check
    /// is orthogonal to Strategy A/B gates and is *not* bypassed by
    /// the admin `force=true` flag.
    #[serde(default = "default_auto_compact_disk_headroom_bytes")]
    pub auto_compact_disk_headroom_bytes: u64,

    /// Phase 4.4 — WAL checkpoint + rotation settings. Defaults are
    /// conservative (5-minute checkpoint, 64 MiB rotation threshold)
    /// so idle deployments don't fsync for nothing while long-lived
    /// write-heavy instances still see their WAL truncated before it
    /// grows unbounded.
    #[serde(default)]
    pub wal: WalConfig,
}

/// Phase 4.4 — Write-Ahead-Log runtime parameters.
///
/// The WAL mirrors every metadata mutation before the redb transaction
/// commits. Once redb is durable the WAL entry is redundant, so we
/// periodically truncate ("checkpoint") the log to keep its on-disk
/// footprint bounded. Two independent triggers:
///
/// * **Time-based** (`checkpoint_interval_secs`): wake every N seconds
///   and checkpoint if the WAL contains at least one entry. Handles
///   the steady-state low-write case.
/// * **Size-based** (`max_file_size_mb`): checkpoint immediately once
///   the file exceeds this threshold. Handles bursty writes that
///   would otherwise blow past the time budget.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Master switch. Default: **true** (checkpoint loop runs).
    /// Set to `false` for workloads that rely on external snapshots
    /// and want a fully growing WAL for forensic replay.
    #[serde(default = "default_wal_checkpoint_enabled")]
    pub checkpoint_enabled: bool,

    /// How often the background task considers checkpointing.
    /// `0` disables the periodic trigger (size-based still fires).
    #[serde(default = "default_wal_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,

    /// Size threshold in megabytes above which the WAL is
    /// checkpointed on the next tick. `0` disables the size trigger
    /// (time-based still fires).
    #[serde(default = "default_wal_max_file_size_mb")]
    pub max_file_size_mb: u64,

    /// Phase 4.4 follow-up — number of rotated old WAL segments to
    /// keep on disk alongside the active file. `0` keeps the legacy
    /// in-place truncate behaviour (old contents discarded). Positive
    /// `N` renames `jihuan.wal` → `jihuan.wal.1` on each checkpoint,
    /// shifting existing `.1` → `.2` etc., pruning anything past
    /// `jihuan.wal.N`. The rotated segments are **not** replayed on
    /// startup — redb's per-transaction fsync is source of truth;
    /// the archives exist for forensic replay and compliance.
    #[serde(default = "default_wal_keep_old_logs")]
    pub keep_old_logs: u32,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            checkpoint_enabled: default_wal_checkpoint_enabled(),
            checkpoint_interval_secs: default_wal_checkpoint_interval_secs(),
            max_file_size_mb: default_wal_max_file_size_mb(),
            keep_old_logs: default_wal_keep_old_logs(),
        }
    }
}

fn default_wal_checkpoint_enabled() -> bool {
    true
}
fn default_wal_checkpoint_interval_secs() -> u64 {
    300
}
fn default_wal_max_file_size_mb() -> u64 {
    64
}
fn default_wal_keep_old_logs() -> u32 {
    3
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

    /// v0.4.5: per-request wall-clock timeout in seconds for non-upload
    /// HTTP routes. Guards against slow-loris clients, stuck backend
    /// tasks, and misbehaving reverse proxies that hold connections
    /// indefinitely. `0` disables the timeout (not recommended in
    /// production). Upload and download routes are intentionally exempt
    /// — streaming a multi-GB file can legitimately take much longer
    /// than the default.
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Phase 4.3 — per-IP rate limiting. Disabled by default so upgrades
    /// don't trip existing clients; flip `rate_limit.enabled = true` to
    /// turn on a token-bucket limiter that rejects bursts with HTTP
    /// 429 + `Retry-After`.
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
}

fn default_request_timeout_secs() -> u64 {
    30
}

/// Phase 4.3 — per-IP token-bucket rate limiting config.
///
/// The limiter runs at the tower layer, **before** the auth middleware,
/// so even unauthenticated flood traffic costs only the TCP accept +
/// one `Extension` lookup. Probe routes (`/healthz`, `/readyz`), the
/// metrics endpoint, and the login form are exempt from the limiter
/// so orchestrators and humans locked out by misconfiguration can
/// always get back in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Master switch. Default: **false**.
    #[serde(default)]
    pub enabled: bool,

    /// Steady-state allowance per client IP, requests per second.
    /// tower_governor models this as `per_second` → one token refills
    /// every `1/per_second` seconds.
    #[serde(default = "default_rate_per_ip_per_sec")]
    pub per_ip_per_sec: u32,

    /// Maximum burst size (initial bucket capacity).
    #[serde(default = "default_rate_burst")]
    pub burst: u32,

    /// Phase 4.3 follow-up — switch to `SmartIpKeyExtractor`, which
    /// reads `Forwarded` / `X-Forwarded-For` / `X-Real-IP` headers
    /// before falling back to the peer socket IP. **Leave `false` for
    /// direct-socket deployments** — otherwise an attacker can spoof
    /// the header to bypass the limiter. Flip to `true` only when the
    /// server is behind a trusted reverse proxy (Caddy/nginx/ALB)
    /// that *overwrites* the client-supplied header with the real
    /// peer IP.
    #[serde(default)] // defaults to false
    pub trust_forwarded_for: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            per_ip_per_sec: default_rate_per_ip_per_sec(),
            burst: default_rate_burst(),
            trust_forwarded_for: false,
        }
    }
}

fn default_rate_per_ip_per_sec() -> u32 {
    50
}

fn default_rate_burst() -> u32 {
    100
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

    /// v0.4.5: emit the `Secure` flag on session cookies so browsers only
    /// return them over HTTPS. **Leave `false` for plain-HTTP dev** —
    /// a `Secure` cookie over HTTP is silently dropped by every modern
    /// browser, breaking the UI login flow. Flip to `true` once the
    /// deployment is behind TLS (reverse proxy or direct).
    #[serde(default)] // defaults to false
    pub cookie_secure: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            exempt_routes: default_exempt_routes(),
            audit_retention_days: default_audit_retention_days(),
            cookie_secure: false,
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

/// TLS / HTTPS configuration (Phase 3).
///
/// When `enabled=true` the server terminates TLS for both HTTP and gRPC
/// listeners using the same certificate+key pair. When `enabled=false`
/// both listeners fall back to plaintext — which is the correct choice
/// behind a reverse proxy (Caddy / nginx / Traefik) that already does
/// TLS termination.
///
/// # Certificate sources
///
/// * **Static files** (`cert_path` + `key_path`): both PEM-encoded.
///   `cert_path` may contain a chain (leaf first); `key_path` holds a
///   single PKCS#8 or RSA private key.
/// * **`auto_selfsigned = true`** (dev-only escape hatch): the server
///   generates a short-lived self-signed cert for `localhost` / `127.0.0.1`
///   via the `rcgen` crate on every boot. Never enable this in
///   production — the cert is not persisted and clients will see
///   `ERR_CERT_AUTHORITY_INVALID` until they pin it manually.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Master switch. Default: **false** (plaintext).
    #[serde(default)]
    pub enabled: bool,

    /// PEM-encoded certificate chain (leaf-first). Empty ⇒ use
    /// `auto_selfsigned` or refuse to start when `enabled=true`.
    #[serde(default)]
    pub cert_path: String,

    /// PEM-encoded PKCS#8 or RSA private key.
    #[serde(default)]
    pub key_path: String,

    /// Dev-only: generate a self-signed certificate at startup for
    /// `localhost` / `127.0.0.1`. Mutually exclusive with non-empty
    /// `cert_path` (static files win).
    #[serde(default)]
    pub auto_selfsigned: bool,
}

/// Top-level application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub storage: StorageConfig,
    pub server: ServerConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    /// Phase 3 — TLS/HTTPS. Defaults to disabled so upgrades from
    /// v0.4.x remain plaintext-compatible; flip `tls.enabled = true`
    /// and supply a cert to move the single-port listener to HTTPS.
    #[serde(default)]
    pub tls: TlsConfig,
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
        // v0.4.7: pre-scan for deprecated keys so operators get a
        // clear migration error rather than a silently ignored
        // setting. We only reject the two keys removed in v0.4.7;
        // everything else goes through the regular deserializer
        // which allows unknown fields (forward compat with newer
        // engines).
        Self::reject_deprecated_v047_keys(content)?;
        let cfg: AppConfig = toml::from_str(content)
            .map_err(|e| JiHuanError::Config(format!("Failed to parse config: {}", e)))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// v0.4.7: scan raw TOML for two removed keys under `[storage]` and
    /// reject them with a migration-friendly error message. Matches
    /// both `storage.key = ...` inline syntax and `[storage]\nkey = ...`
    /// table syntax.
    fn reject_deprecated_v047_keys(content: &str) -> Result<()> {
        let value: toml::Value = content
            .parse()
            .map_err(|e| JiHuanError::Config(format!("Failed to parse config: {}", e)))?;
        let storage = match value.get("storage").and_then(|v| v.as_table()) {
            Some(t) => t,
            None => return Ok(()),
        };
        const DEPRECATED: &[(&str, &str)] = &[
            (
                "auto_compact_min_size_bytes",
                "superseded by `auto_compact_undersize_ratio` in v0.4.7 \
                 (see config/README.md § Auto-compaction). Delete the line to migrate.",
            ),
            (
                "auto_compact_min_benefit_bytes",
                "removed in v0.4.7 — Strategy A's `auto_compact_threshold` already controls \
                 commit aggressiveness (see config/README.md § Auto-compaction). \
                 Delete the line to migrate.",
            ),
        ];
        for (key, hint) in DEPRECATED {
            if storage.contains_key(*key) {
                return Err(JiHuanError::Config(format!(
                    "storage.{}: {}",
                    key, hint
                )));
            }
        }
        Ok(())
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
        if !(0.0..=1.0).contains(&s.auto_compact_threshold) {
            return Err(JiHuanError::Config(
                "auto_compact_threshold must be between 0.0 and 1.0".into(),
            ));
        }
        if !(0.0..=1.0).contains(&s.auto_compact_undersize_ratio) {
            return Err(JiHuanError::Config(
                "auto_compact_undersize_ratio must be between 0.0 and 1.0".into(),
            ));
        }

        // Phase 4.3 — rate-limit sanity: reject "enabled but zero rate"
        // (would reject every request) and "burst < 1" (bucket never
        // holds a token).
        let rl = &self.server.rate_limit;
        if rl.enabled {
            if rl.per_ip_per_sec == 0 {
                return Err(JiHuanError::Config(
                    "server.rate_limit.per_ip_per_sec must be > 0 when enabled".into(),
                ));
            }
            if rl.burst == 0 {
                return Err(JiHuanError::Config(
                    "server.rate_limit.burst must be > 0 when enabled".into(),
                ));
            }
        }

        // Phase 3 — TLS sanity: when enabled, operator must supply either
        // a static cert pair *or* opt in to auto-selfsigned. `cert_path`
        // and `key_path` must both be present together — a half-configured
        // pair is almost always a copy-paste error.
        let t = &self.tls;
        if t.enabled {
            let has_cert = !t.cert_path.trim().is_empty();
            let has_key = !t.key_path.trim().is_empty();
            if has_cert != has_key {
                return Err(JiHuanError::Config(
                    "tls.cert_path and tls.key_path must be set together".into(),
                ));
            }
            if !has_cert && !t.auto_selfsigned {
                return Err(JiHuanError::Config(
                    "tls.enabled = true but no certificate source configured: set \
                     tls.cert_path + tls.key_path, or tls.auto_selfsigned = true for dev"
                        .into(),
                ));
            }
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
                scrub_interval_hours: default_scrub_interval_hours(),
                max_open_block_files: 64,
                verify_on_read: true,
                max_storage_bytes: None,
                auto_compact_enabled: false,
                auto_compact_threshold: default_auto_compact_threshold(),
                auto_compact_every_gc_ticks: default_auto_compact_every_gc_ticks(),
                auto_compact_undersize_ratio: default_auto_compact_undersize_ratio(),
                auto_compact_min_file_saved: default_auto_compact_min_file_saved(),
                auto_compact_min_block_age_secs: default_auto_compact_min_block_age_secs(),
                auto_compact_disk_headroom_bytes: default_auto_compact_disk_headroom_bytes(),
                wal: WalConfig::default(),
            },
            server: ServerConfig {
                http_addr: default_http_addr(),
                grpc_addr: default_grpc_addr(),
                metrics_addr: default_metrics_addr(),
                worker_threads: default_worker_threads(),
                max_body_size: None,
                enable_access_log: true,
                cors_origins: default_cors_origins(),
                request_timeout_secs: default_request_timeout_secs(),
                rate_limit: RateLimitConfig::default(),
            },
            auth: AuthConfig::default(),
            tls: TlsConfig::default(),
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
    fn test_tls_defaults_disabled() {
        // Phase 3: plaintext-by-default so v0.4 configs keep working.
        let dir = tempdir().unwrap();
        let cfg = ConfigTemplate::general(dir.path().to_path_buf());
        assert!(!cfg.tls.enabled);
        assert!(cfg.tls.cert_path.is_empty());
        assert!(cfg.tls.key_path.is_empty());
        assert!(!cfg.tls.auto_selfsigned);
    }

    #[test]
    fn test_tls_validation_rejects_enabled_without_source() {
        let dir = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(dir.path().to_path_buf());
        cfg.tls.enabled = true;
        // Neither cert_path nor auto_selfsigned — must fail.
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("tls.enabled"), "got: {err}");
    }

    #[test]
    fn test_tls_validation_rejects_half_configured_pair() {
        let dir = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(dir.path().to_path_buf());
        cfg.tls.enabled = true;
        cfg.tls.cert_path = "/etc/jihuan/cert.pem".to_string();
        // key_path intentionally empty
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("cert_path") && err.contains("key_path"), "got: {err}");
    }

    #[test]
    fn test_scrub_scheduler_defaults_to_weekly() {
        // Phase 4.5 follow-up: default cadence is 168 h (weekly).
        // `0` would mean "disable the scheduler" — not the default.
        let dir = tempdir().unwrap();
        let cfg = ConfigTemplate::general(dir.path().to_path_buf());
        assert_eq!(cfg.storage.scrub_interval_hours, 168);
    }

    #[test]
    fn test_wal_defaults_enabled() {
        // Phase 4.4: WAL checkpoint loop is on by default so operators
        // don't need to opt in — an unbounded WAL is a worse default.
        let dir = tempdir().unwrap();
        let cfg = ConfigTemplate::general(dir.path().to_path_buf());
        assert!(cfg.storage.wal.checkpoint_enabled);
        assert_eq!(cfg.storage.wal.checkpoint_interval_secs, 300);
        assert_eq!(cfg.storage.wal.max_file_size_mb, 64);
        // Phase 4.4 follow-up: keep 3 rotated archives by default so
        // `jihuan.wal.1/.2/.3` are available for forensic replay.
        assert_eq!(cfg.storage.wal.keep_old_logs, 3);
    }

    #[test]
    fn test_rate_limit_defaults_disabled() {
        let dir = tempdir().unwrap();
        let cfg = ConfigTemplate::general(dir.path().to_path_buf());
        assert!(!cfg.server.rate_limit.enabled);
        assert_eq!(cfg.server.rate_limit.per_ip_per_sec, 50);
        assert_eq!(cfg.server.rate_limit.burst, 100);
        // Phase 4.3 follow-up: trust_forwarded_for defaults to false
        // (XFF spoofing is the default-deny posture for direct-socket
        // deployments; operators opt in after putting the server
        // behind a trusted reverse proxy).
        assert!(!cfg.server.rate_limit.trust_forwarded_for);
    }

    #[test]
    fn test_rate_limit_rejects_zero_rate() {
        let dir = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(dir.path().to_path_buf());
        cfg.server.rate_limit.enabled = true;
        cfg.server.rate_limit.per_ip_per_sec = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("per_ip_per_sec"), "got: {err}");
    }

    #[test]
    fn test_rate_limit_rejects_zero_burst() {
        let dir = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(dir.path().to_path_buf());
        cfg.server.rate_limit.enabled = true;
        cfg.server.rate_limit.burst = 0;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("burst"), "got: {err}");
    }

    #[test]
    fn test_tls_validation_accepts_auto_selfsigned() {
        let dir = tempdir().unwrap();
        let mut cfg = ConfigTemplate::general(dir.path().to_path_buf());
        cfg.tls.enabled = true;
        cfg.tls.auto_selfsigned = true;
        cfg.validate().unwrap();
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
