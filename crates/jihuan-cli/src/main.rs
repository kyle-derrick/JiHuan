use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use serde::Deserialize;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use jihuan_core::config::AppConfig;

mod backup;

const DEFAULT_SERVER: &str = "http://127.0.0.1:8080";

#[derive(Parser, Debug)]
#[command(
    name = "jihuan",
    version,
    about = "JiHuan storage CLI — talks to a running jihuan-server via HTTP",
    long_about = None,
)]
struct Cli {
    /// Server base URL
    #[arg(
        long,
        env = "JIHUAN_SERVER",
        default_value = DEFAULT_SERVER,
        global = true
    )]
    server: String,

    /// API key for authentication (env: JIHUAN_API_KEY)
    #[arg(long, env = "JIHUAN_API_KEY", global = true)]
    api_key: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Upload a file
    Put {
        /// Path to the local file to upload
        file: PathBuf,
        /// Override the stored file name
        #[arg(long)]
        name: Option<String>,
        /// Content-Type header (e.g. image/jpeg)
        #[arg(long)]
        content_type: Option<String>,
        /// v0.4.6: caller-supplied file_id. S3-key style / UTF-8 (incl.
        /// Chinese) accepted; subject to NFC normalisation + length
        /// and path-safety rules. Omit for server-generated UUID.
        #[arg(long)]
        file_id: Option<String>,
        /// v0.4.6: action to take when `file_id` collides with an
        /// existing record. Default `error` (HTTP 409).
        #[arg(long, default_value = "error", value_parser = ["error", "skip", "overwrite"])]
        on_conflict: String,
    },
    /// Download a file by ID
    Get {
        /// File ID returned by `put`
        file_id: String,
        /// Save to this path instead of printing to stdout
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Delete a file by ID
    Delete {
        /// File ID
        file_id: String,
    },
    /// Show metadata for a file
    Stat {
        /// File ID
        file_id: String,
    },
    /// Show server system status
    Status,
    /// Trigger server-side garbage collection
    Gc,
    /// v0.4.4: force-seal the currently active (unsealed) block.
    /// Needed before you can compact a block — an active block is always rejected.
    Seal,
    /// v0.4.4: trigger block compaction (rewrite low-utilisation blocks)
    Compact {
        /// Compact this specific block_id. Mutually exclusive with threshold.
        #[arg(long)]
        block_id: Option<String>,
        /// Utilisation threshold (0.0–1.0). Blocks below this are compacted.
        #[arg(long, default_value_t = 0.5)]
        threshold: f64,
        /// Skip blocks smaller than this many bytes.
        #[arg(long, default_value_t = 4 * 1024 * 1024)]
        min_size_bytes: u64,
    },
    /// List all block files on the server
    ListBlocks,
    /// Validate a local config file (no server needed)
    ValidateConfig {
        /// Path to config TOML file
        config: PathBuf,
    },
    /// v0.4.5 recovery: rebuild footer+index for a quarantined `.blk.orphan`.
    ///
    /// Run this with the server STOPPED. The tool opens the metadata DB
    /// directly, re-derives every live chunk's `(offset, compressed_size)`
    /// from redb, re-computes the per-chunk and block-level CRC32 from
    /// the on-disk bytes, writes a fresh index + footer, and renames
    /// `.blk.orphan` → `.blk`. Any trailing partial-chunk bytes from the
    /// original crash are truncated (they were never committed anyway).
    ResealOrphan {
        /// Path to the server config TOML (same one the server uses).
        config: PathBuf,
        /// Block ID (from the startup log line `block_id=...`).
        block_id: String,
    },

    /// Phase 4.6: export a full backup of `data_dir + meta_dir + wal_dir`
    /// to a single `.tar.gz` archive. The server **must be stopped** for
    /// the snapshot to be consistent — redb takes an exclusive lock, so
    /// attempting to export while running fails loudly instead of
    /// silently producing a half-written archive.
    ///
    /// Archive layout:
    ///   MANIFEST.json        — version, counts, created_at, sha256
    ///   data/...             — every .blk file
    ///   meta/meta.db         — redb store
    ///   wal/*.wal            — uncheckpointed WAL
    ///
    /// Use `jihuan import --in <archive> --config <new.toml>` on the
    /// target host to restore into a fresh data directory.
    Export {
        /// Path to the live server config TOML (so we know where
        /// data/meta/wal live).
        #[arg(long)]
        config: PathBuf,
        /// Output archive path (`.tar.gz` extension recommended).
        #[arg(long)]
        out: PathBuf,
        /// Phase 4.6 follow-up — produce an **incremental** archive
        /// against this parent (`.tar.gz`). Files whose `(archive path,
        /// size)` already appear in the parent are skipped, so the
        /// delta only contains new-or-modified content. Restore with
        /// `jihuan import --in <parent> ...` first, then
        /// `jihuan import --in <delta> ... --force`.
        #[arg(long, name = "against")]
        against: Option<PathBuf>,
    },

    /// Phase 4.6: restore a backup produced by `jihuan export`.
    ///
    /// Untars the archive into the `data_dir` / `meta_dir` / `wal_dir`
    /// declared in `--config`. Refuses to overwrite a non-empty target
    /// unless `--force` is passed. The server **must be stopped**
    /// during restore.
    Import {
        /// Input archive (`.tar.gz`).
        #[arg(long, name = "in")]
        input: PathBuf,
        /// Path to the server config TOML that declares where data
        /// should land.
        #[arg(long)]
        config: PathBuf,
        /// Allow restoring on top of a non-empty data/meta/wal dir.
        /// Existing files are overwritten with archive contents;
        /// nothing is deleted up front, so stale files left over from
        /// a previous run can still linger — operators should wipe the
        /// target dir manually when in doubt.
        #[arg(long, default_value_t = false)]
        force: bool,
    },

    /// Phase 4.6: read `MANIFEST.json` out of a backup archive and
    /// print its contents without unpacking anything else. Safe to run
    /// while the server is live.
    BackupVerify {
        /// Input archive (`.tar.gz`).
        #[arg(long, name = "in")]
        input: PathBuf,
    },
}

// ── Response types matching the server's JSON ─────────────────────────────────

#[derive(Debug, Deserialize)]
struct UploadResponse {
    file_id: String,
    file_name: String,
    file_size: u64,
    /// v0.4.6: "created" / "skipped" / "overwritten".
    #[serde(default)]
    outcome: String,
}

#[derive(Debug, Deserialize)]
struct FileMetaResponse {
    file_id: String,
    file_name: String,
    file_size: u64,
    content_type: Option<String>,
    create_time: u64,
    chunk_count: usize,
}

#[derive(Debug, Deserialize)]
struct StatusResponse {
    file_count: u64,
    block_count: u64,
    version: String,
    hash_algorithm: String,
    compression_algorithm: String,
    compression_level: i32,
}

#[derive(Debug, Deserialize)]
struct GcResponse {
    blocks_deleted: u64,
    bytes_reclaimed: u64,
    partitions_deleted: u64,
    files_deleted: u64,
    duration_ms: u64,
}

#[derive(Debug, Deserialize)]
struct CompactionBlockStats {
    old_block_id: String,
    new_block_id: Option<String>,
    old_size_bytes: u64,
    new_size_bytes: u64,
    bytes_saved: i64,
    live_chunks: u64,
    dropped_chunks: u64,
}

#[derive(Debug, Deserialize)]
struct CompactResponse {
    compacted: Vec<CompactionBlockStats>,
    total_bytes_saved: i64,
}

#[derive(Debug, Deserialize)]
struct BlockInfo {
    block_id: String,
    size: u64,
    ref_count: u64,
    path: String,
}

#[derive(Debug, Deserialize)]
struct BlockListResponse {
    blocks: Vec<BlockInfo>,
}

// ── Error helper ──────────────────────────────────────────────────────────────

async fn check_response(resp: reqwest::Response, context: &str) -> Result<reqwest::Response> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let body = resp.text().await.unwrap_or_default();
    Err(anyhow!(
        "{}: server returned {} — {}",
        context,
        status,
        body.trim()
    ))
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    let base = cli.server.trim_end_matches('/').to_string();
    let api_key = cli.api_key.clone();
    let client = reqwest::Client::new();

    macro_rules! authed {
        ($req:expr) => {{
            let r = $req;
            if let Some(ref key) = api_key {
                r.header("X-API-Key", key)
            } else {
                r
            }
        }};
    }

    match &cli.command {
        // ── put ───────────────────────────────────────────────────────────────
        Commands::Put {
            file,
            name,
            content_type,
            file_id,
            on_conflict,
        } => {
            let data = std::fs::read(file)
                .with_context(|| format!("Cannot read file: {}", file.display()))?;

            let file_name = name.clone().unwrap_or_else(|| {
                file.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned()
            });

            let mime = content_type
                .clone()
                .unwrap_or_else(|| "application/octet-stream".to_string());

            let part = reqwest::multipart::Part::bytes(data)
                .file_name(file_name.clone())
                .mime_str(&mime)?;
            // v0.4.6: build multipart with optional file_id / on_conflict
            // text parts before the file part. Order is preserved by
            // reqwest; server reads text parts before the file stream.
            let mut form = reqwest::multipart::Form::new();
            if let Some(ref fid) = file_id {
                form = form.text("file_id", fid.clone());
            }
            form = form.text("on_conflict", on_conflict.clone());
            let form = form.part("file", part);

            let resp = authed!(client
                .post(format!("{}/api/v1/files", base))
                .multipart(form))
                .send()
                .await
                .context("Failed to connect to server")?;

            let resp = check_response(resp, "put").await?;
            let r: UploadResponse = resp.json().await?;
            println!("file_id:   {}", r.file_id);
            println!("file_name: {}", r.file_name);
            println!("file_size: {} bytes", r.file_size);
            if !r.outcome.is_empty() {
                println!("outcome:   {}", r.outcome);
            }
        }

        // ── get ───────────────────────────────────────────────────────────────
        Commands::Get { file_id, output } => {
            let resp = authed!(client
                .get(format!("{}/api/v1/files/{}", base, file_id)))
                .send()
                .await
                .context("Failed to connect to server")?;

            let resp = check_response(resp, "get").await?;
            let data = resp.bytes().await?;

            match output {
                Some(path) => {
                    std::fs::write(path, &data)?;
                    eprintln!("Saved {} bytes to {}", data.len(), path.display());
                }
                None => {
                    use std::io::Write;
                    std::io::stdout().write_all(&data)?;
                }
            }
        }

        // ── delete ────────────────────────────────────────────────────────────
        Commands::Delete { file_id } => {
            let resp = authed!(client
                .delete(format!("{}/api/v1/files/{}", base, file_id)))
                .send()
                .await
                .context("Failed to connect to server")?;

            check_response(resp, "delete").await?;
            eprintln!("Deleted {}", file_id);
        }

        // ── stat ──────────────────────────────────────────────────────────────
        Commands::Stat { file_id } => {
            let resp = authed!(client
                .get(format!("{}/api/v1/files/{}/meta", base, file_id)))
                .send()
                .await
                .context("Failed to connect to server")?;

            let resp = check_response(resp, "stat").await?;
            let m: FileMetaResponse = resp.json().await?;
            println!("file_id:      {}", m.file_id);
            println!("file_name:    {}", m.file_name);
            println!("file_size:    {} bytes", m.file_size);
            println!("create_time:  {} (unix)", m.create_time);
            println!("chunk_count:  {}", m.chunk_count);
            if let Some(ct) = m.content_type {
                println!("content_type: {}", ct);
            }
        }

        // ── status ────────────────────────────────────────────────────────────
        Commands::Status => {
            let resp = authed!(client.get(format!("{}/api/status", base)))
                .send()
                .await
                .context("Failed to connect to server")?;

            let resp = check_response(resp, "status").await?;
            let s: StatusResponse = resp.json().await?;
            println!("JiHuan Storage Status");
            println!("---------------------");
            println!("Version:     {}", s.version);
            println!("Files:       {}", s.file_count);
            println!("Blocks:      {}", s.block_count);
            println!("Hash:        {}", s.hash_algorithm);
            println!("Compression: {} (level {})", s.compression_algorithm, s.compression_level);
        }

        // ── gc ────────────────────────────────────────────────────────────────
        Commands::Gc => {
            let resp = authed!(client.post(format!("{}/api/gc/trigger", base)))
                .send()
                .await
                .context("Failed to connect to server")?;

            let resp = check_response(resp, "gc").await?;
            let g: GcResponse = resp.json().await?;
            println!("GC completed:");
            println!("  Blocks deleted:     {}", g.blocks_deleted);
            println!("  Bytes reclaimed:    {}", g.bytes_reclaimed);
            println!("  Partitions deleted: {}", g.partitions_deleted);
            println!("  Files deleted:      {}", g.files_deleted);
            println!("  Duration:           {}ms", g.duration_ms);
        }

        // ── seal (v0.4.4) ────────────────────────────────────────────────────
        Commands::Seal => {
            let resp = authed!(client.post(format!("{}/api/admin/seal", base)))
                .send()
                .await
                .context("Failed to connect to server")?;
            let resp = check_response(resp, "seal").await?;
            #[derive(Debug, Deserialize)]
            struct SealResp {
                sealed_block_id: Option<String>,
                size: u64,
            }
            let r: SealResp = resp.json().await?;
            match r.sealed_block_id {
                Some(id) => println!("Sealed block {} (final size {} bytes).", id, r.size),
                None => println!("No active block to seal — active writer was empty."),
            }
        }

        // ── compact (v0.4.4) ─────────────────────────────────────────────────
        Commands::Compact {
            block_id,
            threshold,
            min_size_bytes,
        } => {
            let mut body = serde_json::Map::new();
            if let Some(id) = block_id {
                body.insert("block_id".into(), serde_json::Value::String(id.clone()));
            } else {
                body.insert(
                    "threshold".into(),
                    serde_json::json!(threshold),
                );
                body.insert(
                    "min_size_bytes".into(),
                    serde_json::json!(min_size_bytes),
                );
            }
            let resp = authed!(client
                .post(format!("{}/api/admin/compact", base))
                .json(&serde_json::Value::Object(body)))
            .send()
            .await
            .context("Failed to connect to server")?;

            let resp = check_response(resp, "compact").await?;
            let r: CompactResponse = resp.json().await?;
            if r.compacted.is_empty() {
                println!("No blocks matched the compaction criteria.");
            } else {
                println!("Compacted {} block(s):", r.compacted.len());
                println!(
                    "{:<40} {:<40} {:>14} {:>14} {:>14} {:>8} {:>8}",
                    "OLD BLOCK", "NEW BLOCK", "OLD SIZE", "NEW SIZE", "SAVED", "LIVE", "DROP"
                );
                println!("{}", "-".repeat(140));
                for s in &r.compacted {
                    println!(
                        "{:<40} {:<40} {:>14} {:>14} {:>14} {:>8} {:>8}",
                        s.old_block_id,
                        s.new_block_id.clone().unwrap_or_else(|| "(empty)".into()),
                        s.old_size_bytes,
                        s.new_size_bytes,
                        s.bytes_saved,
                        s.live_chunks,
                        s.dropped_chunks,
                    );
                }
                println!("\nTotal bytes saved: {}", r.total_bytes_saved);
            }
        }

        // ── list-blocks ───────────────────────────────────────────────────────
        Commands::ListBlocks => {
            let resp = authed!(client.get(format!("{}/api/block/list", base)))
                .send()
                .await
                .context("Failed to connect to server")?;

            let resp = check_response(resp, "list-blocks").await?;
            let r: BlockListResponse = resp.json().await?;
            println!(
                "{:<36} {:>14} {:>10}  PATH",
                "BLOCK ID", "SIZE (bytes)", "REF COUNT"
            );
            println!("{}", "-".repeat(90));
            for b in &r.blocks {
                println!(
                    "{:<36} {:>14} {:>10}  {}",
                    b.block_id, b.size, b.ref_count, b.path
                );
            }
            println!("\nTotal: {} blocks", r.blocks.len());
        }

        // ── validate-config (local, no server) ───────────────────────────────
        Commands::ValidateConfig { config } => match AppConfig::from_file(config) {
            Ok(cfg) => {
                println!("Config is valid.");
                println!("  data_dir: {}", cfg.storage.data_dir.display());
                println!("  hash:     {}", cfg.storage.hash_algorithm);
                println!("  compress: {}", cfg.storage.compression_algorithm);
                println!("  http:     {}", cfg.server.http_addr);
                println!("  grpc:     {}", cfg.server.grpc_addr);
                println!("  metrics:  {}", cfg.server.metrics_addr);
            }
            Err(e) => {
                eprintln!("Config error: {}", e);
                std::process::exit(1);
            }
        },

        // ── export (Phase 4.6; server MUST be stopped) ──────────────────────
        Commands::Export { config, out, against } => {
            let cfg = AppConfig::from_file(config)
                .with_context(|| format!("failed to load {}", config.display()))?;
            if let Some(parent) = against {
                eprintln!(
                    "Exporting INCREMENTAL against {} → {}",
                    parent.display(),
                    out.display()
                );
            }
            eprintln!(
                "Exporting data_dir={} meta_dir={} wal_dir={} → {}",
                cfg.storage.data_dir.display(),
                cfg.storage.meta_dir.display(),
                cfg.storage.wal_dir.display(),
                out.display(),
            );
            let manifest = match against {
                Some(parent) => backup::create_incremental_archive(
                    &cfg.storage.data_dir,
                    &cfg.storage.meta_dir,
                    &cfg.storage.wal_dir,
                    out,
                    parent,
                ),
                None => backup::create_archive(
                    &cfg.storage.data_dir,
                    &cfg.storage.meta_dir,
                    &cfg.storage.wal_dir,
                    out,
                ),
            }
            .context("backup export failed")?;
            println!("Export complete.");
            println!("  archive:        {}", out.display());
            println!("  producer:       {}", manifest.producer);
            println!("  data entries:   {} ({} bytes)", manifest.data_entries, manifest.data_bytes);
            println!("  wal entries:    {} ({} bytes)", manifest.wal_entries, manifest.wal_bytes);
            println!("  meta db bytes:  {}", manifest.meta_bytes);
            if let Some(p) = manifest.parent.as_ref() {
                println!(
                    "  parent created_at={} reused {}/{} entries (delta saved ~{} bytes)",
                    p.created_at,
                    p.reused_entries,
                    p.total_entries,
                    p.total_bytes
                        .saturating_sub(manifest.data_bytes + manifest.meta_bytes + manifest.wal_bytes),
                );
            }
        }

        // ── import (Phase 4.6; server MUST be stopped) ──────────────────────
        Commands::Import {
            input,
            config,
            force,
        } => {
            let cfg = AppConfig::from_file(config)
                .with_context(|| format!("failed to load {}", config.display()))?;
            eprintln!(
                "Restoring {} → data_dir={} meta_dir={} wal_dir={} (force={})",
                input.display(),
                cfg.storage.data_dir.display(),
                cfg.storage.meta_dir.display(),
                cfg.storage.wal_dir.display(),
                force,
            );
            let manifest = backup::restore_archive(
                input,
                &cfg.storage.data_dir,
                &cfg.storage.meta_dir,
                &cfg.storage.wal_dir,
                *force,
            )
            .context("backup import failed")?;
            println!("Restore complete.");
            println!("  archive producer: {}", manifest.producer);
            println!("  data entries:     {}", manifest.data_entries);
            println!("  wal entries:      {}", manifest.wal_entries);
            println!("  created_at:       {} (unix seconds)", manifest.created_at);
            println!("You can now start the server.");
        }

        // ── backup-verify (reads manifest only; safe while server runs) ─────
        Commands::BackupVerify { input } => {
            let manifest = backup::read_manifest(input)
                .with_context(|| format!("reading manifest from {}", input.display()))?;
            println!("Archive {}", input.display());
            println!("  manifest version:  {}", manifest.version);
            println!("  producer:          {}", manifest.producer);
            println!("  created_at:        {}", manifest.created_at);
            println!("  data entries:      {}", manifest.data_entries);
            println!("  data bytes:        {}", manifest.data_bytes);
            println!("  wal entries:       {}", manifest.wal_entries);
            println!("  wal bytes:         {}", manifest.wal_bytes);
            println!("  meta db bytes:     {}", manifest.meta_bytes);
            println!("  has meta.db:       {}", manifest.has_meta_db);
            match manifest.parent.as_ref() {
                None => println!("  archive type:      full snapshot"),
                Some(p) => {
                    println!("  archive type:      INCREMENTAL");
                    println!("    parent created_at:    {}", p.created_at);
                    println!("    parent data entries:  {}", p.data_entries);
                    println!("    parent total entries: {}", p.total_entries);
                    println!("    parent total bytes:   {}", p.total_bytes);
                    println!("    reused entries:       {}", p.reused_entries);
                }
            }
        }

        // ── reseal-orphan (local, server MUST be stopped) ────────────────────
        Commands::ResealOrphan { config, block_id } => {
            let cfg = AppConfig::from_file(config)
                .with_context(|| format!("failed to load {}", config.display()))?;

            // We open the metadata DB directly (no Engine). redb holds an
            // exclusive file lock, so if the server is still running this
            // will fail with a clear "Database already open" error — that
            // doubles as a safety interlock against accidental concurrent
            // use.
            let meta_path = cfg.storage.meta_dir.join("meta.db");
            let meta = jihuan_core::metadata::store::MetadataStore::open(&meta_path)
                .with_context(|| {
                    format!(
                        "failed to open metadata store at {} — is the server still running?",
                        meta_path.display()
                    )
                })?;

            match jihuan_core::Engine::reseal_orphan_block_static(
                &meta,
                &cfg.storage.data_dir,
                block_id,
                cfg.storage.compression_algorithm,
            ) {
                Ok(summary) => {
                    println!("Resealed block {}", summary.block_id);
                    println!("  chunks restored:         {}", summary.chunks_restored);
                    println!("  sealed file size:        {} bytes", summary.final_size);
                    println!(
                        "  trailing bytes dropped:  {} (never committed to metadata)",
                        summary.trailing_bytes_truncated
                    );
                    println!("You can now start the server.");
                }
                Err(e) => {
                    eprintln!("reseal-orphan failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}
