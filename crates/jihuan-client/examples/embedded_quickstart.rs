//! Minimal embedded-mode quickstart.
//!
//! This is the "extreme standalone performance" entrypoint: the whole
//! JiHuan engine lives in your process, no HTTP, no gRPC, no sockets.
//!
//! Run with:
//! ```text
//! cargo run --release --example embedded_quickstart -p jihuan-client
//! ```

use jihuan_client::{EmbeddedClient, StorageClient};
use std::time::Instant;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    let client = EmbeddedClient::open_with_defaults(tmp.path())?;
    let (_gc, _compact) = client.start_background_tasks();

    // ── Write N small payloads ────────────────────────────────────────────
    let n = 1_000;
    let payload = vec![42u8; 4096]; // 4 KiB each
    let started = Instant::now();
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let id = client
            .put(payload.clone(), &format!("file-{i:05}.bin"))
            .await?;
        ids.push(id);
    }
    let write_ms = started.elapsed().as_millis();
    println!(
        "wrote  {n} × 4 KiB in {write_ms} ms  →  {:.0} ops/s",
        (n as f64) / (write_ms as f64 / 1000.0)
    );

    // ── Read them back ────────────────────────────────────────────────────
    let started = Instant::now();
    for id in &ids {
        let got = client.get(id).await?;
        debug_assert_eq!(got.len(), 4096);
    }
    let read_ms = started.elapsed().as_millis();
    println!(
        "read   {n} × 4 KiB in {read_ms} ms  →  {:.0} ops/s",
        (n as f64) / (read_ms as f64 / 1000.0)
    );

    // ── Final stats ───────────────────────────────────────────────────────
    let s = client.stats().await?;
    println!(
        "stats: files={}  blocks={}  logical={} B  disk={} B  dedup={:.2}x",
        s.file_count, s.block_count, s.logical_bytes, s.disk_usage_bytes, s.dedup_ratio,
    );

    Ok(())
}
