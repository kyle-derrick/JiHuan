//! Contract tests for the [`StorageClient`] trait.
//!
//! Every backend implementation (embedded, http, cluster) MUST pass these
//! tests. When a new backend lands, add a `#[test]` wrapper that constructs
//! it and calls each `run_contract_*` function below. This guarantees
//! behavioural equivalence across backends — critical since apps bind
//! against the trait and expect matching semantics.

use jihuan_client::{ClientError, EmbeddedClient, StorageClient};
use jihuan_core::config::ConfigTemplate;

/// Build an embedded client pointed at a fresh tempdir.
fn make_embedded() -> (EmbeddedClient, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = ConfigTemplate::general(dir.path().to_path_buf());
    let client = EmbeddedClient::open(cfg).unwrap();
    (client, dir)
}

async fn run_contract_roundtrip(c: &impl StorageClient) {
    let id = c.put(b"hello world".to_vec(), "greet.txt").await.unwrap();
    // Both stat and get must succeed.
    let info = c.stat(&id).await.unwrap();
    assert_eq!(info.file_name, "greet.txt");
    assert_eq!(info.file_size, 11);
    let got = c.get(&id).await.unwrap();
    assert_eq!(got, b"hello world");
}

async fn run_contract_not_found(c: &impl StorageClient) {
    use jihuan_client::FileId;
    let missing = FileId("0".repeat(32));
    match c.get(&missing).await {
        Err(ClientError::NotFound(_)) => {}
        other => panic!("expected NotFound, got {:?}", other),
    }
    match c.stat(&missing).await {
        Err(ClientError::NotFound(_)) => {}
        other => panic!("expected NotFound on stat, got {:?}", other),
    }
}

async fn run_contract_delete_idempotent(c: &impl StorageClient) {
    let id = c.put(b"x".to_vec(), "x.bin").await.unwrap();
    c.delete(&id).await.unwrap();
    // Second delete must surface NotFound — contract: idempotency is the
    // caller's call, the backend must speak the truth.
    match c.delete(&id).await {
        Err(ClientError::NotFound(_)) => {}
        other => panic!("expected NotFound on second delete, got {:?}", other),
    }
}

async fn run_contract_empty_filename_rejected(c: &impl StorageClient) {
    match c.put(b"data".to_vec(), "").await {
        Err(ClientError::InvalidArgument(_)) => {}
        other => panic!("expected InvalidArgument, got {:?}", other),
    }
}

async fn run_contract_list_and_stats(c: &impl StorageClient) {
    // Initially empty (list_files is cache-free → deterministic).
    assert!(c.list_files().await.unwrap().is_empty());

    // Upload two, expect both visible via listing immediately.
    let id1 = c.put(b"alpha".to_vec(), "a.txt").await.unwrap();
    let id2 = c.put(b"beta!!".to_vec(), "b.txt").await.unwrap();
    let listing = c.list_files().await.unwrap();
    assert_eq!(listing.len(), 2);
    assert!(listing.iter().any(|f| f.file_id == id1));
    assert!(listing.iter().any(|f| f.file_id == id2));

    // `stats()` carries a ~5-second TTL cache in the engine (intentional
    // trade-off — see STATS_CACHE_TTL_MS). We don't assert exact equality
    // here; the contract only guarantees that calling `stats` succeeds and
    // returns a well-formed snapshot. Backends with sub-second stats
    // freshness may add stricter assertions in their own per-backend tests.
    let s = c.stats().await.unwrap();
    assert!(s.file_count <= 2, "stats should not over-count");
}

// ── Embedded backend wrappers ────────────────────────────────────────────────

#[tokio::test]
async fn embedded_roundtrip() {
    let (c, _d) = make_embedded();
    run_contract_roundtrip(&c).await;
}

#[tokio::test]
async fn embedded_not_found() {
    let (c, _d) = make_embedded();
    run_contract_not_found(&c).await;
}

#[tokio::test]
async fn embedded_delete_idempotent() {
    let (c, _d) = make_embedded();
    run_contract_delete_idempotent(&c).await;
}

#[tokio::test]
async fn embedded_empty_filename() {
    let (c, _d) = make_embedded();
    run_contract_empty_filename_rejected(&c).await;
}

#[tokio::test]
async fn embedded_list_and_stats() {
    let (c, _d) = make_embedded();
    run_contract_list_and_stats(&c).await;
}

// ── Embedded-only fast-path tests ────────────────────────────────────────────

#[tokio::test]
async fn embedded_clone_shares_engine() {
    // Cloning the client must share the underlying engine. A file uploaded
    // via one clone is visible to the other with no extra sync.
    let (c1, _d) = make_embedded();
    let c2 = c1.clone();
    let id = c1.put(b"shared".to_vec(), "s.txt").await.unwrap();
    let got = c2.get(&id).await.unwrap();
    assert_eq!(got, b"shared");
}

#[tokio::test]
async fn embedded_engine_accessor_returns_same_instance() {
    // `engine()` is the escape hatch; two calls must return Arcs to the
    // SAME engine (Arc::ptr_eq semantics).
    let (c, _d) = make_embedded();
    let a = c.engine();
    let b = c.engine();
    assert!(std::sync::Arc::ptr_eq(&a, &b));
}
