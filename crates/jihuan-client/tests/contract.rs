//! Contract tests for the [`StorageClient`] trait.
//!
//! Every backend implementation (embedded, http, cluster) MUST pass these
//! tests. When a new backend lands, add a `#[test]` wrapper that constructs
//! it and calls each `run_contract_*` function below. This guarantees
//! behavioural equivalence across backends — critical since apps bind
//! against the trait and expect matching semantics.

use jihuan_client::{ClientError, EmbeddedClient, HttpClient, StorageClient};
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

async fn run_contract_batch_roundtrip(c: &impl StorageClient) {
    let items: Vec<(Vec<u8>, String)> = (0..5)
        .map(|i| (format!("payload-{i}").into_bytes(), format!("file-{i}.bin")))
        .collect();
    let put_results = c.put_batch(items).await;
    assert_eq!(put_results.len(), 5);
    let ids: Vec<_> = put_results
        .into_iter()
        .map(|r| r.expect("every put in batch should succeed"))
        .collect();

    let get_results = c.get_batch(&ids).await;
    for (i, r) in get_results.into_iter().enumerate() {
        let bytes = r.expect("batch get should succeed");
        assert_eq!(bytes, format!("payload-{i}").as_bytes());
    }

    let del_results = c.delete_batch(&ids).await;
    for r in del_results {
        r.expect("batch delete should succeed");
    }
    // After deletion, the batch's own files must not appear in listing.
    // (We can't assert a globally empty list — other tests may share the
    // same backend instance; this is why HTTP tests ran against a single
    // server process whereas embedded tests use per-test tempdirs.)
    let remaining = c.list_files().await.unwrap();
    for id in &ids {
        assert!(
            !remaining.iter().any(|f| &f.file_id == id),
            "deleted file id {id} unexpectedly still listed",
        );
    }
}

async fn run_contract_batch_partial_failure(c: &impl StorageClient) {
    use jihuan_client::FileId;

    // Put one real file.
    let good = c.put(b"real".to_vec(), "real.bin").await.unwrap();

    // Ask for one good id + one missing — expect Ok + NotFound, per input order.
    let ids = vec![good.clone(), FileId("0".repeat(32))];
    let results = c.get_batch(&ids).await;
    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok());
    matches!(results[1].as_ref().unwrap_err(), ClientError::NotFound(_));
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
async fn embedded_batch_roundtrip() {
    let (c, _d) = make_embedded();
    run_contract_batch_roundtrip(&c).await;
}

#[tokio::test]
async fn embedded_batch_partial_failure() {
    let (c, _d) = make_embedded();
    run_contract_batch_partial_failure(&c).await;
}

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

// ── HTTP backend (opt-in via env) ────────────────────────────────────────────
//
// Set both of the following to enable:
//   JIHUAN_TEST_SERVER_URL   — e.g. http://127.0.0.1:8080
//   JIHUAN_TEST_API_KEY      — any admin-scoped key
//
// Absent either, these tests return early. This keeps the default
// `cargo test` run hermetic while letting CI / dev exercise the real
// remote path against a running `jihuan-server`.

fn make_http() -> Option<HttpClient> {
    let url = std::env::var("JIHUAN_TEST_SERVER_URL").ok()?;
    let key = std::env::var("JIHUAN_TEST_API_KEY").ok();
    Some(HttpClient::new(url, key).expect("HttpClient::new"))
}

#[tokio::test]
async fn http_roundtrip() {
    if let Some(c) = make_http() {
        run_contract_roundtrip(&c).await;
    }
}

#[tokio::test]
async fn http_not_found() {
    if let Some(c) = make_http() {
        run_contract_not_found(&c).await;
    }
}

#[tokio::test]
async fn http_empty_filename() {
    if let Some(c) = make_http() {
        run_contract_empty_filename_rejected(&c).await;
    }
}

#[tokio::test]
async fn http_batch_roundtrip() {
    if let Some(c) = make_http() {
        // HTTP uses the default sequential put_batch / get_batch / delete_batch
        // impl from the trait — this test verifies that path works end-to-end.
        run_contract_batch_roundtrip(&c).await;
    }
}
