use jihuan_core::config::{CompressionAlgorithm, ConfigTemplate, HashAlgorithm};
use jihuan_core::{Engine, JiHuanError};
use tempfile::tempdir;

fn open(tmp: &tempfile::TempDir) -> Engine {
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    Engine::open(cfg).unwrap()
}

// ─── Basic round-trip ─────────────────────────────────────────────────────────

#[test]
fn test_roundtrip_small_file() {
    let tmp = tempdir().unwrap();
    let engine = open(&tmp);
    let data = b"hello integration test";
    let id = engine
        .put_bytes(data, "hello.txt", Some("text/plain"))
        .unwrap();
    let got = engine.get_bytes(&id).unwrap();
    assert_eq!(got, data);
}

#[test]
fn test_roundtrip_binary_file() {
    let tmp = tempdir().unwrap();
    let engine = open(&tmp);
    let data: Vec<u8> = (0u8..=255).cycle().take(65536).collect();
    let id = engine.put_bytes(&data, "bin.dat", None).unwrap();
    assert_eq!(engine.get_bytes(&id).unwrap(), data);
}

#[test]
fn test_roundtrip_empty_file() {
    let tmp = tempdir().unwrap();
    let engine = open(&tmp);
    let id = engine.put_bytes(b"", "empty.bin", None).unwrap();
    assert_eq!(engine.get_bytes(&id).unwrap(), b"");
}

// ─── Deduplication ────────────────────────────────────────────────────────────

#[test]
fn test_dedup_identical_files() {
    let tmp = tempdir().unwrap();
    let engine = open(&tmp);
    let data: Vec<u8> = vec![0x42; 1024 * 1024]; // 1MB identical data

    let id1 = engine.put_bytes(&data, "a.bin", None).unwrap();
    let id2 = engine.put_bytes(&data, "b.bin", None).unwrap();
    let id3 = engine.put_bytes(&data, "c.bin", None).unwrap();

    // All three should be retrievable
    assert_eq!(engine.get_bytes(&id1).unwrap(), data);
    assert_eq!(engine.get_bytes(&id2).unwrap(), data);
    assert_eq!(engine.get_bytes(&id3).unwrap(), data);

    // Only one block should exist (full dedup)
    assert_eq!(engine.block_count().unwrap(), 1);
    assert_eq!(engine.file_count().unwrap(), 3);
}

#[test]
fn test_dedup_none_algorithm_skips_dedup() {
    let tmp = tempdir().unwrap();
    let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    cfg.storage.hash_algorithm = HashAlgorithm::None;
    let engine = Engine::open(cfg).unwrap();

    let data = b"same data, no dedup";
    engine.put_bytes(data, "a.txt", None).unwrap();
    engine.put_bytes(data, "b.txt", None).unwrap();

    // With dedup disabled, ref counting doesn't apply the same way
    // but both files should still be readable
    assert_eq!(engine.file_count().unwrap(), 2);
}

// ─── Delete & lifecycle ───────────────────────────────────────────────────────

#[test]
fn test_delete_file_not_found_after() {
    let tmp = tempdir().unwrap();
    let engine = open(&tmp);
    let id = engine.put_bytes(b"delete me", "del.txt", None).unwrap();
    engine.delete_file(&id).unwrap();
    assert!(matches!(
        engine.get_bytes(&id),
        Err(JiHuanError::NotFound(_))
    ));
}

#[test]
fn test_delete_nonexistent_returns_error() {
    let tmp = tempdir().unwrap();
    let engine = open(&tmp);
    assert!(matches!(
        engine.delete_file("no-such-id"),
        Err(JiHuanError::NotFound(_))
    ));
}

#[test]
fn test_file_meta_content_type_preserved() {
    let tmp = tempdir().unwrap();
    let engine = open(&tmp);
    let id = engine
        .put_bytes(b"image data", "img.png", Some("image/png"))
        .unwrap();
    let meta = engine.get_file_meta(&id).unwrap().unwrap();
    assert_eq!(meta.content_type, Some("image/png".to_string()));
    assert_eq!(meta.file_name, "img.png");
}

// ─── Multi-chunk files ────────────────────────────────────────────────────────

#[test]
fn test_large_multi_chunk_file() {
    let tmp = tempdir().unwrap();
    let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    cfg.storage.chunk_size = 4096; // 4KB chunks for test speed
    let engine = Engine::open(cfg).unwrap();

    // 100KB → ~25 chunks
    let data: Vec<u8> = (0u8..=255).cycle().take(100 * 1024).collect();
    let id = engine.put_bytes(&data, "large.bin", None).unwrap();
    let got = engine.get_bytes(&id).unwrap();
    assert_eq!(got, data, "Large file data should round-trip exactly");
}

#[test]
fn test_chunk_count_metadata() {
    let tmp = tempdir().unwrap();
    let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    cfg.storage.chunk_size = 1024;
    let engine = Engine::open(cfg).unwrap();

    let data: Vec<u8> = vec![0u8; 5 * 1024]; // 5 chunks
    let id = engine.put_bytes(&data, "five.bin", None).unwrap();
    let meta = engine.get_file_meta(&id).unwrap().unwrap();
    assert_eq!(meta.chunks.len(), 5);
}

// ─── Compression variants ─────────────────────────────────────────────────────

#[test]
fn test_lz4_compression_roundtrip() {
    let tmp = tempdir().unwrap();
    let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    cfg.storage.compression_algorithm = CompressionAlgorithm::Lz4;
    let engine = Engine::open(cfg).unwrap();

    let data: Vec<u8> = vec![0xAA; 128 * 1024];
    let id = engine.put_bytes(&data, "lz4.bin", None).unwrap();
    assert_eq!(engine.get_bytes(&id).unwrap(), data);
}

#[test]
fn test_no_compression_roundtrip() {
    let tmp = tempdir().unwrap();
    let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    cfg.storage.compression_algorithm = CompressionAlgorithm::None;
    let engine = Engine::open(cfg).unwrap();

    let data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    let id = engine.put_bytes(&data, "raw.bin", None).unwrap();
    assert_eq!(engine.get_bytes(&id).unwrap(), data);
}

// ─── GC ───────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_gc_reclaims_deleted_file_blocks() {
    let tmp = tempdir().unwrap();
    let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    cfg.storage.chunk_size = 1024;
    let engine = Engine::open(cfg).unwrap();

    let data: Vec<u8> = vec![0xDE; 4096];
    let id = engine.put_bytes(&data, "gc_test.bin", None).unwrap();

    // Verify block exists
    assert_eq!(engine.block_count().unwrap(), 1);

    // Delete file
    engine.delete_file(&id).unwrap();

    // Seal the active block so its ref_count reflects deletions
    engine.seal_active_block().unwrap();

    // Trigger GC
    let stats = engine.trigger_gc().await.unwrap();
    assert!(
        stats.blocks_deleted >= 1,
        "GC should delete orphaned blocks"
    );
    assert!(stats.bytes_reclaimed > 0);
}

// ─── Persistence ─────────────────────────────────────────────────────────────

#[test]
fn test_data_persists_across_reopen() {
    let tmp = tempdir().unwrap();

    let file_id = {
        let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        let engine = Engine::open(cfg).unwrap();
        let id = engine
            .put_bytes(b"persistent data", "persist.txt", None)
            .unwrap();
        // Drop engine → seals active block
        id
    };

    // Reopen
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine2 = Engine::open(cfg).unwrap();
    let got = engine2.get_bytes(&file_id).unwrap();
    assert_eq!(got, b"persistent data");
}

// ─── Concurrent writes ────────────────────────────────────────────────────────

#[test]
fn test_concurrent_writes_thread_safe() {
    use std::sync::Arc;
    use std::thread;

    let tmp = tempdir().unwrap();
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine = Arc::new(Engine::open(cfg).unwrap());

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let e = engine.clone();
            thread::spawn(move || {
                let data: Vec<u8> = vec![i as u8; 10 * 1024];
                let id = e
                    .put_bytes(&data, &format!("concurrent_{}.bin", i), None)
                    .unwrap();
                let got = e.get_bytes(&id).unwrap();
                assert_eq!(got, data);
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(engine.file_count().unwrap(), 8);
}

// ─── WAL ─────────────────────────────────────────────────────────────────────

#[test]
fn test_wal_records_are_written() {
    let tmp = tempdir().unwrap();
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine = Engine::open(cfg).unwrap();
    engine.put_bytes(b"wal test", "w.txt", None).unwrap();
    drop(engine);

    // WAL file should exist and contain records
    let wal_path = tmp.path().join("wal").join("jihuan.wal");
    assert!(wal_path.exists(), "WAL file should be created");
    let wal = jihuan_core::wal::WriteAheadLog::open(&wal_path).unwrap();
    let records = wal.read_all().unwrap();
    assert!(
        !records.is_empty(),
        "WAL should contain at least one record"
    );
}
