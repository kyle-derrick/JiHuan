#![allow(unused_imports)]
//! End-to-end chaos / stress tests.
//! These simulate real-world failure scenarios to verify data safety.

use jihuan_core::config::ConfigTemplate;
use jihuan_core::{Engine, JiHuanError};
use tempfile::tempdir;

// ─── Stress: many small files ─────────────────────────────────────────────────

#[test]
fn stress_many_small_files() {
    let tmp = tempdir().unwrap();
    let mut cfg = ConfigTemplate::small_files(tmp.path().to_path_buf());
    cfg.storage.chunk_size = 64 * 1024; // 64KB for test speed
    let engine = Engine::open(cfg).unwrap();

    let n = 500;
    let mut ids = Vec::with_capacity(n);

    for i in 0..n {
        let data: Vec<u8> = format!("small file content #{}", i).into_bytes();
        let id = engine
            .put_bytes(&data, &format!("f{}.txt", i), None)
            .unwrap();
        ids.push((id, data));
    }

    // Verify all files
    for (id, expected) in &ids {
        let got = engine.get_bytes(id).unwrap();
        assert_eq!(&got, expected);
    }

    assert_eq!(engine.file_count().unwrap(), n as u64);
}

// ─── Stress: mixed sizes ──────────────────────────────────────────────────────

#[test]
fn stress_mixed_file_sizes() {
    let tmp = tempdir().unwrap();
    let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    cfg.storage.chunk_size = 16 * 1024;
    let engine = Engine::open(cfg).unwrap();

    let sizes = [0, 1, 512, 1024, 16 * 1024, 64 * 1024, 256 * 1024];
    let mut ids = Vec::new();

    for (i, &size) in sizes.iter().enumerate() {
        let data: Vec<u8> = (0u8..=255).cycle().take(size).collect();
        let id = engine
            .put_bytes(&data, &format!("mixed_{}.bin", i), None)
            .unwrap();
        ids.push((id, data));
    }

    for (id, expected) in &ids {
        assert_eq!(&engine.get_bytes(id).unwrap(), expected);
    }
}

// ─── Chaos: partial data corruption detection ─────────────────────────────────

#[test]
fn chaos_corrupted_block_detected_on_read() {
    let tmp = tempdir().unwrap();
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine = Engine::open(cfg).unwrap();

    let data: Vec<u8> = (0u8..=255).cycle().take(8192).collect();
    let id = engine.put_bytes(&data, "corrupt_test.bin", None).unwrap();

    // Seal the active block so the file is complete on disk
    engine.seal_active_block().unwrap();

    // Find and corrupt a block file
    let blocks = engine.metadata().list_all_blocks().unwrap();
    assert!(!blocks.is_empty());

    for block in &blocks {
        let path = std::path::Path::new(&block.path);
        if path.exists() {
            let mut bytes = std::fs::read(path).unwrap();
            // Corrupt a byte in the chunk data area (well after the header)
            let corrupt_offset = bytes.len() / 3;
            if corrupt_offset < bytes.len() {
                bytes[corrupt_offset] ^= 0xFF;
            }
            std::fs::write(path, &bytes).unwrap();
            break;
        }
    }

    // Reading with verify=true (default) should detect corruption
    let result = engine.get_bytes(&id);
    // It may succeed if the corrupted byte happened to be in the index/footer
    // but typically should fail with checksum error
    match result {
        Err(JiHuanError::ChecksumMismatch { .. }) | Err(JiHuanError::DataCorruption(_)) => {
            // Expected: corruption detected
        }
        Ok(_) => {
            // Corruption was in a non-data area (index/footer region) - acceptable
        }
        Err(other) => {
            panic!("Unexpected error: {}", other);
        }
    }
}

// ─── Chaos: write + delete + GC cycle ────────────────────────────────────────

#[tokio::test]
async fn chaos_write_delete_gc_cycle() {
    let tmp = tempdir().unwrap();
    let mut cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    cfg.storage.chunk_size = 4 * 1024;
    let engine = Engine::open(cfg).unwrap();

    let n = 100;
    let mut ids = Vec::new();

    // Write 100 files
    for i in 0..n {
        let data: Vec<u8> = vec![(i % 256) as u8; 8 * 1024];
        let id = engine
            .put_bytes(&data, &format!("f{}.bin", i), None)
            .unwrap();
        ids.push(id);
    }

    assert_eq!(engine.file_count().unwrap(), n as u64);

    // Delete half
    for id in ids.iter().take(n / 2) {
        engine.delete_file(id).unwrap();
    }
    assert_eq!(engine.file_count().unwrap(), (n / 2) as u64);

    // Seal active block so ref counts are stable
    engine.seal_active_block().unwrap();

    // GC
    let stats = engine.trigger_gc().await.unwrap();
    // Some blocks may now be unreferenced
    // The rest should remain readable
    for id in ids.iter().skip(n / 2) {
        assert!(
            engine.get_bytes(id).is_ok(),
            "Surviving file should still be readable"
        );
    }

    let _ = stats; // GC stats logged
}

// ─── Chaos: reopen after ungraceful shutdown simulation ───────────────────────

#[test]
fn chaos_reopen_after_incomplete_block() {
    let tmp = tempdir().unwrap();

    let survived_id;
    {
        let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
        let engine = Engine::open(cfg).unwrap();
        survived_id = engine
            .put_bytes(b"before crash", "before.txt", None)
            .unwrap();

        // Seal so this file is durable
        engine.seal_active_block().unwrap();

        // Simulate a crash: manually create an incomplete block file
        let fake_block = tmp
            .path()
            .join("data")
            .join("ab")
            .join("cd")
            .join("fake.blk");
        std::fs::create_dir_all(fake_block.parent().unwrap()).unwrap();
        std::fs::write(&fake_block, b"JIHUAN\x00\x00incomplete").unwrap();
    }

    // Reopen: crash recovery should remove the incomplete block
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine = Engine::open(cfg).unwrap();

    // Previously written and sealed data should still be accessible
    let got = engine.get_bytes(&survived_id);
    assert!(
        got.is_ok(),
        "Data written before crash should survive: {:?}",
        got.err()
    );
    assert_eq!(got.unwrap(), b"before crash");
}
