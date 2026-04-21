//! Benchmarks for `Engine::stats()` — the Dashboard's hot path.
//!
//! Before Phase 6b-P4 this walked `data_dir` and re-scanned every file on
//! every call. After P4 it caches for `STATS_CACHE_TTL_MS` so the second
//! and subsequent calls (within the TTL) are microseconds. This bench
//! exposes both the cold (first) and warm (cached) cost, so regressions in
//! either direction show up on `cargo bench -- --baseline`.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use jihuan_core::config::ConfigTemplate;
use jihuan_core::Engine;
use tempfile::tempdir;

/// Populate an engine with `n` synthetic files of `size` bytes, with
/// content varied just enough to avoid dedup (so disk_usage_bytes reflects
/// N * size).
fn seed_engine(n: usize, size: usize) -> (tempfile::TempDir, Engine) {
    let tmp = tempdir().unwrap();
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine = Engine::open(cfg).unwrap();
    let mut buf = vec![0u8; size];
    for i in 0..n {
        // Salt the first 8 bytes with the file index so each chunk hashes
        // differently — defeats dedup so we actually grow on-disk bytes.
        buf[..8].copy_from_slice(&(i as u64).to_le_bytes());
        engine
            .put_bytes(&buf, &format!("file_{i}.bin"), None)
            .unwrap();
    }
    (tmp, engine)
}

fn bench_stats_cached(c: &mut Criterion) {
    // 100 files × 16 KB = a realistic "small deployment" snapshot.
    let (_tmp, engine) = seed_engine(100, 16 * 1024);
    // Warm the cache so this exercises the fast path.
    let _ = engine.stats().unwrap();

    c.bench_function("stats_cached_100files", |b| {
        b.iter(|| black_box(engine.stats().unwrap()));
    });
}

fn bench_stats_cold(c: &mut Criterion) {
    let (_tmp, engine) = seed_engine(100, 16 * 1024);

    c.bench_function("stats_cold_100files", |b| {
        b.iter_batched(
            || {
                // Force cold path for each iteration by invalidating the cache.
                engine.invalidate_stats_cache();
            },
            |_| black_box(engine.stats().unwrap()),
            BatchSize::SmallInput,
        );
    });
}

fn bench_stats_cold_1k(c: &mut Criterion) {
    // Larger scale: 1000 files × 4 KB. Exposes whether the dir walk scales
    // linearly. Kept off by default (long setup) — enable by running with
    // `--bench stats_bench cold_1kfiles`.
    let (_tmp, engine) = seed_engine(1000, 4 * 1024);

    let mut group = c.benchmark_group("stats_scale");
    group.sample_size(10);
    group.bench_function("cold_1kfiles", |b| {
        b.iter_batched(
            || engine.invalidate_stats_cache(),
            |_| black_box(engine.stats().unwrap()),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_stats_cached, bench_stats_cold, bench_stats_cold_1k);
criterion_main!(benches);
