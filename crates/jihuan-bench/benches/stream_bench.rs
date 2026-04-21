//! Bench `put_stream` vs `put_bytes` to track the Phase 6b-P1 migration
//! (unifying the two code paths so large uploads do not load the whole
//! payload into memory). Until P1 lands, these numbers quantify the
//! overhead we inherit from the synchronous-in-memory path.

use std::io::Cursor;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use jihuan_core::config::ConfigTemplate;
use jihuan_core::Engine;
use tempfile::tempdir;

fn bench_put_stream(c: &mut Criterion) {
    let tmp = tempdir().unwrap();
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine = Engine::open(cfg).unwrap();

    let mut group = c.benchmark_group("put_stream");

    // 1 MB + 16 MB. We skip 1 GB by default — include by running
    // `cargo bench --bench stream_bench -- 1GB` if you have the disk budget.
    for size in [1024 * 1024usize, 16 * 1024 * 1024] {
        let data: Vec<u8> = (0u8..=255).cycle().take(size).collect();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}MB", size / 1024 / 1024)),
            &data,
            |b, d| {
                b.iter(|| {
                    engine
                        .put_stream(Cursor::new(black_box(d.clone())), "stream.bin", None)
                        .unwrap()
                });
            },
        );
    }

    group.finish();
}

fn bench_put_bytes_same_sizes(c: &mut Criterion) {
    let tmp = tempdir().unwrap();
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine = Engine::open(cfg).unwrap();

    let mut group = c.benchmark_group("put_bytes_vs_stream");
    for size in [1024 * 1024usize, 16 * 1024 * 1024] {
        let data: Vec<u8> = (0u8..=255).cycle().take(size).collect();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}MB", size / 1024 / 1024)),
            &data,
            |b, d| {
                b.iter(|| engine.put_bytes(black_box(d), "bytes.bin", None).unwrap());
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_put_stream, bench_put_bytes_same_sizes);
criterion_main!(benches);
