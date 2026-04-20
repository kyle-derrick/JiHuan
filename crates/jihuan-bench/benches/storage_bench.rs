use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use jihuan_core::config::ConfigTemplate;
use jihuan_core::Engine;
use tempfile::tempdir;

fn bench_write(c: &mut Criterion) {
    let tmp = tempdir().unwrap();
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine = Engine::open(cfg).unwrap();

    let mut group = c.benchmark_group("write");

    for size in [1024usize, 64 * 1024, 1024 * 1024, 4 * 1024 * 1024] {
        let data: Vec<u8> = (0u8..=255).cycle().take(size).collect();
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}B", size)),
            &data,
            |b, d| {
                b.iter(|| engine.put_bytes(black_box(d), "bench.bin", None).unwrap());
            },
        );
    }

    group.finish();
}

fn bench_read(c: &mut Criterion) {
    let tmp = tempdir().unwrap();
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine = Engine::open(cfg).unwrap();

    let mut group = c.benchmark_group("read");

    for size in [1024usize, 64 * 1024, 1024 * 1024] {
        let data: Vec<u8> = (0u8..=255).cycle().take(size).collect();
        let file_id = engine.put_bytes(&data, "bench.bin", None).unwrap();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}B", size)),
            &file_id,
            |b, id| {
                b.iter(|| engine.get_bytes(black_box(id)).unwrap());
            },
        );
    }

    group.finish();
}

fn bench_dedup_write(c: &mut Criterion) {
    let tmp = tempdir().unwrap();
    let cfg = ConfigTemplate::general(tmp.path().to_path_buf());
    let engine = Engine::open(cfg).unwrap();

    let data: Vec<u8> = vec![0xAB; 64 * 1024]; // 64KB identical data

    c.bench_function("dedup_write_64KB", |b| {
        b.iter(|| {
            engine
                .put_bytes(black_box(&data), "dedup.bin", None)
                .unwrap()
        });
    });
}

criterion_group!(benches, bench_write, bench_read, bench_dedup_write);
criterion_main!(benches);
