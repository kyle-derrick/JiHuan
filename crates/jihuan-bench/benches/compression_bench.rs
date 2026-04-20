use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use jihuan_core::compression::{compress, decompress};
use jihuan_core::config::CompressionAlgorithm;

fn bench_compress(c: &mut Criterion) {
    // Compressible data: repeated pattern
    let data: Vec<u8> = (0u8..=63).cycle().take(4 * 1024 * 1024).collect();

    let mut group = c.benchmark_group("compress_4MB_compressible");
    group.throughput(Throughput::Bytes(data.len() as u64));

    group.bench_function("lz4", |b| {
        b.iter(|| compress(black_box(&data), CompressionAlgorithm::Lz4, 0).unwrap())
    });
    group.bench_function("zstd_l1", |b| {
        b.iter(|| compress(black_box(&data), CompressionAlgorithm::Zstd, 1).unwrap())
    });
    group.bench_function("zstd_l3", |b| {
        b.iter(|| compress(black_box(&data), CompressionAlgorithm::Zstd, 3).unwrap())
    });
    group.bench_function("zstd_l9", |b| {
        b.iter(|| compress(black_box(&data), CompressionAlgorithm::Zstd, 9).unwrap())
    });

    group.finish();
}

fn bench_decompress(c: &mut Criterion) {
    let data: Vec<u8> = (0u8..=63).cycle().take(4 * 1024 * 1024).collect();
    let lz4_compressed = compress(&data, CompressionAlgorithm::Lz4, 0).unwrap();
    let zstd_compressed = compress(&data, CompressionAlgorithm::Zstd, 1).unwrap();

    let mut group = c.benchmark_group("decompress_4MB");
    group.throughput(Throughput::Bytes(data.len() as u64));

    group.bench_function("lz4", |b| {
        b.iter(|| {
            decompress(
                black_box(&lz4_compressed),
                CompressionAlgorithm::Lz4,
                data.len(),
            )
            .unwrap()
        })
    });
    group.bench_function("zstd_l1", |b| {
        b.iter(|| {
            decompress(
                black_box(&zstd_compressed),
                CompressionAlgorithm::Zstd,
                data.len(),
            )
            .unwrap()
        })
    });

    group.finish();
}

criterion_group!(benches, bench_compress, bench_decompress);
criterion_main!(benches);
