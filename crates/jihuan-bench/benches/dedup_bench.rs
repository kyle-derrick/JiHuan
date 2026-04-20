use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use jihuan_core::config::HashAlgorithm;
use jihuan_core::dedup::hash_chunk;

fn bench_hash(c: &mut Criterion) {
    let data: Vec<u8> = (0u8..=255).cycle().take(4 * 1024 * 1024).collect(); // 4MB

    let mut group = c.benchmark_group("hash_4MB");
    group.throughput(Throughput::Bytes(data.len() as u64));

    group.bench_function("md5", |b| {
        b.iter(|| hash_chunk(black_box(&data), HashAlgorithm::Md5))
    });
    group.bench_function("sha1", |b| {
        b.iter(|| hash_chunk(black_box(&data), HashAlgorithm::Sha1))
    });
    group.bench_function("sha256", |b| {
        b.iter(|| hash_chunk(black_box(&data), HashAlgorithm::Sha256))
    });

    group.finish();
}

criterion_group!(benches, bench_hash);
criterion_main!(benches);
