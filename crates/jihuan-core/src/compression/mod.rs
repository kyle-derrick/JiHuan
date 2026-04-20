use crate::config::CompressionAlgorithm;
use crate::error::{JiHuanError, Result};

/// Compress data with the given algorithm and level
pub fn compress(data: &[u8], algo: CompressionAlgorithm, level: i32) -> Result<Vec<u8>> {
    match algo {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Lz4 => compress_lz4(data),
        CompressionAlgorithm::Zstd => compress_zstd(data, level),
    }
}

/// Decompress data with the given algorithm
pub fn decompress(
    data: &[u8],
    algo: CompressionAlgorithm,
    original_size: usize,
) -> Result<Vec<u8>> {
    match algo {
        CompressionAlgorithm::None => Ok(data.to_vec()),
        CompressionAlgorithm::Lz4 => decompress_lz4(data, original_size),
        CompressionAlgorithm::Zstd => decompress_zstd(data),
    }
}

fn compress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    Ok(lz4_flex::compress_prepend_size(data))
}

fn decompress_lz4(data: &[u8], _original_size: usize) -> Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| JiHuanError::Compression(format!("LZ4 decompress error: {}", e)))
}

fn compress_zstd(data: &[u8], level: i32) -> Result<Vec<u8>> {
    let level = if level == 0 { 1 } else { level };
    zstd::encode_all(data, level)
        .map_err(|e| JiHuanError::Compression(format!("Zstd compress error: {}", e)))
}

fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data)
        .map_err(|e| JiHuanError::Compression(format!("Zstd decompress error: {}", e)))
}

/// Returns estimated compressed size ratio (compressed / original)
pub fn estimate_ratio(algo: CompressionAlgorithm, level: i32, sample: &[u8]) -> f64 {
    if sample.is_empty() {
        return 1.0;
    }
    match compress(sample, algo, level) {
        Ok(compressed) => compressed.len() as f64 / sample.len() as f64,
        Err(_) => 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn roundtrip(algo: CompressionAlgorithm, level: i32, data: &[u8]) {
        let compressed = compress(data, algo, level).unwrap();
        let decompressed = decompress(&compressed, algo, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_none_roundtrip() {
        let data = b"hello world, this is test data for compression!";
        roundtrip(CompressionAlgorithm::None, 0, data);
    }

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"hello world, this is test data for compression! repeated repeated repeated";
        roundtrip(CompressionAlgorithm::Lz4, 0, data);
    }

    #[test]
    fn test_zstd_roundtrip_level1() {
        let data = b"hello world, this is test data for compression! repeated repeated repeated";
        roundtrip(CompressionAlgorithm::Zstd, 1, data);
    }

    #[test]
    fn test_zstd_roundtrip_level9() {
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        roundtrip(CompressionAlgorithm::Zstd, 9, &data);
    }

    #[test]
    fn test_empty_data_roundtrip() {
        for algo in [
            CompressionAlgorithm::None,
            CompressionAlgorithm::Lz4,
            CompressionAlgorithm::Zstd,
        ] {
            roundtrip(algo, 1, &[]);
        }
    }

    #[test]
    fn test_zstd_compression_actually_compresses() {
        let data: Vec<u8> = vec![0u8; 100_000];
        let compressed = compress(&data, CompressionAlgorithm::Zstd, 1).unwrap();
        assert!(
            compressed.len() < data.len(),
            "zstd should compress zeros: {} vs {}",
            compressed.len(),
            data.len()
        );
    }

    proptest! {
        #[test]
        fn prop_lz4_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
            roundtrip(CompressionAlgorithm::Lz4, 0, &data);
        }

        #[test]
        fn prop_zstd_roundtrip(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
            roundtrip(CompressionAlgorithm::Zstd, 1, &data);
        }
    }
}
