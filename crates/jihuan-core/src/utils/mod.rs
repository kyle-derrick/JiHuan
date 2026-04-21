use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{JiHuanError, Result};

/// Returns current Unix timestamp in seconds
pub fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_secs()
}

/// Returns current Unix timestamp in milliseconds
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_millis() as u64
}

/// Returns current Unix timestamp in microseconds
pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time is after epoch")
        .as_micros() as u64
}

/// Compute partition id from a Unix timestamp given a partition size in hours
pub fn partition_id_from_ts(ts_secs: u64, partition_hours: u32) -> u64 {
    let partition_secs = partition_hours as u64 * 3600;
    ts_secs / partition_secs
}

/// Compute partition id for the current time
pub fn current_partition_id(partition_hours: u32) -> u64 {
    partition_id_from_ts(now_secs(), partition_hours)
}

/// Get available disk space in bytes for the given path.
/// Returns `None` if the information is not available on this platform.
pub fn disk_available_bytes<P: AsRef<Path>>(path: P) -> Option<u64> {
    // Best-effort: use std::fs metadata to walk the directory
    // and estimate free space. A proper implementation would use
    // platform-specific statvfs/GetDiskFreeSpaceEx; that can be added
    // as an optional feature using the `sysinfo` crate.
    let _ = path;
    None
}

/// Compute the used-bytes total for all files under `path`.
/// Useful for tracking how much storage is consumed by block files.
pub fn disk_usage_bytes<P: AsRef<Path>>(path: P) -> u64 {
    let path = path.as_ref();
    if !path.exists() {
        return 0;
    }
    let mut used: u64 = 0;
    for entry in walkdir::WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }
        // IMPORTANT: use `std::fs::metadata` rather than `entry.metadata()`.
        // `walkdir` caches the metadata obtained from the directory-enumeration call
        // (`FindFirstFile`/`FindNextFile` on Windows). That cached size can be
        // stale for files that still have buffered/unsynced writes from the current
        // process — the on-disk file actually has more bytes than the cache claims.
        // A fresh `fs::metadata` call issues a real stat and returns the current size.
        if let Ok(meta) = std::fs::metadata(entry.path()) {
            used += meta.len();
        }
    }
    used
}

/// Format bytes into a human-readable string
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit_idx = 0;
    while value >= 1024.0 && unit_idx < UNITS.len() - 1 {
        value /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[unit_idx])
    } else {
        format!("{:.2} {}", value, UNITS[unit_idx])
    }
}

/// Generate a new random UUID string (without dashes)
pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Encode bytes as lowercase hex string
pub fn to_hex(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

/// Ensure a directory exists, create it (and parents) if it does not
pub fn ensure_dir<P: AsRef<Path>>(path: P) -> Result<()> {
    std::fs::create_dir_all(path.as_ref()).map_err(|e| {
        JiHuanError::Io(std::io::Error::new(
            e.kind(),
            format!(
                "Failed to create directory '{}': {}",
                path.as_ref().display(),
                e
            ),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_id_from_ts() {
        // 24h partition: ts=0 -> partition 0, ts=86399 -> partition 0, ts=86400 -> partition 1
        assert_eq!(partition_id_from_ts(0, 24), 0);
        assert_eq!(partition_id_from_ts(86399, 24), 0);
        assert_eq!(partition_id_from_ts(86400, 24), 1);
        assert_eq!(partition_id_from_ts(86401, 24), 1);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_new_id_is_unique() {
        let a = new_id();
        let b = new_id();
        assert_ne!(a, b);
        assert_eq!(a.len(), 32);
    }

    #[test]
    fn test_to_hex() {
        assert_eq!(to_hex(&[0x0a, 0xbc, 0xff]), "0abcff");
    }

    #[test]
    fn test_ensure_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let nested = tmp.path().join("a").join("b").join("c");
        assert!(ensure_dir(&nested).is_ok());
        assert!(nested.is_dir());
    }
}
