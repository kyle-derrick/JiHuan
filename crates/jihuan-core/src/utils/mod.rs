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

/// Maximum byte length of a user-supplied `file_id` after NFC normalization
/// (v0.4.6). UTF-8 bounds the character count: ~340 CJK chars or ~1024 ASCII.
pub const MAX_FILE_ID_BYTES: usize = 1024;

/// Normalise and validate a user-supplied `file_id` (v0.4.6).
///
/// Rules:
/// 1. Must be valid UTF-8 (enforced by `&str` input).
/// 2. **NFC normalized** on return so that macOS NFD and Windows NFC
///    renderings of the same visible characters compare equal.
/// 3. No [`char::is_control`] code points (tabs, newlines, DEL, etc.).
/// 4. Byte length (post-NFC) must be in `1..=MAX_FILE_ID_BYTES`.
/// 5. No leading `/`, no `//` segments, no `.`/`..` path segments —
///    defends against filesystem-export features that might resolve
///    the id as a path in the future.
///
/// Returns the NFC-normalised form for use as the canonical storage key.
/// Rejects with [`JiHuanError::InvalidFileId`] on any rule failure.
pub fn normalize_and_validate_file_id(input: &str) -> Result<String> {
    use unicode_normalization::UnicodeNormalization;

    if input.is_empty() {
        return Err(JiHuanError::InvalidFileId("must not be empty".into()));
    }

    // NFC normalisation — cheap for already-NFC input (the common case).
    let normalized: String = input.nfc().collect();

    let bytes = normalized.len();
    if bytes > MAX_FILE_ID_BYTES {
        return Err(JiHuanError::InvalidFileId(format!(
            "byte length {} exceeds limit {}",
            bytes, MAX_FILE_ID_BYTES
        )));
    }

    // Control-character scan in a single pass over chars.
    for c in normalized.chars() {
        if c.is_control() {
            return Err(JiHuanError::InvalidFileId(format!(
                "contains control character U+{:04X}",
                c as u32
            )));
        }
    }

    // Path-segment safety rules.
    if normalized.starts_with('/') {
        return Err(JiHuanError::InvalidFileId(
            "must not start with '/'".into(),
        ));
    }
    if normalized.contains("//") {
        return Err(JiHuanError::InvalidFileId(
            "must not contain consecutive '/' characters".into(),
        ));
    }
    for segment in normalized.split('/') {
        if segment == "." || segment == ".." {
            return Err(JiHuanError::InvalidFileId(format!(
                "must not contain '{}' path segment",
                segment
            )));
        }
        // A trailing slash produces an empty segment. Disallow it too;
        // the "no //" rule already covers the internal case, and leading
        // '/' is already rejected, so the only remaining source is a
        // trailing slash.
        if segment.is_empty() {
            return Err(JiHuanError::InvalidFileId(
                "must not end with '/'".into(),
            ));
        }
    }

    Ok(normalized)
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

    // ── v0.4.6: file_id validation ────────────────────────────────────────

    #[test]
    fn test_validate_file_id_accepts_uuid_like() {
        let out = normalize_and_validate_file_id("ab12cd34ef56").unwrap();
        assert_eq!(out, "ab12cd34ef56");
    }

    #[test]
    fn test_validate_file_id_accepts_s3_key_style() {
        let out = normalize_and_validate_file_id("images/2024/cat.jpg").unwrap();
        assert_eq!(out, "images/2024/cat.jpg");
    }

    #[test]
    fn test_validate_file_id_accepts_chinese() {
        let out = normalize_and_validate_file_id("订单-2024/发票.pdf").unwrap();
        assert_eq!(out, "订单-2024/发票.pdf");
    }

    #[test]
    fn test_validate_file_id_nfc_normalizes() {
        // "é" as NFD (e + U+0301) should normalise to NFC (U+00E9) — two
        // inputs that render the same must produce the same stored key.
        let nfd = "caf\u{0065}\u{0301}.txt"; // e + combining acute
        let nfc = "caf\u{00E9}.txt"; // precomposed é
        let a = normalize_and_validate_file_id(nfd).unwrap();
        let b = normalize_and_validate_file_id(nfc).unwrap();
        assert_eq!(a, b, "NFD and NFC of 'café.txt' must normalise identically");
        assert_eq!(a, nfc);
    }

    #[test]
    fn test_validate_file_id_rejects_empty() {
        assert!(matches!(
            normalize_and_validate_file_id(""),
            Err(JiHuanError::InvalidFileId(_))
        ));
    }

    #[test]
    fn test_validate_file_id_rejects_control_chars() {
        for bad in ["a\nb", "a\tb", "a\0b", "a\x7fb"] {
            assert!(
                matches!(
                    normalize_and_validate_file_id(bad),
                    Err(JiHuanError::InvalidFileId(_))
                ),
                "expected reject for {:?}",
                bad
            );
        }
    }

    #[test]
    fn test_validate_file_id_rejects_path_traversal() {
        for bad in [".", "..", "a/..", "../b", "a/./b", "a/../b"] {
            assert!(
                matches!(
                    normalize_and_validate_file_id(bad),
                    Err(JiHuanError::InvalidFileId(_))
                ),
                "expected reject for {:?}",
                bad
            );
        }
    }

    #[test]
    fn test_validate_file_id_rejects_slash_edges() {
        for bad in ["/leading", "trailing/", "a//b"] {
            assert!(
                matches!(
                    normalize_and_validate_file_id(bad),
                    Err(JiHuanError::InvalidFileId(_))
                ),
                "expected reject for {:?}",
                bad
            );
        }
    }

    #[test]
    fn test_validate_file_id_rejects_overlong() {
        let big = "a".repeat(MAX_FILE_ID_BYTES + 1);
        assert!(matches!(
            normalize_and_validate_file_id(&big),
            Err(JiHuanError::InvalidFileId(_))
        ));
    }

    #[test]
    fn test_validate_file_id_accepts_boundary_length() {
        let exact = "a".repeat(MAX_FILE_ID_BYTES);
        assert!(normalize_and_validate_file_id(&exact).is_ok());
    }
}
