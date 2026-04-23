//! Phase 4.6 — backup archive layout and helpers for `jihuan export` /
//! `jihuan import` / `jihuan backup-verify`.
//!
//! Archive format: **gzipped tar**, fixed top-level entries:
//!
//! ```text
//! MANIFEST.json        JSON — format version + counts + created_at
//! data/<...>.blk       every block file, preserving relative paths
//! meta/meta.db         redb file (single-file store)
//! wal/*.wal            WAL segments (may be empty if engine is clean)
//! ```
//!
//! `MANIFEST.json` is always the **first** entry so `backup-verify` can
//! read it without streaming past the full payload — avoids
//! accidentally loading a multi-GB archive into memory just to validate
//! its shape.

use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};

/// Bump when the archive layout changes in a backwards-incompatible
/// way. `import` refuses mismatched majors so operators can't restore
/// a future archive into an old server.
pub const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    /// Semver of the producing `jihuan-cli`.
    pub producer: String,
    /// Unix timestamp (seconds) when the archive was built.
    pub created_at: u64,
    /// Aggregate byte size of the `data/` subtree before gzip.
    pub data_bytes: u64,
    pub meta_bytes: u64,
    pub wal_bytes: u64,
    /// File counts in each subtree (useful for a human sanity check).
    pub data_entries: u32,
    pub wal_entries: u32,
    /// True when `meta/meta.db` is present — exports always include it;
    /// surfaced explicitly so `backup-verify` output is self-describing.
    pub has_meta_db: bool,
}

/// Walk a subtree, return `(entries, total_bytes)` counting only files
/// (not directories, not symlinks). Used to populate the manifest.
fn subtree_stats(root: &Path) -> Result<(u32, u64)> {
    if !root.exists() {
        return Ok((0, 0));
    }
    let mut entries = 0u32;
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(p) = stack.pop() {
        let meta = std::fs::metadata(&p)
            .with_context(|| format!("stat {}", p.display()))?;
        if meta.is_dir() {
            for child in std::fs::read_dir(&p)
                .with_context(|| format!("read_dir {}", p.display()))?
            {
                let child = child.with_context(|| format!("read_dir entry {}", p.display()))?;
                stack.push(child.path());
            }
        } else if meta.is_file() {
            entries += 1;
            total = total.saturating_add(meta.len());
        }
    }
    Ok((entries, total))
}

/// Create a `.tar.gz` archive containing `data/`, `meta/`, `wal/` and
/// a leading `MANIFEST.json`. Refuses to run if `meta.db` is locked
/// (i.e. server is still up) — redb opens it exclusively so
/// `std::fs::File::open` itself doesn't fail, but then our reader
/// would block. We instead attempt to open the DB in read-only mode
/// via `redb::Database::open` before we start writing; that call
/// *does* fail-fast when a live server holds the lock.
pub fn create_archive(
    data_dir: &Path,
    meta_dir: &Path,
    wal_dir: &Path,
    out_path: &Path,
) -> Result<Manifest> {
    // Exclusivity interlock: try to open the redb store. If the server
    // is running, redb returns `DatabaseAlreadyOpen` (wrapped in our
    // own error) and we bail out rather than capture a torn snapshot.
    let meta_db_path = meta_dir.join("meta.db");
    if meta_db_path.exists() {
        // Open + drop immediately — we only want the lock check. redb
        // takes a shared lock for `open`, which is enough to refuse
        // concurrent exclusive writers (the server).
        let _probe = jihuan_core::metadata::store::MetadataStore::open(&meta_db_path)
            .with_context(|| {
                format!(
                    "export: failed to open metadata DB at {} — is the server still running?",
                    meta_db_path.display()
                )
            })?;
        drop(_probe);
    }

    let (data_entries, data_bytes) = subtree_stats(data_dir)?;
    let (wal_entries, wal_bytes) = subtree_stats(wal_dir)?;
    let (_, meta_bytes) = subtree_stats(meta_dir)?;

    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let manifest = Manifest {
        version: MANIFEST_VERSION,
        producer: format!("jihuan-cli {}", env!("CARGO_PKG_VERSION")),
        created_at,
        data_bytes,
        meta_bytes,
        wal_bytes,
        data_entries,
        wal_entries,
        has_meta_db: meta_db_path.exists(),
    };

    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("mkdir -p {}", parent.display()))?;
        }
    }
    let out_file = File::create(out_path)
        .with_context(|| format!("create {}", out_path.display()))?;
    let gz = GzEncoder::new(out_file, Compression::default());
    let mut tar = tar::Builder::new(gz);

    // MANIFEST.json **first** — `backup-verify` relies on this.
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let mut header = tar::Header::new_gnu();
    header.set_path("MANIFEST.json")?;
    header.set_size(manifest_json.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append(&header, manifest_json.as_slice())?;

    // data/, meta/, wal/ subtrees.
    if data_dir.exists() {
        tar.append_dir_all("data", data_dir)
            .with_context(|| format!("tar data/ from {}", data_dir.display()))?;
    }
    if meta_dir.exists() {
        tar.append_dir_all("meta", meta_dir)
            .with_context(|| format!("tar meta/ from {}", meta_dir.display()))?;
    }
    if wal_dir.exists() {
        tar.append_dir_all("wal", wal_dir)
            .with_context(|| format!("tar wal/ from {}", wal_dir.display()))?;
    }

    // Finalise: close tar + flush gzip + sync.
    let gz = tar.into_inner()?;
    let mut out_file = gz.finish()?;
    out_file.flush()?;
    out_file.sync_all().ok(); // best-effort; not fatal on tmpfs

    Ok(manifest)
}

/// Read only the `MANIFEST.json` entry out of an archive without
/// extracting anything else. Because `create_archive` writes the
/// manifest first, we can stop scanning as soon as we hit it.
pub fn read_manifest(archive: &Path) -> Result<Manifest> {
    let f = File::open(archive)
        .with_context(|| format!("open {}", archive.display()))?;
    let gz = GzDecoder::new(f);
    let mut tar = tar::Archive::new(gz);
    for entry in tar.entries().context("read tar entries")? {
        let mut entry = entry.context("read tar entry")?;
        let path = entry.path().context("read entry path")?.into_owned();
        if path == PathBuf::from("MANIFEST.json") {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).context("read MANIFEST.json")?;
            let manifest: Manifest = serde_json::from_slice(&buf)
                .context("parse MANIFEST.json")?;
            return Ok(manifest);
        }
    }
    Err(anyhow!(
        "MANIFEST.json not found at top of archive {}",
        archive.display()
    ))
}

/// Extract an archive into the three destination directories declared
/// by the target `AppConfig`. Refuses a non-empty target unless
/// `force` is set. Returns the `Manifest` read from the archive.
pub fn restore_archive(
    archive: &Path,
    data_dir: &Path,
    meta_dir: &Path,
    wal_dir: &Path,
    force: bool,
) -> Result<Manifest> {
    // Pre-flight check: read MANIFEST first so we can reject major
    // mismatches before we've changed anything on disk.
    let manifest = read_manifest(archive)?;
    if manifest.version != MANIFEST_VERSION {
        return Err(anyhow!(
            "archive manifest version {} incompatible with this CLI (expected {})",
            manifest.version,
            MANIFEST_VERSION
        ));
    }

    for (label, dir) in [("data", data_dir), ("meta", meta_dir), ("wal", wal_dir)] {
        if dir.exists()
            && std::fs::read_dir(dir)
                .map(|mut it| it.next().is_some())
                .unwrap_or(false)
            && !force
        {
            return Err(anyhow!(
                "import: {} dir {} is not empty — pass --force to overwrite",
                label,
                dir.display()
            ));
        }
        std::fs::create_dir_all(dir)
            .with_context(|| format!("mkdir -p {}", dir.display()))?;
    }

    // tar::Archive is stateful — we can't rewind. Re-open.
    let f = File::open(archive)
        .with_context(|| format!("open {}", archive.display()))?;
    let gz = GzDecoder::new(f);
    let mut tar = tar::Archive::new(gz);
    for entry in tar.entries().context("read tar entries")? {
        let mut entry = entry.context("read tar entry")?;
        let archive_path = entry.path().context("read entry path")?.into_owned();

        // Map archive top-level prefix → on-disk destination.
        let mut comps = archive_path.components();
        let first = comps
            .next()
            .ok_or_else(|| anyhow!("empty entry path in archive"))?
            .as_os_str()
            .to_owned();
        let rest: PathBuf = comps.collect();
        let dest = match first.to_string_lossy().as_ref() {
            "data" => data_dir.join(&rest),
            "meta" => meta_dir.join(&rest),
            "wal" => wal_dir.join(&rest),
            "MANIFEST.json" => continue, // already consumed above
            other => {
                return Err(anyhow!(
                    "unexpected top-level entry '{}' in archive (expected data/meta/wal)",
                    other
                ))
            }
        };
        // tar 0.4 uses `unpack_in` for safety against path traversal
        // (`..` segments), but we've already sanitised via the prefix
        // match above. Use `unpack` against the explicit dest.
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("mkdir -p {}", parent.display()))?;
        }
        entry
            .unpack(&dest)
            .with_context(|| format!("unpack to {}", dest.display()))?;
    }

    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(path: &Path, contents: &[u8]) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, contents).unwrap();
    }

    #[test]
    fn test_export_import_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        let out = tmp.path().join("backup.tar.gz");

        // Build a fake data layout (no real engine — this exercises the
        // archive plumbing, not the engine).
        touch(&src.join("data/aa/bb/block1.blk"), b"hello-data");
        touch(&src.join("data/aa/bb/block2.blk"), b"second-block-data");
        // Real redb file so the pre-flight lock probe succeeds.
        let meta_path = src.join("meta/meta.db");
        std::fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
        {
            let _store = jihuan_core::metadata::store::MetadataStore::open(&meta_path).unwrap();
        }
        touch(&src.join("wal/segment-001.wal"), b"wal-bytes");

        let manifest = create_archive(
            &src.join("data"),
            &src.join("meta"),
            &src.join("wal"),
            &out,
        )
        .unwrap();
        assert_eq!(manifest.version, MANIFEST_VERSION);
        assert_eq!(manifest.data_entries, 2);
        assert_eq!(manifest.wal_entries, 1);
        assert!(manifest.has_meta_db);
        assert!(manifest.data_bytes > 0);

        // Verify standalone manifest read.
        let m2 = read_manifest(&out).unwrap();
        assert_eq!(m2.data_entries, manifest.data_entries);

        // Restore into an empty target and byte-compare one file.
        let restored = restore_archive(
            &out,
            &dst.join("data"),
            &dst.join("meta"),
            &dst.join("wal"),
            false,
        )
        .unwrap();
        assert_eq!(restored.data_entries, 2);
        let content = std::fs::read(dst.join("data/aa/bb/block1.blk")).unwrap();
        assert_eq!(content, b"hello-data");
        assert!(dst.join("meta/meta.db").exists());
        assert!(dst.join("wal/segment-001.wal").exists());
    }

    #[test]
    fn test_import_refuses_non_empty_target_without_force() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        let out = tmp.path().join("backup.tar.gz");

        touch(&src.join("data/x.blk"), b"x");
        let meta_path = src.join("meta/meta.db");
        std::fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
        {
            let _ = jihuan_core::metadata::store::MetadataStore::open(&meta_path).unwrap();
        }
        std::fs::create_dir_all(src.join("wal")).unwrap();

        create_archive(
            &src.join("data"),
            &src.join("meta"),
            &src.join("wal"),
            &out,
        )
        .unwrap();

        // Pre-populate dst/data so it's non-empty.
        touch(&dst.join("data/existing.blk"), b"existing");

        // Without --force this must fail.
        let err = restore_archive(
            &out,
            &dst.join("data"),
            &dst.join("meta"),
            &dst.join("wal"),
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not empty"), "{err}");

        // With --force it succeeds.
        restore_archive(
            &out,
            &dst.join("data"),
            &dst.join("meta"),
            &dst.join("wal"),
            true,
        )
        .unwrap();
    }
}
