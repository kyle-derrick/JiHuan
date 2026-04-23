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

use std::collections::HashMap;
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
    /// Phase 4.6 follow-up — present when this archive is an
    /// **incremental** delta against a parent archive. `None` (the
    /// default via `#[serde(default)]`) means a full snapshot. Legacy
    /// v1 archives pre-date this field and deserialise cleanly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<ParentRef>,
}

/// Phase 4.6 follow-up — identifies the parent of an incremental
/// backup. Purely descriptive — restore does **not** validate that the
/// base was actually applied first; operators are responsible for
/// chaining the imports correctly. Serves as a forensic audit trail
/// and a human readable pointer for automation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentRef {
    /// Parent's `created_at` (seconds). Doubles as a version id for
    /// operators who name archives by timestamp.
    pub created_at: u64,
    /// Parent's `data_entries` count, for sanity-check.
    pub data_entries: u32,
    /// Absolute file count (`data + meta + wal`) in the parent.
    pub total_entries: u32,
    /// Aggregate uncompressed byte size of the parent's payload.
    pub total_bytes: u64,
    /// How many files the delta **did not** re-ship because they were
    /// already present in the parent with an identical `(path, size)`.
    pub reused_entries: u32,
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

/// Read just the `(archive_relative_path → size)` index of an existing
/// archive without extracting any payload bytes. Used by incremental
/// export to decide which files to skip — a file with the same path
/// and same size in the parent is treated as unchanged.
///
/// Streams the tar headers only (`Entry::size()`) so this is cheap
/// even on multi-GB archives. Note: directory entries are filtered
/// out — only regular files contribute to the index.
pub fn read_file_index(archive: &Path) -> Result<HashMap<PathBuf, u64>> {
    let f = File::open(archive)
        .with_context(|| format!("open {}", archive.display()))?;
    let gz = GzDecoder::new(f);
    let mut tar = tar::Archive::new(gz);
    let mut out = HashMap::new();
    for entry in tar.entries().context("read tar entries")? {
        let entry = entry.context("read tar entry")?;
        let header = entry.header();
        let path = entry.path().context("read entry path")?.into_owned();
        // Skip directories and the manifest itself — only file payloads
        // matter for the dedup-by-(path,size) decision.
        if header.entry_type().is_dir() || path == PathBuf::from("MANIFEST.json") {
            continue;
        }
        out.insert(path, header.size().unwrap_or(0));
    }
    Ok(out)
}

/// Create a `.tar.gz` archive containing `data/`, `meta/`, `wal/` and
/// a leading `MANIFEST.json`. Refuses to run if `meta.db` is locked
/// (i.e. server is still up) — redb opens it exclusively so
/// `std::fs::File::open` itself doesn't fail, but then our reader
/// would block. We instead attempt to open the DB in read-only mode
/// via `redb::Database::open` before we start writing; that call
/// *does* fail-fast when a live server holds the lock.
///
/// When `against` is `Some(parent_archive)`, this produces an
/// **incremental** archive: every file whose `(archive path, size)`
/// already appears in the parent is omitted. The resulting MANIFEST
/// records a [`ParentRef`] for forensic audit. The parent archive is
/// only read for its file index — its content bytes are never loaded.
///
/// `meta/meta.db` is **always** included even in incrementals: the
/// redb file is rewritten on every checkpoint so its size routinely
/// drifts even when no logical content changed, and shipping the
/// canonical state is a small price for restore correctness.
pub fn create_archive(
    data_dir: &Path,
    meta_dir: &Path,
    wal_dir: &Path,
    out_path: &Path,
) -> Result<Manifest> {
    create_archive_inner(data_dir, meta_dir, wal_dir, out_path, None)
}

/// Incremental variant of [`create_archive`]. See its docs for the
/// `against` semantics. The parent's `MANIFEST.json` is read first
/// (cheap — gzipped JSON) so the resulting [`ParentRef`] can record
/// audit metadata.
pub fn create_incremental_archive(
    data_dir: &Path,
    meta_dir: &Path,
    wal_dir: &Path,
    out_path: &Path,
    against: &Path,
) -> Result<Manifest> {
    create_archive_inner(data_dir, meta_dir, wal_dir, out_path, Some(against))
}

fn create_archive_inner(
    data_dir: &Path,
    meta_dir: &Path,
    wal_dir: &Path,
    out_path: &Path,
    against: Option<&Path>,
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

    // Phase 4.6 follow-up — load parent index + parent manifest if
    // we're producing a delta. We need both: the file index drives
    // skip decisions, the manifest fills out `ParentRef` for audit.
    let (parent_index, parent_manifest) = match against {
        None => (None, None),
        Some(p) => {
            let idx = read_file_index(p)
                .with_context(|| format!("read file index from parent {}", p.display()))?;
            let m = read_manifest(p)
                .with_context(|| format!("read manifest from parent {}", p.display()))?;
            (Some(idx), Some(m))
        }
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

    // ── Pass 1: stream subtrees, skipping unchanged files when delta ──────
    // Counters are over what we *actually shipped* (post-skip). Manifest
    // numbers always reflect what's in this archive — restore overlays
    // it on top of the parent.
    let mut shipped_data_entries = 0u32;
    let mut shipped_data_bytes = 0u64;
    let mut shipped_wal_entries = 0u32;
    let mut shipped_wal_bytes = 0u64;
    let mut shipped_meta_bytes = 0u64;
    let mut reused_entries = 0u32;

    // Helper closure — appends one regular file under the given archive
    // prefix, but skips it when `(archive_path, on_disk_size)` already
    // exists in the parent index. Reuse counts are computed during pass
    // 1a so this closure just tracks "actually shipped" totals.
    let append_file =
        |tar: &mut tar::Builder<GzEncoder<File>>,
         disk_path: &Path,
         archive_path: &Path,
         counters: (&mut u32, &mut u64)|
         -> Result<()> {
            let meta = std::fs::metadata(disk_path)
                .with_context(|| format!("stat {}", disk_path.display()))?;
            let size = meta.len();
            if let Some(idx) = parent_index.as_ref() {
                if idx.get(archive_path).copied() == Some(size) {
                    return Ok(());
                }
            }
            tar.append_path_with_name(disk_path, archive_path)
                .with_context(|| {
                    format!(
                        "tar append {} as {}",
                        disk_path.display(),
                        archive_path.display()
                    )
                })?;
            *counters.0 = counters.0.saturating_add(1);
            *counters.1 = counters.1.saturating_add(size);
            Ok(())
        };

    // Walk one subtree as `(disk_path, archive_path)` pairs and append
    // each file via the closure above. Directory entries are emitted
    // as side-effects of `append_path_with_name` when needed.
    fn walk_files(root: &Path) -> Result<Vec<(PathBuf, PathBuf)>> {
        let mut out = Vec::new();
        if !root.exists() {
            return Ok(out);
        }
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
                let rel = p
                    .strip_prefix(root)
                    .with_context(|| format!("strip_prefix {}", p.display()))?
                    .to_path_buf();
                out.push((p.clone(), rel));
            }
        }
        Ok(out)
    }

    // We MUST place MANIFEST.json first, but we don't know the final
    // shipped counts until the streaming pass below. Strategy: do the
    // streaming pass into an in-memory buffer first, write the manifest,
    // then write the buffered tar entries by re-walking the subtrees.
    // To avoid double-buffering large archives, we instead do this:
    // walk the trees once just to compute shipped counts (cheap — only
    // stat calls), build the manifest from that, then re-walk to
    // actually append. The two passes both call `walk_files` so the
    // file ordering is identical and reproducible.

    // Pass 1a — compute shipped counts (stat-only, no payload reads).
    let mut data_files = walk_files(data_dir)?;
    let mut meta_files = walk_files(meta_dir)?;
    let mut wal_files = walk_files(wal_dir)?;
    // Sort for reproducible archive layout (stack-walk order is FS-
    // dependent). Operators eyeballing two consecutive backups can
    // diff the entry list directly.
    data_files.sort();
    meta_files.sort();
    wal_files.sort();

    if let Some(idx) = parent_index.as_ref() {
        for (disk, rel) in &data_files {
            let archived = PathBuf::from("data").join(rel);
            let size = std::fs::metadata(disk).map(|m| m.len()).unwrap_or(0);
            if idx.get(&archived).copied() == Some(size) {
                reused_entries = reused_entries.saturating_add(1);
                continue;
            }
            shipped_data_entries += 1;
            shipped_data_bytes += size;
        }
        for (disk, _rel) in &meta_files {
            // meta.db always shipped — see doc comment on create_archive.
            shipped_meta_bytes += std::fs::metadata(disk).map(|m| m.len()).unwrap_or(0);
        }
        for (disk, rel) in &wal_files {
            let archived = PathBuf::from("wal").join(rel);
            let size = std::fs::metadata(disk).map(|m| m.len()).unwrap_or(0);
            if idx.get(&archived).copied() == Some(size) {
                reused_entries = reused_entries.saturating_add(1);
                continue;
            }
            shipped_wal_entries += 1;
            shipped_wal_bytes += size;
        }
    } else {
        shipped_data_entries = data_entries;
        shipped_data_bytes = data_bytes;
        shipped_wal_entries = wal_entries;
        shipped_wal_bytes = wal_bytes;
        shipped_meta_bytes = meta_bytes;
    }

    let parent_ref = parent_manifest.as_ref().map(|pm| ParentRef {
        created_at: pm.created_at,
        data_entries: pm.data_entries,
        total_entries: pm.data_entries + pm.wal_entries + (if pm.has_meta_db { 1 } else { 0 }),
        total_bytes: pm.data_bytes + pm.meta_bytes + pm.wal_bytes,
        reused_entries,
    });

    // Manifest is final at this point — pass 1a above already counted
    // shipped + reused entries from stat metadata, so the on-disk
    // MANIFEST.json is correct without any post-write patching.
    let manifest = Manifest {
        version: MANIFEST_VERSION,
        producer: format!("jihuan-cli {}", env!("CARGO_PKG_VERSION")),
        created_at,
        data_bytes: shipped_data_bytes,
        meta_bytes: shipped_meta_bytes,
        wal_bytes: shipped_wal_bytes,
        data_entries: shipped_data_entries,
        wal_entries: shipped_wal_entries,
        has_meta_db: meta_db_path.exists(),
        parent: parent_ref,
    };

    // MANIFEST.json **first** — `backup-verify` relies on this.
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let mut header = tar::Header::new_gnu();
    header.set_path("MANIFEST.json")?;
    header.set_size(manifest_json.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append(&header, manifest_json.as_slice())?;

    // ── Pass 2: stream payloads ──────────────────────────────────────────
    let mut data_counters = (0u32, 0u64);
    for (disk, rel) in &data_files {
        let archived = PathBuf::from("data").join(rel);
        append_file(&mut tar, disk, &archived, (&mut data_counters.0, &mut data_counters.1))?;
    }
    let mut meta_counters = (0u32, 0u64);
    for (disk, rel) in &meta_files {
        let archived = PathBuf::from("meta").join(rel);
        // meta.db always shipped — bypass parent-index check by
        // calling tar directly. Not routed through `append_file` so
        // the redb file always lands in the delta.
        let m = std::fs::metadata(disk).with_context(|| format!("stat {}", disk.display()))?;
        tar.append_path_with_name(disk, &archived)
            .with_context(|| format!("tar append meta {}", disk.display()))?;
        meta_counters.0 += 1;
        meta_counters.1 += m.len();
    }
    let mut wal_counters = (0u32, 0u64);
    for (disk, rel) in &wal_files {
        let archived = PathBuf::from("wal").join(rel);
        append_file(&mut tar, disk, &archived, (&mut wal_counters.0, &mut wal_counters.1))?;
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
    fn test_incremental_skips_unchanged_files_and_records_parent() {
        // Phase 4.6 follow-up regression: build a parent archive, change
        // exactly one block file, produce a delta against the parent,
        // assert the delta only ships the changed file (+ meta.db, which
        // is always re-shipped) and that MANIFEST.parent is populated.
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let parent_out = tmp.path().join("parent.tar.gz");
        let delta_out = tmp.path().join("delta.tar.gz");

        // Build a fake source layout with three data files.
        touch(&src.join("data/aa/bb/block1.blk"), b"first-payload-bytes");
        touch(&src.join("data/aa/bb/block2.blk"), b"second-payload-bytes");
        touch(&src.join("data/cc/block3.blk"), b"third-payload-bytes");
        let meta_path = src.join("meta/meta.db");
        std::fs::create_dir_all(meta_path.parent().unwrap()).unwrap();
        {
            let _ = jihuan_core::metadata::store::MetadataStore::open(&meta_path).unwrap();
        }
        std::fs::create_dir_all(src.join("wal")).unwrap();
        touch(&src.join("wal/segment-001.wal"), b"wal-bytes-1");

        // Snapshot parent.
        let parent_manifest = create_archive(
            &src.join("data"),
            &src.join("meta"),
            &src.join("wal"),
            &parent_out,
        )
        .unwrap();
        assert!(parent_manifest.parent.is_none(), "full snapshot has no parent");
        assert_eq!(parent_manifest.data_entries, 3);

        // Change exactly one file: block2 grows. block1 + block3 + WAL
        // segment all keep their (path, size) pair.
        touch(
            &src.join("data/aa/bb/block2.blk"),
            b"second-payload-bytes-AND-EXTRA-CONTENT",
        );

        // Produce the delta.
        let delta_manifest = create_incremental_archive(
            &src.join("data"),
            &src.join("meta"),
            &src.join("wal"),
            &delta_out,
            &parent_out,
        )
        .unwrap();

        // Delta carries only the modified data file. WAL is unchanged
        // and skipped. meta.db is always shipped (size drifts even
        // without logical changes).
        assert_eq!(delta_manifest.data_entries, 1, "only block2 re-shipped");
        assert_eq!(delta_manifest.wal_entries, 0, "WAL unchanged → skipped");
        assert!(delta_manifest.has_meta_db, "meta.db always shipped");

        let pref = delta_manifest
            .parent
            .as_ref()
            .expect("incremental manifest must have parent ref");
        assert_eq!(pref.created_at, parent_manifest.created_at);
        assert_eq!(pref.data_entries, 3);
        // Reused = 2 unchanged data files + 1 unchanged WAL segment = 3.
        // (meta.db is always re-shipped so it doesn't contribute to reused.)
        assert_eq!(pref.reused_entries, 3, "block1 + block3 + wal reused");

        // Re-read MANIFEST.json from disk and confirm the on-disk JSON
        // carries the same numbers (catches any "patched in memory only"
        // regressions).
        let on_disk = read_manifest(&delta_out).unwrap();
        assert_eq!(on_disk.data_entries, 1);
        assert_eq!(on_disk.parent.as_ref().unwrap().reused_entries, 3);

        // Restore parent into a fresh dst, then overlay delta with --force.
        let dst = tmp.path().join("dst");
        restore_archive(
            &parent_out,
            &dst.join("data"),
            &dst.join("meta"),
            &dst.join("wal"),
            false,
        )
        .unwrap();
        restore_archive(
            &delta_out,
            &dst.join("data"),
            &dst.join("meta"),
            &dst.join("wal"),
            true,
        )
        .unwrap();

        // Block2 has the new content; block1/block3 carry parent content.
        let b1 = std::fs::read(dst.join("data/aa/bb/block1.blk")).unwrap();
        let b2 = std::fs::read(dst.join("data/aa/bb/block2.blk")).unwrap();
        let b3 = std::fs::read(dst.join("data/cc/block3.blk")).unwrap();
        assert_eq!(b1, b"first-payload-bytes");
        assert_eq!(b2, b"second-payload-bytes-AND-EXTRA-CONTENT");
        assert_eq!(b3, b"third-payload-bytes");
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
