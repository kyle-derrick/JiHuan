use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::dedup::{crc32, HashBytes};
use crate::error::{JiHuanError, Result};

/// WAL record types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalOperation {
    /// A file was inserted
    InsertFile { file_id: String },
    /// A file was deleted
    DeleteFile { file_id: String },
    /// A block was created
    InsertBlock { block_id: String },
    /// A block reference count changed
    BlockRefDelta { block_id: String, delta: i64 },
    /// A block was deleted (ref_count reached 0)
    DeleteBlock { block_id: String },
    /// A dedup entry was inserted (v0.4.8: 32 raw bytes, was hex String)
    InsertDedup { hash: HashBytes },
    /// A dedup entry was removed (v0.4.8: 32 raw bytes, was hex String)
    RemoveDedup { hash: HashBytes },
    /// A partition was deleted
    DeletePartition { partition_id: u64 },
    /// Checkpoint marker – all records before this are applied
    Checkpoint,
}

/// A single WAL log entry with length-prefix framing and CRC32 integrity.
///
/// v0.4.8: frame + payload are bincode-encoded (not JSON). The CRC32 is
/// taken over the bincode-encoded `op` bytes — a bincode round-trip is
/// canonical (same struct always produces the same byte sequence with
/// `config::standard()`), which is what the replay verifier relies on.
#[derive(Debug, Serialize, Deserialize)]
struct WalFrame {
    /// Sequence number (monotonically increasing)
    seq: u64,
    /// The operation payload
    op: WalOperation,
    /// CRC32 of the bincode-encoded op (canonical under
    /// `bincode::config::standard()`)
    crc32: u32,
}

/// Append-only write-ahead log
pub struct WriteAheadLog {
    path: PathBuf,
    writer: BufWriter<File>,
    next_seq: u64,
}

impl WriteAheadLog {
    /// Open (or create) the WAL at the given path.
    /// If the file already exists its contents are preserved; new records are appended.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(JiHuanError::Io)?;
        }

        // Scan existing records to find the highest sequence number
        let next_seq = if path.exists() {
            let records = Self::read_all_raw(&path)?;
            records.last().map(|r| r.seq + 1).unwrap_or(0)
        } else {
            0
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(JiHuanError::Io)?;

        Ok(Self {
            path,
            writer: BufWriter::new(file),
            next_seq,
        })
    }

    /// Append an operation to the WAL
    pub fn append(&mut self, op: WalOperation) -> Result<u64> {
        let op_bytes = bincode::serde::encode_to_vec(&op, bincode::config::standard())
            .map_err(|e| JiHuanError::Serialization(e.to_string()))?;
        let crc = crc32(&op_bytes);

        let frame = WalFrame {
            seq: self.next_seq,
            op,
            crc32: crc,
        };

        // Length-prefix framing: 4-byte little-endian length + bincode bytes
        let frame_bytes = bincode::serde::encode_to_vec(&frame, bincode::config::standard())
            .map_err(|e| JiHuanError::Serialization(e.to_string()))?;
        let len = frame_bytes.len() as u32;
        self.writer
            .write_all(&len.to_le_bytes())
            .map_err(JiHuanError::Io)?;
        self.writer
            .write_all(&frame_bytes)
            .map_err(JiHuanError::Io)?;
        self.writer.flush().map_err(JiHuanError::Io)?;

        let seq = self.next_seq;
        self.next_seq += 1;
        Ok(seq)
    }

    /// Flush and fsync to guarantee durability
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush().map_err(JiHuanError::Io)?;
        self.writer.get_ref().sync_all().map_err(JiHuanError::Io)?;
        Ok(())
    }

    /// Read all valid WAL records (stops at first corruption)
    pub fn read_all(&self) -> Result<Vec<(u64, WalOperation)>> {
        let frames = Self::read_all_raw(&self.path)?;
        Ok(frames.into_iter().map(|f| (f.seq, f.op)).collect())
    }

    fn read_all_raw(path: &Path) -> Result<Vec<WalFrame>> {
        if !path.exists() {
            return Ok(vec![]);
        }
        let mut file = File::open(path).map_err(JiHuanError::Io)?;
        let mut records = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(JiHuanError::Io(e)),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            if len == 0 || len > 64 * 1024 * 1024 {
                // Sanity guard – stop reading, remaining data is corrupted
                tracing::warn!(len, "WAL: suspicious frame length, stopping replay");
                break;
            }
            let mut frame_buf = vec![0u8; len];
            match file.read_exact(&mut frame_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    tracing::warn!("WAL: truncated frame at end, ignoring");
                    break;
                }
                Err(e) => return Err(JiHuanError::Io(e)),
            }

            let frame: WalFrame = match bincode::serde::decode_from_slice::<WalFrame, _>(
                &frame_buf,
                bincode::config::standard(),
            ) {
                Ok((f, _)) => f,
                Err(e) => {
                    tracing::warn!(error=%e, "WAL: failed to decode frame, stopping replay");
                    break;
                }
            };

            // Verify integrity (CRC32 taken over the bincode-encoded op)
            let op_bytes = bincode::serde::encode_to_vec(&frame.op, bincode::config::standard())
                .map_err(|e| JiHuanError::Serialization(e.to_string()))?;
            if crc32(&op_bytes) != frame.crc32 {
                tracing::warn!(seq = frame.seq, "WAL: CRC32 mismatch, stopping replay");
                break;
            }

            records.push(frame);
        }
        Ok(records)
    }

    /// Truncate the WAL (after successful checkpoint). In-place:
    /// existing records are discarded. Use [`rotate`] instead to keep
    /// archival copies of prior segments.
    pub fn truncate(&mut self) -> Result<()> {
        self.writer.flush().map_err(JiHuanError::Io)?;
        drop(std::mem::replace(
            &mut self.writer,
            BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(&self.path)
                    .map_err(JiHuanError::Io)?,
            ),
        ));
        self.append(WalOperation::Checkpoint)?;
        self.sync()?;
        Ok(())
    }

    /// Phase 4.4 follow-up — rotate the WAL, keeping up to
    /// `keep_old_logs` archival segments.
    ///
    /// `keep_old_logs == 0` falls back to [`Self::truncate`]. Otherwise
    /// the current active file `foo.wal` is renamed to `foo.wal.1`,
    /// the existing `foo.wal.1` (if any) shifts to `.2`, and so on up
    /// to `foo.wal.{keep_old_logs}`. Anything past that is deleted.
    ///
    /// A fresh empty active file is opened afterwards (no Checkpoint
    /// marker is written — the file genuinely starts empty so callers
    /// that inspect size-on-disk see "0", matching the size-gate logic
    /// in `Engine::checkpoint_wal`).
    ///
    /// Safety: rotated segments are **archives only** — the engine
    /// never replays them on startup (redb's per-transaction fsync is
    /// the source of truth). They exist for forensic replay.
    pub fn rotate(&mut self, keep_old_logs: u32) -> Result<()> {
        if keep_old_logs == 0 {
            return self.truncate();
        }

        // Flush any buffered bytes and drop the writer handle so the
        // rename below can't race with an in-flight write.
        self.writer.flush().map_err(JiHuanError::Io)?;
        self.writer.get_ref().sync_all().map_err(JiHuanError::Io)?;

        // Shift the chain: .{N-1} → .{N}, …, .1 → .2, active → .1.
        // Start from the high end so no rename overwrites a file we
        // still need to read.
        let n = keep_old_logs as u64;
        let base = &self.path;

        // Prune anything older than `.{N}` — e.g. if an operator just
        // shrank `keep_old_logs` we want to tidy up immediately.
        let mut i = n + 1;
        loop {
            let stale = Self::segment_path(base, i);
            if !stale.exists() {
                break;
            }
            let _ = std::fs::remove_file(&stale);
            i += 1;
        }

        for i in (1..n).rev() {
            let from = Self::segment_path(base, i);
            if !from.exists() {
                continue;
            }
            let to = Self::segment_path(base, i + 1);
            // `rename` is atomic on the same filesystem; a crash in the
            // middle at worst leaves us with two copies of one segment,
            // which replay tolerates (it doesn't consult rotated files).
            std::fs::rename(&from, &to).map_err(JiHuanError::Io)?;
        }

        // Swap the active file into the .1 slot.
        let first = Self::segment_path(base, 1);
        if first.exists() {
            let _ = std::fs::remove_file(&first);
        }

        // Windows refuses to rename a file with an open handle. Point
        // `self.writer` at a scratch file first so the real active
        // file handle is dropped before the rename; the scratch file
        // is deleted at the end of the successful path.
        let scratch = base.with_extension("rotating");
        let scratch_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&scratch)
            .map_err(JiHuanError::Io)?;
        let _old = std::mem::replace(&mut self.writer, BufWriter::new(scratch_file));
        drop(_old); // explicit: release the OS handle on `base`

        std::fs::rename(base, &first).map_err(JiHuanError::Io)?;
        let fresh = OpenOptions::new()
            .create(true)
            .append(true)
            .open(base)
            .map_err(JiHuanError::Io)?;
        self.writer = BufWriter::new(fresh);
        let _ = std::fs::remove_file(&scratch);

        // Sequence numbers continue monotonically — no reset. A reader
        // stitching segments together sees a gap-free seq stream.
        self.sync()?;
        Ok(())
    }

    /// Build the filesystem path for rotated segment index `i`
    /// (1-indexed). `foo.wal` → `foo.wal.1`, `foo.wal.2`, …
    fn segment_path(base: &Path, i: u64) -> PathBuf {
        let mut s = base.as_os_str().to_os_string();
        s.push(format!(".{}", i));
        PathBuf::from(s)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_append_and_read_back() {
        let tmp = tempdir().unwrap();
        let wal_path = tmp.path().join("test.wal");
        let mut wal = WriteAheadLog::open(&wal_path).unwrap();

        wal.append(WalOperation::InsertFile {
            file_id: "file1".to_string(),
        })
        .unwrap();
        wal.append(WalOperation::InsertBlock {
            block_id: "blk1".to_string(),
        })
        .unwrap();
        wal.sync().unwrap();

        let records = wal.read_all().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].1,
            WalOperation::InsertFile {
                file_id: "file1".to_string()
            }
        );
        assert_eq!(records[0].0, 0);
        assert_eq!(records[1].0, 1);
    }

    #[test]
    fn test_reopen_continues_seq() {
        let tmp = tempdir().unwrap();
        let wal_path = tmp.path().join("seq.wal");

        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            wal.append(WalOperation::InsertFile {
                file_id: "a".to_string(),
            })
            .unwrap();
            wal.append(WalOperation::InsertFile {
                file_id: "b".to_string(),
            })
            .unwrap();
            wal.sync().unwrap();
        }

        // Reopen: should continue from seq 2
        let mut wal2 = WriteAheadLog::open(&wal_path).unwrap();
        assert_eq!(wal2.next_seq(), 2);
        wal2.append(WalOperation::DeleteFile {
            file_id: "a".to_string(),
        })
        .unwrap();

        let records = wal2.read_all().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[2].0, 2);
    }

    #[test]
    fn test_truncate_resets_wal() {
        let tmp = tempdir().unwrap();
        let wal_path = tmp.path().join("trunc.wal");
        let mut wal = WriteAheadLog::open(&wal_path).unwrap();
        for i in 0..10 {
            wal.append(WalOperation::InsertFile {
                file_id: format!("f{}", i),
            })
            .unwrap();
        }
        wal.truncate().unwrap();

        // After truncation only the Checkpoint marker should remain
        let records = wal.read_all().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, WalOperation::Checkpoint);
    }

    #[test]
    fn test_corrupt_frame_stops_replay() {
        let tmp = tempdir().unwrap();
        let wal_path = tmp.path().join("corrupt.wal");
        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            wal.append(WalOperation::InsertFile {
                file_id: "good".to_string(),
            })
            .unwrap();
            wal.sync().unwrap();
        }
        // Append garbage bytes
        let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
        f.write_all(b"\xff\xff\xff\xff garbage data here").unwrap();

        let wal = WriteAheadLog::open(&wal_path).unwrap();
        let records = wal.read_all().unwrap();
        assert_eq!(records.len(), 1); // garbage stops replay
    }

    #[test]
    fn test_rotate_zero_falls_back_to_truncate() {
        // Phase 4.4 follow-up: `keep_old_logs == 0` must preserve the
        // legacy behaviour bit-for-bit — no sidecar files on disk.
        let tmp = tempdir().unwrap();
        let wal_path = tmp.path().join("trunc.wal");
        let mut wal = WriteAheadLog::open(&wal_path).unwrap();
        wal.append(WalOperation::InsertFile { file_id: "x".into() }).unwrap();
        wal.rotate(0).unwrap();

        // Active file now has only the Checkpoint marker, like truncate().
        let recs = wal.read_all().unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].1, WalOperation::Checkpoint);
        // No rotated segment was created.
        assert!(!wal_path.with_extension("wal.1").exists());
    }

    #[test]
    fn test_rotate_keeps_archival_segment() {
        // Single rotation with keep=3 must:
        //  1. rename active → .1
        //  2. leave active file empty (next_seq continues monotonically)
        //  3. the .1 file contains the pre-rotation records, readable.
        let tmp = tempdir().unwrap();
        let wal_path = tmp.path().join("rot.wal");
        let mut wal = WriteAheadLog::open(&wal_path).unwrap();
        for i in 0..3 {
            wal.append(WalOperation::InsertFile {
                file_id: format!("f{}", i),
            })
            .unwrap();
        }
        wal.rotate(3).unwrap();

        // Active file is empty (size = 0 → checkpoint_wal's size gate works).
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);
        assert_eq!(wal.read_all().unwrap().len(), 0);
        // next_seq continues from 3 — seqs are monotonic across segments.
        assert_eq!(wal.next_seq(), 3);

        // The rotated .1 segment holds the three pre-rotation records.
        let archive = WriteAheadLog::segment_path(&wal_path, 1);
        let archived = WriteAheadLog::read_all_raw(&archive).unwrap();
        assert_eq!(archived.len(), 3);
        assert_eq!(archived[0].seq, 0);
        assert_eq!(archived[2].seq, 2);
    }

    #[test]
    fn test_rotate_chain_shifts_and_prunes() {
        // Three rotations with keep=2 must leave exactly `.1` and `.2`
        // on disk; everything older is dropped. Verifies the high-to-low
        // rename order doesn't accidentally overwrite surviving files.
        let tmp = tempdir().unwrap();
        let wal_path = tmp.path().join("chain.wal");
        let mut wal = WriteAheadLog::open(&wal_path).unwrap();

        for round in 0..3 {
            wal.append(WalOperation::InsertFile {
                file_id: format!("round{}", round),
            })
            .unwrap();
            wal.rotate(2).unwrap();
        }

        // .1 = most recent rotation (round2), .2 = round1, round0 pruned.
        let s1 = WriteAheadLog::read_all_raw(&WriteAheadLog::segment_path(&wal_path, 1)).unwrap();
        let s2 = WriteAheadLog::read_all_raw(&WriteAheadLog::segment_path(&wal_path, 2)).unwrap();
        assert_eq!(s1.len(), 1);
        assert_eq!(s2.len(), 1);
        match &s1[0].op {
            WalOperation::InsertFile { file_id } => assert_eq!(file_id, "round2"),
            _ => panic!("wrong op in .1"),
        }
        match &s2[0].op {
            WalOperation::InsertFile { file_id } => assert_eq!(file_id, "round1"),
            _ => panic!("wrong op in .2"),
        }
        // No .3 should exist (keep=2).
        assert!(!WriteAheadLog::segment_path(&wal_path, 3).exists());
    }

    #[test]
    fn test_rotate_shrinking_keep_prunes_excess() {
        // Operator shipped with keep=5, accumulates .1..5, then shrinks
        // to keep=1. The next rotation must drop .2..5 (and any stragglers).
        let tmp = tempdir().unwrap();
        let wal_path = tmp.path().join("shrink.wal");
        let mut wal = WriteAheadLog::open(&wal_path).unwrap();
        for _ in 0..5 {
            wal.append(WalOperation::InsertFile { file_id: "x".into() }).unwrap();
            wal.rotate(5).unwrap();
        }
        // Baseline: .1..5 all exist.
        for i in 1..=5u64 {
            assert!(WriteAheadLog::segment_path(&wal_path, i).exists(), ".{i} missing");
        }

        // Shrink: next rotation with keep=1 should end up with only .1.
        wal.append(WalOperation::InsertFile { file_id: "final".into() }).unwrap();
        wal.rotate(1).unwrap();

        assert!(WriteAheadLog::segment_path(&wal_path, 1).exists());
        for i in 2..=6u64 {
            assert!(
                !WriteAheadLog::segment_path(&wal_path, i).exists(),
                ".{i} should have been pruned"
            );
        }
    }
}
