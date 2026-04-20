use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::dedup::crc32;
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
    /// A dedup entry was inserted
    InsertDedup { hash: String },
    /// A dedup entry was removed
    RemoveDedup { hash: String },
    /// A partition was deleted
    DeletePartition { partition_id: u64 },
    /// Checkpoint marker – all records before this are applied
    Checkpoint,
}

/// A single WAL log entry with length-prefix framing and CRC32 integrity
#[derive(Debug, Serialize, Deserialize)]
struct WalFrame {
    /// Sequence number (monotonically increasing)
    seq: u64,
    /// The operation payload
    op: WalOperation,
    /// CRC32 of the JSON-serialised op
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
        let op_json =
            serde_json::to_vec(&op).map_err(|e| JiHuanError::Serialization(e.to_string()))?;
        let crc = crc32(&op_json);

        let frame = WalFrame {
            seq: self.next_seq,
            op,
            crc32: crc,
        };

        // Length-prefix framing: 4-byte little-endian length + JSON bytes
        let frame_bytes =
            serde_json::to_vec(&frame).map_err(|e| JiHuanError::Serialization(e.to_string()))?;
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

            let frame: WalFrame = match serde_json::from_slice(&frame_buf) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(error=%e, "WAL: failed to decode frame, stopping replay");
                    break;
                }
            };

            // Verify integrity
            let op_json = serde_json::to_vec(&frame.op)
                .map_err(|e| JiHuanError::Serialization(e.to_string()))?;
            if crc32(&op_json) != frame.crc32 {
                tracing::warn!(seq = frame.seq, "WAL: CRC32 mismatch, stopping replay");
                break;
            }

            records.push(frame);
        }
        Ok(records)
    }

    /// Truncate the WAL (after successful checkpoint)
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
}
