//! # JiHuan Core
//!
//! High-performance small-file storage engine with global deduplication and
//! time-partition based lifecycle management.

pub mod block;
pub mod chunking;
pub mod compression;
pub mod config;
pub mod dedup;
pub mod engine;
pub mod error;
pub mod gc;
pub mod metadata;
pub mod metrics;
pub mod utils;
pub mod wal;

pub use engine::{
    CompactionBlockStats, ConflictPolicy, Engine, PutOptions, PutOutcome, SealedBlockInfo,
};
pub use error::{JiHuanError, Result};
