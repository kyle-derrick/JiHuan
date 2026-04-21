/// Engine-level statistics snapshot.
/// Collected synchronously from the MetadataStore; no Prometheus recorder needed.
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub file_count: u64,
    pub block_count: u64,
    /// Sum of logical file sizes (what users uploaded, before dedup or
    /// compression). This is the headline "data stored" number.
    pub logical_bytes: u64,
    /// Total bytes physically on disk under `data_dir` — includes block file
    /// headers, footers, compressed chunks, and any not-yet-GC'd orphans.
    pub disk_usage_bytes: u64,
    /// Rough deduplication ratio: `logical_bytes / disk_usage_bytes`
    /// (>1 means savings).
    pub dedup_ratio: f64,
}

impl std::fmt::Display for EngineStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "files={} blocks={} disk={} dedup_ratio={:.2}x",
            self.file_count,
            self.block_count,
            crate::utils::format_bytes(self.disk_usage_bytes),
            self.dedup_ratio,
        )
    }
}
