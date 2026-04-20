/// Engine-level statistics snapshot.
/// Collected synchronously from the MetadataStore; no Prometheus recorder needed.
#[derive(Debug, Clone, Default)]
pub struct EngineStats {
    pub file_count: u64,
    pub block_count: u64,
    /// Total compressed bytes across all sealed block files on disk
    pub disk_usage_bytes: u64,
    /// Rough deduplication ratio: total_logical / disk_usage (>1 means savings)
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
