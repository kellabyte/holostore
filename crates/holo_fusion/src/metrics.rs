//! In-process counters for SQL predicate pushdown and scan behavior.
//!
//! These metrics are intentionally lightweight and lock-free so they can be
//! updated on hot read paths without noticeable overhead.

use std::sync::atomic::{AtomicU64, Ordering};

/// Aggregated counters for pushdown support checks and scan execution.
#[derive(Debug, Default)]
pub struct PushdownMetrics {
    /// Number of filter expressions that were fully pushed down.
    supports_exact: AtomicU64,
    /// Number of filter expressions that were only partially supported.
    supports_unsupported: AtomicU64,
    /// Number of scan requests issued through providers.
    scans: AtomicU64,
    /// Total number of paged RPC calls used across scans.
    scan_rpc_pages: AtomicU64,
    /// Total rows inspected from storage before final filtering.
    scan_rows_scanned: AtomicU64,
    /// Total rows returned to the query engine.
    scan_rows_returned: AtomicU64,
    /// Total storage bytes processed while scanning.
    scan_bytes_scanned: AtomicU64,
}

/// Immutable snapshot view of [`PushdownMetrics`].
#[derive(Debug, Clone, Copy)]
pub struct PushdownMetricsSnapshot {
    /// Number of exact pushdown decisions.
    pub supports_exact: u64,
    /// Number of non-exact pushdown decisions.
    pub supports_unsupported: u64,
    /// Number of scan invocations.
    pub scans: u64,
    /// Number of scan RPC pages fetched.
    pub scan_rpc_pages: u64,
    /// Number of rows scanned from storage.
    pub scan_rows_scanned: u64,
    /// Number of rows emitted to DataFusion.
    pub scan_rows_returned: u64,
    /// Number of bytes scanned from storage.
    pub scan_bytes_scanned: u64,
}

impl PushdownMetrics {
    /// Records whether a filter expression can be executed exactly at storage.
    pub fn record_filter_support(&self, exact: bool) {
        // Decision: increment the "exact" or "unsupported" counter based on
        // whether storage-level filtering can preserve SQL semantics exactly.
        // Decision: evaluate `if exact {` to choose the correct SQL/storage control path.
        if exact {
            self.supports_exact.fetch_add(1, Ordering::Relaxed);
        } else {
            self.supports_unsupported.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Records scan-level cardinality and byte counters for one scan request.
    pub fn record_scan(
        &self,
        rpc_pages: u64,
        rows_scanned: u64,
        rows_returned: u64,
        bytes_scanned: u64,
    ) {
        self.scans.fetch_add(1, Ordering::Relaxed);
        self.scan_rpc_pages.fetch_add(rpc_pages, Ordering::Relaxed);
        self.scan_rows_scanned
            .fetch_add(rows_scanned, Ordering::Relaxed);
        self.scan_rows_returned
            .fetch_add(rows_returned, Ordering::Relaxed);
        self.scan_bytes_scanned
            .fetch_add(bytes_scanned, Ordering::Relaxed);
    }

    /// Captures a point-in-time copy of all counters.
    pub fn snapshot(&self) -> PushdownMetricsSnapshot {
        PushdownMetricsSnapshot {
            supports_exact: self.supports_exact.load(Ordering::Relaxed),
            supports_unsupported: self.supports_unsupported.load(Ordering::Relaxed),
            scans: self.scans.load(Ordering::Relaxed),
            scan_rpc_pages: self.scan_rpc_pages.load(Ordering::Relaxed),
            scan_rows_scanned: self.scan_rows_scanned.load(Ordering::Relaxed),
            scan_rows_returned: self.scan_rows_returned.load(Ordering::Relaxed),
            scan_bytes_scanned: self.scan_bytes_scanned.load(Ordering::Relaxed),
        }
    }

    /// Renders metrics in a plain-text format suitable for `/metrics`.
    pub fn render_text(&self) -> String {
        let s = self.snapshot();
        format!(
            "pushdown_support_exact={}\npushdown_support_unsupported={}\nscan_requests={}\nscan_rpc_pages={}\nscan_rows_scanned={}\nscan_rows_returned={}\nscan_bytes_scanned={}\n",
            s.supports_exact,
            s.supports_unsupported,
            s.scans,
            s.scan_rpc_pages,
            s.scan_rows_scanned,
            s.scan_rows_returned,
            s.scan_bytes_scanned,
        )
    }
}
