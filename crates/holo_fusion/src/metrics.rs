//! In-process counters for SQL predicate pushdown and scan behavior.
//!
//! These metrics are intentionally lightweight and lock-free so they can be
//! updated on hot read paths without noticeable overhead.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// Per-shard distributed write aggregates used for hotspot visibility.
#[derive(Debug, Default, Clone, Copy)]
pub struct DistributedShardMetrics {
    /// Number of apply RPCs issued for this shard.
    pub apply_ops: u64,
    /// Number of rows attempted in apply RPCs for this shard.
    pub apply_rows: u64,
    /// Sum of apply RPC latency in nanoseconds.
    pub apply_latency_ns: u64,
    /// Number of rollback RPCs issued for this shard.
    pub rollback_ops: u64,
    /// Number of rows attempted in rollback RPCs for this shard.
    pub rollback_rows: u64,
    /// Sum of rollback RPC latency in nanoseconds.
    pub rollback_latency_ns: u64,
    /// Number of conflict responses observed on this shard.
    pub conflicts: u64,
}

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
    /// Number of successful explicit `BEGIN` statements.
    tx_begin_count: AtomicU64,
    /// Number of successful explicit `COMMIT` statements.
    tx_commit_count: AtomicU64,
    /// Number of successful explicit `ROLLBACK` statements.
    tx_rollback_count: AtomicU64,
    /// Number of transaction conflicts (e.g. SQLSTATE `40001`).
    tx_conflict_count: AtomicU64,
    /// Sum of explicit `COMMIT` latency in nanoseconds.
    tx_commit_latency_ns_total: AtomicU64,
    /// Number of distributed write apply RPCs.
    distributed_write_apply_ops: AtomicU64,
    /// Number of rows sent through distributed write apply RPCs.
    distributed_write_apply_rows: AtomicU64,
    /// Sum of distributed write apply latency in nanoseconds.
    distributed_write_apply_latency_ns_total: AtomicU64,
    /// Number of distributed rollback RPCs.
    distributed_write_rollback_ops: AtomicU64,
    /// Number of rows sent through distributed rollback RPCs.
    distributed_write_rollback_rows: AtomicU64,
    /// Sum of distributed rollback latency in nanoseconds.
    distributed_write_rollback_latency_ns_total: AtomicU64,
    /// Number of distributed write conflicts.
    distributed_write_conflicts: AtomicU64,
    /// Number of statements rejected due to timeout.
    statement_timeout_count: AtomicU64,
    /// Number of statements rejected by admission control.
    admission_reject_count: AtomicU64,
    /// Number of statements rejected by scan row limit guardrail.
    scan_row_limit_reject_count: AtomicU64,
    /// Number of statements rejected by transaction staging limit guardrail.
    txn_stage_limit_reject_count: AtomicU64,
    /// Per-shard distributed write aggregates.
    distributed_by_shard: Mutex<BTreeMap<usize, DistributedShardMetrics>>,
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
    /// Number of successful explicit `BEGIN` statements.
    pub tx_begin_count: u64,
    /// Number of successful explicit `COMMIT` statements.
    pub tx_commit_count: u64,
    /// Number of successful explicit `ROLLBACK` statements.
    pub tx_rollback_count: u64,
    /// Number of transaction conflicts.
    pub tx_conflict_count: u64,
    /// Sum of `COMMIT` latency in nanoseconds.
    pub tx_commit_latency_ns_total: u64,
    /// Number of distributed write apply RPCs.
    pub distributed_write_apply_ops: u64,
    /// Number of rows sent through distributed write apply RPCs.
    pub distributed_write_apply_rows: u64,
    /// Sum of distributed write apply latency in nanoseconds.
    pub distributed_write_apply_latency_ns_total: u64,
    /// Number of distributed rollback RPCs.
    pub distributed_write_rollback_ops: u64,
    /// Number of rows sent through distributed rollback RPCs.
    pub distributed_write_rollback_rows: u64,
    /// Sum of distributed rollback latency in nanoseconds.
    pub distributed_write_rollback_latency_ns_total: u64,
    /// Number of distributed write conflicts.
    pub distributed_write_conflicts: u64,
    /// Number of timed out statements.
    pub statement_timeout_count: u64,
    /// Number of admission-control rejections.
    pub admission_reject_count: u64,
    /// Number of scan row limit rejections.
    pub scan_row_limit_reject_count: u64,
    /// Number of transaction staging limit rejections.
    pub txn_stage_limit_reject_count: u64,
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

    /// Records one successful explicit `BEGIN`.
    pub fn record_tx_begin(&self) {
        self.tx_begin_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one successful explicit `COMMIT` and its latency.
    pub fn record_tx_commit(&self, latency: Duration) {
        self.tx_commit_count.fetch_add(1, Ordering::Relaxed);
        self.tx_commit_latency_ns_total
            .fetch_add(latency.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Records one successful explicit `ROLLBACK`.
    pub fn record_tx_rollback(&self) {
        self.tx_rollback_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one explicit transaction conflict.
    pub fn record_tx_conflict(&self) {
        self.tx_conflict_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one distributed write apply RPC for a shard.
    pub fn record_distributed_apply(&self, shard_index: usize, rows: u64, latency: Duration) {
        let latency_ns = latency.as_nanos() as u64;
        self.distributed_write_apply_ops
            .fetch_add(1, Ordering::Relaxed);
        self.distributed_write_apply_rows
            .fetch_add(rows, Ordering::Relaxed);
        self.distributed_write_apply_latency_ns_total
            .fetch_add(latency_ns, Ordering::Relaxed);
        let mut by_shard = self
            .distributed_by_shard
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let shard = by_shard.entry(shard_index).or_default();
        shard.apply_ops = shard.apply_ops.saturating_add(1);
        shard.apply_rows = shard.apply_rows.saturating_add(rows);
        shard.apply_latency_ns = shard.apply_latency_ns.saturating_add(latency_ns);
    }

    /// Records one distributed write conflict for a shard.
    pub fn record_distributed_conflict(&self, shard_index: usize) {
        self.distributed_write_conflicts
            .fetch_add(1, Ordering::Relaxed);
        let mut by_shard = self
            .distributed_by_shard
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let shard = by_shard.entry(shard_index).or_default();
        shard.conflicts = shard.conflicts.saturating_add(1);
    }

    /// Records one distributed rollback RPC for a shard.
    pub fn record_distributed_rollback(&self, shard_index: usize, rows: u64, latency: Duration) {
        let latency_ns = latency.as_nanos() as u64;
        self.distributed_write_rollback_ops
            .fetch_add(1, Ordering::Relaxed);
        self.distributed_write_rollback_rows
            .fetch_add(rows, Ordering::Relaxed);
        self.distributed_write_rollback_latency_ns_total
            .fetch_add(latency_ns, Ordering::Relaxed);
        let mut by_shard = self
            .distributed_by_shard
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let shard = by_shard.entry(shard_index).or_default();
        shard.rollback_ops = shard.rollback_ops.saturating_add(1);
        shard.rollback_rows = shard.rollback_rows.saturating_add(rows);
        shard.rollback_latency_ns = shard.rollback_latency_ns.saturating_add(latency_ns);
    }

    /// Records one statement timeout.
    pub fn record_statement_timeout(&self) {
        self.statement_timeout_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one admission-control rejection.
    pub fn record_admission_reject(&self) {
        self.admission_reject_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one scan row limit rejection.
    pub fn record_scan_row_limit_reject(&self) {
        self.scan_row_limit_reject_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records one transaction staging limit rejection.
    pub fn record_txn_stage_limit_reject(&self) {
        self.txn_stage_limit_reject_count
            .fetch_add(1, Ordering::Relaxed);
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
            tx_begin_count: self.tx_begin_count.load(Ordering::Relaxed),
            tx_commit_count: self.tx_commit_count.load(Ordering::Relaxed),
            tx_rollback_count: self.tx_rollback_count.load(Ordering::Relaxed),
            tx_conflict_count: self.tx_conflict_count.load(Ordering::Relaxed),
            tx_commit_latency_ns_total: self.tx_commit_latency_ns_total.load(Ordering::Relaxed),
            distributed_write_apply_ops: self.distributed_write_apply_ops.load(Ordering::Relaxed),
            distributed_write_apply_rows: self.distributed_write_apply_rows.load(Ordering::Relaxed),
            distributed_write_apply_latency_ns_total: self
                .distributed_write_apply_latency_ns_total
                .load(Ordering::Relaxed),
            distributed_write_rollback_ops: self
                .distributed_write_rollback_ops
                .load(Ordering::Relaxed),
            distributed_write_rollback_rows: self
                .distributed_write_rollback_rows
                .load(Ordering::Relaxed),
            distributed_write_rollback_latency_ns_total: self
                .distributed_write_rollback_latency_ns_total
                .load(Ordering::Relaxed),
            distributed_write_conflicts: self.distributed_write_conflicts.load(Ordering::Relaxed),
            statement_timeout_count: self.statement_timeout_count.load(Ordering::Relaxed),
            admission_reject_count: self.admission_reject_count.load(Ordering::Relaxed),
            scan_row_limit_reject_count: self.scan_row_limit_reject_count.load(Ordering::Relaxed),
            txn_stage_limit_reject_count: self.txn_stage_limit_reject_count.load(Ordering::Relaxed),
        }
    }

    /// Renders metrics in a plain-text format suitable for `/metrics`.
    pub fn render_text(&self) -> String {
        let s = self.snapshot();
        let mut out = format!(
            "pushdown_support_exact={}\npushdown_support_unsupported={}\nscan_requests={}\nscan_rpc_pages={}\nscan_rows_scanned={}\nscan_rows_returned={}\nscan_bytes_scanned={}\ntx_begin_count={}\ntx_commit_count={}\ntx_rollback_count={}\ntx_conflict_count={}\ntx_commit_latency_ns_total={}\ndistributed_write_apply_ops={}\ndistributed_write_apply_rows={}\ndistributed_write_apply_latency_ns_total={}\ndistributed_write_rollback_ops={}\ndistributed_write_rollback_rows={}\ndistributed_write_rollback_latency_ns_total={}\ndistributed_write_conflicts={}\nstatement_timeout_count={}\nadmission_reject_count={}\nscan_row_limit_reject_count={}\ntxn_stage_limit_reject_count={}\n",
            s.supports_exact,
            s.supports_unsupported,
            s.scans,
            s.scan_rpc_pages,
            s.scan_rows_scanned,
            s.scan_rows_returned,
            s.scan_bytes_scanned,
            s.tx_begin_count,
            s.tx_commit_count,
            s.tx_rollback_count,
            s.tx_conflict_count,
            s.tx_commit_latency_ns_total,
            s.distributed_write_apply_ops,
            s.distributed_write_apply_rows,
            s.distributed_write_apply_latency_ns_total,
            s.distributed_write_rollback_ops,
            s.distributed_write_rollback_rows,
            s.distributed_write_rollback_latency_ns_total,
            s.distributed_write_conflicts,
            s.statement_timeout_count,
            s.admission_reject_count,
            s.scan_row_limit_reject_count,
            s.txn_stage_limit_reject_count,
        );
        let by_shard = self
            .distributed_by_shard
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (shard, metrics) in by_shard.iter() {
            out.push_str(
                format!(
                    "distributed_write_shard_{shard}_apply_ops={}\ndistributed_write_shard_{shard}_apply_rows={}\ndistributed_write_shard_{shard}_apply_latency_ns={}\ndistributed_write_shard_{shard}_rollback_ops={}\ndistributed_write_shard_{shard}_rollback_rows={}\ndistributed_write_shard_{shard}_rollback_latency_ns={}\ndistributed_write_shard_{shard}_conflicts={}\n",
                    metrics.apply_ops,
                    metrics.apply_rows,
                    metrics.apply_latency_ns,
                    metrics.rollback_ops,
                    metrics.rollback_rows,
                    metrics.rollback_latency_ns,
                    metrics.conflicts,
                )
                .as_str(),
            );
        }
        out
    }
}
