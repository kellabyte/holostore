//! In-process counters for SQL predicate pushdown and scan behavior.
//!
//! These metrics are intentionally lightweight and lock-free so they can be
//! updated on hot read paths without noticeable overhead.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

const TIMELINE_RING_CAPACITY: usize = 1024;
const INGEST_JOB_HISTORY_CAPACITY: usize = 256;

/// One active query execution tracked by session id.
#[derive(Debug, Clone)]
struct ActiveQueryExecution {
    query_execution_id: String,
    statement_kind: String,
    protocol: String,
    started_at_unix_ns: u64,
}

/// Timeline event used for query/stage observability.
#[derive(Debug, Clone)]
pub struct QueryStageEvent {
    pub query_execution_id: String,
    pub stage_id: u64,
    pub kind: String,
    pub detail: String,
    pub at_unix_ns: u64,
}

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

/// Circuit-breaker state used for per-target flow-control diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal request flow.
    Closed,
    /// Requests are being shed until open timeout elapses.
    Open,
    /// One probe request is allowed to test recovery.
    HalfOpen,
}

impl CircuitState {
    fn as_metric_value(self) -> u64 {
        match self {
            Self::Closed => 0,
            Self::Open => 1,
            Self::HalfOpen => 2,
        }
    }
}

/// Per-target distributed-write and circuit-breaker aggregates.
#[derive(Debug, Default, Clone, Copy)]
pub struct DistributedTargetMetrics {
    /// Number of apply RPC failures.
    pub apply_failures: u64,
    /// Number of retryable apply failures.
    pub retryable_failures: u64,
    /// Number of non-retryable apply failures.
    pub non_retryable_failures: u64,
    /// Number of times breaker transitioned to open.
    pub circuit_open_count: u64,
    /// Number of requests rejected while breaker was open/half-open.
    pub circuit_reject_count: u64,
    /// Current breaker state (`0=closed,1=open,2=half_open`).
    pub circuit_state: u64,
}

/// Admission rejection reason used for per-class overload diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejectKind {
    /// Request waited longer than the configured queue timeout.
    QueueTimeout,
    /// Request was rejected because queue depth exceeded configured limit.
    QueueLimit,
}

impl AdmissionRejectKind {
    fn as_metric_suffix(self) -> &'static str {
        match self {
            Self::QueueTimeout => "queue_timeout",
            Self::QueueLimit => "queue_limit",
        }
    }
}

/// Per-class admission-control metrics used for workload-management visibility.
#[derive(Debug, Default, Clone, Copy)]
pub struct AdmissionClassMetrics {
    /// Number of admitted statements for this class.
    pub admitted: u64,
    /// Number of overload rejections for this class.
    pub rejected: u64,
    /// Number of queue-timeout rejections for this class.
    pub rejected_queue_timeout: u64,
    /// Number of queue-depth-limit rejections for this class.
    pub rejected_queue_limit: u64,
    /// Number of times statements had to queue for this class.
    pub wait_count: u64,
    /// Sum of queue wait time in nanoseconds for this class.
    pub wait_ns_total: u64,
    /// Current observed queue depth.
    pub queue_depth: u64,
    /// Peak observed queue depth.
    pub queue_depth_peak: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IngestJobStatus {
    Running,
    Completed,
    Failed,
}

impl IngestJobStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
struct IngestJobMetrics {
    table_name: String,
    status: IngestJobStatus,
    started_at_unix_ns: u64,
    updated_at_unix_ns: u64,
    finished_at_unix_ns: u64,
    rows_ingested: u64,
    queue_depth: u64,
    inflight_rows: u64,
    inflight_bytes: u64,
    inflight_rpcs: u64,
    per_shard_lag: BTreeMap<usize, u64>,
    last_error: String,
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
    /// Number of query executions started.
    query_execution_started: AtomicU64,
    /// Number of query executions completed.
    query_execution_completed: AtomicU64,
    /// Number of query executions completed with failure.
    query_execution_failed: AtomicU64,
    /// Number of stage-level timeline events emitted.
    stage_events: AtomicU64,
    /// Number of scan page retries due to transient scan failures.
    scan_retry_count: AtomicU64,
    /// Number of scan reroutes triggered by topology/failure handling.
    scan_reroute_count: AtomicU64,
    /// Number of scan chunks emitted by the typed scan contract.
    scan_chunk_count: AtomicU64,
    /// Number of duplicate rows skipped during idempotent scan merge.
    scan_duplicate_rows_skipped: AtomicU64,
    /// Monotonic query execution id sequence.
    next_query_execution_id: AtomicU64,
    /// Monotonic stage execution id sequence.
    next_stage_execution_id: AtomicU64,
    /// Per-shard distributed write aggregates.
    distributed_by_shard: Mutex<BTreeMap<usize, DistributedShardMetrics>>,
    /// Per-target distributed write/circuit-breaker aggregates.
    distributed_by_target: Mutex<BTreeMap<String, DistributedTargetMetrics>>,
    /// Per-class admission-control counters/gauges.
    admission_by_class: Mutex<BTreeMap<String, AdmissionClassMetrics>>,
    /// Total rows ingested through streaming bulk paths.
    ingest_rows_ingested_total: AtomicU64,
    /// Number of bulk ingest jobs started.
    ingest_jobs_started: AtomicU64,
    /// Number of bulk ingest jobs completed successfully.
    ingest_jobs_completed: AtomicU64,
    /// Number of bulk ingest jobs completed with failure.
    ingest_jobs_failed: AtomicU64,
    /// Active/recent ingest job status by job id.
    ingest_jobs_by_id: Mutex<BTreeMap<String, IngestJobMetrics>>,
    /// Active query execution per session id.
    active_queries_by_session: Mutex<BTreeMap<String, ActiveQueryExecution>>,
    /// Ring buffer of recent stage/query timeline events.
    recent_stage_events: Mutex<VecDeque<QueryStageEvent>>,
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
    /// Number of query executions started.
    pub query_execution_started: u64,
    /// Number of query executions completed.
    pub query_execution_completed: u64,
    /// Number of query executions failed.
    pub query_execution_failed: u64,
    /// Number of stage timeline events emitted.
    pub stage_events: u64,
    /// Number of scan retries.
    pub scan_retry_count: u64,
    /// Number of scan reroutes.
    pub scan_reroute_count: u64,
    /// Number of scan chunks emitted.
    pub scan_chunk_count: u64,
    /// Number of duplicate scan rows skipped.
    pub scan_duplicate_rows_skipped: u64,
    /// Number of sessions with active query execution context.
    pub active_query_sessions: u64,
    /// Number of rows ingested through streaming bulk paths.
    pub ingest_rows_ingested_total: u64,
    /// Number of ingest jobs started.
    pub ingest_jobs_started: u64,
    /// Number of ingest jobs completed successfully.
    pub ingest_jobs_completed: u64,
    /// Number of ingest jobs completed with failure.
    pub ingest_jobs_failed: u64,
    /// Number of active/recent ingest jobs tracked.
    pub ingest_jobs_tracked: u64,
}

impl PushdownMetrics {
    /// Creates a query execution id and associates it with a session.
    pub fn begin_query_execution(
        &self,
        session_id: &str,
        statement_kind: &str,
        protocol: &str,
    ) -> String {
        let raw = self
            .next_query_execution_id
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let query_execution_id = format!("q{raw:016x}");
        let started_at_unix_ns = unix_timestamp_nanos();

        self.query_execution_started.fetch_add(1, Ordering::Relaxed);
        let previous = {
            let mut active = self
                .active_queries_by_session
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            active.insert(
                session_id.to_string(),
                ActiveQueryExecution {
                    query_execution_id: query_execution_id.clone(),
                    statement_kind: statement_kind.to_string(),
                    protocol: protocol.to_string(),
                    started_at_unix_ns,
                },
            )
        };
        if let Some(previous) = previous {
            self.query_execution_completed
                .fetch_add(1, Ordering::Relaxed);
            self.record_stage_event(
                previous.query_execution_id.as_str(),
                0,
                "query_finish",
                "result=detached",
            );
        }

        self.record_stage_event(
            query_execution_id.as_str(),
            0,
            "query_start",
            format!("statement={statement_kind} protocol={protocol}"),
        );
        query_execution_id
    }

    /// Marks a query execution as completed for a session.
    pub fn finish_query_execution(&self, session_id: &str, query_execution_id: &str, ok: bool) {
        let mut removed = false;
        {
            let mut active = self
                .active_queries_by_session
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if matches!(
                active.get(session_id),
                Some(existing) if existing.query_execution_id == query_execution_id
            ) {
                active.remove(session_id);
                removed = true;
            }
        }

        if removed {
            self.query_execution_completed
                .fetch_add(1, Ordering::Relaxed);
            if !ok {
                self.query_execution_failed.fetch_add(1, Ordering::Relaxed);
            }
            self.record_stage_event(
                query_execution_id,
                0,
                "query_finish",
                if ok {
                    "result=ok".to_string()
                } else {
                    "result=error".to_string()
                },
            );
        }
    }

    /// Returns the active query execution id for a session when present.
    pub fn active_query_execution_id(&self, session_id: &str) -> Option<String> {
        let active = self
            .active_queries_by_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        active
            .get(session_id)
            .map(|entry| entry.query_execution_id.clone())
    }

    /// Ensures a session has a query execution id, creating one if needed.
    pub fn ensure_query_execution_for_session(
        &self,
        session_id: &str,
        statement_kind: &str,
    ) -> String {
        if let Some(existing) = self.active_query_execution_id(session_id) {
            return existing;
        }
        self.begin_query_execution(session_id, statement_kind, "engine")
    }

    /// Allocates a monotonic stage execution id.
    pub fn next_stage_execution_id(&self) -> u64 {
        self.next_stage_execution_id
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }

    /// Emits one query/stage timeline event.
    pub fn record_stage_event(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        kind: &str,
        detail: impl Into<String>,
    ) {
        self.stage_events.fetch_add(1, Ordering::Relaxed);
        let mut recent = self
            .recent_stage_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        recent.push_back(QueryStageEvent {
            query_execution_id: query_execution_id.to_string(),
            stage_id,
            kind: kind.to_string(),
            detail: detail.into(),
            at_unix_ns: unix_timestamp_nanos(),
        });
        while recent.len() > TIMELINE_RING_CAPACITY {
            recent.pop_front();
        }
    }

    /// Returns a copy of most recent stage events.
    pub fn recent_stage_events(&self, limit: usize) -> Vec<QueryStageEvent> {
        let recent = self
            .recent_stage_events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if limit == 0 {
            return Vec::new();
        }
        recent
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// Records one scan retry.
    pub fn record_scan_retry(&self) {
        self.scan_retry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one scan reroute.
    pub fn record_scan_reroute(&self) {
        self.scan_reroute_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one emitted scan chunk.
    pub fn record_scan_chunk(&self) {
        self.scan_chunk_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a count of skipped duplicate scan rows.
    pub fn record_scan_duplicate_rows_skipped(&self, rows: u64) {
        self.scan_duplicate_rows_skipped
            .fetch_add(rows, Ordering::Relaxed);
    }

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

    /// Records one distributed apply failure for a target endpoint.
    pub fn record_distributed_apply_failure(&self, target: &str, retryable: bool) {
        let mut by_target = self
            .distributed_by_target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = by_target.entry(target.to_string()).or_default();
        entry.apply_failures = entry.apply_failures.saturating_add(1);
        if retryable {
            entry.retryable_failures = entry.retryable_failures.saturating_add(1);
        } else {
            entry.non_retryable_failures = entry.non_retryable_failures.saturating_add(1);
        }
    }

    /// Records a circuit-breaker state transition/refresh for one target.
    pub fn record_circuit_state(&self, target: &str, state: CircuitState) {
        let mut by_target = self
            .distributed_by_target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = by_target.entry(target.to_string()).or_default();
        entry.circuit_state = state.as_metric_value();
    }

    /// Records one transition into `open` for a target circuit breaker.
    pub fn record_circuit_open(&self, target: &str) {
        let mut by_target = self
            .distributed_by_target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = by_target.entry(target.to_string()).or_default();
        entry.circuit_open_count = entry.circuit_open_count.saturating_add(1);
        entry.circuit_state = CircuitState::Open.as_metric_value();
    }

    /// Records one request rejected because the breaker was not closed.
    pub fn record_circuit_reject(&self, target: &str) {
        let mut by_target = self
            .distributed_by_target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = by_target.entry(target.to_string()).or_default();
        entry.circuit_reject_count = entry.circuit_reject_count.saturating_add(1);
    }

    /// Records one statement timeout.
    pub fn record_statement_timeout(&self) {
        self.statement_timeout_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one admission-control rejection.
    pub fn record_admission_reject(&self) {
        self.admission_reject_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records current queue depth for one admission class.
    pub fn record_admission_queue_depth(&self, class: &str, queue_depth: u64) {
        let mut by_class = self
            .admission_by_class
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = by_class.entry(class.to_string()).or_default();
        entry.queue_depth = queue_depth;
        if queue_depth > entry.queue_depth_peak {
            entry.queue_depth_peak = queue_depth;
        }
    }

    /// Records one admitted statement for an admission class.
    pub fn record_admission_grant(&self, class: &str, wait: Duration) {
        let wait_ns = wait.as_nanos().min(u64::MAX as u128) as u64;
        let mut by_class = self
            .admission_by_class
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = by_class.entry(class.to_string()).or_default();
        entry.admitted = entry.admitted.saturating_add(1);
        entry.wait_count = entry.wait_count.saturating_add(1);
        entry.wait_ns_total = entry.wait_ns_total.saturating_add(wait_ns);
    }

    /// Records one admission rejection for an admission class and reason.
    pub fn record_admission_reject_class(&self, class: &str, reason: AdmissionRejectKind) {
        self.record_admission_reject();
        let mut by_class = self
            .admission_by_class
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = by_class.entry(class.to_string()).or_default();
        entry.rejected = entry.rejected.saturating_add(1);
        match reason {
            AdmissionRejectKind::QueueTimeout => {
                entry.rejected_queue_timeout = entry.rejected_queue_timeout.saturating_add(1)
            }
            AdmissionRejectKind::QueueLimit => {
                entry.rejected_queue_limit = entry.rejected_queue_limit.saturating_add(1)
            }
        }
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

    /// Starts/refreshes one bulk ingest job status entry.
    pub fn begin_ingest_job(&self, job_id: &str, table_name: &str) {
        let now = unix_timestamp_nanos();
        self.ingest_jobs_started.fetch_add(1, Ordering::Relaxed);
        let mut jobs = self
            .ingest_jobs_by_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        jobs.insert(
            job_id.to_string(),
            IngestJobMetrics {
                table_name: table_name.to_string(),
                status: IngestJobStatus::Running,
                started_at_unix_ns: now,
                updated_at_unix_ns: now,
                finished_at_unix_ns: 0,
                rows_ingested: 0,
                queue_depth: 0,
                inflight_rows: 0,
                inflight_bytes: 0,
                inflight_rpcs: 0,
                per_shard_lag: BTreeMap::new(),
                last_error: String::new(),
            },
        );
        while jobs.len() > INGEST_JOB_HISTORY_CAPACITY {
            if let Some(first_key) = jobs.keys().next().cloned() {
                jobs.remove(first_key.as_str());
            } else {
                break;
            }
        }
    }

    /// Updates progress counters for one active bulk ingest job.
    pub fn record_ingest_progress(
        &self,
        job_id: &str,
        rows_delta: u64,
        queue_depth: u64,
        inflight_rows: u64,
        inflight_bytes: u64,
        inflight_rpcs: u64,
        per_shard_lag: &[(usize, u64)],
    ) {
        if rows_delta > 0 {
            self.ingest_rows_ingested_total
                .fetch_add(rows_delta, Ordering::Relaxed);
        }
        let now = unix_timestamp_nanos();
        let mut jobs = self
            .ingest_jobs_by_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(job) = jobs.get_mut(job_id) {
            job.rows_ingested = job.rows_ingested.saturating_add(rows_delta);
            job.queue_depth = queue_depth;
            job.inflight_rows = inflight_rows;
            job.inflight_bytes = inflight_bytes;
            job.inflight_rpcs = inflight_rpcs;
            job.updated_at_unix_ns = now;
            job.per_shard_lag.clear();
            for (shard, lag) in per_shard_lag {
                job.per_shard_lag.insert(*shard, *lag);
            }
        }
    }

    /// Marks one ingest job as completed (success/failure) and stores final status.
    pub fn finish_ingest_job(&self, job_id: &str, success: bool, last_error: Option<&str>) {
        let now = unix_timestamp_nanos();
        if success {
            self.ingest_jobs_completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.ingest_jobs_failed.fetch_add(1, Ordering::Relaxed);
        }
        let mut jobs = self
            .ingest_jobs_by_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(job) = jobs.get_mut(job_id) {
            job.status = if success {
                IngestJobStatus::Completed
            } else {
                IngestJobStatus::Failed
            };
            job.finished_at_unix_ns = now;
            job.updated_at_unix_ns = now;
            job.inflight_rows = 0;
            job.inflight_bytes = 0;
            job.inflight_rpcs = 0;
            if let Some(err) = last_error {
                job.last_error = err.to_string();
            }
        }
    }

    /// Captures a point-in-time copy of all counters.
    pub fn snapshot(&self) -> PushdownMetricsSnapshot {
        let active_query_sessions = self
            .active_queries_by_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len() as u64;
        let ingest_jobs_tracked = self
            .ingest_jobs_by_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len() as u64;
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
            query_execution_started: self.query_execution_started.load(Ordering::Relaxed),
            query_execution_completed: self.query_execution_completed.load(Ordering::Relaxed),
            query_execution_failed: self.query_execution_failed.load(Ordering::Relaxed),
            stage_events: self.stage_events.load(Ordering::Relaxed),
            scan_retry_count: self.scan_retry_count.load(Ordering::Relaxed),
            scan_reroute_count: self.scan_reroute_count.load(Ordering::Relaxed),
            scan_chunk_count: self.scan_chunk_count.load(Ordering::Relaxed),
            scan_duplicate_rows_skipped: self.scan_duplicate_rows_skipped.load(Ordering::Relaxed),
            active_query_sessions,
            ingest_rows_ingested_total: self.ingest_rows_ingested_total.load(Ordering::Relaxed),
            ingest_jobs_started: self.ingest_jobs_started.load(Ordering::Relaxed),
            ingest_jobs_completed: self.ingest_jobs_completed.load(Ordering::Relaxed),
            ingest_jobs_failed: self.ingest_jobs_failed.load(Ordering::Relaxed),
            ingest_jobs_tracked,
        }
    }

    /// Renders metrics in a plain-text format suitable for `/metrics`.
    pub fn render_text(&self) -> String {
        let s = self.snapshot();
        let mut out = format!(
            "pushdown_support_exact={}\npushdown_support_unsupported={}\nscan_requests={}\nscan_rpc_pages={}\nscan_rows_scanned={}\nscan_rows_returned={}\nscan_bytes_scanned={}\ntx_begin_count={}\ntx_commit_count={}\ntx_rollback_count={}\ntx_conflict_count={}\ntx_commit_latency_ns_total={}\ndistributed_write_apply_ops={}\ndistributed_write_apply_rows={}\ndistributed_write_apply_latency_ns_total={}\ndistributed_write_rollback_ops={}\ndistributed_write_rollback_rows={}\ndistributed_write_rollback_latency_ns_total={}\ndistributed_write_conflicts={}\nstatement_timeout_count={}\nadmission_reject_count={}\nscan_row_limit_reject_count={}\ntxn_stage_limit_reject_count={}\nquery_execution_started={}\nquery_execution_completed={}\nquery_execution_failed={}\nstage_events={}\nscan_retry_count={}\nscan_reroute_count={}\nscan_chunk_count={}\nscan_duplicate_rows_skipped={}\nactive_query_sessions={}\ningest_rows_ingested_total={}\ningest_jobs_started={}\ningest_jobs_completed={}\ningest_jobs_failed={}\ningest_jobs_tracked={}\n",
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
            s.query_execution_started,
            s.query_execution_completed,
            s.query_execution_failed,
            s.stage_events,
            s.scan_retry_count,
            s.scan_reroute_count,
            s.scan_chunk_count,
            s.scan_duplicate_rows_skipped,
            s.active_query_sessions,
            s.ingest_rows_ingested_total,
            s.ingest_jobs_started,
            s.ingest_jobs_completed,
            s.ingest_jobs_failed,
            s.ingest_jobs_tracked,
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
        drop(by_shard);

        let by_target = self
            .distributed_by_target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (target, metrics) in by_target.iter() {
            let target = sanitize_metric_key(target.as_str());
            out.push_str(
                format!(
                    "distributed_write_target_{target}_apply_failures={}\ndistributed_write_target_{target}_retryable_failures={}\ndistributed_write_target_{target}_non_retryable_failures={}\ndistributed_write_target_{target}_circuit_open_count={}\ndistributed_write_target_{target}_circuit_reject_count={}\ndistributed_write_target_{target}_circuit_state={}\n",
                    metrics.apply_failures,
                    metrics.retryable_failures,
                    metrics.non_retryable_failures,
                    metrics.circuit_open_count,
                    metrics.circuit_reject_count,
                    metrics.circuit_state,
                )
                .as_str(),
            );
        }
        drop(by_target);

        let by_class = self
            .admission_by_class
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (class, metrics) in by_class.iter() {
            let class = sanitize_metric_key(class.as_str());
            out.push_str(
                format!(
                    "admission_{class}_admitted={}\nadmission_{class}_rejected={}\nadmission_{class}_rejected_{}={}\nadmission_{class}_rejected_{}={}\nadmission_{class}_wait_count={}\nadmission_{class}_wait_ns_total={}\nadmission_{class}_queue_depth={}\nadmission_{class}_queue_depth_peak={}\n",
                    metrics.admitted,
                    metrics.rejected,
                    AdmissionRejectKind::QueueTimeout.as_metric_suffix(),
                    metrics.rejected_queue_timeout,
                    AdmissionRejectKind::QueueLimit.as_metric_suffix(),
                    metrics.rejected_queue_limit,
                    metrics.wait_count,
                    metrics.wait_ns_total,
                    metrics.queue_depth,
                    metrics.queue_depth_peak,
                )
                .as_str(),
            );
        }
        drop(by_class);

        let ingest_jobs = self
            .ingest_jobs_by_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (idx, (job_id, job)) in ingest_jobs.iter().enumerate() {
            let elapsed_ns = if job.finished_at_unix_ns > 0 {
                job.finished_at_unix_ns
                    .saturating_sub(job.started_at_unix_ns)
            } else {
                unix_timestamp_nanos().saturating_sub(job.started_at_unix_ns)
            };
            let rows_per_second = if elapsed_ns == 0 {
                0u64
            } else {
                ((job.rows_ingested as u128)
                    .saturating_mul(1_000_000_000u128)
                    .saturating_div(elapsed_ns as u128))
                .min(u64::MAX as u128) as u64
            };
            out.push_str(
                format!(
                    "ingest_job_{idx}_id={}\ningest_job_{idx}_table={}\ningest_job_{idx}_status={}\ningest_job_{idx}_rows_ingested={}\ningest_job_{idx}_rows_per_second={}\ningest_job_{idx}_queue_depth={}\ningest_job_{idx}_inflight_rows={}\ningest_job_{idx}_inflight_bytes={}\ningest_job_{idx}_inflight_rpcs={}\ningest_job_{idx}_started_at_unix_ns={}\ningest_job_{idx}_updated_at_unix_ns={}\ningest_job_{idx}_finished_at_unix_ns={}\ningest_job_{idx}_last_error={}\n",
                    sanitize_metric_value(job_id.as_str()),
                    sanitize_metric_value(job.table_name.as_str()),
                    job.status.as_str(),
                    job.rows_ingested,
                    rows_per_second,
                    job.queue_depth,
                    job.inflight_rows,
                    job.inflight_bytes,
                    job.inflight_rpcs,
                    job.started_at_unix_ns,
                    job.updated_at_unix_ns,
                    job.finished_at_unix_ns,
                    sanitize_metric_value(job.last_error.as_str()),
                )
                .as_str(),
            );
            for (shard, lag) in &job.per_shard_lag {
                out.push_str(format!("ingest_job_{idx}_shard_{shard}_lag={lag}\n").as_str());
            }
        }
        drop(ingest_jobs);

        let active_queries = self
            .active_queries_by_session
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (idx, (session_id, active)) in active_queries.iter().enumerate() {
            out.push_str(
                format!(
                    "active_query_{idx}_session={}\nactive_query_{idx}_id={}\nactive_query_{idx}_statement={}\nactive_query_{idx}_protocol={}\nactive_query_{idx}_started_at_unix_ns={}\n",
                    sanitize_metric_value(session_id.as_str()),
                    sanitize_metric_value(active.query_execution_id.as_str()),
                    sanitize_metric_value(active.statement_kind.as_str()),
                    sanitize_metric_value(active.protocol.as_str()),
                    active.started_at_unix_ns
                )
                .as_str(),
            );
        }
        out
    }
}

fn unix_timestamp_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

fn sanitize_metric_value(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '\n' | '\r' | '\t' => '_',
            _ => ch,
        })
        .collect()
}

fn sanitize_metric_key(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}
