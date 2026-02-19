//! DataFusion table provider backed by HoloStore key/value primitives.
//!
//! This module owns row encoding/decoding, scan/write paths, conditional
//! mutation helpers, and DataFusion `TableProvider` integration.

use std::any::Any;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, StringBuilder, TimestampNanosecondBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::MemTable;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{
    not_impl_err, DataFusionError, Result as DFResult, ScalarValue, SchemaExt,
};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{col, Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures_util::StreamExt;
use holo_store::{
    HoloStoreClient, LatestEntry, ReplicatedConditionalWriteEntry, ReplicatedWriteEntry, RpcVersion,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, info_span, warn, Instrument};

use crate::indexing::{
    decode_secondary_index_primary_key, decode_secondary_index_row_values,
    encode_secondary_index_lookup_prefix_for_prefix, encode_secondary_index_row_key,
    encode_secondary_index_row_value, encode_secondary_index_unique_key,
    encode_secondary_index_unique_value, index_tombstone_value, is_sql_tombstone_value,
    SecondaryIndexDistribution, SecondaryIndexRecord, SecondaryIndexState,
};
use crate::metadata::{
    apply_column_defaults_for_missing, table_model_row_v1, validate_row_against_metadata,
    PrimaryKeyDistribution, TableCheckConstraintRecord, TableColumnRecord, TableColumnType,
    TableMetadataRecord,
};
use crate::metrics::{CircuitState, PushdownMetrics};
use crate::optimizer::planner::{AccessPath, PlannedAccessPath, QueryShape};
use crate::optimizer::predicates::{extract_predicate_summary, ColumnPredicate, PredicateSummary};
use crate::optimizer::stats::{build_table_stats_from_sample, TableStats};
use crate::optimizer::OptimizerEngine;
use crate::topology::{
    fetch_topology, min_end_bound, require_non_empty_topology, route_key, scan_targets,
};

pub const ORDERS_TABLE_NAME: &str = "orders";
pub const ORDERS_TABLE_ID: u64 = 100;
pub const ORDERS_TABLE_MODEL: &str = "orders_v1";
pub const ORDERS_SHARD_INDEX: usize = 0;

const DATA_PREFIX_PRIMARY_ROW: u8 = 0x20;
const HASH_BUCKET_TAG: u8 = 0x68;
const TUPLE_TAG_INT64: u8 = 0x02;
const ROW_FORMAT_VERSION_V1: u8 = 1;
const ROW_FORMAT_VERSION_V2: u8 = 2;
const ROW_FLAG_TOMBSTONE: u8 = 0x01;
const SIGN_FLIP_MASK: u64 = 1u64 << 63;
const DEFAULT_MAX_SCAN_ROWS: usize = 0;
const DEFAULT_HASH_PK_BUCKETS: usize = 32;
const DEFAULT_DISTRIBUTED_WRITE_MAX_BATCH_ENTRIES: usize = 1_024;
const DEFAULT_DISTRIBUTED_WRITE_MAX_BATCH_BYTES: usize = 1_048_576;
const DEFAULT_DISTRIBUTED_WRITE_RETRY_LIMIT: usize = 5;
const DEFAULT_DISTRIBUTED_WRITE_RETRY_BASE_DELAY_MS: u64 = 50;
const DEFAULT_DISTRIBUTED_WRITE_RETRY_MAX_DELAY_MS: u64 = 1_000;
const DEFAULT_DISTRIBUTED_WRITE_RETRY_BUDGET_MS: u64 = 60_000;
const DEFAULT_DISTRIBUTED_SCAN_RETRY_LIMIT: usize = 5;
const DEFAULT_DISTRIBUTED_SCAN_RETRY_DELAY_MS: u64 = 60;
const DEFAULT_DISTRIBUTED_CIRCUIT_BREAKER_FAILURE_THRESHOLD: u32 = 4;
const DEFAULT_DISTRIBUTED_CIRCUIT_BREAKER_OPEN_MS: u64 = 2_000;
const DEFAULT_DISTRIBUTED_WRITE_MAX_INFLIGHT_ROWS: usize = 32_768;
const DEFAULT_DISTRIBUTED_WRITE_MAX_INFLIGHT_BYTES: usize = 32 * 1_024 * 1_024;
const DEFAULT_DISTRIBUTED_WRITE_MAX_INFLIGHT_RPCS: usize = 32;
const DEFAULT_INDEX_BACKFILL_PAGE_SIZE: usize = 512;
const DEFAULT_BULK_CHUNK_ROWS_INITIAL: usize = 1_024;
const DEFAULT_BULK_CHUNK_ROWS_MIN: usize = 128;
const DEFAULT_BULK_CHUNK_ROWS_MAX: usize = 8_192;
const DEFAULT_BULK_CHUNK_LOW_LATENCY_MS: u64 = 40;
const DEFAULT_BULK_CHUNK_HIGH_LATENCY_MS: u64 = 150;
const DEFAULT_OPTIMIZER_BOOTSTRAP_ROW_COUNT: u64 = 4_096;

/// Executes `default max scan rows` for this component.
fn default_max_scan_rows() -> usize {
    DEFAULT_MAX_SCAN_ROWS
}

/// Executes `configured max scan rows` for this component.
fn configured_max_scan_rows() -> usize {
    std::env::var("HOLO_FUSION_SCAN_MAX_ROWS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_SCAN_ROWS)
}

/// Executes `configured write max batch entries` for this component.
fn configured_write_max_batch_entries() -> usize {
    std::env::var("HOLO_FUSION_DML_WRITE_MAX_BATCH_ENTRIES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_WRITE_MAX_BATCH_ENTRIES)
}

/// Executes `configured write max batch bytes` for this component.
fn configured_write_max_batch_bytes() -> usize {
    std::env::var("HOLO_FUSION_DML_WRITE_MAX_BATCH_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_WRITE_MAX_BATCH_BYTES)
}

/// Executes `configured bulk chunk rows initial` for this component.
fn configured_bulk_chunk_rows_initial() -> usize {
    std::env::var("HOLO_FUSION_BULK_CHUNK_ROWS_INITIAL")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BULK_CHUNK_ROWS_INITIAL)
}

/// Executes `configured bulk chunk rows min` for this component.
fn configured_bulk_chunk_rows_min() -> usize {
    std::env::var("HOLO_FUSION_BULK_CHUNK_ROWS_MIN")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BULK_CHUNK_ROWS_MIN)
}

/// Executes `configured bulk chunk rows max` for this component.
fn configured_bulk_chunk_rows_max() -> usize {
    std::env::var("HOLO_FUSION_BULK_CHUNK_ROWS_MAX")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BULK_CHUNK_ROWS_MAX)
}

/// Executes `configured bulk chunk low latency ms` for this component.
fn configured_bulk_chunk_low_latency_ms() -> u64 {
    std::env::var("HOLO_FUSION_BULK_CHUNK_LOW_LATENCY_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BULK_CHUNK_LOW_LATENCY_MS)
}

/// Executes `configured bulk chunk high latency ms` for this component.
fn configured_bulk_chunk_high_latency_ms() -> u64 {
    std::env::var("HOLO_FUSION_BULK_CHUNK_HIGH_LATENCY_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_BULK_CHUNK_HIGH_LATENCY_MS)
}

fn configured_write_retry_limit() -> usize {
    std::env::var("HOLO_FUSION_DML_RETRY_MAX_ATTEMPTS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_WRITE_RETRY_LIMIT)
}

fn configured_write_retry_base_delay_ms() -> u64 {
    std::env::var("HOLO_FUSION_DML_RETRY_BASE_DELAY_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_WRITE_RETRY_BASE_DELAY_MS)
}

fn configured_write_retry_max_delay_ms() -> u64 {
    std::env::var("HOLO_FUSION_DML_RETRY_MAX_DELAY_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_WRITE_RETRY_MAX_DELAY_MS)
}

fn configured_write_retry_budget_ms() -> u64 {
    std::env::var("HOLO_FUSION_DML_RETRY_BUDGET_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_WRITE_RETRY_BUDGET_MS)
}

fn configured_scan_retry_limit() -> usize {
    std::env::var("HOLO_FUSION_SCAN_RETRY_MAX_ATTEMPTS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_SCAN_RETRY_LIMIT)
}

fn configured_scan_retry_delay_ms() -> u64 {
    std::env::var("HOLO_FUSION_SCAN_RETRY_DELAY_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_SCAN_RETRY_DELAY_MS)
}

fn configured_scan_parallelism() -> usize {
    std::env::var("HOLO_FUSION_SCAN_PARALLELISM")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|parallelism| parallelism.get())
                .unwrap_or(8)
                .clamp(1, 64)
        })
}

fn configured_write_inflight_max_rows() -> usize {
    std::env::var("HOLO_FUSION_DML_WRITE_MAX_INFLIGHT_ROWS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_WRITE_MAX_INFLIGHT_ROWS)
}

fn configured_write_inflight_max_bytes() -> usize {
    std::env::var("HOLO_FUSION_DML_WRITE_MAX_INFLIGHT_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_WRITE_MAX_INFLIGHT_BYTES)
}

fn configured_write_inflight_max_rpcs() -> usize {
    std::env::var("HOLO_FUSION_DML_WRITE_MAX_INFLIGHT_RPCS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_WRITE_MAX_INFLIGHT_RPCS)
}

fn configured_index_backfill_page_size() -> usize {
    std::env::var("HOLO_FUSION_INDEX_BACKFILL_PAGE_SIZE")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_INDEX_BACKFILL_PAGE_SIZE)
}

fn configured_circuit_breaker_failure_threshold() -> u32 {
    std::env::var("HOLO_FUSION_DML_CIRCUIT_BREAKER_FAILURE_THRESHOLD")
        .ok()
        .and_then(|raw| raw.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_CIRCUIT_BREAKER_FAILURE_THRESHOLD)
}

fn configured_circuit_breaker_open_ms() -> u64 {
    std::env::var("HOLO_FUSION_DML_CIRCUIT_BREAKER_OPEN_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_DISTRIBUTED_CIRCUIT_BREAKER_OPEN_MS)
}

fn configured_retry_governance_enabled() -> bool {
    std::env::var("HOLO_FUSION_PHASE10_RETRY_GOVERNANCE_ENABLED")
        .ok()
        .and_then(|raw| raw.parse::<bool>().ok())
        .unwrap_or(true)
}

fn configured_circuit_breaker_enabled() -> bool {
    std::env::var("HOLO_FUSION_PHASE10_CIRCUIT_BREAKER_ENABLED")
        .ok()
        .and_then(|raw| raw.parse::<bool>().ok())
        .unwrap_or(true)
}

fn configured_bulk_ingest_enabled() -> bool {
    std::env::var("HOLO_FUSION_PHASE10_BULK_INGEST_ENABLED")
        .ok()
        .and_then(|raw| raw.parse::<bool>().ok())
        .unwrap_or(true)
}

/// Classifies distributed write failures for retry governance and SLO metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryableErrorClass {
    Topology,
    TransientRpc,
    Overload,
    NonRetryable,
}

impl RetryableErrorClass {
    fn is_retryable(self) -> bool {
        !matches!(self, Self::NonRetryable)
    }
}

#[derive(Debug, Clone, Copy)]
struct RetryPolicy {
    enabled: bool,
    max_attempts: usize,
    max_elapsed: Duration,
    base_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    fn from_env() -> Self {
        let base_delay_ms = configured_write_retry_base_delay_ms();
        let max_delay_ms = configured_write_retry_max_delay_ms().max(base_delay_ms);
        Self {
            enabled: configured_retry_governance_enabled(),
            max_attempts: configured_write_retry_limit().max(1),
            max_elapsed: Duration::from_millis(configured_write_retry_budget_ms()),
            base_delay: Duration::from_millis(base_delay_ms),
            max_delay: Duration::from_millis(max_delay_ms),
        }
    }

    fn max_attempts(self) -> usize {
        if self.enabled {
            self.max_attempts.max(1)
        } else {
            1
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct InflightBudgetConfig {
    max_rows: usize,
    max_bytes: usize,
    max_rpcs: usize,
}

impl InflightBudgetConfig {
    fn from_env() -> Self {
        Self {
            max_rows: configured_write_inflight_max_rows().max(1),
            max_bytes: configured_write_inflight_max_bytes().max(1),
            max_rpcs: configured_write_inflight_max_rpcs().max(1),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CircuitBreakerConfig {
    enabled: bool,
    failure_threshold: u32,
    open_duration: Duration,
}

impl CircuitBreakerConfig {
    fn from_env() -> Self {
        Self {
            enabled: configured_circuit_breaker_enabled(),
            failure_threshold: configured_circuit_breaker_failure_threshold().max(1),
            open_duration: Duration::from_millis(configured_circuit_breaker_open_ms()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CircuitAcquireKind {
    Closed,
    HalfOpenProbe,
}

#[derive(Debug, Clone, Copy)]
struct CircuitAcquireToken {
    kind: CircuitAcquireKind,
}

#[derive(Debug, Clone, Copy)]
struct TargetCircuitState {
    state: CircuitState,
    consecutive_failures: u32,
    open_until: Option<Instant>,
    half_open_probe_inflight: bool,
}

impl Default for TargetCircuitState {
    fn default() -> Self {
        Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            open_until: None,
            half_open_probe_inflight: false,
        }
    }
}

/// Structured overload rejection used for deterministic SQLSTATE mapping.
#[derive(Debug, Clone)]
pub struct OverloadError {
    message: String,
}

impl OverloadError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for OverloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl std::error::Error for OverloadError {}

/// Returns `true` when `message` encodes overload/flow-control rejection.
pub fn is_overload_error_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("server is overloaded")
        || lower.contains("circuit breaker open")
        || lower.contains("write inflight budget exceeded")
}

/// Backoff policy for distributed write retries with bounded jitter.
fn retry_backoff(policy: RetryPolicy, attempt: usize, jitter_seed: u64) -> std::time::Duration {
    let exp = 1u64 << attempt.min(8);
    let base_ms = policy.base_delay.as_millis().min(u64::MAX as u128) as u64;
    let max_ms = policy.max_delay.as_millis().min(u64::MAX as u128) as u64;
    let without_jitter = base_ms.saturating_mul(exp).min(max_ms);
    // Decision: deterministic pseudo-jitter avoids synchronization without
    // requiring additional RNG dependencies.
    let jitter = jitter_seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add((attempt as u64).saturating_mul(0x9E3779B97F4A7C15));
    let jitter_ms = jitter % (without_jitter.saturating_div(4).max(1));
    std::time::Duration::from_millis(without_jitter.saturating_add(jitter_ms))
}

fn classify_write_error_message(message: &str) -> RetryableErrorClass {
    let lower = message.to_ascii_lowercase();
    if lower.contains("key routed to shard")
        || lower.contains("request targeted shard")
        || lower.contains("no shard route found")
        || lower.contains("split key does not map")
        || lower.contains("split key must")
        || lower.contains("range metadata changed")
    {
        return RetryableErrorClass::Topology;
    }
    if lower.contains("timed out")
        || lower.contains("deadline")
        || lower.contains("temporarily unavailable")
        || lower.contains("unavailable")
        || lower.contains("failed to reach quorum")
        || lower.contains("quorum not reached")
        || lower.contains("preaccept")
        || lower.contains("pre-accept")
        || lower.contains("transport")
        || lower.contains("connection reset")
        || lower.contains("connection refused")
        || lower.contains("broken pipe")
        || lower.contains("network")
    {
        return RetryableErrorClass::TransientRpc;
    }
    if is_overload_error_message(message) || lower.contains("too many requests") {
        return RetryableErrorClass::Overload;
    }
    RetryableErrorClass::NonRetryable
}

/// Estimates one replicated write entry payload size for batch sizing.
fn replicated_write_entry_size(entry: &ReplicatedWriteEntry) -> usize {
    entry
        .key
        .len()
        .saturating_add(entry.value.len())
        .saturating_add(32)
}

/// Estimates one conditional write payload size for batch sizing.
fn prepared_conditional_entry_size(entry: &PreparedConditionalEntry) -> usize {
    entry
        .key
        .len()
        .saturating_add(entry.value.len())
        .saturating_add(40)
}

/// Splits replicated writes into bounded chunks to avoid oversized RPCs.
fn chunk_replicated_writes(
    entries: Vec<ReplicatedWriteEntry>,
    max_entries: usize,
    max_bytes: usize,
) -> Vec<Vec<ReplicatedWriteEntry>> {
    let max_entries = max_entries.max(1);
    let max_bytes = max_bytes.max(1);
    let mut iter = entries.into_iter().peekable();
    let mut chunks = Vec::new();

    while let Some(first) = iter.next() {
        let mut chunk = Vec::with_capacity(max_entries.min(128));
        let mut chunk_bytes = replicated_write_entry_size(&first);
        chunk.push(first);

        while chunk.len() < max_entries {
            let Some(next) = iter.peek() else {
                break;
            };
            let next_size = replicated_write_entry_size(next);
            if chunk_bytes.saturating_add(next_size) > max_bytes {
                break;
            }
            chunk_bytes = chunk_bytes.saturating_add(next_size);
            let next_owned = iter.next().expect("peeked entry should exist");
            chunk.push(next_owned);
        }

        chunks.push(chunk);
    }

    chunks
}

/// Builds contiguous index ranges for bounded conditional write RPC chunks.
fn conditional_chunk_ranges(
    entries: &[PreparedConditionalEntry],
    max_entries: usize,
    max_bytes: usize,
) -> Vec<Range<usize>> {
    if entries.is_empty() {
        return Vec::new();
    }
    let max_entries = max_entries.max(1);
    let max_bytes = max_bytes.max(1);
    let mut ranges = Vec::new();
    let mut start = 0usize;

    while start < entries.len() {
        let mut end = start;
        let mut bytes = 0usize;

        while end < entries.len() && end.saturating_sub(start) < max_entries {
            let entry_bytes = prepared_conditional_entry_size(&entries[end]);
            if end > start && bytes.saturating_add(entry_bytes) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(entry_bytes);
            end = end.saturating_add(1);
        }

        if end == start {
            end = end.saturating_add(1);
        }
        ranges.push(start..end);
        start = end;
    }

    ranges
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents the `OrdersSeedRow` component used by the holo_fusion runtime.
pub struct OrdersSeedRow {
    pub order_id: i64,
    pub customer_id: i64,
    pub status: Option<String>,
    pub total_cents: i64,
    pub created_at_ns: i64,
}

impl OrdersSeedRow {
    /// Executes `new` for this component.
    pub fn new(
        order_id: i64,
        customer_id: i64,
        status: Option<impl Into<String>>,
        total_cents: i64,
        created_at_ns: i64,
    ) -> Self {
        Self {
            order_id,
            customer_id,
            status: status.map(Into::into),
            total_cents,
            created_at_ns,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents the `VersionedOrdersRow` component used by the holo_fusion runtime.
pub struct VersionedOrdersRow {
    pub row: OrdersSeedRow,
    pub version: RpcVersion,
}

#[derive(Debug, Clone)]
/// Represents the `ConditionalOrderWrite` component used by the holo_fusion runtime.
pub struct ConditionalOrderWrite {
    pub order_id: i64,
    pub expected_version: RpcVersion,
    pub value: Vec<u8>,
    pub rollback_value: Vec<u8>,
}

#[derive(Debug, Clone)]
/// Generic conditional write keyed by normalized signed-integer primary key.
pub struct ConditionalPrimaryWrite {
    pub primary_key: i64,
    pub expected_version: RpcVersion,
    pub value: Vec<u8>,
    pub rollback_value: Vec<u8>,
}

#[derive(Debug, Clone)]
/// Generic conditional write keyed directly by storage key bytes.
struct ConditionalStorageWrite {
    key: Vec<u8>,
    expected_version: RpcVersion,
    value: Vec<u8>,
    rollback_value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EncodedSecondaryIndexMutation {
    row_key: Vec<u8>,
    row_value: Vec<u8>,
    unique_key: Option<Vec<u8>>,
    unique_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
/// Generic row decoded from a row_v1 table scan with storage version.
pub struct VersionedGenericRow {
    pub primary_key: i64,
    pub values: Vec<ScalarValue>,
    pub version: RpcVersion,
}

#[derive(Debug, Clone)]
/// One conjunction filter clause used by grouped aggregate pushdown.
pub struct GroupedAggregateFilter {
    pub column: String,
    pub allowed_values: Vec<ScalarValue>,
}

#[derive(Debug, Clone)]
/// Query shape supported by grouped aggregate + top-k pushdown path.
pub struct GroupedAggregateTopKSpec {
    pub group_columns: Vec<String>,
    pub sum_column: String,
    pub filters: Vec<GroupedAggregateFilter>,
    pub having_min_count: u64,
    pub limit: usize,
}

#[derive(Debug, Clone)]
/// One output row emitted by grouped aggregate + top-k pushdown path.
pub struct GroupedAggregateTopKRow {
    pub group_values: Vec<ScalarValue>,
    pub count: u64,
    pub sum: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Enumerates states/variants for `ConditionalWriteOutcome`.
pub enum ConditionalWriteOutcome {
    Applied(u64),
    Conflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Supported primary-key aggregate extremes for fast-path execution.
pub enum PrimaryKeyExtreme {
    Min,
    Max,
}

impl PrimaryKeyExtreme {
    /// Returns `true` when the storage scan should run in reverse key order.
    fn reverse_scan(self) -> bool {
        matches!(self, Self::Max)
    }

    /// Picks the best aggregate candidate under this extreme.
    fn pick(self, current: i64, candidate: i64) -> i64 {
        match self {
            Self::Min => current.min(candidate),
            Self::Max => current.max(candidate),
        }
    }
}

#[derive(Debug, Clone)]
/// Detailed result of one conditional apply operation including rollback metadata.
struct ConditionalWriteAppliedBatch {
    applied_rows: u64,
    applied_targets: Vec<(WriteTarget, Vec<PreparedConditionalEntry>)>,
}

#[derive(Debug, Clone)]
/// Detailed conditional apply outcome used by streaming ingest/rollback flows.
enum ConditionalWriteDetailedOutcome {
    Applied(ConditionalWriteAppliedBatch),
    Conflict,
}

#[derive(Debug, Clone)]
/// Structured duplicate-key violation used to preserve SQLSTATE mapping.
pub struct DuplicateKeyViolation {
    constraint_name: String,
}

impl DuplicateKeyViolation {
    /// Creates a duplicate-key violation for `<table>_pkey`.
    fn for_table(table_name: &str) -> Self {
        Self {
            constraint_name: format!("{table_name}_pkey"),
        }
    }

    /// Creates a duplicate-key violation for an explicit constraint/index name.
    fn for_constraint(constraint_name: impl Into<String>) -> Self {
        Self {
            constraint_name: constraint_name.into(),
        }
    }
}

impl fmt::Display for DuplicateKeyViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "duplicate key value violates unique constraint '{}'",
            self.constraint_name
        )
    }
}

impl std::error::Error for DuplicateKeyViolation {}

/// Returns `true` when `message` encodes a duplicate-key violation.
pub fn is_duplicate_key_violation_message(message: &str) -> bool {
    message.contains("duplicate key value violates unique constraint")
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Represents the `HoloProviderCodecSpec` component used by the holo_fusion runtime.
pub struct HoloProviderCodecSpec {
    pub table_name: String,
    pub table_id: u64,
    pub table_model: String,
    #[serde(default)]
    pub columns: Vec<TableColumnRecord>,
    #[serde(default)]
    pub check_constraints: Vec<TableCheckConstraintRecord>,
    #[serde(default)]
    pub primary_key_column: Option<String>,
    #[serde(default)]
    pub primary_key_distribution: PrimaryKeyDistribution,
    #[serde(default)]
    pub primary_key_hash_buckets: Option<usize>,
    #[serde(default)]
    pub secondary_indexes: Vec<SecondaryIndexRecord>,
    pub preferred_shards: Vec<usize>,
    pub page_size: usize,
    #[serde(default = "default_max_scan_rows")]
    pub max_scan_rows: usize,
    pub local_grpc_addr: String,
}

/// Executes `orders schema` for this component.
pub fn orders_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("total_cents", DataType::Int64, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

/// Returns canonical column metadata for the legacy orders row model.
fn orders_columns() -> Vec<TableColumnRecord> {
    vec![
        TableColumnRecord {
            name: "order_id".to_string(),
            column_type: TableColumnType::Int64,
            nullable: false,
            default_value: None,
        },
        TableColumnRecord {
            name: "customer_id".to_string(),
            column_type: TableColumnType::Int64,
            nullable: false,
            default_value: None,
        },
        TableColumnRecord {
            name: "status".to_string(),
            column_type: TableColumnType::Utf8,
            nullable: true,
            default_value: None,
        },
        TableColumnRecord {
            name: "total_cents".to_string(),
            column_type: TableColumnType::Int64,
            nullable: false,
            default_value: None,
        },
        TableColumnRecord {
            name: "created_at".to_string(),
            column_type: TableColumnType::TimestampNanosecond,
            nullable: false,
            default_value: None,
        },
    ]
}

/// Executes `encode orders latest entry` for this component.
pub fn encode_orders_latest_entry(row: &OrdersSeedRow, version: RpcVersion) -> LatestEntry {
    LatestEntry {
        key: encode_orders_primary_key(row.order_id),
        value: encode_orders_row_value(row),
        version,
    }
}

/// Executes `encode table primary key` for this component.
pub fn encode_table_primary_key(table_id: u64, primary_key: i64) -> Vec<u8> {
    encode_primary_key(table_id, primary_key)
}

/// Executes `encode orders primary key` for this component.
pub fn encode_orders_primary_key(order_id: i64) -> Vec<u8> {
    encode_table_primary_key(ORDERS_TABLE_ID, order_id)
}

/// Executes `table key start` for this component.
pub fn table_key_start(table_id: u64) -> Vec<u8> {
    table_key_prefix(table_id)
}

/// Executes `table key end` for this component.
pub fn table_key_end(table_id: u64) -> Vec<u8> {
    prefix_end(&table_key_start(table_id)).unwrap_or_default()
}

/// Executes `orders table key start` for this component.
pub fn orders_table_key_start() -> Vec<u8> {
    table_key_start(ORDERS_TABLE_ID)
}

/// Executes `orders table key end` for this component.
pub fn orders_table_key_end() -> Vec<u8> {
    table_key_end(ORDERS_TABLE_ID)
}

/// Executes `encode orders row value` for this component.
pub fn encode_orders_row_value(row: &OrdersSeedRow) -> Vec<u8> {
    let mut payloads = vec![
        Some(encode_i64(row.order_id)),
        Some(encode_i64(row.customer_id)),
        row.status.as_ref().map(|status| encode_utf8(status)),
        Some(encode_i64(row.total_cents)),
        Some(encode_i64(row.created_at_ns)),
    ];

    let column_count = payloads.len() as u16;
    let null_bitmap_len = payloads.len().div_ceil(8);
    let mut null_bitmap = vec![0u8; null_bitmap_len];
    for (idx, payload) in payloads.iter().enumerate() {
        // Decision: evaluate `payload.is_none()` to choose the correct SQL/storage control path.
        if payload.is_none() {
            let byte_idx = idx / 8;
            let bit_idx = idx % 8;
            null_bitmap[byte_idx] |= 1u8 << bit_idx;
        }
    }

    let mut out = Vec::new();
    out.push(ROW_FORMAT_VERSION_V1);
    out.push(0);
    out.extend_from_slice(&column_count.to_be_bytes());
    out.extend_from_slice(&(null_bitmap_len as u16).to_be_bytes());
    out.extend_from_slice(&null_bitmap);

    for payload in payloads.drain(..) {
        // Decision: evaluate `let Some(payload) = payload` to choose the correct SQL/storage control path.
        if let Some(payload) = payload {
            out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            out.extend_from_slice(&payload);
        }
    }

    out
}

/// Executes `encode orders tombstone value` for this component.
pub fn encode_orders_tombstone_value() -> Vec<u8> {
    vec![ROW_FORMAT_VERSION_V1, ROW_FLAG_TOMBSTONE]
}

/// Executes `encode generic tombstone value` for this component.
fn encode_generic_tombstone_value() -> Vec<u8> {
    vec![ROW_FORMAT_VERSION_V2, ROW_FLAG_TOMBSTONE]
}

#[derive(Debug, Clone)]
/// Represents the `HoloStoreTableProvider` component used by the holo_fusion runtime.
pub struct HoloStoreTableProvider {
    schema: SchemaRef,
    columns: Vec<TableColumnRecord>,
    check_constraints: Vec<TableCheckConstraintRecord>,
    secondary_indexes: Vec<SecondaryIndexRecord>,
    table_name: String,
    table_id: u64,
    table_model: String,
    row_codec_mode: RowCodecMode,
    primary_key_column: String,
    primary_key_index: usize,
    primary_key_distribution: PrimaryKeyDistribution,
    primary_key_hash_buckets: Option<usize>,
    preferred_shards: Vec<usize>,
    page_size: usize,
    max_scan_rows: usize,
    distributed_write_max_batch_entries: usize,
    distributed_write_max_batch_bytes: usize,
    write_retry_policy: RetryPolicy,
    scan_retry_limit: usize,
    scan_retry_delay: Duration,
    scan_parallelism: usize,
    inflight_budget: InflightBudgetConfig,
    circuit_breaker_config: CircuitBreakerConfig,
    circuit_by_target: Arc<std::sync::Mutex<BTreeMap<SocketAddr, TargetCircuitState>>>,
    bulk_ingest_enabled: bool,
    optimizer: OptimizerEngine,
    optimizer_stats: Arc<std::sync::Mutex<OptimizerStatsCache>>,
    local_grpc_addr: SocketAddr,
    client: HoloStoreClient,
    metrics: Arc<PushdownMetrics>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Row codec discriminator selected from table metadata model.
enum RowCodecMode {
    OrdersV1,
    RowV1,
}

#[derive(Debug, Clone)]
struct OptimizerStatsCache {
    stats: Option<TableStats>,
    last_collected_at: Option<Instant>,
    approx_row_count: u64,
    stats_sample_cursor: Option<i64>,
}

impl Default for OptimizerStatsCache {
    fn default() -> Self {
        Self {
            stats: None,
            last_collected_at: None,
            approx_row_count: DEFAULT_OPTIMIZER_BOOTSTRAP_ROW_COUNT,
            stats_sample_cursor: None,
        }
    }
}

/// Builds a DataFusion schema from persisted metadata columns.
fn schema_from_columns(columns: &[TableColumnRecord]) -> Result<SchemaRef> {
    if columns.is_empty() {
        return Err(anyhow!("table metadata has no columns"));
    }

    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        if column.name.trim().is_empty() {
            return Err(anyhow!("table metadata has empty column name"));
        }
        fields.push(Field::new(
            column.name.as_str(),
            arrow_data_type_for_column(column.column_type),
            column.nullable,
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Maps persisted metadata column type to Arrow logical type.
fn arrow_data_type_for_column(column_type: TableColumnType) -> DataType {
    match column_type {
        TableColumnType::Int8 => DataType::Int8,
        TableColumnType::Int16 => DataType::Int16,
        TableColumnType::Int32 => DataType::Int32,
        TableColumnType::Int64 => DataType::Int64,
        TableColumnType::UInt8 => DataType::UInt8,
        TableColumnType::UInt16 => DataType::UInt16,
        TableColumnType::UInt32 => DataType::UInt32,
        TableColumnType::UInt64 => DataType::UInt64,
        TableColumnType::Float64 => DataType::Float64,
        TableColumnType::Boolean => DataType::Boolean,
        TableColumnType::Utf8 => DataType::Utf8,
        TableColumnType::TimestampNanosecond => DataType::Timestamp(TimeUnit::Nanosecond, None),
    }
}

/// Resolves primary key index from column list and persisted pk column name.
fn primary_key_index_for_columns(
    columns: &[TableColumnRecord],
    primary_key_column: &str,
) -> Result<usize> {
    columns
        .iter()
        .position(|column| column.name == primary_key_column)
        .ok_or_else(|| {
            anyhow!(
                "primary key column '{}' not found in table columns",
                primary_key_column
            )
        })
}

/// Derives provider layout from persisted table metadata.
fn provider_layout_from_metadata(
    table: &TableMetadataRecord,
) -> Result<(
    SchemaRef,
    Vec<TableColumnRecord>,
    RowCodecMode,
    String,
    usize,
)> {
    if table.table_model == ORDERS_TABLE_MODEL {
        let columns = orders_columns();
        return Ok((
            orders_schema(),
            columns,
            RowCodecMode::OrdersV1,
            "order_id".to_string(),
            0,
        ));
    }
    if table.table_model == table_model_row_v1() {
        let columns = table.columns.clone();
        let primary_key_column = table.primary_key_column.clone().ok_or_else(|| {
            anyhow!(
                "row_v1 table '{}' missing primary key metadata",
                table.table_name
            )
        })?;
        let primary_key_index =
            primary_key_index_for_columns(columns.as_slice(), primary_key_column.as_str())?;
        let schema = schema_from_columns(columns.as_slice())?;
        return Ok((
            schema,
            columns,
            RowCodecMode::RowV1,
            primary_key_column,
            primary_key_index,
        ));
    }
    Err(anyhow!(
        "unsupported table model '{}' for table '{}'",
        table.table_model,
        table.table_name
    ))
}

/// Derives provider layout from serialized Ballista provider codec spec.
fn provider_layout_from_codec(
    spec: &HoloProviderCodecSpec,
) -> Result<(
    SchemaRef,
    Vec<TableColumnRecord>,
    RowCodecMode,
    String,
    usize,
)> {
    if spec.table_model == ORDERS_TABLE_MODEL {
        let columns = orders_columns();
        return Ok((
            orders_schema(),
            columns,
            RowCodecMode::OrdersV1,
            "order_id".to_string(),
            0,
        ));
    }
    if spec.table_model == table_model_row_v1() {
        let columns = spec.columns.clone();
        if columns.is_empty() {
            return Err(anyhow!(
                "row_v1 provider codec for table '{}' is missing columns",
                spec.table_name
            ));
        }
        let primary_key_column = spec.primary_key_column.clone().ok_or_else(|| {
            anyhow!(
                "row_v1 provider codec for table '{}' missing primary key column",
                spec.table_name
            )
        })?;
        let primary_key_index =
            primary_key_index_for_columns(columns.as_slice(), primary_key_column.as_str())?;
        let schema = schema_from_columns(columns.as_slice())?;
        return Ok((
            schema,
            columns,
            RowCodecMode::RowV1,
            primary_key_column,
            primary_key_index,
        ));
    }
    Err(anyhow!(
        "unsupported table model '{}' in provider codec",
        spec.table_model
    ))
}

impl HoloStoreTableProvider {
    /// Executes `orders` for this component.
    pub fn orders(client: HoloStoreClient, metrics: Arc<PushdownMetrics>) -> Self {
        let local_grpc_addr = client.target();
        let columns = orders_columns();
        Self {
            schema: orders_schema(),
            columns,
            check_constraints: Vec::new(),
            secondary_indexes: Vec::new(),
            table_name: ORDERS_TABLE_NAME.to_string(),
            table_id: ORDERS_TABLE_ID,
            table_model: ORDERS_TABLE_MODEL.to_string(),
            row_codec_mode: RowCodecMode::OrdersV1,
            primary_key_column: "order_id".to_string(),
            primary_key_index: 0,
            primary_key_distribution: PrimaryKeyDistribution::Range,
            primary_key_hash_buckets: None,
            preferred_shards: vec![ORDERS_SHARD_INDEX],
            page_size: 2048,
            max_scan_rows: configured_max_scan_rows(),
            distributed_write_max_batch_entries: configured_write_max_batch_entries(),
            distributed_write_max_batch_bytes: configured_write_max_batch_bytes(),
            write_retry_policy: RetryPolicy::from_env(),
            scan_retry_limit: configured_scan_retry_limit().max(1),
            scan_retry_delay: Duration::from_millis(configured_scan_retry_delay_ms()),
            scan_parallelism: configured_scan_parallelism().max(1),
            inflight_budget: InflightBudgetConfig::from_env(),
            circuit_breaker_config: CircuitBreakerConfig::from_env(),
            circuit_by_target: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            bulk_ingest_enabled: configured_bulk_ingest_enabled(),
            optimizer: OptimizerEngine::default(),
            optimizer_stats: Arc::new(std::sync::Mutex::new(OptimizerStatsCache::default())),
            local_grpc_addr,
            client,
            metrics,
        }
    }

    /// Executes `from table metadata` for this component.
    pub fn from_table_metadata(
        table: &TableMetadataRecord,
        client: HoloStoreClient,
        metrics: Arc<PushdownMetrics>,
    ) -> Result<Self> {
        Self::from_table_metadata_with_indexes(table, Vec::new(), client, metrics)
    }

    /// Builds provider from table metadata plus persisted secondary index metadata.
    pub fn from_table_metadata_with_indexes(
        table: &TableMetadataRecord,
        secondary_indexes: Vec<SecondaryIndexRecord>,
        client: HoloStoreClient,
        metrics: Arc<PushdownMetrics>,
    ) -> Result<Self> {
        table.validate()?;
        let (schema, columns, row_codec_mode, primary_key_column, primary_key_index) =
            provider_layout_from_metadata(table)?;

        let local_grpc_addr = client.target();
        Ok(Self {
            schema,
            columns,
            check_constraints: table.check_constraints.clone(),
            secondary_indexes,
            table_name: table.table_name.clone(),
            table_id: table.table_id,
            table_model: table.table_model.clone(),
            row_codec_mode,
            primary_key_column,
            primary_key_index,
            primary_key_distribution: table.primary_key_distribution,
            primary_key_hash_buckets: table.primary_key_hash_buckets,
            preferred_shards: table.preferred_shards.clone(),
            page_size: table.page_size.max(1),
            max_scan_rows: configured_max_scan_rows(),
            distributed_write_max_batch_entries: configured_write_max_batch_entries(),
            distributed_write_max_batch_bytes: configured_write_max_batch_bytes(),
            write_retry_policy: RetryPolicy::from_env(),
            scan_retry_limit: configured_scan_retry_limit().max(1),
            scan_retry_delay: Duration::from_millis(configured_scan_retry_delay_ms()),
            scan_parallelism: configured_scan_parallelism().max(1),
            inflight_budget: InflightBudgetConfig::from_env(),
            circuit_breaker_config: CircuitBreakerConfig::from_env(),
            circuit_by_target: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            bulk_ingest_enabled: configured_bulk_ingest_enabled(),
            optimizer: OptimizerEngine::default(),
            optimizer_stats: Arc::new(std::sync::Mutex::new(OptimizerStatsCache::default())),
            local_grpc_addr,
            client,
            metrics,
        })
    }

    /// Executes `to codec spec` for this component.
    pub fn to_codec_spec(&self) -> HoloProviderCodecSpec {
        HoloProviderCodecSpec {
            table_name: self.table_name.clone(),
            table_id: self.table_id,
            table_model: self.table_model.clone(),
            columns: self.columns.clone(),
            check_constraints: self.check_constraints.clone(),
            secondary_indexes: self.secondary_indexes.clone(),
            primary_key_column: Some(self.primary_key_column.clone()),
            primary_key_distribution: self.primary_key_distribution,
            primary_key_hash_buckets: self.primary_key_hash_buckets,
            preferred_shards: self.preferred_shards.clone(),
            page_size: self.page_size,
            max_scan_rows: self.max_scan_rows,
            local_grpc_addr: self.local_grpc_addr.to_string(),
        }
    }

    /// Executes `from codec spec` for this component.
    pub fn from_codec_spec(
        spec: HoloProviderCodecSpec,
        metrics: Arc<PushdownMetrics>,
    ) -> Result<Self> {
        let (schema, columns, row_codec_mode, primary_key_column, primary_key_index) =
            provider_layout_from_codec(&spec)?;

        let local_grpc_addr = spec
            .local_grpc_addr
            .parse::<SocketAddr>()
            .with_context(|| {
                format!(
                    "invalid grpc addr in provider codec: {}",
                    spec.local_grpc_addr
                )
            })?;
        let client = HoloStoreClient::new(local_grpc_addr);
        Ok(Self {
            schema,
            columns,
            check_constraints: spec.check_constraints,
            secondary_indexes: spec.secondary_indexes,
            table_name: spec.table_name,
            table_id: spec.table_id,
            table_model: spec.table_model,
            row_codec_mode,
            primary_key_column,
            primary_key_index,
            primary_key_distribution: spec.primary_key_distribution,
            primary_key_hash_buckets: spec.primary_key_hash_buckets,
            preferred_shards: spec.preferred_shards,
            page_size: spec.page_size.max(1),
            max_scan_rows: spec.max_scan_rows,
            distributed_write_max_batch_entries: configured_write_max_batch_entries(),
            distributed_write_max_batch_bytes: configured_write_max_batch_bytes(),
            write_retry_policy: RetryPolicy::from_env(),
            scan_retry_limit: configured_scan_retry_limit().max(1),
            scan_retry_delay: Duration::from_millis(configured_scan_retry_delay_ms()),
            scan_parallelism: configured_scan_parallelism().max(1),
            inflight_budget: InflightBudgetConfig::from_env(),
            circuit_breaker_config: CircuitBreakerConfig::from_env(),
            circuit_by_target: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
            bulk_ingest_enabled: configured_bulk_ingest_enabled(),
            optimizer: OptimizerEngine::default(),
            optimizer_stats: Arc::new(std::sync::Mutex::new(OptimizerStatsCache::default())),
            local_grpc_addr,
            client,
            metrics,
        })
    }

    /// Executes `table name` for this component.
    pub fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    /// Returns stable table identifier from metadata.
    pub fn table_id(&self) -> u64 {
        self.table_id
    }

    /// Executes `table model` for this component.
    pub fn table_model(&self) -> &str {
        self.table_model.as_str()
    }

    /// Executes `primary key column` for this component.
    pub fn primary_key_column(&self) -> &str {
        self.primary_key_column.as_str()
    }

    /// Executes `column definitions` for this component.
    pub fn columns(&self) -> &[TableColumnRecord] {
        self.columns.as_slice()
    }

    /// Returns persisted CHECK constraints for this table metadata.
    pub fn check_constraints(&self) -> &[TableCheckConstraintRecord] {
        self.check_constraints.as_slice()
    }

    /// Returns persisted secondary index metadata associated with this table.
    pub fn secondary_indexes(&self) -> &[SecondaryIndexRecord] {
        self.secondary_indexes.as_slice()
    }

    /// Returns physical primary-key distribution mode.
    pub fn primary_key_distribution(&self) -> PrimaryKeyDistribution {
        self.primary_key_distribution
    }

    /// Returns optional hash bucket count for hash-distributed primary keys.
    pub fn primary_key_hash_buckets(&self) -> Option<usize> {
        self.primary_key_hash_buckets
    }

    /// Returns preferred shard affinity from metadata.
    pub fn preferred_shards(&self) -> &[usize] {
        self.preferred_shards.as_slice()
    }

    /// Returns configured scan page size.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Executes `is orders v1 table` for this component.
    pub fn is_orders_v1_table(&self) -> bool {
        self.row_codec_mode == RowCodecMode::OrdersV1
    }

    /// Executes `is row v1 table` for this component.
    pub fn is_row_v1_table(&self) -> bool {
        self.row_codec_mode == RowCodecMode::RowV1
    }

    /// Returns configured hash bucket count when this table uses hash-PK routing.
    fn hash_bucket_count(&self) -> Option<usize> {
        if self.primary_key_distribution != PrimaryKeyDistribution::Hash {
            return None;
        }
        Some(
            self.primary_key_hash_buckets
                .unwrap_or(DEFAULT_HASH_PK_BUCKETS)
                .max(1)
                .min(u16::MAX as usize),
        )
    }

    /// Encodes one table primary key according to persisted PK distribution mode.
    fn encode_storage_primary_key(&self, primary_key: i64) -> Vec<u8> {
        encode_primary_key_with_distribution(
            self.table_id,
            primary_key,
            self.primary_key_distribution,
            self.hash_bucket_count(),
        )
    }

    /// Builds one or more scan ranges for PK bounds under current PK distribution mode.
    fn scan_ranges_for_bounds(
        &self,
        bounds: PkBounds,
        table_id: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if bounds.is_empty() {
            return Ok(Vec::new());
        }
        if self.primary_key_distribution != PrimaryKeyDistribution::Hash {
            return Ok(vec![bounds.as_scan_range(table_id)?]);
        }

        let bucket_count = self.hash_bucket_count().unwrap_or(DEFAULT_HASH_PK_BUCKETS);
        let mut ranges = Vec::with_capacity(bucket_count);
        for bucket in 0..bucket_count {
            let bucket_u16 = u16::try_from(bucket)
                .map_err(|_| anyhow!("hash bucket {} out of u16 range", bucket))?;
            let mut start = table_hash_bucket_prefix(table_id, bucket_u16);
            let mut end = prefix_end(start.as_slice()).unwrap_or_default();

            if let Some((lower, inclusive)) = bounds.lower {
                let lower_key = encode_primary_key_hash(table_id, bucket_u16, lower);
                start = if inclusive {
                    lower_key
                } else {
                    prefix_end(&lower_key).unwrap_or(lower_key)
                };
            }
            if let Some((upper, inclusive)) = bounds.upper {
                let upper_key = encode_primary_key_hash(table_id, bucket_u16, upper);
                end = if inclusive {
                    prefix_end(&upper_key).unwrap_or_default()
                } else {
                    upper_key
                };
            }
            if !end.is_empty() && start >= end {
                continue;
            }
            ranges.push((start, end));
        }
        Ok(ranges)
    }

    fn public_secondary_indexes(&self) -> Vec<&SecondaryIndexRecord> {
        self.secondary_indexes
            .iter()
            .filter(|index| matches!(index.state, SecondaryIndexState::Public))
            .collect::<Vec<_>>()
    }

    fn filter_supports_secondary_index_pushdown(&self, filter: &Expr) -> bool {
        if self.row_codec_mode != RowCodecMode::RowV1 {
            return false;
        }
        let indexes = self.public_secondary_indexes();
        if indexes.is_empty() {
            return false;
        }

        let summary = extract_predicate_summary(std::slice::from_ref(filter));
        summary_supports_secondary_index_pushdown(&summary, indexes.as_slice())
    }

    fn required_columns_for_projection(&self, projection: Option<&Vec<usize>>) -> BTreeSet<String> {
        match projection {
            Some(projected) => projected
                .iter()
                .filter_map(|idx| self.columns.get(*idx))
                .map(|column| column.name.to_ascii_lowercase())
                .collect::<BTreeSet<_>>(),
            None => self
                .columns
                .iter()
                .map(|column| column.name.to_ascii_lowercase())
                .collect::<BTreeSet<_>>(),
        }
    }

    fn required_column_indexes(&self, required_columns: &BTreeSet<String>) -> BTreeSet<usize> {
        let mut out = BTreeSet::<usize>::new();
        out.insert(self.primary_key_index);
        for (idx, column) in self.columns.iter().enumerate() {
            if required_columns.contains(&column.name.to_ascii_lowercase()) {
                out.insert(idx);
            }
        }
        out
    }

    fn optimizer_bootstrap_stats(&self, approx_row_count: u64) -> TableStats {
        let public_indexes = self
            .public_secondary_indexes()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        build_table_stats_from_sample(
            self.columns.as_slice(),
            public_indexes.as_slice(),
            &[],
            approx_row_count.max(1),
            now_unix_millis(),
        )
    }

    async fn optimizer_collect_sample_rows(
        &self,
        sample_target: usize,
        cursor: Option<i64>,
    ) -> Result<Vec<VersionedGenericRow>> {
        if sample_target == 0 {
            return Ok(Vec::new());
        }

        let mut rows = Vec::<VersionedGenericRow>::new();
        let mut seen_primary = BTreeSet::<i64>::new();

        if let Some(last_primary_key) = cursor {
            let mut tail_bounds = PkBounds::default();
            tail_bounds.apply(Operator::Gt, last_primary_key);
            for row in self
                .scan_rows_with_bounds_with_versions_generic(tail_bounds, Some(sample_target), 0)
                .await?
            {
                if seen_primary.insert(row.primary_key) {
                    rows.push(row);
                    if rows.len() >= sample_target {
                        return Ok(rows);
                    }
                }
            }
        }

        let remaining = sample_target.saturating_sub(rows.len());
        if remaining > 0 {
            for row in self
                .scan_rows_with_bounds_with_versions_generic(
                    PkBounds::default(),
                    Some(remaining),
                    0,
                )
                .await?
            {
                if seen_primary.insert(row.primary_key) {
                    rows.push(row);
                    if rows.len() >= sample_target {
                        break;
                    }
                }
            }
        }

        Ok(rows)
    }

    async fn optimizer_table_stats(&self) -> Result<TableStats> {
        let now = Instant::now();
        let (cached_stats, cached_at, approx_row_count, stats_sample_cursor) = {
            let cache = self
                .optimizer_stats
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (
                cache.stats.clone(),
                cache.last_collected_at,
                cache.approx_row_count.max(1),
                cache.stats_sample_cursor,
            )
        };

        let refresh_interval = self.optimizer.config().stats_refresh_interval;
        if let (Some(stats), Some(collected_at)) = (cached_stats.clone(), cached_at) {
            if now.duration_since(collected_at) <= refresh_interval {
                return Ok(stats);
            }
        }

        if self.row_codec_mode != RowCodecMode::RowV1 {
            let stats =
                cached_stats.unwrap_or_else(|| self.optimizer_bootstrap_stats(approx_row_count));
            return Ok(stats);
        }

        let sample_target = self.optimizer.config().stats_sample_rows.max(1);
        let sampled_rows = self
            .optimizer_collect_sample_rows(sample_target, stats_sample_cursor)
            .await
            .unwrap_or_default();
        let next_sample_cursor = sampled_rows.last().map(|row| row.primary_key);
        let sample_rows = sampled_rows
            .into_iter()
            .map(|row| row.values)
            .collect::<Vec<Vec<ScalarValue>>>();
        let observed = sample_rows.len() as u64;

        let public_indexes = self
            .public_secondary_indexes()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let stats = build_table_stats_from_sample(
            self.columns.as_slice(),
            public_indexes.as_slice(),
            sample_rows.as_slice(),
            approx_row_count.max(observed),
            now_unix_millis(),
        );

        let mut cache = self
            .optimizer_stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.approx_row_count = cache.approx_row_count.max(observed).max(1);
        cache.stats_sample_cursor = next_sample_cursor;
        cache.last_collected_at = Some(now);
        cache.stats = Some(stats.clone());
        Ok(stats)
    }

    fn optimizer_record_row_count_delta(&self, delta: i64) {
        let mut cache = self
            .optimizer_stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if delta >= 0 {
            cache.approx_row_count = cache.approx_row_count.saturating_add(delta as u64).max(1);
        } else {
            cache.approx_row_count = cache
                .approx_row_count
                .saturating_sub((-delta) as u64)
                .max(1);
        }
    }

    fn optimizer_observe_row_count_lower_bound(&self, observed: usize) {
        if observed == 0 {
            return;
        }
        let mut cache = self
            .optimizer_stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache.approx_row_count = cache.approx_row_count.max(observed as u64).max(1);
    }

    /// Emits structured optimizer decision traces for EXPLAIN/diagnostics.
    fn emit_optimizer_decision_trace_events(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        plan: &PlannedAccessPath,
    ) {
        if plan.considered.is_empty() {
            return;
        }

        let mut ranked = plan.considered.clone();
        ranked.sort_by(|left, right| {
            left.total_cost
                .partial_cmp(&right.total_cost)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let winner_name = optimizer_candidate_name_for_access_path(&plan.path);
        let winner = ranked
            .iter()
            .find(|candidate| candidate.name == winner_name)
            .or_else(|| ranked.first());
        let runner_up = winner.and_then(|winner| {
            ranked
                .iter()
                .find(|candidate| candidate.name != winner.name)
        });

        if let Some(winner) = winner {
            let reason = if plan.fallback_decision.is_some() {
                "uncertainty_fallback_applied"
            } else if runner_up.is_some() {
                "lowest_total_cost"
            } else {
                "single_candidate"
            };
            let (runner_up_name, runner_up_cost, cost_delta, cost_ratio) =
                if let Some(runner_up) = runner_up {
                    let delta = runner_up.total_cost - winner.total_cost;
                    let ratio = if winner.total_cost.abs() <= f64::EPSILON {
                        0.0
                    } else {
                        runner_up.total_cost / winner.total_cost
                    };
                    (runner_up.name.as_str(), runner_up.total_cost, delta, ratio)
                } else {
                    ("none", 0.0, 0.0, 0.0)
                };
            self.metrics.record_stage_event(
                query_execution_id,
                stage_id,
                "optimizer_plan_decision",
                format!(
                    "reason={} winner={} winner_cost={:.3} runner_up={} runner_up_cost={:.3} cost_delta={:.3} cost_ratio={:.3} candidate_count={}",
                    reason,
                    winner.name,
                    winner.total_cost,
                    runner_up_name,
                    runner_up_cost,
                    cost_delta,
                    cost_ratio,
                    ranked.len(),
                ),
            );
        }

        // Keep trace compact while preserving enough ranked context for diagnosis.
        for (rank, candidate) in ranked.iter().take(8).enumerate() {
            self.metrics.record_stage_event(
                query_execution_id,
                stage_id,
                "optimizer_candidate_eval",
                format!(
                    "rank={} name={} total_cost={:.3} estimated_output_rows={:.2} estimated_scan_rows={:.2} uncertainty={:.3} feedback_multiplier={:.3}",
                    rank + 1,
                    candidate.name,
                    candidate.total_cost,
                    candidate.estimated_output_rows,
                    candidate.estimated_scan_rows,
                    candidate.uncertainty,
                    candidate.feedback_multiplier,
                ),
            );
        }
    }

    /// Reads MIN/MAX(primary_key) using one directed page per scan target.
    pub async fn scan_primary_key_extreme(
        &self,
        extreme: PrimaryKeyExtreme,
    ) -> Result<Option<i64>> {
        let scan_ranges = self.scan_ranges_for_bounds(PkBounds::default(), self.table_id)?;
        if scan_ranges.is_empty() {
            return Ok(None);
        }

        let mut best: Option<i64> = None;
        for (range_start, range_end) in scan_ranges {
            let targets = self
                .scan_targets_for_range(range_start.as_slice(), range_end.as_slice())
                .await;
            for target in targets {
                let Some(candidate) = self
                    .scan_target_primary_key_extreme(&target, extreme)
                    .await?
                else {
                    continue;
                };
                best = Some(match best {
                    Some(current) => extreme.pick(current, candidate),
                    None => candidate,
                });
            }
        }

        Ok(best)
    }

    /// Reads one target-local MIN/MAX(primary_key) candidate using directed pagination.
    async fn scan_target_primary_key_extreme(
        &self,
        target: &LocalScanTarget,
        extreme: PrimaryKeyExtreme,
    ) -> Result<Option<i64>> {
        let mut cursor = Vec::new();
        let page_limit = self.page_size.min(64).max(1);
        let scan_client = self.client_for(target.grpc_addr);

        loop {
            let (entries, next_cursor, done) = scan_client
                .range_snapshot_latest(
                    target.shard_index,
                    target.start_key.as_slice(),
                    target.end_key.as_slice(),
                    cursor.as_slice(),
                    page_limit,
                    extreme.reverse_scan(),
                )
                .await
                .with_context(|| {
                    format!(
                        "scan primary-key {:?} candidate shard={} target={}",
                        extreme, target.shard_index, target.grpc_addr
                    )
                })?;

            for entry in entries {
                if row_payload_is_sql_tombstone(entry.value.as_slice()) {
                    continue;
                }
                let primary_key = decode_primary_key(entry.key.as_slice(), self.table_id)?;
                return Ok(Some(primary_key));
            }

            if done || next_cursor.is_empty() {
                return Ok(None);
            }
            cursor = next_cursor;
        }
    }

    /// Executes `scan orders by order id bounds` for this component.
    pub async fn scan_orders_by_order_id_bounds(
        &self,
        lower: Option<(i64, bool)>,
        upper: Option<(i64, bool)>,
        limit: Option<usize>,
    ) -> Result<Vec<OrdersSeedRow>> {
        if self.row_codec_mode != RowCodecMode::OrdersV1 {
            return Err(anyhow!(
                "orders mutation helpers are only available for orders_v1 table model"
            ));
        }
        let rows = self
            .scan_orders_with_versions_by_order_id_bounds(lower, upper, limit)
            .await?;
        Ok(rows.into_iter().map(|entry| entry.row).collect())
    }

    /// Executes `scan orders with versions by order id bounds` for this component.
    pub async fn scan_orders_with_versions_by_order_id_bounds(
        &self,
        lower: Option<(i64, bool)>,
        upper: Option<(i64, bool)>,
        limit: Option<usize>,
    ) -> Result<Vec<VersionedOrdersRow>> {
        if self.row_codec_mode != RowCodecMode::OrdersV1 {
            return Err(anyhow!(
                "orders mutation helpers are only available for orders_v1 table model"
            ));
        }
        let mut bounds = PkBounds::default();
        // Decision: evaluate `let Some((value, inclusive)) = lower` to choose the correct SQL/storage control path.
        if let Some((value, inclusive)) = lower {
            bounds.apply(
                // Decision: evaluate `inclusive` to choose the correct SQL/storage control path.
                if inclusive {
                    Operator::GtEq
                } else {
                    Operator::Gt
                },
                value,
            );
        }
        // Decision: evaluate `let Some((value, inclusive)) = upper` to choose the correct SQL/storage control path.
        if let Some((value, inclusive)) = upper {
            bounds.apply(
                // Decision: evaluate `inclusive` to choose the correct SQL/storage control path.
                if inclusive {
                    Operator::LtEq
                } else {
                    Operator::Lt
                },
                value,
            );
        }
        self.scan_rows_with_bounds_with_versions(bounds, limit, 1)
            .await
    }

    /// Scans row_v1 rows by primary-key bounds and returns values with versions.
    pub async fn scan_generic_rows_with_versions_by_primary_key_bounds(
        &self,
        lower: Option<(i64, bool)>,
        upper: Option<(i64, bool)>,
        limit: Option<usize>,
    ) -> Result<Vec<VersionedGenericRow>> {
        if self.row_codec_mode != RowCodecMode::RowV1 {
            return Err(anyhow!(
                "generic row scan is only available for row_v1 table model"
            ));
        }
        let mut bounds = PkBounds::default();
        if let Some((value, inclusive)) = lower {
            bounds.apply(
                if inclusive {
                    Operator::GtEq
                } else {
                    Operator::Gt
                },
                value,
            );
        }
        if let Some((value, inclusive)) = upper {
            bounds.apply(
                if inclusive {
                    Operator::LtEq
                } else {
                    Operator::Lt
                },
                value,
            );
        }
        self.scan_rows_with_bounds_with_versions_generic(bounds, limit, 1)
            .await
    }

    /// Executes grouped `COUNT(*)/SUM(column)` aggregation with top-k ordering using provider pushdown.
    pub async fn execute_grouped_aggregate_topk(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        spec: &GroupedAggregateTopKSpec,
    ) -> Result<Vec<GroupedAggregateTopKRow>> {
        if self.row_codec_mode != RowCodecMode::RowV1 {
            return Err(anyhow!(
                "grouped aggregate pushdown is only available for row_v1 tables"
            ));
        }
        if spec.limit == 0 {
            return Ok(Vec::new());
        }
        if spec.group_columns.is_empty() {
            return Err(anyhow!(
                "grouped aggregate pushdown requires at least one GROUP BY column"
            ));
        }

        let mut by_name = BTreeMap::<String, usize>::new();
        for (idx, column) in self.columns.iter().enumerate() {
            by_name.insert(column.name.to_ascii_lowercase(), idx);
        }

        let mut group_indexes = Vec::<usize>::with_capacity(spec.group_columns.len());
        let mut required_columns = BTreeSet::<String>::new();
        for column in &spec.group_columns {
            let normalized = column.to_ascii_lowercase();
            let Some(idx) = by_name.get(normalized.as_str()).copied() else {
                return Err(anyhow!(
                    "grouped aggregate pushdown column '{}' does not exist",
                    column
                ));
            };
            group_indexes.push(idx);
            required_columns.insert(normalized);
        }

        let sum_column_normalized = spec.sum_column.to_ascii_lowercase();
        let Some(sum_column_index) = by_name.get(sum_column_normalized.as_str()).copied() else {
            return Err(anyhow!(
                "grouped aggregate pushdown SUM column '{}' does not exist",
                spec.sum_column
            ));
        };
        required_columns.insert(sum_column_normalized.clone());

        let mut runtime_filters = Vec::<AggregateFilterRuntime>::new();
        let mut pushed_filters = Vec::<Expr>::new();
        for filter in &spec.filters {
            if filter.allowed_values.is_empty() {
                return Ok(Vec::new());
            }
            let normalized = filter.column.to_ascii_lowercase();
            let Some(column_index) = by_name.get(normalized.as_str()).copied() else {
                return Err(anyhow!(
                    "grouped aggregate pushdown filter column '{}' does not exist",
                    filter.column
                ));
            };
            required_columns.insert(normalized.clone());
            runtime_filters.push(AggregateFilterRuntime {
                column_index,
                allowed_values: filter.allowed_values.clone(),
            });

            let column_expr = col(normalized.as_str());
            let filter_expr = if filter.allowed_values.len() == 1 {
                column_expr.eq(Expr::Literal(filter.allowed_values[0].clone(), None))
            } else {
                let values = filter
                    .allowed_values
                    .iter()
                    .cloned()
                    .map(|value| Expr::Literal(value, None))
                    .collect::<Vec<_>>();
                column_expr.in_list(values, false)
            };
            pushed_filters.push(filter_expr);
        }

        let required_column_indexes = self.required_column_indexes(&required_columns);
        let predicates = extract_predicate_summary(pushed_filters.as_slice());
        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "optimizer_predicate_summary",
            format_optimizer_predicate_summary(&predicates),
        );
        let stats = match self.optimizer_table_stats().await {
            Ok(stats) => stats,
            Err(err) => {
                self.metrics.record_stage_event(
                    query_execution_id,
                    stage_id,
                    "aggregate_pushdown_optimizer_stats_fallback",
                    format!("error={err}"),
                );
                self.optimizer_bootstrap_stats(DEFAULT_OPTIMIZER_BOOTSTRAP_ROW_COUNT)
            }
        };

        let query_shape = QueryShape {
            required_columns: required_columns.clone(),
            limit: None,
            preferred_shard_count: self.preferred_shards.len().max(1),
        };
        let public_indexes = self
            .public_secondary_indexes()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let plan = self.optimizer.choose_access_path(
            self.columns.as_slice(),
            public_indexes.as_slice(),
            self.primary_key_column.as_str(),
            &stats,
            &predicates,
            &query_shape,
        );
        self.metrics.record_optimizer_plan_choice(match &plan.path {
            AccessPath::TableScan => "table_scan",
            AccessPath::Index {
                index_only_table_covering: true,
                ..
            } => "index_only",
            AccessPath::Index { .. } => "index_scan",
        });
        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "aggregate_pushdown_plan_choice",
            format!(
                "path={} estimated_output_rows={:.2} estimated_scan_rows={:.2} uncertainty={:.3} cost={:.3}",
                format_access_path(&plan.path),
                plan.estimated_output_rows,
                plan.estimated_scan_rows,
                plan.uncertainty,
                plan.total_cost
            ),
        );
        if let Some(fallback) = plan.fallback_decision.as_ref() {
            self.metrics.record_stage_event(
                query_execution_id,
                stage_id,
                "optimizer_uncertainty_fallback",
                format!(
                    "reason={} source={} source_cost={:.3} fallback={} fallback_cost={:.3} uncertainty={:.3} threshold={:.3} cost_penalty_ratio={:.3}",
                    fallback.reason,
                    fallback.source_candidate,
                    fallback.source_cost,
                    fallback.fallback_candidate,
                    fallback.fallback_cost,
                    fallback.uncertainty,
                    fallback.uncertainty_threshold,
                    fallback.fallback_cost_penalty_ratio,
                ),
            );
        }
        self.emit_optimizer_decision_trace_events(query_execution_id, stage_id, &plan);

        let mut global_groups = BTreeMap::<Vec<u8>, AggregateGroupState>::new();
        let scan_stats = match &plan.path {
            AccessPath::TableScan => {
                self.aggregate_table_scan_streaming_ctx(
                    query_execution_id,
                    stage_id,
                    &group_indexes,
                    sum_column_index,
                    runtime_filters.as_slice(),
                    &required_column_indexes,
                    &mut global_groups,
                )
                .await?
            }
            AccessPath::Index { .. } => {
                match self
                    .aggregate_index_scan_streaming_ctx(
                        query_execution_id,
                        stage_id,
                        &plan,
                        &group_indexes,
                        sum_column_index,
                        runtime_filters.as_slice(),
                        &required_columns,
                        &mut global_groups,
                    )
                    .await
                {
                    Ok(stats) => stats,
                    Err(err) => {
                        self.metrics.record_stage_event(
                            query_execution_id,
                            stage_id,
                            "aggregate_pushdown_index_fallback",
                            format!("error={err}"),
                        );
                        self.aggregate_table_scan_streaming_ctx(
                            query_execution_id,
                            stage_id,
                            &group_indexes,
                            sum_column_index,
                            runtime_filters.as_slice(),
                            &required_column_indexes,
                            &mut global_groups,
                        )
                        .await?
                    }
                }
            }
        };
        self.optimizer_observe_row_count_lower_bound(scan_stats.rows_scanned as usize);

        let rows = finalize_grouped_topk_rows(global_groups, spec.having_min_count, spec.limit)?;

        self.metrics.record_scan(
            scan_stats.rpc_pages,
            scan_stats.rows_scanned,
            scan_stats.rows_output,
            scan_stats.bytes_scanned,
        );
        if scan_stats.duplicate_rows_skipped > 0 {
            self.metrics
                .record_scan_duplicate_rows_skipped(scan_stats.duplicate_rows_skipped);
        }
        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "aggregate_pushdown_complete",
            format!(
                "groups_emitted={} having_min_count={} limit={}",
                rows.len(),
                spec.having_min_count,
                spec.limit
            ),
        );
        Ok(rows)
    }

    fn writable_secondary_indexes(&self) -> Vec<&SecondaryIndexRecord> {
        self.secondary_indexes
            .iter()
            .filter(|index| {
                matches!(
                    index.state,
                    SecondaryIndexState::WriteOnly | SecondaryIndexState::Public
                )
            })
            .collect::<Vec<_>>()
    }

    fn encode_secondary_index_mutation(
        &self,
        index: &SecondaryIndexRecord,
        row_values: &[ScalarValue],
        primary_key: i64,
    ) -> Result<EncodedSecondaryIndexMutation> {
        let row_key = encode_secondary_index_row_key(
            index,
            self.columns.as_slice(),
            row_values,
            primary_key,
        )?;
        let row_value =
            encode_secondary_index_row_value(index, self.columns.as_slice(), row_values)?;
        let unique_key =
            encode_secondary_index_unique_key(index, self.columns.as_slice(), row_values)?;
        let unique_value = unique_key
            .as_ref()
            .map(|_| encode_secondary_index_unique_value(primary_key));
        Ok(EncodedSecondaryIndexMutation {
            row_key,
            row_value,
            unique_key,
            unique_value,
        })
    }

    fn decode_row_payload_for_indexing(
        &self,
        primary_key: i64,
        payload: &[u8],
    ) -> Result<Option<Vec<ScalarValue>>> {
        if self.row_codec_mode != RowCodecMode::RowV1 {
            return Ok(None);
        }
        decode_generic_row_value(
            payload,
            self.columns.as_slice(),
            self.primary_key_index,
            primary_key,
        )
    }

    async fn read_exact_latest_entry(&self, key: &[u8]) -> Result<Option<LatestEntry>> {
        let topology = fetch_topology(&self.client).await?;
        require_non_empty_topology(&topology)?;

        let mut end = key.to_vec();
        end.push(0x00);
        let targets = scan_targets(&topology, self.local_grpc_addr, key, end.as_slice(), &[]);

        let mut latest: Option<LatestEntry> = None;
        for target in targets {
            let scan_client = self.client_for(target.grpc_addr);
            let (entries, _, _) = scan_client
                .range_snapshot_latest(
                    target.shard_index,
                    target.start_key.as_slice(),
                    target.end_key.as_slice(),
                    &[],
                    16,
                    false,
                )
                .await?;
            for entry in entries {
                if entry.key != key {
                    continue;
                }
                match latest.as_ref() {
                    Some(current) if current.version >= entry.version => {}
                    _ => latest = Some(entry),
                }
            }
        }

        Ok(latest)
    }

    async fn read_exact_latest_entry_cached(
        &self,
        key: &[u8],
        cache: &mut BTreeMap<Vec<u8>, Option<LatestEntry>>,
    ) -> Result<Option<LatestEntry>> {
        if let Some(cached) = cache.get(key) {
            return Ok(cached.clone());
        }
        let latest = self.read_exact_latest_entry(key).await?;
        cache.insert(key.to_vec(), latest.clone());
        Ok(latest)
    }

    async fn augment_primary_writes_with_secondary_indexes(
        &self,
        writes: &[ConditionalPrimaryWrite],
    ) -> Result<Vec<ConditionalStorageWrite>> {
        let mut out = writes
            .iter()
            .map(|write| ConditionalStorageWrite {
                key: self.encode_storage_primary_key(write.primary_key),
                expected_version: write.expected_version,
                value: write.value.clone(),
                rollback_value: write.rollback_value.clone(),
            })
            .collect::<Vec<_>>();

        if self.row_codec_mode != RowCodecMode::RowV1 {
            return Ok(out);
        }
        let writable_indexes = self.writable_secondary_indexes();
        if writable_indexes.is_empty() {
            return Ok(out);
        }

        let index_tombstone = index_tombstone_value();
        let mut latest_cache = BTreeMap::<Vec<u8>, Option<LatestEntry>>::new();

        for write in writes {
            let old_values = self.decode_row_payload_for_indexing(
                write.primary_key,
                write.rollback_value.as_slice(),
            )?;
            let new_values =
                self.decode_row_payload_for_indexing(write.primary_key, write.value.as_slice())?;

            for index in &writable_indexes {
                let old_encoded = old_values
                    .as_ref()
                    .map(|row| {
                        self.encode_secondary_index_mutation(
                            index,
                            row.as_slice(),
                            write.primary_key,
                        )
                    })
                    .transpose()?;
                let new_encoded = new_values
                    .as_ref()
                    .map(|row| {
                        self.encode_secondary_index_mutation(
                            index,
                            row.as_slice(),
                            write.primary_key,
                        )
                    })
                    .transpose()?;

                let row_key_changed = old_encoded.as_ref().map(|v| &v.row_key)
                    != new_encoded.as_ref().map(|v| &v.row_key);
                let row_value_changed = old_encoded.as_ref().map(|v| &v.row_value)
                    != new_encoded.as_ref().map(|v| &v.row_value);
                let unique_key_changed = old_encoded.as_ref().and_then(|v| v.unique_key.as_ref())
                    != new_encoded.as_ref().and_then(|v| v.unique_key.as_ref());

                // Remove stale row-index key when row disappears or key changes.
                if let Some(old_entry) = old_encoded.as_ref() {
                    if new_encoded.is_none() || row_key_changed {
                        if let Some(existing) = self
                            .read_exact_latest_entry_cached(
                                old_entry.row_key.as_slice(),
                                &mut latest_cache,
                            )
                            .await?
                        {
                            if !is_sql_tombstone_value(existing.value.as_slice()) {
                                out.push(ConditionalStorageWrite {
                                    key: old_entry.row_key.clone(),
                                    expected_version: existing.version,
                                    value: index_tombstone.clone(),
                                    rollback_value: existing.value,
                                });
                            }
                        }
                    } else if row_value_changed {
                        if let Some(existing) = self
                            .read_exact_latest_entry_cached(
                                old_entry.row_key.as_slice(),
                                &mut latest_cache,
                            )
                            .await?
                        {
                            if !is_sql_tombstone_value(existing.value.as_slice()) {
                                if let Some(new_entry) = new_encoded.as_ref() {
                                    out.push(ConditionalStorageWrite {
                                        key: old_entry.row_key.clone(),
                                        expected_version: existing.version,
                                        value: new_entry.row_value.clone(),
                                        rollback_value: existing.value,
                                    });
                                }
                            }
                        }
                    }
                }

                // Insert fresh row-index key when row appears or key changes.
                if let Some(new_entry) = new_encoded.as_ref() {
                    if old_encoded.is_none() || row_key_changed {
                        out.push(ConditionalStorageWrite {
                            key: new_entry.row_key.clone(),
                            expected_version: RpcVersion::zero(),
                            value: new_entry.row_value.clone(),
                            rollback_value: index_tombstone.clone(),
                        });
                    }
                }

                // Remove stale unique reservation when key changed or row deleted.
                if let Some(old_unique_key) =
                    old_encoded.as_ref().and_then(|v| v.unique_key.as_ref())
                {
                    if new_encoded.is_none() || unique_key_changed {
                        if let Some(existing) = self
                            .read_exact_latest_entry_cached(
                                old_unique_key.as_slice(),
                                &mut latest_cache,
                            )
                            .await?
                        {
                            if !is_sql_tombstone_value(existing.value.as_slice()) {
                                out.push(ConditionalStorageWrite {
                                    key: old_unique_key.clone(),
                                    expected_version: existing.version,
                                    value: index_tombstone.clone(),
                                    rollback_value: existing.value,
                                });
                            }
                        }
                    }
                }

                // Insert unique reservation for the new key (if present).
                if let Some(new_unique_key) =
                    new_encoded.as_ref().and_then(|v| v.unique_key.as_ref())
                {
                    if old_encoded.is_none() || unique_key_changed {
                        if let Some(existing) = self
                            .read_exact_latest_entry_cached(
                                new_unique_key.as_slice(),
                                &mut latest_cache,
                            )
                            .await?
                        {
                            if !is_sql_tombstone_value(existing.value.as_slice()) {
                                return Err(DuplicateKeyViolation::for_constraint(
                                    index.index_name.clone(),
                                )
                                .into());
                            }
                        }
                        out.push(ConditionalStorageWrite {
                            key: new_unique_key.clone(),
                            expected_version: RpcVersion::zero(),
                            value: new_encoded
                                .as_ref()
                                .and_then(|v| v.unique_value.clone())
                                .unwrap_or_else(|| {
                                    encode_secondary_index_unique_value(write.primary_key)
                                }),
                            rollback_value: index_tombstone.clone(),
                        });
                    }
                }
            }
        }

        Ok(out)
    }

    /// Backfills one secondary index from currently visible table rows.
    pub async fn backfill_secondary_index(&self, index: &SecondaryIndexRecord) -> Result<u64> {
        if self.row_codec_mode != RowCodecMode::RowV1 {
            return Err(anyhow!(
                "secondary indexes are currently supported only for row_v1 tables"
            ));
        }
        if index.table_id != self.table_id {
            return Err(anyhow!(
                "secondary index '{}' targets table_id={}, provider table_id={}",
                index.index_name,
                index.table_id,
                self.table_id
            ));
        }

        // Keep index backfill batches smaller than generic scan pages to avoid
        // long per-RPC conditional apply stalls under hot-shard pressure.
        let page_size = self
            .page_size
            .min(configured_index_backfill_page_size())
            .max(1);
        let mut lower = None::<(i64, bool)>;
        let mut backfilled_rows = 0u64;
        let tombstone = index_tombstone_value();

        loop {
            let rows = self
                .scan_generic_rows_with_versions_by_primary_key_bounds(lower, None, Some(page_size))
                .await?;
            if rows.is_empty() {
                break;
            }

            let mut writes = Vec::<ConditionalStorageWrite>::new();
            for row in &rows {
                let encoded = self.encode_secondary_index_mutation(
                    index,
                    row.values.as_slice(),
                    row.primary_key,
                )?;
                writes.push(ConditionalStorageWrite {
                    key: encoded.row_key,
                    expected_version: RpcVersion::zero(),
                    value: encoded.row_value,
                    rollback_value: tombstone.clone(),
                });
                if let Some(unique_key) = encoded.unique_key {
                    writes.push(ConditionalStorageWrite {
                        key: unique_key,
                        expected_version: RpcVersion::zero(),
                        value: encoded.unique_value.unwrap_or_else(|| {
                            encode_secondary_index_unique_value(row.primary_key)
                        }),
                        rollback_value: tombstone.clone(),
                    });
                }
            }

            match self
                .apply_storage_writes_conditional_detailed(writes.as_slice(), "index_backfill")
                .await?
            {
                ConditionalWriteDetailedOutcome::Applied(_) => {}
                ConditionalWriteDetailedOutcome::Conflict => {
                    return Err(
                        DuplicateKeyViolation::for_constraint(index.index_name.clone()).into(),
                    );
                }
            }

            backfilled_rows = backfilled_rows.saturating_add(rows.len() as u64);
            let last_pk = rows.last().map(|row| row.primary_key).unwrap_or(i64::MAX);
            lower = Some((last_pk, false));
        }

        Ok(backfilled_rows)
    }

    /// Executes `upsert orders rows` for this component.
    pub async fn upsert_orders_rows(&self, rows: &[OrdersSeedRow]) -> Result<u64> {
        if self.row_codec_mode != RowCodecMode::OrdersV1 {
            return Err(anyhow!(
                "orders mutation helpers are only available for orders_v1 table model"
            ));
        }
        let entries = rows
            .iter()
            .map(|row| (row.order_id, encode_orders_row_value(row)))
            .collect::<Vec<_>>();
        self.write_primary_entries(entries, "upsert").await
    }

    /// Inserts orders rows and fails on duplicate primary keys.
    pub async fn insert_orders_rows(&self, rows: &[OrdersSeedRow]) -> Result<u64> {
        if self.row_codec_mode != RowCodecMode::OrdersV1 {
            return Err(anyhow!(
                "orders mutation helpers are only available for orders_v1 table model"
            ));
        }
        let entries = rows
            .iter()
            .map(|row| (row.order_id, encode_orders_row_value(row)))
            .collect::<Vec<_>>();
        self.insert_primary_entries(entries).await
    }

    /// Upserts generic row_v1 rows encoded from typed ScalarValue vectors.
    pub async fn upsert_generic_rows(&self, rows: &[Vec<ScalarValue>]) -> Result<u64> {
        if self.row_codec_mode != RowCodecMode::RowV1 {
            return Err(anyhow!(
                "generic upsert helpers are only available for row_v1 table model"
            ));
        }
        if rows.is_empty() {
            return Ok(0);
        }

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != self.columns.len() {
                return Err(anyhow!(
                    "row_v1 upsert expects {} columns, got {}",
                    self.columns.len(),
                    row.len()
                ));
            }
            validate_row_against_metadata(
                self.columns.as_slice(),
                self.check_constraints.as_slice(),
                row.as_slice(),
            )
            .map_err(row_constraint_to_anyhow)?;
            let primary_key = scalar_to_primary_key_i64(
                row[self.primary_key_index].clone(),
                &self.columns[self.primary_key_index],
            )?;
            let payload = self.encode_row_payload(row.as_slice())?;
            entries.push((primary_key, payload));
        }

        self.write_primary_entries(entries, "upsert").await
    }

    /// Inserts generic row_v1 rows and fails on duplicate primary keys.
    pub async fn insert_generic_rows(&self, rows: &[Vec<ScalarValue>]) -> Result<u64> {
        if self.row_codec_mode != RowCodecMode::RowV1 {
            return Err(anyhow!(
                "generic insert helpers are only available for row_v1 table model"
            ));
        }
        if rows.is_empty() {
            return Ok(0);
        }

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != self.columns.len() {
                return Err(anyhow!(
                    "row_v1 insert expects {} columns, got {}",
                    self.columns.len(),
                    row.len()
                ));
            }
            validate_row_against_metadata(
                self.columns.as_slice(),
                self.check_constraints.as_slice(),
                row.as_slice(),
            )
            .map_err(row_constraint_to_anyhow)?;
            let primary_key = scalar_to_primary_key_i64(
                row[self.primary_key_index].clone(),
                &self.columns[self.primary_key_index],
            )?;
            let payload = self.encode_row_payload(row.as_slice())?;
            entries.push((primary_key, payload));
        }

        self.insert_primary_entries(entries).await
    }

    /// Executes `tombstone orders by order id` for this component.
    pub async fn tombstone_orders_by_order_id(&self, order_ids: &[i64]) -> Result<u64> {
        if self.row_codec_mode != RowCodecMode::OrdersV1 {
            return Err(anyhow!(
                "orders mutation helpers are only available for orders_v1 table model"
            ));
        }
        let tombstone = encode_orders_tombstone_value();
        let entries = order_ids
            .iter()
            .map(|order_id| (*order_id, tombstone.clone()))
            .collect::<Vec<_>>();
        self.write_primary_entries(entries, "delete").await
    }

    /// Tombstones row_v1 rows by primary key.
    pub async fn tombstone_generic_rows_by_primary_key(&self, primary_keys: &[i64]) -> Result<u64> {
        if self.row_codec_mode != RowCodecMode::RowV1 {
            return Err(anyhow!(
                "generic tombstone helpers are only available for row_v1 table model"
            ));
        }
        if primary_keys.is_empty() {
            return Ok(0);
        }
        let tombstone = encode_generic_tombstone_value();
        let entries = primary_keys
            .iter()
            .map(|primary_key| (*primary_key, tombstone.clone()))
            .collect::<Vec<_>>();
        self.write_primary_entries(entries, "delete").await
    }

    /// Executes `apply orders writes conditional` for this component.
    pub async fn apply_orders_writes_conditional(
        &self,
        writes: &[ConditionalOrderWrite],
        op: &'static str,
    ) -> Result<ConditionalWriteOutcome> {
        if self.row_codec_mode != RowCodecMode::OrdersV1 {
            return Err(anyhow!(
                "orders mutation helpers are only available for orders_v1 table model"
            ));
        }
        // Decision: evaluate `writes.is_empty()` to choose the correct SQL/storage control path.
        if writes.is_empty() {
            return Ok(ConditionalWriteOutcome::Applied(0));
        }

        let generic_writes = writes
            .iter()
            .map(|write| ConditionalPrimaryWrite {
                primary_key: write.order_id,
                expected_version: write.expected_version,
                value: write.value.clone(),
                rollback_value: write.rollback_value.clone(),
            })
            .collect::<Vec<_>>();
        self.apply_primary_writes_conditional(generic_writes.as_slice(), op)
            .await
    }

    /// Applies conditional writes for row_v1 table rows keyed by primary key.
    pub async fn apply_generic_writes_conditional(
        &self,
        writes: &[ConditionalPrimaryWrite],
        op: &'static str,
    ) -> Result<ConditionalWriteOutcome> {
        if self.row_codec_mode != RowCodecMode::RowV1 {
            return Err(anyhow!(
                "generic conditional writes are only available for row_v1 table model"
            ));
        }
        self.apply_primary_writes_conditional(writes, op).await
    }

    /// Encodes one row payload according to this table's row codec.
    pub fn encode_row_payload(&self, values: &[ScalarValue]) -> Result<Vec<u8>> {
        if self.row_codec_mode != RowCodecMode::RowV1 {
            return Err(anyhow!(
                "generic row payload encoding is only available for row_v1 table model"
            ));
        }
        if values.len() != self.columns.len() {
            return Err(anyhow!(
                "row payload column count mismatch for table '{}': expected {}, got {}",
                self.table_name,
                self.columns.len(),
                values.len()
            ));
        }
        let mut payloads = Vec::with_capacity(values.len());
        for (column, value) in self.columns.iter().zip(values.iter()) {
            payloads.push(encode_scalar_payload(value.clone(), column)?);
        }
        Ok(encode_generic_row_value(payloads))
    }

    /// Encodes the tombstone marker payload for this table model.
    pub fn encode_tombstone_payload(&self) -> Vec<u8> {
        match self.row_codec_mode {
            RowCodecMode::OrdersV1 => encode_orders_tombstone_value(),
            RowCodecMode::RowV1 => encode_generic_tombstone_value(),
        }
    }

    /// Applies conditional writes keyed by normalized primary key.
    async fn apply_primary_writes_conditional(
        &self,
        writes: &[ConditionalPrimaryWrite],
        op: &'static str,
    ) -> Result<ConditionalWriteOutcome> {
        match self
            .apply_primary_writes_conditional_detailed(writes, op)
            .await?
        {
            ConditionalWriteDetailedOutcome::Applied(applied) => {
                Ok(ConditionalWriteOutcome::Applied(applied.applied_rows))
            }
            ConditionalWriteDetailedOutcome::Conflict => Ok(ConditionalWriteOutcome::Conflict),
        }
    }

    /// Applies primary-key writes and any required secondary-index writes atomically.
    async fn apply_primary_writes_conditional_detailed(
        &self,
        writes: &[ConditionalPrimaryWrite],
        op: &'static str,
    ) -> Result<ConditionalWriteDetailedOutcome> {
        if writes.is_empty() {
            return Ok(ConditionalWriteDetailedOutcome::Applied(
                ConditionalWriteAppliedBatch {
                    applied_rows: 0,
                    applied_targets: Vec::new(),
                },
            ));
        }
        let storage_writes = self
            .augment_primary_writes_with_secondary_indexes(writes)
            .await?;
        match self
            .apply_storage_writes_conditional_detailed(storage_writes.as_slice(), op)
            .await?
        {
            ConditionalWriteDetailedOutcome::Applied(mut applied) => {
                applied.applied_rows = writes.len() as u64;
                Ok(ConditionalWriteDetailedOutcome::Applied(applied))
            }
            ConditionalWriteDetailedOutcome::Conflict => {
                Ok(ConditionalWriteDetailedOutcome::Conflict)
            }
        }
    }

    /// Applies generic storage-key conditional writes and returns rollback metadata.
    async fn apply_storage_writes_conditional_detailed(
        &self,
        writes: &[ConditionalStorageWrite],
        op: &'static str,
    ) -> Result<ConditionalWriteDetailedOutcome> {
        if writes.is_empty() {
            return Ok(ConditionalWriteDetailedOutcome::Applied(
                ConditionalWriteAppliedBatch {
                    applied_rows: 0,
                    applied_targets: Vec::new(),
                },
            ));
        }
        let retry_policy = self.write_retry_policy;
        let retry_limit = retry_policy.max_attempts();
        let retry_deadline = if retry_policy.enabled && retry_policy.max_elapsed > Duration::ZERO {
            Some(Instant::now() + retry_policy.max_elapsed)
        } else {
            None
        };
        let mut last_retryable_error: Option<anyhow::Error> = None;

        'attempts: for attempt in 0..retry_limit {
            if let Some(deadline) = retry_deadline {
                if Instant::now() >= deadline {
                    break 'attempts;
                }
            }
            let topology = match fetch_topology(&self.client)
                .await
                .with_context(|| format!("fetch topology for {op}"))
            {
                Ok(topology) => topology,
                Err(err) => {
                    let class = classify_write_error_message(err.to_string().as_str());
                    if class.is_retryable() && attempt + 1 < retry_limit {
                        last_retryable_error = Some(err);
                        tokio::time::sleep(retry_backoff(
                            retry_policy,
                            attempt,
                            now_timestamp_nanos().unsigned_abs(),
                        ))
                        .await;
                        continue 'attempts;
                    }
                    return Err(err);
                }
            };
            if let Err(err) = require_non_empty_topology(&topology) {
                let class = classify_write_error_message(err.to_string().as_str());
                if class.is_retryable() && attempt + 1 < retry_limit {
                    last_retryable_error = Some(err);
                    tokio::time::sleep(retry_backoff(
                        retry_policy,
                        attempt,
                        now_timestamp_nanos().unsigned_abs(),
                    ))
                    .await;
                    continue 'attempts;
                }
                return Err(err);
            }

            let mut by_target = BTreeMap::<WriteTarget, Vec<PreparedConditionalEntry>>::new();
            for write in writes {
                let key = write.key.clone();
                let route = match route_key(
                    &topology,
                    self.local_grpc_addr,
                    key.as_slice(),
                    &self.preferred_shards,
                ) {
                    Some(route) => route,
                    None => {
                        let err = anyhow!(
                            "no shard route found for key table={} key={}",
                            self.table_name,
                            hex::encode(&write.key)
                        );
                        let class = classify_write_error_message(err.to_string().as_str());
                        if class.is_retryable() && attempt + 1 < retry_limit {
                            last_retryable_error = Some(err);
                            tokio::time::sleep(retry_backoff(
                                retry_policy,
                                attempt,
                                now_timestamp_nanos().unsigned_abs(),
                            ))
                            .await;
                            continue 'attempts;
                        }
                        return Err(err);
                    }
                };

                let target = WriteTarget {
                    shard_index: route.shard_index,
                    grpc_addr: route.grpc_addr,
                    start_key: route.start_key,
                    end_key: route.end_key,
                };
                by_target
                    .entry(target)
                    .or_default()
                    .push(PreparedConditionalEntry {
                        key,
                        value: write.value.clone(),
                        expected_version: write.expected_version,
                        applied_version: RpcVersion::zero(),
                        rollback_value: write.rollback_value.clone(),
                    });
            }
            let planned_rows = writes.len();
            let mut planned_bytes = 0usize;
            let mut planned_rpcs = 0usize;
            for entries in by_target.values() {
                planned_bytes = planned_bytes.saturating_add(
                    entries
                        .iter()
                        .map(prepared_conditional_entry_size)
                        .fold(0usize, |acc, next| acc.saturating_add(next)),
                );
                planned_rpcs = planned_rpcs.saturating_add(
                    conditional_chunk_ranges(
                        entries.as_slice(),
                        self.distributed_write_max_batch_entries,
                        self.distributed_write_max_batch_bytes,
                    )
                    .len(),
                );
            }
            self.enforce_inflight_budget(planned_rows, planned_bytes, planned_rpcs, "write_plan")?;

            let mut applied_targets = Vec::<(WriteTarget, Vec<PreparedConditionalEntry>)>::new();
            for (target, mut entries) in by_target {
                let expected = entries.len() as u64;
                let write_client = self.client_for(target.grpc_addr);
                let batch_ranges = conditional_chunk_ranges(
                    entries.as_slice(),
                    self.distributed_write_max_batch_entries,
                    self.distributed_write_max_batch_bytes,
                );
                let batch_count = batch_ranges.len();
                let mut applied_this_target = Vec::<PreparedConditionalEntry>::new();
                let mut applied_total = 0u64;

                for (batch_idx, batch_range) in batch_ranges.into_iter().enumerate() {
                    let start = batch_range.start;
                    let end = batch_range.end;
                    let batch_bytes = entries[start..end]
                        .iter()
                        .map(prepared_conditional_entry_size)
                        .fold(0usize, |acc, next| acc.saturating_add(next));
                    let request_entries = entries[start..end]
                        .iter()
                        .map(|entry| ReplicatedConditionalWriteEntry {
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                            expected_version: entry.expected_version,
                        })
                        .collect::<Vec<_>>();
                    let batch_expected = request_entries.len() as u64;
                    self.enforce_inflight_budget(
                        batch_expected as usize,
                        batch_bytes,
                        1,
                        "conditional_apply_batch",
                    )?;
                    let circuit_token = match self.circuit_acquire(target.grpc_addr) {
                        Ok(token) => token,
                        Err(err) => {
                            let apply_error = anyhow!(
                                "apply conditional {op} batch table={} shard={} target={} chunk={}/{} failed: {err}",
                                self.table_name,
                                target.shard_index,
                                target.grpc_addr,
                                batch_idx + 1,
                                batch_count
                            );
                            let classification =
                                classify_write_error_message(apply_error.to_string().as_str());
                            self.metrics.record_distributed_apply_failure(
                                target.grpc_addr.to_string().as_str(),
                                classification.is_retryable(),
                            );
                            if !applied_this_target.is_empty() {
                                let rollback_scope =
                                    vec![(target.clone(), applied_this_target.clone())];
                                self.rollback_conditional_targets(rollback_scope.as_slice(), op)
                                    .await
                                    .with_context(|| {
                                        format!(
                                            "rollback conditional {op} writes for shard {} after circuit rejection",
                                            target.shard_index
                                        )
                                    })?;
                            }
                            self.rollback_conditional_targets(applied_targets.as_slice(), op)
                                .await
                                .with_context(|| {
                                    format!(
                                        "rollback previously applied conditional {op} writes after circuit rejection"
                                    )
                                })?;

                            if classification.is_retryable() && attempt + 1 < retry_limit {
                                if let Some(deadline) = retry_deadline {
                                    if Instant::now() >= deadline {
                                        return Err(apply_error);
                                    }
                                }
                                last_retryable_error = Some(apply_error);
                                tokio::time::sleep(retry_backoff(
                                    retry_policy,
                                    attempt,
                                    target.grpc_addr.port() as u64,
                                ))
                                .await;
                                continue 'attempts;
                            }
                            return Err(apply_error);
                        }
                    };
                    let apply_span = info_span!(
                        "holo_fusion.distributed_write_apply",
                        table = %self.table_name,
                        op = op,
                        shard_index = target.shard_index,
                        target = %target.grpc_addr,
                        batch_index = batch_idx + 1,
                        batch_count = batch_count
                    );
                    let apply_started = Instant::now();
                    let result = match write_client
                        .range_write_latest_conditional(
                            target.shard_index,
                            target.start_key.as_slice(),
                            target.end_key.as_slice(),
                            request_entries,
                        )
                        .instrument(apply_span)
                        .await
                    {
                        Ok(result) => {
                            self.circuit_on_success(target.grpc_addr, circuit_token);
                            result
                        }
                        Err(err) => {
                            self.circuit_on_failure(target.grpc_addr, circuit_token);
                            let apply_error = anyhow!(
                                "apply conditional {op} batch table={} shard={} target={} chunk={}/{} failed: {err}",
                                self.table_name,
                                target.shard_index,
                                target.grpc_addr,
                                batch_idx + 1,
                                batch_count
                            );
                            let classification =
                                classify_write_error_message(apply_error.to_string().as_str());
                            self.metrics.record_distributed_apply_failure(
                                target.grpc_addr.to_string().as_str(),
                                classification.is_retryable(),
                            );
                            if !applied_this_target.is_empty() {
                                let rollback_scope =
                                    vec![(target.clone(), applied_this_target.clone())];
                                self.rollback_conditional_targets(rollback_scope.as_slice(), op)
                                    .await
                                    .with_context(|| {
                                        format!(
                                            "rollback conditional {op} writes for shard {} after apply failure",
                                            target.shard_index
                                        )
                                    })?;
                            }
                            self.rollback_conditional_targets(applied_targets.as_slice(), op)
                                .await
                                .with_context(|| {
                                    format!(
                                        "rollback previously applied conditional {op} writes after apply failure"
                                    )
                                })?;

                            if classification.is_retryable() && attempt + 1 < retry_limit {
                                if let Some(deadline) = retry_deadline {
                                    if Instant::now() >= deadline {
                                        return Err(apply_error);
                                    }
                                }
                                last_retryable_error = Some(apply_error);
                                tokio::time::sleep(retry_backoff(
                                    retry_policy,
                                    attempt,
                                    target.grpc_addr.port() as u64,
                                ))
                                .await;
                                continue 'attempts;
                            }
                            return Err(apply_error);
                        }
                    };
                    self.metrics.record_distributed_apply(
                        target.shard_index,
                        batch_expected,
                        apply_started.elapsed(),
                    );

                    // Decision: evaluate `result.conflicts > 0` to choose the correct SQL/storage control path.
                    if result.conflicts > 0 {
                        self.metrics.record_distributed_conflict(target.shard_index);
                        if !applied_this_target.is_empty() {
                            let rollback_scope =
                                vec![(target.clone(), applied_this_target.clone())];
                            self.rollback_conditional_targets(rollback_scope.as_slice(), op)
                                .await?;
                        }
                        self.rollback_conditional_targets(applied_targets.as_slice(), op)
                            .await?;
                        return Ok(ConditionalWriteDetailedOutcome::Conflict);
                    }

                    // Decision: evaluate `result.applied != batch_expected` to choose the correct SQL/storage control path.
                    if result.applied != batch_expected {
                        if !applied_this_target.is_empty() {
                            let rollback_scope =
                                vec![(target.clone(), applied_this_target.clone())];
                            self.rollback_conditional_targets(rollback_scope.as_slice(), op)
                                .await?;
                        }
                        self.rollback_conditional_targets(applied_targets.as_slice(), op)
                            .await?;
                        return Err(anyhow!(
                            "partial conditional {op} apply on shard {} chunk {}/{}: expected {}, applied {}",
                            target.shard_index,
                            batch_idx + 1,
                            batch_count,
                            batch_expected,
                            result.applied
                        ));
                    }

                    let mut versions_by_key = BTreeMap::<Vec<u8>, RpcVersion>::new();
                    for item in result.applied_versions {
                        versions_by_key.insert(item.key, item.version);
                    }
                    // Decision: evaluate `versions_by_key.len() != batch_expected as usize` to choose the correct SQL/storage control path.
                    if versions_by_key.len() != batch_expected as usize {
                        if !applied_this_target.is_empty() {
                            let rollback_scope =
                                vec![(target.clone(), applied_this_target.clone())];
                            self.rollback_conditional_targets(rollback_scope.as_slice(), op)
                                .await?;
                        }
                        self.rollback_conditional_targets(applied_targets.as_slice(), op)
                            .await?;
                        return Err(anyhow!(
                            "conditional {op} response missing applied versions on shard {} chunk {}/{}: expected {}, got {}",
                            target.shard_index,
                            batch_idx + 1,
                            batch_count,
                            batch_expected,
                            versions_by_key.len()
                        ));
                    }

                    for entry in &mut entries[start..end] {
                        let applied_version = versions_by_key
                            .get(entry.key.as_slice())
                            .copied()
                            .ok_or_else(|| {
                                anyhow!(
                                    "conditional {op} response missing applied version for key={} on shard {} chunk {}/{}",
                                    hex::encode(&entry.key),
                                    target.shard_index,
                                    batch_idx + 1,
                                    batch_count
                                )
                            })?;
                        entry.applied_version = applied_version;
                    }
                    applied_this_target.extend(entries[start..end].iter().cloned());
                    applied_total = applied_total.saturating_add(result.applied);
                }

                // Decision: evaluate `applied_total != expected` to choose the correct SQL/storage control path.
                if applied_total != expected {
                    if !applied_this_target.is_empty() {
                        let rollback_scope = vec![(target.clone(), applied_this_target)];
                        self.rollback_conditional_targets(rollback_scope.as_slice(), op)
                            .await?;
                    }
                    self.rollback_conditional_targets(applied_targets.as_slice(), op)
                        .await?;
                    return Err(anyhow!(
                        "conditional {op} apply row count mismatch on shard {}: expected {}, applied {}",
                        target.shard_index,
                        expected,
                        applied_total
                    ));
                }

                applied_targets.push((target, entries));
            }

            return Ok(ConditionalWriteDetailedOutcome::Applied(
                ConditionalWriteAppliedBatch {
                    applied_rows: writes.len() as u64,
                    applied_targets,
                },
            ));
        }

        Err(last_retryable_error
            .unwrap_or_else(|| anyhow!("conditional {op} write retries exhausted")))
    }

    /// Scans orders rows with explicit query/stage context.
    async fn scan_rows_with_context(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        pushed_filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Vec<OrdersSeedRow>> {
        let mut bounds = PkBounds::default();
        let mut predicate_terms = Vec::new();
        let mut unsupported_filters = 0usize;
        for filter in pushed_filters {
            // Decision: evaluate `let Some(predicates) = extract_supported_predicates(filter)` to choose the correct SQL/storage control path.
            if let Some(predicates) =
                extract_supported_predicates(filter, self.primary_key_column.as_str())
            {
                for predicate in predicates {
                    bounds.apply(predicate.op, predicate.value);
                    if let Some(op) = ScanPredicateOp::from_operator(predicate.op) {
                        predicate_terms.push(ScanPredicateExpr::Comparison {
                            column: self.primary_key_column.clone(),
                            op,
                            value: predicate.value,
                        });
                    }
                }
            } else {
                let supports_secondary = self.filter_supports_secondary_index_pushdown(filter);
                if !supports_secondary {
                    unsupported_filters = unsupported_filters.saturating_add(1);
                }
            }
        }
        if unsupported_filters > 0 {
            self.metrics.record_stage_event(
                query_execution_id,
                stage_id,
                "scan_pushdown_fallback",
                format!("unsupported_filters={unsupported_filters}"),
            );
        }

        let predicate = if predicate_terms.is_empty() {
            None
        } else if predicate_terms.len() == 1 {
            predicate_terms.into_iter().next()
        } else {
            Some(ScanPredicateExpr::And(predicate_terms))
        };

        let rows = self
            .scan_rows_with_bounds_with_versions_ctx(
                query_execution_id,
                stage_id,
                bounds,
                limit,
                pushed_filters.len(),
                predicate,
            )
            .await?;
        Ok(rows.into_iter().map(|entry| entry.row).collect())
    }

    /// Executes `scan rows with bounds with versions` for this component.
    async fn scan_rows_with_bounds_with_versions(
        &self,
        bounds: PkBounds,
        limit: Option<usize>,
        pushed_filter_count: usize,
    ) -> Result<Vec<VersionedOrdersRow>> {
        let query_execution_id = self
            .metrics
            .ensure_query_execution_for_session("internal_scan_orders", "INTERNAL_SCAN");
        let stage_id = self.metrics.next_stage_execution_id();
        self.scan_rows_with_bounds_with_versions_ctx(
            query_execution_id.as_str(),
            stage_id,
            bounds,
            limit,
            pushed_filter_count,
            None,
        )
        .await
    }

    /// Scans versioned orders rows with explicit query/stage context.
    async fn scan_rows_with_bounds_with_versions_ctx(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        bounds: PkBounds,
        limit: Option<usize>,
        pushed_filter_count: usize,
        predicate: Option<ScanPredicateExpr>,
    ) -> Result<Vec<VersionedOrdersRow>> {
        // Decision: evaluate `bounds.is_empty()` to choose the correct SQL/storage control path.
        if bounds.is_empty() {
            return Ok(Vec::new());
        }

        let spec = ScanSpec {
            query_execution_id: query_execution_id.to_string(),
            stage_id,
            table_id: self.table_id,
            table_name: self.table_name.clone(),
            projected_columns: (0..self.schema.fields().len()).collect(),
            predicate,
            limit,
            bounds,
        };
        let execution = self.execute_scan_spec(&spec).await?;
        let mut rows = Vec::new();
        for entry in execution.entries {
            let row = decode_orders_entry_with_version(self.table_id, &entry)?;
            if let Some(row) = row {
                if spec.bounds.matches(row.row.order_id) {
                    rows.push(row);
                    if self.max_scan_rows > 0 && rows.len() > self.max_scan_rows {
                        self.metrics.record_scan_row_limit_reject();
                        return Err(anyhow!(
                            "scan row limit exceeded for table '{}': max_scan_rows={}, rows_returned_so_far={}",
                            self.table_name,
                            self.max_scan_rows,
                            rows.len()
                        ));
                    }
                    if let Some(limit) = spec.limit {
                        if rows.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }

        self.metrics.record_scan(
            execution.stats.rpc_pages,
            execution.stats.rows_scanned,
            rows.len() as u64,
            execution.stats.bytes_scanned,
        );
        if execution.stats.duplicate_rows_skipped > 0 {
            self.metrics
                .record_scan_duplicate_rows_skipped(execution.stats.duplicate_rows_skipped);
        }

        info!(
            query_execution_id = query_execution_id,
            stage_id,
            table = %self.table_name,
            target_count = self.preferred_shards.len(),
            pushed_filter_count,
            rpc_pages = execution.stats.rpc_pages,
            rows_scanned = execution.stats.rows_scanned,
            rows_returned = rows.len(),
            bytes_scanned = execution.stats.bytes_scanned,
            retries = execution.stats.retries,
            reroutes = execution.stats.reroutes,
            chunks = execution.stats.chunks,
            "holofusion scan completed"
        );

        Ok(rows)
    }

    /// Scans generic row_v1 rows with explicit query/stage context.
    async fn scan_rows_generic_with_context(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        pushed_filters: &[Expr],
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Vec<Vec<ScalarValue>>> {
        let mut bounds = PkBounds::default();
        let mut predicate_terms = Vec::new();
        let mut unsupported_filters = 0usize;
        for filter in pushed_filters {
            if let Some(predicates) =
                extract_supported_predicates(filter, self.primary_key_column.as_str())
            {
                for predicate in predicates {
                    bounds.apply(predicate.op, predicate.value);
                    if let Some(op) = ScanPredicateOp::from_operator(predicate.op) {
                        predicate_terms.push(ScanPredicateExpr::Comparison {
                            column: self.primary_key_column.clone(),
                            op,
                            value: predicate.value,
                        });
                    }
                }
            } else {
                let supports_secondary = self.filter_supports_secondary_index_pushdown(filter);
                if !supports_secondary {
                    unsupported_filters = unsupported_filters.saturating_add(1);
                }
            }
        }
        if unsupported_filters > 0 {
            self.metrics.record_stage_event(
                query_execution_id,
                stage_id,
                "scan_pushdown_fallback",
                format!("unsupported_filters={unsupported_filters}"),
            );
        }
        let predicate = if predicate_terms.is_empty() {
            None
        } else if predicate_terms.len() == 1 {
            predicate_terms.into_iter().next()
        } else {
            Some(ScanPredicateExpr::And(predicate_terms))
        };
        let default_scan = || async {
            self.scan_rows_with_bounds_with_versions_generic_ctx(
                query_execution_id,
                stage_id,
                bounds,
                limit,
                pushed_filters.len(),
                predicate.clone(),
            )
            .await
        };

        let stats = match self.optimizer_table_stats().await {
            Ok(stats) => stats,
            Err(err) => {
                self.metrics.record_stage_event(
                    query_execution_id,
                    stage_id,
                    "optimizer_stats_fallback",
                    format!("error={err}"),
                );
                let rows = default_scan().await?;
                return Ok(rows.into_iter().map(|entry| entry.values).collect());
            }
        };

        let predicates = extract_predicate_summary(pushed_filters);
        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "optimizer_predicate_summary",
            format_optimizer_predicate_summary(&predicates),
        );
        let mut required_columns = self.required_columns_for_projection(projection);
        for (column, column_predicate) in predicates.columns() {
            if column_predicate.has_any_constraint() {
                required_columns.insert(column.to_ascii_lowercase());
            }
        }
        let query_shape = QueryShape {
            required_columns: required_columns.clone(),
            limit,
            preferred_shard_count: self.preferred_shards.len().max(1),
        };
        let public_indexes = self
            .public_secondary_indexes()
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();
        let plan = self.optimizer.choose_access_path(
            self.columns.as_slice(),
            public_indexes.as_slice(),
            self.primary_key_column.as_str(),
            &stats,
            &predicates,
            &query_shape,
        );
        self.metrics.record_optimizer_plan_choice(match &plan.path {
            AccessPath::TableScan => "table_scan",
            AccessPath::Index {
                index_only_table_covering: true,
                ..
            } => "index_only",
            AccessPath::Index { .. } => "index_scan",
        });
        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "optimizer_plan_choice",
            format!(
                "path={} estimated_output_rows={:.2} estimated_scan_rows={:.2} uncertainty={:.3} cost={:.3} model=distributed_v1",
                format_access_path(&plan.path),
                plan.estimated_output_rows,
                plan.estimated_scan_rows,
                plan.uncertainty,
                plan.total_cost
            ),
        );
        let considered_preview = plan
            .considered
            .iter()
            .take(4)
            .map(|candidate| format!("{}:{:.3}", candidate.name, candidate.total_cost))
            .collect::<Vec<_>>()
            .join(",");
        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "optimizer_plan_candidates",
            format!("candidates={considered_preview}"),
        );
        if let Some(fallback) = plan.fallback_decision.as_ref() {
            self.metrics.record_stage_event(
                query_execution_id,
                stage_id,
                "optimizer_uncertainty_fallback",
                format!(
                    "reason={} source={} source_cost={:.3} fallback={} fallback_cost={:.3} uncertainty={:.3} threshold={:.3} cost_penalty_ratio={:.3}",
                    fallback.reason,
                    fallback.source_candidate,
                    fallback.source_cost,
                    fallback.fallback_candidate,
                    fallback.fallback_cost,
                    fallback.uncertainty,
                    fallback.uncertainty_threshold,
                    fallback.fallback_cost_penalty_ratio,
                ),
            );
        }
        self.emit_optimizer_decision_trace_events(query_execution_id, stage_id, &plan);

        let rows = match &plan.path {
            AccessPath::TableScan => default_scan().await?,
            AccessPath::Index { .. } => {
                match self
                    .scan_rows_with_index_plan_ctx(
                        query_execution_id,
                        stage_id,
                        &plan,
                        limit,
                        pushed_filters.len(),
                        predicate.clone(),
                        bounds,
                        &required_columns,
                    )
                    .await
                {
                    Ok(rows) => rows,
                    Err(err) => {
                        self.metrics.record_stage_event(
                            query_execution_id,
                            stage_id,
                            "optimizer_index_fallback",
                            format!("error={err}"),
                        );
                        default_scan().await?
                    }
                }
            }
        };

        self.optimizer_observe_row_count_lower_bound(rows.len());
        let feedback = self.optimizer.observe_execution(&plan, rows.len() as u64);
        self.metrics.record_optimizer_feedback(
            plan.estimated_output_rows,
            rows.len() as u64,
            feedback.bad_miss,
        );
        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "optimizer_feedback",
            format!(
                "multiplier_before={:.3} multiplier_after={:.3} abs_error_ratio={:.3} bad_miss={}",
                feedback.multiplier_before,
                feedback.multiplier_after,
                feedback.absolute_error_ratio,
                feedback.bad_miss
            ),
        );

        Ok(rows.into_iter().map(|entry| entry.values).collect())
    }

    /// Executes `scan rows with bounds with versions generic` for this component.
    async fn scan_rows_with_bounds_with_versions_generic(
        &self,
        bounds: PkBounds,
        limit: Option<usize>,
        pushed_filter_count: usize,
    ) -> Result<Vec<VersionedGenericRow>> {
        let query_execution_id = self
            .metrics
            .ensure_query_execution_for_session("internal_scan_generic", "INTERNAL_SCAN");
        let stage_id = self.metrics.next_stage_execution_id();
        self.scan_rows_with_bounds_with_versions_generic_ctx(
            query_execution_id.as_str(),
            stage_id,
            bounds,
            limit,
            pushed_filter_count,
            None,
        )
        .await
    }

    /// Scans generic rows with explicit query/stage context.
    async fn scan_rows_with_bounds_with_versions_generic_ctx(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        bounds: PkBounds,
        limit: Option<usize>,
        pushed_filter_count: usize,
        predicate: Option<ScanPredicateExpr>,
    ) -> Result<Vec<VersionedGenericRow>> {
        if bounds.is_empty() {
            return Ok(Vec::new());
        }

        let spec = ScanSpec {
            query_execution_id: query_execution_id.to_string(),
            stage_id,
            table_id: self.table_id,
            table_name: self.table_name.clone(),
            projected_columns: (0..self.schema.fields().len()).collect(),
            predicate,
            limit,
            bounds,
        };
        let execution = self.execute_scan_spec(&spec).await?;
        let mut rows = Vec::new();
        for entry in execution.entries {
            let row = decode_generic_entry_with_version(
                self.table_id,
                self.columns.as_slice(),
                self.primary_key_index,
                &entry,
            )?;
            if let Some(row) = row {
                if spec.bounds.matches(row.primary_key) {
                    rows.push(row);
                    if self.max_scan_rows > 0 && rows.len() > self.max_scan_rows {
                        self.metrics.record_scan_row_limit_reject();
                        return Err(anyhow!(
                            "scan row limit exceeded for table '{}': max_scan_rows={}, rows_returned_so_far={}",
                            self.table_name,
                            self.max_scan_rows,
                            rows.len()
                        ));
                    }
                    if let Some(limit) = spec.limit {
                        if rows.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }

        self.metrics.record_scan(
            execution.stats.rpc_pages,
            execution.stats.rows_scanned,
            rows.len() as u64,
            execution.stats.bytes_scanned,
        );
        if execution.stats.duplicate_rows_skipped > 0 {
            self.metrics
                .record_scan_duplicate_rows_skipped(execution.stats.duplicate_rows_skipped);
        }
        self.optimizer_observe_row_count_lower_bound(execution.stats.rows_scanned as usize);
        info!(
            query_execution_id = query_execution_id,
            stage_id,
            table = %self.table_name,
            target_count = self.preferred_shards.len(),
            pushed_filter_count,
            rpc_pages = execution.stats.rpc_pages,
            rows_scanned = execution.stats.rows_scanned,
            rows_returned = rows.len(),
            bytes_scanned = execution.stats.bytes_scanned,
            retries = execution.stats.retries,
            reroutes = execution.stats.reroutes,
            chunks = execution.stats.chunks,
            "holofusion scan completed"
        );
        Ok(rows)
    }

    async fn scan_rows_with_index_plan_ctx(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        plan: &PlannedAccessPath,
        limit: Option<usize>,
        pushed_filter_count: usize,
        predicate: Option<ScanPredicateExpr>,
        pk_bounds: PkBounds,
        required_columns: &BTreeSet<String>,
    ) -> Result<Vec<VersionedGenericRow>> {
        let AccessPath::Index {
            index_name,
            prefix,
            ordered_limit_scan,
            ..
        } = &plan.path
        else {
            return Err(anyhow!("scan_rows_with_index_plan_ctx requires index plan"));
        };
        let Some(index) = self
            .public_secondary_indexes()
            .into_iter()
            .find(|candidate| candidate.index_name.eq_ignore_ascii_case(index_name))
            .cloned()
        else {
            return Err(anyhow!(
                "optimizer selected missing secondary index '{}'",
                index_name
            ));
        };

        let probe_ranges = self.build_secondary_index_probe_ranges(&index, prefix.as_slice())?;
        if probe_ranges.is_empty() {
            return Ok(Vec::new());
        }

        let index_scan = self
            .execute_scan_ranges_ctx(
                query_execution_id,
                stage_id,
                probe_ranges.as_slice(),
                if *ordered_limit_scan { None } else { limit },
                "index_scan_stage",
            )
            .await?;
        let mut rows = Vec::<VersionedGenericRow>::new();
        let mut primary_lookup_rpc_pages = 0u64;
        let mut primary_lookup_rows_scanned = 0u64;
        let mut primary_lookup_bytes_scanned = 0u64;
        let mut index_entries = index_scan.entries;
        if *ordered_limit_scan {
            self.metrics.record_stage_event(
                query_execution_id,
                stage_id,
                "index_ordered_scan",
                "enabled=true",
            );
        }
        if *ordered_limit_scan {
            index_entries.sort_by(|left, right| left.key.cmp(&right.key));
        }

        let index_only_table_covering = matches!(
            &plan.path,
            AccessPath::Index {
                index_only_table_covering: true,
                ..
            }
        );
        if index_only_table_covering {
            if !index_covers_required_columns(
                &index,
                self.primary_key_column.as_str(),
                required_columns,
            ) {
                return Err(anyhow!(
                    "optimizer selected index-only path for non-covering required columns on index '{}'",
                    index.index_name
                ));
            }
            let mut seen_primary = BTreeSet::<i64>::new();
            for entry in index_entries {
                let primary_key = decode_secondary_index_primary_key(&index, entry.key.as_slice())
                    .with_context(|| {
                        format!(
                            "decode secondary index key '{}' from index '{}'",
                            hex::encode(&entry.key),
                            index.index_name
                        )
                    })?;
                if !seen_primary.insert(primary_key) || !pk_bounds.matches(primary_key) {
                    continue;
                }
                let values = decode_secondary_index_row_values(
                    &index,
                    self.columns.as_slice(),
                    self.primary_key_column.as_str(),
                    entry.key.as_slice(),
                    entry.value.as_slice(),
                )?;
                rows.push(VersionedGenericRow {
                    primary_key,
                    values,
                    version: entry.version,
                });
                if self.max_scan_rows > 0 && rows.len() > self.max_scan_rows {
                    self.metrics.record_scan_row_limit_reject();
                    return Err(anyhow!(
                        "scan row limit exceeded for table '{}': max_scan_rows={}, rows_returned_so_far={}",
                        self.table_name,
                        self.max_scan_rows,
                        rows.len()
                    ));
                }
                if let Some(scan_limit) = limit {
                    if rows.len() >= scan_limit {
                        break;
                    }
                }
            }
        } else {
            let required_column_indexes = self.required_column_indexes(required_columns);
            let mut primary_keys = Vec::<i64>::new();
            let mut seen_primary = BTreeSet::<i64>::new();
            for entry in &index_entries {
                let primary_key = decode_secondary_index_primary_key(&index, entry.key.as_slice())
                    .with_context(|| {
                        format!(
                            "decode secondary index key '{}' from index '{}'",
                            hex::encode(&entry.key),
                            index.index_name
                        )
                    })?;
                if seen_primary.insert(primary_key) {
                    primary_keys.push(primary_key);
                }
            }

            let lookup = self
                .fetch_primary_entries_for_keys_batched_ctx(
                    query_execution_id,
                    stage_id,
                    primary_keys.as_slice(),
                )
                .await?;
            primary_lookup_rpc_pages = lookup.stats.rpc_pages;
            primary_lookup_rows_scanned = lookup.stats.rows_scanned;
            primary_lookup_bytes_scanned = lookup.stats.bytes_scanned;
            for primary_key in primary_keys {
                if !pk_bounds.matches(primary_key) {
                    continue;
                }
                let Some(entry) = lookup.entries_by_primary.get(&primary_key) else {
                    continue;
                };
                let decoded = decode_generic_entry_with_version_projected(
                    self.table_id,
                    self.columns.as_slice(),
                    self.primary_key_index,
                    &required_column_indexes,
                    entry,
                )?;
                if let Some(row) = decoded {
                    rows.push(row);
                    if self.max_scan_rows > 0 && rows.len() > self.max_scan_rows {
                        self.metrics.record_scan_row_limit_reject();
                        return Err(anyhow!(
                            "scan row limit exceeded for table '{}': max_scan_rows={}, rows_returned_so_far={}",
                            self.table_name,
                            self.max_scan_rows,
                            rows.len()
                        ));
                    }
                    if let Some(scan_limit) = limit {
                        if rows.len() >= scan_limit {
                            break;
                        }
                    }
                }
            }
        }

        self.metrics.record_scan(
            index_scan
                .stats
                .rpc_pages
                .saturating_add(primary_lookup_rpc_pages),
            index_scan
                .stats
                .rows_scanned
                .saturating_add(primary_lookup_rows_scanned),
            rows.len() as u64,
            index_scan
                .stats
                .bytes_scanned
                .saturating_add(primary_lookup_bytes_scanned),
        );
        self.optimizer_observe_row_count_lower_bound(index_scan.stats.rows_scanned as usize);
        if index_scan.stats.duplicate_rows_skipped > 0 {
            self.metrics
                .record_scan_duplicate_rows_skipped(index_scan.stats.duplicate_rows_skipped);
        }
        if let Some(predicate) = predicate.as_ref() {
            self.metrics.record_stage_event(
                query_execution_id,
                stage_id,
                "index_scan_predicate",
                format!("predicate={}", format_scan_predicate_expr(predicate)),
            );
        }
        info!(
            query_execution_id = query_execution_id,
            stage_id,
            table = %self.table_name,
            index = %index.index_name,
            pushed_filter_count,
            rpc_pages = index_scan.stats.rpc_pages,
            rows_scanned = index_scan.stats.rows_scanned,
            rows_returned = rows.len(),
            bytes_scanned = index_scan.stats.bytes_scanned,
            retries = index_scan.stats.retries,
            reroutes = index_scan.stats.reroutes,
            chunks = index_scan.stats.chunks,
            primary_lookup_rpc_pages,
            primary_lookup_rows_scanned,
            "holofusion index scan completed"
        );
        Ok(rows)
    }

    async fn fetch_primary_entries_for_keys_batched_ctx(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        primary_keys: &[i64],
    ) -> Result<PrimaryLookupBatchResult> {
        if primary_keys.is_empty() {
            return Ok(PrimaryLookupBatchResult::default());
        }

        const MAX_KEYS_PER_RANGE: usize = 256;
        const MAX_PRIMARY_KEY_GAP_PER_RANGE: i64 = 2_048;

        let mut unique_sorted_keys = primary_keys.to_vec();
        unique_sorted_keys.sort_unstable();
        unique_sorted_keys.dedup();

        let mut encoded = Vec::<(i64, Vec<u8>)>::with_capacity(unique_sorted_keys.len());
        for key in unique_sorted_keys {
            encoded.push((key, self.encode_storage_primary_key(key)));
        }
        encoded.sort_by(|left, right| left.1.cmp(&right.1));

        let mut ranges = Vec::<(Vec<u8>, Vec<u8>)>::new();
        let mut group_start = 0usize;
        while group_start < encoded.len() {
            let mut group_end = group_start + 1;
            let mut last_pk = encoded[group_start].0;
            while group_end < encoded.len() {
                if group_end - group_start >= MAX_KEYS_PER_RANGE {
                    break;
                }
                let next_pk = encoded[group_end].0;
                if next_pk.saturating_sub(last_pk) > MAX_PRIMARY_KEY_GAP_PER_RANGE {
                    break;
                }
                last_pk = next_pk;
                group_end += 1;
            }
            let start = encoded[group_start].1.clone();
            let mut end = prefix_end(encoded[group_end - 1].1.as_slice()).unwrap_or_default();
            if end.is_empty() {
                end = encoded[group_end - 1].1.clone();
                end.push(0x00);
            }
            if start < end {
                ranges.push((start, end));
            }
            group_start = group_end;
        }
        if ranges.is_empty() {
            return Ok(PrimaryLookupBatchResult::default());
        }

        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "index_primary_lookup_plan",
            format!(
                "requested_keys={} ranges={} max_keys_per_range={} max_pk_gap={}",
                encoded.len(),
                ranges.len(),
                MAX_KEYS_PER_RANGE,
                MAX_PRIMARY_KEY_GAP_PER_RANGE
            ),
        );

        let scan = self
            .execute_scan_ranges_ctx(
                query_execution_id,
                stage_id,
                ranges.as_slice(),
                None,
                "index_primary_lookup_stage",
            )
            .await?;
        let mut key_to_primary = BTreeMap::<Vec<u8>, i64>::new();
        for (primary_key, key) in encoded {
            key_to_primary.insert(key, primary_key);
        }

        let mut entries_by_primary = BTreeMap::<i64, LatestEntry>::new();
        for entry in scan.entries {
            let Some(primary_key) = key_to_primary.get(entry.key.as_slice()).copied() else {
                continue;
            };
            match entries_by_primary.get(&primary_key) {
                Some(existing) if existing.version >= entry.version => {}
                _ => {
                    entries_by_primary.insert(primary_key, entry);
                }
            }
        }

        Ok(PrimaryLookupBatchResult {
            entries_by_primary,
            stats: scan.stats,
        })
    }

    fn build_secondary_index_probe_ranges(
        &self,
        index: &SecondaryIndexRecord,
        prefix: &[crate::optimizer::planner::IndexPrefixConstraint],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if prefix.is_empty() {
            return Ok(Vec::new());
        }
        let mut value_matrix = Vec::<Vec<ScalarValue>>::new();
        for column in prefix {
            if column.values.is_empty() {
                return Ok(Vec::new());
            }
            value_matrix.push(column.values.clone());
        }
        let mut combinations = Vec::<Vec<ScalarValue>>::new();
        let mut current = Vec::<ScalarValue>::new();
        expand_scalar_combinations(
            value_matrix.as_slice(),
            0,
            &mut current,
            self.optimizer.config().planner.max_index_probes,
            &mut combinations,
        );
        let mut ranges = Vec::with_capacity(combinations.len());
        for combo in combinations {
            let start = encode_secondary_index_lookup_prefix_for_prefix(
                index,
                self.columns.as_slice(),
                combo.as_slice(),
            )?;
            let end = prefix_end(start.as_slice()).unwrap_or_default();
            if !end.is_empty() && start < end {
                ranges.push((start, end));
            }
        }

        if matches!(index.distribution, SecondaryIndexDistribution::Hash) {
            ranges.sort_by(|left, right| left.0.cmp(&right.0));
            ranges.dedup_by(|left, right| left.0 == right.0 && left.1 == right.1);
        }
        Ok(ranges)
    }

    async fn build_scan_range_tasks(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Vec<ScanRangeTask> {
        let mut tasks = Vec::<ScanRangeTask>::new();
        for (range_start_key, range_end_key) in ranges {
            let targets = self
                .scan_targets_for_range(range_start_key.as_slice(), range_end_key.as_slice())
                .await;
            for target in targets {
                tasks.push(ScanRangeTask {
                    task_id: tasks.len(),
                    range_end_key: range_end_key.clone(),
                    target,
                });
            }
        }
        tasks
    }

    async fn execute_single_scan_range_task_ctx(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        task: ScanRangeTask,
        limit: Option<usize>,
        stage_label: &str,
        chunk_sequence: &std::sync::atomic::AtomicU64,
    ) -> Result<ScanExecutionResult> {
        let mut result = ScanExecutionResult::default();
        let mut seen_keys = BTreeSet::<Vec<u8>>::new();
        let mut target = task.target;
        let mut cursor = Vec::new();
        let mut retries = 0usize;

        loop {
            let remaining = limit.map(|max| max.saturating_sub(result.entries.len()));
            if matches!(remaining, Some(0)) {
                break;
            }
            let page_limit = remaining
                .map(|remaining| remaining.min(self.page_size).max(1))
                .unwrap_or(self.page_size);

            let scan_client = self.client_for(target.grpc_addr);
            match scan_client
                .range_snapshot_latest(
                    target.shard_index,
                    target.start_key.as_slice(),
                    target.end_key.as_slice(),
                    cursor.as_slice(),
                    page_limit,
                    false,
                )
                .await
            {
                Ok((entries, next_cursor, done)) => {
                    retries = 0;
                    result.stats.rpc_pages = result.stats.rpc_pages.saturating_add(1);
                    let mut chunk = ScanChunk {
                        sequence: chunk_sequence
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                            .saturating_add(1),
                        resume_cursor: next_cursor.clone(),
                        rows_scanned: 0,
                        bytes_scanned: 0,
                    };

                    for entry in entries {
                        let bytes = entry.key.len() as u64 + entry.value.len() as u64;
                        result.stats.rows_scanned = result.stats.rows_scanned.saturating_add(1);
                        result.stats.bytes_scanned =
                            result.stats.bytes_scanned.saturating_add(bytes);
                        chunk.rows_scanned = chunk.rows_scanned.saturating_add(1);
                        chunk.bytes_scanned = chunk.bytes_scanned.saturating_add(bytes);
                        if !seen_keys.insert(entry.key.clone()) {
                            result.stats.duplicate_rows_skipped =
                                result.stats.duplicate_rows_skipped.saturating_add(1);
                            continue;
                        }
                        result.entries.push(entry);
                    }

                    result.stats.chunks = result.stats.chunks.saturating_add(1);
                    self.metrics.record_scan_chunk();
                    self.metrics.record_stage_event(
                        query_execution_id,
                        stage_id,
                        "scan_chunk",
                        format!(
                            "sequence={} shard={} target={} rows_scanned={} bytes_scanned={} resume_cursor={}",
                            chunk.sequence,
                            target.shard_index,
                            target.grpc_addr,
                            chunk.rows_scanned,
                            chunk.bytes_scanned,
                            hex::encode(&chunk.resume_cursor)
                        ),
                    );

                    if done
                        || next_cursor.is_empty()
                        || matches!(limit, Some(max) if result.entries.len() >= max)
                    {
                        break;
                    }
                    cursor = next_cursor;
                }
                Err(err) => {
                    if retries + 1 >= self.scan_retry_limit.max(1) {
                        return Err(anyhow!(
                            "snapshot stage={} table={} shard={} target={} start={} end={} cursor={} limit={} failed after {} retries: {}",
                            stage_label,
                            self.table_name,
                            target.shard_index,
                            target.grpc_addr,
                            hex::encode(&target.start_key),
                            hex::encode(&target.end_key),
                            hex::encode(&cursor),
                            page_limit,
                            retries,
                            err
                        ));
                    }
                    retries = retries.saturating_add(1);
                    result.stats.retries = result.stats.retries.saturating_add(1);
                    self.metrics.record_scan_retry();
                    self.metrics.record_stage_event(
                        query_execution_id,
                        stage_id,
                        "scan_retry",
                        format!(
                            "attempt={} shard={} target={} error={}",
                            retries, target.shard_index, target.grpc_addr, err
                        ),
                    );

                    let resume_key = if cursor.is_empty() {
                        target.start_key.clone()
                    } else {
                        cursor.clone()
                    };
                    if let Some(rerouted) = self
                        .reroute_scan_target(resume_key.as_slice(), task.range_end_key.as_slice())
                        .await
                    {
                        if rerouted.grpc_addr != target.grpc_addr
                            || rerouted.shard_index != target.shard_index
                        {
                            result.stats.reroutes = result.stats.reroutes.saturating_add(1);
                            self.metrics.record_scan_reroute();
                            self.metrics.record_stage_event(
                                query_execution_id,
                                stage_id,
                                "scan_reroute",
                                format!(
                                    "from_shard={} from_target={} to_shard={} to_target={}",
                                    target.shard_index,
                                    target.grpc_addr,
                                    rerouted.shard_index,
                                    rerouted.grpc_addr
                                ),
                            );
                        }
                        target = rerouted;
                    }
                    tokio::time::sleep(self.scan_retry_delay).await;
                }
            }
        }
        Ok(result)
    }

    async fn execute_scan_ranges_ctx(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        ranges: &[(Vec<u8>, Vec<u8>)],
        limit: Option<usize>,
        stage_label: &str,
    ) -> Result<ScanExecutionResult> {
        let mut result = ScanExecutionResult::default();
        let mut seen_keys = BTreeSet::<Vec<u8>>::new();

        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            stage_label,
            format!(
                "ranges={} limit={}",
                ranges.len(),
                limit
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ),
        );

        let tasks = self.build_scan_range_tasks(ranges).await;
        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "scan_partition_plan",
            format!(
                "ranges={} tasks={} parallelism={} limit={}",
                ranges.len(),
                tasks.len(),
                self.scan_parallelism.max(1),
                limit
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ),
        );
        if tasks.is_empty() {
            return Ok(result);
        }

        let chunk_sequence = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let allow_parallel = limit.is_none() && tasks.len() > 1;
        if allow_parallel {
            let concurrency = self.scan_parallelism.max(1).min(tasks.len());
            let mut completed = futures_util::stream::iter(tasks.into_iter().map(|task| async {
                let task_id = task.task_id;
                let scan = self
                    .execute_single_scan_range_task_ctx(
                        query_execution_id,
                        stage_id,
                        task,
                        None,
                        stage_label,
                        chunk_sequence.as_ref(),
                    )
                    .await;
                (task_id, scan)
            }))
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
            completed.sort_by_key(|(task_id, _)| *task_id);

            for (_task_id, task_scan) in completed {
                let task_scan = task_scan?;
                result.stats.rpc_pages = result
                    .stats
                    .rpc_pages
                    .saturating_add(task_scan.stats.rpc_pages);
                result.stats.rows_scanned = result
                    .stats
                    .rows_scanned
                    .saturating_add(task_scan.stats.rows_scanned);
                result.stats.bytes_scanned = result
                    .stats
                    .bytes_scanned
                    .saturating_add(task_scan.stats.bytes_scanned);
                result.stats.retries = result.stats.retries.saturating_add(task_scan.stats.retries);
                result.stats.reroutes = result
                    .stats
                    .reroutes
                    .saturating_add(task_scan.stats.reroutes);
                result.stats.chunks = result.stats.chunks.saturating_add(task_scan.stats.chunks);
                result.stats.duplicate_rows_skipped = result
                    .stats
                    .duplicate_rows_skipped
                    .saturating_add(task_scan.stats.duplicate_rows_skipped);
                for entry in task_scan.entries {
                    if !seen_keys.insert(entry.key.clone()) {
                        result.stats.duplicate_rows_skipped =
                            result.stats.duplicate_rows_skipped.saturating_add(1);
                        continue;
                    }
                    result.stats.rows_output = result.stats.rows_output.saturating_add(1);
                    result.entries.push(entry);
                }
            }
            return Ok(result);
        }

        for task in tasks {
            let remaining = limit.map(|max| max.saturating_sub(result.entries.len()));
            if matches!(remaining, Some(0)) {
                break;
            }
            let task_scan = self
                .execute_single_scan_range_task_ctx(
                    query_execution_id,
                    stage_id,
                    task,
                    remaining,
                    stage_label,
                    chunk_sequence.as_ref(),
                )
                .await?;
            result.stats.rpc_pages = result
                .stats
                .rpc_pages
                .saturating_add(task_scan.stats.rpc_pages);
            result.stats.rows_scanned = result
                .stats
                .rows_scanned
                .saturating_add(task_scan.stats.rows_scanned);
            result.stats.bytes_scanned = result
                .stats
                .bytes_scanned
                .saturating_add(task_scan.stats.bytes_scanned);
            result.stats.retries = result.stats.retries.saturating_add(task_scan.stats.retries);
            result.stats.reroutes = result
                .stats
                .reroutes
                .saturating_add(task_scan.stats.reroutes);
            result.stats.chunks = result.stats.chunks.saturating_add(task_scan.stats.chunks);
            result.stats.duplicate_rows_skipped = result
                .stats
                .duplicate_rows_skipped
                .saturating_add(task_scan.stats.duplicate_rows_skipped);
            for entry in task_scan.entries {
                if !seen_keys.insert(entry.key.clone()) {
                    result.stats.duplicate_rows_skipped =
                        result.stats.duplicate_rows_skipped.saturating_add(1);
                    continue;
                }
                result.stats.rows_output = result.stats.rows_output.saturating_add(1);
                result.entries.push(entry);
                if matches!(limit, Some(max) if result.entries.len() >= max) {
                    break;
                }
            }
        }
        Ok(result)
    }

    async fn aggregate_single_index_scan_task_ctx(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        index: &SecondaryIndexRecord,
        task: ScanRangeTask,
        group_indexes: &[usize],
        sum_column_index: usize,
        runtime_filters: &[AggregateFilterRuntime],
        chunk_sequence: &std::sync::atomic::AtomicU64,
    ) -> Result<(BTreeMap<Vec<u8>, AggregateGroupState>, ScanStats)> {
        let mut local_groups = BTreeMap::<Vec<u8>, AggregateGroupState>::new();
        let mut stats = ScanStats::default();
        let mut seen_keys = BTreeSet::<Vec<u8>>::new();
        let mut target = task.target;
        let mut cursor = Vec::new();
        let mut retries = 0usize;

        loop {
            let page_limit = self.page_size.max(1);
            let scan_client = self.client_for(target.grpc_addr);
            match scan_client
                .range_snapshot_latest(
                    target.shard_index,
                    target.start_key.as_slice(),
                    target.end_key.as_slice(),
                    cursor.as_slice(),
                    page_limit,
                    false,
                )
                .await
            {
                Ok((entries, next_cursor, done)) => {
                    retries = 0;
                    stats.rpc_pages = stats.rpc_pages.saturating_add(1);
                    stats.chunks = stats.chunks.saturating_add(1);
                    self.metrics.record_scan_chunk();
                    let mut chunk = ScanChunk {
                        sequence: chunk_sequence
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                            .saturating_add(1),
                        resume_cursor: next_cursor.clone(),
                        rows_scanned: 0,
                        bytes_scanned: 0,
                    };
                    for entry in entries {
                        let bytes = entry.key.len() as u64 + entry.value.len() as u64;
                        stats.rows_scanned = stats.rows_scanned.saturating_add(1);
                        stats.bytes_scanned = stats.bytes_scanned.saturating_add(bytes);
                        chunk.rows_scanned = chunk.rows_scanned.saturating_add(1);
                        chunk.bytes_scanned = chunk.bytes_scanned.saturating_add(bytes);
                        if !seen_keys.insert(entry.key.clone()) {
                            stats.duplicate_rows_skipped =
                                stats.duplicate_rows_skipped.saturating_add(1);
                            continue;
                        }
                        let row_values = decode_secondary_index_row_values(
                            index,
                            self.columns.as_slice(),
                            self.primary_key_column.as_str(),
                            entry.key.as_slice(),
                            entry.value.as_slice(),
                        )?;
                        if !row_matches_grouped_filters(row_values.as_slice(), runtime_filters) {
                            continue;
                        }
                        stats.rows_output = stats.rows_output.saturating_add(1);
                        aggregate_grouped_row(
                            &mut local_groups,
                            self.columns.as_slice(),
                            group_indexes,
                            sum_column_index,
                            row_values.as_slice(),
                        )?;
                    }
                    self.metrics.record_stage_event(
                        query_execution_id,
                        stage_id,
                        "scan_chunk",
                        format!(
                            "sequence={} shard={} target={} rows_scanned={} bytes_scanned={} resume_cursor={}",
                            chunk.sequence,
                            target.shard_index,
                            target.grpc_addr,
                            chunk.rows_scanned,
                            chunk.bytes_scanned,
                            hex::encode(&chunk.resume_cursor)
                        ),
                    );
                    if done || next_cursor.is_empty() {
                        break;
                    }
                    cursor = next_cursor;
                }
                Err(err) => {
                    if retries + 1 >= self.scan_retry_limit.max(1) {
                        return Err(anyhow!(
                            "aggregate index scan failed after {} retries for shard={} target={}: {}",
                            retries,
                            target.shard_index,
                            target.grpc_addr,
                            err
                        ));
                    }
                    retries = retries.saturating_add(1);
                    stats.retries = stats.retries.saturating_add(1);
                    self.metrics.record_scan_retry();
                    self.metrics.record_stage_event(
                        query_execution_id,
                        stage_id,
                        "aggregate_pushdown_scan_retry",
                        format!(
                            "attempt={} shard={} target={} error={}",
                            retries, target.shard_index, target.grpc_addr, err
                        ),
                    );
                    let resume_key = if cursor.is_empty() {
                        target.start_key.clone()
                    } else {
                        cursor.clone()
                    };
                    if let Some(rerouted) = self
                        .reroute_scan_target(resume_key.as_slice(), task.range_end_key.as_slice())
                        .await
                    {
                        if rerouted.grpc_addr != target.grpc_addr
                            || rerouted.shard_index != target.shard_index
                        {
                            stats.reroutes = stats.reroutes.saturating_add(1);
                            self.metrics.record_scan_reroute();
                            self.metrics.record_stage_event(
                                query_execution_id,
                                stage_id,
                                "aggregate_pushdown_scan_reroute",
                                format!(
                                    "from_shard={} from_target={} to_shard={} to_target={}",
                                    target.shard_index,
                                    target.grpc_addr,
                                    rerouted.shard_index,
                                    rerouted.grpc_addr
                                ),
                            );
                        }
                        target = rerouted;
                    }
                    tokio::time::sleep(self.scan_retry_delay).await;
                }
            }
        }

        Ok((local_groups, stats))
    }

    async fn aggregate_index_scan_streaming_ctx(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        plan: &PlannedAccessPath,
        group_indexes: &[usize],
        sum_column_index: usize,
        runtime_filters: &[AggregateFilterRuntime],
        required_columns: &BTreeSet<String>,
        global_groups: &mut BTreeMap<Vec<u8>, AggregateGroupState>,
    ) -> Result<ScanStats> {
        let AccessPath::Index {
            index_name, prefix, ..
        } = &plan.path
        else {
            return Err(anyhow!(
                "aggregate index scan requires an index access path"
            ));
        };
        let Some(index) = self
            .public_secondary_indexes()
            .into_iter()
            .find(|candidate| candidate.index_name.eq_ignore_ascii_case(index_name))
            .cloned()
        else {
            return Err(anyhow!(
                "optimizer selected missing secondary index '{}'",
                index_name
            ));
        };
        if !index_covers_required_columns(
            &index,
            self.primary_key_column.as_str(),
            required_columns,
        ) {
            return Err(anyhow!(
                "aggregate index scan requires covering index '{}' for required columns",
                index.index_name
            ));
        }

        let probe_ranges = self.build_secondary_index_probe_ranges(&index, prefix.as_slice())?;
        if probe_ranges.is_empty() {
            return Ok(ScanStats::default());
        }
        let tasks = self.build_scan_range_tasks(probe_ranges.as_slice()).await;
        if tasks.is_empty() {
            return Ok(ScanStats::default());
        }
        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "aggregate_pushdown_index_scan_start",
            format!(
                "index={} ranges={} tasks={} parallelism={}",
                index.index_name,
                probe_ranges.len(),
                tasks.len(),
                self.scan_parallelism.max(1)
            ),
        );

        let chunk_sequence = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let concurrency = self.scan_parallelism.max(1).min(tasks.len());
        let mut completed = futures_util::stream::iter(tasks.into_iter().map(|task| async {
            let task_id = task.task_id;
            let result = self
                .aggregate_single_index_scan_task_ctx(
                    query_execution_id,
                    stage_id,
                    &index,
                    task,
                    group_indexes,
                    sum_column_index,
                    runtime_filters,
                    chunk_sequence.as_ref(),
                )
                .await;
            (task_id, result)
        }))
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
        completed.sort_by_key(|(task_id, _)| *task_id);

        let mut stats = ScanStats::default();
        for (task_id, item) in completed {
            let (local_groups, local_stats) = item?;
            stats.rpc_pages = stats.rpc_pages.saturating_add(local_stats.rpc_pages);
            stats.rows_scanned = stats.rows_scanned.saturating_add(local_stats.rows_scanned);
            stats.rows_output = stats.rows_output.saturating_add(local_stats.rows_output);
            stats.bytes_scanned = stats
                .bytes_scanned
                .saturating_add(local_stats.bytes_scanned);
            stats.retries = stats.retries.saturating_add(local_stats.retries);
            stats.reroutes = stats.reroutes.saturating_add(local_stats.reroutes);
            stats.chunks = stats.chunks.saturating_add(local_stats.chunks);
            stats.duplicate_rows_skipped = stats
                .duplicate_rows_skipped
                .saturating_add(local_stats.duplicate_rows_skipped);

            let local_group_count = local_groups.len();
            merge_aggregate_group_maps(global_groups, local_groups)?;
            self.metrics.record_stage_event(
                query_execution_id,
                stage_id,
                "aggregate_pushdown_partial_merge",
                format!(
                    "task={} local_groups={} global_groups={}",
                    task_id,
                    local_group_count,
                    global_groups.len()
                ),
            );
        }
        Ok(stats)
    }

    /// Streams a full-table scan and maintains per-target partial aggregates, then merges globally.
    async fn aggregate_table_scan_streaming_ctx(
        &self,
        query_execution_id: &str,
        stage_id: u64,
        group_indexes: &[usize],
        sum_column_index: usize,
        runtime_filters: &[AggregateFilterRuntime],
        required_column_indexes: &BTreeSet<usize>,
        global_groups: &mut BTreeMap<Vec<u8>, AggregateGroupState>,
    ) -> Result<ScanStats> {
        let scan_ranges = self.scan_ranges_for_bounds(PkBounds::default(), self.table_id)?;
        if scan_ranges.is_empty() {
            return Ok(ScanStats::default());
        }

        self.metrics.record_stage_event(
            query_execution_id,
            stage_id,
            "aggregate_pushdown_table_scan_start",
            format!("ranges={}", scan_ranges.len()),
        );

        let mut stats = ScanStats::default();
        let mut seen_keys = BTreeSet::<Vec<u8>>::new();

        for (range_start_key, range_end_key) in scan_ranges {
            let targets = self
                .scan_targets_for_range(range_start_key.as_slice(), range_end_key.as_slice())
                .await;
            for base_target in targets {
                let mut local_groups = BTreeMap::<Vec<u8>, AggregateGroupState>::new();
                let mut target = base_target;
                let mut cursor = Vec::new();
                let mut retries = 0usize;

                loop {
                    let page_limit = self.page_size.max(1);
                    let scan_client = self.client_for(target.grpc_addr);
                    match scan_client
                        .range_snapshot_latest(
                            target.shard_index,
                            target.start_key.as_slice(),
                            target.end_key.as_slice(),
                            cursor.as_slice(),
                            page_limit,
                            false,
                        )
                        .await
                    {
                        Ok((entries, next_cursor, done)) => {
                            retries = 0;
                            stats.rpc_pages = stats.rpc_pages.saturating_add(1);
                            stats.chunks = stats.chunks.saturating_add(1);
                            self.metrics.record_scan_chunk();

                            for entry in entries {
                                let bytes = entry.key.len() as u64 + entry.value.len() as u64;
                                stats.rows_scanned = stats.rows_scanned.saturating_add(1);
                                stats.bytes_scanned = stats.bytes_scanned.saturating_add(bytes);
                                if !seen_keys.insert(entry.key.clone()) {
                                    stats.duplicate_rows_skipped =
                                        stats.duplicate_rows_skipped.saturating_add(1);
                                    continue;
                                }
                                let decoded = decode_generic_entry_with_version_projected(
                                    self.table_id,
                                    self.columns.as_slice(),
                                    self.primary_key_index,
                                    required_column_indexes,
                                    &entry,
                                )?;
                                let Some(row) = decoded else {
                                    continue;
                                };
                                if !row_matches_grouped_filters(
                                    row.values.as_slice(),
                                    runtime_filters,
                                ) {
                                    continue;
                                }
                                stats.rows_output = stats.rows_output.saturating_add(1);
                                aggregate_grouped_row(
                                    &mut local_groups,
                                    self.columns.as_slice(),
                                    group_indexes,
                                    sum_column_index,
                                    row.values.as_slice(),
                                )?;
                            }

                            if done || next_cursor.is_empty() {
                                break;
                            }
                            cursor = next_cursor;
                        }
                        Err(err) => {
                            if retries + 1 >= self.scan_retry_limit.max(1) {
                                return Err(anyhow!(
                                    "aggregate table scan failed after {} retries for shard={} target={}: {}",
                                    retries,
                                    target.shard_index,
                                    target.grpc_addr,
                                    err
                                ));
                            }
                            retries = retries.saturating_add(1);
                            stats.retries = stats.retries.saturating_add(1);
                            self.metrics.record_scan_retry();
                            self.metrics.record_stage_event(
                                query_execution_id,
                                stage_id,
                                "aggregate_pushdown_scan_retry",
                                format!(
                                    "attempt={} shard={} target={} error={}",
                                    retries, target.shard_index, target.grpc_addr, err
                                ),
                            );

                            let resume_key = if cursor.is_empty() {
                                target.start_key.clone()
                            } else {
                                cursor.clone()
                            };
                            if let Some(rerouted) = self
                                .reroute_scan_target(
                                    resume_key.as_slice(),
                                    range_end_key.as_slice(),
                                )
                                .await
                            {
                                if rerouted.grpc_addr != target.grpc_addr
                                    || rerouted.shard_index != target.shard_index
                                {
                                    stats.reroutes = stats.reroutes.saturating_add(1);
                                    self.metrics.record_scan_reroute();
                                    self.metrics.record_stage_event(
                                        query_execution_id,
                                        stage_id,
                                        "aggregate_pushdown_scan_reroute",
                                        format!(
                                            "from_shard={} from_target={} to_shard={} to_target={}",
                                            target.shard_index,
                                            target.grpc_addr,
                                            rerouted.shard_index,
                                            rerouted.grpc_addr
                                        ),
                                    );
                                }
                                target = rerouted;
                            }
                            tokio::time::sleep(self.scan_retry_delay).await;
                        }
                    }
                }

                let local_group_count = local_groups.len();
                merge_aggregate_group_maps(global_groups, local_groups)?;
                self.metrics.record_stage_event(
                    query_execution_id,
                    stage_id,
                    "aggregate_pushdown_partial_merge",
                    format!(
                        "shard={} target={} local_groups={} global_groups={}",
                        target.shard_index,
                        target.grpc_addr,
                        local_group_count,
                        global_groups.len()
                    ),
                );
            }
        }

        Ok(stats)
    }

    /// Builds initial scan targets for a bounded key range.
    async fn scan_targets_for_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<LocalScanTarget> {
        match fetch_topology(&self.client).await {
            Ok(topology) => {
                if let Err(err) = require_non_empty_topology(&topology) {
                    warn!(error = %err, "empty topology; falling back to local scan target");
                    vec![LocalScanTarget {
                        shard_index: self.preferred_shards.first().copied().unwrap_or(0),
                        grpc_addr: self.local_grpc_addr,
                        start_key: start_key.to_vec(),
                        end_key: end_key.to_vec(),
                    }]
                } else {
                    scan_targets(
                        &topology,
                        self.local_grpc_addr,
                        start_key,
                        end_key,
                        &self.preferred_shards,
                    )
                    .into_iter()
                    .map(LocalScanTarget::from)
                    .collect()
                }
            }
            Err(err) => {
                warn!(error = %err, "topology fetch failed; falling back to local scan target");
                vec![LocalScanTarget {
                    shard_index: self.preferred_shards.first().copied().unwrap_or(0),
                    grpc_addr: self.local_grpc_addr,
                    start_key: start_key.to_vec(),
                    end_key: end_key.to_vec(),
                }]
            }
        }
    }

    /// Re-routes a scan attempt using latest topology and resume key.
    async fn reroute_scan_target(
        &self,
        resume_key: &[u8],
        original_end_key: &[u8],
    ) -> Option<LocalScanTarget> {
        let topology = fetch_topology(&self.client).await.ok()?;
        require_non_empty_topology(&topology).ok()?;
        let route = route_key(
            &topology,
            self.local_grpc_addr,
            resume_key,
            &self.preferred_shards,
        )?;
        let end_key = min_end_bound(original_end_key, route.end_key.as_slice());
        if !end_key.is_empty() && resume_key >= end_key.as_slice() {
            return None;
        }
        Some(LocalScanTarget {
            shard_index: route.shard_index,
            grpc_addr: route.grpc_addr,
            start_key: resume_key.to_vec(),
            end_key,
        })
    }

    /// Executes typed storage scan contract and returns raw entries + stats.
    async fn execute_scan_spec(&self, spec: &ScanSpec) -> Result<ScanExecutionResult> {
        if spec.bounds.is_empty() {
            return Ok(ScanExecutionResult::default());
        }

        let scan_ranges = self.scan_ranges_for_bounds(spec.bounds, spec.table_id)?;
        if scan_ranges.is_empty() {
            return Ok(ScanExecutionResult::default());
        }

        let mut all_targets = Vec::<LocalScanTarget>::new();
        for (start_key, end_key) in &scan_ranges {
            let mut targets = self
                .scan_targets_for_range(start_key.as_slice(), end_key.as_slice())
                .await;
            all_targets.append(&mut targets);
        }
        self.metrics.record_stage_event(
            spec.query_execution_id.as_str(),
            spec.stage_id,
            "scan_stage_start",
            format!(
                "table={} targets={} projection_columns={} limit={}",
                spec.table_name,
                all_targets.len(),
                spec.projected_columns.len(),
                spec.limit
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_string())
            ),
        );
        if let Some(predicate) = spec.predicate.as_ref() {
            self.metrics.record_stage_event(
                spec.query_execution_id.as_str(),
                spec.stage_id,
                "scan_spec_predicate",
                format!("predicate={}", format_scan_predicate_expr(predicate)),
            );
        }

        let result = self
            .execute_scan_ranges_ctx(
                spec.query_execution_id.as_str(),
                spec.stage_id,
                scan_ranges.as_slice(),
                spec.limit,
                "scan_stage_plan",
            )
            .await?;

        self.metrics.record_stage_event(
            spec.query_execution_id.as_str(),
            spec.stage_id,
            "scan_stage_finish",
            format!(
                "rpc_pages={} rows_scanned={} rows_output={} bytes_scanned={} retries={} reroutes={} chunks={} duplicate_rows_skipped={}",
                result.stats.rpc_pages,
                result.stats.rows_scanned,
                result.stats.rows_output,
                result.stats.bytes_scanned,
                result.stats.retries,
                result.stats.reroutes,
                result.stats.chunks,
                result.stats.duplicate_rows_skipped
            ),
        );

        Ok(result)
    }

    /// Executes `rollback conditional targets` for this component.
    async fn rollback_conditional_targets(
        &self,
        applied_targets: &[(WriteTarget, Vec<PreparedConditionalEntry>)],
        op: &'static str,
    ) -> Result<()> {
        // Decision: evaluate `applied_targets.is_empty()` to choose the correct SQL/storage control path.
        if applied_targets.is_empty() {
            return Ok(());
        }

        let routed_topology = match fetch_topology(&self.client).await {
            Ok(topology) => {
                if let Err(err) = require_non_empty_topology(&topology) {
                    warn!(error = %err, "rollback topology is empty; falling back to original targets");
                    None
                } else {
                    Some(topology)
                }
            }
            Err(err) => {
                warn!(error = %err, "failed to fetch topology for rollback; falling back to original targets");
                None
            }
        };

        let rollback_targets = if let Some(topology) = routed_topology.as_ref() {
            let mut rerouted = BTreeMap::<WriteTarget, Vec<PreparedConditionalEntry>>::new();
            for (original_target, entries) in applied_targets.iter().rev() {
                for entry in entries {
                    let target = route_key(
                        topology,
                        self.local_grpc_addr,
                        entry.key.as_slice(),
                        &self.preferred_shards,
                    )
                    .map(|route| WriteTarget {
                        shard_index: route.shard_index,
                        grpc_addr: route.grpc_addr,
                        start_key: route.start_key,
                        end_key: route.end_key,
                    })
                    .unwrap_or_else(|| original_target.clone());
                    rerouted.entry(target).or_default().push(entry.clone());
                }
            }
            rerouted.into_iter().collect::<Vec<_>>()
        } else {
            applied_targets
                .iter()
                .rev()
                .map(|(target, entries)| (target.clone(), entries.clone()))
                .collect::<Vec<_>>()
        };

        let retry_policy = self.write_retry_policy;
        let retry_limit = retry_policy.max_attempts();
        let retry_deadline = if retry_policy.enabled && retry_policy.max_elapsed > Duration::ZERO {
            Some(Instant::now() + retry_policy.max_elapsed)
        } else {
            None
        };
        let mut first_err: Option<anyhow::Error> = None;
        for (target, entries) in rollback_targets {
            let client = self.client_for(target.grpc_addr);
            let batch_ranges = conditional_chunk_ranges(
                entries.as_slice(),
                self.distributed_write_max_batch_entries,
                self.distributed_write_max_batch_bytes,
            );
            let batch_count = batch_ranges.len();
            for (batch_idx, batch_range) in batch_ranges.into_iter().enumerate() {
                let start = batch_range.start;
                let end = batch_range.end;
                let rollback_entries = entries[start..end]
                    .iter()
                    .map(|entry| ReplicatedConditionalWriteEntry {
                        key: entry.key.clone(),
                        value: entry.rollback_value.clone(),
                        expected_version: entry.applied_version,
                    })
                    .collect::<Vec<_>>();
                let expected = rollback_entries.len() as u64;
                let rollback_bytes = entries[start..end]
                    .iter()
                    .map(prepared_conditional_entry_size)
                    .fold(0usize, |acc, next| acc.saturating_add(next));
                let mut batch_succeeded = false;
                let mut batch_err: Option<anyhow::Error> = None;
                for attempt in 0..retry_limit {
                    if let Some(deadline) = retry_deadline {
                        if Instant::now() >= deadline {
                            break;
                        }
                    }

                    if let Err(err) = self.enforce_inflight_budget(
                        expected as usize,
                        rollback_bytes,
                        1,
                        "rollback_batch",
                    ) {
                        let wrapped = anyhow!(
                            "rollback for {op} budget rejected on shard {} chunk {}/{} target={}: {err}",
                            target.shard_index,
                            batch_idx + 1,
                            batch_count,
                            target.grpc_addr
                        );
                        warn!(error = %wrapped, "rollback budget rejection");
                        batch_err = Some(wrapped);
                        break;
                    }

                    let circuit_token = match self.circuit_acquire(target.grpc_addr) {
                        Ok(token) => token,
                        Err(err) => {
                            let wrapped = anyhow!(
                                "rollback for {op} circuit rejection on shard {} chunk {}/{} target={}: {err}",
                                target.shard_index,
                                batch_idx + 1,
                                batch_count,
                                target.grpc_addr
                            );
                            let class = classify_write_error_message(wrapped.to_string().as_str());
                            warn!(
                                error = %wrapped,
                                attempt = attempt + 1,
                                retry_limit = retry_limit,
                                retryable = class.is_retryable(),
                                "rollback circuit rejection"
                            );
                            batch_err = Some(wrapped);
                            if class.is_retryable() && attempt + 1 < retry_limit {
                                if let Some(deadline) = retry_deadline {
                                    if Instant::now() >= deadline {
                                        break;
                                    }
                                }
                                tokio::time::sleep(retry_backoff(
                                    retry_policy,
                                    attempt,
                                    target.grpc_addr.port() as u64,
                                ))
                                .await;
                                continue;
                            }
                            break;
                        }
                    };

                    let rollback_span = info_span!(
                        "holo_fusion.distributed_write_rollback",
                        table = %self.table_name,
                        op = op,
                        shard_index = target.shard_index,
                        target = %target.grpc_addr,
                        batch_index = batch_idx + 1,
                        batch_count = batch_count
                    );
                    let rollback_started = Instant::now();
                    // Decision: evaluate `client` to choose the correct SQL/storage control path.
                    match client
                        .range_write_latest_conditional(
                            target.shard_index,
                            target.start_key.as_slice(),
                            target.end_key.as_slice(),
                            rollback_entries.clone(),
                        )
                        .instrument(rollback_span)
                        .await
                    {
                        Ok(result) => {
                            self.circuit_on_success(target.grpc_addr, circuit_token);
                            self.metrics.record_distributed_rollback(
                                target.shard_index,
                                expected,
                                rollback_started.elapsed(),
                            );
                            // Decision: evaluate `result.conflicts > 0` to choose the correct SQL/storage control path.
                            if result.conflicts > 0 {
                                self.metrics.record_distributed_conflict(target.shard_index);
                                let wrapped = anyhow!(
                                    "rollback for {op} conflicted on shard {} chunk {}/{} (conflicts={})",
                                    target.shard_index,
                                    batch_idx + 1,
                                    batch_count,
                                    result.conflicts
                                );
                                warn!(error = %wrapped, "conditional rollback conflict");
                                batch_err = Some(wrapped);
                            // Decision: evaluate `result.applied != expected` to choose the correct SQL/storage control path.
                            } else if result.applied != expected {
                                let wrapped = anyhow!(
                                    "rollback for {op} partially applied on shard {} chunk {}/{}: expected {}, applied {}",
                                    target.shard_index,
                                    batch_idx + 1,
                                    batch_count,
                                    expected,
                                    result.applied
                                );
                                warn!(error = %wrapped, "conditional rollback partial apply");
                                batch_err = Some(wrapped);
                            } else {
                                batch_succeeded = true;
                                batch_err = None;
                            }
                            break;
                        }
                        Err(err) => {
                            self.circuit_on_failure(target.grpc_addr, circuit_token);
                            self.metrics.record_distributed_apply_failure(
                                target.grpc_addr.to_string().as_str(),
                                classify_write_error_message(err.to_string().as_str())
                                    .is_retryable(),
                            );
                            self.metrics.record_distributed_rollback(
                                target.shard_index,
                                expected,
                                rollback_started.elapsed(),
                            );
                            let wrapped = anyhow!(
                                "rollback for {op} failed on shard {} chunk {}/{} target={} with error: {err}",
                                target.shard_index,
                                batch_idx + 1,
                                batch_count,
                                target.grpc_addr
                            );
                            let class = classify_write_error_message(wrapped.to_string().as_str());
                            warn!(
                                error = %wrapped,
                                attempt = attempt + 1,
                                retry_limit = retry_limit,
                                retryable = class.is_retryable(),
                                "conditional rollback rpc failure"
                            );
                            batch_err = Some(wrapped);
                            if class.is_retryable() && attempt + 1 < retry_limit {
                                if let Some(deadline) = retry_deadline {
                                    if Instant::now() >= deadline {
                                        break;
                                    }
                                }
                                tokio::time::sleep(retry_backoff(
                                    retry_policy,
                                    attempt,
                                    target.grpc_addr.port() as u64,
                                ))
                                .await;
                                continue;
                            }
                            break;
                        }
                    }
                }

                if !batch_succeeded {
                    let err = batch_err.unwrap_or_else(|| {
                        anyhow!(
                            "rollback for {op} retries exhausted on shard {} chunk {}/{} target={}",
                            target.shard_index,
                            batch_idx + 1,
                            batch_count,
                            target.grpc_addr
                        )
                    });
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
            }
        }

        // Decision: evaluate `let Some(err) = first_err` to choose the correct SQL/storage control path.
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    /// Executes `write batches` for this component.
    async fn write_batches(&self, batches: &[RecordBatch]) -> Result<u64> {
        // Decision: evaluate `batches.is_empty()` to choose the correct SQL/storage control path.
        if batches.is_empty() {
            return Ok(0);
        }

        if self.row_codec_mode == RowCodecMode::RowV1 {
            let mut entries = Vec::new();
            for batch in batches {
                entries.extend(decode_generic_rows(
                    batch,
                    self.columns.as_slice(),
                    self.check_constraints.as_slice(),
                    self.primary_key_index,
                )?);
            }
            return self.insert_primary_entries(entries).await;
        }

        let mut rows = Vec::new();
        for batch in batches {
            rows.extend(decode_orders_rows(batch)?);
        }
        self.insert_orders_rows(&rows).await
    }

    /// Decodes one insert input batch into primary-key/value entries.
    fn decode_batch_to_primary_entries(&self, batch: &RecordBatch) -> Result<Vec<(i64, Vec<u8>)>> {
        if self.row_codec_mode == RowCodecMode::RowV1 {
            return decode_generic_rows(
                batch,
                self.columns.as_slice(),
                self.check_constraints.as_slice(),
                self.primary_key_index,
            );
        }

        let rows = decode_orders_rows(batch)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.order_id, encode_orders_row_value(&row)))
            .collect())
    }

    /// Applies one streaming insert chunk and records detailed rollback metadata.
    async fn apply_streaming_insert_chunk(
        &self,
        pending: &mut Vec<(i64, Vec<u8>)>,
        rollback_value: &[u8],
        applied_targets: &mut Vec<(WriteTarget, Vec<PreparedConditionalEntry>)>,
        chunk_id: &str,
    ) -> Result<(u64, Duration, Vec<(usize, u64)>)> {
        if pending.is_empty() {
            return Ok((0, Duration::ZERO, Vec::new()));
        }
        let writes = pending
            .drain(..)
            .map(|(primary_key, value)| ConditionalPrimaryWrite {
                primary_key,
                expected_version: RpcVersion::zero(),
                value,
                rollback_value: rollback_value.to_vec(),
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        match self
            .apply_primary_writes_conditional_detailed(writes.as_slice(), "bulk_insert")
            .await
        {
            Ok(ConditionalWriteDetailedOutcome::Applied(applied)) => {
                let mut applied_rows_by_shard = BTreeMap::<usize, u64>::new();
                for (target, rows) in &applied.applied_targets {
                    let entry = applied_rows_by_shard.entry(target.shard_index).or_default();
                    *entry = entry.saturating_add(rows.len() as u64);
                }
                info!(
                    table = %self.table_name,
                    chunk_id = chunk_id,
                    rows = applied.applied_rows,
                    latency_ms = started.elapsed().as_millis() as u64,
                    "bulk insert chunk applied"
                );
                applied_targets.extend(applied.applied_targets);
                Ok((
                    applied.applied_rows,
                    started.elapsed(),
                    applied_rows_by_shard.into_iter().collect(),
                ))
            }
            Ok(ConditionalWriteDetailedOutcome::Conflict) => {
                if let Err(rollback_err) = self
                    .rollback_conditional_targets(applied_targets.as_slice(), "bulk_insert")
                    .await
                {
                    return Err(anyhow!(
                        "bulk insert conflict in {chunk_id}; rollback previously applied chunks also failed: {rollback_err}"
                    ));
                }
                Err(DuplicateKeyViolation::for_table(self.table_name.as_str()).into())
            }
            Err(err) => {
                if let Err(rollback_err) = self
                    .rollback_conditional_targets(applied_targets.as_slice(), "bulk_insert")
                    .await
                {
                    return Err(anyhow!(
                        "bulk insert chunk {chunk_id} failed: {err}; rollback previously applied chunks also failed: {rollback_err}"
                    ));
                }
                Err(err).with_context(|| format!("bulk insert chunk failed in {chunk_id}"))
            }
        }
    }

    /// Streaming bulk insert path used by `INSERT ... SELECT` / sink execution.
    async fn write_streaming_insert(&self, data: &mut SendableRecordBatchStream) -> Result<u64> {
        if !self.bulk_ingest_enabled {
            let mut buffered = Vec::new();
            while let Some(batch) = data.next().await.transpose()? {
                self.schema
                    .logically_equivalent_names_and_types(&batch.schema())?;
                buffered.push(batch);
            }
            return self.write_batches(buffered.as_slice()).await;
        }

        let rollback_value = self.encode_tombstone_payload();
        let statement_id = format!("bulk_{}", now_timestamp_nanos().unsigned_abs());
        self.metrics
            .begin_ingest_job(statement_id.as_str(), self.table_name.as_str());
        let result = async {
            let mut seen_keys = HashSet::<i64>::new();
            let mut pending = Vec::<(i64, Vec<u8>)>::new();
            let mut pending_bytes = 0usize;
            let mut applied_targets = Vec::<(WriteTarget, Vec<PreparedConditionalEntry>)>::new();
            let mut written_rows = 0u64;

            let chunk_min = configured_bulk_chunk_rows_min().max(1);
            let chunk_max = configured_bulk_chunk_rows_max().max(chunk_min);
            let mut chunk_target = configured_bulk_chunk_rows_initial()
                .max(chunk_min)
                .min(chunk_max);
            let low_latency = Duration::from_millis(configured_bulk_chunk_low_latency_ms());
            let high_latency = Duration::from_millis(configured_bulk_chunk_high_latency_ms());
            let mut chunk_seq = 0u64;

            while let Some(batch) = data.next().await.transpose()? {
                self.schema
                    .logically_equivalent_names_and_types(&batch.schema())?;
                let entries = self.decode_batch_to_primary_entries(&batch)?;
                for (primary_key, value) in entries {
                    if !seen_keys.insert(primary_key) {
                        if let Err(rollback_err) = self
                            .rollback_conditional_targets(applied_targets.as_slice(), "bulk_insert")
                            .await
                        {
                            return Err(anyhow!(
                                "duplicate key in streaming bulk insert primary_key={primary_key}; rollback previously applied chunks also failed: {rollback_err}"
                            ));
                        }
                        return Err(DuplicateKeyViolation::for_table(self.table_name.as_str()).into());
                    }
                    pending_bytes = pending_bytes
                        .saturating_add(value.len())
                        .saturating_add(std::mem::size_of::<i64>());
                    pending.push((primary_key, value));

                    if pending.len() >= chunk_target
                        || pending_bytes >= self.distributed_write_max_batch_bytes
                    {
                        chunk_seq = chunk_seq.saturating_add(1);
                        let chunk_id = format!("{statement_id}:{chunk_seq}");
                        let (applied, latency, by_shard) = self
                            .apply_streaming_insert_chunk(
                                &mut pending,
                                rollback_value.as_slice(),
                                &mut applied_targets,
                                chunk_id.as_str(),
                            )
                            .await?;
                        written_rows = written_rows.saturating_add(applied);
                        pending_bytes = 0;
                        let shard_lag = by_shard
                            .iter()
                            .map(|(shard, _)| (*shard, 0u64))
                            .collect::<Vec<_>>();
                        self.metrics.record_ingest_progress(
                            statement_id.as_str(),
                            applied,
                            pending.len() as u64,
                            0,
                            0,
                            0,
                            shard_lag.as_slice(),
                        );
                        if latency > high_latency {
                            chunk_target = (chunk_target / 2).max(chunk_min);
                        } else if latency <= low_latency {
                            chunk_target = chunk_target
                                .saturating_add(chunk_target / 4)
                                .min(chunk_max);
                        }
                    }
                }
            }

            if !pending.is_empty() {
                chunk_seq = chunk_seq.saturating_add(1);
                let chunk_id = format!("{statement_id}:{chunk_seq}");
                let (applied, _latency, by_shard) = self
                    .apply_streaming_insert_chunk(
                        &mut pending,
                        rollback_value.as_slice(),
                        &mut applied_targets,
                        chunk_id.as_str(),
                    )
                    .await?;
                written_rows = written_rows.saturating_add(applied);
                let shard_lag = by_shard
                    .iter()
                    .map(|(shard, _)| (*shard, 0u64))
                    .collect::<Vec<_>>();
                self.metrics.record_ingest_progress(
                    statement_id.as_str(),
                    applied,
                    pending.len() as u64,
                    0,
                    0,
                    0,
                    shard_lag.as_slice(),
                );
            }

            Ok::<u64, anyhow::Error>(written_rows)
        }
        .await;

        match result {
            Ok(written_rows) => {
                self.metrics
                    .finish_ingest_job(statement_id.as_str(), true, None);
                self.optimizer_record_row_count_delta(written_rows as i64);
                Ok(written_rows)
            }
            Err(err) => {
                let message = err.to_string();
                self.metrics.finish_ingest_job(
                    statement_id.as_str(),
                    false,
                    Some(message.as_str()),
                );
                Err(err)
            }
        }
    }

    /// Executes strict insert semantics (fail on duplicate PK) for encoded entries.
    async fn insert_primary_entries(&self, rows: Vec<(i64, Vec<u8>)>) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        // Catch obvious duplicate keys in one statement before issuing distributed RPCs.
        let mut seen = HashSet::<i64>::with_capacity(rows.len());
        let mut writes = Vec::with_capacity(rows.len());
        let rollback_value = self.encode_tombstone_payload();
        for (primary_key, value) in rows {
            if !seen.insert(primary_key) {
                return Err(DuplicateKeyViolation::for_table(self.table_name.as_str()).into());
            }
            writes.push(ConditionalPrimaryWrite {
                primary_key,
                expected_version: RpcVersion::zero(),
                value,
                rollback_value: rollback_value.clone(),
            });
        }

        match self
            .apply_primary_writes_conditional(writes.as_slice(), "insert")
            .await?
        {
            ConditionalWriteOutcome::Applied(applied) => {
                self.optimizer_record_row_count_delta(applied as i64);
                Ok(applied)
            }
            ConditionalWriteOutcome::Conflict => {
                Err(DuplicateKeyViolation::for_table(self.table_name.as_str()).into())
            }
        }
    }

    /// Executes `write primary entries` for this component.
    async fn write_primary_entries(
        &self,
        rows: Vec<(i64, Vec<u8>)>,
        op: &'static str,
    ) -> Result<u64> {
        // Decision: evaluate `rows.is_empty()` to choose the correct SQL/storage control path.
        if rows.is_empty() {
            return Ok(0);
        }
        let row_count = rows.len() as u64;
        let retry_policy = self.write_retry_policy;
        let retry_limit = retry_policy.max_attempts();
        let retry_deadline = if retry_policy.enabled && retry_policy.max_elapsed > Duration::ZERO {
            Some(Instant::now() + retry_policy.max_elapsed)
        } else {
            None
        };
        let mut last_error: Option<anyhow::Error> = None;

        'attempts: for attempt in 0..retry_limit {
            if let Some(deadline) = retry_deadline {
                if Instant::now() >= deadline {
                    break 'attempts;
                }
            }
            let topology = match fetch_topology(&self.client)
                .await
                .with_context(|| format!("fetch topology for {op}"))
            {
                Ok(topology) => topology,
                Err(err) => {
                    let class = classify_write_error_message(err.to_string().as_str());
                    if class.is_retryable() && attempt + 1 < retry_limit {
                        last_error = Some(err);
                        tokio::time::sleep(retry_backoff(
                            retry_policy,
                            attempt,
                            now_timestamp_nanos().unsigned_abs(),
                        ))
                        .await;
                        continue 'attempts;
                    }
                    return Err(err);
                }
            };
            if let Err(err) = require_non_empty_topology(&topology) {
                let class = classify_write_error_message(err.to_string().as_str());
                if class.is_retryable() && attempt + 1 < retry_limit {
                    last_error = Some(err);
                    tokio::time::sleep(retry_backoff(
                        retry_policy,
                        attempt,
                        now_timestamp_nanos().unsigned_abs(),
                    ))
                    .await;
                    continue 'attempts;
                }
                return Err(err);
            }

            let mut writes = BTreeMap::<WriteTarget, Vec<ReplicatedWriteEntry>>::new();
            for (primary_key, value) in &rows {
                let key = self.encode_storage_primary_key(*primary_key);
                let route = match route_key(
                    &topology,
                    self.local_grpc_addr,
                    key.as_slice(),
                    &self.preferred_shards,
                ) {
                    Some(route) => route,
                    None => {
                        let err = anyhow!(
                            "no shard route found for key table={} primary_key={}",
                            self.table_name,
                            primary_key
                        );
                        let class = classify_write_error_message(err.to_string().as_str());
                        if class.is_retryable() && attempt + 1 < retry_limit {
                            last_error = Some(err);
                            tokio::time::sleep(retry_backoff(
                                retry_policy,
                                attempt,
                                now_timestamp_nanos().unsigned_abs(),
                            ))
                            .await;
                            continue 'attempts;
                        }
                        return Err(err);
                    }
                };

                let target = WriteTarget {
                    shard_index: route.shard_index,
                    grpc_addr: route.grpc_addr,
                    start_key: route.start_key,
                    end_key: route.end_key,
                };
                writes
                    .entry(target)
                    .or_default()
                    .push(ReplicatedWriteEntry {
                        key,
                        value: value.clone(),
                    });
            }
            let mut planned_bytes = 0usize;
            let mut planned_rpcs = 0usize;
            for entries in writes.values() {
                planned_bytes = planned_bytes.saturating_add(
                    entries
                        .iter()
                        .map(replicated_write_entry_size)
                        .fold(0usize, |acc, next| acc.saturating_add(next)),
                );
                planned_rpcs = planned_rpcs.saturating_add(
                    chunk_replicated_writes(
                        entries.clone(),
                        self.distributed_write_max_batch_entries,
                        self.distributed_write_max_batch_bytes,
                    )
                    .len(),
                );
            }
            self.enforce_inflight_budget(rows.len(), planned_bytes, planned_rpcs, "write_plan")?;

            let mut written = 0u64;
            let mut attempt_error: Option<anyhow::Error> = None;
            for (target, entries) in writes {
                let write_client = self.client_for(target.grpc_addr);
                let expected = entries.len() as u64;
                let chunks = chunk_replicated_writes(
                    entries,
                    self.distributed_write_max_batch_entries,
                    self.distributed_write_max_batch_bytes,
                );
                let batch_count = chunks.len();
                let mut target_written = 0u64;
                for (batch_idx, batch_entries) in chunks.into_iter().enumerate() {
                    let batch_expected = batch_entries.len() as u64;
                    let batch_bytes = batch_entries
                        .iter()
                        .map(replicated_write_entry_size)
                        .fold(0usize, |acc, next| acc.saturating_add(next));
                    if let Err(err) = self.enforce_inflight_budget(
                        batch_expected as usize,
                        batch_bytes,
                        1,
                        "write_batch",
                    ) {
                        attempt_error = Some(err);
                        break;
                    }
                    let circuit_token = match self.circuit_acquire(target.grpc_addr) {
                        Ok(token) => token,
                        Err(err) => {
                            attempt_error = Some(err);
                            break;
                        }
                    };
                    let apply_span = info_span!(
                        "holo_fusion.distributed_write_apply",
                        table = %self.table_name,
                        op = op,
                        shard_index = target.shard_index,
                        target = %target.grpc_addr,
                        batch_index = batch_idx + 1,
                        batch_count = batch_count
                    );
                    let apply_started = Instant::now();
                    match write_client
                        .range_write_latest(
                            target.shard_index,
                            target.start_key.as_slice(),
                            target.end_key.as_slice(),
                            batch_entries,
                        )
                        .instrument(apply_span)
                        .await
                    {
                        Ok(applied) => {
                            self.circuit_on_success(target.grpc_addr, circuit_token);
                            self.metrics.record_distributed_apply(
                                target.shard_index,
                                batch_expected,
                                apply_started.elapsed(),
                            );
                            if applied != batch_expected {
                                attempt_error = Some(anyhow!(
                                    "partial {op} apply on shard {} chunk {}/{}: expected {}, applied {}",
                                    target.shard_index,
                                    batch_idx + 1,
                                    batch_count,
                                    batch_expected,
                                    applied
                                ));
                                break;
                            }
                            target_written = target_written.saturating_add(applied);
                            written = written.saturating_add(applied);
                        }
                        Err(err) => {
                            self.circuit_on_failure(target.grpc_addr, circuit_token);
                            let wrapped = anyhow!(
                                "apply {op} batch table={} shard={} target={} chunk {}/{} failed: {err}",
                                self.table_name,
                                target.shard_index,
                                target.grpc_addr,
                                batch_idx + 1,
                                batch_count
                            );
                            let classification =
                                classify_write_error_message(wrapped.to_string().as_str());
                            self.metrics.record_distributed_apply_failure(
                                target.grpc_addr.to_string().as_str(),
                                classification.is_retryable(),
                            );
                            attempt_error = Some(anyhow!("{}", wrapped));
                            break;
                        }
                    }
                }
                if attempt_error.is_some() {
                    break;
                }
                if target_written != expected {
                    attempt_error = Some(anyhow!(
                        "{op} apply row count mismatch on shard {}: expected {}, written {}",
                        target.shard_index,
                        expected,
                        target_written
                    ));
                    break;
                }
            }

            if let Some(err) = attempt_error {
                let class = classify_write_error_message(err.to_string().as_str());
                last_error = Some(err);
                if class.is_retryable() && attempt + 1 < retry_limit {
                    if let Some(deadline) = retry_deadline {
                        if Instant::now() >= deadline {
                            break 'attempts;
                        }
                    }
                    tokio::time::sleep(retry_backoff(
                        retry_policy,
                        attempt,
                        now_timestamp_nanos().unsigned_abs(),
                    ))
                    .await;
                    continue;
                }
                break 'attempts;
            } else {
                if written != row_count {
                    return Err(anyhow!(
                        "{op} row count mismatch: expected {}, written {}",
                        row_count,
                        written
                    ));
                }
                return Ok(written);
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("distributed {op} write retries exhausted")))
    }

    /// Executes `client for` for this component.
    fn client_for(&self, target: SocketAddr) -> HoloStoreClient {
        // Decision: evaluate `target == self.local_grpc_addr` to choose the correct SQL/storage control path.
        if target == self.local_grpc_addr {
            self.client.clone()
        } else {
            HoloStoreClient::with_timeout(target, self.client.timeout())
        }
    }

    fn enforce_inflight_budget(
        &self,
        rows: usize,
        bytes: usize,
        rpcs: usize,
        context: &'static str,
    ) -> Result<()> {
        if rows > self.inflight_budget.max_rows
            || bytes > self.inflight_budget.max_bytes
            || rpcs > self.inflight_budget.max_rpcs
        {
            return Err(
                OverloadError::new(format!(
                    "server is overloaded: write inflight budget exceeded in {} (rows={} max_rows={} bytes={} max_bytes={} rpcs={} max_rpcs={})",
                    context,
                    rows,
                    self.inflight_budget.max_rows,
                    bytes,
                    self.inflight_budget.max_bytes,
                    rpcs,
                    self.inflight_budget.max_rpcs
                ))
                .into(),
            );
        }
        Ok(())
    }

    fn circuit_acquire(&self, target: SocketAddr) -> Result<CircuitAcquireToken> {
        if !self.circuit_breaker_config.enabled {
            return Ok(CircuitAcquireToken {
                kind: CircuitAcquireKind::Closed,
            });
        }
        let target_key = target.to_string();
        let mut by_target = self
            .circuit_by_target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = by_target.entry(target).or_default();
        let now = Instant::now();

        if state.state == CircuitState::Open {
            if let Some(open_until) = state.open_until {
                if now < open_until {
                    self.metrics.record_circuit_reject(target_key.as_str());
                    self.metrics
                        .record_circuit_state(target_key.as_str(), CircuitState::Open);
                    return Err(OverloadError::new(format!(
                        "server is overloaded: circuit breaker open for {}",
                        target
                    ))
                    .into());
                }
            }
            state.state = CircuitState::HalfOpen;
            state.half_open_probe_inflight = false;
            state.open_until = None;
            self.metrics
                .record_circuit_state(target_key.as_str(), CircuitState::HalfOpen);
        }

        if state.state == CircuitState::HalfOpen {
            if state.half_open_probe_inflight {
                self.metrics.record_circuit_reject(target_key.as_str());
                return Err(OverloadError::new(format!(
                    "server is overloaded: circuit breaker half-open for {}",
                    target
                ))
                .into());
            }
            state.half_open_probe_inflight = true;
            self.metrics
                .record_circuit_state(target_key.as_str(), CircuitState::HalfOpen);
            return Ok(CircuitAcquireToken {
                kind: CircuitAcquireKind::HalfOpenProbe,
            });
        }

        self.metrics
            .record_circuit_state(target_key.as_str(), CircuitState::Closed);
        Ok(CircuitAcquireToken {
            kind: CircuitAcquireKind::Closed,
        })
    }

    fn circuit_on_success(&self, target: SocketAddr, token: CircuitAcquireToken) {
        if !self.circuit_breaker_config.enabled {
            return;
        }
        let target_key = target.to_string();
        let mut by_target = self
            .circuit_by_target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = by_target.entry(target).or_default();
        state.consecutive_failures = 0;
        state.open_until = None;
        state.half_open_probe_inflight = false;
        if token.kind == CircuitAcquireKind::HalfOpenProbe || state.state != CircuitState::Closed {
            state.state = CircuitState::Closed;
            self.metrics
                .record_circuit_state(target_key.as_str(), CircuitState::Closed);
        }
    }

    fn circuit_on_failure(&self, target: SocketAddr, token: CircuitAcquireToken) {
        if !self.circuit_breaker_config.enabled {
            return;
        }
        let target_key = target.to_string();
        let mut by_target = self
            .circuit_by_target
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = by_target.entry(target).or_default();

        if token.kind == CircuitAcquireKind::HalfOpenProbe {
            state.state = CircuitState::Open;
            state.half_open_probe_inflight = false;
            state.open_until = Some(Instant::now() + self.circuit_breaker_config.open_duration);
            state.consecutive_failures = 0;
            self.metrics.record_circuit_open(target_key.as_str());
            return;
        }

        state.consecutive_failures = state.consecutive_failures.saturating_add(1);
        if state.consecutive_failures >= self.circuit_breaker_config.failure_threshold {
            state.state = CircuitState::Open;
            state.open_until = Some(Instant::now() + self.circuit_breaker_config.open_duration);
            state.consecutive_failures = 0;
            state.half_open_probe_inflight = false;
            self.metrics.record_circuit_open(target_key.as_str());
        }
    }
}

#[async_trait]
impl TableProvider for HoloStoreTableProvider {
    /// Executes `as any` for this component.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Executes `schema` for this component.
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Executes `table type` for this component.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Executes `scan` for this component.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let query_execution_id = self
            .metrics
            .ensure_query_execution_for_session(state.session_id(), "SCAN");
        let stage_id = self.metrics.next_stage_execution_id();

        // Decision: evaluate `matches!(limit, Some(0))` to choose the correct SQL/storage control path.
        if matches!(limit, Some(0)) {
            self.metrics.record_stage_event(
                query_execution_id.as_str(),
                stage_id,
                "scan_stage_finish",
                "rows_returned=0 limit=0",
            );
            let mem = MemTable::try_new(
                self.schema(),
                vec![vec![RecordBatch::new_empty(self.schema())]],
            )?;
            return mem.scan(state, projection, &[], limit).await;
        }

        debug!(
            query_execution_id = query_execution_id.as_str(),
            stage_id,
            table = %self.table_name,
            filters = filters.len(),
            limit = ?limit,
            projection = ?projection,
            "starting holofusion table scan"
        );

        let table_schema = self.schema();
        let (batch, mem_schema, mem_projection) = if self.row_codec_mode == RowCodecMode::RowV1 {
            let rows = self
                .scan_rows_generic_with_context(
                    query_execution_id.as_str(),
                    stage_id,
                    filters,
                    projection,
                    limit,
                )
                .await
                .map_err(df_external)?;
            if let Some(projected_columns) = projection {
                // Row-v1 decode materializes only required columns and leaves omitted columns as
                // Null placeholders. Build the scan batch on projected schema to avoid emitting
                // impossible NULLs for omitted non-nullable columns.
                let projected_schema = Arc::new(
                    table_schema
                        .project(projected_columns.as_slice())
                        .map_err(|err| DataFusionError::Execution(err.to_string()))?,
                );
                let projected_rows = project_generic_rows(rows.as_slice(), projected_columns)
                    .map_err(df_external)?;
                (
                    rows_to_batch_generic(projected_schema.clone(), projected_rows.as_slice())
                        .map_err(df_external)?,
                    projected_schema,
                    None,
                )
            } else {
                (
                    rows_to_batch_generic(table_schema.clone(), rows.as_slice())
                        .map_err(df_external)?,
                    table_schema.clone(),
                    None,
                )
            }
        } else {
            let rows = self
                .scan_rows_with_context(query_execution_id.as_str(), stage_id, filters, limit)
                .await
                .map_err(df_external)?;
            (
                rows_to_batch(table_schema.clone(), &rows).map_err(df_external)?,
                table_schema,
                projection.cloned(),
            )
        };
        let mem = MemTable::try_new(mem_schema, vec![vec![batch]])?;
        mem.scan(state, mem_projection.as_ref(), &[], limit).await
    }

    /// Executes `insert into` for this component.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.schema()
            .logically_equivalent_names_and_types(&input.schema())?;
        // Decision: evaluate `insert_op != InsertOp::Append` to choose the correct SQL/storage control path.
        if insert_op != InsertOp::Append {
            return not_impl_err!("{insert_op} is not implemented for HoloStoreTableProvider");
        }

        let sink = HoloStoreInsertSink::new(Arc::new(self.clone()));
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }

    /// Executes `supports filters pushdown` for this component.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let mut support = Vec::with_capacity(filters.len());
        for filter in filters {
            let exact_primary_key =
                extract_supported_predicates(filter, self.primary_key_column.as_str()).is_some();
            let inexact_index =
                !exact_primary_key && self.filter_supports_secondary_index_pushdown(filter);

            self.metrics
                .record_filter_support(exact_primary_key || inexact_index);
            support.push(if exact_primary_key {
                TableProviderFilterPushDown::Exact
            } else if inexact_index {
                TableProviderFilterPushDown::Inexact
            } else {
                TableProviderFilterPushDown::Unsupported
            });
        }
        Ok(support)
    }
}

#[derive(Debug, Clone)]
/// Represents the `LocalScanTarget` component used by the holo_fusion runtime.
struct LocalScanTarget {
    shard_index: usize,
    grpc_addr: SocketAddr,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ScanRangeTask {
    task_id: usize,
    range_end_key: Vec<u8>,
    target: LocalScanTarget,
}

impl From<crate::topology::ScanTarget> for LocalScanTarget {
    /// Executes `from` for this component.
    fn from(value: crate::topology::ScanTarget) -> Self {
        Self {
            shard_index: value.shard_index,
            grpc_addr: value.grpc_addr,
            start_key: value.start_key,
            end_key: value.end_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
/// Represents the `WriteTarget` component used by the holo_fusion runtime.
struct WriteTarget {
    shard_index: usize,
    grpc_addr: SocketAddr,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

#[derive(Debug, Clone)]
/// Represents the `PreparedConditionalEntry` component used by the holo_fusion runtime.
struct PreparedConditionalEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    expected_version: RpcVersion,
    applied_version: RpcVersion,
    rollback_value: Vec<u8>,
}

/// Typed scan contract sent from query planning to storage scan execution.
#[derive(Debug, Clone)]
struct ScanSpec {
    query_execution_id: String,
    stage_id: u64,
    table_id: u64,
    table_name: String,
    projected_columns: Vec<usize>,
    predicate: Option<ScanPredicateExpr>,
    limit: Option<usize>,
    bounds: PkBounds,
}

/// Typed predicate expression contract for storage-facing scans.
#[derive(Debug, Clone)]
enum ScanPredicateExpr {
    And(Vec<ScanPredicateExpr>),
    Comparison {
        column: String,
        op: ScanPredicateOp,
        value: i64,
    },
}

/// Comparison operator supported by the scan predicate contract.
#[derive(Debug, Clone, Copy)]
enum ScanPredicateOp {
    Eq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl ScanPredicateOp {
    /// Converts DataFusion operator to typed scan predicate operator.
    fn from_operator(op: Operator) -> Option<Self> {
        match op {
            Operator::Eq => Some(Self::Eq),
            Operator::Lt => Some(Self::Lt),
            Operator::LtEq => Some(Self::LtEq),
            Operator::Gt => Some(Self::Gt),
            Operator::GtEq => Some(Self::GtEq),
            _ => None,
        }
    }

    /// Renders operator as canonical SQL comparison token.
    fn as_sql(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
        }
    }
}

/// One emitted scan chunk from storage scan execution.
#[derive(Debug, Clone)]
struct ScanChunk {
    sequence: u64,
    resume_cursor: Vec<u8>,
    rows_scanned: u64,
    bytes_scanned: u64,
}

/// Aggregated scan statistics across all emitted chunks.
#[derive(Debug, Clone, Copy, Default)]
struct ScanStats {
    rpc_pages: u64,
    rows_scanned: u64,
    rows_output: u64,
    bytes_scanned: u64,
    retries: u64,
    reroutes: u64,
    chunks: u64,
    duplicate_rows_skipped: u64,
}

/// Scan result carrying raw storage entries and aggregated statistics.
#[derive(Debug, Default)]
struct ScanExecutionResult {
    entries: Vec<LatestEntry>,
    stats: ScanStats,
}

/// Batched primary-row lookup result keyed by decoded primary key.
#[derive(Debug, Default)]
struct PrimaryLookupBatchResult {
    entries_by_primary: BTreeMap<i64, LatestEntry>,
    stats: ScanStats,
}

#[derive(Debug, Clone)]
struct AggregateFilterRuntime {
    column_index: usize,
    allowed_values: Vec<ScalarValue>,
}

#[derive(Debug, Clone)]
struct AggregateGroupState {
    group_values: Vec<ScalarValue>,
    count: u64,
    sum: i128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TopKRankedGroup {
    sum: i128,
    tie_key: Vec<u8>,
}

impl Ord for TopKRankedGroup {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.sum
            .cmp(&other.sum)
            .then_with(|| self.tie_key.cmp(&other.tie_key))
    }
}

impl PartialOrd for TopKRankedGroup {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Formats typed scan predicate IR into a compact textual form.
fn format_scan_predicate_expr(expr: &ScanPredicateExpr) -> String {
    match expr {
        ScanPredicateExpr::And(terms) => terms
            .iter()
            .map(format_scan_predicate_expr)
            .collect::<Vec<_>>()
            .join(" AND "),
        ScanPredicateExpr::Comparison { column, op, value } => {
            format!("{column} {} {value}", op.as_sql())
        }
    }
}

fn finalize_grouped_topk_rows(
    groups: BTreeMap<Vec<u8>, AggregateGroupState>,
    having_min_count: u64,
    limit: usize,
) -> Result<Vec<GroupedAggregateTopKRow>> {
    if limit == 0 || groups.is_empty() {
        return Ok(Vec::new());
    }

    let mut eligible = BTreeMap::<Vec<u8>, AggregateGroupState>::new();
    let mut heap = BinaryHeap::<Reverse<TopKRankedGroup>>::new();
    for (key, state) in groups {
        if state.count < having_min_count {
            continue;
        }
        let ranked = TopKRankedGroup {
            sum: state.sum,
            tie_key: key.clone(),
        };
        eligible.insert(key, state);
        if heap.len() < limit {
            heap.push(Reverse(ranked));
            continue;
        }
        if let Some(Reverse(current_smallest)) = heap.peek() {
            if ranked > *current_smallest {
                heap.pop();
                heap.push(Reverse(ranked));
            }
        }
    }

    let mut ranked = heap
        .into_iter()
        .map(|Reverse(item)| item)
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .sum
            .cmp(&left.sum)
            .then_with(|| left.tie_key.cmp(&right.tie_key))
    });

    let mut out = Vec::<GroupedAggregateTopKRow>::with_capacity(ranked.len());
    for item in ranked {
        let Some(state) = eligible.remove(&item.tie_key) else {
            continue;
        };
        let sum = i64::try_from(state.sum).map_err(|_| {
            anyhow!(
                "aggregate SUM overflow: value {} exceeds signed 64-bit range",
                state.sum
            )
        })?;
        out.push(GroupedAggregateTopKRow {
            group_values: state.group_values,
            count: state.count,
            sum,
        });
    }
    Ok(out)
}

fn row_matches_grouped_filters(
    row_values: &[ScalarValue],
    filters: &[AggregateFilterRuntime],
) -> bool {
    filters.iter().all(|filter| {
        let Some(value) = row_values.get(filter.column_index) else {
            return false;
        };
        if scalar_is_nullish(value) {
            return false;
        }
        filter
            .allowed_values
            .iter()
            .any(|allowed| scalar_values_equal(value, allowed))
    })
}

fn scalar_values_equal(left: &ScalarValue, right: &ScalarValue) -> bool {
    if scalar_is_nullish(left) || scalar_is_nullish(right) {
        return false;
    }
    if let (Some(l), Some(r)) = (
        scalar_to_i128_for_filter_compare(left),
        scalar_to_i128_for_filter_compare(right),
    ) {
        return l == r;
    }

    match (left, right) {
        (ScalarValue::Utf8(Some(l)), ScalarValue::Utf8(Some(r)))
        | (ScalarValue::LargeUtf8(Some(l)), ScalarValue::LargeUtf8(Some(r)))
        | (ScalarValue::Utf8(Some(l)), ScalarValue::LargeUtf8(Some(r)))
        | (ScalarValue::LargeUtf8(Some(l)), ScalarValue::Utf8(Some(r))) => l == r,
        (ScalarValue::Boolean(Some(l)), ScalarValue::Boolean(Some(r))) => l == r,
        _ => left == right,
    }
}

fn scalar_to_i128_for_filter_compare(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(i128::from(*v)),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(i128::from(*v)),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(i128::from(*v)),
        ScalarValue::TimestampSecond(Some(v), _) => Some(i128::from(*v)),
        _ => None,
    }
}

fn scalar_is_nullish(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Null
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::UInt8(None)
            | ScalarValue::UInt16(None)
            | ScalarValue::UInt32(None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Float32(None)
            | ScalarValue::Float64(None)
            | ScalarValue::Boolean(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::TimestampNanosecond(None, _)
            | ScalarValue::TimestampMicrosecond(None, _)
            | ScalarValue::TimestampMillisecond(None, _)
            | ScalarValue::TimestampSecond(None, _)
    )
}

fn aggregate_grouped_row(
    groups: &mut BTreeMap<Vec<u8>, AggregateGroupState>,
    columns: &[TableColumnRecord],
    group_indexes: &[usize],
    sum_column_index: usize,
    row_values: &[ScalarValue],
) -> Result<()> {
    let mut group_values = Vec::<ScalarValue>::with_capacity(group_indexes.len());
    let mut group_key = Vec::<u8>::new();
    group_key.push(0x47);

    for column_index in group_indexes {
        let value = row_values
            .get(*column_index)
            .cloned()
            .unwrap_or(ScalarValue::Null);
        group_values.push(value.clone());
        let payload = encode_scalar_payload(value, &columns[*column_index])?;
        match payload {
            Some(payload) => {
                group_key.push(1);
                group_key.extend_from_slice(&(payload.len() as u32).to_be_bytes());
                group_key.extend_from_slice(payload.as_slice());
            }
            None => {
                group_key.push(0);
            }
        }
    }

    let Some(sum_value) = row_values.get(sum_column_index) else {
        return Err(anyhow!(
            "aggregate SUM column index {} out of bounds",
            sum_column_index
        ));
    };
    let row_sum = if scalar_is_nullish(sum_value) {
        0i128
    } else {
        let parsed = scalar_to_i64(sum_value.clone()).ok_or_else(|| {
            anyhow!(
                "aggregate SUM column '{}' has non-numeric value",
                columns[sum_column_index].name
            )
        })?;
        i128::from(parsed)
    };

    let entry = groups
        .entry(group_key)
        .or_insert_with(|| AggregateGroupState {
            group_values,
            count: 0,
            sum: 0,
        });
    entry.count = entry.count.saturating_add(1);
    entry.sum = entry
        .sum
        .checked_add(row_sum)
        .ok_or_else(|| anyhow!("aggregate SUM overflow while accumulating grouped results"))?;
    Ok(())
}

fn merge_aggregate_group_maps(
    global: &mut BTreeMap<Vec<u8>, AggregateGroupState>,
    local: BTreeMap<Vec<u8>, AggregateGroupState>,
) -> Result<()> {
    for (key, local_state) in local {
        let global_state = global.entry(key).or_insert_with(|| AggregateGroupState {
            group_values: local_state.group_values.clone(),
            count: 0,
            sum: 0,
        });
        global_state.count = global_state.count.saturating_add(local_state.count);
        global_state.sum = global_state
            .sum
            .checked_add(local_state.sum)
            .ok_or_else(|| anyhow!("aggregate SUM overflow while merging shard partials"))?;
    }
    Ok(())
}

#[derive(Debug)]
/// Represents the `HoloStoreInsertSink` component used by the holo_fusion runtime.
struct HoloStoreInsertSink {
    provider: Arc<HoloStoreTableProvider>,
    schema: SchemaRef,
}

impl HoloStoreInsertSink {
    /// Executes `new` for this component.
    fn new(provider: Arc<HoloStoreTableProvider>) -> Self {
        Self {
            schema: provider.schema(),
            provider,
        }
    }
}

impl DisplayAs for HoloStoreInsertSink {
    /// Executes `fmt as` for this component.
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Decision: evaluate `t` to choose the correct SQL/storage control path.
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "HoloStoreInsertSink(table={})", self.provider.table_name)
            }
            DisplayFormatType::TreeRender => write!(f, ""),
        }
    }
}

#[async_trait]
impl DataSink for HoloStoreInsertSink {
    /// Executes `as any` for this component.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Executes `schema` for this component.
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Executes `write all` for this component.
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        self.provider
            .write_streaming_insert(&mut data)
            .await
            .map_err(df_external)
    }
}

/// Executes `rows to batch` for this component.
fn rows_to_batch(schema: SchemaRef, rows: &[OrdersSeedRow]) -> Result<RecordBatch> {
    let mut order_id = Int64Builder::new();
    let mut customer_id = Int64Builder::new();
    let mut status = StringBuilder::new();
    let mut total_cents = Int64Builder::new();
    let mut created_at = TimestampNanosecondBuilder::new();

    for row in rows {
        order_id.append_value(row.order_id);
        customer_id.append_value(row.customer_id);
        // Decision: evaluate `row.status.as_ref()` to choose the correct SQL/storage control path.
        match row.status.as_ref() {
            Some(value) => status.append_value(value),
            None => status.append_null(),
        }
        total_cents.append_value(row.total_cents);
        created_at.append_value(row.created_at_ns);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(order_id.finish()),
        Arc::new(customer_id.finish()),
        Arc::new(status.finish()),
        Arc::new(total_cents.finish()),
        Arc::new(created_at.finish()),
    ];

    RecordBatch::try_new(schema, arrays).map_err(Into::into)
}

/// Executes `rows to batch generic` for this component.
fn rows_to_batch_generic(schema: SchemaRef, rows: &[Vec<ScalarValue>]) -> Result<RecordBatch> {
    let field_count = schema.fields().len();
    for row in rows {
        if row.len() != field_count {
            return Err(anyhow!(
                "generic scan row has {} columns but schema has {}",
                row.len(),
                field_count
            ));
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(field_count);
    for col_idx in 0..field_count {
        let field = schema.field(col_idx);
        let array: ArrayRef = match field.data_type() {
            DataType::Int8 => {
                let mut builder = Int8Builder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::Int8(Some(v)) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::Int8(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected Int8, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int16 => {
                let mut builder = Int16Builder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::Int16(Some(v)) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::Int16(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected Int16, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::Int32(Some(v)) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::Int32(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected Int32, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::Int64(Some(v)) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::Int64(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected Int64, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::UInt8 => {
                let mut builder = UInt8Builder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::UInt8(Some(v)) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::UInt8(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected UInt8, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::UInt16 => {
                let mut builder = UInt16Builder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::UInt16(Some(v)) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::UInt16(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected UInt16, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::UInt32 => {
                let mut builder = UInt32Builder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::UInt32(Some(v)) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::UInt32(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected UInt32, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::UInt64 => {
                let mut builder = UInt64Builder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::UInt64(Some(v)) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::UInt64(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected UInt64, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::Float64(Some(v)) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::Float64(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected Float64, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::Boolean(Some(v)) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::Boolean(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected Boolean, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                            builder.append_value(v)
                        }
                        ScalarValue::Null
                        | ScalarValue::Utf8(None)
                        | ScalarValue::LargeUtf8(None) => builder.append_null(),
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected Utf8, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let mut builder = TimestampNanosecondBuilder::new();
                for row in rows {
                    match row[col_idx].clone() {
                        ScalarValue::TimestampNanosecond(Some(v), _) => builder.append_value(v),
                        ScalarValue::Null | ScalarValue::TimestampNanosecond(None, _) => {
                            builder.append_null()
                        }
                        other => {
                            return Err(anyhow!(
                                "row value type mismatch for column '{}': expected TimestampNanosecond, got {other:?}",
                                field.name()
                            ));
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            other => {
                return Err(anyhow!(
                    "unsupported field type '{}' for column '{}' in row_v1 table",
                    other,
                    field.name()
                ));
            }
        };
        arrays.push(array);
    }

    RecordBatch::try_new(schema, arrays).map_err(Into::into)
}

/// Projects decoded generic rows onto a target schema column order.
fn project_generic_rows(
    rows: &[Vec<ScalarValue>],
    projected_columns: &[usize],
) -> Result<Vec<Vec<ScalarValue>>> {
    let mut projected_rows = Vec::with_capacity(rows.len());
    for (row_idx, row) in rows.iter().enumerate() {
        let mut projected_row = Vec::with_capacity(projected_columns.len());
        for projected_column in projected_columns {
            let value = row.get(*projected_column).ok_or_else(|| {
                anyhow!(
                    "projection index {} out of bounds for row {} with {} columns",
                    projected_column,
                    row_idx,
                    row.len()
                )
            })?;
            projected_row.push(value.clone());
        }
        projected_rows.push(projected_row);
    }
    Ok(projected_rows)
}

/// Executes `decode orders rows` for this component.
fn decode_orders_rows(batch: &RecordBatch) -> Result<Vec<OrdersSeedRow>> {
    // Decision: evaluate `batch.num_columns() != 5` to choose the correct SQL/storage control path.
    if batch.num_columns() != 5 {
        return Err(anyhow!(
            "orders insert expects 5 columns, got {}",
            batch.num_columns()
        ));
    }

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let order_id = scalar_to_required_i64(
            ScalarValue::try_from_array(batch.column(0).as_ref(), row_idx)?,
            "order_id",
        )?;
        let customer_id = scalar_to_required_i64(
            ScalarValue::try_from_array(batch.column(1).as_ref(), row_idx)?,
            "customer_id",
        )?;
        let status = scalar_to_optional_string(ScalarValue::try_from_array(
            batch.column(2).as_ref(),
            row_idx,
        )?)?;
        let total_cents = scalar_to_required_i64(
            ScalarValue::try_from_array(batch.column(3).as_ref(), row_idx)?,
            "total_cents",
        )?;
        let created_at_ns = scalar_to_required_timestamp_ns(
            ScalarValue::try_from_array(batch.column(4).as_ref(), row_idx)?,
            "created_at",
        )?;

        rows.push(OrdersSeedRow {
            order_id,
            customer_id,
            status,
            total_cents,
            created_at_ns,
        });
    }
    Ok(rows)
}

/// Executes `decode generic rows` for this component.
fn decode_generic_rows(
    batch: &RecordBatch,
    columns: &[TableColumnRecord],
    checks: &[TableCheckConstraintRecord],
    primary_key_index: usize,
) -> Result<Vec<(i64, Vec<u8>)>> {
    if primary_key_index >= columns.len() {
        return Err(anyhow!(
            "primary key index {} is out of bounds for {} columns",
            primary_key_index,
            columns.len()
        ));
    }

    let mut metadata_index_by_name = BTreeMap::<String, usize>::new();
    for (idx, column) in columns.iter().enumerate() {
        metadata_index_by_name.insert(column.name.to_ascii_lowercase(), idx);
    }

    let mut input_index_by_metadata = vec![None; columns.len()];
    for (input_idx, field) in batch.schema().fields().iter().enumerate() {
        let normalized = field.name().to_ascii_lowercase();
        let Some(metadata_idx) = metadata_index_by_name.get(normalized.as_str()).copied() else {
            return Err(anyhow!(
                "row_v1 insert references unknown column '{}' for table schema",
                field.name()
            ));
        };
        if input_index_by_metadata[metadata_idx].is_some() {
            return Err(anyhow!(
                "row_v1 insert contains duplicate column '{}' in input schema",
                field.name()
            ));
        }
        input_index_by_metadata[metadata_idx] = Some(input_idx);
    }

    let mut rows = Vec::with_capacity(batch.num_rows());
    let now_timestamp_ns = now_timestamp_nanos();
    for row_idx in 0..batch.num_rows() {
        let mut values = vec![ScalarValue::Null; columns.len()];
        let mut missing_mask = vec![true; columns.len()];
        let mut primary_key: Option<i64> = None;
        for (col_idx, column) in columns.iter().enumerate() {
            if let Some(input_idx) = input_index_by_metadata[col_idx] {
                values[col_idx] =
                    ScalarValue::try_from_array(batch.column(input_idx).as_ref(), row_idx)?;
                missing_mask[col_idx] = false;
            }
            if col_idx == primary_key_index && !matches!(values[col_idx], ScalarValue::Null) {
                let key = scalar_to_primary_key_i64(values[col_idx].clone(), column)?;
                primary_key = Some(key);
            }
        }

        apply_column_defaults_for_missing(
            columns,
            values.as_mut_slice(),
            missing_mask.as_slice(),
            now_timestamp_ns,
        )
        .map_err(row_constraint_to_anyhow)?;
        validate_row_against_metadata(columns, checks, values.as_slice())
            .map_err(row_constraint_to_anyhow)?;

        if primary_key.is_none() {
            let column = &columns[primary_key_index];
            let key = scalar_to_primary_key_i64(values[primary_key_index].clone(), column)?;
            primary_key = Some(key);
        }

        let mut encoded = Vec::with_capacity(columns.len());
        for (column, value) in columns.iter().zip(values.iter()) {
            let payload = encode_scalar_payload(value.clone(), column)?;
            encoded.push(payload);
        }
        let primary_key = primary_key.ok_or_else(|| anyhow!("missing primary key value"))?;
        rows.push((primary_key, encode_generic_row_value(encoded)));
    }
    Ok(rows)
}

fn now_timestamp_nanos() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration
            .as_nanos()
            .min(i64::MAX as u128)
            .try_into()
            .unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

fn now_unix_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(u64::MAX as u128) as u64,
        Err(_) => 0,
    }
}

fn format_access_path(path: &AccessPath) -> String {
    match path {
        AccessPath::TableScan => "table_scan".to_string(),
        AccessPath::Index {
            index_name,
            prefix,
            index_only_table_covering,
            ordered_limit_scan,
            probe_count,
        } => format!(
            "index_scan[index={} prefix_columns={} probes={} covering={} ordered={}]",
            index_name,
            prefix.len(),
            probe_count,
            index_only_table_covering,
            ordered_limit_scan
        ),
    }
}

fn format_optimizer_predicate_summary(predicates: &PredicateSummary) -> String {
    let mut recognized_columns = predicates
        .columns()
        .filter_map(|(column, predicate)| format_column_predicate_summary(column, predicate))
        .collect::<Vec<_>>();
    recognized_columns.sort();
    let columns = if recognized_columns.is_empty() {
        "none".to_string()
    } else {
        recognized_columns.join(",")
    };

    format!(
        "total_terms={} recognized_terms={} residual_terms={} recognized_columns={}",
        predicates.total_terms, predicates.recognized_terms, predicates.residual_terms, columns
    )
}

fn format_column_predicate_summary(column: &str, predicate: &ColumnPredicate) -> Option<String> {
    if !predicate.has_any_constraint() {
        return None;
    }

    let mut terms = Vec::<String>::new();
    if !predicate.equals.is_empty() {
        terms.push(format!("eq{}", predicate.equals.len()));
    }
    if let Some(lower) = predicate.lower.as_ref() {
        terms.push(if lower.inclusive {
            "ge".to_string()
        } else {
            "gt".to_string()
        });
    }
    if let Some(upper) = predicate.upper.as_ref() {
        terms.push(if upper.inclusive {
            "le".to_string()
        } else {
            "lt".to_string()
        });
    }
    if predicate.is_null {
        terms.push("isnull".to_string());
    }
    if predicate.is_not_null {
        terms.push("notnull".to_string());
    }

    Some(format!("{column}:{}", terms.join("+")))
}

fn optimizer_candidate_name_for_access_path(path: &AccessPath) -> String {
    match path {
        AccessPath::TableScan => "table_scan".to_string(),
        AccessPath::Index {
            index_name,
            index_only_table_covering,
            ordered_limit_scan,
            ..
        } => {
            let prefix = if *index_only_table_covering {
                "index_only"
            } else if *ordered_limit_scan {
                "ordered_index"
            } else {
                "index_lookup"
            };
            format!("{prefix}:{}", index_name.to_ascii_lowercase())
        }
    }
}

fn summary_supports_secondary_index_pushdown(
    summary: &PredicateSummary,
    indexes: &[&SecondaryIndexRecord],
) -> bool {
    if summary.recognized_terms == 0 {
        return false;
    }

    indexes.iter().any(|index| {
        index.key_columns.iter().any(|key_column| {
            summary
                .column(key_column.to_ascii_lowercase().as_str())
                .is_some_and(column_predicate_supports_secondary_index_pushdown)
        })
    })
}

fn column_predicate_supports_secondary_index_pushdown(predicate: &ColumnPredicate) -> bool {
    !predicate.equals.is_empty()
        || predicate.lower.is_some()
        || predicate.upper.is_some()
        || predicate.is_null
}

fn index_covers_required_columns(
    index: &SecondaryIndexRecord,
    primary_key_column: &str,
    required_columns: &BTreeSet<String>,
) -> bool {
    if required_columns.is_empty() {
        return false;
    }

    let mut covered = BTreeSet::<String>::new();
    covered.insert(primary_key_column.to_ascii_lowercase());
    for column in &index.key_columns {
        covered.insert(column.to_ascii_lowercase());
    }
    for column in &index.include_columns {
        covered.insert(column.to_ascii_lowercase());
    }
    required_columns
        .iter()
        .all(|column| covered.contains(&column.to_ascii_lowercase()))
}

fn expand_scalar_combinations(
    value_matrix: &[Vec<ScalarValue>],
    depth: usize,
    current: &mut Vec<ScalarValue>,
    max_output: usize,
    out: &mut Vec<Vec<ScalarValue>>,
) {
    if out.len() >= max_output {
        return;
    }
    if depth >= value_matrix.len() {
        out.push(current.clone());
        return;
    }
    for value in &value_matrix[depth] {
        if out.len() >= max_output {
            break;
        }
        current.push(value.clone());
        expand_scalar_combinations(value_matrix, depth + 1, current, max_output, out);
        current.pop();
    }
}

fn row_constraint_to_anyhow(err: crate::metadata::RowConstraintError) -> anyhow::Error {
    anyhow!("[{}] {}", err.sqlstate(), err.message())
}

/// Executes `scalar to required i64` for this component.
fn scalar_to_required_i64(value: ScalarValue, field: &str) -> Result<i64> {
    scalar_to_i64(value).ok_or_else(|| anyhow!("invalid {field} value type for int64"))
}

/// Executes `scalar to i64` for this component.
fn scalar_to_i64(value: ScalarValue) -> Option<i64> {
    // Decision: evaluate `value` to choose the correct SQL/storage control path.
    match value {
        ScalarValue::Int64(v) => v,
        ScalarValue::Int32(v) => v.map(i64::from),
        ScalarValue::Int16(v) => v.map(i64::from),
        ScalarValue::Int8(v) => v.map(i64::from),
        ScalarValue::UInt64(v) => v.and_then(|v| i64::try_from(v).ok()),
        ScalarValue::UInt32(v) => v.map(i64::from),
        ScalarValue::UInt16(v) => v.map(i64::from),
        ScalarValue::UInt8(v) => v.map(i64::from),
        _ => None,
    }
}

/// Executes `scalar to optional string` for this component.
fn scalar_to_optional_string(value: ScalarValue) -> Result<Option<String>> {
    // Decision: evaluate `value` to choose the correct SQL/storage control path.
    match value {
        ScalarValue::Utf8(v) => Ok(v),
        ScalarValue::LargeUtf8(v) => Ok(v),
        ScalarValue::Null => Ok(None),
        _ => Err(anyhow!("invalid status value type for utf8")),
    }
}

/// Executes `scalar to required timestamp ns` for this component.
fn scalar_to_required_timestamp_ns(value: ScalarValue, field: &str) -> Result<i64> {
    // Decision: evaluate `value` to choose the correct SQL/storage control path.
    match value {
        ScalarValue::TimestampNanosecond(v, _) => {
            v.ok_or_else(|| anyhow!("null value for required field {field}"))
        }
        ScalarValue::TimestampMicrosecond(v, _) => v
            .map(|v| v.saturating_mul(1_000))
            .ok_or_else(|| anyhow!("null value for required field {field}")),
        ScalarValue::TimestampMillisecond(v, _) => v
            .map(|v| v.saturating_mul(1_000_000))
            .ok_or_else(|| anyhow!("null value for required field {field}")),
        ScalarValue::TimestampSecond(v, _) => v
            .map(|v| v.saturating_mul(1_000_000_000))
            .ok_or_else(|| anyhow!("null value for required field {field}")),
        _ => Err(anyhow!("invalid {field} value type for timestamp")),
    }
}

/// Encodes one generic row payload in row_v1 format.
fn encode_generic_row_value(payloads: Vec<Option<Vec<u8>>>) -> Vec<u8> {
    let column_count = payloads.len() as u16;
    let null_bitmap_len = payloads.len().div_ceil(8);
    let mut null_bitmap = vec![0u8; null_bitmap_len];
    for (idx, payload) in payloads.iter().enumerate() {
        if payload.is_none() {
            let byte_idx = idx / 8;
            let bit_idx = idx % 8;
            null_bitmap[byte_idx] |= 1u8 << bit_idx;
        }
    }

    let mut out = Vec::new();
    out.push(ROW_FORMAT_VERSION_V2);
    out.push(0);
    out.extend_from_slice(&column_count.to_be_bytes());
    out.extend_from_slice(&(null_bitmap_len as u16).to_be_bytes());
    out.extend_from_slice(&null_bitmap);
    for payload in payloads {
        if let Some(payload) = payload {
            out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            out.extend_from_slice(&payload);
        }
    }
    out
}

/// Encodes one typed scalar payload according to metadata column type.
fn encode_scalar_payload(
    value: ScalarValue,
    column: &TableColumnRecord,
) -> Result<Option<Vec<u8>>> {
    if matches!(value, ScalarValue::Null) {
        if !column.nullable {
            return Err(anyhow!(
                "null value violates not-null constraint for column '{}'",
                column.name
            ));
        }
        return Ok(None);
    }

    let payload = match column.column_type {
        TableColumnType::Int8 => scalar_to_i64(value)
            .and_then(|v| i8::try_from(v).ok())
            .map(|v| vec![v as u8])
            .ok_or_else(|| anyhow!("invalid value type for Int8 column '{}'", column.name))?,
        TableColumnType::Int16 => scalar_to_i64(value)
            .and_then(|v| i16::try_from(v).ok())
            .map(|v| v.to_be_bytes().to_vec())
            .ok_or_else(|| anyhow!("invalid value type for Int16 column '{}'", column.name))?,
        TableColumnType::Int32 => scalar_to_i64(value)
            .and_then(|v| i32::try_from(v).ok())
            .map(|v| v.to_be_bytes().to_vec())
            .ok_or_else(|| anyhow!("invalid value type for Int32 column '{}'", column.name))?,
        TableColumnType::Int64 => scalar_to_i64(value)
            .map(|v| v.to_be_bytes().to_vec())
            .ok_or_else(|| anyhow!("invalid value type for Int64 column '{}'", column.name))?,
        TableColumnType::UInt8 => scalar_to_i64(value)
            .and_then(|v| u8::try_from(v).ok())
            .map(|v| vec![v])
            .ok_or_else(|| anyhow!("invalid value type for UInt8 column '{}'", column.name))?,
        TableColumnType::UInt16 => scalar_to_i64(value)
            .and_then(|v| u16::try_from(v).ok())
            .map(|v| v.to_be_bytes().to_vec())
            .ok_or_else(|| anyhow!("invalid value type for UInt16 column '{}'", column.name))?,
        TableColumnType::UInt32 => scalar_to_i64(value)
            .and_then(|v| u32::try_from(v).ok())
            .map(|v| v.to_be_bytes().to_vec())
            .ok_or_else(|| anyhow!("invalid value type for UInt32 column '{}'", column.name))?,
        TableColumnType::UInt64 => match value {
            ScalarValue::UInt64(Some(v)) => v.to_be_bytes().to_vec(),
            ScalarValue::UInt32(Some(v)) => (v as u64).to_be_bytes().to_vec(),
            ScalarValue::UInt16(Some(v)) => (v as u64).to_be_bytes().to_vec(),
            ScalarValue::UInt8(Some(v)) => (v as u64).to_be_bytes().to_vec(),
            ScalarValue::Int64(Some(v)) if v >= 0 => (v as u64).to_be_bytes().to_vec(),
            ScalarValue::Int32(Some(v)) if v >= 0 => (v as u64).to_be_bytes().to_vec(),
            ScalarValue::Int16(Some(v)) if v >= 0 => (v as u64).to_be_bytes().to_vec(),
            ScalarValue::Int8(Some(v)) if v >= 0 => (v as u64).to_be_bytes().to_vec(),
            _ => {
                return Err(anyhow!(
                    "invalid value type for UInt64 column '{}'",
                    column.name
                ))
            }
        },
        TableColumnType::Float64 => match value {
            ScalarValue::Float64(Some(v)) => v.to_be_bytes().to_vec(),
            ScalarValue::Float32(Some(v)) => (v as f64).to_be_bytes().to_vec(),
            ScalarValue::Int64(Some(v)) => (v as f64).to_be_bytes().to_vec(),
            ScalarValue::Int32(Some(v)) => (v as f64).to_be_bytes().to_vec(),
            ScalarValue::Int16(Some(v)) => (v as f64).to_be_bytes().to_vec(),
            ScalarValue::Int8(Some(v)) => (v as f64).to_be_bytes().to_vec(),
            _ => {
                return Err(anyhow!(
                    "invalid value type for Float64 column '{}'",
                    column.name
                ))
            }
        },
        TableColumnType::Boolean => match value {
            ScalarValue::Boolean(Some(v)) => vec![u8::from(v)],
            _ => {
                return Err(anyhow!(
                    "invalid value type for Boolean column '{}'",
                    column.name
                ))
            }
        },
        TableColumnType::Utf8 => match value {
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => v.into_bytes(),
            _ => {
                return Err(anyhow!(
                    "invalid value type for Utf8 column '{}'",
                    column.name
                ))
            }
        },
        TableColumnType::TimestampNanosecond => {
            let nanos = scalar_to_required_timestamp_ns(value, column.name.as_str())?;
            nanos.to_be_bytes().to_vec()
        }
    };
    Ok(Some(payload))
}

/// Converts one scalar into normalized `i64` primary-key value.
fn scalar_to_primary_key_i64(value: ScalarValue, column: &TableColumnRecord) -> Result<i64> {
    let value = scalar_to_i64(value).ok_or_else(|| {
        anyhow!(
            "invalid primary key value type for column '{}'",
            column.name
        )
    })?;
    match column.column_type {
        TableColumnType::Int8 => i8::try_from(value).map(|v| i64::from(v)).map_err(|_| {
            anyhow!(
                "primary key value out of range for Int8 column '{}'",
                column.name
            )
        }),
        TableColumnType::Int16 => i16::try_from(value).map(|v| i64::from(v)).map_err(|_| {
            anyhow!(
                "primary key value out of range for Int16 column '{}'",
                column.name
            )
        }),
        TableColumnType::Int32 => i32::try_from(value).map(|v| i64::from(v)).map_err(|_| {
            anyhow!(
                "primary key value out of range for Int32 column '{}'",
                column.name
            )
        }),
        TableColumnType::Int64 => Ok(value),
        _ => Err(anyhow!(
            "primary key column '{}' must be a signed integer",
            column.name
        )),
    }
}

/// Decodes one storage entry for a generic row_v1 table.
fn decode_generic_entry_with_version(
    expected_table_id: u64,
    columns: &[TableColumnRecord],
    primary_key_index: usize,
    entry: &LatestEntry,
) -> Result<Option<VersionedGenericRow>> {
    let primary_key = decode_primary_key(entry.key.as_slice(), expected_table_id)?;
    let decoded = decode_generic_row_value(
        entry.value.as_slice(),
        columns,
        primary_key_index,
        primary_key,
    )?;
    Ok(decoded.map(|values| VersionedGenericRow {
        primary_key,
        values,
        version: entry.version,
    }))
}

/// Decodes one storage entry and materializes only required columns.
fn decode_generic_entry_with_version_projected(
    expected_table_id: u64,
    columns: &[TableColumnRecord],
    primary_key_index: usize,
    required_column_indexes: &BTreeSet<usize>,
    entry: &LatestEntry,
) -> Result<Option<VersionedGenericRow>> {
    let primary_key = decode_primary_key(entry.key.as_slice(), expected_table_id)?;
    let decoded = decode_generic_row_value_projected(
        entry.value.as_slice(),
        columns,
        primary_key_index,
        primary_key,
        required_column_indexes,
    )?;
    Ok(decoded.map(|values| VersionedGenericRow {
        primary_key,
        values,
        version: entry.version,
    }))
}

/// Decodes generic row_v1 payload into typed scalar values.
fn decode_generic_row_value(
    bytes: &[u8],
    columns: &[TableColumnRecord],
    primary_key_index: usize,
    primary_key: i64,
) -> Result<Option<Vec<ScalarValue>>> {
    if bytes.len() < 2 {
        return Err(anyhow!("row value too short"));
    }

    let format = bytes[0];
    if format != ROW_FORMAT_VERSION_V2 {
        return Err(anyhow!(
            "unsupported row format version {format} for row_v1"
        ));
    }
    let flags = bytes[1];
    if flags & ROW_FLAG_TOMBSTONE != 0 {
        return Ok(None);
    }

    let mut cursor = 2usize;
    let column_count = read_u16(bytes, &mut cursor)? as usize;
    if column_count != columns.len() {
        return Err(anyhow!(
            "row_v1 column count mismatch: payload={}, metadata={}",
            column_count,
            columns.len()
        ));
    }
    let null_bitmap_len = read_u16(bytes, &mut cursor)? as usize;
    let null_bitmap = read_bytes(bytes, &mut cursor, null_bitmap_len)?;

    let mut payloads: Vec<Option<Vec<u8>>> = Vec::with_capacity(column_count);
    for column_idx in 0..column_count {
        if is_null(null_bitmap, column_idx) {
            payloads.push(None);
            continue;
        }
        let payload_len = read_u32(bytes, &mut cursor)? as usize;
        let payload = read_bytes(bytes, &mut cursor, payload_len)?;
        payloads.push(Some(payload.to_vec()));
    }

    let mut values = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        if idx == primary_key_index {
            values.push(primary_key_scalar(column, primary_key)?);
            continue;
        }
        values.push(payload_to_scalar(payloads[idx].as_deref(), column)?);
    }
    Ok(Some(values))
}

/// Decodes generic row_v1 payload while only materializing required columns.
fn decode_generic_row_value_projected(
    bytes: &[u8],
    columns: &[TableColumnRecord],
    primary_key_index: usize,
    primary_key: i64,
    required_column_indexes: &BTreeSet<usize>,
) -> Result<Option<Vec<ScalarValue>>> {
    if bytes.len() < 2 {
        return Err(anyhow!("row value too short"));
    }

    let format = bytes[0];
    if format != ROW_FORMAT_VERSION_V2 {
        return Err(anyhow!(
            "unsupported row format version {format} for row_v1"
        ));
    }
    let flags = bytes[1];
    if flags & ROW_FLAG_TOMBSTONE != 0 {
        return Ok(None);
    }

    let mut cursor = 2usize;
    let column_count = read_u16(bytes, &mut cursor)? as usize;
    if column_count != columns.len() {
        return Err(anyhow!(
            "row_v1 column count mismatch: payload={}, metadata={}",
            column_count,
            columns.len()
        ));
    }
    let null_bitmap_len = read_u16(bytes, &mut cursor)? as usize;
    let null_bitmap = read_bytes(bytes, &mut cursor, null_bitmap_len)?;

    let mut values = vec![ScalarValue::Null; columns.len()];
    values[primary_key_index] = primary_key_scalar(&columns[primary_key_index], primary_key)?;

    let materialized =
        |idx: usize| -> bool { idx == primary_key_index || required_column_indexes.contains(&idx) };

    for column_idx in 0..column_count {
        let is_null_value = is_null(null_bitmap, column_idx);
        if is_null_value {
            if materialized(column_idx) && column_idx != primary_key_index {
                values[column_idx] = payload_to_scalar(None, &columns[column_idx])?;
            }
            continue;
        }
        let payload_len = read_u32(bytes, &mut cursor)? as usize;
        let payload = read_bytes(bytes, &mut cursor, payload_len)?;
        if materialized(column_idx) && column_idx != primary_key_index {
            values[column_idx] = payload_to_scalar(Some(payload), &columns[column_idx])?;
        }
    }

    Ok(Some(values))
}

/// Converts one persisted payload into typed ScalarValue.
fn payload_to_scalar(payload: Option<&[u8]>, column: &TableColumnRecord) -> Result<ScalarValue> {
    let Some(payload) = payload else {
        if column.nullable {
            return Ok(ScalarValue::Null);
        }
        return Err(anyhow!(
            "missing non-nullable payload for column '{}'",
            column.name
        ));
    };

    match column.column_type {
        TableColumnType::Int8 => {
            if payload.len() != 1 {
                return Err(anyhow!("invalid Int8 payload length for '{}'", column.name));
            }
            Ok(ScalarValue::Int8(Some(payload[0] as i8)))
        }
        TableColumnType::Int16 => {
            if payload.len() != 2 {
                return Err(anyhow!(
                    "invalid Int16 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 2];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::Int16(Some(i16::from_be_bytes(bytes))))
        }
        TableColumnType::Int32 => {
            if payload.len() != 4 {
                return Err(anyhow!(
                    "invalid Int32 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::Int32(Some(i32::from_be_bytes(bytes))))
        }
        TableColumnType::Int64 => {
            if payload.len() != 8 {
                return Err(anyhow!(
                    "invalid Int64 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::Int64(Some(i64::from_be_bytes(bytes))))
        }
        TableColumnType::UInt8 => {
            if payload.len() != 1 {
                return Err(anyhow!(
                    "invalid UInt8 payload length for '{}'",
                    column.name
                ));
            }
            Ok(ScalarValue::UInt8(Some(payload[0])))
        }
        TableColumnType::UInt16 => {
            if payload.len() != 2 {
                return Err(anyhow!(
                    "invalid UInt16 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 2];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::UInt16(Some(u16::from_be_bytes(bytes))))
        }
        TableColumnType::UInt32 => {
            if payload.len() != 4 {
                return Err(anyhow!(
                    "invalid UInt32 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::UInt32(Some(u32::from_be_bytes(bytes))))
        }
        TableColumnType::UInt64 => {
            if payload.len() != 8 {
                return Err(anyhow!(
                    "invalid UInt64 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::UInt64(Some(u64::from_be_bytes(bytes))))
        }
        TableColumnType::Float64 => {
            if payload.len() != 8 {
                return Err(anyhow!(
                    "invalid Float64 payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::Float64(Some(f64::from_be_bytes(bytes))))
        }
        TableColumnType::Boolean => {
            if payload.len() != 1 {
                return Err(anyhow!(
                    "invalid Boolean payload length for '{}'",
                    column.name
                ));
            }
            Ok(ScalarValue::Boolean(Some(payload[0] != 0)))
        }
        TableColumnType::Utf8 => {
            let value = std::str::from_utf8(payload)
                .with_context(|| format!("invalid Utf8 payload for '{}'", column.name))?;
            Ok(ScalarValue::Utf8(Some(value.to_string())))
        }
        TableColumnType::TimestampNanosecond => {
            if payload.len() != 8 {
                return Err(anyhow!(
                    "invalid Timestamp payload length for '{}'",
                    column.name
                ));
            }
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(payload);
            Ok(ScalarValue::TimestampNanosecond(
                Some(i64::from_be_bytes(bytes)),
                None,
            ))
        }
    }
}

/// Converts normalized primary-key i64 into the column's scalar variant.
fn primary_key_scalar(column: &TableColumnRecord, value: i64) -> Result<ScalarValue> {
    match column.column_type {
        TableColumnType::Int8 => i8::try_from(value)
            .map(|v| ScalarValue::Int8(Some(v)))
            .map_err(|_| anyhow!("primary key out of range for Int8 column '{}'", column.name)),
        TableColumnType::Int16 => i16::try_from(value)
            .map(|v| ScalarValue::Int16(Some(v)))
            .map_err(|_| {
                anyhow!(
                    "primary key out of range for Int16 column '{}'",
                    column.name
                )
            }),
        TableColumnType::Int32 => i32::try_from(value)
            .map(|v| ScalarValue::Int32(Some(v)))
            .map_err(|_| {
                anyhow!(
                    "primary key out of range for Int32 column '{}'",
                    column.name
                )
            }),
        TableColumnType::Int64 => Ok(ScalarValue::Int64(Some(value))),
        _ => Err(anyhow!(
            "primary key column '{}' must be a signed integer",
            column.name
        )),
    }
}

#[derive(Debug, Clone, Copy)]
/// Represents the `PkPredicate` component used by the holo_fusion runtime.
struct PkPredicate {
    op: Operator,
    value: i64,
}

#[derive(Debug, Clone, Copy, Default)]
/// Represents the `PkBounds` component used by the holo_fusion runtime.
struct PkBounds {
    lower: Option<(i64, bool)>,
    upper: Option<(i64, bool)>,
    empty: bool,
}

impl PkBounds {
    /// Executes `is empty` for this component.
    fn is_empty(&self) -> bool {
        self.empty
    }

    /// Executes `apply` for this component.
    fn apply(&mut self, op: Operator, value: i64) {
        // Decision: evaluate `self.empty` to choose the correct SQL/storage control path.
        if self.empty {
            return;
        }

        // Decision: evaluate `op` to choose the correct SQL/storage control path.
        match op {
            Operator::Eq => {
                self.restrict_lower(value, true);
                self.restrict_upper(value, true);
            }
            Operator::Gt => self.restrict_lower(value, false),
            Operator::GtEq => self.restrict_lower(value, true),
            Operator::Lt => self.restrict_upper(value, false),
            Operator::LtEq => self.restrict_upper(value, true),
            _ => {}
        }

        self.validate();
    }

    /// Executes `restrict lower` for this component.
    fn restrict_lower(&mut self, value: i64, inclusive: bool) {
        // Decision: evaluate `self.lower` to choose the correct SQL/storage control path.
        match self.lower {
            None => self.lower = Some((value, inclusive)),
            Some((cur, cur_inclusive)) => {
                // Decision: evaluate `value > cur || (value == cur && !inclusive && cur_inclusive)` to choose the correct SQL/storage control path.
                if value > cur || (value == cur && !inclusive && cur_inclusive) {
                    self.lower = Some((value, inclusive));
                }
            }
        }
    }

    /// Executes `restrict upper` for this component.
    fn restrict_upper(&mut self, value: i64, inclusive: bool) {
        // Decision: evaluate `self.upper` to choose the correct SQL/storage control path.
        match self.upper {
            None => self.upper = Some((value, inclusive)),
            Some((cur, cur_inclusive)) => {
                // Decision: evaluate `value < cur || (value == cur && !inclusive && cur_inclusive)` to choose the correct SQL/storage control path.
                if value < cur || (value == cur && !inclusive && cur_inclusive) {
                    self.upper = Some((value, inclusive));
                }
            }
        }
    }

    /// Executes `validate` for this component.
    fn validate(&mut self) {
        // Decision: evaluate `let (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) =` to choose the correct SQL/storage control path.
        if let (Some((lower, lower_inclusive)), Some((upper, upper_inclusive))) =
            (self.lower, self.upper)
        {
            // Decision: evaluate `lower > upper || (lower == upper && (!lower_inclusive || !upper_inclusive))` to choose the correct SQL/storage control path.
            if lower > upper || (lower == upper && (!lower_inclusive || !upper_inclusive)) {
                self.empty = true;
            }
        }
    }

    /// Executes `matches` for this component.
    fn matches(&self, order_id: i64) -> bool {
        // Decision: evaluate `self.empty` to choose the correct SQL/storage control path.
        if self.empty {
            return false;
        }

        // Decision: evaluate `let Some((lower, inclusive)) = self.lower` to choose the correct SQL/storage control path.
        if let Some((lower, inclusive)) = self.lower {
            // Decision: evaluate `inclusive` to choose the correct SQL/storage control path.
            if inclusive {
                // Decision: evaluate `order_id < lower` to choose the correct SQL/storage control path.
                if order_id < lower {
                    return false;
                }
            // Decision: evaluate `order_id <= lower` to choose the correct SQL/storage control path.
            } else if order_id <= lower {
                return false;
            }
        }

        // Decision: evaluate `let Some((upper, inclusive)) = self.upper` to choose the correct SQL/storage control path.
        if let Some((upper, inclusive)) = self.upper {
            // Decision: evaluate `inclusive` to choose the correct SQL/storage control path.
            if inclusive {
                // Decision: evaluate `order_id > upper` to choose the correct SQL/storage control path.
                if order_id > upper {
                    return false;
                }
            // Decision: evaluate `order_id >= upper` to choose the correct SQL/storage control path.
            } else if order_id >= upper {
                return false;
            }
        }

        true
    }

    /// Executes `as scan range` for this component.
    fn as_scan_range(&self, table_id: u64) -> Result<(Vec<u8>, Vec<u8>)> {
        let mut start = table_key_prefix(table_id);
        let mut end = prefix_end(&start).unwrap_or_default();

        // Decision: evaluate `let Some((lower, inclusive)) = self.lower` to choose the correct SQL/storage control path.
        if let Some((lower, inclusive)) = self.lower {
            let lower_key = encode_primary_key(table_id, lower);
            start = if inclusive {
                lower_key
            } else {
                prefix_end(&lower_key).unwrap_or(lower_key)
            };
        }

        // Decision: evaluate `let Some((upper, inclusive)) = self.upper` to choose the correct SQL/storage control path.
        if let Some((upper, inclusive)) = self.upper {
            let upper_key = encode_primary_key(table_id, upper);
            end = if inclusive {
                prefix_end(&upper_key).unwrap_or_default()
            } else {
                upper_key
            };
        }

        Ok((start, end))
    }
}

/// Executes `extract supported predicates` for this component.
fn extract_supported_predicates(expr: &Expr, primary_key_column: &str) -> Option<Vec<PkPredicate>> {
    let mut predicates = Vec::new();
    // Decision: evaluate `collect_supported_predicates(expr, &mut predicates)` to choose the correct SQL/storage control path.
    if collect_supported_predicates(expr, primary_key_column, &mut predicates) {
        Some(predicates)
    } else {
        None
    }
}

/// Executes `collect supported predicates` for this component.
fn collect_supported_predicates(
    expr: &Expr,
    primary_key_column: &str,
    out: &mut Vec<PkPredicate>,
) -> bool {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_supported_predicates(binary.left.as_ref(), primary_key_column, out)
                && collect_supported_predicates(binary.right.as_ref(), primary_key_column, out)
        }
        Expr::BinaryExpr(binary) => {
            // Decision: evaluate `(` to choose the correct SQL/storage control path.
            let (column, op, literal) = match (
                extract_column_name(binary.left.as_ref()),
                extract_i64_literal(binary.right.as_ref()),
            ) {
                (Some(column), Some(value)) => (column, binary.op, value),
                _ => match (
                    extract_column_name(binary.right.as_ref()),
                    extract_i64_literal(binary.left.as_ref()),
                ) {
                    (Some(column), Some(value)) => (column, reverse_comparison(binary.op), value),
                    _ => return false,
                },
            };

            // Decision: evaluate `column != primary_key_column` to choose the correct SQL/storage control path.
            if !column.eq_ignore_ascii_case(primary_key_column) {
                return false;
            }

            // Decision: evaluate `!matches!(` to choose the correct SQL/storage control path.
            if !matches!(
                op,
                Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            ) {
                return false;
            }

            out.push(PkPredicate { op, value: literal });
            true
        }
        _ => false,
    }
}

/// Executes `extract column name` for this component.
fn extract_column_name(expr: &Expr) -> Option<&str> {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        Expr::Cast(cast) => extract_column_name(cast.expr.as_ref()),
        Expr::TryCast(cast) => extract_column_name(cast.expr.as_ref()),
        _ => None,
    }
}

/// Executes `extract i64 literal` for this component.
fn extract_i64_literal(expr: &Expr) -> Option<i64> {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        Expr::Literal(value, _) => scalar_value_to_i64(value),
        Expr::Cast(cast) => extract_i64_literal(cast.expr.as_ref()),
        Expr::TryCast(cast) => extract_i64_literal(cast.expr.as_ref()),
        _ => None,
    }
}

/// Executes `scalar value to i64` for this component.
fn scalar_value_to_i64(value: &ScalarValue) -> Option<i64> {
    // Decision: evaluate `value` to choose the correct SQL/storage control path.
    match value {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some((*v).into()),
        ScalarValue::Int16(Some(v)) => Some((*v).into()),
        ScalarValue::Int8(Some(v)) => Some((*v).into()),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        ScalarValue::UInt32(Some(v)) => Some((*v).into()),
        ScalarValue::UInt16(Some(v)) => Some((*v).into()),
        ScalarValue::UInt8(Some(v)) => Some((*v).into()),
        _ => None,
    }
}

/// Executes `reverse comparison` for this component.
fn reverse_comparison(op: Operator) -> Operator {
    // Decision: evaluate `op` to choose the correct SQL/storage control path.
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

/// Executes `decode orders entry with version` for this component.
fn decode_orders_entry_with_version(
    expected_table_id: u64,
    entry: &LatestEntry,
) -> Result<Option<VersionedOrdersRow>> {
    let order_id = decode_primary_key(entry.key.as_slice(), expected_table_id)?;
    let decoded = decode_orders_row_value(entry.value.as_slice())?;

    Ok(decoded.map(|row| VersionedOrdersRow {
        row: OrdersSeedRow {
            order_id,
            customer_id: row.customer_id,
            status: row.status,
            total_cents: row.total_cents,
            created_at_ns: row.created_at_ns,
        },
        version: entry.version,
    }))
}

#[derive(Debug)]
/// Represents the `DecodedRowValue` component used by the holo_fusion runtime.
struct DecodedRowValue {
    customer_id: i64,
    status: Option<String>,
    total_cents: i64,
    created_at_ns: i64,
}

/// Executes `decode orders row value` for this component.
fn decode_orders_row_value(bytes: &[u8]) -> Result<Option<DecodedRowValue>> {
    // Decision: evaluate `bytes.len() < 2` to choose the correct SQL/storage control path.
    if bytes.len() < 2 {
        return Err(anyhow!("row value too short"));
    }

    let format = bytes[0];
    // Decision: evaluate `format != ROW_FORMAT_VERSION_V1` to choose the correct SQL/storage control path.
    if format != ROW_FORMAT_VERSION_V1 {
        return Err(anyhow!("unsupported row format version {format}"));
    }

    let flags = bytes[1];
    // Decision: evaluate `flags & ROW_FLAG_TOMBSTONE != 0` to choose the correct SQL/storage control path.
    if flags & ROW_FLAG_TOMBSTONE != 0 {
        return Ok(None);
    }

    let mut cursor = 2usize;
    let column_count = read_u16(bytes, &mut cursor)? as usize;
    let null_bitmap_len = read_u16(bytes, &mut cursor)? as usize;
    // Decision: evaluate `column_count != 5` to choose the correct SQL/storage control path.
    if column_count != 5 {
        return Err(anyhow!(
            "unexpected column_count={column_count} for orders row"
        ));
    }

    let null_bitmap = read_bytes(bytes, &mut cursor, null_bitmap_len)?;

    let mut payloads: Vec<Option<Vec<u8>>> = Vec::with_capacity(column_count);
    for column_idx in 0..column_count {
        // Decision: evaluate `is_null(null_bitmap, column_idx)` to choose the correct SQL/storage control path.
        if is_null(null_bitmap, column_idx) {
            payloads.push(None);
            continue;
        }

        let payload_len = read_u32(bytes, &mut cursor)? as usize;
        let payload = read_bytes(bytes, &mut cursor, payload_len)?;
        payloads.push(Some(payload.to_vec()));
    }

    let customer_id = decode_required_i64(payloads[1].as_deref(), "customer_id")?;
    // Decision: evaluate `payloads[2].as_deref()` to choose the correct SQL/storage control path.
    let status = match payloads[2].as_deref() {
        Some(payload) => Some(decode_utf8(payload)?),
        None => None,
    };
    let total_cents = decode_required_i64(payloads[3].as_deref(), "total_cents")?;
    let created_at_ns = decode_required_i64(payloads[4].as_deref(), "created_at")?;

    Ok(Some(DecodedRowValue {
        customer_id,
        status,
        total_cents,
        created_at_ns,
    }))
}

/// Returns whether one row payload encodes a SQL tombstone marker.
fn row_payload_is_sql_tombstone(value: &[u8]) -> bool {
    if value.len() < 2 {
        return false;
    }
    matches!(value[0], ROW_FORMAT_VERSION_V1 | ROW_FORMAT_VERSION_V2)
        && (value[1] & ROW_FLAG_TOMBSTONE != 0)
}

/// Executes `decode primary key` for this component.
fn decode_primary_key(key: &[u8], expected_table_id: u64) -> Result<i64> {
    // Decision: evaluate minimum key length to choose the correct SQL/storage control path.
    if key.len() < 1 + 8 + 1 + 1 + 8 {
        return Err(anyhow!("primary key too short"));
    }

    // Decision: evaluate `key[0] != DATA_PREFIX_PRIMARY_ROW` to choose the correct SQL/storage control path.
    if key[0] != DATA_PREFIX_PRIMARY_ROW {
        return Err(anyhow!("invalid primary key prefix {}", key[0]));
    }

    let mut table_id_bytes = [0u8; 8];
    table_id_bytes.copy_from_slice(&key[1..9]);
    let table_id = u64::from_be_bytes(table_id_bytes);
    // Decision: evaluate `table_id != expected_table_id` to choose the correct SQL/storage control path.
    if table_id != expected_table_id {
        return Err(anyhow!(
            "unexpected table id in key: expected={}, actual={}",
            expected_table_id,
            table_id
        ));
    }

    let tuple_offset = if key[9] == TUPLE_TAG_INT64 {
        9usize
    } else if key[9] == HASH_BUCKET_TAG {
        if key.len() < 1 + 8 + 1 + 2 + 1 + 1 + 8 {
            return Err(anyhow!("hash primary key too short"));
        }
        12usize
    } else {
        return Err(anyhow!("unsupported tuple tag {}", key[9]));
    };
    if key[tuple_offset] != TUPLE_TAG_INT64 {
        return Err(anyhow!("unsupported tuple tag {}", key[tuple_offset]));
    }
    if key[tuple_offset + 1] != 0 {
        return Err(anyhow!("primary key cannot be null"));
    }

    let mut payload = [0u8; 8];
    payload.copy_from_slice(&key[(tuple_offset + 2)..(tuple_offset + 10)]);
    Ok(decode_i64_ordered(payload))
}

/// Executes `table key prefix` for this component.
fn table_key_prefix(table_id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8);
    out.push(DATA_PREFIX_PRIMARY_ROW);
    out.extend_from_slice(&table_id.to_be_bytes());
    out
}

/// Executes `table hash prefix` for this component.
fn table_hash_prefix(table_id: u64) -> Vec<u8> {
    let mut out = table_key_prefix(table_id);
    out.push(HASH_BUCKET_TAG);
    out
}

/// Executes `table hash bucket prefix` for this component.
fn table_hash_bucket_prefix(table_id: u64, bucket: u16) -> Vec<u8> {
    let mut out = table_hash_prefix(table_id);
    out.extend_from_slice(&bucket.to_be_bytes());
    out
}

/// Executes `encode primary key` for this component.
fn encode_primary_key(table_id: u64, key: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + 1 + 1 + 8);
    out.push(DATA_PREFIX_PRIMARY_ROW);
    out.extend_from_slice(&table_id.to_be_bytes());
    out.push(TUPLE_TAG_INT64);
    out.push(0);
    out.extend_from_slice(&encode_i64_ordered(key));
    out
}

/// Executes `encode primary key hash` for this component.
fn encode_primary_key_hash(table_id: u64, bucket: u16, key: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + 1 + 2 + 1 + 1 + 8);
    out.push(DATA_PREFIX_PRIMARY_ROW);
    out.extend_from_slice(&table_id.to_be_bytes());
    out.push(HASH_BUCKET_TAG);
    out.extend_from_slice(&bucket.to_be_bytes());
    out.push(TUPLE_TAG_INT64);
    out.push(0);
    out.extend_from_slice(&encode_i64_ordered(key));
    out
}

/// Executes `hash bucket for primary key` for this component.
fn hash_bucket_for_primary_key(primary_key: i64, bucket_count: usize) -> u16 {
    let count = bucket_count.max(1).min(u16::MAX as usize);
    // Stable splitmix64 mixer over signed PK payload for deterministic bucket assignment.
    let mut mixed = (primary_key as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^= mixed >> 31;
    (mixed % (count as u64)) as u16
}

/// Encodes one PK key according to table-level primary-key distribution metadata.
fn encode_primary_key_with_distribution(
    table_id: u64,
    key: i64,
    distribution: PrimaryKeyDistribution,
    hash_bucket_count: Option<usize>,
) -> Vec<u8> {
    match distribution {
        PrimaryKeyDistribution::Range => encode_primary_key(table_id, key),
        PrimaryKeyDistribution::Hash => {
            let bucket = hash_bucket_for_primary_key(
                key,
                hash_bucket_count.unwrap_or(DEFAULT_HASH_PK_BUCKETS),
            );
            encode_primary_key_hash(table_id, bucket, key)
        }
    }
}

/// Executes `prefix end` for this component.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    for idx in (0..out.len()).rev() {
        // Decision: evaluate `out[idx] != 0xFF` to choose the correct SQL/storage control path.
        if out[idx] != 0xFF {
            out[idx] = out[idx].saturating_add(1);
            out.truncate(idx + 1);
            return Some(out);
        }
    }
    None
}

/// Executes `encode i64` for this component.
fn encode_i64(value: i64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// Executes `decode required i64` for this component.
fn decode_required_i64(payload: Option<&[u8]>, field: &str) -> Result<i64> {
    let payload = payload.ok_or_else(|| anyhow!("missing required field {field}"))?;
    // Decision: evaluate `payload.len() != 8` to choose the correct SQL/storage control path.
    if payload.len() != 8 {
        return Err(anyhow!(
            "invalid payload length for {field}: {}",
            payload.len()
        ));
    }
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(payload);
    Ok(i64::from_be_bytes(bytes))
}

/// Executes `encode utf8` for this component.
fn encode_utf8(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + value.len());
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    out
}

/// Executes `decode utf8` for this component.
fn decode_utf8(payload: &[u8]) -> Result<String> {
    // Decision: evaluate `payload.len() < 4` to choose the correct SQL/storage control path.
    if payload.len() < 4 {
        return Err(anyhow!("utf8 payload too short"));
    }

    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&payload[..4]);
    let len = u32::from_be_bytes(len_bytes) as usize;
    // Decision: evaluate `payload.len() != 4 + len` to choose the correct SQL/storage control path.
    if payload.len() != 4 + len {
        return Err(anyhow!(
            "utf8 payload length mismatch: declared={}, actual={}",
            len,
            payload.len().saturating_sub(4)
        ));
    }

    let value = std::str::from_utf8(&payload[4..])?;
    Ok(value.to_string())
}

/// Executes `encode i64 ordered` for this component.
fn encode_i64_ordered(value: i64) -> [u8; 8] {
    (value as u64 ^ SIGN_FLIP_MASK).to_be_bytes()
}

/// Executes `decode i64 ordered` for this component.
fn decode_i64_ordered(bytes: [u8; 8]) -> i64 {
    let raw = u64::from_be_bytes(bytes) ^ SIGN_FLIP_MASK;
    raw as i64
}

/// Executes `read u16` for this component.
fn read_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16> {
    let slice = read_bytes(bytes, cursor, 2)?;
    let mut out = [0u8; 2];
    out.copy_from_slice(slice);
    Ok(u16::from_be_bytes(out))
}

/// Executes `read u32` for this component.
fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let slice = read_bytes(bytes, cursor, 4)?;
    let mut out = [0u8; 4];
    out.copy_from_slice(slice);
    Ok(u32::from_be_bytes(out))
}

/// Executes `read bytes` for this component.
fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor.saturating_add(len);
    // Decision: evaluate `end > bytes.len()` to choose the correct SQL/storage control path.
    if end > bytes.len() {
        return Err(anyhow!(
            "buffer underflow: need {} bytes at offset {}, total={}",
            len,
            cursor,
            bytes.len()
        ));
    }
    let out = &bytes[*cursor..end];
    *cursor = end;
    Ok(out)
}

/// Executes `is null` for this component.
fn is_null(bitmap: &[u8], column_idx: usize) -> bool {
    let byte_idx = column_idx / 8;
    let bit_idx = column_idx % 8;
    // Decision: evaluate `byte_idx >= bitmap.len()` to choose the correct SQL/storage control path.
    if byte_idx >= bitmap.len() {
        return false;
    }
    (bitmap[byte_idx] & (1u8 << bit_idx)) != 0
}

/// Executes `df external` for this component.
fn df_external(err: anyhow::Error) -> DataFusionError {
    DataFusionError::Execution(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// Executes `orders key round trip` for this component.
    fn orders_key_round_trip() {
        let ids = [i64::MIN, -1, 0, 42, i64::MAX];
        for id in ids {
            let key = encode_orders_primary_key(id);
            let decoded = decode_primary_key(&key, ORDERS_TABLE_ID).expect("decode key");
            assert_eq!(decoded, id);
        }
    }

    #[test]
    /// Executes `orders row round trip` for this component.
    fn orders_row_round_trip() {
        let row = OrdersSeedRow::new(1001, 42, Some("paid"), 1299, 1_707_654_000_000_000_000);
        let value = encode_orders_row_value(&row);
        let decoded = decode_orders_row_value(&value)
            .expect("decode value")
            .expect("tombstone check");
        assert_eq!(decoded.customer_id, row.customer_id);
        assert_eq!(decoded.status.as_deref(), Some("paid"));
        assert_eq!(decoded.total_cents, row.total_cents);
        assert_eq!(decoded.created_at_ns, row.created_at_ns);
    }

    #[test]
    fn projected_generic_rows_avoid_omitted_non_nullable_null_validation() {
        let full_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
        ]));
        let rows = vec![
            vec![ScalarValue::Int64(Some(1)), ScalarValue::Null],
            vec![ScalarValue::Int64(Some(2)), ScalarValue::Null],
        ];

        let full_err = rows_to_batch_generic(full_schema.clone(), rows.as_slice())
            .expect_err("full-schema batch should reject NULL in non-nullable column");
        assert!(
            full_err
                .to_string()
                .contains("Column 'customer_id' is declared as non-nullable"),
            "unexpected full-schema error: {full_err}"
        );

        let projected_rows =
            project_generic_rows(rows.as_slice(), &[0]).expect("project rows to order_id");
        let projected_schema = Arc::new(
            full_schema
                .project(&[0])
                .expect("project schema to order_id column"),
        );
        let projected_batch = rows_to_batch_generic(projected_schema, projected_rows.as_slice())
            .expect("projected schema should ignore omitted non-nullable column");
        assert_eq!(projected_batch.num_columns(), 1);
        assert_eq!(projected_batch.num_rows(), 2);
    }

    #[test]
    /// Executes `supported filter parser handles conjunction` for this component.
    fn supported_filter_parser_handles_conjunction() {
        use datafusion::logical_expr::col;
        use datafusion::logical_expr::lit;

        let expr = col("order_id")
            .gt_eq(lit(1000i64))
            .and(col("order_id").lt(lit(2000i64)));
        let predicates =
            extract_supported_predicates(&expr, "order_id").expect("supported predicate");
        assert_eq!(predicates.len(), 2);
    }

    #[test]
    fn scan_predicate_formatter_renders_conjunction() {
        let predicate = ScanPredicateExpr::And(vec![
            ScanPredicateExpr::Comparison {
                column: "order_id".to_string(),
                op: ScanPredicateOp::GtEq,
                value: 100,
            },
            ScanPredicateExpr::Comparison {
                column: "order_id".to_string(),
                op: ScanPredicateOp::Lt,
                value: 200,
            },
        ]);
        assert_eq!(
            format_scan_predicate_expr(&predicate),
            "order_id >= 100 AND order_id < 200"
        );
    }

    #[test]
    fn chunk_replicated_writes_respects_limits() {
        let entries = (0..5)
            .map(|idx| ReplicatedWriteEntry {
                key: vec![idx as u8],
                value: vec![idx as u8; 8],
            })
            .collect::<Vec<_>>();

        let chunks = chunk_replicated_writes(entries, 2, usize::MAX);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 2);
        assert_eq!(chunks[1].len(), 2);
        assert_eq!(chunks[2].len(), 1);
    }

    #[test]
    fn conditional_chunk_ranges_respect_bytes_and_entry_cap() {
        let entries = (0..4)
            .map(|idx| PreparedConditionalEntry {
                key: vec![idx as u8],
                value: vec![idx as u8; 16],
                expected_version: RpcVersion::zero(),
                applied_version: RpcVersion::zero(),
                rollback_value: vec![idx as u8; 16],
            })
            .collect::<Vec<_>>();

        let by_entries = conditional_chunk_ranges(entries.as_slice(), 2, usize::MAX);
        assert_eq!(by_entries, vec![0..2, 2..4]);

        let one_entry_bytes = prepared_conditional_entry_size(&entries[0]);
        let by_bytes = conditional_chunk_ranges(
            entries.as_slice(),
            usize::MAX,
            one_entry_bytes.saturating_add(1),
        );
        assert_eq!(by_bytes, vec![0..1, 1..2, 2..3, 3..4]);
    }

    #[test]
    fn retryable_conditional_error_classifier_matches_route_mismatch() {
        let err = anyhow::anyhow!(
            "apply conditional insert batch table=sales_facts shard=1 target=127.0.0.1:15051 chunk=15/20 failed: range_write_latest_conditional rpc failed for 127.0.0.1:15051: rpc status: key routed to shard 2, but request targeted shard 1"
        );
        assert!(classify_write_error_message(err.to_string().as_str()).is_retryable());

        let non_retryable = anyhow::anyhow!("conditional insert response missing applied versions");
        assert!(!classify_write_error_message(non_retryable.to_string().as_str()).is_retryable());
    }

    #[test]
    fn overload_error_classifier_matches_circuit_and_budget_messages() {
        assert!(is_overload_error_message(
            "server is overloaded: circuit breaker open for 127.0.0.1:15051"
        ));
        assert!(is_overload_error_message(
            "server is overloaded: write inflight budget exceeded in write_batch"
        ));
        assert!(!is_overload_error_message(
            "duplicate key value violates unique constraint"
        ));
    }

    #[test]
    fn summary_pushdown_support_accepts_second_key_constraints() {
        use datafusion::logical_expr::{col, lit};

        let index = SecondaryIndexRecord {
            db_id: 1,
            schema_id: 1,
            table_id: 1,
            index_id: 1,
            table_name: "sales_facts".to_string(),
            index_name: "idx_sales_status_day_merchant_cover".to_string(),
            unique: false,
            key_columns: vec![
                "status".to_string(),
                "event_day".to_string(),
                "merchant_id".to_string(),
            ],
            include_columns: vec!["amount_cents".to_string()],
            distribution: SecondaryIndexDistribution::Range,
            hash_bucket_count: None,
            state: SecondaryIndexState::Public,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        let summary = extract_predicate_summary(
            vec![col("Use event_day@1").in_list(
                vec![lit(20i64), lit(21i64), lit(22i64), lit(23i64)],
                false,
            )]
            .as_slice(),
        );

        assert!(summary_supports_secondary_index_pushdown(
            &summary,
            &[&index]
        ));
    }

    #[test]
    fn summary_pushdown_support_rejects_non_indexed_constraints() {
        use datafusion::logical_expr::{col, lit};

        let index = SecondaryIndexRecord {
            db_id: 1,
            schema_id: 1,
            table_id: 1,
            index_id: 1,
            table_name: "sales_facts".to_string(),
            index_name: "idx_sales_status_day_merchant_cover".to_string(),
            unique: false,
            key_columns: vec![
                "status".to_string(),
                "event_day".to_string(),
                "merchant_id".to_string(),
            ],
            include_columns: vec!["amount_cents".to_string()],
            distribution: SecondaryIndexDistribution::Range,
            hash_bucket_count: None,
            state: SecondaryIndexState::Public,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        let summary =
            extract_predicate_summary(vec![col("amount_cents").gt(lit(1000i64))].as_slice());

        assert!(!summary_supports_secondary_index_pushdown(
            &summary,
            &[&index]
        ));
    }
}
