//! DataFusion table provider backed by HoloStore key/value primitives.
//!
//! This module owns row encoding/decoding, scan/write paths, conditional
//! mutation helpers, and DataFusion `TableProvider` integration.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures_util::StreamExt;
use holo_store::{
    HoloStoreClient, LatestEntry, ReplicatedConditionalWriteEntry, ReplicatedWriteEntry, RpcVersion,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, info_span, warn, Instrument};

use crate::metadata::{
    apply_column_defaults_for_missing, table_model_row_v1, validate_row_against_metadata,
    TableCheckConstraintRecord, TableColumnRecord, TableColumnType, TableMetadataRecord,
};
use crate::metrics::PushdownMetrics;
use crate::topology::{
    fetch_topology, min_end_bound, require_non_empty_topology, route_key, scan_targets,
};

pub const ORDERS_TABLE_NAME: &str = "orders";
pub const ORDERS_TABLE_ID: u64 = 100;
pub const ORDERS_TABLE_MODEL: &str = "orders_v1";
pub const ORDERS_SHARD_INDEX: usize = 0;

const DATA_PREFIX_PRIMARY_ROW: u8 = 0x20;
const TUPLE_TAG_INT64: u8 = 0x02;
const ROW_FORMAT_VERSION_V1: u8 = 1;
const ROW_FORMAT_VERSION_V2: u8 = 2;
const ROW_FLAG_TOMBSTONE: u8 = 0x01;
const SIGN_FLIP_MASK: u64 = 1u64 << 63;
const DEFAULT_MAX_SCAN_ROWS: usize = 100_000;
const DEFAULT_DISTRIBUTED_WRITE_MAX_BATCH_ENTRIES: usize = 1_024;
const DEFAULT_DISTRIBUTED_WRITE_MAX_BATCH_BYTES: usize = 1_048_576;
const DISTRIBUTED_WRITE_RETRY_LIMIT: usize = 5;
const DISTRIBUTED_WRITE_RETRY_DELAY_MS: u64 = 100;
const DISTRIBUTED_SCAN_RETRY_LIMIT: usize = 5;
const DISTRIBUTED_SCAN_RETRY_DELAY_MS: u64 = 60;

/// Executes `default max scan rows` for this component.
fn default_max_scan_rows() -> usize {
    DEFAULT_MAX_SCAN_ROWS
}

/// Executes `configured max scan rows` for this component.
fn configured_max_scan_rows() -> usize {
    std::env::var("HOLO_FUSION_SCAN_MAX_ROWS")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
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

/// Backoff policy for distributed conditional-write retries.
fn retry_backoff(attempt: usize) -> std::time::Duration {
    let exp = 1u64 << attempt.min(5);
    std::time::Duration::from_millis(DISTRIBUTED_WRITE_RETRY_DELAY_MS.saturating_mul(exp))
}

/// Returns whether a conditional write error can be retried after topology refresh.
fn is_retryable_conditional_write_error(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("key routed to shard")
        || message.contains("request targeted shard")
        || message.contains("no shard route found")
        || message.contains("split key does not map")
        || message.contains("split key must")
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
/// Generic row decoded from a row_v1 table scan with storage version.
pub struct VersionedGenericRow {
    pub primary_key: i64,
    pub values: Vec<ScalarValue>,
    pub version: RpcVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Enumerates states/variants for `ConditionalWriteOutcome`.
pub enum ConditionalWriteOutcome {
    Applied(u64),
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
    table_name: String,
    table_id: u64,
    table_model: String,
    row_codec_mode: RowCodecMode,
    primary_key_column: String,
    primary_key_index: usize,
    preferred_shards: Vec<usize>,
    page_size: usize,
    max_scan_rows: usize,
    distributed_write_max_batch_entries: usize,
    distributed_write_max_batch_bytes: usize,
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
            table_name: ORDERS_TABLE_NAME.to_string(),
            table_id: ORDERS_TABLE_ID,
            table_model: ORDERS_TABLE_MODEL.to_string(),
            row_codec_mode: RowCodecMode::OrdersV1,
            primary_key_column: "order_id".to_string(),
            primary_key_index: 0,
            preferred_shards: vec![ORDERS_SHARD_INDEX],
            page_size: 2048,
            max_scan_rows: configured_max_scan_rows(),
            distributed_write_max_batch_entries: configured_write_max_batch_entries(),
            distributed_write_max_batch_bytes: configured_write_max_batch_bytes(),
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
        table.validate()?;
        let (schema, columns, row_codec_mode, primary_key_column, primary_key_index) =
            provider_layout_from_metadata(table)?;

        let local_grpc_addr = client.target();
        Ok(Self {
            schema,
            columns,
            check_constraints: table.check_constraints.clone(),
            table_name: table.table_name.clone(),
            table_id: table.table_id,
            table_model: table.table_model.clone(),
            row_codec_mode,
            primary_key_column,
            primary_key_index,
            preferred_shards: table.preferred_shards.clone(),
            page_size: table.page_size.max(1),
            max_scan_rows: configured_max_scan_rows(),
            distributed_write_max_batch_entries: configured_write_max_batch_entries(),
            distributed_write_max_batch_bytes: configured_write_max_batch_bytes(),
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
            primary_key_column: Some(self.primary_key_column.clone()),
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
            table_name: spec.table_name,
            table_id: spec.table_id,
            table_model: spec.table_model,
            row_codec_mode,
            primary_key_column,
            primary_key_index,
            preferred_shards: spec.preferred_shards,
            page_size: spec.page_size.max(1),
            max_scan_rows: spec.max_scan_rows.max(1),
            distributed_write_max_batch_entries: configured_write_max_batch_entries(),
            distributed_write_max_batch_bytes: configured_write_max_batch_bytes(),
            local_grpc_addr,
            client,
            metrics,
        })
    }

    /// Executes `table name` for this component.
    pub fn table_name(&self) -> &str {
        self.table_name.as_str()
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

    /// Executes `is orders v1 table` for this component.
    pub fn is_orders_v1_table(&self) -> bool {
        self.row_codec_mode == RowCodecMode::OrdersV1
    }

    /// Executes `is row v1 table` for this component.
    pub fn is_row_v1_table(&self) -> bool {
        self.row_codec_mode == RowCodecMode::RowV1
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
        if writes.is_empty() {
            return Ok(ConditionalWriteOutcome::Applied(0));
        }
        let retry_limit = DISTRIBUTED_WRITE_RETRY_LIMIT.max(1);
        let mut last_retryable_error: Option<anyhow::Error> = None;

        'attempts: for attempt in 0..retry_limit {
            let topology = match fetch_topology(&self.client)
                .await
                .with_context(|| format!("fetch topology for {op}"))
            {
                Ok(topology) => topology,
                Err(err) => {
                    if attempt + 1 < retry_limit {
                        last_retryable_error = Some(err);
                        tokio::time::sleep(retry_backoff(attempt)).await;
                        continue 'attempts;
                    }
                    return Err(err);
                }
            };
            if let Err(err) = require_non_empty_topology(&topology) {
                if attempt + 1 < retry_limit {
                    last_retryable_error = Some(err);
                    tokio::time::sleep(retry_backoff(attempt)).await;
                    continue 'attempts;
                }
                return Err(err);
            }

            let mut by_target = BTreeMap::<WriteTarget, Vec<PreparedConditionalEntry>>::new();
            for write in writes {
                let key = encode_primary_key(self.table_id, write.primary_key);
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
                            write.primary_key
                        );
                        if attempt + 1 < retry_limit {
                            last_retryable_error = Some(err);
                            tokio::time::sleep(retry_backoff(attempt)).await;
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
                    let request_entries = entries[start..end]
                        .iter()
                        .map(|entry| ReplicatedConditionalWriteEntry {
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                            expected_version: entry.expected_version,
                        })
                        .collect::<Vec<_>>();
                    let batch_expected = request_entries.len() as u64;
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
                        Ok(result) => result,
                        Err(err) => {
                            let apply_error = anyhow!(
                                "apply conditional {op} batch table={} shard={} target={} chunk={}/{} failed: {err}",
                                self.table_name,
                                target.shard_index,
                                target.grpc_addr,
                                batch_idx + 1,
                                batch_count
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

                            if attempt + 1 < retry_limit
                                && is_retryable_conditional_write_error(&apply_error)
                            {
                                last_retryable_error = Some(apply_error);
                                tokio::time::sleep(retry_backoff(attempt)).await;
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
                        return Ok(ConditionalWriteOutcome::Conflict);
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

            return Ok(ConditionalWriteOutcome::Applied(writes.len() as u64));
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
                unsupported_filters = unsupported_filters.saturating_add(1);
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
                    if rows.len() > self.max_scan_rows {
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
                unsupported_filters = unsupported_filters.saturating_add(1);
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
            .scan_rows_with_bounds_with_versions_generic_ctx(
                query_execution_id,
                stage_id,
                bounds,
                limit,
                pushed_filters.len(),
                predicate,
            )
            .await?;
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
                    if rows.len() > self.max_scan_rows {
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

        let (start_key, end_key) = spec.bounds.as_scan_range(spec.table_id)?;
        if !end_key.is_empty() && start_key >= end_key {
            return Ok(ScanExecutionResult::default());
        }

        let targets = self
            .scan_targets_for_range(start_key.as_slice(), end_key.as_slice())
            .await;
        self.metrics.record_stage_event(
            spec.query_execution_id.as_str(),
            spec.stage_id,
            "scan_stage_start",
            format!(
                "table={} targets={} projection_columns={} limit={}",
                spec.table_name,
                targets.len(),
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

        let mut result = ScanExecutionResult::default();
        let mut seen_keys = BTreeSet::<Vec<u8>>::new();

        for base_target in targets {
            let mut target = base_target;
            let mut cursor = Vec::new();
            let mut retries = 0usize;
            loop {
                let remaining = spec
                    .limit
                    .map(|limit| limit.saturating_sub(result.entries.len()));
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
                    )
                    .await
                {
                    Ok((entries, next_cursor, done)) => {
                        retries = 0;
                        result.stats.rpc_pages = result.stats.rpc_pages.saturating_add(1);

                        let mut chunk = ScanChunk {
                            sequence: result.stats.chunks.saturating_add(1),
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
                            spec.query_execution_id.as_str(),
                            spec.stage_id,
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
                            || matches!(spec.limit, Some(limit) if result.entries.len() >= limit)
                        {
                            break;
                        }
                        cursor = next_cursor;
                    }
                    Err(err) => {
                        if retries + 1 >= DISTRIBUTED_SCAN_RETRY_LIMIT.max(1) {
                            return Err(anyhow!(
                                "snapshot table={} shard={} target={} start={} end={} cursor={} limit={} failed after {} retries: {}",
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
                            spec.query_execution_id.as_str(),
                            spec.stage_id,
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
                            .reroute_scan_target(resume_key.as_slice(), end_key.as_slice())
                            .await
                        {
                            if rerouted.grpc_addr != target.grpc_addr
                                || rerouted.shard_index != target.shard_index
                            {
                                result.stats.reroutes = result.stats.reroutes.saturating_add(1);
                                self.metrics.record_scan_reroute();
                                self.metrics.record_stage_event(
                                    spec.query_execution_id.as_str(),
                                    spec.stage_id,
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

                        tokio::time::sleep(std::time::Duration::from_millis(
                            DISTRIBUTED_SCAN_RETRY_DELAY_MS,
                        ))
                        .await;
                    }
                }
            }
        }

        self.metrics.record_stage_event(
            spec.query_execution_id.as_str(),
            spec.stage_id,
            "scan_stage_finish",
            format!(
                "rpc_pages={} rows_scanned={} bytes_scanned={} retries={} reroutes={} chunks={} duplicate_rows_skipped={}",
                result.stats.rpc_pages,
                result.stats.rows_scanned,
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
                        rollback_entries,
                    )
                    .instrument(rollback_span)
                    .await
                {
                    Ok(result) => {
                        self.metrics.record_distributed_rollback(
                            target.shard_index,
                            expected,
                            rollback_started.elapsed(),
                        );
                        // Decision: evaluate `result.conflicts > 0` to choose the correct SQL/storage control path.
                        if result.conflicts > 0 {
                            self.metrics.record_distributed_conflict(target.shard_index);
                            let err = anyhow!(
                                "rollback for {op} conflicted on shard {} chunk {}/{} (conflicts={})",
                                target.shard_index,
                                batch_idx + 1,
                                batch_count,
                                result.conflicts
                            );
                            warn!(error = %err, "conditional rollback conflict");
                            // Decision: evaluate `first_err.is_none()` to choose the correct SQL/storage control path.
                            if first_err.is_none() {
                                first_err = Some(err);
                            }
                        // Decision: evaluate `result.applied != expected` to choose the correct SQL/storage control path.
                        } else if result.applied != expected {
                            let err = anyhow!(
                                "rollback for {op} partially applied on shard {} chunk {}/{}: expected {}, applied {}",
                                target.shard_index,
                                batch_idx + 1,
                                batch_count,
                                expected,
                                result.applied
                            );
                            warn!(error = %err, "conditional rollback partial apply");
                            // Decision: evaluate `first_err.is_none()` to choose the correct SQL/storage control path.
                            if first_err.is_none() {
                                first_err = Some(err);
                            }
                        }
                    }
                    Err(err) => {
                        self.metrics.record_distributed_rollback(
                            target.shard_index,
                            expected,
                            rollback_started.elapsed(),
                        );
                        let err = anyhow!(
                            "rollback for {op} failed on shard {} chunk {}/{} target={} with error: {err}",
                            target.shard_index,
                            batch_idx + 1,
                            batch_count,
                            target.grpc_addr
                        );
                        warn!(error = %err, "conditional rollback rpc failure");
                        // Decision: evaluate `first_err.is_none()` to choose the correct SQL/storage control path.
                        if first_err.is_none() {
                            first_err = Some(err);
                        }
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
            ConditionalWriteOutcome::Applied(applied) => Ok(applied),
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
        let mut last_error: Option<anyhow::Error> = None;

        for attempt in 0..DISTRIBUTED_WRITE_RETRY_LIMIT.max(1) {
            let topology = fetch_topology(&self.client)
                .await
                .with_context(|| format!("fetch topology for {op}"))?;
            require_non_empty_topology(&topology)?;

            let mut writes = BTreeMap::<WriteTarget, Vec<ReplicatedWriteEntry>>::new();
            for (primary_key, value) in &rows {
                let key = encode_primary_key(self.table_id, *primary_key);
                let route = route_key(
                    &topology,
                    self.local_grpc_addr,
                    key.as_slice(),
                    &self.preferred_shards,
                )
                .ok_or_else(|| {
                    anyhow!(
                        "no shard route found for key table={} primary_key={}",
                        self.table_name,
                        primary_key
                    )
                })?;

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
                            attempt_error = Some(anyhow!(
                                "apply {op} batch table={} shard={} target={} chunk {}/{} failed: {err}",
                                self.table_name,
                                target.shard_index,
                                target.grpc_addr,
                                batch_idx + 1,
                                batch_count
                            ));
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
                last_error = Some(err);
                if attempt + 1 < DISTRIBUTED_WRITE_RETRY_LIMIT {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        DISTRIBUTED_WRITE_RETRY_DELAY_MS,
                    ))
                    .await;
                    continue;
                }
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

        let batch = if self.row_codec_mode == RowCodecMode::RowV1 {
            let rows = self
                .scan_rows_generic_with_context(
                    query_execution_id.as_str(),
                    stage_id,
                    filters,
                    limit,
                )
                .await
                .map_err(df_external)?;
            rows_to_batch_generic(self.schema(), rows.as_slice()).map_err(df_external)?
        } else {
            let rows = self
                .scan_rows_with_context(query_execution_id.as_str(), stage_id, filters, limit)
                .await
                .map_err(df_external)?;
            rows_to_batch(self.schema(), &rows).map_err(df_external)?
        };
        let mem = MemTable::try_new(self.schema(), vec![vec![batch]])?;
        mem.scan(state, projection, &[], limit).await
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
            let exact =
                extract_supported_predicates(filter, self.primary_key_column.as_str()).is_some();
            self.metrics.record_filter_support(exact);
            support.push(if exact {
                TableProviderFilterPushDown::Exact
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
        let mut batches = Vec::new();
        while let Some(batch) = data.next().await.transpose()? {
            self.schema
                .logically_equivalent_names_and_types(&batch.schema())?;
            batches.push(batch);
        }

        self.provider
            .write_batches(&batches)
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

/// Executes `decode primary key` for this component.
fn decode_primary_key(key: &[u8], expected_table_id: u64) -> Result<i64> {
    // Decision: evaluate `key.len() < 1 + 8 + 1 + 1 + 8` to choose the correct SQL/storage control path.
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

    // Decision: evaluate `key[9] != TUPLE_TAG_INT64` to choose the correct SQL/storage control path.
    if key[9] != TUPLE_TAG_INT64 {
        return Err(anyhow!("unsupported tuple tag {}", key[9]));
    }

    // Decision: evaluate `key[10] != 0` to choose the correct SQL/storage control path.
    if key[10] != 0 {
        return Err(anyhow!("primary key cannot be null"));
    }

    let mut payload = [0u8; 8];
    payload.copy_from_slice(&key[11..19]);
    Ok(decode_i64_ordered(payload))
}

/// Executes `table key prefix` for this component.
fn table_key_prefix(table_id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8);
    out.push(DATA_PREFIX_PRIMARY_ROW);
    out.extend_from_slice(&table_id.to_be_bytes());
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
        assert!(is_retryable_conditional_write_error(&err));

        let non_retryable = anyhow::anyhow!("conditional insert response missing applied versions");
        assert!(!is_retryable_conditional_write_error(&non_retryable));
    }
}
