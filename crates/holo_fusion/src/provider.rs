//! DataFusion table provider backed by HoloStore key/value primitives.
//!
//! This module owns row encoding/decoding, scan/write paths, conditional
//! mutation helpers, and DataFusion `TableProvider` integration.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Builder, StringBuilder, TimestampNanosecondBuilder};
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
use tracing::{debug, info, warn};

use crate::metadata::TableMetadataRecord;
use crate::metrics::PushdownMetrics;
use crate::topology::{fetch_topology, require_non_empty_topology, route_key, scan_targets};

pub const ORDERS_TABLE_NAME: &str = "orders";
pub const ORDERS_TABLE_ID: u64 = 100;
pub const ORDERS_TABLE_MODEL: &str = "orders_v1";
pub const ORDERS_SHARD_INDEX: usize = 0;

const DATA_PREFIX_PRIMARY_ROW: u8 = 0x20;
const TUPLE_TAG_INT64: u8 = 0x02;
const ROW_FORMAT_VERSION_V1: u8 = 1;
const ROW_FLAG_TOMBSTONE: u8 = 0x01;
const SIGN_FLIP_MASK: u64 = 1u64 << 63;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Enumerates states/variants for `ConditionalWriteOutcome`.
pub enum ConditionalWriteOutcome {
    Applied(u64),
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Represents the `HoloProviderCodecSpec` component used by the holo_fusion runtime.
pub struct HoloProviderCodecSpec {
    pub table_name: String,
    pub table_id: u64,
    pub table_model: String,
    pub preferred_shards: Vec<usize>,
    pub page_size: usize,
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

#[derive(Debug, Clone)]
/// Represents the `HoloStoreTableProvider` component used by the holo_fusion runtime.
pub struct HoloStoreTableProvider {
    schema: SchemaRef,
    table_name: String,
    table_id: u64,
    table_model: String,
    preferred_shards: Vec<usize>,
    page_size: usize,
    local_grpc_addr: SocketAddr,
    client: HoloStoreClient,
    metrics: Arc<PushdownMetrics>,
}

impl HoloStoreTableProvider {
    /// Executes `orders` for this component.
    pub fn orders(client: HoloStoreClient, metrics: Arc<PushdownMetrics>) -> Self {
        let local_grpc_addr = client.target();
        Self {
            schema: orders_schema(),
            table_name: ORDERS_TABLE_NAME.to_string(),
            table_id: ORDERS_TABLE_ID,
            table_model: ORDERS_TABLE_MODEL.to_string(),
            preferred_shards: vec![ORDERS_SHARD_INDEX],
            page_size: 2048,
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
        // Decision: evaluate `table.table_model != ORDERS_TABLE_MODEL` to choose the correct SQL/storage control path.
        if table.table_model != ORDERS_TABLE_MODEL {
            return Err(anyhow!(
                "unsupported table model '{}' for table '{}'",
                table.table_model,
                table.table_name
            ));
        }

        let local_grpc_addr = client.target();
        Ok(Self {
            schema: orders_schema(),
            table_name: table.table_name.clone(),
            table_id: table.table_id,
            table_model: table.table_model.clone(),
            preferred_shards: table.preferred_shards.clone(),
            page_size: table.page_size.max(1),
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
            preferred_shards: self.preferred_shards.clone(),
            page_size: self.page_size,
            local_grpc_addr: self.local_grpc_addr.to_string(),
        }
    }

    /// Executes `from codec spec` for this component.
    pub fn from_codec_spec(
        spec: HoloProviderCodecSpec,
        metrics: Arc<PushdownMetrics>,
    ) -> Result<Self> {
        // Decision: evaluate `spec.table_model != ORDERS_TABLE_MODEL` to choose the correct SQL/storage control path.
        if spec.table_model != ORDERS_TABLE_MODEL {
            return Err(anyhow!(
                "unsupported table model '{}' in provider codec",
                spec.table_model
            ));
        }

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
            schema: orders_schema(),
            table_name: spec.table_name,
            table_id: spec.table_id,
            table_model: spec.table_model,
            preferred_shards: spec.preferred_shards,
            page_size: spec.page_size.max(1),
            local_grpc_addr,
            client,
            metrics,
        })
    }

    /// Executes `table name` for this component.
    pub fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    /// Executes `scan orders by order id bounds` for this component.
    pub async fn scan_orders_by_order_id_bounds(
        &self,
        lower: Option<(i64, bool)>,
        upper: Option<(i64, bool)>,
        limit: Option<usize>,
    ) -> Result<Vec<OrdersSeedRow>> {
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

    /// Executes `upsert orders rows` for this component.
    pub async fn upsert_orders_rows(&self, rows: &[OrdersSeedRow]) -> Result<u64> {
        let entries = rows
            .iter()
            .map(|row| (row.order_id, encode_orders_row_value(row)))
            .collect::<Vec<_>>();
        self.write_order_entries(entries, "upsert").await
    }

    /// Executes `tombstone orders by order id` for this component.
    pub async fn tombstone_orders_by_order_id(&self, order_ids: &[i64]) -> Result<u64> {
        let tombstone = encode_orders_tombstone_value();
        let entries = order_ids
            .iter()
            .map(|order_id| (*order_id, tombstone.clone()))
            .collect::<Vec<_>>();
        self.write_order_entries(entries, "delete").await
    }

    /// Executes `apply orders writes conditional` for this component.
    pub async fn apply_orders_writes_conditional(
        &self,
        writes: &[ConditionalOrderWrite],
        op: &'static str,
    ) -> Result<ConditionalWriteOutcome> {
        // Decision: evaluate `writes.is_empty()` to choose the correct SQL/storage control path.
        if writes.is_empty() {
            return Ok(ConditionalWriteOutcome::Applied(0));
        }

        let topology = fetch_topology(&self.client)
            .await
            .with_context(|| format!("fetch topology for {op}"))?;
        require_non_empty_topology(&topology)?;

        let mut by_target = BTreeMap::<WriteTarget, Vec<PreparedConditionalEntry>>::new();
        for write in writes {
            let key = encode_primary_key(self.table_id, write.order_id);
            let route = route_key(
                &topology,
                self.local_grpc_addr,
                key.as_slice(),
                &self.preferred_shards,
            )
            .ok_or_else(|| {
                anyhow!(
                    "no shard route found for key table={} order_id={}",
                    self.table_name,
                    write.order_id
                )
            })?;

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
            let request_entries = entries
                .iter()
                .map(|entry| ReplicatedConditionalWriteEntry {
                    key: entry.key.clone(),
                    value: entry.value.clone(),
                    expected_version: entry.expected_version,
                })
                .collect::<Vec<_>>();

            let write_client = self.client_for(target.grpc_addr);
            let result = write_client
                .range_write_latest_conditional(
                    target.shard_index,
                    target.start_key.as_slice(),
                    target.end_key.as_slice(),
                    request_entries,
                )
                .await
                .with_context(|| {
                    format!(
                        "apply conditional {op} batch table={} shard={} target={}",
                        self.table_name, target.shard_index, target.grpc_addr
                    )
                })?;

            // Decision: evaluate `result.conflicts > 0` to choose the correct SQL/storage control path.
            if result.conflicts > 0 {
                self.rollback_conditional_targets(applied_targets.as_slice(), op)
                    .await?;
                return Ok(ConditionalWriteOutcome::Conflict);
            }

            // Decision: evaluate `result.applied != expected` to choose the correct SQL/storage control path.
            if result.applied != expected {
                self.rollback_conditional_targets(applied_targets.as_slice(), op)
                    .await?;
                return Err(anyhow!(
                    "partial conditional {op} apply on shard {}: expected {}, applied {}",
                    target.shard_index,
                    expected,
                    result.applied
                ));
            }

            let mut versions_by_key = BTreeMap::<Vec<u8>, RpcVersion>::new();
            for item in result.applied_versions {
                versions_by_key.insert(item.key, item.version);
            }
            // Decision: evaluate `versions_by_key.len() != expected as usize` to choose the correct SQL/storage control path.
            if versions_by_key.len() != expected as usize {
                self.rollback_conditional_targets(applied_targets.as_slice(), op)
                    .await?;
                return Err(anyhow!(
                    "conditional {op} response missing applied versions on shard {}: expected {}, got {}",
                    target.shard_index,
                    expected,
                    versions_by_key.len()
                ));
            }

            for entry in &mut entries {
                let applied_version = versions_by_key.get(entry.key.as_slice()).copied().ok_or_else(
                    || {
                        anyhow!(
                            "conditional {op} response missing applied version for key={} on shard {}",
                            hex::encode(&entry.key),
                            target.shard_index
                        )
                    },
                )?;
                entry.applied_version = applied_version;
            }

            applied_targets.push((target, entries));
        }

        Ok(ConditionalWriteOutcome::Applied(writes.len() as u64))
    }

    /// Executes `scan rows` for this component.
    async fn scan_rows(
        &self,
        pushed_filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Vec<OrdersSeedRow>> {
        let mut bounds = PkBounds::default();
        for filter in pushed_filters {
            // Decision: evaluate `let Some(predicates) = extract_supported_predicates(filter)` to choose the correct SQL/storage control path.
            if let Some(predicates) = extract_supported_predicates(filter) {
                for predicate in predicates {
                    bounds.apply(predicate.op, predicate.value);
                }
            }
        }

        let rows = self
            .scan_rows_with_bounds_with_versions(bounds, limit, pushed_filters.len())
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
        // Decision: evaluate `bounds.is_empty()` to choose the correct SQL/storage control path.
        if bounds.is_empty() {
            return Ok(Vec::new());
        }

        let (start_key, end_key) = bounds.as_scan_range(self.table_id)?;
        // Decision: evaluate `!end_key.is_empty() && start_key >= end_key` to choose the correct SQL/storage control path.
        if !end_key.is_empty() && start_key >= end_key {
            return Ok(Vec::new());
        }

        // Decision: evaluate `fetch_topology(&self.client).await` to choose the correct SQL/storage control path.
        let targets = match fetch_topology(&self.client).await {
            Ok(topology) => {
                // Decision: evaluate `let Err(err) = require_non_empty_topology(&topology)` to choose the correct SQL/storage control path.
                if let Err(err) = require_non_empty_topology(&topology) {
                    warn!(error = %err, "empty topology; falling back to local scan target");
                    vec![LocalScanTarget {
                        shard_index: self.preferred_shards.first().copied().unwrap_or(0),
                        grpc_addr: self.local_grpc_addr,
                        start_key: start_key.clone(),
                        end_key: end_key.clone(),
                    }]
                } else {
                    scan_targets(
                        &topology,
                        self.local_grpc_addr,
                        &start_key,
                        &end_key,
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
                    start_key: start_key.clone(),
                    end_key: end_key.clone(),
                }]
            }
        };

        let mut rows = Vec::new();
        let mut rpc_pages = 0u64;
        let mut rows_scanned = 0u64;
        let mut bytes_scanned = 0u64;

        for target in targets {
            let scan_client = self.client_for(target.grpc_addr);
            let mut cursor = Vec::new();
            loop {
                let remaining = limit.map(|limit| limit.saturating_sub(rows.len()));
                // Decision: evaluate `matches!(remaining, Some(0))` to choose the correct SQL/storage control path.
                if matches!(remaining, Some(0)) {
                    break;
                }

                let page_limit = remaining
                    .map(|remaining| remaining.min(self.page_size).max(1))
                    .unwrap_or(self.page_size);

                let (entries, next_cursor, done) = scan_client
                    .range_snapshot_latest(
                        target.shard_index,
                        target.start_key.as_slice(),
                        target.end_key.as_slice(),
                        cursor.as_slice(),
                        page_limit,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "snapshot table={} shard={} target={} start={} end={} cursor={} limit={}",
                            self.table_name,
                            target.shard_index,
                            target.grpc_addr,
                            hex::encode(&target.start_key),
                            hex::encode(&target.end_key),
                            hex::encode(&cursor),
                            page_limit
                        )
                    })?;

                rpc_pages = rpc_pages.saturating_add(1);
                for entry in entries {
                    rows_scanned = rows_scanned.saturating_add(1);
                    bytes_scanned = bytes_scanned
                        .saturating_add(entry.key.len() as u64)
                        .saturating_add(entry.value.len() as u64);

                    let row = decode_orders_entry_with_version(self.table_id, &entry)?;
                    // Decision: evaluate `let Some(row) = row` to choose the correct SQL/storage control path.
                    if let Some(row) = row {
                        // Decision: evaluate `bounds.matches(row.row.order_id)` to choose the correct SQL/storage control path.
                        if bounds.matches(row.row.order_id) {
                            rows.push(row);
                            // Decision: evaluate `let Some(limit) = limit` to choose the correct SQL/storage control path.
                            if let Some(limit) = limit {
                                // Decision: evaluate `rows.len() >= limit` to choose the correct SQL/storage control path.
                                if rows.len() >= limit {
                                    break;
                                }
                            }
                        }
                    }
                }

                // Decision: evaluate `done` to choose the correct SQL/storage control path.
                if done
                    || next_cursor.is_empty()
                    || matches!(limit, Some(limit) if rows.len() >= limit)
                {
                    break;
                }
                cursor = next_cursor;
            }

            // Decision: evaluate `matches!(limit, Some(limit) if rows.len() >= limit)` to choose the correct SQL/storage control path.
            if matches!(limit, Some(limit) if rows.len() >= limit) {
                break;
            }
        }

        self.metrics
            .record_scan(rpc_pages, rows_scanned, rows.len() as u64, bytes_scanned);

        info!(
            table = %self.table_name,
            target_count = self.preferred_shards.len(),
            pushed_filter_count,
            rpc_pages,
            rows_scanned,
            rows_returned = rows.len(),
            bytes_scanned,
            "holofusion scan completed"
        );

        Ok(rows)
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

        let mut first_err: Option<anyhow::Error> = None;
        for (target, entries) in applied_targets.iter().rev() {
            let expected = entries.len() as u64;
            let rollback_entries = entries
                .iter()
                .map(|entry| ReplicatedConditionalWriteEntry {
                    key: entry.key.clone(),
                    value: entry.rollback_value.clone(),
                    expected_version: entry.applied_version,
                })
                .collect::<Vec<_>>();

            let client = self.client_for(target.grpc_addr);
            // Decision: evaluate `client` to choose the correct SQL/storage control path.
            match client
                .range_write_latest_conditional(
                    target.shard_index,
                    target.start_key.as_slice(),
                    target.end_key.as_slice(),
                    rollback_entries,
                )
                .await
            {
                Ok(result) => {
                    // Decision: evaluate `result.conflicts > 0` to choose the correct SQL/storage control path.
                    if result.conflicts > 0 {
                        let err = anyhow!(
                            "rollback for {op} conflicted on shard {} (conflicts={})",
                            target.shard_index,
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
                            "rollback for {op} partially applied on shard {}: expected {}, applied {}",
                            target.shard_index,
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
                    let err = anyhow!(
                        "rollback for {op} failed on shard {} target={} with error: {err}",
                        target.shard_index,
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

        let mut rows = Vec::new();
        for batch in batches {
            rows.extend(decode_orders_rows(batch)?);
        }
        self.upsert_orders_rows(&rows).await
    }

    /// Executes `write order entries` for this component.
    async fn write_order_entries(
        &self,
        rows: Vec<(i64, Vec<u8>)>,
        op: &'static str,
    ) -> Result<u64> {
        // Decision: evaluate `rows.is_empty()` to choose the correct SQL/storage control path.
        if rows.is_empty() {
            return Ok(0);
        }

        let topology = fetch_topology(&self.client)
            .await
            .with_context(|| format!("fetch topology for {op}"))?;
        require_non_empty_topology(&topology)?;

        let mut writes = BTreeMap::<WriteTarget, Vec<ReplicatedWriteEntry>>::new();
        let row_count = rows.len() as u64;

        for (order_id, value) in rows {
            let key = encode_primary_key(self.table_id, order_id);
            let route = route_key(
                &topology,
                self.local_grpc_addr,
                key.as_slice(),
                &self.preferred_shards,
            )
            .ok_or_else(|| {
                anyhow!(
                    "no shard route found for key table={} order_id={}",
                    self.table_name,
                    order_id
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
                .push(ReplicatedWriteEntry { key, value });
        }

        let mut written = 0u64;
        for (target, entries) in writes {
            let expected = entries.len() as u64;
            let write_client = self.client_for(target.grpc_addr);
            let applied = write_client
                .range_write_latest(
                    target.shard_index,
                    target.start_key.as_slice(),
                    target.end_key.as_slice(),
                    entries,
                )
                .await
                .with_context(|| {
                    format!(
                        "apply {op} batch table={} shard={} target={}",
                        self.table_name, target.shard_index, target.grpc_addr
                    )
                })?;

            // Decision: evaluate `applied != expected` to choose the correct SQL/storage control path.
            if applied != expected {
                return Err(anyhow!(
                    "partial {op} apply on shard {}: expected {}, applied {}",
                    target.shard_index,
                    expected,
                    applied
                ));
            }
            written = written.saturating_add(applied);
        }

        // Decision: evaluate `written != row_count` to choose the correct SQL/storage control path.
        if written != row_count {
            return Err(anyhow!(
                "{op} row count mismatch: expected {}, written {}",
                row_count,
                written
            ));
        }

        Ok(written)
    }

    /// Executes `client for` for this component.
    fn client_for(&self, target: SocketAddr) -> HoloStoreClient {
        // Decision: evaluate `target == self.local_grpc_addr` to choose the correct SQL/storage control path.
        if target == self.local_grpc_addr {
            self.client.clone()
        } else {
            HoloStoreClient::new(target)
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
        // Decision: evaluate `matches!(limit, Some(0))` to choose the correct SQL/storage control path.
        if matches!(limit, Some(0)) {
            let mem = MemTable::try_new(
                self.schema(),
                vec![vec![RecordBatch::new_empty(self.schema())]],
            )?;
            return mem.scan(state, projection, &[], limit).await;
        }

        debug!(
            table = %self.table_name,
            filters = filters.len(),
            limit = ?limit,
            projection = ?projection,
            "starting holofusion table scan"
        );

        let rows = self.scan_rows(filters, limit).await.map_err(df_external)?;
        let batch = rows_to_batch(self.schema(), &rows).map_err(df_external)?;
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
            let exact = extract_supported_predicates(filter).is_some();
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
fn extract_supported_predicates(expr: &Expr) -> Option<Vec<PkPredicate>> {
    let mut predicates = Vec::new();
    // Decision: evaluate `collect_supported_predicates(expr, &mut predicates)` to choose the correct SQL/storage control path.
    if collect_supported_predicates(expr, &mut predicates) {
        Some(predicates)
    } else {
        None
    }
}

/// Executes `collect supported predicates` for this component.
fn collect_supported_predicates(expr: &Expr, out: &mut Vec<PkPredicate>) -> bool {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            collect_supported_predicates(binary.left.as_ref(), out)
                && collect_supported_predicates(binary.right.as_ref(), out)
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

            // Decision: evaluate `column != "order_id"` to choose the correct SQL/storage control path.
            if column != "order_id" {
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
        let predicates = extract_supported_predicates(&expr).expect("supported predicate");
        assert_eq!(predicates.len(), 2);
    }
}
