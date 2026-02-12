//! Table-metadata persistence and lookup on top of HoloStore keyspace.
//!
//! Metadata rows describe logical SQL tables and are used to bootstrap
//! `TableProvider` instances on each HoloFusion node.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use datafusion::common::ScalarValue;
use holo_store::{HoloStoreClient, ReplicatedConditionalWriteEntry, RpcVersion};
use serde::{Deserialize, Serialize};

use crate::topology::{fetch_topology, require_non_empty_topology, route_key, scan_targets};

/// Keyspace prefix for table metadata records.
const META_PREFIX_TABLE: u8 = 0x03;
/// Keyspace prefix for metadata-control records (schema version/checkpoints).
const META_PREFIX_CONTROL: u8 = 0x09;
/// Static key suffix for metadata schema migration state.
const META_KEY_SCHEMA_STATE_SUFFIX: u8 = 0x01;
/// Default catalog/database id currently used by HoloFusion.
const DEFAULT_DB_ID: u64 = 1;
/// Default schema id currently used by HoloFusion.
const DEFAULT_SCHEMA_ID: u64 = 1;
/// Storage model tag for the current orders table layout.
const TABLE_MODEL_ORDERS_V1: &str = "orders_v1";
/// Storage model tag for generic row-oriented table layout.
const TABLE_MODEL_ROW_V1: &str = "row_v1";
/// Page size used when scanning metadata rows.
const DEFAULT_SCAN_PAGE_SIZE: usize = 1024;
/// Visibility wait timeout after metadata writes.
const METADATA_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(5);
/// Overall timeout budget for metadata creation retries.
const METADATA_CREATE_TIMEOUT: Duration = Duration::from_secs(15);
/// Delay between metadata creation retries under contention.
const METADATA_CREATE_RETRY_INTERVAL: Duration = Duration::from_millis(100);
/// Maximum number of attempts for one conditional metadata migration write.
const METADATA_MIGRATION_WRITE_RETRY_LIMIT: usize = 8;
/// Batch size for checkpoint updates during metadata backfill.
const METADATA_MIGRATION_BATCH_SIZE: usize = 128;
/// Legacy metadata schema version used when no migration state exists yet.
const METADATA_SCHEMA_VERSION_BASELINE: u32 = 1;
/// Current metadata schema version expected by this binary.
const METADATA_SCHEMA_VERSION_CURRENT: u32 = 2;

/// Result of attempting to create table metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableMetadataOutcome {
    /// Metadata record that exists after the operation.
    pub record: TableMetadataRecord,
    /// Whether this call created a new record (`true`) vs observed existing (`false`).
    pub created: bool,
}

/// Summary of one metadata migration/backfill execution pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataMigrationOutcome {
    /// Schema version observed at migration start.
    pub from_schema_version: u32,
    /// Schema version after migration completion.
    pub to_schema_version: u32,
    /// Resume checkpoint loaded from prior incomplete migration, if any.
    pub resumed_from_checkpoint: Option<u64>,
    /// Number of table metadata rows scanned by this pass.
    pub scanned_tables: usize,
    /// Number of metadata rows rewritten/backfilled by this pass.
    pub migrated_tables: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Persisted metadata schema migration state/checkpoint record.
struct MetadataSchemaStateRecord {
    /// Completed schema version for table metadata format.
    #[serde(default = "default_metadata_schema_version")]
    schema_version: u32,
    /// Last processed table id checkpoint for resumable backfill.
    #[serde(default)]
    checkpoint_table_id: Option<u64>,
    /// Last update timestamp (Unix epoch millis).
    #[serde(default)]
    updated_at_unix_ms: u64,
}

#[derive(Debug, Clone)]
/// Loaded metadata schema state paired with latest storage version.
struct MetadataSchemaStateSnapshot {
    version: RpcVersion,
    state: MetadataSchemaStateRecord,
}

#[derive(Debug, Clone)]
/// Internal metadata entry including storage key/version for backfill writes.
struct TableMetadataEntry {
    key: Vec<u8>,
    version: RpcVersion,
    record: TableMetadataRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Physical column codec used by schema-driven table metadata.
pub enum TableColumnType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float64,
    Boolean,
    Utf8,
    TimestampNanosecond,
}

impl TableColumnType {
    /// Returns `true` when values of this type can act as signed integer PKs.
    pub fn is_signed_integer(&self) -> bool {
        matches!(self, Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Supported persisted column default expressions.
pub enum ColumnDefaultValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
    TimestampNanosecond(i64),
    CurrentTimestampNanosecond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Supported binary operations in persisted CHECK expressions.
pub enum CheckBinaryOperator {
    And,
    Or,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
/// Persisted table-level CHECK expression AST.
pub enum CheckExpression {
    Column(String),
    Literal(ColumnDefaultValue),
    Not(Box<CheckExpression>),
    IsNull(Box<CheckExpression>),
    IsNotNull(Box<CheckExpression>),
    Binary {
        op: CheckBinaryOperator,
        left: Box<CheckExpression>,
        right: Box<CheckExpression>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Persisted CHECK constraint metadata.
pub struct TableCheckConstraintRecord {
    /// Constraint name (generated when omitted in DDL).
    pub name: String,
    /// Canonical expression tree for this constraint.
    pub expr: CheckExpression,
}

#[derive(Debug, Clone)]
/// Typed row validation/default-evaluation failure with SQLSTATE mapping.
pub struct RowConstraintError {
    sqlstate: &'static str,
    message: String,
}

impl RowConstraintError {
    /// Builds a new row-constraint error carrying SQLSTATE and message.
    pub fn new(sqlstate: &'static str, message: impl Into<String>) -> Self {
        Self {
            sqlstate,
            message: message.into(),
        }
    }

    /// Returns SQLSTATE code for this validation error.
    pub fn sqlstate(&self) -> &'static str {
        self.sqlstate
    }

    /// Returns human-readable validation error.
    pub fn message(&self) -> &str {
        self.message.as_str()
    }
}

impl std::fmt::Display for RowConstraintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RowConstraintError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// One logical SQL column persisted in table metadata.
pub struct TableColumnRecord {
    /// SQL-visible column name.
    pub name: String,
    /// Persisted codec type used for row encoding/decoding.
    pub column_type: TableColumnType,
    /// Whether this column allows SQL `NULL`.
    #[serde(default)]
    pub nullable: bool,
    /// Optional persisted default expression evaluated on INSERT when omitted.
    #[serde(default)]
    pub default_value: Option<ColumnDefaultValue>,
}

#[derive(Debug, Clone, PartialEq)]
/// Input contract for creating one metadata table row.
pub struct CreateTableMetadataSpec {
    /// Storage model to assign to the newly created table.
    pub table_model: String,
    /// Persisted logical columns.
    pub columns: Vec<TableColumnRecord>,
    /// Persisted CHECK constraints for this table.
    pub check_constraints: Vec<TableCheckConstraintRecord>,
    /// Name of single-column primary key used for row-key encoding.
    pub primary_key_column: String,
    /// Optional shard affinity for scans and writes.
    pub preferred_shards: Vec<usize>,
    /// Suggested page size for scan pagination.
    pub page_size: usize,
}

impl CreateTableMetadataSpec {
    /// Validates user-provided metadata before persistence.
    pub fn validate(&self) -> Result<()> {
        if self.table_model.trim().is_empty() {
            return Err(anyhow!("table metadata spec has empty table_model"));
        }
        if self.columns.is_empty() {
            return Err(anyhow!("table metadata spec has no columns"));
        }
        let mut check_names = BTreeSet::new();
        for constraint in &self.check_constraints {
            if constraint.name.trim().is_empty() {
                return Err(anyhow!(
                    "table metadata spec has empty check constraint name"
                ));
            }
            let normalized = constraint.name.to_ascii_lowercase();
            if !check_names.insert(normalized.clone()) {
                return Err(anyhow!(
                    "table metadata spec has duplicate check constraint '{}'",
                    normalized
                ));
            }
        }
        if self.primary_key_column.trim().is_empty() {
            return Err(anyhow!("table metadata spec has empty primary_key_column"));
        }
        if self.page_size == 0 {
            return Err(anyhow!("table metadata spec has invalid page_size=0"));
        }
        Ok(())
    }
}

/// Persisted metadata row describing one logical SQL table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMetadataRecord {
    /// Logical database id.
    pub db_id: u64,
    /// Logical schema id.
    pub schema_id: u64,
    /// Stable table identifier used in key encoding.
    pub table_id: u64,
    /// SQL-visible table name.
    pub table_name: String,
    /// Storage model discriminator used by provider factory.
    pub table_model: String,
    /// Persisted logical column list used to construct provider schema.
    #[serde(default)]
    pub columns: Vec<TableColumnRecord>,
    /// Persisted CHECK constraints used for row-level validation.
    #[serde(default)]
    pub check_constraints: Vec<TableCheckConstraintRecord>,
    /// Name of single-column primary key used for row-key encoding.
    #[serde(default)]
    pub primary_key_column: Option<String>,
    /// Optional shard affinity for scans and writes.
    #[serde(default)]
    pub preferred_shards: Vec<usize>,
    /// Recommended scan page size for this table.
    #[serde(default = "default_page_size")]
    pub page_size: usize,
}

/// Default page size used when metadata omits a table-level value.
fn default_page_size() -> usize {
    2048
}

/// Default metadata schema version assumed for pre-migration clusters.
fn default_metadata_schema_version() -> u32 {
    METADATA_SCHEMA_VERSION_BASELINE
}

impl TableMetadataRecord {
    /// Validates required metadata fields before registration or persistence.
    pub fn validate(&self) -> Result<()> {
        // Decision: reject empty table names to avoid catalog ambiguity.
        if self.table_name.trim().is_empty() {
            return Err(anyhow!("table metadata has empty table_name"));
        }
        // Decision: reject zero table id because id `0` is reserved for "unset".
        if self.table_id == 0 {
            return Err(anyhow!("table metadata has invalid table_id=0"));
        }
        // Decision: enforce positive page size to prevent empty pagination loops.
        if self.page_size == 0 {
            return Err(anyhow!("table metadata has invalid page_size=0"));
        }
        // Decision: storage model is mandatory for provider selection.
        if self.table_model.trim().is_empty() {
            return Err(anyhow!(
                "table metadata has empty table_model for table {}",
                self.table_name
            ));
        }

        // Decision: legacy metadata rows may not include explicit column
        // specs; keep them valid for backward compatibility.
        if self.columns.is_empty() {
            if self.table_model == TABLE_MODEL_ROW_V1 {
                return Err(anyhow!(
                    "table metadata has empty columns for row_v1 table {}",
                    self.table_name
                ));
            }
            return Ok(());
        }

        let mut seen = BTreeSet::<String>::new();
        for column in &self.columns {
            if column.name.trim().is_empty() {
                return Err(anyhow!(
                    "table metadata has empty column name for table {}",
                    self.table_name
                ));
            }
            let normalized = column.name.to_ascii_lowercase();
            if !seen.insert(normalized.clone()) {
                return Err(anyhow!(
                    "table metadata has duplicate column '{}' for table {}",
                    normalized,
                    self.table_name
                ));
            }
            if let Some(default_value) = &column.default_value {
                validate_column_default(column, default_value).map_err(|err| {
                    anyhow!(
                        "table metadata default expression invalid for column '{}' in table {}: {}",
                        column.name,
                        self.table_name,
                        err
                    )
                })?;
            }
        }

        let by_name = self
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column))
            .collect::<BTreeMap<_, _>>();
        let mut check_names = BTreeSet::<String>::new();
        for check in &self.check_constraints {
            if check.name.trim().is_empty() {
                return Err(anyhow!(
                    "table metadata has empty check constraint name for table {}",
                    self.table_name
                ));
            }
            let normalized = check.name.to_ascii_lowercase();
            if !check_names.insert(normalized.clone()) {
                return Err(anyhow!(
                    "table metadata has duplicate check constraint '{}' for table {}",
                    normalized,
                    self.table_name
                ));
            }
            validate_check_expression(&check.expr, &by_name).map_err(|err| {
                anyhow!(
                    "table metadata has invalid check constraint '{}' for table {}: {}",
                    check.name,
                    self.table_name,
                    err
                )
            })?;
        }

        let primary_key_column = self
            .primary_key_column
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "table metadata has empty primary_key_column for table {}",
                    self.table_name
                )
            })?;
        let pk = self
            .columns
            .iter()
            .find(|column| column.name == primary_key_column)
            .ok_or_else(|| {
                anyhow!(
                    "table metadata primary key column '{}' not found in table {}",
                    primary_key_column,
                    self.table_name
                )
            })?;
        if pk.nullable {
            return Err(anyhow!(
                "table metadata primary key column '{}' cannot be nullable in table {}",
                primary_key_column,
                self.table_name
            ));
        }
        if !pk.column_type.is_signed_integer() {
            return Err(anyhow!(
                "table metadata primary key column '{}' must be signed integer in table {}",
                primary_key_column,
                self.table_name
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckExpressionType {
    Unknown,
    Boolean,
    Scalar(TableColumnType),
}

/// Validates default and CHECK metadata semantics for one table definition.
pub fn validate_table_constraints(
    columns: &[TableColumnRecord],
    checks: &[TableCheckConstraintRecord],
) -> std::result::Result<(), RowConstraintError> {
    let mut by_name = BTreeMap::<&str, &TableColumnRecord>::new();
    for column in columns {
        if let Some(default_value) = &column.default_value {
            validate_column_default(column, default_value)?;
        }
        by_name.insert(column.name.as_str(), column);
    }
    for check in checks {
        validate_check_expression(&check.expr, &by_name)?;
    }
    Ok(())
}

/// Applies defaults for columns omitted from INSERT input.
pub fn apply_column_defaults_for_missing(
    columns: &[TableColumnRecord],
    values: &mut [ScalarValue],
    missing_mask: &[bool],
    now_timestamp_ns: i64,
) -> std::result::Result<(), RowConstraintError> {
    if columns.len() != values.len() || values.len() != missing_mask.len() {
        return Err(RowConstraintError::new(
            "42804",
            format!(
                "row shape mismatch during default evaluation: columns={}, values={}, missing_mask={}",
                columns.len(),
                values.len(),
                missing_mask.len()
            ),
        ));
    }

    for idx in 0..columns.len() {
        if !missing_mask[idx] {
            continue;
        }
        if let Some(default_value) = &columns[idx].default_value {
            values[idx] =
                materialize_default_value(default_value, &columns[idx], now_timestamp_ns)?;
        }
    }
    Ok(())
}

/// Validates one row against column nullability/types and table CHECK constraints.
pub fn validate_row_against_metadata(
    columns: &[TableColumnRecord],
    checks: &[TableCheckConstraintRecord],
    values: &[ScalarValue],
) -> std::result::Result<(), RowConstraintError> {
    if columns.len() != values.len() {
        return Err(RowConstraintError::new(
            "42804",
            format!(
                "row column count mismatch: columns={}, values={}",
                columns.len(),
                values.len()
            ),
        ));
    }

    let mut by_name = BTreeMap::<String, ScalarValue>::new();
    for (column, value) in columns.iter().zip(values.iter()) {
        if matches!(value, ScalarValue::Null) {
            if !column.nullable {
                return Err(RowConstraintError::new(
                    "23502",
                    format!(
                        "null value in column '{}' violates not-null constraint",
                        column.name
                    ),
                ));
            }
            by_name.insert(column.name.to_ascii_lowercase(), ScalarValue::Null);
            continue;
        }
        if !is_scalar_compatible_with_column(value, column.column_type) {
            return Err(RowConstraintError::new(
                "42804",
                format!(
                    "value for column '{}' is incompatible with {}",
                    column.name,
                    column_type_name(column.column_type)
                ),
            ));
        }
        by_name.insert(column.name.to_ascii_lowercase(), value.clone());
    }

    for check in checks {
        let evaluated = evaluate_check_expression(check.expr.clone(), &by_name)?;
        if matches!(evaluated, Some(false)) {
            return Err(RowConstraintError::new(
                "23514",
                format!("check constraint '{}' is violated", check.name),
            ));
        }
    }

    Ok(())
}

fn validate_column_default(
    column: &TableColumnRecord,
    default_value: &ColumnDefaultValue,
) -> std::result::Result<(), RowConstraintError> {
    if matches!(default_value, ColumnDefaultValue::Null) && !column.nullable {
        return Err(RowConstraintError::new(
            "23502",
            format!(
                "default NULL is invalid for non-nullable column '{}'",
                column.name
            ),
        ));
    }
    let _ = materialize_default_value(default_value, column, 0)?;
    Ok(())
}

fn validate_check_expression(
    expr: &CheckExpression,
    columns: &BTreeMap<&str, &TableColumnRecord>,
) -> std::result::Result<(), RowConstraintError> {
    let expression_type = infer_check_expression_type(expr, columns)?;
    if !matches!(
        expression_type,
        CheckExpressionType::Boolean | CheckExpressionType::Unknown
    ) {
        return Err(RowConstraintError::new(
            "42804",
            "CHECK expression must evaluate to boolean",
        ));
    }
    Ok(())
}

fn infer_check_expression_type(
    expr: &CheckExpression,
    columns: &BTreeMap<&str, &TableColumnRecord>,
) -> std::result::Result<CheckExpressionType, RowConstraintError> {
    match expr {
        CheckExpression::Column(name) => {
            let normalized = name.to_ascii_lowercase();
            let column = columns.get(normalized.as_str()).ok_or_else(|| {
                RowConstraintError::new(
                    "42703",
                    format!("unknown column '{}' in CHECK expression", name),
                )
            })?;
            Ok(CheckExpressionType::Scalar(column.column_type))
        }
        CheckExpression::Literal(default_value) => Ok(default_value_type(default_value)),
        CheckExpression::Not(inner) => {
            let inner = infer_check_expression_type(inner, columns)?;
            if is_booleanish(inner) {
                Ok(CheckExpressionType::Boolean)
            } else {
                Err(RowConstraintError::new(
                    "42804",
                    "NOT operand in CHECK expression must be boolean",
                ))
            }
        }
        CheckExpression::IsNull(inner) | CheckExpression::IsNotNull(inner) => {
            let _ = infer_check_expression_type(inner, columns)?;
            Ok(CheckExpressionType::Boolean)
        }
        CheckExpression::Binary { op, left, right } => {
            let left_type = infer_check_expression_type(left, columns)?;
            let right_type = infer_check_expression_type(right, columns)?;
            match op {
                CheckBinaryOperator::And | CheckBinaryOperator::Or => {
                    if !is_booleanish(left_type) || !is_booleanish(right_type) {
                        return Err(RowConstraintError::new(
                            "42804",
                            "logical CHECK operands must be boolean",
                        ));
                    }
                    Ok(CheckExpressionType::Boolean)
                }
                CheckBinaryOperator::Eq | CheckBinaryOperator::NotEq => {
                    if are_comparable_check_types(left_type, right_type) {
                        Ok(CheckExpressionType::Boolean)
                    } else {
                        Err(RowConstraintError::new(
                            "42804",
                            "comparison CHECK operands are not comparable",
                        ))
                    }
                }
                CheckBinaryOperator::Lt
                | CheckBinaryOperator::LtEq
                | CheckBinaryOperator::Gt
                | CheckBinaryOperator::GtEq => {
                    if are_orderable_check_types(left_type, right_type) {
                        Ok(CheckExpressionType::Boolean)
                    } else {
                        Err(RowConstraintError::new(
                            "42804",
                            "ordered CHECK comparison operands are not compatible",
                        ))
                    }
                }
            }
        }
    }
}

fn is_booleanish(value: CheckExpressionType) -> bool {
    matches!(
        value,
        CheckExpressionType::Boolean | CheckExpressionType::Unknown
    )
}

fn default_value_type(value: &ColumnDefaultValue) -> CheckExpressionType {
    match value {
        ColumnDefaultValue::Null => CheckExpressionType::Unknown,
        ColumnDefaultValue::Boolean(_) => CheckExpressionType::Scalar(TableColumnType::Boolean),
        ColumnDefaultValue::Int64(_) => CheckExpressionType::Scalar(TableColumnType::Int64),
        ColumnDefaultValue::UInt64(_) => CheckExpressionType::Scalar(TableColumnType::UInt64),
        ColumnDefaultValue::Float64(_) => CheckExpressionType::Scalar(TableColumnType::Float64),
        ColumnDefaultValue::Utf8(_) => CheckExpressionType::Scalar(TableColumnType::Utf8),
        ColumnDefaultValue::TimestampNanosecond(_)
        | ColumnDefaultValue::CurrentTimestampNanosecond => {
            CheckExpressionType::Scalar(TableColumnType::TimestampNanosecond)
        }
    }
}

fn are_comparable_check_types(left: CheckExpressionType, right: CheckExpressionType) -> bool {
    if matches!(left, CheckExpressionType::Unknown) || matches!(right, CheckExpressionType::Unknown)
    {
        return true;
    }
    match (left, right) {
        (CheckExpressionType::Scalar(l), CheckExpressionType::Scalar(r)) => {
            if is_numeric_column_type(l) && is_numeric_column_type(r) {
                true
            } else {
                l == r
            }
        }
        (CheckExpressionType::Boolean, CheckExpressionType::Boolean) => true,
        _ => false,
    }
}

fn are_orderable_check_types(left: CheckExpressionType, right: CheckExpressionType) -> bool {
    if matches!(left, CheckExpressionType::Unknown) || matches!(right, CheckExpressionType::Unknown)
    {
        return true;
    }
    match (left, right) {
        (CheckExpressionType::Scalar(l), CheckExpressionType::Scalar(r)) => {
            (is_numeric_column_type(l) && is_numeric_column_type(r))
                || (l == TableColumnType::Utf8 && r == TableColumnType::Utf8)
                || (l == TableColumnType::TimestampNanosecond
                    && r == TableColumnType::TimestampNanosecond)
        }
        _ => false,
    }
}

fn is_numeric_column_type(column_type: TableColumnType) -> bool {
    matches!(
        column_type,
        TableColumnType::Int8
            | TableColumnType::Int16
            | TableColumnType::Int32
            | TableColumnType::Int64
            | TableColumnType::UInt8
            | TableColumnType::UInt16
            | TableColumnType::UInt32
            | TableColumnType::UInt64
            | TableColumnType::Float64
    )
}

fn materialize_default_value(
    default_value: &ColumnDefaultValue,
    column: &TableColumnRecord,
    now_timestamp_ns: i64,
) -> std::result::Result<ScalarValue, RowConstraintError> {
    match default_value {
        ColumnDefaultValue::Null => {
            if !column.nullable {
                return Err(RowConstraintError::new(
                    "23502",
                    format!(
                        "null default is invalid for non-nullable column '{}'",
                        column.name
                    ),
                ));
            }
            Ok(ScalarValue::Null)
        }
        ColumnDefaultValue::CurrentTimestampNanosecond => {
            if column.column_type != TableColumnType::TimestampNanosecond {
                return Err(RowConstraintError::new(
                    "42804",
                    format!(
                        "CURRENT_TIMESTAMP default is only valid for timestamp column '{}'",
                        column.name
                    ),
                ));
            }
            Ok(ScalarValue::TimestampNanosecond(
                Some(now_timestamp_ns),
                None,
            ))
        }
        other => coerce_default_literal_to_scalar(other, column),
    }
}

fn coerce_default_literal_to_scalar(
    value: &ColumnDefaultValue,
    column: &TableColumnRecord,
) -> std::result::Result<ScalarValue, RowConstraintError> {
    match column.column_type {
        TableColumnType::Int8 => {
            let value = default_numeric_to_i64(value, column)?;
            let narrowed = i8::try_from(value).map_err(|_| {
                RowConstraintError::new(
                    "22003",
                    format!("default value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::Int8(Some(narrowed)))
        }
        TableColumnType::Int16 => {
            let value = default_numeric_to_i64(value, column)?;
            let narrowed = i16::try_from(value).map_err(|_| {
                RowConstraintError::new(
                    "22003",
                    format!("default value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::Int16(Some(narrowed)))
        }
        TableColumnType::Int32 => {
            let value = default_numeric_to_i64(value, column)?;
            let narrowed = i32::try_from(value).map_err(|_| {
                RowConstraintError::new(
                    "22003",
                    format!("default value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::Int32(Some(narrowed)))
        }
        TableColumnType::Int64 => {
            let value = default_numeric_to_i64(value, column)?;
            Ok(ScalarValue::Int64(Some(value)))
        }
        TableColumnType::UInt8 => {
            let value = default_numeric_to_u64(value, column)?;
            let narrowed = u8::try_from(value).map_err(|_| {
                RowConstraintError::new(
                    "22003",
                    format!("default value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::UInt8(Some(narrowed)))
        }
        TableColumnType::UInt16 => {
            let value = default_numeric_to_u64(value, column)?;
            let narrowed = u16::try_from(value).map_err(|_| {
                RowConstraintError::new(
                    "22003",
                    format!("default value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::UInt16(Some(narrowed)))
        }
        TableColumnType::UInt32 => {
            let value = default_numeric_to_u64(value, column)?;
            let narrowed = u32::try_from(value).map_err(|_| {
                RowConstraintError::new(
                    "22003",
                    format!("default value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::UInt32(Some(narrowed)))
        }
        TableColumnType::UInt64 => {
            let value = default_numeric_to_u64(value, column)?;
            Ok(ScalarValue::UInt64(Some(value)))
        }
        TableColumnType::Float64 => {
            let value = default_numeric_to_f64(value, column)?;
            Ok(ScalarValue::Float64(Some(value)))
        }
        TableColumnType::Boolean => match value {
            ColumnDefaultValue::Boolean(v) => Ok(ScalarValue::Boolean(Some(*v))),
            _ => Err(RowConstraintError::new(
                "42804",
                format!(
                    "default expression is incompatible with boolean column '{}'",
                    column.name
                ),
            )),
        },
        TableColumnType::Utf8 => match value {
            ColumnDefaultValue::Utf8(v) => Ok(ScalarValue::Utf8(Some(v.clone()))),
            _ => Err(RowConstraintError::new(
                "42804",
                format!(
                    "default expression is incompatible with text column '{}'",
                    column.name
                ),
            )),
        },
        TableColumnType::TimestampNanosecond => match value {
            ColumnDefaultValue::TimestampNanosecond(v) => {
                Ok(ScalarValue::TimestampNanosecond(Some(*v), None))
            }
            _ => Err(RowConstraintError::new(
                "42804",
                format!(
                    "default expression is incompatible with timestamp column '{}'",
                    column.name
                ),
            )),
        },
    }
}

fn default_numeric_to_i64(
    value: &ColumnDefaultValue,
    column: &TableColumnRecord,
) -> std::result::Result<i64, RowConstraintError> {
    match value {
        ColumnDefaultValue::Int64(v) => Ok(*v),
        ColumnDefaultValue::UInt64(v) => i64::try_from(*v).map_err(|_| {
            RowConstraintError::new(
                "22003",
                format!("default value out of range for column '{}'", column.name),
            )
        }),
        _ => Err(RowConstraintError::new(
            "42804",
            format!(
                "default expression is incompatible with integer column '{}'",
                column.name
            ),
        )),
    }
}

fn default_numeric_to_u64(
    value: &ColumnDefaultValue,
    column: &TableColumnRecord,
) -> std::result::Result<u64, RowConstraintError> {
    match value {
        ColumnDefaultValue::UInt64(v) => Ok(*v),
        ColumnDefaultValue::Int64(v) => u64::try_from(*v).map_err(|_| {
            RowConstraintError::new(
                "22003",
                format!("default value out of range for column '{}'", column.name),
            )
        }),
        _ => Err(RowConstraintError::new(
            "42804",
            format!(
                "default expression is incompatible with unsigned integer column '{}'",
                column.name
            ),
        )),
    }
}

fn default_numeric_to_f64(
    value: &ColumnDefaultValue,
    column: &TableColumnRecord,
) -> std::result::Result<f64, RowConstraintError> {
    match value {
        ColumnDefaultValue::Float64(v) => Ok(*v),
        ColumnDefaultValue::Int64(v) => Ok(*v as f64),
        ColumnDefaultValue::UInt64(v) => Ok(*v as f64),
        _ => Err(RowConstraintError::new(
            "42804",
            format!(
                "default expression is incompatible with numeric column '{}'",
                column.name
            ),
        )),
    }
}

fn is_scalar_compatible_with_column(value: &ScalarValue, column_type: TableColumnType) -> bool {
    match column_type {
        TableColumnType::Int8 => matches!(
            value,
            ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
        ),
        TableColumnType::Int16 => matches!(
            value,
            ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
        ),
        TableColumnType::Int32 => matches!(
            value,
            ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
        ),
        TableColumnType::Int64 => matches!(
            value,
            ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
        ),
        TableColumnType::UInt8 => matches!(
            value,
            ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
        ),
        TableColumnType::UInt16 => matches!(
            value,
            ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
        ),
        TableColumnType::UInt32 => matches!(
            value,
            ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
        ),
        TableColumnType::UInt64 => matches!(
            value,
            ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
        ),
        TableColumnType::Float64 => matches!(
            value,
            ScalarValue::Float32(_)
                | ScalarValue::Float64(_)
                | ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
        ),
        TableColumnType::Boolean => matches!(value, ScalarValue::Boolean(_)),
        TableColumnType::Utf8 => matches!(value, ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_)),
        TableColumnType::TimestampNanosecond => matches!(
            value,
            ScalarValue::TimestampNanosecond(_, _)
                | ScalarValue::TimestampMicrosecond(_, _)
                | ScalarValue::TimestampMillisecond(_, _)
                | ScalarValue::TimestampSecond(_, _)
        ),
    }
}

fn column_type_name(column_type: TableColumnType) -> &'static str {
    match column_type {
        TableColumnType::Int8 => "int8",
        TableColumnType::Int16 => "int16",
        TableColumnType::Int32 => "int32",
        TableColumnType::Int64 => "int64",
        TableColumnType::UInt8 => "uint8",
        TableColumnType::UInt16 => "uint16",
        TableColumnType::UInt32 => "uint32",
        TableColumnType::UInt64 => "uint64",
        TableColumnType::Float64 => "float64",
        TableColumnType::Boolean => "boolean",
        TableColumnType::Utf8 => "utf8",
        TableColumnType::TimestampNanosecond => "timestamp_ns",
    }
}

fn evaluate_check_expression(
    expr: CheckExpression,
    values: &BTreeMap<String, ScalarValue>,
) -> std::result::Result<Option<bool>, RowConstraintError> {
    match expr {
        CheckExpression::Column(name) => {
            let value = values
                .get(name.to_ascii_lowercase().as_str())
                .ok_or_else(|| {
                    RowConstraintError::new(
                        "42703",
                        format!("unknown column '{}' in CHECK evaluation", name),
                    )
                })?;
            scalar_to_optional_bool(value)
        }
        CheckExpression::Literal(value) => default_literal_to_optional_bool(value),
        CheckExpression::Not(inner) => Ok(evaluate_check_expression(*inner, values)?.map(|v| !v)),
        CheckExpression::IsNull(inner) => Ok(Some(
            evaluate_check_scalar(*inner, values)
                .map(|scalar| matches!(scalar, ScalarValue::Null))
                .unwrap_or(true),
        )),
        CheckExpression::IsNotNull(inner) => Ok(Some(
            evaluate_check_scalar(*inner, values)
                .map(|scalar| !matches!(scalar, ScalarValue::Null))
                .unwrap_or(false),
        )),
        CheckExpression::Binary { op, left, right } => match op {
            CheckBinaryOperator::And => {
                let left = evaluate_check_expression(*left, values)?;
                let right = evaluate_check_expression(*right, values)?;
                Ok(tri_and(left, right))
            }
            CheckBinaryOperator::Or => {
                let left = evaluate_check_expression(*left, values)?;
                let right = evaluate_check_expression(*right, values)?;
                Ok(tri_or(left, right))
            }
            CheckBinaryOperator::Eq
            | CheckBinaryOperator::NotEq
            | CheckBinaryOperator::Lt
            | CheckBinaryOperator::LtEq
            | CheckBinaryOperator::Gt
            | CheckBinaryOperator::GtEq => {
                let left = evaluate_check_scalar(*left, values);
                let right = evaluate_check_scalar(*right, values);
                if let (Some(left), Some(right)) = (left, right) {
                    compare_scalars(op, left, right)
                } else {
                    Ok(None)
                }
            }
        },
    }
}

fn evaluate_check_scalar(
    expr: CheckExpression,
    values: &BTreeMap<String, ScalarValue>,
) -> Option<ScalarValue> {
    match expr {
        CheckExpression::Column(name) => values.get(name.to_ascii_lowercase().as_str()).cloned(),
        CheckExpression::Literal(value) => default_literal_to_scalar(value),
        CheckExpression::Not(_)
        | CheckExpression::IsNull(_)
        | CheckExpression::IsNotNull(_)
        | CheckExpression::Binary {
            op: CheckBinaryOperator::And | CheckBinaryOperator::Or,
            ..
        } => evaluate_check_expression(expr, values)
            .ok()
            .flatten()
            .map(|v| ScalarValue::Boolean(Some(v))),
        CheckExpression::Binary { .. } => evaluate_check_expression(expr, values)
            .ok()
            .flatten()
            .map(|v| ScalarValue::Boolean(Some(v))),
    }
}

fn scalar_to_optional_bool(
    value: &ScalarValue,
) -> std::result::Result<Option<bool>, RowConstraintError> {
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(Some(*v)),
        ScalarValue::Boolean(None) | ScalarValue::Null => Ok(None),
        _ => Err(RowConstraintError::new(
            "42804",
            "CHECK expression expected boolean value",
        )),
    }
}

fn default_literal_to_optional_bool(
    value: ColumnDefaultValue,
) -> std::result::Result<Option<bool>, RowConstraintError> {
    match value {
        ColumnDefaultValue::Boolean(v) => Ok(Some(v)),
        ColumnDefaultValue::Null => Ok(None),
        _ => Err(RowConstraintError::new(
            "42804",
            "CHECK expression expected boolean literal",
        )),
    }
}

fn default_literal_to_scalar(value: ColumnDefaultValue) -> Option<ScalarValue> {
    match value {
        ColumnDefaultValue::Null => None,
        ColumnDefaultValue::Boolean(v) => Some(ScalarValue::Boolean(Some(v))),
        ColumnDefaultValue::Int64(v) => Some(ScalarValue::Int64(Some(v))),
        ColumnDefaultValue::UInt64(v) => Some(ScalarValue::UInt64(Some(v))),
        ColumnDefaultValue::Float64(v) => Some(ScalarValue::Float64(Some(v))),
        ColumnDefaultValue::Utf8(v) => Some(ScalarValue::Utf8(Some(v))),
        ColumnDefaultValue::TimestampNanosecond(v) => {
            Some(ScalarValue::TimestampNanosecond(Some(v), None))
        }
        ColumnDefaultValue::CurrentTimestampNanosecond => None,
    }
}

fn tri_and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn tri_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn compare_scalars(
    op: CheckBinaryOperator,
    left: ScalarValue,
    right: ScalarValue,
) -> std::result::Result<Option<bool>, RowConstraintError> {
    let ordering = compare_scalar_values(left, right)?;
    let outcome = match op {
        CheckBinaryOperator::Eq => ordering.map(|v| v == std::cmp::Ordering::Equal),
        CheckBinaryOperator::NotEq => ordering.map(|v| v != std::cmp::Ordering::Equal),
        CheckBinaryOperator::Lt => ordering.map(|v| v == std::cmp::Ordering::Less),
        CheckBinaryOperator::LtEq => {
            ordering.map(|v| matches!(v, std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
        }
        CheckBinaryOperator::Gt => ordering.map(|v| v == std::cmp::Ordering::Greater),
        CheckBinaryOperator::GtEq => {
            ordering.map(|v| matches!(v, std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
        }
        CheckBinaryOperator::And | CheckBinaryOperator::Or => {
            return Err(RowConstraintError::new(
                "42804",
                "invalid binary operator for scalar comparison",
            ))
        }
    };
    Ok(outcome)
}

fn compare_scalar_values(
    left: ScalarValue,
    right: ScalarValue,
) -> std::result::Result<Option<std::cmp::Ordering>, RowConstraintError> {
    if matches!(left, ScalarValue::Null) || matches!(right, ScalarValue::Null) {
        return Ok(None);
    }

    if let (Some(left), Some(right)) = (scalar_to_f64_lossy(&left), scalar_to_f64_lossy(&right)) {
        return Ok(left.partial_cmp(&right));
    }

    match (&left, &right) {
        (ScalarValue::Utf8(Some(left)), ScalarValue::Utf8(Some(right)))
        | (ScalarValue::Utf8(Some(left)), ScalarValue::LargeUtf8(Some(right)))
        | (ScalarValue::LargeUtf8(Some(left)), ScalarValue::Utf8(Some(right)))
        | (ScalarValue::LargeUtf8(Some(left)), ScalarValue::LargeUtf8(Some(right))) => {
            Ok(Some(left.cmp(right)))
        }
        (ScalarValue::Boolean(Some(left)), ScalarValue::Boolean(Some(right))) => {
            Ok(Some(left.cmp(right)))
        }
        (
            ScalarValue::TimestampNanosecond(Some(left), _),
            ScalarValue::TimestampNanosecond(Some(right), _),
        ) => Ok(Some(left.cmp(right))),
        _ => Err(RowConstraintError::new(
            "42804",
            "CHECK comparison operands are not comparable",
        )),
    }
}

fn scalar_to_f64_lossy(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::Float32(Some(v)) => Some(f64::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Int16(Some(v)) => Some(*v as f64),
        ScalarValue::Int8(Some(v)) => Some(*v as f64),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt32(Some(v)) => Some(*v as f64),
        ScalarValue::UInt16(Some(v)) => Some(*v as f64),
        ScalarValue::UInt8(Some(v)) => Some(*v as f64),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
/// Legacy table metadata shape used before schema-driven column metadata.
struct LegacyTableMetadataRecordV0 {
    db_id: u64,
    schema_id: u64,
    table_id: u64,
    table_name: String,
    #[serde(default)]
    table_model: Option<String>,
    #[serde(default)]
    preferred_shards: Vec<usize>,
    #[serde(default)]
    page_size: Option<usize>,
}

impl LegacyTableMetadataRecordV0 {
    /// Converts legacy metadata into current in-memory representation.
    fn into_current(self) -> TableMetadataRecord {
        TableMetadataRecord {
            db_id: self.db_id,
            schema_id: self.schema_id,
            table_id: self.table_id,
            table_name: self.table_name,
            table_model: self
                .table_model
                .unwrap_or_else(|| TABLE_MODEL_ORDERS_V1.to_string()),
            columns: Vec::new(),
            check_constraints: Vec::new(),
            primary_key_column: None,
            preferred_shards: self.preferred_shards,
            page_size: self.page_size.unwrap_or_else(default_page_size).max(1),
        }
    }
}

/// Applies metadata schema migrations and resumable backfills for existing clusters.
pub async fn ensure_metadata_migration(
    client: &HoloStoreClient,
) -> Result<MetadataMigrationOutcome> {
    let mut state_snapshot = load_metadata_schema_state(client).await?;
    let from_schema_version = state_snapshot.state.schema_version;
    let resumed_from_checkpoint = state_snapshot.state.checkpoint_table_id;

    // Decision: return fast when metadata schema already satisfies this binary.
    if state_snapshot.state.schema_version >= METADATA_SCHEMA_VERSION_CURRENT {
        return Ok(MetadataMigrationOutcome {
            from_schema_version,
            to_schema_version: state_snapshot.state.schema_version,
            resumed_from_checkpoint,
            scanned_tables: 0,
            migrated_tables: 0,
        });
    }

    let (scanned_tables, migrated_tables, checkpoint_after_backfill) =
        backfill_orders_metadata_rows(client, resumed_from_checkpoint).await?;

    // Decision: on fresh clusters with no migration candidates, avoid writing a
    // schema-state marker during bootstrap so single-node startup in a larger
    // quorum configuration does not fail before peers join.
    if scanned_tables == 0 && migrated_tables == 0 && resumed_from_checkpoint.is_none() {
        return Ok(MetadataMigrationOutcome {
            from_schema_version,
            to_schema_version: state_snapshot.state.schema_version,
            resumed_from_checkpoint,
            scanned_tables,
            migrated_tables,
        });
    }

    // Decision: persist a completion checkpoint with upgraded schema version.
    let desired_state = MetadataSchemaStateRecord {
        schema_version: METADATA_SCHEMA_VERSION_CURRENT,
        checkpoint_table_id: None,
        updated_at_unix_ms: now_unix_epoch_millis(),
    };
    state_snapshot = persist_metadata_schema_state_with_retry(client, state_snapshot, &desired_state)
        .await
        .with_context(|| {
            format!(
                "persist metadata schema version={} after backfill (checkpoint={checkpoint_after_backfill:?})",
                METADATA_SCHEMA_VERSION_CURRENT
            )
        })?;

    Ok(MetadataMigrationOutcome {
        from_schema_version,
        to_schema_version: state_snapshot.state.schema_version,
        resumed_from_checkpoint,
        scanned_tables,
        migrated_tables,
    })
}

/// Executes the Phase 8 #3 backfill for legacy `orders_v1` metadata rows.
async fn backfill_orders_metadata_rows(
    client: &HoloStoreClient,
    checkpoint: Option<u64>,
) -> Result<(usize, usize, Option<u64>)> {
    let mut entries = list_table_metadata_entries(client).await?;
    entries.sort_by_key(|entry| entry.record.table_id);

    // Decision: resume from stored checkpoint to avoid rescanning completed ids.
    let resume_after = checkpoint.unwrap_or(0);
    let mut candidates = entries
        .into_iter()
        .filter(|entry| {
            entry.record.table_id > resume_after && needs_orders_metadata_backfill(&entry.record)
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|entry| entry.record.table_id);

    let mut scanned_tables = 0usize;
    let mut migrated_tables = 0usize;
    let mut latest_checkpoint = checkpoint;

    for batch in candidates.chunks(METADATA_MIGRATION_BATCH_SIZE.max(1)) {
        for entry in batch {
            scanned_tables = scanned_tables.saturating_add(1);
            let migrated = backfill_orders_metadata_entry(client, entry).await?;
            if migrated {
                migrated_tables = migrated_tables.saturating_add(1);
            }
            latest_checkpoint = Some(entry.record.table_id);
        }

        // Decision: checkpoint batch progress under baseline schema version so
        // restart can resume from last processed table id.
        let checkpoint_state = MetadataSchemaStateRecord {
            schema_version: METADATA_SCHEMA_VERSION_BASELINE,
            checkpoint_table_id: latest_checkpoint,
            updated_at_unix_ms: now_unix_epoch_millis(),
        };
        let snapshot = load_metadata_schema_state(client).await?;
        let snapshot =
            persist_metadata_schema_state_with_retry(client, snapshot, &checkpoint_state)
                .await
                .context("persist metadata migration checkpoint")?;
        // Decision: stop early when another node already published completion.
        if snapshot.state.schema_version >= METADATA_SCHEMA_VERSION_CURRENT {
            return Ok((scanned_tables, migrated_tables, latest_checkpoint));
        }
    }

    Ok((scanned_tables, migrated_tables, latest_checkpoint))
}

/// Backfills one legacy `orders_v1` metadata row with canonical schema fields.
async fn backfill_orders_metadata_entry(
    client: &HoloStoreClient,
    entry: &TableMetadataEntry,
) -> Result<bool> {
    // Decision: skip no-op rows to keep writes bounded and idempotent.
    if !needs_orders_metadata_backfill(&entry.record) {
        return Ok(false);
    }

    let upgraded = upgraded_orders_metadata_record(&entry.record);
    upgraded.validate()?;
    let value = serde_json::to_vec(&upgraded)
        .with_context(|| format!("encode upgraded metadata for table {}", upgraded.table_name))?;

    let mut expected_version = entry.version;
    for _ in 0..METADATA_MIGRATION_WRITE_RETRY_LIMIT.max(1) {
        let topology = fetch_topology(client).await?;
        require_non_empty_topology(&topology)?;

        // Decision: route each attempt so rebalances during migration do not
        // pin retries to stale leaseholders.
        let route = route_key(&topology, client.target(), &entry.key, &[]).ok_or_else(|| {
            anyhow!(
                "failed to route metadata backfill key for table {}",
                entry.record.table_name
            )
        })?;
        let write_client = HoloStoreClient::new(route.grpc_addr);

        let result = write_client
            .range_write_latest_conditional(
                route.shard_index,
                route.start_key.as_slice(),
                route.end_key.as_slice(),
                vec![ReplicatedConditionalWriteEntry {
                    key: entry.key.clone(),
                    value: value.clone(),
                    expected_version,
                }],
            )
            .await
            .with_context(|| {
                format!(
                    "apply metadata backfill for table '{}' shard={} target={}",
                    entry.record.table_name, route.shard_index, route.grpc_addr
                )
            })?;

        // Decision: conditional apply succeeded; row is now backfilled.
        if result.applied == 1 && result.conflicts == 0 {
            return Ok(true);
        }

        // Decision: on conflict, reload latest row and stop retrying if row is
        // already upgraded by another concurrent migrator.
        let latest = find_table_metadata_by_name(client, entry.record.table_name.as_str())
            .await
            .with_context(|| format!("reload metadata row {}", entry.record.table_name))?;
        if let Some(record) = latest {
            if !needs_orders_metadata_backfill(&record) {
                return Ok(false);
            }
            let latest_entry = find_table_metadata_entry_by_id(
                client,
                record.db_id,
                record.schema_id,
                record.table_id,
            )
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "reloaded metadata entry missing for table {}",
                    record.table_name
                )
            })?;
            expected_version = latest_entry.version;
            continue;
        }
    }

    Err(anyhow!(
        "metadata backfill retries exhausted for table '{}'",
        entry.record.table_name
    ))
}

/// Returns `true` when one `orders_v1` metadata row needs schema backfill.
fn needs_orders_metadata_backfill(record: &TableMetadataRecord) -> bool {
    // Decision: only legacy `orders_v1` rows are migrated in this phase.
    if record.table_model != TABLE_MODEL_ORDERS_V1 {
        return false;
    }
    let canonical_columns = canonical_orders_columns();
    if record.columns != canonical_columns {
        return true;
    }
    match record.primary_key_column.as_deref().map(str::trim) {
        Some("order_id") => {}
        _ => return true,
    }
    record.page_size == 0
}

/// Produces canonical post-migration metadata payload for `orders_v1`.
fn upgraded_orders_metadata_record(record: &TableMetadataRecord) -> TableMetadataRecord {
    let mut upgraded = record.clone();
    upgraded.columns = canonical_orders_columns();
    upgraded.primary_key_column = Some("order_id".to_string());
    upgraded.page_size = upgraded.page_size.max(default_page_size());
    upgraded
}

/// Canonical persisted column metadata for legacy `orders_v1` layout.
fn canonical_orders_columns() -> Vec<TableColumnRecord> {
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

/// Loads persisted metadata schema state with default fallback for legacy clusters.
async fn load_metadata_schema_state(
    client: &HoloStoreClient,
) -> Result<MetadataSchemaStateSnapshot> {
    let state_key = metadata_schema_state_key();
    let latest = read_exact_latest_entry(client, state_key.as_slice())
        .await
        .context("read metadata schema state key")?;
    if let Some(entry) = latest {
        let state: MetadataSchemaStateRecord =
            serde_json::from_slice(&entry.value).context("decode metadata schema state value")?;
        return Ok(MetadataSchemaStateSnapshot {
            version: entry.version,
            state,
        });
    }

    Ok(MetadataSchemaStateSnapshot {
        version: RpcVersion::zero(),
        state: MetadataSchemaStateRecord {
            schema_version: METADATA_SCHEMA_VERSION_BASELINE,
            checkpoint_table_id: None,
            updated_at_unix_ms: now_unix_epoch_millis(),
        },
    })
}

/// Persists metadata schema state via conditional write and conflict reload.
async fn persist_metadata_schema_state_with_retry(
    client: &HoloStoreClient,
    mut snapshot: MetadataSchemaStateSnapshot,
    desired_state: &MetadataSchemaStateRecord,
) -> Result<MetadataSchemaStateSnapshot> {
    // Decision: avoid redundant writes when observed state already satisfies or
    // exceeds desired version.
    if snapshot.state.schema_version >= desired_state.schema_version
        && snapshot.state.schema_version >= METADATA_SCHEMA_VERSION_CURRENT
    {
        return Ok(snapshot);
    }

    let desired_value =
        serde_json::to_vec(desired_state).context("encode metadata schema state value")?;
    let state_key = metadata_schema_state_key();

    for _ in 0..METADATA_MIGRATION_WRITE_RETRY_LIMIT.max(1) {
        let topology = fetch_topology(client).await?;
        require_non_empty_topology(&topology)?;
        let route = route_key(&topology, client.target(), state_key.as_slice(), &[])
            .ok_or_else(|| anyhow!("failed to route metadata schema state key"))?;
        let write_client = HoloStoreClient::new(route.grpc_addr);
        let result = write_client
            .range_write_latest_conditional(
                route.shard_index,
                route.start_key.as_slice(),
                route.end_key.as_slice(),
                vec![ReplicatedConditionalWriteEntry {
                    key: state_key.clone(),
                    value: desired_value.clone(),
                    expected_version: snapshot.version,
                }],
            )
            .await
            .with_context(|| {
                format!(
                    "persist metadata schema state shard={} target={}",
                    route.shard_index, route.grpc_addr
                )
            })?;

        // Decision: apply success means this caller advanced migration state.
        if result.applied == 1 && result.conflicts == 0 {
            let new_version = result
                .applied_versions
                .iter()
                .find(|version| version.key == state_key)
                .map(|version| version.version)
                .unwrap_or(snapshot.version);
            return Ok(MetadataSchemaStateSnapshot {
                version: new_version,
                state: desired_state.clone(),
            });
        }

        // Decision: on conflict, reload latest state and stop retrying if
        // another node already advanced to current schema version.
        snapshot = load_metadata_schema_state(client).await?;
        if snapshot.state.schema_version >= METADATA_SCHEMA_VERSION_CURRENT {
            return Ok(snapshot);
        }
    }

    Err(anyhow!(
        "metadata schema state update retries exhausted (desired_version={})",
        desired_state.schema_version
    ))
}

/// Ensures metadata keyspace is reachable and returns current records.
pub async fn ensure_table_metadata(client: &HoloStoreClient) -> Result<Vec<TableMetadataRecord>> {
    list_table_metadata(client)
        .await
        .context("list table metadata")
}

/// Lists all table metadata records across relevant topology targets.
pub async fn list_table_metadata(client: &HoloStoreClient) -> Result<Vec<TableMetadataRecord>> {
    let entries = list_table_metadata_entries(client).await?;
    Ok(entries.into_iter().map(|entry| entry.record).collect())
}

/// Lists metadata entries with storage key/version for migration workflows.
async fn list_table_metadata_entries(client: &HoloStoreClient) -> Result<Vec<TableMetadataEntry>> {
    let topology = fetch_topology(client).await?;
    require_non_empty_topology(&topology)?;

    let start_key = vec![META_PREFIX_TABLE];
    let end_key = prefix_end(&start_key).unwrap_or_default();
    let targets = scan_targets(&topology, client.target(), &start_key, &end_key, &[]);

    let mut by_key = BTreeMap::<(u64, u64, u64), TableMetadataEntry>::new();
    for target in targets {
        let scan_client = HoloStoreClient::new(target.grpc_addr);
        let mut cursor = Vec::new();
        loop {
            let (entries, next_cursor, done) = scan_client
                .range_snapshot_latest(
                    target.shard_index,
                    target.start_key.as_slice(),
                    target.end_key.as_slice(),
                    cursor.as_slice(),
                    DEFAULT_SCAN_PAGE_SIZE,
                )
                .await
                .with_context(|| {
                    format!(
                        "scan metadata shard={} target={}",
                        target.shard_index, target.grpc_addr
                    )
                })?;

            for entry in entries {
                // Decision: ignore non-metadata keys so scans can safely overlap
                // wider ranges in future layouts.
                // Decision: evaluate `if !entry.key.starts_with(&[META_PREFIX_TABLE]) {` to choose the correct SQL/storage control path.
                if !entry.key.starts_with(&[META_PREFIX_TABLE]) {
                    continue;
                }
                let (db_id, schema_id, table_id) = decode_table_metadata_key(entry.key.as_slice())
                    .ok_or_else(|| anyhow!("invalid metadata key: {}", hex::encode(&entry.key)))?;
                let record =
                    decode_table_metadata_value(entry.value.as_slice()).with_context(|| {
                        format!(
                            "decode table metadata value key={}",
                            hex::encode(&entry.key)
                        )
                    })?;

                // Decision: verify key/value identity fields match so corruption
                // or stale writes cannot silently poison catalog state.
                // Decision: evaluate `if record.db_id != db_id` to choose the correct SQL/storage control path.
                if record.db_id != db_id
                    || record.schema_id != schema_id
                    || record.table_id != table_id
                {
                    return Err(anyhow!(
                        "table metadata key/value mismatch for table {} (key={db_id}/{schema_id}/{table_id}, value={}/{}/{})",
                        record.table_name,
                        record.db_id,
                        record.schema_id,
                        record.table_id
                    ));
                }
                record.validate()?;
                let identity = (db_id, schema_id, table_id);
                // Decision: choose the highest-version row when multiple shard
                // scans race during replication convergence.
                match by_key.get(&identity) {
                    Some(current) if current.version >= entry.version => {}
                    _ => {
                        by_key.insert(
                            identity,
                            TableMetadataEntry {
                                key: entry.key,
                                version: entry.version,
                                record,
                            },
                        );
                    }
                }
            }

            // Decision: finish paging when server indicates completion or when
            // cursor is empty to avoid tight polling loops.
            // Decision: evaluate `if done || next_cursor.is_empty() {` to choose the correct SQL/storage control path.
            if done || next_cursor.is_empty() {
                break;
            }
            cursor = next_cursor;
        }
    }

    Ok(by_key.into_values().collect())
}

/// Looks up a table metadata record by normalized table name.
pub async fn find_table_metadata_by_name(
    client: &HoloStoreClient,
    table_name: &str,
) -> Result<Option<TableMetadataRecord>> {
    let table_name = table_name.trim();
    // Decision: treat empty names as "not found" rather than failing callers
    // that probe optional metadata.
    // Decision: evaluate `if table_name.is_empty() {` to choose the correct SQL/storage control path.
    if table_name.is_empty() {
        return Ok(None);
    }
    let rows = list_table_metadata(client).await?;
    Ok(rows.into_iter().find(|row| row.table_name == table_name))
}

/// Creates metadata for a table if absent and waits for read visibility.
pub async fn create_table_metadata(
    client: &HoloStoreClient,
    table_name: &str,
    if_not_exists: bool,
    spec: &CreateTableMetadataSpec,
) -> Result<CreateTableMetadataOutcome> {
    let table_name = table_name.trim();
    // Decision: fail early on empty names to avoid creating unreachable entries.
    if table_name.is_empty() {
        return Err(anyhow!("table metadata create has empty table_name"));
    }
    spec.validate()?;

    let deadline = tokio::time::Instant::now() + METADATA_CREATE_TIMEOUT;

    loop {
        let existing = list_table_metadata(client).await?;
        // Decision: resolve duplicate creates by honoring `IF NOT EXISTS` and
        // surfacing deterministic "already exists" otherwise.
        // Decision: evaluate `if let Some(record) = existing` to choose the correct SQL/storage control path.
        if let Some(record) = existing
            .iter()
            .find(|row| row.table_name == table_name)
            .cloned()
        {
            // Decision: evaluate `if if_not_exists {` to choose the correct SQL/storage control path.
            if if_not_exists {
                return Ok(CreateTableMetadataOutcome {
                    record,
                    created: false,
                });
            }
            return Err(anyhow!("table '{table_name}' already exists"));
        }

        // Decision: allocate the next table id monotonically from visible state.
        let next_table_id = existing
            .iter()
            .map(|row| row.table_id)
            .max()
            .unwrap_or(0)
            .saturating_add(1);
        // Decision: guard against overflow wrapping back to zero.
        if next_table_id == 0 {
            return Err(anyhow!(
                "invalid table id allocation for table '{table_name}'"
            ));
        }

        let record = TableMetadataRecord {
            db_id: DEFAULT_DB_ID,
            schema_id: DEFAULT_SCHEMA_ID,
            table_id: next_table_id,
            table_name: table_name.to_string(),
            table_model: spec.table_model.clone(),
            columns: spec.columns.clone(),
            check_constraints: spec.check_constraints.clone(),
            primary_key_column: Some(spec.primary_key_column.clone()),
            preferred_shards: spec.preferred_shards.clone(),
            page_size: spec.page_size.max(1),
        };
        record.validate()?;

        let topology = fetch_topology(client).await?;
        require_non_empty_topology(&topology)?;

        let key = encode_table_metadata_key(record.db_id, record.schema_id, record.table_id);
        let value = serde_json::to_vec(&record).context("encode table metadata json")?;
        let route = route_key(&topology, client.target(), &key, &[]).ok_or_else(|| {
            anyhow!(
                "failed to route metadata key for table {}",
                record.table_name
            )
        })?;

        let write_client = HoloStoreClient::new(route.grpc_addr);
        let result = write_client
            .range_write_latest_conditional(
                route.shard_index,
                route.start_key.as_slice(),
                route.end_key.as_slice(),
                vec![ReplicatedConditionalWriteEntry {
                    key: key.clone(),
                    value: value.clone(),
                    expected_version: RpcVersion::zero(),
                }],
            )
            .await
            .with_context(|| format!("persist metadata for table '{}'", record.table_name))?;

        // Decision: evaluate `if result.conflicts > 0 || result.applied != 1 {` to choose the correct SQL/storage control path.
        if result.conflicts > 0 || result.applied != 1 {
            // Decision: concurrent creators may race on id allocation; retry
            // until timeout to provide eventual successful create.
            let err = anyhow!(
                "metadata create race for table '{}' (applied={}, conflicts={})",
                record.table_name,
                result.applied,
                result.conflicts
            );
            // Decision: stop retrying when timeout budget is exhausted.
            if tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(METADATA_CREATE_RETRY_INTERVAL).await;
            continue;
        } else {
            // Decision: after a successful write, poll for visibility so caller
            // can immediately register/use the table.
            let visible = wait_for_table_metadata_by_name(
                client,
                record.table_name.as_str(),
                METADATA_VISIBILITY_TIMEOUT,
            )
            .await?;
            // Decision: evaluate `if let Some(found) = visible {` to choose the correct SQL/storage control path.
            if let Some(found) = visible {
                return Ok(CreateTableMetadataOutcome {
                    record: found,
                    created: true,
                });
            }
            let err = anyhow!(
                "metadata create applied for table '{}' but record was not visible",
                record.table_name
            );
            // Decision: keep retrying visibility races until deadline.
            if tokio::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(METADATA_CREATE_RETRY_INTERVAL).await;
            continue;
        }
    }
}

/// Returns whether a table model tag is recognized by this binary.
pub fn is_supported_table_model(table_model: &str) -> bool {
    matches!(table_model, TABLE_MODEL_ORDERS_V1 | TABLE_MODEL_ROW_V1)
}

/// Returns the canonical `orders_v1` model tag.
pub fn table_model_orders_v1() -> &'static str {
    TABLE_MODEL_ORDERS_V1
}

/// Returns the canonical `row_v1` model tag.
pub fn table_model_row_v1() -> &'static str {
    TABLE_MODEL_ROW_V1
}

/// Decodes one table metadata value, including legacy pre-schema payloads.
fn decode_table_metadata_value(value: &[u8]) -> Result<TableMetadataRecord> {
    // Decision: prefer current schema decode for modern payloads.
    match serde_json::from_slice::<TableMetadataRecord>(value) {
        Ok(record) => Ok(record),
        Err(current_err) => {
            // Decision: fallback to legacy schema to keep existing clusters
            // readable during migration rollout.
            match serde_json::from_slice::<LegacyTableMetadataRecordV0>(value) {
                Ok(legacy) => Ok(legacy.into_current()),
                Err(legacy_err) => Err(anyhow!(
                    "decode metadata value failed (current={current_err}; legacy={legacy_err})"
                )),
            }
        }
    }
}

/// Looks up one metadata entry by identity key.
async fn find_table_metadata_entry_by_id(
    client: &HoloStoreClient,
    db_id: u64,
    schema_id: u64,
    table_id: u64,
) -> Result<Option<TableMetadataEntry>> {
    let key = encode_table_metadata_key(db_id, schema_id, table_id);
    if let Some(entry) = read_exact_latest_entry(client, key.as_slice()).await? {
        let record = decode_table_metadata_value(entry.value.as_slice()).with_context(|| {
            format!(
                "decode metadata value for table key={}",
                hex::encode(key.as_slice())
            )
        })?;
        return Ok(Some(TableMetadataEntry {
            key,
            version: entry.version,
            record,
        }));
    }
    Ok(None)
}

/// Encodes metadata key as `{prefix}{db_id}{schema_id}{table_id}`.
fn encode_table_metadata_key(db_id: u64, schema_id: u64, table_id: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + 8 + 8);
    out.push(META_PREFIX_TABLE);
    out.extend_from_slice(&db_id.to_be_bytes());
    out.extend_from_slice(&schema_id.to_be_bytes());
    out.extend_from_slice(&table_id.to_be_bytes());
    out
}

/// Decodes metadata key components when key shape matches expected format.
fn decode_table_metadata_key(key: &[u8]) -> Option<(u64, u64, u64)> {
    // Decision: strict length/prefix check prevents accidental decoding of
    // unrelated keyspaces.
    // Decision: evaluate `if key.len() != 25 || key.first().copied()? != META_PREFIX_TABLE {` to choose the correct SQL/storage control path.
    if key.len() != 25 || key.first().copied()? != META_PREFIX_TABLE {
        return None;
    }
    let mut db = [0u8; 8];
    db.copy_from_slice(&key[1..9]);
    let mut schema = [0u8; 8];
    schema.copy_from_slice(&key[9..17]);
    let mut table = [0u8; 8];
    table.copy_from_slice(&key[17..25]);
    Some((
        u64::from_be_bytes(db),
        u64::from_be_bytes(schema),
        u64::from_be_bytes(table),
    ))
}

/// Returns the metadata schema-state control key.
fn metadata_schema_state_key() -> Vec<u8> {
    vec![META_PREFIX_CONTROL, META_KEY_SCHEMA_STATE_SUFFIX]
}

/// Computes exclusive end key for a prefix scan.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    for idx in (0..out.len()).rev() {
        // Decision: increment first non-0xFF suffix byte to build smallest
        // lexicographically greater key after the prefix.
        // Decision: evaluate `if out[idx] != 0xFF {` to choose the correct SQL/storage control path.
        if out[idx] != 0xFF {
            out[idx] = out[idx].saturating_add(1);
            out.truncate(idx + 1);
            return Some(out);
        }
    }
    None
}

/// Computes exclusive end bound for an exact-key scan.
fn exact_key_end(key: &[u8]) -> Vec<u8> {
    let mut end = key.to_vec();
    end.push(0x00);
    end
}

/// Reads the newest visible value for one exact key across scan targets.
async fn read_exact_latest_entry(
    client: &HoloStoreClient,
    key: &[u8],
) -> Result<Option<holo_store::LatestEntry>> {
    let topology = fetch_topology(client).await?;
    require_non_empty_topology(&topology)?;

    let end = exact_key_end(key);
    let targets = scan_targets(&topology, client.target(), key, end.as_slice(), &[]);

    let mut latest: Option<holo_store::LatestEntry> = None;
    for target in targets {
        let scan_client = HoloStoreClient::new(target.grpc_addr);
        let (entries, _, _) = scan_client
            .range_snapshot_latest(
                target.shard_index,
                target.start_key.as_slice(),
                target.end_key.as_slice(),
                &[],
                DEFAULT_SCAN_PAGE_SIZE,
            )
            .await
            .with_context(|| {
                format!(
                    "scan exact metadata key={} shard={} target={}",
                    hex::encode(key),
                    target.shard_index,
                    target.grpc_addr
                )
            })?;
        for entry in entries {
            if entry.key != key {
                continue;
            }
            match &latest {
                Some(current) if current.version >= entry.version => {}
                _ => latest = Some(entry),
            }
        }
    }

    Ok(latest)
}

/// Returns current Unix epoch milliseconds.
fn now_unix_epoch_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(u64::MAX as u128) as u64,
        Err(_) => 0,
    }
}

/// Polls until metadata for `table_name` is observable or timeout elapses.
async fn wait_for_table_metadata_by_name(
    client: &HoloStoreClient,
    table_name: &str,
    timeout: Duration,
) -> Result<Option<TableMetadataRecord>> {
    let table_name = table_name.trim();
    // Decision: empty table names are treated as absent and return immediately.
    if table_name.is_empty() {
        return Ok(None);
    }

    let deadline = tokio::time::Instant::now() + timeout.max(Duration::from_millis(1));
    let mut last_err: Option<anyhow::Error> = None;
    loop {
        // Decision: evaluate `match find_table_metadata_by_name(client, table_name).await {` to choose the correct SQL/storage control path.
        match find_table_metadata_by_name(client, table_name).await {
            Ok(Some(record)) => return Ok(Some(record)),
            Ok(None) => {}
            Err(err) => last_err = Some(err),
        }

        // Decision: stop polling when timeout is reached.
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Decision: surface the most recent lookup error if visibility was blocked
    // by repeated read failures.
    // Decision: evaluate `if let Some(err) = last_err {` to choose the correct SQL/storage control path.
    if let Some(err) = last_err {
        return Err(anyhow!(
            "table metadata for '{}' did not become visible before timeout: {err}",
            table_name
        ));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::net::{SocketAddr, TcpListener};
    use std::time::Duration;

    use anyhow::{Context, Result};
    use holo_store::{start_embedded_node, EmbeddedNodeConfig, ReplicatedWriteEntry};
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn decode_legacy_metadata_defaults_orders_model() -> Result<()> {
        let payload = serde_json::json!({
            "db_id": 1u64,
            "schema_id": 1u64,
            "table_id": 42u64,
            "table_name": "orders_legacy"
        });
        let decoded = decode_table_metadata_value(payload.to_string().as_bytes())?;
        assert_eq!(decoded.table_model, table_model_orders_v1());
        assert!(decoded.columns.is_empty());
        assert!(decoded.primary_key_column.is_none());
        assert_eq!(decoded.page_size, default_page_size());
        Ok(())
    }

    #[test]
    fn upgraded_orders_metadata_sets_canonical_fields() -> Result<()> {
        let legacy = TableMetadataRecord {
            db_id: 1,
            schema_id: 1,
            table_id: 7,
            table_name: "orders".to_string(),
            table_model: table_model_orders_v1().to_string(),
            columns: Vec::new(),
            check_constraints: Vec::new(),
            primary_key_column: None,
            preferred_shards: Vec::new(),
            page_size: 0,
        };
        assert!(needs_orders_metadata_backfill(&legacy));
        let upgraded = upgraded_orders_metadata_record(&legacy);
        assert_eq!(upgraded.columns, canonical_orders_columns());
        assert_eq!(upgraded.primary_key_column.as_deref(), Some("order_id"));
        assert!(upgraded.page_size >= default_page_size());
        upgraded.validate()?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metadata_migration_backfills_legacy_orders_rows() -> Result<()> {
        let temp_dir = TempDir::new().context("create temp dir for metadata migration test")?;
        let redis_addr: SocketAddr = format!("127.0.0.1:{}", free_port()?)
            .parse()
            .context("parse redis addr")?;
        let grpc_addr: SocketAddr = format!("127.0.0.1:{}", free_port()?)
            .parse()
            .context("parse grpc addr")?;

        let node = start_embedded_node(EmbeddedNodeConfig {
            node_id: 1,
            listen_redis: redis_addr,
            listen_grpc: grpc_addr,
            bootstrap: true,
            join: None,
            initial_members: format!("1@{grpc_addr}"),
            data_dir: temp_dir.path().join("node-1"),
            ready_timeout: Duration::from_secs(20),
            max_shards: 1,
            initial_ranges: 1,
            routing_mode: Some("range".to_string()),
        })
        .await
        .context("start embedded holostore for migration test")?;

        let client = HoloStoreClient::new(grpc_addr);
        let created = create_table_metadata(
            &client,
            "orders",
            false,
            &CreateTableMetadataSpec {
                table_model: table_model_orders_v1().to_string(),
                columns: canonical_orders_columns(),
                check_constraints: Vec::new(),
                primary_key_column: "order_id".to_string(),
                preferred_shards: Vec::new(),
                page_size: default_page_size(),
            },
        )
        .await
        .context("create baseline orders metadata")?;

        let legacy_payload = serde_json::json!({
            "db_id": created.record.db_id,
            "schema_id": created.record.schema_id,
            "table_id": created.record.table_id,
            "table_name": created.record.table_name,
            "table_model": table_model_orders_v1()
        })
        .to_string()
        .into_bytes();
        let metadata_key = encode_table_metadata_key(
            created.record.db_id,
            created.record.schema_id,
            created.record.table_id,
        );
        let topology = fetch_topology(&client).await?;
        let route = route_key(&topology, client.target(), metadata_key.as_slice(), &[])
            .context("route legacy metadata rewrite key")?;
        let write_client = HoloStoreClient::new(route.grpc_addr);
        let rewritten = write_client
            .range_write_latest(
                route.shard_index,
                route.start_key.as_slice(),
                route.end_key.as_slice(),
                vec![ReplicatedWriteEntry {
                    key: metadata_key.clone(),
                    value: legacy_payload,
                }],
            )
            .await
            .context("rewrite metadata row to legacy payload")?;
        assert_eq!(rewritten, 1);

        let before = find_table_metadata_by_name(&client, "orders")
            .await?
            .context("orders metadata should exist before migration")?;
        assert!(needs_orders_metadata_backfill(&before));

        let first = ensure_metadata_migration(&client)
            .await
            .context("run first metadata migration pass")?;
        assert_eq!(
            first.to_schema_version, METADATA_SCHEMA_VERSION_CURRENT,
            "metadata migration should advance schema version"
        );
        assert!(
            first.migrated_tables >= 1,
            "expected at least one migrated row, got {}",
            first.migrated_tables
        );

        let after = find_table_metadata_by_name(&client, "orders")
            .await?
            .context("orders metadata should exist after migration")?;
        assert_eq!(after.columns, canonical_orders_columns());
        assert_eq!(after.primary_key_column.as_deref(), Some("order_id"));

        let second = ensure_metadata_migration(&client)
            .await
            .context("run second metadata migration pass")?;
        assert_eq!(second.migrated_tables, 0);
        assert_eq!(
            second.to_schema_version, METADATA_SCHEMA_VERSION_CURRENT,
            "second pass should stay on current schema version"
        );

        node.shutdown()
            .await
            .context("shutdown migration test node")?;
        Ok(())
    }

    #[test]
    fn row_defaults_and_checks_apply_and_validate() -> Result<()> {
        let columns = vec![
            TableColumnRecord {
                name: "id".to_string(),
                column_type: TableColumnType::Int64,
                nullable: false,
                default_value: None,
            },
            TableColumnRecord {
                name: "status".to_string(),
                column_type: TableColumnType::Utf8,
                nullable: false,
                default_value: Some(ColumnDefaultValue::Utf8("new".to_string())),
            },
            TableColumnRecord {
                name: "qty".to_string(),
                column_type: TableColumnType::Int64,
                nullable: false,
                default_value: Some(ColumnDefaultValue::Int64(0)),
            },
            TableColumnRecord {
                name: "created_at".to_string(),
                column_type: TableColumnType::TimestampNanosecond,
                nullable: false,
                default_value: Some(ColumnDefaultValue::CurrentTimestampNanosecond),
            },
        ];
        let checks = vec![
            TableCheckConstraintRecord {
                name: "qty_non_negative".to_string(),
                expr: CheckExpression::Binary {
                    op: CheckBinaryOperator::GtEq,
                    left: Box::new(CheckExpression::Column("qty".to_string())),
                    right: Box::new(CheckExpression::Literal(ColumnDefaultValue::Int64(0))),
                },
            },
            TableCheckConstraintRecord {
                name: "status_not_empty".to_string(),
                expr: CheckExpression::Binary {
                    op: CheckBinaryOperator::NotEq,
                    left: Box::new(CheckExpression::Column("status".to_string())),
                    right: Box::new(CheckExpression::Literal(ColumnDefaultValue::Utf8(
                        "".to_string(),
                    ))),
                },
            },
        ];

        validate_table_constraints(columns.as_slice(), checks.as_slice())?;

        let mut row = vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Null,
            ScalarValue::Null,
            ScalarValue::Null,
        ];
        let missing = vec![false, true, true, true];
        apply_column_defaults_for_missing(
            columns.as_slice(),
            row.as_mut_slice(),
            missing.as_slice(),
            1_700_000_000_000_000_000,
        )?;
        validate_row_against_metadata(columns.as_slice(), checks.as_slice(), row.as_slice())?;

        assert_eq!(row[1], ScalarValue::Utf8(Some("new".to_string())));
        assert_eq!(row[2], ScalarValue::Int64(Some(0)));
        assert!(matches!(
            row[3],
            ScalarValue::TimestampNanosecond(Some(_), _)
        ));
        Ok(())
    }

    #[test]
    fn row_check_violation_reports_23514() -> Result<()> {
        let columns = vec![
            TableColumnRecord {
                name: "id".to_string(),
                column_type: TableColumnType::Int64,
                nullable: false,
                default_value: None,
            },
            TableColumnRecord {
                name: "qty".to_string(),
                column_type: TableColumnType::Int64,
                nullable: false,
                default_value: None,
            },
        ];
        let checks = vec![TableCheckConstraintRecord {
            name: "qty_non_negative".to_string(),
            expr: CheckExpression::Binary {
                op: CheckBinaryOperator::GtEq,
                left: Box::new(CheckExpression::Column("qty".to_string())),
                right: Box::new(CheckExpression::Literal(ColumnDefaultValue::Int64(0))),
            },
        }];

        validate_table_constraints(columns.as_slice(), checks.as_slice())?;
        let err = validate_row_against_metadata(
            columns.as_slice(),
            checks.as_slice(),
            &[ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(-5))],
        )
        .expect_err("negative qty should violate CHECK");
        assert_eq!(err.sqlstate(), "23514");
        Ok(())
    }

    fn free_port() -> Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0").context("bind ephemeral port")?;
        Ok(listener
            .local_addr()
            .context("read ephemeral port addr")?
            .port())
    }
}
