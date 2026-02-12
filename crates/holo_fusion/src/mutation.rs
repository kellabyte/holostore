//! PostgreSQL DML/query hook layer for HoloFusion.
//!
//! This module intercepts SQL statements, routes transactional commands,
//! validates supported DML/DDL forms, and translates them into provider
//! operations against HoloStore-backed tables.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Builder, StringBuilder, TimestampNanosecondBuilder};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::MemTable;
use datafusion::common::{ParamValues, ScalarValue, ToDFSchema};
use datafusion::logical_expr::expr::Placeholder;
use datafusion::logical_expr::{Expr as DfExpr, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, ColumnDef, ColumnOption, CreateTable,
    CreateTableOptions, DataType, Delete, Expr as SqlExpr, FromTable, IndexColumn, Insert,
    ObjectName, SetExpr, Statement, TableConstraint, TableFactor, TableObject, TableWithJoins,
    TimezoneInfo, UnaryOperator, ValueWithSpan,
};
use datafusion_postgres::arrow_pg::datatypes::df;
use datafusion_postgres::hooks::QueryHook;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::types::format::FormatOptions;
use holo_store::{HoloStoreClient, RpcVersion};

use crate::catalog::register_table_from_metadata;
use crate::metadata::create_table_metadata;
use crate::metrics::PushdownMetrics;
use crate::provider::{
    encode_orders_row_value, encode_orders_tombstone_value, orders_schema, ConditionalOrderWrite,
    ConditionalWriteOutcome, HoloStoreTableProvider, OrdersSeedRow, VersionedOrdersRow,
};

#[derive(Debug, Clone, Copy)]
/// Represents the `DmlRuntimeConfig` component used by the holo_fusion runtime.
pub struct DmlRuntimeConfig {
    pub prewrite_delay: Duration,
}

impl Default for DmlRuntimeConfig {
    /// Executes `default` for this component.
    fn default() -> Self {
        Self {
            prewrite_delay: Duration::ZERO,
        }
    }
}

#[derive(Debug)]
/// Represents the `DmlHook` component used by the holo_fusion runtime.
pub struct DmlHook {
    config: DmlRuntimeConfig,
    tx_manager: Arc<TxnSessionManager>,
    holostore_client: HoloStoreClient,
    pushdown_metrics: Arc<PushdownMetrics>,
}

impl DmlHook {
    /// Executes `new` for this component.
    pub fn new(
        config: DmlRuntimeConfig,
        holostore_client: HoloStoreClient,
        pushdown_metrics: Arc<PushdownMetrics>,
    ) -> Self {
        Self {
            config,
            tx_manager: Arc::new(TxnSessionManager::default()),
            holostore_client,
            pushdown_metrics,
        }
    }

    /// Executes `session id for client` for this component.
    fn session_id_for_client(&self, client: &mut (dyn ClientInfo + Send + Sync)) -> String {
        // Decision: evaluate `let Some(existing) = client.metadata().get(SESSION_TXN_ID_KEY)` to choose the correct SQL/storage control path.
        if let Some(existing) = client.metadata().get(SESSION_TXN_ID_KEY) {
            return existing.clone();
        }

        let next = self
            .tx_manager
            .next_session_id
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        let (pid, secret) = client.pid_and_secret_key();
        let session_id = format!("{pid}:{secret:?}:{next}");
        client
            .metadata_mut()
            .insert(SESSION_TXN_ID_KEY.to_string(), session_id.clone());
        session_id
    }

    /// Executes `route statement` for this component.
    async fn route_statement(
        &self,
        statement: &Statement,
        params: &ParamValues,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
        protocol: QueryProtocol,
    ) -> Option<PgWireResult<Response>> {
        let session_id = self.session_id_for_client(client);

        // Decision: evaluate `is_txn_control_statement(statement)` to choose the correct SQL/storage control path.
        if is_txn_control_statement(statement) {
            return Some(
                self.handle_txn_control(statement, session_context, session_id)
                    .await,
            );
        }

        // Decision: evaluate `let Some(result) = self` to choose the correct SQL/storage control path.
        if let Some(result) = self
            .route_transactional_statement(
                statement,
                params,
                &session_id,
                session_context,
                client,
                protocol,
            )
            .await
        {
            return Some(result);
        }

        // Decision: evaluate `let Statement::CreateTable(create) = statement` to choose the correct SQL/storage control path.
        if let Statement::CreateTable(create) = statement {
            return Some(
                execute_create_table(
                    create,
                    session_context,
                    self.holostore_client.clone(),
                    self.pushdown_metrics.clone(),
                )
                .await,
            );
        }

        // Decision: evaluate `!matches!(statement, Statement::Update { .. } | Statement::Delete(_))` to choose the correct SQL/storage control path.
        if !matches!(statement, Statement::Update { .. } | Statement::Delete(_)) {
            return None;
        }

        Some(execute_dml(statement, params, session_context, self.config).await)
    }

    /// Executes `route transactional statement` for this component.
    async fn route_transactional_statement(
        &self,
        statement: &Statement,
        params: &ParamValues,
        session_id: &str,
        session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
        protocol: QueryProtocol,
    ) -> Option<PgWireResult<Response>> {
        let state = {
            let sessions = self.tx_manager.sessions.lock().await;
            sessions.get(session_id).cloned()
        };

        let Some(state) = state else {
            return None;
        };

        // Decision: evaluate `matches!(state, SessionTxnState::Aborted)` to choose the correct SQL/storage control path.
        if matches!(state, SessionTxnState::Aborted) {
            return Some(Err(aborted_tx_error()));
        }

        // Decision: evaluate `statement` to choose the correct SQL/storage control path.
        let result = match statement {
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning,
                or,
            } => {
                execute_update_in_transaction(
                    table,
                    assignments,
                    from.is_some(),
                    selection.as_ref(),
                    returning.is_some(),
                    or.is_some(),
                    params,
                    session_context,
                    &self.tx_manager,
                    session_id,
                )
                .await
            }
            Statement::Delete(delete) => {
                execute_delete_in_transaction(
                    delete,
                    params,
                    session_context,
                    &self.tx_manager,
                    session_id,
                )
                .await
            }
            Statement::Insert(insert) => {
                execute_insert_in_transaction(
                    insert,
                    params,
                    session_context,
                    &self.tx_manager,
                    session_id,
                )
                .await
            }
            Statement::Query(_) => {
                execute_query_in_transaction(
                    statement,
                    &self.tx_manager,
                    session_id,
                    session_context,
                    client,
                    protocol,
                )
                .await
            }
            _ => Err(to_user_error(
                "0A000",
                "statement is not supported inside explicit transactions",
            )),
        };

        Some(result)
    }

    /// Executes `handle txn control` for this component.
    async fn handle_txn_control(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        session_id: String,
    ) -> PgWireResult<Response> {
        // Decision: evaluate `statement` to choose the correct SQL/storage control path.
        match statement {
            Statement::StartTransaction {
                modes,
                transaction,
                modifier,
                ..
            } => {
                // Decision: evaluate `!modes.is_empty() || transaction.is_some() || modifier.is_some()` to choose the correct SQL/storage control path.
                if !modes.is_empty() || transaction.is_some() || modifier.is_some() {
                    return Err(to_user_error(
                        "0A000",
                        "transaction options are not supported yet",
                    ));
                }

                {
                    let sessions = self.tx_manager.sessions.lock().await;
                    // Decision: evaluate `sessions.contains_key(&session_id)` to choose the correct SQL/storage control path.
                    if sessions.contains_key(&session_id) {
                        return Err(to_user_error("25001", "transaction already in progress"));
                    }
                }

                {
                    let mut sessions = self.tx_manager.sessions.lock().await;
                    // Decision: evaluate `sessions.contains_key(&session_id)` to choose the correct SQL/storage control path.
                    if sessions.contains_key(&session_id) {
                        return Err(to_user_error("25001", "transaction already in progress"));
                    }
                    sessions.insert(session_id, SessionTxnState::Active(TxnContext::default()));
                }
                Ok(Response::TransactionStart(Tag::new("BEGIN")))
            }
            Statement::Commit { chain, .. } => {
                // Decision: evaluate `*chain` to choose the correct SQL/storage control path.
                if *chain {
                    return Err(to_user_error("0A000", "COMMIT AND CHAIN is not supported"));
                }

                let state = {
                    let mut sessions = self.tx_manager.sessions.lock().await;
                    sessions.remove(&session_id)
                };

                let Some(state) = state else {
                    return Err(to_user_error(
                        "25P01",
                        "COMMIT called without an active transaction",
                    ));
                };

                let SessionTxnState::Active(txn) = state else {
                    return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
                };

                let writes = txn.as_conditional_writes();
                // Decision: evaluate `writes.is_empty()` to choose the correct SQL/storage control path.
                if writes.is_empty() {
                    return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
                }
                let table_name = txn.table_name.ok_or_else(|| {
                    api_error("transaction has pending writes but no bound target table")
                })?;

                let provider = get_provider_for_table(session_context, table_name.as_str()).await?;
                maybe_prewrite_delay(self.config).await;
                // Decision: evaluate `provider` to choose the correct SQL/storage control path.
                match provider
                    .apply_orders_writes_conditional(&writes, "txn_commit")
                    .await
                    .map_err(|err| api_error(err.to_string()))?
                {
                    ConditionalWriteOutcome::Applied(_) => {
                        Ok(Response::TransactionEnd(Tag::new("COMMIT")))
                    }
                    ConditionalWriteOutcome::Conflict => {
                        let mut sessions = self.tx_manager.sessions.lock().await;
                        sessions.insert(session_id, SessionTxnState::Aborted);
                        Err(to_user_error(
                            "40001",
                            "transaction commit conflict; ROLLBACK required before retry",
                        ))
                    }
                }
            }
            Statement::Rollback { chain, savepoint } => {
                // Decision: evaluate `*chain` to choose the correct SQL/storage control path.
                if *chain {
                    return Err(to_user_error(
                        "0A000",
                        "ROLLBACK AND CHAIN is not supported",
                    ));
                }
                // Decision: evaluate `savepoint.is_some()` to choose the correct SQL/storage control path.
                if savepoint.is_some() {
                    return Err(to_user_error(
                        "0A000",
                        "ROLLBACK TO SAVEPOINT is not supported",
                    ));
                }

                let removed = {
                    let mut sessions = self.tx_manager.sessions.lock().await;
                    sessions.remove(&session_id)
                };

                // Decision: evaluate `removed.is_none()` to choose the correct SQL/storage control path.
                if removed.is_none() {
                    return Err(to_user_error(
                        "25P01",
                        "ROLLBACK called without an active transaction",
                    ));
                }

                Ok(Response::TransactionEnd(Tag::new("ROLLBACK")))
            }
            _ => Err(api_error("unexpected transaction control statement")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Enumerates states/variants for `QueryProtocol`.
enum QueryProtocol {
    Simple,
    Extended,
}

const SESSION_TXN_ID_KEY: &str = "holo_fusion.txn.session_id";

#[derive(Debug, Default)]
/// Represents the `TxnSessionManager` component used by the holo_fusion runtime.
struct TxnSessionManager {
    next_session_id: AtomicU64,
    sessions: tokio::sync::Mutex<HashMap<String, SessionTxnState>>,
}

#[derive(Debug, Clone)]
/// Enumerates states/variants for `SessionTxnState`.
enum SessionTxnState {
    Active(TxnContext),
    Aborted,
}

#[derive(Debug, Clone, Default)]
/// Represents the `TxnContext` component used by the holo_fusion runtime.
struct TxnContext {
    table_name: Option<String>,
    snapshot_rows: BTreeMap<i64, VersionedSnapshotRow>,
    pending_writes: BTreeMap<i64, PendingTxnWrite>,
}

#[derive(Debug, Clone)]
/// Represents the `VersionedSnapshotRow` component used by the holo_fusion runtime.
struct VersionedSnapshotRow {
    row: OrdersSeedRow,
    version: RpcVersion,
}

#[derive(Debug, Clone)]
/// Represents the `PendingTxnWrite` component used by the holo_fusion runtime.
struct PendingTxnWrite {
    row: Option<OrdersSeedRow>,
    value: Vec<u8>,
    expected_version: RpcVersion,
    rollback_value: Vec<u8>,
}

impl TxnContext {
    /// Executes `bind snapshot` for this component.
    fn bind_snapshot(&mut self, table_name: String, rows: Vec<VersionedOrdersRow>) {
        self.table_name = Some(table_name);
        let snapshot_rows = rows
            .into_iter()
            .map(|row| {
                (
                    row.row.order_id,
                    VersionedSnapshotRow {
                        row: row.row,
                        version: row.version,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        self.snapshot_rows = snapshot_rows;
        self.pending_writes.clear();
    }

    /// Executes `bound table name` for this component.
    fn bound_table_name(&self) -> Option<&str> {
        self.table_name.as_deref()
    }

    /// Executes `ensure same table` for this component.
    fn ensure_same_table(&self, table_name: &str) -> PgWireResult<()> {
        // Decision: evaluate `let Some(bound) = self.table_name.as_deref()` to choose the correct SQL/storage control path.
        if let Some(bound) = self.table_name.as_deref() {
            // Decision: evaluate `bound != table_name` to choose the correct SQL/storage control path.
            if bound != table_name {
                return Err(to_user_error(
                    "0A000",
                    format!(
                        "multi-table explicit transactions are not supported (bound table='{bound}', attempted table='{table_name}')"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Executes `current row` for this component.
    fn current_row(&self, order_id: i64) -> Option<OrdersSeedRow> {
        // Decision: evaluate `let Some(write) = self.pending_writes.get(&order_id)` to choose the correct SQL/storage control path.
        if let Some(write) = self.pending_writes.get(&order_id) {
            return write.row.clone();
        }
        self.snapshot_rows.get(&order_id).map(|row| row.row.clone())
    }

    /// Executes `rows matching bounds` for this component.
    fn rows_matching_bounds(&self, bounds: OrderIdBounds) -> Vec<OrdersSeedRow> {
        let mut keys = BTreeSet::new();
        keys.extend(self.snapshot_rows.keys().copied());
        keys.extend(self.pending_writes.keys().copied());

        let mut rows = Vec::new();
        for key in keys {
            // Decision: evaluate `!bounds.matches(key)` to choose the correct SQL/storage control path.
            if !bounds.matches(key) {
                continue;
            }
            // Decision: evaluate `let Some(row) = self.current_row(key)` to choose the correct SQL/storage control path.
            if let Some(row) = self.current_row(key) {
                rows.push(row);
            }
        }
        rows
    }

    /// Executes `visible rows for query` for this component.
    fn visible_rows_for_query(&self) -> Vec<OrdersSeedRow> {
        let mut keys = BTreeSet::new();
        keys.extend(self.snapshot_rows.keys().copied());
        keys.extend(self.pending_writes.keys().copied());

        let mut rows = Vec::new();
        for key in keys {
            // Decision: evaluate `let Some(row) = self.current_row(key)` to choose the correct SQL/storage control path.
            if let Some(row) = self.current_row(key) {
                rows.push(row);
            }
        }
        rows
    }

    /// Executes `stage row for order` for this component.
    fn stage_row_for_order(&mut self, order_id: i64, row: Option<OrdersSeedRow>) {
        let (expected_version, rollback_value) =
            // Decision: evaluate `let Some(existing) = self.pending_writes.get(&order_id)` to choose the correct SQL/storage control path.
            if let Some(existing) = self.pending_writes.get(&order_id) {
                (existing.expected_version, existing.rollback_value.clone())
            // Decision: evaluate `let Some(base) = self.snapshot_rows.get(&order_id)` to choose the correct SQL/storage control path.
            } else if let Some(base) = self.snapshot_rows.get(&order_id) {
                (base.version, encode_orders_row_value(&base.row))
            } else {
                (RpcVersion::zero(), encode_orders_tombstone_value())
            };

        // Decision: evaluate `row.as_ref()` to choose the correct SQL/storage control path.
        let value = match row.as_ref() {
            Some(row) => encode_orders_row_value(row),
            None => encode_orders_tombstone_value(),
        };

        self.pending_writes.insert(
            order_id,
            PendingTxnWrite {
                row,
                value,
                expected_version,
                rollback_value,
            },
        );
    }

    /// Executes `as conditional writes` for this component.
    fn as_conditional_writes(&self) -> Vec<ConditionalOrderWrite> {
        self.pending_writes
            .iter()
            .map(|(order_id, write)| ConditionalOrderWrite {
                order_id: *order_id,
                expected_version: write.expected_version,
                value: write.value.clone(),
                rollback_value: write.rollback_value.clone(),
            })
            .collect()
    }
}

#[async_trait]
impl QueryHook for DmlHook {
    /// Executes `handle simple query` for this component.
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        let params = ParamValues::List(Vec::new());
        self.route_statement(
            statement,
            &params,
            session_context,
            client,
            QueryProtocol::Simple,
        )
        .await
    }

    /// Executes `handle extended parse query` for this component.
    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // Decision: evaluate `matches!(` to choose the correct SQL/storage control path.
        if matches!(
            statement,
            Statement::Update { .. } | Statement::Delete(_) | Statement::CreateTable(_)
        ) {
            Some(dml_placeholder_plan(statement))
        } else {
            None
        }
    }

    /// Executes `handle extended query` for this component.
    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        params: &ParamValues,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        self.route_statement(
            statement,
            params,
            session_context,
            client,
            QueryProtocol::Extended,
        )
        .await
    }
}

/// Executes `is txn control statement` for this component.
fn is_txn_control_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}

/// Executes `execute dml` for this component.
async fn execute_dml(
    statement: &Statement,
    params: &ParamValues,
    session_context: &SessionContext,
    config: DmlRuntimeConfig,
) -> PgWireResult<Response> {
    // Decision: evaluate `statement` to choose the correct SQL/storage control path.
    match statement {
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            or,
        } => {
            execute_update(
                table,
                assignments,
                from.is_some(),
                selection.as_ref(),
                returning.is_some(),
                or.is_some(),
                params,
                session_context,
                config,
            )
            .await
        }
        Statement::Delete(delete) => execute_delete(delete, params, session_context, config).await,
        _ => Err(PgWireError::ApiError(Box::new(std::io::Error::other(
            "unsupported dml statement in hook",
        )))),
    }
}

/// Executes `execute create table` for this component.
async fn execute_create_table(
    create: &CreateTable,
    session_context: &SessionContext,
    holostore_client: HoloStoreClient,
    pushdown_metrics: Arc<PushdownMetrics>,
) -> PgWireResult<Response> {
    validate_supported_create_table(create)?;
    let table_name = parse_create_table_name(&create.name)?;
    validate_orders_v1_table_definition(create, table_name.as_str())?;

    let outcome =
        create_table_metadata(&holostore_client, table_name.as_str(), create.if_not_exists)
            .await
            .map_err(map_create_table_metadata_error)?;

    register_table_from_metadata(
        session_context,
        holostore_client,
        pushdown_metrics,
        &outcome.record,
    )
    .await
    .map_err(|err| api_error(format!("register created table '{}': {err}", table_name)))?;

    Ok(Response::Execution(Tag::new("CREATE TABLE")))
}

/// Executes `validate supported create table` for this component.
fn validate_supported_create_table(create: &CreateTable) -> PgWireResult<()> {
    // Decision: evaluate `create.or_replace` to choose the correct SQL/storage control path.
    if create.or_replace {
        return Err(to_user_error(
            "0A000",
            "CREATE OR REPLACE TABLE is not supported",
        ));
    }
    // Decision: evaluate `create.temporary` to choose the correct SQL/storage control path.
    if create.temporary
        || create.external
        || create.global.is_some()
        || create.transient
        || create.volatile
    {
        return Err(to_user_error(
            "0A000",
            "temporary/external/global table variants are not supported",
        ));
    }
    // Decision: evaluate `create.iceberg` to choose the correct SQL/storage control path.
    if create.iceberg {
        return Err(to_user_error(
            "0A000",
            "ICEBERG CREATE TABLE is not supported in current HoloFusion scope",
        ));
    }
    // Decision: evaluate `create.query.is_some()` to choose the correct SQL/storage control path.
    if create.query.is_some() {
        return Err(to_user_error(
            "0A000",
            "CREATE TABLE AS SELECT is not supported in current HoloFusion scope",
        ));
    }
    // Decision: evaluate `create.like.is_some() || create.clone.is_some()` to choose the correct SQL/storage control path.
    if create.like.is_some() || create.clone.is_some() {
        return Err(to_user_error(
            "0A000",
            "CREATE TABLE LIKE/CLONE is not supported in current HoloFusion scope",
        ));
    }
    // Decision: evaluate `&create.table_options` to choose the correct SQL/storage control path.
    match &create.table_options {
        CreateTableOptions::None => {}
        CreateTableOptions::With(options)
        | CreateTableOptions::Options(options)
        | CreateTableOptions::Plain(options)
        | CreateTableOptions::TableProperties(options)
            // Decision: evaluate `options.is_empty() => {}` to choose the correct SQL/storage control path.
            if options.is_empty() => {}
        _ => {
            return Err(to_user_error(
                "0A000",
                "CREATE TABLE options are not supported in current HoloFusion scope",
            ));
        }
    }
    // Decision: evaluate `create.comment.is_some()` to choose the correct SQL/storage control path.
    if create.comment.is_some()
        || create.on_commit.is_some()
        || create.on_cluster.is_some()
        || create.order_by.is_some()
        || create.partition_by.is_some()
        || create.cluster_by.is_some()
        || create.clustered_by.is_some()
        || create.inherits.is_some()
        || create.strict
        || create.copy_grants
        || create.enable_schema_evolution.is_some()
        || create.change_tracking.is_some()
        || create.data_retention_time_in_days.is_some()
        || create.max_data_extension_time_in_days.is_some()
        || create.default_ddl_collation.is_some()
        || create.with_aggregation_policy.is_some()
        || create.with_row_access_policy.is_some()
        || create.with_tags.is_some()
        || create.external_volume.is_some()
        || create.base_location.is_some()
        || create.catalog.is_some()
        || create.catalog_sync.is_some()
        || create.storage_serialization_policy.is_some()
    {
        return Err(to_user_error(
            "0A000",
            "CREATE TABLE clause is not supported in current HoloFusion scope",
        ));
    }

    // Decision: evaluate `create.columns.is_empty()` to choose the correct SQL/storage control path.
    if create.columns.is_empty() {
        return Err(to_user_error(
            "0A000",
            "CREATE TABLE requires explicit column definitions",
        ));
    }

    Ok(())
}

/// Executes `parse create table name` for this component.
fn parse_create_table_name(name: &ObjectName) -> PgWireResult<String> {
    let parts = name
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(normalize_ident)
                .ok_or_else(|| to_user_error("42601", "invalid object name"))
        })
        .collect::<PgWireResult<Vec<_>>>()?;

    // Decision: evaluate `parts.as_slice()` to choose the correct SQL/storage control path.
    let table_name = match parts.as_slice() {
        [table] => table.clone(),
        [schema, table] => {
            // Decision: evaluate `schema != "public"` to choose the correct SQL/storage control path.
            if schema != "public" {
                return Err(to_user_error(
                    "3F000",
                    format!("schema '{schema}' is not supported"),
                ));
            }
            table.clone()
        }
        [catalog, schema, table] => {
            // Decision: evaluate `catalog != "datafusion"` to choose the correct SQL/storage control path.
            if catalog != "datafusion" {
                return Err(to_user_error(
                    "3D000",
                    format!("catalog '{catalog}' is not supported"),
                ));
            }
            // Decision: evaluate `schema != "public"` to choose the correct SQL/storage control path.
            if schema != "public" {
                return Err(to_user_error(
                    "3F000",
                    format!("schema '{schema}' is not supported"),
                ));
            }
            table.clone()
        }
        _ => {
            return Err(to_user_error("42601", "invalid CREATE TABLE name format"));
        }
    };

    // Decision: evaluate `table_name.trim().is_empty()` to choose the correct SQL/storage control path.
    if table_name.trim().is_empty() {
        return Err(to_user_error("42601", "table name cannot be empty"));
    }
    Ok(table_name)
}

/// Executes `normalize ident` for this component.
fn normalize_ident(ident: &datafusion::sql::sqlparser::ast::Ident) -> String {
    // Decision: evaluate `ident.quote_style.is_some()` to choose the correct SQL/storage control path.
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}

/// Executes `validate orders v1 table definition` for this component.
fn validate_orders_v1_table_definition(create: &CreateTable, table_name: &str) -> PgWireResult<()> {
    let mut pk_columns = BTreeSet::<String>::new();
    let mut by_name = BTreeMap::<String, &ColumnDef>::new();

    for column in &create.columns {
        let column_name = normalize_ident(&column.name);
        // Decision: evaluate `by_name.insert(column_name.clone(), column).is_some()` to choose the correct SQL/storage control path.
        if by_name.insert(column_name.clone(), column).is_some() {
            return Err(to_user_error(
                "42701",
                format!("column '{}' specified more than once", column_name),
            ));
        }
        for option in &column.options {
            // Decision: evaluate `&option.option` to choose the correct SQL/storage control path.
            match &option.option {
                ColumnOption::Null | ColumnOption::NotNull => {}
                ColumnOption::Unique { is_primary, .. } if *is_primary => {
                    pk_columns.insert(column_name.clone());
                }
                ColumnOption::Comment(_) => {}
                ColumnOption::Unique { .. } => {
                    return Err(to_user_error(
                        "0A000",
                        "UNIQUE constraints are not supported; only PRIMARY KEY(order_id) is allowed",
                    ));
                }
                _ => {
                    return Err(to_user_error(
                        "0A000",
                        format!(
                            "column option '{}' is not supported in CREATE TABLE",
                            option.option
                        ),
                    ));
                }
            }
        }
    }

    for constraint in &create.constraints {
        // Decision: evaluate `constraint` to choose the correct SQL/storage control path.
        match constraint {
            TableConstraint::PrimaryKey { columns, .. } => {
                for index_column in columns {
                    let column = index_column_name(index_column).ok_or_else(|| {
                        to_user_error("0A000", "PRIMARY KEY must reference plain column names")
                    })?;
                    pk_columns.insert(column);
                }
            }
            _ => {
                return Err(to_user_error(
                    "0A000",
                    "table constraints other than PRIMARY KEY are not supported",
                ));
            }
        }
    }
    // Decision: evaluate `let Some(primary_key_expr) = &create.primary_key` to choose the correct SQL/storage control path.
    if let Some(primary_key_expr) = &create.primary_key {
        let column = extract_column_name(primary_key_expr.as_ref())
            .map(|value| value.to_ascii_lowercase())
            .ok_or_else(|| {
                to_user_error(
                    "0A000",
                    "PRIMARY KEY expression must reference a plain column name",
                )
            })?;
        pk_columns.insert(column);
    }

    let required_columns = [
        "order_id",
        "customer_id",
        "status",
        "total_cents",
        "created_at",
    ];
    // Decision: evaluate `by_name.len() != required_columns.len()` to choose the correct SQL/storage control path.
    if by_name.len() != required_columns.len()
        || required_columns
            .iter()
            .any(|name| !by_name.contains_key(*name))
    {
        return Err(to_user_error(
            "0A000",
            format!(
                "CREATE TABLE '{}' must use the orders_v1 column set: (order_id, customer_id, status, total_cents, created_at)",
                table_name
            ),
        ));
    }

    let order_id = by_name
        .get("order_id")
        .ok_or_else(|| to_user_error("42601", "missing column order_id"))?;
    // Decision: evaluate `!is_bigint_data_type(&order_id.data_type)` to choose the correct SQL/storage control path.
    if !is_bigint_data_type(&order_id.data_type) {
        return Err(to_user_error("42804", "column 'order_id' must be BIGINT"));
    }
    // Decision: evaluate `column_nullable(order_id)?` to choose the correct SQL/storage control path.
    if column_nullable(order_id)? {
        return Err(to_user_error(
            "23502",
            "column 'order_id' must be declared NOT NULL",
        ));
    }

    let customer_id = by_name
        .get("customer_id")
        .ok_or_else(|| to_user_error("42601", "missing column customer_id"))?;
    // Decision: evaluate `!is_bigint_data_type(&customer_id.data_type)` to choose the correct SQL/storage control path.
    if !is_bigint_data_type(&customer_id.data_type) {
        return Err(to_user_error(
            "42804",
            "column 'customer_id' must be BIGINT",
        ));
    }
    // Decision: evaluate `column_nullable(customer_id)?` to choose the correct SQL/storage control path.
    if column_nullable(customer_id)? {
        return Err(to_user_error(
            "23502",
            "column 'customer_id' must be declared NOT NULL",
        ));
    }

    let status = by_name
        .get("status")
        .ok_or_else(|| to_user_error("42601", "missing column status"))?;
    // Decision: evaluate `!is_status_data_type(&status.data_type)` to choose the correct SQL/storage control path.
    if !is_status_data_type(&status.data_type) {
        return Err(to_user_error(
            "42804",
            "column 'status' must be TEXT/VARCHAR",
        ));
    }
    // Decision: evaluate `!column_nullable(status)?` to choose the correct SQL/storage control path.
    if !column_nullable(status)? {
        return Err(to_user_error(
            "0A000",
            "column 'status' must allow NULL for orders_v1 table model",
        ));
    }

    let total_cents = by_name
        .get("total_cents")
        .ok_or_else(|| to_user_error("42601", "missing column total_cents"))?;
    // Decision: evaluate `!is_bigint_data_type(&total_cents.data_type)` to choose the correct SQL/storage control path.
    if !is_bigint_data_type(&total_cents.data_type) {
        return Err(to_user_error(
            "42804",
            "column 'total_cents' must be BIGINT",
        ));
    }
    // Decision: evaluate `column_nullable(total_cents)?` to choose the correct SQL/storage control path.
    if column_nullable(total_cents)? {
        return Err(to_user_error(
            "23502",
            "column 'total_cents' must be declared NOT NULL",
        ));
    }

    let created_at = by_name
        .get("created_at")
        .ok_or_else(|| to_user_error("42601", "missing column created_at"))?;
    // Decision: evaluate `!is_created_at_data_type(&created_at.data_type)` to choose the correct SQL/storage control path.
    if !is_created_at_data_type(&created_at.data_type) {
        return Err(to_user_error(
            "42804",
            "column 'created_at' must be TIMESTAMP [WITHOUT TIME ZONE]",
        ));
    }
    // Decision: evaluate `column_nullable(created_at)?` to choose the correct SQL/storage control path.
    if column_nullable(created_at)? {
        return Err(to_user_error(
            "23502",
            "column 'created_at' must be declared NOT NULL",
        ));
    }

    // Decision: evaluate `pk_columns.len() != 1 || !pk_columns.contains("order_id")` to choose the correct SQL/storage control path.
    if pk_columns.len() != 1 || !pk_columns.contains("order_id") {
        return Err(to_user_error(
            "0A000",
            "CREATE TABLE requires PRIMARY KEY(order_id)",
        ));
    }

    Ok(())
}

/// Executes `column nullable` for this component.
fn column_nullable(column: &ColumnDef) -> PgWireResult<bool> {
    let mut nullable = true;
    for option in &column.options {
        // Decision: evaluate `&option.option` to choose the correct SQL/storage control path.
        match &option.option {
            ColumnOption::Null => nullable = true,
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Unique { is_primary, .. } if *is_primary => nullable = false,
            ColumnOption::Comment(_) => {}
            ColumnOption::Unique { .. } => {
                return Err(to_user_error(
                    "0A000",
                    "UNIQUE constraints are not supported; only PRIMARY KEY(order_id) is allowed",
                ));
            }
            _ => {
                return Err(to_user_error(
                    "0A000",
                    format!(
                        "column option '{}' is not supported in CREATE TABLE",
                        option.option
                    ),
                ));
            }
        }
    }
    Ok(nullable)
}

/// Executes `is bigint data type` for this component.
fn is_bigint_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::BigInt(_)
            | DataType::Int8(_)
            | DataType::Int64
            | DataType::Integer(_)
            | DataType::Int(_)
            | DataType::Int4(_)
    )
}

/// Executes `is status data type` for this component.
fn is_status_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Text
            | DataType::Varchar(_)
            | DataType::CharVarying(_)
            | DataType::CharacterVarying(_)
    )
}

/// Executes `is created at data type` for this component.
fn is_created_at_data_type(data_type: &DataType) -> bool {
    // Decision: evaluate `data_type` to choose the correct SQL/storage control path.
    match data_type {
        DataType::Timestamp(_, tz) => {
            matches!(tz, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone)
        }
        DataType::TimestampNtz | DataType::Datetime(_) => true,
        _ => false,
    }
}

/// Executes `index column name` for this component.
fn index_column_name(column: &IndexColumn) -> Option<String> {
    extract_column_name(&column.column.expr).map(|value| value.to_ascii_lowercase())
}

/// Executes `map create table metadata error` for this component.
fn map_create_table_metadata_error(err: anyhow::Error) -> PgWireError {
    let msg = err.to_string();
    // Decision: evaluate `msg.contains("already exists")` to choose the correct SQL/storage control path.
    if msg.contains("already exists") {
        to_user_error("42P07", msg)
    } else {
        api_error(format!("create table metadata error: {err}"))
    }
}

/// Executes `execute update` for this component.
async fn execute_update(
    table: &TableWithJoins,
    assignments: &[Assignment],
    has_from: bool,
    selection: Option<&SqlExpr>,
    has_returning: bool,
    has_or: bool,
    params: &ParamValues,
    session_context: &SessionContext,
    config: DmlRuntimeConfig,
) -> PgWireResult<Response> {
    // Decision: evaluate `has_or` to choose the correct SQL/storage control path.
    if has_or {
        return Err(to_user_error(
            "0A000",
            "UPDATE OR <conflict-clause> is not supported",
        ));
    }
    // Decision: evaluate `has_from` to choose the correct SQL/storage control path.
    if has_from {
        return Err(to_user_error(
            "0A000",
            "UPDATE ... FROM is not supported; only single-table updates are allowed",
        ));
    }
    // Decision: evaluate `has_returning` to choose the correct SQL/storage control path.
    if has_returning {
        return Err(to_user_error(
            "0A000",
            "UPDATE ... RETURNING is not supported",
        ));
    }

    let table_name = extract_mutation_target_table_name(table, "UPDATE")?;
    let selection = selection.ok_or_else(|| {
        to_user_error(
            "0A000",
            "UPDATE requires a PK-bounded WHERE clause on order_id",
        )
    })?;
    let bounds = parse_pk_bounds(selection, params)?;
    // Decision: evaluate `bounds.is_unbounded()` to choose the correct SQL/storage control path.
    if bounds.is_unbounded() {
        return Err(to_user_error(
            "0A000",
            "UPDATE requires a PK-bounded WHERE clause on order_id",
        ));
    }

    let patch = parse_update_patch(assignments, params)?;
    let provider = get_provider_for_table(session_context, table_name.as_str()).await?;

    let rows = provider
        .scan_orders_with_versions_by_order_id_bounds(bounds.lower, bounds.upper, None)
        .await
        .map_err(|err| api_error(err.to_string()))?;
    // Decision: evaluate `rows.is_empty()` to choose the correct SQL/storage control path.
    if rows.is_empty() {
        return Ok(Response::Execution(Tag::new("UPDATE").with_rows(0)));
    }

    let mut writes = Vec::with_capacity(rows.len());
    for row in &rows {
        let updated = patch.apply(&row.row);
        // Keep UPDATE idempotent for retries by skipping no-op rewrites.
        // Decision: evaluate `updated != row.row` to choose the correct SQL/storage control path.
        if updated != row.row {
            writes.push(ConditionalOrderWrite {
                order_id: updated.order_id,
                expected_version: row.version,
                value: encode_orders_row_value(&updated),
                rollback_value: encode_orders_row_value(&row.row),
            });
        }
    }
    // Decision: evaluate `!writes.is_empty()` to choose the correct SQL/storage control path.
    if !writes.is_empty() {
        maybe_prewrite_delay(config).await;
        // Decision: evaluate `provider` to choose the correct SQL/storage control path.
        match provider
            .apply_orders_writes_conditional(&writes, "update")
            .await
            .map_err(|err| api_error(err.to_string()))?
        {
            ConditionalWriteOutcome::Applied(_) => {}
            ConditionalWriteOutcome::Conflict => {
                return Err(to_user_error(
                    "40001",
                    "write conflict detected during UPDATE; retry statement",
                ));
            }
        }
    }

    Ok(Response::Execution(
        Tag::new("UPDATE").with_rows(rows.len()),
    ))
}

/// Executes `execute delete` for this component.
async fn execute_delete(
    delete: &Delete,
    params: &ParamValues,
    session_context: &SessionContext,
    config: DmlRuntimeConfig,
) -> PgWireResult<Response> {
    // Decision: evaluate `!delete.tables.is_empty()` to choose the correct SQL/storage control path.
    if !delete.tables.is_empty() {
        return Err(to_user_error(
            "0A000",
            "multi-table DELETE is not supported",
        ));
    }
    // Decision: evaluate `delete.using.is_some()` to choose the correct SQL/storage control path.
    if delete.using.is_some() {
        return Err(to_user_error(
            "0A000",
            "DELETE ... USING is not supported; only single-table deletes are allowed",
        ));
    }
    // Decision: evaluate `!delete.order_by.is_empty() || delete.limit.is_some()` to choose the correct SQL/storage control path.
    if !delete.order_by.is_empty() || delete.limit.is_some() {
        return Err(to_user_error(
            "0A000",
            "DELETE ... ORDER BY / LIMIT is not supported",
        ));
    }
    // Decision: evaluate `delete.returning.is_some()` to choose the correct SQL/storage control path.
    if delete.returning.is_some() {
        return Err(to_user_error(
            "0A000",
            "DELETE ... RETURNING is not supported",
        ));
    }

    let table = extract_single_delete_table(&delete.from)?;
    let table_name = extract_mutation_target_table_name(table, "DELETE")?;

    let selection = delete.selection.as_ref().ok_or_else(|| {
        to_user_error(
            "0A000",
            "DELETE requires a PK-bounded WHERE clause on order_id",
        )
    })?;
    let bounds = parse_pk_bounds(selection, params)?;
    // Decision: evaluate `bounds.is_unbounded()` to choose the correct SQL/storage control path.
    if bounds.is_unbounded() {
        return Err(to_user_error(
            "0A000",
            "DELETE requires a PK-bounded WHERE clause on order_id",
        ));
    }

    let provider = get_provider_for_table(session_context, table_name.as_str()).await?;
    let rows = provider
        .scan_orders_with_versions_by_order_id_bounds(bounds.lower, bounds.upper, None)
        .await
        .map_err(|err| api_error(err.to_string()))?;
    // Decision: evaluate `rows.is_empty()` to choose the correct SQL/storage control path.
    if rows.is_empty() {
        return Ok(Response::Execution(Tag::new("DELETE").with_rows(0)));
    }

    let tombstone = encode_orders_tombstone_value();
    let writes = rows
        .iter()
        .map(|row| ConditionalOrderWrite {
            order_id: row.row.order_id,
            expected_version: row.version,
            value: tombstone.clone(),
            rollback_value: encode_orders_row_value(&row.row),
        })
        .collect::<Vec<_>>();

    maybe_prewrite_delay(config).await;
    // Decision: evaluate `provider` to choose the correct SQL/storage control path.
    match provider
        .apply_orders_writes_conditional(&writes, "delete")
        .await
        .map_err(|err| api_error(err.to_string()))?
    {
        ConditionalWriteOutcome::Applied(_) => {}
        ConditionalWriteOutcome::Conflict => {
            return Err(to_user_error(
                "40001",
                "write conflict detected during DELETE; retry statement",
            ));
        }
    }

    Ok(Response::Execution(
        Tag::new("DELETE").with_rows(writes.len()),
    ))
}

/// Executes `execute update in transaction` for this component.
async fn execute_update_in_transaction(
    table: &TableWithJoins,
    assignments: &[Assignment],
    has_from: bool,
    selection: Option<&SqlExpr>,
    has_returning: bool,
    has_or: bool,
    params: &ParamValues,
    session_context: &SessionContext,
    tx_manager: &TxnSessionManager,
    session_id: &str,
) -> PgWireResult<Response> {
    // Decision: evaluate `has_or` to choose the correct SQL/storage control path.
    if has_or {
        return Err(to_user_error(
            "0A000",
            "UPDATE OR <conflict-clause> is not supported",
        ));
    }
    // Decision: evaluate `has_from` to choose the correct SQL/storage control path.
    if has_from {
        return Err(to_user_error(
            "0A000",
            "UPDATE ... FROM is not supported; only single-table updates are allowed",
        ));
    }
    // Decision: evaluate `has_returning` to choose the correct SQL/storage control path.
    if has_returning {
        return Err(to_user_error(
            "0A000",
            "UPDATE ... RETURNING is not supported",
        ));
    }

    let table_name = extract_mutation_target_table_name(table, "UPDATE")?;
    ensure_txn_snapshot_for_table(tx_manager, session_id, session_context, table_name.as_str())
        .await?;
    let selection = selection.ok_or_else(|| {
        to_user_error(
            "0A000",
            "UPDATE requires a PK-bounded WHERE clause on order_id",
        )
    })?;
    let bounds = parse_pk_bounds(selection, params)?;
    // Decision: evaluate `bounds.is_unbounded()` to choose the correct SQL/storage control path.
    if bounds.is_unbounded() {
        return Err(to_user_error(
            "0A000",
            "UPDATE requires a PK-bounded WHERE clause on order_id",
        ));
    }

    let patch = parse_update_patch(assignments, params)?;
    with_active_txn_mut(tx_manager, session_id, |txn| {
        txn.ensure_same_table(table_name.as_str())?;
        let rows = txn.rows_matching_bounds(bounds);
        // Decision: evaluate `rows.is_empty()` to choose the correct SQL/storage control path.
        if rows.is_empty() {
            return Ok(Response::Execution(Tag::new("UPDATE").with_rows(0)));
        }

        for row in &rows {
            let updated = patch.apply(row);
            // Decision: evaluate `updated != *row` to choose the correct SQL/storage control path.
            if updated != *row {
                txn.stage_row_for_order(updated.order_id, Some(updated));
            }
        }

        Ok(Response::Execution(
            Tag::new("UPDATE").with_rows(rows.len()),
        ))
    })
    .await
}

/// Executes `execute delete in transaction` for this component.
async fn execute_delete_in_transaction(
    delete: &Delete,
    params: &ParamValues,
    session_context: &SessionContext,
    tx_manager: &TxnSessionManager,
    session_id: &str,
) -> PgWireResult<Response> {
    // Decision: evaluate `!delete.tables.is_empty()` to choose the correct SQL/storage control path.
    if !delete.tables.is_empty() {
        return Err(to_user_error(
            "0A000",
            "multi-table DELETE is not supported",
        ));
    }
    // Decision: evaluate `delete.using.is_some()` to choose the correct SQL/storage control path.
    if delete.using.is_some() {
        return Err(to_user_error(
            "0A000",
            "DELETE ... USING is not supported; only single-table deletes are allowed",
        ));
    }
    // Decision: evaluate `!delete.order_by.is_empty() || delete.limit.is_some()` to choose the correct SQL/storage control path.
    if !delete.order_by.is_empty() || delete.limit.is_some() {
        return Err(to_user_error(
            "0A000",
            "DELETE ... ORDER BY / LIMIT is not supported",
        ));
    }
    // Decision: evaluate `delete.returning.is_some()` to choose the correct SQL/storage control path.
    if delete.returning.is_some() {
        return Err(to_user_error(
            "0A000",
            "DELETE ... RETURNING is not supported",
        ));
    }

    let table = extract_single_delete_table(&delete.from)?;
    let table_name = extract_mutation_target_table_name(table, "DELETE")?;
    ensure_txn_snapshot_for_table(tx_manager, session_id, session_context, table_name.as_str())
        .await?;

    let selection = delete.selection.as_ref().ok_or_else(|| {
        to_user_error(
            "0A000",
            "DELETE requires a PK-bounded WHERE clause on order_id",
        )
    })?;
    let bounds = parse_pk_bounds(selection, params)?;
    // Decision: evaluate `bounds.is_unbounded()` to choose the correct SQL/storage control path.
    if bounds.is_unbounded() {
        return Err(to_user_error(
            "0A000",
            "DELETE requires a PK-bounded WHERE clause on order_id",
        ));
    }

    with_active_txn_mut(tx_manager, session_id, |txn| {
        txn.ensure_same_table(table_name.as_str())?;
        let rows = txn.rows_matching_bounds(bounds);
        // Decision: evaluate `rows.is_empty()` to choose the correct SQL/storage control path.
        if rows.is_empty() {
            return Ok(Response::Execution(Tag::new("DELETE").with_rows(0)));
        }

        for row in &rows {
            txn.stage_row_for_order(row.order_id, None);
        }
        Ok(Response::Execution(
            Tag::new("DELETE").with_rows(rows.len()),
        ))
    })
    .await
}

/// Executes `execute insert in transaction` for this component.
async fn execute_insert_in_transaction(
    insert: &Insert,
    params: &ParamValues,
    session_context: &SessionContext,
    tx_manager: &TxnSessionManager,
    session_id: &str,
) -> PgWireResult<Response> {
    // Decision: evaluate `insert.returning.is_some()` to choose the correct SQL/storage control path.
    if insert.returning.is_some() {
        return Err(to_user_error(
            "0A000",
            "INSERT ... RETURNING is not supported",
        ));
    }
    // Decision: evaluate `insert.on.is_some() || insert.ignore || insert.or.is_some() || insert.replace_into` to choose the correct SQL/storage control path.
    if insert.on.is_some() || insert.ignore || insert.or.is_some() || insert.replace_into {
        return Err(to_user_error(
            "0A000",
            "INSERT conflict clauses are not supported",
        ));
    }
    // Decision: evaluate `!insert.assignments.is_empty()` to choose the correct SQL/storage control path.
    if !insert.assignments.is_empty() {
        return Err(to_user_error(
            "0A000",
            "INSERT ... SET syntax is not supported",
        ));
    }

    let table_name = insert_table_name(&insert.table)?;
    ensure_txn_snapshot_for_table(tx_manager, session_id, session_context, table_name.as_str())
        .await?;

    let source = insert.source.as_ref().ok_or_else(|| {
        to_user_error(
            "0A000",
            "INSERT source is required; only VALUES inserts are supported",
        )
    })?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(to_user_error(
            "0A000",
            "INSERT INTO ... SELECT is not supported inside explicit transactions",
        ));
    };

    // Decision: evaluate `values.rows.is_empty()` to choose the correct SQL/storage control path.
    if values.rows.is_empty() {
        return Ok(Response::Execution(Tag::new("INSERT").with_rows(0)));
    }

    let columns = if insert.columns.is_empty() {
        vec![
            "order_id".to_string(),
            "customer_id".to_string(),
            "status".to_string(),
            "total_cents".to_string(),
            "created_at".to_string(),
        ]
    } else {
        insert
            .columns
            .iter()
            .map(|ident| ident.value.clone())
            .collect()
    };

    let rows = values
        .rows
        .iter()
        .map(|exprs| decode_insert_orders_row(exprs, &columns, params))
        .collect::<PgWireResult<Vec<_>>>()?;

    with_active_txn_mut(tx_manager, session_id, |txn| {
        txn.ensure_same_table(table_name.as_str())?;
        let constraint_name = format!("{table_name}_pkey");
        for row in &rows {
            // Decision: evaluate `txn.current_row(row.order_id).is_some()` to choose the correct SQL/storage control path.
            if txn.current_row(row.order_id).is_some() {
                return Err(to_user_error(
                    "23505",
                    format!(
                        "duplicate key value violates unique constraint '{}' (order_id={})",
                        constraint_name, row.order_id
                    ),
                ));
            }
            txn.stage_row_for_order(row.order_id, Some(row.clone()));
        }

        Ok(Response::Execution(
            Tag::new("INSERT").with_oid(0).with_rows(rows.len()),
        ))
    })
    .await
}

/// Executes `execute query in transaction` for this component.
async fn execute_query_in_transaction(
    statement: &Statement,
    tx_manager: &TxnSessionManager,
    session_id: &str,
    _session_context: &SessionContext,
    client: &(dyn ClientInfo + Send + Sync),
    protocol: QueryProtocol,
) -> PgWireResult<Response> {
    let (table_name, rows) = with_active_txn(tx_manager, session_id, |txn| {
        (
            txn.bound_table_name().map(|name| name.to_string()),
            txn.visible_rows_for_query(),
        )
    })
    .await?;
    let table_name = table_name.ok_or_else(|| {
        to_user_error(
            "0A000",
            "transactional SELECT requires a prior INSERT/UPDATE/DELETE statement",
        )
    })?;
    let schema = orders_schema();
    let batch = rows_to_batch(schema.clone(), &rows).map_err(|err| api_error(err.to_string()))?;
    let mem = MemTable::try_new(schema.clone(), vec![vec![batch]])
        .map_err(|err| api_error(err.to_string()))?;

    let query_ctx = SessionContext::new();
    query_ctx
        .register_table(table_name.as_str(), Arc::new(mem))
        .map_err(|err| api_error(err.to_string()))?;

    let sql = statement.to_string();
    let dataframe = query_ctx
        .sql(sql.as_str())
        .await
        .map_err(|err| api_error(err.to_string()))?;
    let format_options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
    // Decision: evaluate `protocol` to choose the correct SQL/storage control path.
    let wire_format = match protocol {
        QueryProtocol::Simple => Format::UnifiedText,
        QueryProtocol::Extended => Format::UnifiedBinary,
    };
    let query = df::encode_dataframe(dataframe, &wire_format, Some(format_options))
        .await
        .map_err(|err| api_error(err.to_string()))?;
    Ok(Response::Query(query))
}

/// Executes `with active txn mut` for this component.
async fn with_active_txn_mut<T>(
    tx_manager: &TxnSessionManager,
    session_id: &str,
    mut f: impl FnMut(&mut TxnContext) -> PgWireResult<T>,
) -> PgWireResult<T> {
    let mut sessions = tx_manager.sessions.lock().await;
    let Some(state) = sessions.get_mut(session_id) else {
        return Err(no_active_tx_error("active transaction required"));
    };
    // Decision: evaluate `state` to choose the correct SQL/storage control path.
    match state {
        SessionTxnState::Active(txn) => f(txn),
        SessionTxnState::Aborted => Err(aborted_tx_error()),
    }
}

/// Executes `with active txn` for this component.
async fn with_active_txn<T>(
    tx_manager: &TxnSessionManager,
    session_id: &str,
    mut f: impl FnMut(&TxnContext) -> T,
) -> PgWireResult<T> {
    let sessions = tx_manager.sessions.lock().await;
    let Some(state) = sessions.get(session_id) else {
        return Err(no_active_tx_error("active transaction required"));
    };
    // Decision: evaluate `state` to choose the correct SQL/storage control path.
    match state {
        SessionTxnState::Active(txn) => Ok(f(txn)),
        SessionTxnState::Aborted => Err(aborted_tx_error()),
    }
}

/// Executes `ensure txn snapshot for table` for this component.
async fn ensure_txn_snapshot_for_table(
    tx_manager: &TxnSessionManager,
    session_id: &str,
    session_context: &SessionContext,
    table_name: &str,
) -> PgWireResult<()> {
    let needs_snapshot = {
        let sessions = tx_manager.sessions.lock().await;
        let Some(state) = sessions.get(session_id) else {
            return Err(no_active_tx_error("active transaction required"));
        };
        // Decision: evaluate `state` to choose the correct SQL/storage control path.
        match state {
            SessionTxnState::Active(txn) => {
                txn.ensure_same_table(table_name)?;
                txn.bound_table_name().is_none()
            }
            SessionTxnState::Aborted => return Err(aborted_tx_error()),
        }
    };
    // Decision: evaluate `!needs_snapshot` to choose the correct SQL/storage control path.
    if !needs_snapshot {
        return Ok(());
    }

    let provider = get_provider_for_table(session_context, table_name).await?;
    let rows = provider
        .scan_orders_with_versions_by_order_id_bounds(None, None, None)
        .await
        .map_err(|err| api_error(err.to_string()))?;

    with_active_txn_mut(tx_manager, session_id, |txn| {
        txn.ensure_same_table(table_name)?;
        // Decision: evaluate `txn.bound_table_name().is_none()` to choose the correct SQL/storage control path.
        if txn.bound_table_name().is_none() {
            txn.bind_snapshot(table_name.to_string(), rows.clone());
        }
        Ok(())
    })
    .await
}

/// Executes `get provider for table` for this component.
async fn get_provider_for_table(
    session_context: &SessionContext,
    table_name: &str,
) -> PgWireResult<HoloStoreTableProvider> {
    let provider = session_context
        .table_provider(table_name)
        .await
        .map_err(|err| api_error(err.to_string()))?;
    provider
        .as_any()
        .downcast_ref::<HoloStoreTableProvider>()
        .cloned()
        .ok_or_else(|| {
            to_user_error(
                "42P01",
                format!("table '{table_name}' is not available for mutations"),
            )
        })
}

/// Executes `maybe prewrite delay` for this component.
async fn maybe_prewrite_delay(config: DmlRuntimeConfig) {
    // Decision: evaluate `config.prewrite_delay > Duration::ZERO` to choose the correct SQL/storage control path.
    if config.prewrite_delay > Duration::ZERO {
        tokio::time::sleep(config.prewrite_delay).await;
    }
}

/// Executes `dml placeholder plan` for this component.
fn dml_placeholder_plan(statement: &Statement) -> PgWireResult<LogicalPlan> {
    let placeholders = extract_placeholder_ids(statement);
    let inferred_types = infer_placeholder_types(statement);
    // Decision: evaluate `placeholders.is_empty()` to choose the correct SQL/storage control path.
    if placeholders.is_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "__dml_placeholder__",
            ArrowDataType::Null,
            true,
        )]));
        let df_schema = schema
            .to_dfschema()
            .map_err(|err| api_error(err.to_string()))?;
        return Ok(LogicalPlan::EmptyRelation(
            datafusion::logical_expr::EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(df_schema),
            },
        ));
    }

    let exprs = placeholders
        .into_iter()
        .map(|id| {
            let inferred = inferred_types.get(&id).cloned();
            DfExpr::Placeholder(Placeholder::new(id, inferred))
        })
        .collect::<Vec<_>>();
    LogicalPlanBuilder::values(vec![exprs])
        .and_then(|builder| builder.build())
        .map_err(|err| api_error(err.to_string()))
}

/// Executes `extract placeholder ids` for this component.
fn extract_placeholder_ids(statement: &Statement) -> Vec<String> {
    let sql = statement.to_string();
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    let mut idx = 0usize;
    while idx < bytes.len() {
        // Decision: evaluate `bytes[idx] == b'$'` to choose the correct SQL/storage control path.
        if bytes[idx] == b'$' {
            let start = idx + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            // Decision: evaluate `end > start` to choose the correct SQL/storage control path.
            if end > start {
                let id = format!("${}", &sql[start..end]);
                // Decision: evaluate `seen.insert(id.clone())` to choose the correct SQL/storage control path.
                if seen.insert(id.clone()) {
                    out.push(id);
                }
                idx = end;
                continue;
            }
        }
        idx = idx.saturating_add(1);
    }

    out.sort_by_key(|id| id[1..].parse::<usize>().unwrap_or(usize::MAX));
    out
}

/// Executes `infer placeholder types` for this component.
fn infer_placeholder_types(statement: &Statement) -> HashMap<String, ArrowDataType> {
    let mut out = HashMap::new();

    // Decision: evaluate `statement` to choose the correct SQL/storage control path.
    match statement {
        Statement::Update {
            assignments,
            selection,
            ..
        } => {
            for assignment in assignments {
                let expected = assignment_column_name(&assignment.target).and_then(|column| {
                    // Decision: evaluate `column.as_str()` to choose the correct SQL/storage control path.
                    match column.as_str() {
                        "order_id" | "customer_id" | "total_cents" => Some(ArrowDataType::Int64),
                        "status" => Some(ArrowDataType::Utf8),
                        "created_at" => Some(ArrowDataType::Timestamp(TimeUnit::Nanosecond, None)),
                        _ => None,
                    }
                });
                // Decision: evaluate `let Some(expected) = expected` to choose the correct SQL/storage control path.
                if let Some(expected) = expected {
                    collect_placeholders_with_type(&assignment.value, expected, &mut out);
                }
            }
            // Decision: evaluate `let Some(selection) = selection` to choose the correct SQL/storage control path.
            if let Some(selection) = selection {
                infer_order_id_predicate_placeholder_types(selection, &mut out);
            }
        }
        Statement::Delete(delete) => {
            // Decision: evaluate `let Some(selection) = &delete.selection` to choose the correct SQL/storage control path.
            if let Some(selection) = &delete.selection {
                infer_order_id_predicate_placeholder_types(selection, &mut out);
            }
        }
        _ => {}
    }

    out
}

/// Executes `assignment column name` for this component.
fn assignment_column_name(target: &AssignmentTarget) -> Option<String> {
    let AssignmentTarget::ColumnName(column) = target else {
        return None;
    };
    column
        .0
        .last()
        .and_then(|part| part.as_ident().map(|ident| ident.value.clone()))
}

/// Executes `infer order id predicate placeholder types` for this component.
fn infer_order_id_predicate_placeholder_types(
    expr: &SqlExpr,
    output: &mut HashMap<String, ArrowDataType>,
) {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            infer_order_id_predicate_placeholder_types(left, output);
            infer_order_id_predicate_placeholder_types(right, output);
        }
        SqlExpr::BinaryOp { left, right, .. } => {
            // Decision: evaluate `extract_column_name(left).as_deref() == Some("order_id")` to choose the correct SQL/storage control path.
            if extract_column_name(left).as_deref() == Some("order_id") {
                collect_placeholders_with_type(right, ArrowDataType::Int64, output);
            }
            // Decision: evaluate `extract_column_name(right).as_deref() == Some("order_id")` to choose the correct SQL/storage control path.
            if extract_column_name(right).as_deref() == Some("order_id") {
                collect_placeholders_with_type(left, ArrowDataType::Int64, output);
            }
        }
        SqlExpr::Nested(inner) => infer_order_id_predicate_placeholder_types(inner, output),
        _ => {}
    }
}

/// Executes `collect placeholders with type` for this component.
fn collect_placeholders_with_type(
    expr: &SqlExpr,
    expected: ArrowDataType,
    output: &mut HashMap<String, ArrowDataType>,
) {
    use datafusion::sql::sqlparser::ast::Value;

    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        SqlExpr::Value(value) => {
            // Decision: evaluate `let Value::Placeholder(id) = &value.value` to choose the correct SQL/storage control path.
            if let Value::Placeholder(id) = &value.value {
                output.entry(id.clone()).or_insert(expected);
            }
        }
        SqlExpr::Cast { expr, .. }
        | SqlExpr::UnaryOp { expr, .. }
        | SqlExpr::Nested(expr)
        | SqlExpr::Collate { expr, .. } => collect_placeholders_with_type(expr, expected, output),
        SqlExpr::BinaryOp { left, right, .. } => {
            collect_placeholders_with_type(left, expected.clone(), output);
            collect_placeholders_with_type(right, expected, output);
        }
        _ => {}
    }
}

#[derive(Debug, Clone, Copy, Default)]
/// Represents the `OrderIdBounds` component used by the holo_fusion runtime.
struct OrderIdBounds {
    lower: Option<(i64, bool)>,
    upper: Option<(i64, bool)>,
    impossible: bool,
    predicate_count: usize,
}

impl OrderIdBounds {
    /// Executes `is unbounded` for this component.
    fn is_unbounded(&self) -> bool {
        !self.impossible
            && self.lower.is_none()
            && self.upper.is_none()
            && self.predicate_count == 0
    }

    /// Executes `apply` for this component.
    fn apply(&mut self, op: BinaryOperator, value: i64) {
        // Decision: evaluate `self.impossible` to choose the correct SQL/storage control path.
        if self.impossible {
            return;
        }
        self.predicate_count = self.predicate_count.saturating_add(1);

        // Decision: evaluate `op` to choose the correct SQL/storage control path.
        match op {
            BinaryOperator::Eq => {
                self.restrict_lower(value, true);
                self.restrict_upper(value, true);
            }
            BinaryOperator::Gt => self.restrict_lower(value, false),
            BinaryOperator::GtEq => self.restrict_lower(value, true),
            BinaryOperator::Lt => self.restrict_upper(value, false),
            BinaryOperator::LtEq => self.restrict_upper(value, true),
            _ => self.impossible = true,
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
                self.impossible = true;
            }
        }
    }

    /// Executes `matches` for this component.
    fn matches(&self, value: i64) -> bool {
        // Decision: evaluate `self.impossible` to choose the correct SQL/storage control path.
        if self.impossible {
            return false;
        }
        // Decision: evaluate `let Some((lower, inclusive)) = self.lower` to choose the correct SQL/storage control path.
        if let Some((lower, inclusive)) = self.lower {
            // Decision: evaluate `inclusive` to choose the correct SQL/storage control path.
            if inclusive {
                // Decision: evaluate `value < lower` to choose the correct SQL/storage control path.
                if value < lower {
                    return false;
                }
            // Decision: evaluate `value <= lower` to choose the correct SQL/storage control path.
            } else if value <= lower {
                return false;
            }
        }
        // Decision: evaluate `let Some((upper, inclusive)) = self.upper` to choose the correct SQL/storage control path.
        if let Some((upper, inclusive)) = self.upper {
            // Decision: evaluate `inclusive` to choose the correct SQL/storage control path.
            if inclusive {
                // Decision: evaluate `value > upper` to choose the correct SQL/storage control path.
                if value > upper {
                    return false;
                }
            // Decision: evaluate `value >= upper` to choose the correct SQL/storage control path.
            } else if value >= upper {
                return false;
            }
        }
        true
    }
}

/// Executes `parse pk bounds` for this component.
fn parse_pk_bounds(selection: &SqlExpr, params: &ParamValues) -> PgWireResult<OrderIdBounds> {
    let mut bounds = OrderIdBounds::default();
    collect_pk_bounds(selection, params, &mut bounds)?;
    // Decision: evaluate `bounds.predicate_count == 0` to choose the correct SQL/storage control path.
    if bounds.predicate_count == 0 {
        return Err(to_user_error(
            "0A000",
            "WHERE clause must include order_id predicates",
        ));
    }
    Ok(bounds)
}

/// Executes `collect pk bounds` for this component.
fn collect_pk_bounds(
    expr: &SqlExpr,
    params: &ParamValues,
    bounds: &mut OrderIdBounds,
) -> PgWireResult<()> {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            collect_pk_bounds(left, params, bounds)?;
            collect_pk_bounds(right, params, bounds)?;
            Ok(())
        }
        SqlExpr::BinaryOp { left, op, right } => {
            // Decision: evaluate `(` to choose the correct SQL/storage control path.
            let (column, op, literal) = match (
                extract_column_name(left.as_ref()),
                extract_i64_expr(right.as_ref(), params)?,
            ) {
                (Some(column), Some(value)) => (column, op.clone(), value),
                _ => match (
                    extract_column_name(right.as_ref()),
                    extract_i64_expr(left.as_ref(), params)?,
                ) {
                    (Some(column), Some(value)) => (column, reverse_comparison(op), value),
                    _ => {
                        return Err(to_user_error(
                            "0A000",
                            "WHERE clause must contain only order_id comparisons joined by AND",
                        ));
                    }
                },
            };

            // Decision: evaluate `column != "order_id"` to choose the correct SQL/storage control path.
            if column != "order_id" {
                return Err(to_user_error(
                    "0A000",
                    "WHERE clause must contain only order_id comparisons",
                ));
            }
            // Decision: evaluate `!matches!(` to choose the correct SQL/storage control path.
            if !matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
            ) {
                return Err(to_user_error(
                    "0A000",
                    "WHERE clause supports only =, >, >=, <, <= on order_id",
                ));
            }

            bounds.apply(op, literal);
            Ok(())
        }
        SqlExpr::Nested(inner) => collect_pk_bounds(inner, params, bounds),
        _ => Err(to_user_error(
            "0A000",
            "WHERE clause must contain only order_id comparisons joined by AND",
        )),
    }
}

/// Executes `reverse comparison` for this component.
fn reverse_comparison(op: &BinaryOperator) -> BinaryOperator {
    // Decision: evaluate `op` to choose the correct SQL/storage control path.
    match op {
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        other => other.clone(),
    }
}

#[derive(Debug, Default)]
/// Represents the `UpdatePatch` component used by the holo_fusion runtime.
struct UpdatePatch {
    customer_id: Option<i64>,
    status: Option<Option<String>>,
    total_cents: Option<i64>,
    created_at_ns: Option<i64>,
}

impl UpdatePatch {
    /// Executes `apply` for this component.
    fn apply(&self, row: &OrdersSeedRow) -> OrdersSeedRow {
        OrdersSeedRow {
            order_id: row.order_id,
            customer_id: self.customer_id.unwrap_or(row.customer_id),
            status: self.status.clone().unwrap_or_else(|| row.status.clone()),
            total_cents: self.total_cents.unwrap_or(row.total_cents),
            created_at_ns: self.created_at_ns.unwrap_or(row.created_at_ns),
        }
    }
}

/// Executes `parse update patch` for this component.
fn parse_update_patch(
    assignments: &[Assignment],
    params: &ParamValues,
) -> PgWireResult<UpdatePatch> {
    // Decision: evaluate `assignments.is_empty()` to choose the correct SQL/storage control path.
    if assignments.is_empty() {
        return Err(to_user_error(
            "42601",
            "UPDATE requires at least one assignment",
        ));
    }

    let mut patch = UpdatePatch::default();
    let mut seen = HashSet::new();

    for assignment in assignments {
        let column = extract_assignment_column(&assignment.target)?;
        // Decision: evaluate `!seen.insert(column.clone())` to choose the correct SQL/storage control path.
        if !seen.insert(column.clone()) {
            return Err(to_user_error(
                "42601",
                format!("duplicate assignment for column '{column}'"),
            ));
        }

        // Decision: evaluate `column.as_str()` to choose the correct SQL/storage control path.
        match column.as_str() {
            "order_id" => {
                return Err(to_user_error(
                    "0A000",
                    "updating primary key column 'order_id' is not supported",
                ));
            }
            "customer_id" => {
                patch.customer_id =
                    Some(extract_i64_expr(&assignment.value, params)?.ok_or_else(|| {
                        to_user_error("22023", "customer_id assignment must be an int64 literal")
                    })?);
            }
            "status" => {
                patch.status = Some(extract_optional_string_expr(&assignment.value, params)?);
            }
            "total_cents" => {
                patch.total_cents =
                    Some(extract_i64_expr(&assignment.value, params)?.ok_or_else(|| {
                        to_user_error("22023", "total_cents assignment must be an int64 literal")
                    })?);
            }
            "created_at" => {
                patch.created_at_ns = Some(extract_timestamp_ns_expr(&assignment.value, params)?);
            }
            _ => {
                return Err(to_user_error(
                    "0A000",
                    format!("updating column '{column}' is not supported"),
                ));
            }
        }
    }

    Ok(patch)
}

/// Executes `extract single delete table` for this component.
fn extract_single_delete_table(from: &FromTable) -> PgWireResult<&TableWithJoins> {
    // Decision: evaluate `from` to choose the correct SQL/storage control path.
    let tables = match from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    // Decision: evaluate `tables.len() != 1` to choose the correct SQL/storage control path.
    if tables.len() != 1 {
        return Err(to_user_error(
            "0A000",
            "DELETE supports exactly one target table",
        ));
    }
    Ok(&tables[0])
}

/// Executes `extract mutation target table name` for this component.
fn extract_mutation_target_table_name(
    table: &TableWithJoins,
    statement_kind: &str,
) -> PgWireResult<String> {
    // Decision: evaluate `!table.joins.is_empty()` to choose the correct SQL/storage control path.
    if !table.joins.is_empty() {
        return Err(to_user_error(
            "0A000",
            format!("joins are not supported in {statement_kind}"),
        ));
    }

    let TableFactor::Table { name, .. } = &table.relation else {
        return Err(to_user_error(
            "0A000",
            format!("only base tables are supported in {statement_kind}"),
        ));
    };
    object_name_leaf(name)
}

/// Executes `extract assignment column` for this component.
fn extract_assignment_column(target: &AssignmentTarget) -> PgWireResult<String> {
    let AssignmentTarget::ColumnName(column) = target else {
        return Err(to_user_error(
            "0A000",
            "tuple assignments are not supported in UPDATE",
        ));
    };
    object_name_leaf(column)
}

/// Executes `object name leaf` for this component.
fn object_name_leaf(name: &ObjectName) -> PgWireResult<String> {
    name.0
        .last()
        .and_then(|part| part.as_ident().map(normalize_ident))
        .ok_or_else(|| to_user_error("42601", "invalid object name"))
}

/// Executes `insert table name` for this component.
fn insert_table_name(table: &TableObject) -> PgWireResult<String> {
    // Decision: evaluate `table` to choose the correct SQL/storage control path.
    match table {
        TableObject::TableName(name) => object_name_leaf(name),
        TableObject::TableFunction(_) => Err(to_user_error(
            "0A000",
            "table functions are not supported for transactional INSERT",
        )),
    }
}

/// Executes `decode insert orders row` for this component.
fn decode_insert_orders_row(
    exprs: &[SqlExpr],
    columns: &[String],
    params: &ParamValues,
) -> PgWireResult<OrdersSeedRow> {
    // Decision: evaluate `exprs.len() != columns.len()` to choose the correct SQL/storage control path.
    if exprs.len() != columns.len() {
        return Err(to_user_error(
            "42601",
            format!(
                "INSERT row has {} values but {} columns were provided",
                exprs.len(),
                columns.len()
            ),
        ));
    }

    let mut order_id: Option<i64> = None;
    let mut customer_id: Option<i64> = None;
    let mut status: Option<Option<String>> = None;
    let mut total_cents: Option<i64> = None;
    let mut created_at_ns: Option<i64> = None;

    let mut seen = HashSet::new();
    for (column, expr) in columns.iter().zip(exprs.iter()) {
        let column = column.trim().to_ascii_lowercase();
        // Decision: evaluate `!seen.insert(column.clone())` to choose the correct SQL/storage control path.
        if !seen.insert(column.clone()) {
            return Err(to_user_error(
                "42601",
                format!("duplicate INSERT column '{column}'"),
            ));
        }

        // Decision: evaluate `column.as_str()` to choose the correct SQL/storage control path.
        match column.as_str() {
            "order_id" => {
                order_id = Some(
                    extract_i64_expr(expr, params)?
                        .ok_or_else(|| to_user_error("22023", "order_id must be an int64"))?,
                );
            }
            "customer_id" => {
                customer_id = Some(
                    extract_i64_expr(expr, params)?
                        .ok_or_else(|| to_user_error("22023", "customer_id must be an int64"))?,
                );
            }
            "status" => {
                status = Some(extract_optional_string_expr(expr, params)?);
            }
            "total_cents" => {
                total_cents = Some(
                    extract_i64_expr(expr, params)?
                        .ok_or_else(|| to_user_error("22023", "total_cents must be an int64"))?,
                );
            }
            "created_at" => {
                created_at_ns = Some(extract_timestamp_ns_expr(expr, params)?);
            }
            other => {
                return Err(to_user_error(
                    "0A000",
                    format!("INSERT column '{other}' is not supported"),
                ));
            }
        }
    }

    Ok(OrdersSeedRow {
        order_id: order_id.ok_or_else(|| {
            to_user_error(
                "23502",
                "null value in column 'order_id' violates not-null constraint",
            )
        })?,
        customer_id: customer_id.ok_or_else(|| {
            to_user_error(
                "23502",
                "null value in column 'customer_id' violates not-null constraint",
            )
        })?,
        status: status.unwrap_or(None),
        total_cents: total_cents.ok_or_else(|| {
            to_user_error(
                "23502",
                "null value in column 'total_cents' violates not-null constraint",
            )
        })?,
        created_at_ns: created_at_ns.ok_or_else(|| {
            to_user_error(
                "23502",
                "null value in column 'created_at' violates not-null constraint",
            )
        })?,
    })
}

/// Executes `extract column name` for this component.
fn extract_column_name(expr: &SqlExpr) -> Option<String> {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        SqlExpr::Identifier(ident) => Some(ident.value.clone()),
        SqlExpr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.clone()),
        SqlExpr::Nested(inner) => extract_column_name(inner),
        SqlExpr::Cast { expr, .. } => extract_column_name(expr),
        _ => None,
    }
}

/// Executes `extract i64 expr` for this component.
fn extract_i64_expr(expr: &SqlExpr, params: &ParamValues) -> PgWireResult<Option<i64>> {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        SqlExpr::Value(value) => scalar_to_i64(resolve_sql_value(value, params)?)
            .ok_or_else(|| to_user_error("22023", "invalid int64 literal"))
            .map(Some),
        SqlExpr::UnaryOp { op, expr } => {
            let Some(value) = extract_i64_expr(expr, params)? else {
                return Ok(None);
            };
            // Decision: evaluate `op` to choose the correct SQL/storage control path.
            match op {
                UnaryOperator::Plus => Ok(Some(value)),
                UnaryOperator::Minus => Ok(Some(value.saturating_neg())),
                _ => Ok(None),
            }
        }
        SqlExpr::Nested(inner) => extract_i64_expr(inner, params),
        SqlExpr::Cast { expr, .. } => extract_i64_expr(expr, params),
        _ => Ok(None),
    }
}

/// Executes `extract optional string expr` for this component.
fn extract_optional_string_expr(
    expr: &SqlExpr,
    params: &ParamValues,
) -> PgWireResult<Option<String>> {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        SqlExpr::Value(value) => scalar_to_optional_string(resolve_sql_value(value, params)?),
        SqlExpr::Nested(inner) => extract_optional_string_expr(inner, params),
        SqlExpr::Cast { expr, .. } => extract_optional_string_expr(expr, params),
        _ => Err(to_user_error(
            "22023",
            "status assignment must be a string literal or NULL",
        )),
    }
}

/// Executes `extract timestamp ns expr` for this component.
fn extract_timestamp_ns_expr(expr: &SqlExpr, params: &ParamValues) -> PgWireResult<i64> {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        SqlExpr::Value(value) => scalar_to_timestamp_ns(resolve_sql_value(value, params)?),
        SqlExpr::TypedString { data_type, value } => {
            // Decision: evaluate `!is_timestamp_data_type(data_type)` to choose the correct SQL/storage control path.
            if !is_timestamp_data_type(data_type) {
                return Err(to_user_error(
                    "22023",
                    format!("unsupported typed literal for created_at: {data_type}"),
                ));
            }
            parse_timestamp_ns_from_string(value.value.to_string().trim_matches('\''))
        }
        SqlExpr::Nested(inner) => extract_timestamp_ns_expr(inner, params),
        SqlExpr::Cast { expr, .. } => extract_timestamp_ns_expr(expr, params),
        _ => Err(to_user_error(
            "22023",
            "created_at assignment must be a timestamp literal",
        )),
    }
}

/// Executes `resolve sql value` for this component.
fn resolve_sql_value(value: &ValueWithSpan, params: &ParamValues) -> PgWireResult<ScalarValue> {
    use datafusion::sql::sqlparser::ast::Value;

    // Decision: evaluate `&value.value` to choose the correct SQL/storage control path.
    match &value.value {
        Value::Placeholder(id) => params
            .get_placeholders_with_values(id)
            .map_err(|err| to_user_error("22023", format!("invalid placeholder {id}: {err}"))),
        Value::Null => Ok(ScalarValue::Null),
        Value::Boolean(v) => Ok(ScalarValue::Boolean(Some(*v))),
        Value::Number(raw, _) => {
            // Decision: evaluate `let Ok(v) = raw.parse::<i64>()` to choose the correct SQL/storage control path.
            if let Ok(v) = raw.parse::<i64>() {
                Ok(ScalarValue::Int64(Some(v)))
            // Decision: evaluate `let Ok(v) = raw.parse::<u64>()` to choose the correct SQL/storage control path.
            } else if let Ok(v) = raw.parse::<u64>() {
                Ok(ScalarValue::UInt64(Some(v)))
            // Decision: evaluate `let Ok(v) = raw.parse::<f64>()` to choose the correct SQL/storage control path.
            } else if let Ok(v) = raw.parse::<f64>() {
                Ok(ScalarValue::Float64(Some(v)))
            } else {
                Err(to_user_error(
                    "22023",
                    format!("invalid numeric literal: {raw}"),
                ))
            }
        }
        Value::SingleQuotedString(s)
        | Value::DoubleQuotedString(s)
        | Value::TripleSingleQuotedString(s)
        | Value::TripleDoubleQuotedString(s)
        | Value::EscapedStringLiteral(s)
        | Value::UnicodeStringLiteral(s)
        | Value::NationalStringLiteral(s)
        | Value::HexStringLiteral(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
        other => Err(to_user_error(
            "22023",
            format!("unsupported literal value: {other}"),
        )),
    }
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
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => v.parse::<i64>().ok(),
        _ => None,
    }
}

/// Executes `scalar to optional string` for this component.
fn scalar_to_optional_string(value: ScalarValue) -> PgWireResult<Option<String>> {
    // Decision: evaluate `value` to choose the correct SQL/storage control path.
    match value {
        ScalarValue::Utf8(v) => Ok(v),
        ScalarValue::LargeUtf8(v) => Ok(v),
        ScalarValue::Null => Ok(None),
        _ => Err(to_user_error("22023", "expected string literal or NULL")),
    }
}

/// Executes `scalar to timestamp ns` for this component.
fn scalar_to_timestamp_ns(value: ScalarValue) -> PgWireResult<i64> {
    // Decision: evaluate `value` to choose the correct SQL/storage control path.
    match value {
        ScalarValue::TimestampNanosecond(v, _) => v.ok_or_else(|| {
            to_user_error("22023", "timestamp literal for created_at cannot be NULL")
        }),
        ScalarValue::TimestampMicrosecond(v, _) => v
            .map(|v| v.saturating_mul(1_000))
            .ok_or_else(|| to_user_error("22023", "timestamp literal cannot be NULL")),
        ScalarValue::TimestampMillisecond(v, _) => v
            .map(|v| v.saturating_mul(1_000_000))
            .ok_or_else(|| to_user_error("22023", "timestamp literal cannot be NULL")),
        ScalarValue::TimestampSecond(v, _) => v
            .map(|v| v.saturating_mul(1_000_000_000))
            .ok_or_else(|| to_user_error("22023", "timestamp literal cannot be NULL")),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
            parse_timestamp_ns_from_string(&v)
        }
        _ => Err(to_user_error(
            "22023",
            "expected timestamp literal for created_at",
        )),
    }
}

/// Executes `parse timestamp ns from string` for this component.
fn parse_timestamp_ns_from_string(value: &str) -> PgWireResult<i64> {
    let scalar = ScalarValue::try_from_string(
        value.to_string(),
        &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
    )
    .map_err(|err| {
        to_user_error(
            "22023",
            format!("invalid timestamp literal '{value}': {err}"),
        )
    })?;
    scalar_to_timestamp_ns(scalar)
}

/// Executes `rows to batch` for this component.
fn rows_to_batch(schema: Arc<Schema>, rows: &[OrdersSeedRow]) -> anyhow::Result<RecordBatch> {
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

/// Executes `is timestamp data type` for this component.
fn is_timestamp_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(_, _) | DataType::TimestampNtz
    )
}

/// Executes `to user error` for this component.
fn to_user_error(code: impl Into<String>, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.into(),
        message.into(),
    )))
}

/// Executes `api error` for this component.
fn api_error(message: impl Into<String>) -> PgWireError {
    PgWireError::ApiError(Box::new(std::io::Error::other(message.into())))
}

/// Executes `no active tx error` for this component.
fn no_active_tx_error(message: impl Into<String>) -> PgWireError {
    to_user_error("25P01", message.into())
}

/// Executes `aborted tx error` for this component.
fn aborted_tx_error() -> PgWireError {
    to_user_error(
        "25P02",
        "current transaction is aborted, commands ignored until end of transaction block",
    )
}
