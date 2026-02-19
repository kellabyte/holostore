//! PostgreSQL DML/query hook layer for HoloFusion.
//!
//! This module intercepts SQL statements, routes transactional commands,
//! validates supported DML/DDL forms, and translates them into provider
//! operations against HoloStore-backed tables.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    AlterTableOperation, AnalyzeFormat, Assignment, AssignmentTarget, BinaryOperator, ColumnDef,
    ColumnOption, CreateIndex, CreateTable, CreateTableOptions, DataType, Delete, Expr as SqlExpr,
    FromTable, FunctionArg, FunctionArgExpr, FunctionArguments, IndexColumn, IndexOption,
    IndexType, Insert, LimitClause, ObjectName, ObjectType, OrderByKind, Query, SelectItem,
    SetExpr, SqlOption, Statement, TableConstraint, TableFactor, TableObject, TableWithJoins,
    TimezoneInfo, UnaryOperator, UtilityOption, Value, ValueWithSpan,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_postgres::arrow_pg::datatypes::df;
use datafusion_postgres::hooks::QueryHook;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::types::format::FormatOptions;
use holo_store::{HoloStoreClient, RpcVersion};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{info_span, Instrument};

use crate::catalog::{register_table_from_metadata, sync_catalog_from_metadata};
use crate::indexing::{
    create_secondary_index_metadata, drop_secondary_index_metadata_by_name,
    update_secondary_index_state, CreateSecondaryIndexMetadataSpec, SecondaryIndexDistribution,
    SecondaryIndexState, DEFAULT_SECONDARY_INDEX_HASH_BUCKETS, MAX_SECONDARY_INDEX_HASH_BUCKETS,
};
use crate::metadata::{
    apply_column_defaults_for_missing, create_table_metadata, find_table_metadata_by_name,
    table_model_orders_v1, table_model_row_v1, update_table_primary_key_distribution,
    validate_row_against_metadata, validate_table_constraints, CheckBinaryOperator,
    CheckExpression, ColumnDefaultValue, CreateTableMetadataSpec, PrimaryKeyDistribution,
    TableCheckConstraintRecord, TableColumnRecord, TableColumnType,
};
use crate::metrics::PushdownMetrics;
use crate::provider::{
    encode_orders_row_value, encode_orders_tombstone_value, is_duplicate_key_violation_message,
    is_overload_error_message, orders_schema, ConditionalOrderWrite, ConditionalPrimaryWrite,
    ConditionalWriteOutcome, DuplicateKeyViolation, GroupedAggregateFilter,
    GroupedAggregateTopKRow, GroupedAggregateTopKSpec, HoloStoreTableProvider, OrdersSeedRow,
    PrimaryKeyExtreme, VersionedOrdersRow,
};
use crate::topology::{fetch_topology, ClusterTopology};

#[derive(Debug, Clone, Copy)]
/// Represents the `DmlRuntimeConfig` component used by the holo_fusion runtime.
pub struct DmlRuntimeConfig {
    pub prewrite_delay: Duration,
    pub statement_timeout: Duration,
    pub max_inflight_statements: usize,
    pub max_inflight_reads: usize,
    pub max_inflight_writes: usize,
    pub max_inflight_txns: usize,
    pub max_inflight_background: usize,
    pub admission_queue_limit: usize,
    pub admission_wait_timeout: Duration,
    pub max_scan_rows: usize,
    pub max_txn_staged_rows: usize,
}

impl Default for DmlRuntimeConfig {
    /// Executes `default` for this component.
    fn default() -> Self {
        Self {
            prewrite_delay: Duration::ZERO,
            statement_timeout: Duration::ZERO,
            max_inflight_statements: 1024,
            max_inflight_reads: 1024,
            max_inflight_writes: 1024,
            max_inflight_txns: 512,
            max_inflight_background: 256,
            admission_queue_limit: 4096,
            admission_wait_timeout: Duration::ZERO,
            max_scan_rows: 100_000,
            max_txn_staged_rows: 100_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionClass {
    Read,
    Write,
    Transaction,
    Background,
}

impl AdmissionClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Transaction => "transaction",
            Self::Background => "background",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdmissionRejection {
    QueueTimeout,
    QueueLimit,
}

#[derive(Debug)]
struct AdmissionPermit {
    _global: OwnedSemaphorePermit,
    _class: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct AdmissionController {
    global: Arc<Semaphore>,
    reads: Arc<Semaphore>,
    writes: Arc<Semaphore>,
    txns: Arc<Semaphore>,
    background: Arc<Semaphore>,
    wait_timeout: Duration,
    queue_limit: usize,
    read_queue_depth: AtomicUsize,
    write_queue_depth: AtomicUsize,
    txn_queue_depth: AtomicUsize,
    background_queue_depth: AtomicUsize,
}

impl AdmissionController {
    fn new(config: DmlRuntimeConfig) -> Self {
        Self {
            global: Arc::new(Semaphore::new(config.max_inflight_statements.max(1))),
            reads: Arc::new(Semaphore::new(config.max_inflight_reads.max(1))),
            writes: Arc::new(Semaphore::new(config.max_inflight_writes.max(1))),
            txns: Arc::new(Semaphore::new(config.max_inflight_txns.max(1))),
            background: Arc::new(Semaphore::new(config.max_inflight_background.max(1))),
            wait_timeout: config.admission_wait_timeout,
            queue_limit: config.admission_queue_limit.max(1),
            read_queue_depth: AtomicUsize::new(0),
            write_queue_depth: AtomicUsize::new(0),
            txn_queue_depth: AtomicUsize::new(0),
            background_queue_depth: AtomicUsize::new(0),
        }
    }

    fn queue_counter(&self, class: AdmissionClass) -> &AtomicUsize {
        match class {
            AdmissionClass::Read => &self.read_queue_depth,
            AdmissionClass::Write => &self.write_queue_depth,
            AdmissionClass::Transaction => &self.txn_queue_depth,
            AdmissionClass::Background => &self.background_queue_depth,
        }
    }

    fn class_semaphore(&self, class: AdmissionClass) -> Arc<Semaphore> {
        match class {
            AdmissionClass::Read => self.reads.clone(),
            AdmissionClass::Write => self.writes.clone(),
            AdmissionClass::Transaction => self.txns.clone(),
            AdmissionClass::Background => self.background.clone(),
        }
    }

    async fn acquire(
        &self,
        class: AdmissionClass,
        metrics: &PushdownMetrics,
    ) -> std::result::Result<AdmissionPermit, AdmissionRejection> {
        let queue_counter = self.queue_counter(class);
        let queued = queue_counter
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        metrics.record_admission_queue_depth(class.as_str(), queued as u64);
        if queued > self.queue_limit {
            let depth = queue_counter
                .fetch_sub(1, Ordering::Relaxed)
                .saturating_sub(1);
            metrics.record_admission_queue_depth(class.as_str(), depth as u64);
            metrics.record_admission_reject_class(
                class.as_str(),
                crate::metrics::AdmissionRejectKind::QueueLimit,
            );
            return Err(AdmissionRejection::QueueLimit);
        }

        let wait_started = Instant::now();
        let class_sem = self.class_semaphore(class);
        let global_sem = self.global.clone();
        let (global_permit, class_permit) = if self.wait_timeout > Duration::ZERO {
            let class_sem_wait = class_sem.clone();
            let global_sem_wait = global_sem.clone();
            let acquire_fut = async move {
                let class_permit = class_sem_wait.acquire_owned().await;
                let global_permit = global_sem_wait.acquire_owned().await;
                (global_permit, class_permit)
            };
            match tokio::time::timeout(self.wait_timeout, acquire_fut).await {
                Ok((Ok(global_permit), Ok(class_permit))) => (global_permit, class_permit),
                _ => {
                    let depth = queue_counter
                        .fetch_sub(1, Ordering::Relaxed)
                        .saturating_sub(1);
                    metrics.record_admission_queue_depth(class.as_str(), depth as u64);
                    metrics.record_admission_reject_class(
                        class.as_str(),
                        crate::metrics::AdmissionRejectKind::QueueTimeout,
                    );
                    return Err(AdmissionRejection::QueueTimeout);
                }
            }
        } else {
            let class_permit = match class_sem.try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    let depth = queue_counter
                        .fetch_sub(1, Ordering::Relaxed)
                        .saturating_sub(1);
                    metrics.record_admission_queue_depth(class.as_str(), depth as u64);
                    metrics.record_admission_reject_class(
                        class.as_str(),
                        crate::metrics::AdmissionRejectKind::QueueTimeout,
                    );
                    return Err(AdmissionRejection::QueueTimeout);
                }
            };
            let global_permit = match global_sem.try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    drop(class_permit);
                    let depth = queue_counter
                        .fetch_sub(1, Ordering::Relaxed)
                        .saturating_sub(1);
                    metrics.record_admission_queue_depth(class.as_str(), depth as u64);
                    metrics.record_admission_reject_class(
                        class.as_str(),
                        crate::metrics::AdmissionRejectKind::QueueTimeout,
                    );
                    return Err(AdmissionRejection::QueueTimeout);
                }
            };
            (global_permit, class_permit)
        };

        let depth = queue_counter
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        metrics.record_admission_queue_depth(class.as_str(), depth as u64);

        metrics.record_admission_grant(class.as_str(), wait_started.elapsed());
        Ok(AdmissionPermit {
            _global: global_permit,
            _class: class_permit,
        })
    }
}

#[derive(Debug)]
/// Represents the `DmlHook` component used by the holo_fusion runtime.
pub struct DmlHook {
    config: DmlRuntimeConfig,
    tx_manager: Arc<TxnSessionManager>,
    holostore_client: HoloStoreClient,
    pushdown_metrics: Arc<PushdownMetrics>,
    admission: AdmissionController,
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
            admission: AdmissionController::new(config),
        }
    }

    /// Returns a read-only transaction state view for `pg_catalog` compatibility tables.
    pub fn txn_introspection(&self) -> TxnIntrospection {
        TxnIntrospection {
            tx_manager: self.tx_manager.clone(),
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

    /// Executes `session has transaction` for this component.
    async fn session_has_transaction(&self, session_id: &str) -> bool {
        let sessions = self.tx_manager.sessions.lock().await;
        sessions.contains_key(session_id)
    }

    /// Executes `run statement with controls` for this component.
    async fn run_statement_with_controls(
        &self,
        statement: &Statement,
        session_id: &str,
        query_execution_id: &str,
        protocol: QueryProtocol,
        admission_class: AdmissionClass,
        future: impl std::future::Future<Output = PgWireResult<Response>>,
    ) -> PgWireResult<Response> {
        let permit = match self
            .admission
            .acquire(admission_class, self.pushdown_metrics.as_ref())
            .await
        {
            Ok(permit) => permit,
            Err(AdmissionRejection::QueueTimeout) => {
                return Err(to_user_error(
                    "53300",
                    "server is overloaded: admission queue timeout exceeded",
                ));
            }
            Err(AdmissionRejection::QueueLimit) => {
                return Err(to_user_error(
                    "53300",
                    "server is overloaded: admission queue depth limit exceeded",
                ));
            }
        };

        let span = info_span!(
            "holo_fusion.statement",
            query_execution_id = query_execution_id,
            statement = statement_kind(statement),
            protocol = protocol.as_str(),
            session_id = session_id,
            admission_class = admission_class.as_str()
        );

        let execute = async move {
            let _permit = permit;
            future.await
        };

        if self.config.statement_timeout > Duration::ZERO {
            match tokio::time::timeout(self.config.statement_timeout, execute.instrument(span))
                .await
            {
                Ok(result) => result,
                Err(_) => {
                    self.pushdown_metrics.record_statement_timeout();
                    Err(to_user_error(
                        "57014",
                        "statement timeout exceeded before completion",
                    ))
                }
            }
        } else {
            execute.instrument(span).await
        }
    }

    /// Executes `route statement` for this component.
    async fn route_statement(
        &self,
        statement: &Statement,
        params: &ParamValues,
        session_id: &str,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
        protocol: QueryProtocol,
    ) -> Option<PgWireResult<Response>> {
        let query_execution_id = self
            .pushdown_metrics
            .active_query_execution_id(session_id)
            .unwrap_or_else(|| {
                self.pushdown_metrics
                    .ensure_query_execution_for_session(session_id, statement_kind(statement))
            });
        let has_active_txn = self.session_has_transaction(session_id).await;

        if is_txn_control_statement(statement) {
            return Some(
                self.run_statement_with_controls(
                    statement,
                    session_id,
                    query_execution_id.as_str(),
                    protocol,
                    statement_admission_class(statement, false, session_id),
                    self.handle_txn_control(statement, session_context, session_id.to_string()),
                )
                .await,
            );
        }

        if has_active_txn {
            return Some(
                self.run_statement_with_controls(
                    statement,
                    session_id,
                    query_execution_id.as_str(),
                    protocol,
                    statement_admission_class(statement, true, session_id),
                    async {
                        self.route_transactional_statement(
                            statement,
                            params,
                            session_id,
                            session_context,
                            client,
                            protocol,
                        )
                        .await
                        .unwrap_or_else(|| {
                            Err(to_user_error(
                                "0A000",
                                "statement is not supported inside explicit transactions",
                            ))
                        })
                    },
                )
                .await,
            );
        }

        if matches!(statement, Statement::Explain { .. }) {
            return Some(
                self.run_statement_with_controls(
                    statement,
                    session_id,
                    query_execution_id.as_str(),
                    protocol,
                    statement_admission_class(statement, false, session_id),
                    async {
                        execute_explain_statement(
                            statement,
                            session_context,
                            &self.holostore_client,
                            self.pushdown_metrics.as_ref(),
                            client,
                            protocol,
                            query_execution_id.as_str(),
                        )
                        .await
                    },
                )
                .await,
            );
        }

        if let Statement::CreateTable(create) = statement {
            return Some(
                self.run_statement_with_controls(
                    statement,
                    session_id,
                    query_execution_id.as_str(),
                    protocol,
                    statement_admission_class(statement, false, session_id),
                    async {
                        execute_create_table(
                            create,
                            session_context,
                            self.holostore_client.clone(),
                            self.pushdown_metrics.clone(),
                        )
                        .await
                    },
                )
                .await,
            );
        }

        if let Statement::CreateIndex(create_index) = statement {
            return Some(
                self.run_statement_with_controls(
                    statement,
                    session_id,
                    query_execution_id.as_str(),
                    protocol,
                    statement_admission_class(statement, false, session_id),
                    async {
                        execute_create_index(
                            create_index,
                            session_context,
                            self.holostore_client.clone(),
                            self.pushdown_metrics.clone(),
                        )
                        .await
                    },
                )
                .await,
            );
        }

        if let Statement::AlterTable {
            name, operations, ..
        } = statement
        {
            return Some(
                self.run_statement_with_controls(
                    statement,
                    session_id,
                    query_execution_id.as_str(),
                    protocol,
                    statement_admission_class(statement, false, session_id),
                    async {
                        execute_alter_table(
                            name,
                            operations.as_slice(),
                            session_context,
                            self.holostore_client.clone(),
                            self.pushdown_metrics.clone(),
                        )
                        .await
                    },
                )
                .await,
            );
        }

        if let Statement::Drop {
            object_type: ObjectType::Index,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
            table,
        } = statement
        {
            return Some(
                self.run_statement_with_controls(
                    statement,
                    session_id,
                    query_execution_id.as_str(),
                    protocol,
                    statement_admission_class(statement, false, session_id),
                    async {
                        execute_drop_index(
                            *if_exists,
                            names.as_slice(),
                            *cascade,
                            *restrict,
                            *purge,
                            *temporary,
                            table.as_ref(),
                            session_context,
                            self.holostore_client.clone(),
                            self.pushdown_metrics.clone(),
                        )
                        .await
                    },
                )
                .await,
            );
        }

        if let Statement::Insert(insert) = statement {
            return Some(
                self.run_statement_with_controls(
                    statement,
                    session_id,
                    query_execution_id.as_str(),
                    protocol,
                    statement_admission_class(statement, false, session_id),
                    async { execute_insert(insert, params, session_context).await },
                )
                .await,
            );
        }

        // Extended-protocol hooks currently cannot observe portal result format; keep custom
        // fast SELECT responses on simple protocol until hook APIs expose bind/portal formats.
        if protocol == QueryProtocol::Simple {
            if let Some(spec) = extract_fast_grouped_aggregate_topk_statement(statement, params) {
                if let Some(provider) =
                    try_get_holo_provider_for_table(session_context, spec.table_name.as_str()).await
                {
                    if provider.is_row_v1_table() {
                        let pushdown_metrics = self.pushdown_metrics.clone();
                        let query_execution_id_owned = query_execution_id.clone();
                        return Some(
                            self.run_statement_with_controls(
                                statement,
                                session_id,
                                query_execution_id.as_str(),
                                protocol,
                                statement_admission_class(statement, false, session_id),
                                async move {
                                    execute_fast_grouped_aggregate_topk_query(
                                        provider,
                                        spec,
                                        pushdown_metrics.as_ref(),
                                        query_execution_id_owned.as_str(),
                                        client,
                                        protocol,
                                    )
                                    .await
                                },
                            )
                            .await,
                        );
                    }
                }
            }
        }

        if protocol == QueryProtocol::Simple {
            if let Some(spec) = extract_fast_primary_key_aggregate_statement(statement) {
                if let Some(provider) =
                    try_get_holo_provider_for_table(session_context, spec.table_name.as_str()).await
                {
                    if spec
                        .column_name
                        .eq_ignore_ascii_case(provider.primary_key_column())
                    {
                        return Some(
                            self.run_statement_with_controls(
                                statement,
                                session_id,
                                query_execution_id.as_str(),
                                protocol,
                                statement_admission_class(statement, false, session_id),
                                async move {
                                    execute_fast_primary_key_aggregate_query(
                                        provider, spec, client, protocol,
                                    )
                                    .await
                                },
                            )
                            .await,
                        );
                    }
                }
            }
        }

        if !matches!(statement, Statement::Update { .. } | Statement::Delete(_)) {
            return None;
        }

        Some(
            self.run_statement_with_controls(
                statement,
                session_id,
                query_execution_id.as_str(),
                protocol,
                statement_admission_class(statement, false, session_id),
                async {
                    execute_dml(
                        statement,
                        params,
                        session_context,
                        self.config,
                        self.pushdown_metrics.as_ref(),
                    )
                    .await
                },
            )
            .await,
        )
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

        // Decision: evaluate `matches!(state, SessionTxnState::Aborted { .. })` to choose the correct SQL/storage control path.
        if matches!(state, SessionTxnState::Aborted { .. }) {
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
                    self.config,
                    self.pushdown_metrics.as_ref(),
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
                    self.config,
                    self.pushdown_metrics.as_ref(),
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
                    self.config,
                    self.pushdown_metrics.as_ref(),
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
                    let mut sessions = self.tx_manager.sessions.lock().await;
                    // Decision: evaluate `sessions.contains_key(&session_id)` to choose the correct SQL/storage control path.
                    if sessions.contains_key(&session_id) {
                        return Ok(Response::TransactionStart(Tag::new("BEGIN")));
                    }
                    let meta = TxnSessionMeta {
                        backend_pid: parse_backend_pid(&session_id),
                        session_seq: parse_session_seq(&session_id),
                        txid: self
                            .tx_manager
                            .next_tx_id
                            .fetch_add(1, Ordering::SeqCst)
                            .saturating_add(1),
                    };
                    sessions.insert(
                        session_id,
                        SessionTxnState::Active {
                            meta,
                            txn: TxnContext::default(),
                        },
                    );
                }
                self.pushdown_metrics.record_tx_begin();
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
                    return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
                };

                let SessionTxnState::Active { meta, txn } = state else {
                    return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
                };

                let writes = txn.as_conditional_writes();
                // Decision: evaluate `writes.is_empty()` to choose the correct SQL/storage control path.
                if writes.is_empty() {
                    self.pushdown_metrics.record_tx_commit(Duration::ZERO);
                    return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
                }
                let table_name = txn.table_name.ok_or_else(|| {
                    api_error("transaction has pending writes but no bound target table")
                })?;

                let provider = get_provider_for_table(session_context, table_name.as_str()).await?;
                maybe_prewrite_delay(self.config).await;
                let commit_start = Instant::now();
                // Decision: evaluate `provider` to choose the correct SQL/storage control path.
                match provider
                    .apply_orders_writes_conditional(&writes, "txn_commit")
                    .await
                    .map_err(|err| api_error(err.to_string()))?
                {
                    ConditionalWriteOutcome::Applied(_) => {
                        self.pushdown_metrics
                            .record_tx_commit(commit_start.elapsed());
                        Ok(Response::TransactionEnd(Tag::new("COMMIT")))
                    }
                    ConditionalWriteOutcome::Conflict => {
                        self.pushdown_metrics.record_tx_conflict();
                        let mut sessions = self.tx_manager.sessions.lock().await;
                        sessions.insert(session_id, SessionTxnState::Aborted { meta });
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
                    return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
                }

                self.pushdown_metrics.record_tx_rollback();
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

impl QueryProtocol {
    /// Executes `as str` for this component.
    fn as_str(self) -> &'static str {
        match self {
            QueryProtocol::Simple => "simple",
            QueryProtocol::Extended => "extended",
        }
    }
}

/// Maps query protocol to the wire format used by hook-built query responses.
fn hook_query_wire_format(protocol: QueryProtocol) -> Format {
    match protocol {
        QueryProtocol::Simple => Format::UnifiedText,
        QueryProtocol::Extended => Format::UnifiedBinary,
    }
}

const SESSION_TXN_ID_KEY: &str = "holo_fusion.txn.session_id";
const SESSION_QUERY_EXEC_ID_KEY: &str = "holo_fusion.query.execution_id";

/// Extracts backend PID from our generated session id.
fn parse_backend_pid(session_id: &str) -> i32 {
    session_id
        .split(':')
        .next()
        .and_then(|raw| raw.parse::<i32>().ok())
        .unwrap_or(0)
}

/// Extracts per-session sequence number from our generated session id.
fn parse_session_seq(session_id: &str) -> u64 {
    session_id
        .rsplit(':')
        .next()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Enumerates states/variants for `TxnLifecycleState`.
pub enum TxnLifecycleState {
    Active,
    Aborted,
}

#[derive(Debug, Clone)]
/// Represents one live SQL transaction entry for compatibility introspection.
pub struct ActiveTxnSnapshot {
    pub backend_pid: i32,
    pub session_seq: u64,
    pub txid: u64,
    pub state: TxnLifecycleState,
}

#[derive(Debug, Clone)]
/// Provides read-only snapshots of SQL transaction state for catalog providers.
pub struct TxnIntrospection {
    tx_manager: Arc<TxnSessionManager>,
}

impl TxnIntrospection {
    /// Returns a monotonic current transaction-id hint for xid age calculations.
    pub fn current_txid_hint(&self) -> u64 {
        self.tx_manager.next_tx_id.load(Ordering::SeqCst).max(1)
    }

    /// Captures active/aborted explicit transaction sessions for introspection.
    pub async fn snapshot_active_transactions(&self) -> Vec<ActiveTxnSnapshot> {
        let sessions = self.tx_manager.sessions.lock().await;
        let mut snapshot = Vec::with_capacity(sessions.len());
        for state in sessions.values() {
            match state {
                SessionTxnState::Active { meta, .. } => snapshot.push(ActiveTxnSnapshot {
                    backend_pid: meta.backend_pid,
                    session_seq: meta.session_seq,
                    txid: meta.txid,
                    state: TxnLifecycleState::Active,
                }),
                SessionTxnState::Aborted { meta } => snapshot.push(ActiveTxnSnapshot {
                    backend_pid: meta.backend_pid,
                    session_seq: meta.session_seq,
                    txid: meta.txid,
                    state: TxnLifecycleState::Aborted,
                }),
            }
        }
        snapshot
    }
}

#[derive(Debug, Default)]
/// Represents the `TxnSessionManager` component used by the holo_fusion runtime.
struct TxnSessionManager {
    next_session_id: AtomicU64,
    next_tx_id: AtomicU64,
    sessions: tokio::sync::Mutex<HashMap<String, SessionTxnState>>,
}

#[derive(Debug, Clone)]
/// Stable transaction/session identity metadata used by lock introspection.
struct TxnSessionMeta {
    backend_pid: i32,
    session_seq: u64,
    txid: u64,
}

#[derive(Debug, Clone)]
/// Enumerates states/variants for `SessionTxnState`.
enum SessionTxnState {
    Active {
        meta: TxnSessionMeta,
        txn: TxnContext,
    },
    Aborted {
        meta: TxnSessionMeta,
    },
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
    fn stage_row_for_order(
        &mut self,
        order_id: i64,
        row: Option<OrdersSeedRow>,
        max_txn_staged_rows: usize,
    ) -> PgWireResult<()> {
        let creating_new_entry = !self.pending_writes.contains_key(&order_id);
        if creating_new_entry && self.pending_writes.len() >= max_txn_staged_rows.max(1) {
            return Err(to_user_error(
                "54000",
                format!(
                    "transaction staged row limit exceeded (max_txn_staged_rows={})",
                    max_txn_staged_rows.max(1)
                ),
            ));
        }

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
        Ok(())
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
        let session_id = self.session_id_for_client(client);
        let query_execution_id = self.pushdown_metrics.begin_query_execution(
            session_id.as_str(),
            statement_kind(statement),
            QueryProtocol::Simple.as_str(),
        );
        client.metadata_mut().insert(
            SESSION_QUERY_EXEC_ID_KEY.to_string(),
            query_execution_id.clone(),
        );
        let params = ParamValues::List(Vec::new());
        let routed = self
            .route_statement(
                statement,
                &params,
                session_id.as_str(),
                session_context,
                client,
                QueryProtocol::Simple,
            )
            .await;
        if let Some(result) = &routed {
            self.pushdown_metrics.finish_query_execution(
                session_id.as_str(),
                query_execution_id.as_str(),
                result.is_ok(),
            );
        }
        routed
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
            Statement::Update { .. }
                | Statement::Delete(_)
                | Statement::CreateTable(_)
                | Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
                | Statement::Insert(_)
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
        let session_id = self.session_id_for_client(client);
        let query_execution_id = self.pushdown_metrics.begin_query_execution(
            session_id.as_str(),
            statement_kind(statement),
            QueryProtocol::Extended.as_str(),
        );
        client.metadata_mut().insert(
            SESSION_QUERY_EXEC_ID_KEY.to_string(),
            query_execution_id.clone(),
        );
        let routed = self
            .route_statement(
                statement,
                params,
                session_id.as_str(),
                session_context,
                client,
                QueryProtocol::Extended,
            )
            .await;
        if let Some(result) = &routed {
            self.pushdown_metrics.finish_query_execution(
                session_id.as_str(),
                query_execution_id.as_str(),
                result.is_ok(),
            );
        }
        routed
    }
}

/// Executes `is txn control statement` for this component.
fn is_txn_control_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::StartTransaction { .. } | Statement::Commit { .. } | Statement::Rollback { .. }
    )
}

/// Executes `statement kind` for this component.
fn statement_kind(statement: &Statement) -> &'static str {
    match statement {
        Statement::StartTransaction { .. } => "BEGIN",
        Statement::Commit { .. } => "COMMIT",
        Statement::Rollback { .. } => "ROLLBACK",
        Statement::CreateTable(_) => "CREATE_TABLE",
        Statement::CreateIndex(_) => "CREATE_INDEX",
        Statement::AlterTable { .. } => "ALTER_TABLE",
        Statement::Drop {
            object_type: ObjectType::Index,
            ..
        } => "DROP_INDEX",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::Insert(_) => "INSERT",
        Statement::Explain { .. } => "EXPLAIN",
        Statement::Query(_) => "QUERY",
        _ => "OTHER",
    }
}

#[derive(Debug, Clone)]
struct FastPrimaryKeyAggregateSpec {
    table_name: String,
    column_name: String,
    extreme: PrimaryKeyExtreme,
    coalesce_fallback: Option<i64>,
    output_alias: Option<String>,
}

/// Extracts supported `MIN/MAX(primary_key)` aggregate shape from a statement.
fn extract_fast_primary_key_aggregate_statement(
    statement: &Statement,
) -> Option<FastPrimaryKeyAggregateSpec> {
    let Statement::Query(query) = statement else {
        return None;
    };
    extract_fast_primary_key_aggregate_query(query.as_ref())
}

/// Extracts supported `MIN/MAX(primary_key)` aggregate shape from a query.
fn extract_fast_primary_key_aggregate_query(query: &Query) -> Option<FastPrimaryKeyAggregateSpec> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.selection.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
    {
        return None;
    }
    match &select.group_by {
        datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(exprs, _) if exprs.is_empty() => {
        }
        _ => return None,
    }

    if select.projection.len() != 1 || select.from.len() != 1 {
        return None;
    }

    let from = select.from.first()?;
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &from.relation else {
        return None;
    };
    let table_name = object_name_leaf(name).ok()?;

    let (expr, output_alias) = match select.projection.first()? {
        SelectItem::UnnamedExpr(expr) => (expr, None),
        SelectItem::ExprWithAlias { expr, alias } => (expr, Some(normalize_ident(alias))),
        _ => return None,
    };

    let (column_name, extreme, coalesce_fallback) = extract_primary_key_aggregate_expr(expr)?;
    Some(FastPrimaryKeyAggregateSpec {
        table_name,
        column_name,
        extreme,
        coalesce_fallback,
        output_alias,
    })
}

/// Extracts `column`, `extreme`, and optional COALESCE fallback from one projection expression.
fn extract_primary_key_aggregate_expr(
    expr: &SqlExpr,
) -> Option<(String, PrimaryKeyExtreme, Option<i64>)> {
    match expr {
        SqlExpr::Function(function) if is_function_named(function, "coalesce") => {
            let args = function_args_list(function)?;
            if args.len() != 2 {
                return None;
            }
            let aggregate_expr = function_arg_as_expr(args.first()?)?;
            let fallback_expr = function_arg_as_expr(args.get(1)?)?;
            let (column_name, extreme) = extract_min_max_function(aggregate_expr)?;
            let fallback = parse_i64_literal(fallback_expr)?;
            Some((column_name, extreme, Some(fallback)))
        }
        SqlExpr::Function(_) => {
            let (column_name, extreme) = extract_min_max_function(expr)?;
            Some((column_name, extreme, None))
        }
        _ => None,
    }
}

/// Extracts MIN/MAX aggregate and target column from one SQL expression.
fn extract_min_max_function(expr: &SqlExpr) -> Option<(String, PrimaryKeyExtreme)> {
    let SqlExpr::Function(function) = expr else {
        return None;
    };
    let extreme = if is_function_named(function, "max") {
        PrimaryKeyExtreme::Max
    } else if is_function_named(function, "min") {
        PrimaryKeyExtreme::Min
    } else {
        return None;
    };

    if function.parameters != FunctionArguments::None
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }
    let args = function_args_list(function)?;
    if args.len() != 1 {
        return None;
    }
    let column_name = sql_expr_column_name(function_arg_as_expr(args.first()?)?)?;
    Some((column_name, extreme))
}

/// Returns normalized function name equality.
fn is_function_named(function: &datafusion::sql::sqlparser::ast::Function, expected: &str) -> bool {
    function
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(normalize_ident)
        .map(|name| name.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
}

/// Returns simple positional function args (`f(arg1, arg2)`).
fn function_args_list(
    function: &datafusion::sql::sqlparser::ast::Function,
) -> Option<&[FunctionArg]> {
    match &function.args {
        FunctionArguments::List(list)
            if list.duplicate_treatment.is_none() && list.clauses.is_empty() =>
        {
            Some(list.args.as_slice())
        }
        _ => None,
    }
}

/// Extracts one unnamed expression argument.
fn function_arg_as_expr(arg: &FunctionArg) -> Option<&SqlExpr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Some(expr),
        _ => None,
    }
}

/// Extracts one normalized column name from a SQL expression.
fn sql_expr_column_name(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(ident) => Some(normalize_ident(ident)),
        SqlExpr::CompoundIdentifier(idents) => idents.last().map(normalize_ident),
        _ => None,
    }
}

/// Parses one signed 64-bit integer literal.
fn parse_i64_literal(expr: &SqlExpr) -> Option<i64> {
    match expr {
        SqlExpr::Value(ValueWithSpan {
            value: Value::Number(raw, _),
            ..
        }) => raw.parse::<i64>().ok(),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => parse_i64_literal(expr).map(|value| -value),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => parse_i64_literal(expr),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct FastGroupedAggregateTopKSpec {
    table_name: String,
    group_columns: Vec<String>,
    sum_column: String,
    filters: Vec<FastGroupedFilterSpec>,
    having_min_count: u64,
    limit: usize,
    output_columns: Vec<FastGroupedOutputColumn>,
}

#[derive(Debug, Clone)]
struct FastGroupedFilterSpec {
    column: String,
    allowed_values: Vec<ScalarValue>,
}

#[derive(Debug, Clone)]
enum FastGroupedOutputColumn {
    GroupColumn { column: String, output_name: String },
    Count { output_name: String },
    Sum { output_name: String },
}

/// Extracts grouped aggregate + top-k shape from a statement for provider pushdown fast path.
fn extract_fast_grouped_aggregate_topk_statement(
    statement: &Statement,
    params: &ParamValues,
) -> Option<FastGroupedAggregateTopKSpec> {
    let Statement::Query(query) = statement else {
        return None;
    };
    extract_fast_grouped_aggregate_topk_query(query.as_ref(), params)
}

/// Extracts grouped aggregate + top-k shape from a query for provider pushdown fast path.
fn extract_fast_grouped_aggregate_topk_query(
    query: &Query,
    params: &ParamValues,
) -> Option<FastGroupedAggregateTopKSpec> {
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
    {
        return None;
    }

    if select.from.len() != 1 {
        return None;
    }
    let from = select.from.first()?;
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table { name, .. } = &from.relation else {
        return None;
    };
    let table_name = object_name_leaf(name).ok()?;

    let group_columns = match &select.group_by {
        datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(exprs, _)
            if !exprs.is_empty() =>
        {
            let mut out = Vec::<String>::with_capacity(exprs.len());
            for expr in exprs {
                out.push(sql_expr_column_name(expr)?);
            }
            out
        }
        _ => return None,
    };

    let mut output_columns = Vec::<FastGroupedOutputColumn>::with_capacity(select.projection.len());
    let mut count_output_name = None::<String>;
    let mut sum_output_name = None::<String>;
    let mut sum_column = None::<String>;
    for item in &select.projection {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(normalize_ident(alias))),
            _ => return None,
        };

        if let Some(column) = sql_expr_column_name(expr) {
            if !group_columns
                .iter()
                .any(|group_col| group_col.eq_ignore_ascii_case(column.as_str()))
            {
                return None;
            }
            output_columns.push(FastGroupedOutputColumn::GroupColumn {
                column: column.clone(),
                output_name: alias.unwrap_or(column),
            });
            continue;
        }

        if is_count_star_expr(expr) {
            if count_output_name.is_some() {
                return None;
            }
            let name = alias.unwrap_or_else(|| "count".to_string());
            count_output_name = Some(name.clone());
            output_columns.push(FastGroupedOutputColumn::Count { output_name: name });
            continue;
        }

        if let Some(column) = extract_sum_column(expr) {
            if sum_column.is_some() {
                return None;
            }
            sum_column = Some(column);
            let name = alias.unwrap_or_else(|| "sum".to_string());
            sum_output_name = Some(name.clone());
            output_columns.push(FastGroupedOutputColumn::Sum { output_name: name });
            continue;
        }

        return None;
    }

    let sum_column = sum_column?;
    let _count_output_name = count_output_name?;
    let sum_output_name = sum_output_name?;

    let filters = parse_grouped_where_filters(select.selection.as_ref(), params)?;
    let having_min_count = parse_grouped_having_min_count(select.having.as_ref())?;

    let order_by = query.order_by.as_ref()?;
    let OrderByKind::Expressions(ordering) = &order_by.kind else {
        return None;
    };
    if ordering.len() != 1 || ordering[0].options.asc != Some(false) {
        return None;
    }
    if ordering[0].with_fill.is_some() {
        return None;
    }
    if !matches_order_by_sum(
        &ordering[0].expr,
        sum_output_name.as_str(),
        sum_column.as_str(),
    ) {
        return None;
    }

    let limit = parse_topk_limit(query.limit_clause.as_ref())?;
    Some(FastGroupedAggregateTopKSpec {
        table_name,
        group_columns,
        sum_column,
        filters,
        having_min_count,
        limit,
        output_columns,
    })
}

fn parse_grouped_where_filters(
    selection: Option<&SqlExpr>,
    params: &ParamValues,
) -> Option<Vec<FastGroupedFilterSpec>> {
    let Some(selection) = selection else {
        return Some(Vec::new());
    };
    let mut terms = Vec::<&SqlExpr>::new();
    collect_sql_conjuncts(selection, &mut terms);
    let mut by_column = BTreeMap::<String, Vec<ScalarValue>>::new();
    for term in terms {
        let (column, values) = parse_grouped_filter_term(term, params)?;
        by_column.entry(column).or_default().extend(values);
    }

    let mut filters = Vec::<FastGroupedFilterSpec>::with_capacity(by_column.len());
    for (column, mut values) in by_column {
        values.dedup();
        if values.is_empty() {
            return Some(Vec::new());
        }
        filters.push(FastGroupedFilterSpec {
            column,
            allowed_values: values,
        });
    }
    Some(filters)
}

fn collect_sql_conjuncts<'a>(expr: &'a SqlExpr, out: &mut Vec<&'a SqlExpr>) {
    match expr {
        SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            collect_sql_conjuncts(left.as_ref(), out);
            collect_sql_conjuncts(right.as_ref(), out);
        }
        _ => out.push(expr),
    }
}

fn parse_grouped_filter_term(
    term: &SqlExpr,
    params: &ParamValues,
) -> Option<(String, Vec<ScalarValue>)> {
    match term {
        SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
            if let (Some(column), Some(value)) = (
                sql_expr_column_name(left.as_ref()),
                resolve_sql_expr_scalar(right.as_ref(), params)
                    .ok()
                    .flatten(),
            ) {
                return Some((column, vec![value]));
            }
            if let (Some(column), Some(value)) = (
                sql_expr_column_name(right.as_ref()),
                resolve_sql_expr_scalar(left.as_ref(), params)
                    .ok()
                    .flatten(),
            ) {
                return Some((column, vec![value]));
            }
            None
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } if !negated => {
            let column = sql_expr_column_name(expr.as_ref())?;
            if list.is_empty() {
                return None;
            }
            let mut values = Vec::<ScalarValue>::with_capacity(list.len());
            for value_expr in list {
                values.push(resolve_sql_expr_scalar(value_expr, params).ok().flatten()?);
            }
            Some((column, values))
        }
        _ => None,
    }
}

fn resolve_sql_expr_scalar(
    expr: &SqlExpr,
    params: &ParamValues,
) -> PgWireResult<Option<ScalarValue>> {
    match expr {
        SqlExpr::Value(value) => Ok(Some(resolve_sql_value(value, params)?)),
        SqlExpr::Nested(inner) => resolve_sql_expr_scalar(inner, params),
        SqlExpr::Cast { expr, .. } => resolve_sql_expr_scalar(expr, params),
        SqlExpr::UnaryOp { op, expr } => {
            let Some(value) = resolve_sql_expr_scalar(expr, params)? else {
                return Ok(None);
            };
            match op {
                UnaryOperator::Plus => Ok(Some(value)),
                UnaryOperator::Minus => {
                    let Some(int_value) = scalar_to_i64(value) else {
                        return Ok(None);
                    };
                    Ok(Some(ScalarValue::Int64(Some(int_value.saturating_neg()))))
                }
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

fn parse_grouped_having_min_count(having: Option<&SqlExpr>) -> Option<u64> {
    let Some(expr) = having else {
        return Some(0);
    };
    let SqlExpr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    match op {
        BinaryOperator::GtEq => {
            if is_count_star_expr(left.as_ref()) {
                return parse_i64_literal(right.as_ref()).and_then(|v| u64::try_from(v).ok());
            }
            if is_count_star_expr(right.as_ref()) {
                return parse_i64_literal(left.as_ref()).and_then(|v| u64::try_from(v).ok());
            }
            None
        }
        BinaryOperator::Gt => {
            if is_count_star_expr(left.as_ref()) {
                return parse_i64_literal(right.as_ref())
                    .and_then(|v| v.checked_add(1))
                    .and_then(|v| u64::try_from(v).ok());
            }
            if is_count_star_expr(right.as_ref()) {
                return parse_i64_literal(left.as_ref()).and_then(|v| u64::try_from(v).ok());
            }
            None
        }
        _ => None,
    }
}

fn parse_topk_limit(limit_clause: Option<&LimitClause>) -> Option<usize> {
    match limit_clause? {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if offset.is_some() || !limit_by.is_empty() {
                return None;
            }
            let limit = limit.as_ref()?;
            let value = parse_i64_literal(limit)?;
            usize::try_from(value).ok()
        }
        LimitClause::OffsetCommaLimit { .. } => None,
    }
}

fn is_count_star_expr(expr: &SqlExpr) -> bool {
    let SqlExpr::Function(function) = expr else {
        return false;
    };
    if !is_function_named(function, "count")
        || function.parameters != FunctionArguments::None
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return false;
    }
    let Some(args) = function_args_list(function) else {
        return false;
    };
    if args.len() != 1 {
        return false;
    }
    matches!(
        args[0],
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
            | FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_))
    )
}

fn extract_sum_column(expr: &SqlExpr) -> Option<String> {
    let SqlExpr::Function(function) = expr else {
        return None;
    };
    if !is_function_named(function, "sum")
        || function.parameters != FunctionArguments::None
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }
    let args = function_args_list(function)?;
    if args.len() != 1 {
        return None;
    }
    sql_expr_column_name(function_arg_as_expr(args.first()?)?)
}

fn matches_order_by_sum(order_expr: &SqlExpr, sum_alias: &str, sum_column: &str) -> bool {
    if let Some(name) = sql_expr_column_name(order_expr) {
        if name.eq_ignore_ascii_case(sum_alias) {
            return true;
        }
    }
    extract_sum_column(order_expr)
        .map(|column| column.eq_ignore_ascii_case(sum_column))
        .unwrap_or(false)
}

/// Maps one statement/session context to an admission workload class.
fn statement_admission_class(
    statement: &Statement,
    in_explicit_txn: bool,
    session_id: &str,
) -> AdmissionClass {
    let session_is_background =
        session_id.starts_with("background:") || session_id.starts_with("internal_background:");
    if session_is_background {
        return AdmissionClass::Background;
    }
    if in_explicit_txn || is_txn_control_statement(statement) {
        return AdmissionClass::Transaction;
    }
    match statement {
        Statement::Query(_) | Statement::Explain { .. } => AdmissionClass::Read,
        _ => AdmissionClass::Write,
    }
}

/// Executes `execute dml` for this component.
async fn execute_dml(
    statement: &Statement,
    params: &ParamValues,
    session_context: &SessionContext,
    config: DmlRuntimeConfig,
    pushdown_metrics: &PushdownMetrics,
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
                pushdown_metrics,
            )
            .await
        }
        Statement::Delete(delete) => {
            execute_delete(delete, params, session_context, config, pushdown_metrics).await
        }
        _ => Err(PgWireError::ApiError(Box::new(std::io::Error::other(
            "unsupported dml statement in hook",
        )))),
    }
}

/// Executes INSERT statements in the SQL hook path for VALUES-based inserts.
async fn execute_insert(
    insert: &Insert,
    params: &ParamValues,
    session_context: &SessionContext,
) -> PgWireResult<Response> {
    if insert.returning.is_some() {
        return Err(to_user_error(
            "0A000",
            "INSERT ... RETURNING is not supported",
        ));
    }
    if insert.on.is_some() || insert.ignore || insert.or.is_some() || insert.replace_into {
        return Err(to_user_error(
            "0A000",
            "INSERT conflict clauses are not supported",
        ));
    }
    if !insert.assignments.is_empty() {
        return Err(to_user_error(
            "0A000",
            "INSERT ... SET syntax is not supported",
        ));
    }

    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| to_user_error("0A000", "INSERT source is required"))?;
    let SetExpr::Values(values) = source.body.as_ref() else {
        return execute_insert_via_datafusion(insert, session_context).await;
    };
    if values.rows.is_empty() {
        return Ok(Response::Execution(
            Tag::new("INSERT").with_oid(0).with_rows(0),
        ));
    }

    let table_name = insert_table_name(&insert.table)?;
    let provider = get_provider_for_table(session_context, table_name.as_str()).await?;

    if provider.is_orders_v1_table() {
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
                .map(normalize_ident)
                .collect::<Vec<_>>()
        };
        let rows = values
            .rows
            .iter()
            .map(|exprs| decode_insert_orders_row(exprs, &columns, params))
            .collect::<PgWireResult<Vec<_>>>()?;
        let written = provider
            .insert_orders_rows(rows.as_slice())
            .await
            .map_err(map_insert_provider_error)?;
        return Ok(Response::Execution(
            Tag::new("INSERT").with_oid(0).with_rows(written as usize),
        ));
    }

    let columns = provider.columns();
    if columns.is_empty() {
        return Err(api_error("row_v1 table has empty column metadata"));
    }

    let mut by_name = HashMap::<String, usize>::new();
    for (idx, column) in columns.iter().enumerate() {
        by_name.insert(column.name.to_ascii_lowercase(), idx);
    }

    let target_indexes = if insert.columns.is_empty() {
        (0..columns.len()).collect::<Vec<_>>()
    } else {
        let mut seen = HashSet::<String>::new();
        let mut indexes = Vec::with_capacity(insert.columns.len());
        for ident in &insert.columns {
            let normalized = normalize_ident(ident).to_ascii_lowercase();
            if !seen.insert(normalized.clone()) {
                return Err(to_user_error(
                    "42601",
                    format!("column '{}' specified more than once", normalized),
                ));
            }
            let Some(index) = by_name.get(normalized.as_str()).copied() else {
                return Err(to_user_error(
                    "42703",
                    format!("column '{}' does not exist", normalized),
                ));
            };
            indexes.push(index);
        }
        indexes
    };

    let now_timestamp_ns = now_timestamp_nanos();
    let mut generic_rows = Vec::<Vec<ScalarValue>>::with_capacity(values.rows.len());
    for exprs in &values.rows {
        if exprs.len() != target_indexes.len() {
            return Err(to_user_error(
                "42601",
                format!(
                    "INSERT row has {} expressions but {} target columns",
                    exprs.len(),
                    target_indexes.len()
                ),
            ));
        }

        let mut row = vec![ScalarValue::Null; columns.len()];
        let mut missing_mask = vec![true; columns.len()];
        for (expr, column_idx) in exprs.iter().zip(target_indexes.iter()) {
            row[*column_idx] = parse_update_scalar_for_column(expr, params, &columns[*column_idx])?;
            missing_mask[*column_idx] = false;
        }

        apply_column_defaults_for_missing(
            columns,
            row.as_mut_slice(),
            missing_mask.as_slice(),
            now_timestamp_ns,
        )
        .map_err(row_constraint_error)?;
        validate_row_against_metadata(columns, provider.check_constraints(), row.as_slice())
            .map_err(row_constraint_error)?;
        generic_rows.push(row);
    }

    let written = provider
        .insert_generic_rows(generic_rows.as_slice())
        .await
        .map_err(map_insert_provider_error)?;
    Ok(Response::Execution(
        Tag::new("INSERT").with_oid(0).with_rows(written as usize),
    ))
}

/// Executes `execute insert via datafusion` for non-VALUES INSERT forms.
async fn execute_insert_via_datafusion(
    insert: &Insert,
    session_context: &SessionContext,
) -> PgWireResult<Response> {
    let mut rewritten_insert = insert.clone();
    rewrite_insert_source_fast_primary_key_aggregates(&mut rewritten_insert, session_context)
        .await?;

    let statement = Statement::Insert(rewritten_insert);
    let df_statement = datafusion::sql::parser::Statement::Statement(Box::new(statement));
    let logical_plan = session_context
        .state()
        .statement_to_plan(df_statement)
        .await
        .map_err(map_insert_datafusion_error)?;
    let optimized = session_context
        .state()
        .optimize(&logical_plan)
        .map_err(map_insert_datafusion_error)?;
    let dataframe = session_context
        .execute_logical_plan(optimized)
        .await
        .map_err(map_insert_datafusion_error)?;
    let result = dataframe
        .collect()
        .await
        .map_err(map_insert_datafusion_error)?;

    let rows_affected = result
        .first()
        .and_then(|batch| batch.column_by_name("count"))
        .and_then(|col| {
            col.as_any()
                .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        })
        .map_or(0usize, |array| array.value(0) as usize);
    Ok(Response::Execution(
        Tag::new("INSERT").with_oid(0).with_rows(rows_affected),
    ))
}

/// Rewrites fast-path `MIN/MAX(primary_key)` aggregate subqueries inside INSERT source query.
async fn rewrite_insert_source_fast_primary_key_aggregates(
    insert: &mut Insert,
    session_context: &SessionContext,
) -> PgWireResult<()> {
    let Some(source) = insert.source.as_mut() else {
        return Ok(());
    };

    if let Some(replacement) =
        rewrite_query_if_fast_primary_key_aggregate(source.as_ref(), session_context).await?
    {
        *source = Box::new(replacement);
        return Ok(());
    }

    if let Some(with) = source.with.as_mut() {
        for cte in &mut with.cte_tables {
            if let Some(replacement) =
                rewrite_query_if_fast_primary_key_aggregate(cte.query.as_ref(), session_context)
                    .await?
            {
                cte.query = Box::new(replacement);
            }
        }
    }
    Ok(())
}

/// Returns a rewritten literal query when the input matches fast PK aggregate shape.
async fn rewrite_query_if_fast_primary_key_aggregate(
    query: &Query,
    session_context: &SessionContext,
) -> PgWireResult<Option<Query>> {
    let Some(spec) = extract_fast_primary_key_aggregate_query(query) else {
        return Ok(None);
    };
    let Some(provider) =
        try_get_holo_provider_for_table(session_context, spec.table_name.as_str()).await
    else {
        return Ok(None);
    };
    if !spec
        .column_name
        .eq_ignore_ascii_case(provider.primary_key_column())
    {
        return Ok(None);
    }

    let value = evaluate_fast_primary_key_aggregate(provider, &spec).await?;
    let replacement = parse_literal_select_query(value, spec.output_alias.as_deref())?;
    Ok(Some(replacement))
}

/// Evaluates one fast PK aggregate on a table provider.
async fn evaluate_fast_primary_key_aggregate(
    provider: HoloStoreTableProvider,
    spec: &FastPrimaryKeyAggregateSpec,
) -> PgWireResult<Option<i64>> {
    let value = provider
        .scan_primary_key_extreme(spec.extreme)
        .await
        .map_err(|err| api_error(err.to_string()))?;
    Ok(match value {
        Some(value) => Some(value),
        None => spec.coalesce_fallback,
    })
}

/// Parses `SELECT <literal> [AS <alias>]` into a query AST.
fn parse_literal_select_query(value: Option<i64>, alias: Option<&str>) -> PgWireResult<Query> {
    let value_sql = value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "NULL".to_string());
    let alias_sql = alias
        .map(|name| format!(" AS {}", quote_ident_sql(name)))
        .unwrap_or_default();
    let sql = format!("SELECT {value_sql}{alias_sql}");

    let dialect = PostgreSqlDialect {};
    let mut statements =
        Parser::parse_sql(&dialect, sql.as_str()).map_err(|err| api_error(err.to_string()))?;
    if statements.len() != 1 {
        return Err(api_error(format!(
            "fast aggregate rewrite produced {} statements",
            statements.len()
        )));
    }
    match statements.remove(0) {
        Statement::Query(query) => Ok(*query),
        other => Err(api_error(format!(
            "fast aggregate rewrite expected query statement, got {other:?}"
        ))),
    }
}

/// Quotes one SQL identifier for parser-safe literal query generation.
fn quote_ident_sql(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Maps provider INSERT errors to PostgreSQL-compatible SQLSTATEs.
fn map_insert_provider_error(err: anyhow::Error) -> PgWireError {
    if let Some(duplicate) = err.downcast_ref::<DuplicateKeyViolation>() {
        return to_user_error("23505", duplicate.to_string());
    }
    map_runtime_error(err)
}

/// Maps DataFusion INSERT execution errors to PostgreSQL-compatible SQLSTATEs.
fn map_insert_datafusion_error(err: impl std::fmt::Display) -> PgWireError {
    let message = err.to_string();
    if is_duplicate_key_violation_message(message.as_str()) {
        return to_user_error("23505", extract_duplicate_key_message(message.as_str()));
    }
    map_runtime_error(message)
}

/// Maps runtime/provider errors to user SQLSTATEs when possible.
fn map_runtime_error(err: impl std::fmt::Display) -> PgWireError {
    let message = err.to_string();
    if is_duplicate_key_violation_message(message.as_str()) {
        return to_user_error("23505", extract_duplicate_key_message(message.as_str()));
    }
    if is_overload_error_message(message.as_str()) {
        return to_user_error("53300", message);
    }
    api_error(message)
}

/// Extracts a concise duplicate-key violation message from nested execution errors.
fn extract_duplicate_key_message(message: &str) -> String {
    for line in message.lines() {
        if is_duplicate_key_violation_message(line) {
            return line.trim().to_string();
        }
    }
    message.trim().to_string()
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
    let spec = compile_create_table_metadata_spec(create, table_name.as_str())?;

    let outcome = create_table_metadata(
        &holostore_client,
        table_name.as_str(),
        create.if_not_exists,
        &spec,
    )
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

/// Executes CREATE INDEX in the SQL hook path.
async fn execute_create_index(
    create: &CreateIndex,
    session_context: &SessionContext,
    holostore_client: HoloStoreClient,
    pushdown_metrics: Arc<PushdownMetrics>,
) -> PgWireResult<Response> {
    if create.concurrently {
        return Err(to_user_error(
            "0A000",
            "CREATE INDEX CONCURRENTLY is not supported",
        ));
    }
    if create.predicate.is_some() {
        return Err(to_user_error(
            "0A000",
            "partial indexes (CREATE INDEX ... WHERE ...) are not supported",
        ));
    }
    if matches!(create.nulls_distinct, Some(false)) {
        return Err(to_user_error(
            "0A000",
            "CREATE INDEX NULLS NOT DISTINCT is not supported",
        ));
    }

    let table_name = parse_create_table_name(&create.table_name)?;
    let table = find_table_metadata_by_name(&holostore_client, table_name.as_str())
        .await
        .map_err(map_runtime_error)?
        .ok_or_else(|| {
            to_user_error("42P01", format!("relation '{}' does not exist", table_name))
        })?;
    table.validate().map_err(map_runtime_error)?;

    let spec = compile_create_index_metadata_spec(create, &table)?;
    let outcome = create_secondary_index_metadata(&holostore_client, &table, &spec)
        .await
        .map_err(map_runtime_error)?;

    register_table_from_metadata(
        session_context,
        holostore_client.clone(),
        pushdown_metrics.clone(),
        &table,
    )
    .await
    .map_err(|err| {
        api_error(format!(
            "register table '{}' after CREATE INDEX: {err}",
            table_name
        ))
    })?;

    if outcome.created {
        let provider = get_provider_for_table(session_context, table_name.as_str()).await?;
        let backfill_result = provider
            .backfill_secondary_index(&outcome.record)
            .await
            .map_err(map_runtime_error);
        if let Err(err) = backfill_result {
            let _ = drop_secondary_index_metadata_by_name(
                &holostore_client,
                outcome.record.index_name.as_str(),
                true,
            )
            .await;
            return Err(err);
        }

        update_secondary_index_state(
            &holostore_client,
            outcome.record.table_id,
            outcome.record.index_id,
            SecondaryIndexState::Public,
        )
        .await
        .map_err(map_runtime_error)?;

        let refreshed = find_table_metadata_by_name(&holostore_client, table_name.as_str())
            .await
            .map_err(map_runtime_error)?
            .ok_or_else(|| {
                api_error(format!(
                    "metadata for relation '{}' is not visible after CREATE INDEX",
                    table_name
                ))
            })?;
        register_table_from_metadata(
            session_context,
            holostore_client.clone(),
            pushdown_metrics,
            &refreshed,
        )
        .await
        .map_err(|err| {
            api_error(format!(
                "register table '{}' after index backfill state transition: {err}",
                table_name
            ))
        })?;
    }

    Ok(Response::Execution(Tag::new("CREATE INDEX")))
}

fn compile_create_index_metadata_spec(
    create: &CreateIndex,
    table: &crate::metadata::TableMetadataRecord,
) -> PgWireResult<CreateSecondaryIndexMetadataSpec> {
    if table.table_model != table_model_row_v1() {
        return Err(to_user_error(
            "0A000",
            "CREATE INDEX is currently supported only for row_v1 tables",
        ));
    }
    if create.columns.is_empty() {
        return Err(to_user_error(
            "42601",
            "CREATE INDEX requires at least one key column",
        ));
    }
    let mut key_columns = Vec::<String>::with_capacity(create.columns.len());
    let mut seen_keys = BTreeSet::<String>::new();
    for index_column in &create.columns {
        if matches!(index_column.column.options.asc, Some(false)) {
            return Err(to_user_error(
                "0A000",
                "CREATE INDEX with DESC key columns is not supported",
            ));
        }
        let Some(column_name) = index_column_name(index_column) else {
            return Err(to_user_error(
                "0A000",
                "CREATE INDEX key columns must reference plain column names",
            ));
        };
        if !seen_keys.insert(column_name.clone()) {
            return Err(to_user_error(
                "42601",
                format!(
                    "CREATE INDEX key column '{}' specified more than once",
                    column_name
                ),
            ));
        }
        key_columns.push(column_name);
    }

    let mut include_columns = Vec::<String>::with_capacity(create.include.len());
    let mut seen_include = BTreeSet::<String>::new();
    for ident in &create.include {
        let name = normalize_ident(ident);
        if !seen_include.insert(name.clone()) {
            return Err(to_user_error(
                "42601",
                format!(
                    "CREATE INDEX include column '{}' specified more than once",
                    name
                ),
            ));
        }
        include_columns.push(name);
    }

    let distribution = match create.using.as_ref() {
        Some(IndexType::Hash) => SecondaryIndexDistribution::Hash,
        Some(IndexType::BTree) | None => SecondaryIndexDistribution::Range,
        Some(other) => {
            return Err(to_user_error(
                "0A000",
                format!("CREATE INDEX USING {} is not supported", other),
            ))
        }
    };

    let hash_bucket_count =
        parse_secondary_index_hash_bucket_count_from_with(create.with.as_slice())?;
    if distribution == SecondaryIndexDistribution::Range && hash_bucket_count.is_some() {
        return Err(to_user_error(
            "0A000",
            "CREATE INDEX WITH hash bucket options requires USING HASH",
        ));
    }

    let index_name = if let Some(name) = &create.name {
        parse_index_name(name)?
    } else {
        default_index_name(table.table_name.as_str(), key_columns.as_slice())
    };

    let spec = CreateSecondaryIndexMetadataSpec {
        index_name,
        unique: create.unique,
        key_columns,
        include_columns,
        distribution,
        hash_bucket_count: if distribution == SecondaryIndexDistribution::Hash {
            Some(hash_bucket_count.unwrap_or(DEFAULT_SECONDARY_INDEX_HASH_BUCKETS))
        } else {
            None
        },
        if_not_exists: create.if_not_exists,
    };
    spec.validate_against_table(table)
        .map_err(map_runtime_error)?;
    Ok(spec)
}

fn parse_secondary_index_hash_bucket_count_from_with(
    with: &[SqlExpr],
) -> PgWireResult<Option<usize>> {
    let mut bucket_count = None::<usize>;
    for expr in with {
        let SqlExpr::BinaryOp { left, op, right } = expr else {
            return Err(to_user_error(
                "0A000",
                format!("CREATE INDEX WITH option '{}' is not supported", expr),
            ));
        };
        if *op != BinaryOperator::Eq {
            return Err(to_user_error(
                "0A000",
                format!("CREATE INDEX WITH option '{}' is not supported", expr),
            ));
        }
        let key = match left.as_ref() {
            SqlExpr::Identifier(ident) => normalize_ident(ident).to_ascii_lowercase(),
            _ => {
                return Err(to_user_error(
                    "0A000",
                    format!("CREATE INDEX WITH option '{}' is not supported", expr),
                ))
            }
        };
        if !matches!(
            key.as_str(),
            "hash_shards" | "hash_buckets" | "bucket_count" | "buckets"
        ) {
            return Err(to_user_error(
                "0A000",
                format!("CREATE INDEX WITH option '{}' is not supported", key),
            ));
        }
        if bucket_count.is_some() {
            return Err(to_user_error(
                "42601",
                format!(
                    "CREATE INDEX WITH option '{}' specified more than once",
                    key
                ),
            ));
        }
        let parsed = parse_positive_usize_sql_expr(right.as_ref())?;
        if parsed > MAX_SECONDARY_INDEX_HASH_BUCKETS {
            return Err(to_user_error(
                "22023",
                format!(
                    "hash bucket count {} exceeds supported max {}",
                    parsed, MAX_SECONDARY_INDEX_HASH_BUCKETS
                ),
            ));
        }
        bucket_count = Some(parsed);
    }
    Ok(bucket_count)
}

fn parse_index_name(name: &ObjectName) -> PgWireResult<String> {
    let parts = name
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(normalize_ident)
                .ok_or_else(|| to_user_error("42601", "invalid index name"))
        })
        .collect::<PgWireResult<Vec<_>>>()?;
    let index_name = match parts.as_slice() {
        [index] => index.clone(),
        [schema, index] if schema == "public" => index.clone(),
        [catalog, schema, index] if catalog == "datafusion" && schema == "public" => index.clone(),
        _ => return Err(to_user_error("42601", "invalid CREATE INDEX name format")),
    };
    if index_name.trim().is_empty() {
        return Err(to_user_error("42601", "index name cannot be empty"));
    }
    Ok(index_name)
}

fn default_index_name(table_name: &str, key_columns: &[String]) -> String {
    let mut out = String::new();
    out.push_str(table_name);
    for column in key_columns {
        out.push('_');
        out.push_str(column.as_str());
    }
    out.push_str("_idx");
    out
}

/// Executes DROP INDEX in the SQL hook path.
async fn execute_drop_index(
    if_exists: bool,
    names: &[ObjectName],
    cascade: bool,
    restrict: bool,
    purge: bool,
    temporary: bool,
    table: Option<&ObjectName>,
    session_context: &SessionContext,
    holostore_client: HoloStoreClient,
    pushdown_metrics: Arc<PushdownMetrics>,
) -> PgWireResult<Response> {
    if cascade || restrict || purge || temporary || table.is_some() {
        return Err(to_user_error(
            "0A000",
            "DROP INDEX options are not supported",
        ));
    }
    if names.is_empty() {
        return Err(to_user_error(
            "42601",
            "DROP INDEX requires at least one index name",
        ));
    }

    let mut dropped = 0u64;
    for name in names {
        let index_name = parse_index_name(name)?;
        let removed = drop_secondary_index_metadata_by_name(
            &holostore_client,
            index_name.as_str(),
            if_exists,
        )
        .await
        .map_err(map_runtime_error)?;
        if removed {
            dropped = dropped.saturating_add(1);
        }
    }

    sync_catalog_from_metadata(session_context, holostore_client, pushdown_metrics)
        .await
        .map_err(|err| api_error(format!("catalog sync after DROP INDEX failed: {err}")))?;

    Ok(Response::Execution(
        Tag::new("DROP INDEX").with_rows(dropped as usize),
    ))
}

fn configured_hash_pk_migration_enabled() -> bool {
    std::env::var("HOLO_FUSION_PHASE10_HASH_PK_MIGRATION_ENABLED")
        .ok()
        .and_then(|raw| raw.parse::<bool>().ok())
        .unwrap_or(true)
}

/// Parses ALTER TABLE properties for hash-primary-key migration.
fn parse_hash_pk_migration_from_alter(
    operations: &[AlterTableOperation],
) -> PgWireResult<Option<usize>> {
    if operations.len() != 1 {
        return Err(to_user_error(
            "0A000",
            "ALTER TABLE supports exactly one operation in current HoloFusion scope",
        ));
    }
    let AlterTableOperation::SetTblProperties { table_properties } = &operations[0] else {
        return Err(to_user_error(
            "0A000",
            "ALTER TABLE only supports SET TBLPROPERTIES for primary key migration",
        ));
    };
    if table_properties.is_empty() {
        return Err(to_user_error(
            "0A000",
            "ALTER TABLE SET TBLPROPERTIES requires at least one property",
        ));
    }

    let mut bucket_count = None::<usize>;
    let mut distribution = None::<PrimaryKeyDistribution>;
    for option in table_properties {
        let SqlOption::KeyValue { key, value } = option else {
            return Err(to_user_error(
                "0A000",
                format!("ALTER TABLE property '{}' is not supported", option),
            ));
        };
        let option_key = normalize_ident(key).to_ascii_lowercase();
        match option_key.as_str() {
            "hash_shards" | "hash_buckets" | "bucket_count" | "buckets" => {
                if bucket_count.is_some() {
                    return Err(to_user_error(
                        "42601",
                        format!(
                            "ALTER TABLE property '{}' specified more than once",
                            option_key
                        ),
                    ));
                }
                bucket_count = Some(parse_positive_usize_sql_expr(value)?);
            }
            "primary_key_distribution" | "pk_distribution" => {
                if distribution.is_some() {
                    return Err(to_user_error(
                        "42601",
                        format!(
                            "ALTER TABLE property '{}' specified more than once",
                            option_key
                        ),
                    ));
                }
                let parsed = match value {
                    SqlExpr::Value(value) => match &value.value {
                        Value::SingleQuotedString(raw) | Value::DoubleQuotedString(raw) => {
                            raw.to_ascii_lowercase()
                        }
                        other => {
                            return Err(to_user_error(
                                "22023",
                                format!(
                                    "invalid value '{}' for ALTER TABLE property '{}'",
                                    other, option_key
                                ),
                            ))
                        }
                    },
                    SqlExpr::Identifier(ident) => normalize_ident(ident).to_ascii_lowercase(),
                    _ => {
                        return Err(to_user_error(
                            "22023",
                            format!(
                                "invalid value '{}' for ALTER TABLE property '{}'",
                                value, option_key
                            ),
                        ))
                    }
                };
                distribution = Some(match parsed.as_str() {
                    "hash" => PrimaryKeyDistribution::Hash,
                    "range" => PrimaryKeyDistribution::Range,
                    other => {
                        return Err(to_user_error(
                            "22023",
                            format!(
                                "unsupported primary_key_distribution '{}' (expected 'hash' or 'range')",
                                other
                            ),
                        ))
                    }
                });
            }
            _ => {
                return Err(to_user_error(
                    "0A000",
                    format!("ALTER TABLE property '{}' is not supported", option_key),
                ));
            }
        }
    }

    if matches!(distribution, Some(PrimaryKeyDistribution::Range)) {
        return Err(to_user_error(
            "0A000",
            "ALTER TABLE migration to range primary key is not supported",
        ));
    }
    if distribution != Some(PrimaryKeyDistribution::Hash) && bucket_count.is_none() {
        return Err(to_user_error(
            "0A000",
            "ALTER TABLE must specify hash primary key migration properties",
        ));
    }
    Ok(bucket_count.or(Some(DEFAULT_HASH_PK_BUCKETS)))
}

/// Executes ALTER TABLE hash-primary-key migration with online backfill.
async fn execute_alter_table(
    name: &ObjectName,
    operations: &[AlterTableOperation],
    session_context: &SessionContext,
    holostore_client: HoloStoreClient,
    pushdown_metrics: Arc<PushdownMetrics>,
) -> PgWireResult<Response> {
    if !configured_hash_pk_migration_enabled() {
        return Err(to_user_error(
            "0A000",
            "ALTER TABLE hash primary key migration is disabled by rollout policy",
        ));
    }
    let table_name = parse_create_table_name(name)?;
    let target_buckets =
        parse_hash_pk_migration_from_alter(operations)?.unwrap_or(DEFAULT_HASH_PK_BUCKETS);
    if target_buckets == 0 || target_buckets > MAX_HASH_PK_BUCKETS {
        return Err(to_user_error(
            "22023",
            format!(
                "hash bucket count must be in range [1, {}], got {}",
                MAX_HASH_PK_BUCKETS, target_buckets
            ),
        ));
    }

    let table = find_table_metadata_by_name(&holostore_client, table_name.as_str())
        .await
        .map_err(map_runtime_error)?
        .ok_or_else(|| {
            to_user_error("42P01", format!("relation '{}' does not exist", table_name))
        })?;

    if table.primary_key_distribution == PrimaryKeyDistribution::Hash
        && table.primary_key_hash_buckets == Some(target_buckets)
    {
        return Ok(Response::Execution(Tag::new("ALTER TABLE").with_rows(0)));
    }

    let source_provider = HoloStoreTableProvider::from_table_metadata(
        &table,
        holostore_client.clone(),
        pushdown_metrics.clone(),
    )
    .map_err(map_runtime_error)?;
    let mut target_metadata = table.clone();
    target_metadata.primary_key_distribution = PrimaryKeyDistribution::Hash;
    target_metadata.primary_key_hash_buckets = Some(target_buckets);
    target_metadata.validate().map_err(map_runtime_error)?;
    let target_provider = HoloStoreTableProvider::from_table_metadata(
        &target_metadata,
        holostore_client.clone(),
        pushdown_metrics.clone(),
    )
    .map_err(map_runtime_error)?;

    let migrated_rows = if source_provider.is_row_v1_table() {
        let rows = source_provider
            .scan_generic_rows_with_versions_by_primary_key_bounds(None, None, None)
            .await
            .map_err(map_runtime_error)?;
        let values = rows
            .iter()
            .map(|row| row.values.clone())
            .collect::<Vec<_>>();
        let cleanup_keys = rows.iter().map(|row| row.primary_key).collect::<Vec<_>>();
        target_provider
            .upsert_generic_rows(values.as_slice())
            .await
            .map_err(map_runtime_error)?;
        update_table_primary_key_distribution(
            &holostore_client,
            table_name.as_str(),
            PrimaryKeyDistribution::Hash,
            Some(target_buckets),
        )
        .await
        .map_err(map_runtime_error)?;
        if !cleanup_keys.is_empty() {
            // Best-effort cleanup: stale range keys are unreachable once metadata switches to hash.
            let _ = source_provider
                .tombstone_generic_rows_by_primary_key(cleanup_keys.as_slice())
                .await;
        }
        values.len()
    } else {
        let rows = source_provider
            .scan_orders_with_versions_by_order_id_bounds(None, None, None)
            .await
            .map_err(map_runtime_error)?;
        let values = rows.iter().map(|row| row.row.clone()).collect::<Vec<_>>();
        let cleanup_keys = values.iter().map(|row| row.order_id).collect::<Vec<_>>();
        target_provider
            .upsert_orders_rows(values.as_slice())
            .await
            .map_err(map_runtime_error)?;
        update_table_primary_key_distribution(
            &holostore_client,
            table_name.as_str(),
            PrimaryKeyDistribution::Hash,
            Some(target_buckets),
        )
        .await
        .map_err(map_runtime_error)?;
        if !cleanup_keys.is_empty() {
            // Best-effort cleanup: stale range keys are unreachable once metadata switches to hash.
            let _ = source_provider
                .tombstone_orders_by_order_id(cleanup_keys.as_slice())
                .await;
        }
        values.len()
    };

    let updated = find_table_metadata_by_name(&holostore_client, table_name.as_str())
        .await
        .map_err(map_runtime_error)?
        .ok_or_else(|| {
            to_user_error(
                "XX000",
                format!(
                    "metadata for relation '{}' is not visible after hash primary key migration",
                    table_name
                ),
            )
        })?;
    register_table_from_metadata(
        session_context,
        holostore_client,
        pushdown_metrics,
        &updated,
    )
    .await
    .map_err(map_runtime_error)?;

    Ok(Response::Execution(
        Tag::new("ALTER TABLE").with_rows(migrated_rows),
    ))
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
    validate_supported_create_table_options(&create.table_options)?;
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

/// Default hash bucket count for `PRIMARY KEY ... USING HASH` when unspecified.
const DEFAULT_HASH_PK_BUCKETS: usize = 32;
/// Maximum supported hash bucket count for `USING HASH` layouts.
const MAX_HASH_PK_BUCKETS: usize = u16::MAX as usize;

/// Returns whether new hash-PK DDL adoption is enabled.
fn configured_hash_pk_ddl_enabled() -> bool {
    std::env::var("HOLO_FUSION_PHASE10_HASH_PK_DDL_ENABLED")
        .ok()
        .and_then(|raw| raw.parse::<bool>().ok())
        .unwrap_or(true)
}

/// Validates CREATE TABLE options and allows only hash-PK bucket controls.
fn validate_supported_create_table_options(table_options: &CreateTableOptions) -> PgWireResult<()> {
    let _ = parse_hash_bucket_count_from_create_table_options(table_options)?;
    Ok(())
}

/// Parses optional hash bucket count from supported CREATE TABLE options.
fn parse_hash_bucket_count_from_create_table_options(
    table_options: &CreateTableOptions,
) -> PgWireResult<Option<usize>> {
    let options = match table_options {
        CreateTableOptions::None => return Ok(None),
        CreateTableOptions::With(options)
        | CreateTableOptions::Options(options)
        | CreateTableOptions::Plain(options)
        | CreateTableOptions::TableProperties(options) => options,
    };
    if options.is_empty() {
        return Ok(None);
    }

    let mut bucket_count = None::<usize>;
    for option in options {
        let SqlOption::KeyValue { key, value } = option else {
            return Err(to_user_error(
                "0A000",
                format!("CREATE TABLE option '{}' is not supported", option),
            ));
        };
        let option_key = normalize_ident(key).to_ascii_lowercase();
        if !matches!(
            option_key.as_str(),
            "hash_shards" | "hash_buckets" | "bucket_count" | "buckets"
        ) {
            return Err(to_user_error(
                "0A000",
                format!("CREATE TABLE option '{}' is not supported", option_key),
            ));
        }
        if bucket_count.is_some() {
            return Err(to_user_error(
                "42601",
                format!(
                    "CREATE TABLE option '{}' specified more than once",
                    option_key
                ),
            ));
        }
        let parsed = parse_positive_usize_sql_expr(value)?;
        if parsed > MAX_HASH_PK_BUCKETS {
            return Err(to_user_error(
                "22023",
                format!(
                    "hash bucket count {} exceeds supported max {}",
                    parsed, MAX_HASH_PK_BUCKETS
                ),
            ));
        }
        bucket_count = Some(parsed);
    }

    Ok(bucket_count)
}

/// Parses a strictly positive integer value from CREATE TABLE option expression.
fn parse_positive_usize_sql_expr(expr: &SqlExpr) -> PgWireResult<usize> {
    let raw = match expr {
        SqlExpr::Value(value) => match &value.value {
            Value::Number(raw, _) => raw.as_str(),
            _ => {
                return Err(to_user_error(
                    "22023",
                    format!("expected positive integer option value, got '{}'", expr),
                ))
            }
        },
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => return parse_positive_usize_sql_expr(expr),
        _ => {
            return Err(to_user_error(
                "22023",
                format!("expected positive integer option value, got '{}'", expr),
            ))
        }
    };

    let parsed = raw.parse::<usize>().map_err(|_| {
        to_user_error(
            "22023",
            format!("invalid positive integer option value '{}'", raw),
        )
    })?;
    if parsed == 0 {
        return Err(to_user_error(
            "22023",
            "hash bucket count must be greater than zero",
        ));
    }
    Ok(parsed)
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

#[derive(Debug, Clone)]
struct PendingCheckConstraint {
    explicit_name: Option<String>,
    generated_name_seed: String,
    expr: CheckExpression,
}

#[derive(Debug)]
struct CompiledColumnOptions {
    nullable: bool,
    default_value: Option<ColumnDefaultValue>,
    pending_checks: Vec<PendingCheckConstraint>,
}

/// Compiles CREATE TABLE AST into persisted metadata specification.
fn compile_create_table_metadata_spec(
    create: &CreateTable,
    table_name: &str,
) -> PgWireResult<CreateTableMetadataSpec> {
    let mut pk_columns = BTreeSet::<String>::new();
    let mut primary_key_distribution = PrimaryKeyDistribution::Range;
    let mut primary_key_distribution_explicit = false;
    let mut primary_key_hash_buckets =
        parse_hash_bucket_count_from_create_table_options(&create.table_options)?;
    let mut by_name = BTreeMap::<String, usize>::new();
    let mut columns = Vec::<TableColumnRecord>::with_capacity(create.columns.len());
    let mut pending_checks = Vec::<PendingCheckConstraint>::new();

    for column in &create.columns {
        let column_name = normalize_ident(&column.name);
        // Decision: evaluate `by_name.contains_key(column_name.as_str())` to choose the correct SQL/storage control path.
        if by_name.contains_key(column_name.as_str()) {
            return Err(to_user_error(
                "42701",
                format!("column '{}' specified more than once", column_name),
            ));
        }

        let column_type = sql_data_type_to_column_type(&column.data_type)?;
        let options =
            compile_column_options(column, column_name.as_str(), table_name, &mut pk_columns)?;
        pending_checks.extend(options.pending_checks);
        let index = columns.len();
        columns.push(TableColumnRecord {
            name: column_name.clone(),
            column_type,
            nullable: options.nullable,
            default_value: options.default_value,
        });
        by_name.insert(column_name, index);
    }

    for constraint in &create.constraints {
        // Decision: evaluate `constraint` to choose the correct SQL/storage control path.
        match constraint {
            TableConstraint::PrimaryKey {
                columns,
                index_type,
                index_options,
                ..
            } => {
                let mut maybe_distribution = index_type
                    .as_ref()
                    .map(|index_type| match index_type {
                        IndexType::Hash => Ok(PrimaryKeyDistribution::Hash),
                        IndexType::BTree => Ok(PrimaryKeyDistribution::Range),
                        other => Err(to_user_error(
                            "0A000",
                            format!("PRIMARY KEY USING {} is not supported", other),
                        )),
                    })
                    .transpose()?;
                for option in index_options {
                    let IndexOption::Using(index_type) = option else {
                        continue;
                    };
                    let option_distribution = match index_type {
                        IndexType::Hash => PrimaryKeyDistribution::Hash,
                        IndexType::BTree => PrimaryKeyDistribution::Range,
                        other => {
                            return Err(to_user_error(
                                "0A000",
                                format!("PRIMARY KEY USING {} is not supported", other),
                            ))
                        }
                    };
                    if let Some(existing) = maybe_distribution {
                        if existing != option_distribution {
                            return Err(to_user_error(
                                "42601",
                                "conflicting PRIMARY KEY USING clauses are not supported",
                            ));
                        }
                    }
                    maybe_distribution = Some(option_distribution);
                }

                if let Some(next_distribution) = maybe_distribution {
                    if primary_key_distribution_explicit
                        && primary_key_distribution != next_distribution
                    {
                        return Err(to_user_error(
                            "42601",
                            "conflicting PRIMARY KEY USING clauses are not supported",
                        ));
                    }
                    primary_key_distribution = next_distribution;
                    primary_key_distribution_explicit = true;
                }
                for index_column in columns {
                    let column = index_column_name(index_column).ok_or_else(|| {
                        to_user_error("0A000", "PRIMARY KEY must reference plain column names")
                    })?;
                    pk_columns.insert(column);
                }
            }
            TableConstraint::Check {
                name,
                expr,
                enforced,
            } => {
                if matches!(enforced, Some(false)) {
                    return Err(to_user_error(
                        "0A000",
                        "CHECK constraints declared NOT ENFORCED are not supported",
                    ));
                }
                pending_checks.push(PendingCheckConstraint {
                    explicit_name: name.as_ref().map(normalize_ident),
                    generated_name_seed: generated_check_name_seed(table_name, None),
                    expr: compile_check_expression(expr.as_ref())?,
                });
            }
            _ => {
                return Err(to_user_error(
                    "0A000",
                    "table constraints other than PRIMARY KEY/CHECK are not supported",
                ));
            }
        }
    }
    // Decision: evaluate `let Some(primary_key_expr) = &create.primary_key` to choose the correct SQL/storage control path.
    if let Some(primary_key_expr) = &create.primary_key {
        let column =
            extract_column_name_normalized(primary_key_expr.as_ref()).ok_or_else(|| {
                to_user_error(
                    "0A000",
                    "PRIMARY KEY expression must reference a plain column name",
                )
            })?;
        pk_columns.insert(column);
    }

    // Decision: evaluate `pk_columns.is_empty()` to choose the correct SQL/storage control path.
    if pk_columns.is_empty() {
        return Err(to_user_error(
            "0A000",
            format!("CREATE TABLE '{table_name}' requires a PRIMARY KEY"),
        ));
    }
    // Decision: evaluate `pk_columns.len() != 1` to choose the correct SQL/storage control path.
    if pk_columns.len() != 1 {
        return Err(to_user_error(
            "0A000",
            format!("CREATE TABLE '{table_name}' supports only single-column PRIMARY KEY"),
        ));
    }

    let primary_key_column = pk_columns.into_iter().next().unwrap_or_default();
    let Some(pk_index) = by_name.get(primary_key_column.as_str()).copied() else {
        return Err(to_user_error(
            "42703",
            format!(
                "PRIMARY KEY column '{}' does not exist in CREATE TABLE '{}'",
                primary_key_column, table_name
            ),
        ));
    };

    if columns[pk_index].nullable {
        return Err(to_user_error(
            "23502",
            format!(
                "PRIMARY KEY column '{}' must be declared NOT NULL",
                primary_key_column
            ),
        ));
    }
    if !columns[pk_index].column_type.is_signed_integer() {
        return Err(to_user_error(
            "42804",
            format!(
                "PRIMARY KEY column '{}' must be BIGINT/INTEGER/SMALLINT/TINYINT",
                primary_key_column
            ),
        ));
    }

    match primary_key_distribution {
        PrimaryKeyDistribution::Range => {
            if primary_key_hash_buckets.is_some() {
                return Err(to_user_error(
                    "0A000",
                    "hash bucket options require PRIMARY KEY ... USING HASH",
                ));
            }
        }
        PrimaryKeyDistribution::Hash => {
            if !configured_hash_pk_ddl_enabled() {
                return Err(to_user_error(
                    "0A000",
                    "hash primary key DDL is disabled by rollout policy",
                ));
            }
            let buckets = primary_key_hash_buckets.unwrap_or(DEFAULT_HASH_PK_BUCKETS);
            if buckets == 0 || buckets > MAX_HASH_PK_BUCKETS {
                return Err(to_user_error(
                    "22023",
                    format!(
                        "hash bucket count must be in range [1, {}], got {}",
                        MAX_HASH_PK_BUCKETS, buckets
                    ),
                ));
            }
            primary_key_hash_buckets = Some(buckets);
        }
    }

    let check_constraints = finalize_check_constraints(pending_checks)?;
    validate_table_constraints(columns.as_slice(), check_constraints.as_slice())
        .map_err(row_constraint_error)?;

    let table_model = if check_constraints.is_empty()
        && columns.iter().all(|column| column.default_value.is_none())
        && is_orders_v1_layout(columns.as_slice(), primary_key_column.as_str())
    {
        table_model_orders_v1().to_string()
    } else {
        table_model_row_v1().to_string()
    };

    Ok(CreateTableMetadataSpec {
        table_model,
        columns,
        check_constraints,
        primary_key_column,
        primary_key_distribution,
        primary_key_hash_buckets,
        preferred_shards: Vec::new(),
        page_size: 2048,
    })
}

/// Compiles supported column options into metadata fields.
fn compile_column_options(
    column: &ColumnDef,
    column_name: &str,
    table_name: &str,
    pk_columns: &mut BTreeSet<String>,
) -> PgWireResult<CompiledColumnOptions> {
    let mut nullable = true;
    let mut default_value = None::<ColumnDefaultValue>;
    let mut pending_checks = Vec::<PendingCheckConstraint>::new();

    for option in &column.options {
        // Decision: evaluate `&option.option` to choose the correct SQL/storage control path.
        match &option.option {
            ColumnOption::Null => nullable = true,
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Unique { is_primary, .. } if *is_primary => {
                pk_columns.insert(column_name.to_string());
                nullable = false;
            }
            ColumnOption::Comment(_) => {}
            ColumnOption::Default(expr) => {
                if default_value.is_some() {
                    return Err(to_user_error(
                        "42601",
                        format!("column '{}' specifies DEFAULT more than once", column_name),
                    ));
                }
                default_value = Some(compile_default_expression(expr, column_name)?);
            }
            ColumnOption::Check(expr) => {
                pending_checks.push(PendingCheckConstraint {
                    explicit_name: option.name.as_ref().map(normalize_ident),
                    generated_name_seed: generated_check_name_seed(table_name, Some(column_name)),
                    expr: compile_check_expression(expr)?,
                });
            }
            ColumnOption::Unique { .. } => {
                return Err(to_user_error(
                    "0A000",
                    "UNIQUE constraints are not supported; only PRIMARY KEY is allowed",
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

    Ok(CompiledColumnOptions {
        nullable,
        default_value,
        pending_checks,
    })
}

/// Converts row-constraint metadata errors into SQLSTATE user errors.
fn row_constraint_error(err: crate::metadata::RowConstraintError) -> PgWireError {
    to_user_error(err.sqlstate(), err.message())
}

/// Builds a deterministic generated CHECK constraint name seed.
fn generated_check_name_seed(table_name: &str, column_name: Option<&str>) -> String {
    let table = sanitize_identifier_fragment(table_name);
    match column_name {
        Some(column) => format!("{}_{}_check", table, sanitize_identifier_fragment(column)),
        None => format!("{}_check", table),
    }
}

/// Sanitizes identifier fragments used in generated metadata names.
fn sanitize_identifier_fragment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "check".to_string()
    } else {
        out
    }
}

/// Normalizes a simple/compound SQL expression into a column name.
fn extract_column_name_normalized(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(ident) => Some(normalize_ident(ident)),
        SqlExpr::CompoundIdentifier(idents) => idents.last().map(normalize_ident),
        SqlExpr::Nested(inner) => extract_column_name_normalized(inner),
        SqlExpr::Cast { expr, .. } => extract_column_name_normalized(expr),
        _ => None,
    }
}

/// Assigns final constraint names and checks for duplicate explicit names.
fn finalize_check_constraints(
    pending: Vec<PendingCheckConstraint>,
) -> PgWireResult<Vec<TableCheckConstraintRecord>> {
    let mut used_names = BTreeSet::<String>::new();
    let mut generated_counter = 1usize;
    let mut output = Vec::with_capacity(pending.len());

    for entry in pending {
        let name = match entry.explicit_name {
            Some(name) => {
                let trimmed = name.trim();
                if trimmed.is_empty() {
                    return Err(to_user_error(
                        "42601",
                        "CHECK constraint name cannot be empty",
                    ));
                }
                let normalized = trimmed.to_ascii_lowercase();
                if !used_names.insert(normalized.clone()) {
                    return Err(to_user_error(
                        "42710",
                        format!("duplicate CHECK constraint name '{}'", trimmed),
                    ));
                }
                trimmed.to_string()
            }
            None => {
                let base = if entry.generated_name_seed.trim().is_empty() {
                    "check".to_string()
                } else {
                    entry.generated_name_seed
                };
                let mut candidate = base.clone();
                let mut normalized = candidate.to_ascii_lowercase();
                while used_names.contains(normalized.as_str()) {
                    candidate = format!("{}_{}", base, generated_counter);
                    generated_counter = generated_counter.saturating_add(1);
                    normalized = candidate.to_ascii_lowercase();
                }
                used_names.insert(normalized);
                candidate
            }
        };
        output.push(TableCheckConstraintRecord {
            name,
            expr: entry.expr,
        });
    }

    Ok(output)
}

/// Compiles one column DEFAULT expression into persisted metadata form.
fn compile_default_expression(
    expr: &SqlExpr,
    column_name: &str,
) -> PgWireResult<ColumnDefaultValue> {
    compile_default_literal(expr).map_err(|err| {
        to_user_error(
            "22023",
            format!(
                "invalid DEFAULT expression for column '{}': {}",
                column_name, err
            ),
        )
    })
}

/// Compiles one literal/default SQL expression.
fn compile_default_literal(expr: &SqlExpr) -> PgWireResult<ColumnDefaultValue> {
    match expr {
        SqlExpr::Nested(inner) => compile_default_literal(inner),
        SqlExpr::Cast { expr, .. } => compile_default_literal(expr),
        SqlExpr::Value(value) => default_value_from_sql_value(value),
        SqlExpr::TypedString { data_type, value } => {
            if !is_timestamp_data_type(data_type) {
                return Err(to_user_error(
                    "0A000",
                    format!("typed literal '{}' is not supported in DEFAULT", data_type),
                ));
            }
            let Some(raw) = sql_string_literal(value) else {
                return Err(to_user_error(
                    "22023",
                    "timestamp DEFAULT literal must be a quoted string",
                ));
            };
            Ok(ColumnDefaultValue::TimestampNanosecond(
                parse_timestamp_ns_from_string(raw)?,
            ))
        }
        SqlExpr::UnaryOp { op, expr } => {
            let base = compile_default_literal(expr)?;
            apply_unary_to_default(op, base)
        }
        SqlExpr::Function(function) if is_current_timestamp_function(function) => {
            Ok(ColumnDefaultValue::CurrentTimestampNanosecond)
        }
        SqlExpr::Identifier(ident) if ident.value.eq_ignore_ascii_case("current_timestamp") => {
            Ok(ColumnDefaultValue::CurrentTimestampNanosecond)
        }
        _ => Err(to_user_error(
            "0A000",
            format!("DEFAULT expression '{}' is not supported", expr),
        )),
    }
}

/// Parses one SQL literal into the metadata default literal type.
fn default_value_from_sql_value(value: &ValueWithSpan) -> PgWireResult<ColumnDefaultValue> {
    match &value.value {
        Value::Null => Ok(ColumnDefaultValue::Null),
        Value::Boolean(v) => Ok(ColumnDefaultValue::Boolean(*v)),
        Value::Number(raw, _) => {
            if let Ok(v) = raw.parse::<i64>() {
                Ok(ColumnDefaultValue::Int64(v))
            } else if let Ok(v) = raw.parse::<u64>() {
                Ok(ColumnDefaultValue::UInt64(v))
            } else if let Ok(v) = raw.parse::<f64>() {
                Ok(ColumnDefaultValue::Float64(v))
            } else {
                Err(to_user_error(
                    "22023",
                    format!("invalid numeric literal '{raw}'"),
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
        | Value::HexStringLiteral(s) => Ok(ColumnDefaultValue::Utf8(s.clone())),
        _ => Err(to_user_error(
            "0A000",
            format!("literal '{}' is not supported in DEFAULT", value),
        )),
    }
}

/// Applies unary +/- operators to a parsed default literal.
fn apply_unary_to_default(
    op: &UnaryOperator,
    value: ColumnDefaultValue,
) -> PgWireResult<ColumnDefaultValue> {
    match op {
        UnaryOperator::Plus => match value {
            ColumnDefaultValue::Int64(_)
            | ColumnDefaultValue::UInt64(_)
            | ColumnDefaultValue::Float64(_) => Ok(value),
            _ => Err(to_user_error(
                "0A000",
                "unary plus is only supported for numeric DEFAULT literals",
            )),
        },
        UnaryOperator::Minus => match value {
            ColumnDefaultValue::Int64(v) => v
                .checked_neg()
                .map(ColumnDefaultValue::Int64)
                .ok_or_else(|| to_user_error("22003", "numeric DEFAULT literal out of range")),
            ColumnDefaultValue::UInt64(v) => {
                let signed = i64::try_from(v)
                    .map_err(|_| to_user_error("22003", "numeric DEFAULT literal out of range"))?;
                signed
                    .checked_neg()
                    .map(ColumnDefaultValue::Int64)
                    .ok_or_else(|| to_user_error("22003", "numeric DEFAULT literal out of range"))
            }
            ColumnDefaultValue::Float64(v) => Ok(ColumnDefaultValue::Float64(-v)),
            _ => Err(to_user_error(
                "0A000",
                "unary minus is only supported for numeric DEFAULT literals",
            )),
        },
        _ => Err(to_user_error(
            "0A000",
            "only unary +/- operators are supported in DEFAULT expressions",
        )),
    }
}

/// Returns `true` when SQL function expression represents CURRENT_TIMESTAMP.
fn is_current_timestamp_function(function: &datafusion::sql::sqlparser::ast::Function) -> bool {
    let Some(name) = function
        .name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(normalize_ident)
        .map(|name| name.to_ascii_lowercase())
    else {
        return false;
    };
    if name != "current_timestamp" {
        return false;
    }

    fn args_empty(args: &datafusion::sql::sqlparser::ast::FunctionArguments) -> bool {
        match args {
            datafusion::sql::sqlparser::ast::FunctionArguments::None => true,
            datafusion::sql::sqlparser::ast::FunctionArguments::List(list) => {
                list.args.is_empty()
                    && list.clauses.is_empty()
                    && list.duplicate_treatment.is_none()
            }
            datafusion::sql::sqlparser::ast::FunctionArguments::Subquery(_) => false,
        }
    }

    args_empty(&function.parameters)
        && args_empty(&function.args)
        && function.filter.is_none()
        && function.null_treatment.is_none()
        && function.over.is_none()
        && function.within_group.is_empty()
}

/// Returns the literal string content when value is a quoted SQL string.
fn sql_string_literal(value: &ValueWithSpan) -> Option<&str> {
    match &value.value {
        Value::SingleQuotedString(v)
        | Value::DoubleQuotedString(v)
        | Value::TripleSingleQuotedString(v)
        | Value::TripleDoubleQuotedString(v)
        | Value::EscapedStringLiteral(v)
        | Value::UnicodeStringLiteral(v)
        | Value::NationalStringLiteral(v)
        | Value::HexStringLiteral(v) => Some(v.as_str()),
        _ => None,
    }
}

/// Compiles SQL CHECK expression AST into the persisted metadata AST.
fn compile_check_expression(expr: &SqlExpr) -> PgWireResult<CheckExpression> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(CheckExpression::Column(normalize_ident(ident))),
        SqlExpr::CompoundIdentifier(idents) => {
            let Some(last) = idents.last() else {
                return Err(to_user_error("42601", "invalid CHECK column reference"));
            };
            Ok(CheckExpression::Column(normalize_ident(last)))
        }
        SqlExpr::Nested(inner) | SqlExpr::Cast { expr: inner, .. } => {
            compile_check_expression(inner)
        }
        SqlExpr::UnaryOp {
            op: UnaryOperator::Not,
            expr: inner,
        } => Ok(CheckExpression::Not(Box::new(compile_check_expression(
            inner,
        )?))),
        SqlExpr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            ..
        }
        | SqlExpr::Value(_)
        | SqlExpr::TypedString { .. }
        | SqlExpr::Function(_) => {
            let literal = compile_default_literal(expr)?;
            if matches!(literal, ColumnDefaultValue::CurrentTimestampNanosecond) {
                return Err(to_user_error(
                    "0A000",
                    "CHECK expressions do not support CURRENT_TIMESTAMP",
                ));
            }
            Ok(CheckExpression::Literal(literal))
        }
        SqlExpr::IsNull(inner) => Ok(CheckExpression::IsNull(Box::new(compile_check_expression(
            inner,
        )?))),
        SqlExpr::IsNotNull(inner) => Ok(CheckExpression::IsNotNull(Box::new(
            compile_check_expression(inner)?,
        ))),
        SqlExpr::BinaryOp { left, op, right } => {
            let op = match op {
                BinaryOperator::And => CheckBinaryOperator::And,
                BinaryOperator::Or => CheckBinaryOperator::Or,
                BinaryOperator::Eq => CheckBinaryOperator::Eq,
                BinaryOperator::NotEq => CheckBinaryOperator::NotEq,
                BinaryOperator::Lt => CheckBinaryOperator::Lt,
                BinaryOperator::LtEq => CheckBinaryOperator::LtEq,
                BinaryOperator::Gt => CheckBinaryOperator::Gt,
                BinaryOperator::GtEq => CheckBinaryOperator::GtEq,
                _ => {
                    return Err(to_user_error(
                        "0A000",
                        format!("CHECK expressions do not support operator '{}'", op),
                    ))
                }
            };
            Ok(CheckExpression::Binary {
                op,
                left: Box::new(compile_check_expression(left)?),
                right: Box::new(compile_check_expression(right)?),
            })
        }
        _ => Err(to_user_error(
            "0A000",
            format!("CHECK expression '{}' is not supported", expr),
        )),
    }
}

/// Maps SQL parser type to persisted metadata column type.
fn sql_data_type_to_column_type(data_type: &DataType) -> PgWireResult<TableColumnType> {
    let mapped = match data_type {
        DataType::BigInt(_)
        | DataType::Int8(_)
        | DataType::Int64
        | DataType::Integer(_)
        | DataType::Int(_)
        | DataType::Int4(_)
        | DataType::MediumInt(_)
        | DataType::Signed
        | DataType::SignedInteger => Some(TableColumnType::Int64),
        DataType::Int32 => Some(TableColumnType::Int32),
        DataType::SmallInt(_) | DataType::Int2(_) | DataType::Int16 => Some(TableColumnType::Int16),
        DataType::TinyInt(_) => Some(TableColumnType::Int8),
        DataType::BigIntUnsigned(_)
        | DataType::UBigInt
        | DataType::Int8Unsigned(_)
        | DataType::IntUnsigned(_)
        | DataType::Int4Unsigned(_)
        | DataType::IntegerUnsigned(_)
        | DataType::MediumIntUnsigned(_)
        | DataType::Unsigned
        | DataType::UnsignedInteger
        | DataType::UInt64 => Some(TableColumnType::UInt64),
        DataType::SmallIntUnsigned(_)
        | DataType::Int2Unsigned(_)
        | DataType::USmallInt
        | DataType::UInt16 => Some(TableColumnType::UInt16),
        DataType::TinyIntUnsigned(_) | DataType::UTinyInt | DataType::UInt8 => {
            Some(TableColumnType::UInt8)
        }
        DataType::UInt32 => Some(TableColumnType::UInt32),
        DataType::Text
        | DataType::Varchar(_)
        | DataType::CharVarying(_)
        | DataType::CharacterVarying(_)
        | DataType::Character(_)
        | DataType::Char(_) => Some(TableColumnType::Utf8),
        DataType::Boolean | DataType::Bool => Some(TableColumnType::Boolean),
        DataType::Float(_)
        | DataType::Float4
        | DataType::Float8
        | DataType::Float64
        | DataType::Float32
        | DataType::Real
        | DataType::Double(_)
        | DataType::DoublePrecision => Some(TableColumnType::Float64),
        DataType::Timestamp(_, tz) => {
            if matches!(tz, TimezoneInfo::None | TimezoneInfo::WithoutTimeZone) {
                Some(TableColumnType::TimestampNanosecond)
            } else {
                None
            }
        }
        DataType::TimestampNtz | DataType::Datetime(_) | DataType::Datetime64(_, _) => {
            Some(TableColumnType::TimestampNanosecond)
        }
        _ => None,
    };
    mapped.ok_or_else(|| {
        to_user_error(
            "0A000",
            format!("column data type '{}' is not supported", data_type),
        )
    })
}

/// Returns `true` when compiled column layout matches the legacy orders model.
fn is_orders_v1_layout(columns: &[TableColumnRecord], primary_key_column: &str) -> bool {
    if primary_key_column != "order_id" {
        return false;
    }
    if columns.len() != 5 {
        return false;
    }
    matches!(
        columns,
        [
            TableColumnRecord {
                name,
                column_type: TableColumnType::Int64,
                nullable: false,
                ..
            },
            TableColumnRecord {
                name: customer_name,
                column_type: TableColumnType::Int64,
                nullable: false,
                ..
            },
            TableColumnRecord {
                name: status_name,
                column_type: TableColumnType::Utf8,
                nullable: true,
                ..
            },
            TableColumnRecord {
                name: total_name,
                column_type: TableColumnType::Int64,
                nullable: false,
                ..
            },
            TableColumnRecord {
                name: created_name,
                column_type: TableColumnType::TimestampNanosecond,
                nullable: false,
                ..
            },
        ] if name == "order_id"
            && customer_name == "customer_id"
            && status_name == "status"
            && total_name == "total_cents"
            && created_name == "created_at"
    )
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
    pushdown_metrics: &PushdownMetrics,
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
    let provider = get_provider_for_table(session_context, table_name.as_str()).await?;
    if provider.is_row_v1_table() {
        return execute_update_row_v1(
            assignments,
            selection,
            params,
            config,
            pushdown_metrics,
            &provider,
        )
        .await;
    }

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
    let scan_limit = config.max_scan_rows.max(1);

    let rows = provider
        .scan_orders_with_versions_by_order_id_bounds(
            bounds.lower,
            bounds.upper,
            Some(scan_limit.saturating_add(1)),
        )
        .await
        .map_err(|err| api_error(err.to_string()))?;
    enforce_scan_row_limit(
        rows.len(),
        scan_limit,
        pushdown_metrics,
        "UPDATE matching row count",
    )?;
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
    pushdown_metrics: &PushdownMetrics,
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
    let provider = get_provider_for_table(session_context, table_name.as_str()).await?;
    if provider.is_row_v1_table() {
        return execute_delete_row_v1(
            delete.selection.as_ref(),
            params,
            config,
            pushdown_metrics,
            &provider,
        )
        .await;
    }

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

    let scan_limit = config.max_scan_rows.max(1);
    let rows = provider
        .scan_orders_with_versions_by_order_id_bounds(
            bounds.lower,
            bounds.upper,
            Some(scan_limit.saturating_add(1)),
        )
        .await
        .map_err(|err| api_error(err.to_string()))?;
    enforce_scan_row_limit(
        rows.len(),
        scan_limit,
        pushdown_metrics,
        "DELETE matching row count",
    )?;
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

/// Executes `execute update row v1` for this component.
async fn execute_update_row_v1(
    assignments: &[Assignment],
    selection: Option<&SqlExpr>,
    params: &ParamValues,
    config: DmlRuntimeConfig,
    pushdown_metrics: &PushdownMetrics,
    provider: &HoloStoreTableProvider,
) -> PgWireResult<Response> {
    let pk_column = provider.primary_key_column().to_string();
    let selection = selection.ok_or_else(|| {
        to_user_error(
            "0A000",
            format!("UPDATE requires a PK-bounded WHERE clause on {pk_column}"),
        )
    })?;
    let bounds = parse_pk_bounds_for_column(selection, params, pk_column.as_str())?;
    if bounds.is_unbounded() {
        return Err(to_user_error(
            "0A000",
            format!("UPDATE requires a PK-bounded WHERE clause on {pk_column}"),
        ));
    }

    let patch =
        parse_row_v1_update_patch(assignments, params, provider.columns(), pk_column.as_str())?;
    let scan_limit = config.max_scan_rows.max(1);
    let rows = provider
        .scan_generic_rows_with_versions_by_primary_key_bounds(
            bounds.lower,
            bounds.upper,
            Some(scan_limit.saturating_add(1)),
        )
        .await
        .map_err(|err| api_error(err.to_string()))?;
    enforce_scan_row_limit(
        rows.len(),
        scan_limit,
        pushdown_metrics,
        "UPDATE matching row count",
    )?;
    if rows.is_empty() {
        return Ok(Response::Execution(Tag::new("UPDATE").with_rows(0)));
    }

    let mut writes = Vec::with_capacity(rows.len());
    for row in &rows {
        let updated = patch.apply(row.values.as_slice());
        if updated != row.values {
            validate_row_against_metadata(
                provider.columns(),
                provider.check_constraints(),
                updated.as_slice(),
            )
            .map_err(row_constraint_error)?;
            let value = provider
                .encode_row_payload(updated.as_slice())
                .map_err(|err| api_error(err.to_string()))?;
            let rollback_value = provider
                .encode_row_payload(row.values.as_slice())
                .map_err(|err| api_error(err.to_string()))?;
            writes.push(ConditionalPrimaryWrite {
                primary_key: row.primary_key,
                expected_version: row.version,
                value,
                rollback_value,
            });
        }
    }

    if !writes.is_empty() {
        maybe_prewrite_delay(config).await;
        match provider
            .apply_generic_writes_conditional(&writes, "update")
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

/// Executes `execute delete row v1` for this component.
async fn execute_delete_row_v1(
    selection: Option<&SqlExpr>,
    params: &ParamValues,
    config: DmlRuntimeConfig,
    pushdown_metrics: &PushdownMetrics,
    provider: &HoloStoreTableProvider,
) -> PgWireResult<Response> {
    let pk_column = provider.primary_key_column().to_string();
    let selection = selection.ok_or_else(|| {
        to_user_error(
            "0A000",
            format!("DELETE requires a PK-bounded WHERE clause on {pk_column}"),
        )
    })?;
    let bounds = parse_pk_bounds_for_column(selection, params, pk_column.as_str())?;
    if bounds.is_unbounded() {
        return Err(to_user_error(
            "0A000",
            format!("DELETE requires a PK-bounded WHERE clause on {pk_column}"),
        ));
    }

    let scan_limit = config.max_scan_rows.max(1);
    let rows = provider
        .scan_generic_rows_with_versions_by_primary_key_bounds(
            bounds.lower,
            bounds.upper,
            Some(scan_limit.saturating_add(1)),
        )
        .await
        .map_err(|err| api_error(err.to_string()))?;
    enforce_scan_row_limit(
        rows.len(),
        scan_limit,
        pushdown_metrics,
        "DELETE matching row count",
    )?;
    if rows.is_empty() {
        return Ok(Response::Execution(Tag::new("DELETE").with_rows(0)));
    }

    let tombstone = provider.encode_tombstone_payload();
    let mut writes = Vec::with_capacity(rows.len());
    for row in &rows {
        let rollback_value = provider
            .encode_row_payload(row.values.as_slice())
            .map_err(|err| api_error(err.to_string()))?;
        writes.push(ConditionalPrimaryWrite {
            primary_key: row.primary_key,
            expected_version: row.version,
            value: tombstone.clone(),
            rollback_value,
        });
    }

    maybe_prewrite_delay(config).await;
    match provider
        .apply_generic_writes_conditional(&writes, "delete")
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
    config: DmlRuntimeConfig,
    pushdown_metrics: &PushdownMetrics,
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
    let provider = get_provider_for_table(session_context, table_name.as_str()).await?;
    if provider.is_row_v1_table() {
        return Err(to_user_error(
            "0A000",
            "UPDATE on row_v1 tables inside explicit transactions is not supported yet",
        ));
    }
    ensure_txn_snapshot_for_table(
        tx_manager,
        session_id,
        session_context,
        table_name.as_str(),
        config,
        pushdown_metrics,
    )
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
                if let Err(err) = txn.stage_row_for_order(
                    updated.order_id,
                    Some(updated),
                    config.max_txn_staged_rows.max(1),
                ) {
                    pushdown_metrics.record_txn_stage_limit_reject();
                    return Err(err);
                }
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
    config: DmlRuntimeConfig,
    pushdown_metrics: &PushdownMetrics,
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
    let provider = get_provider_for_table(session_context, table_name.as_str()).await?;
    if provider.is_row_v1_table() {
        return Err(to_user_error(
            "0A000",
            "DELETE on row_v1 tables inside explicit transactions is not supported yet",
        ));
    }
    ensure_txn_snapshot_for_table(
        tx_manager,
        session_id,
        session_context,
        table_name.as_str(),
        config,
        pushdown_metrics,
    )
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
            if let Err(err) =
                txn.stage_row_for_order(row.order_id, None, config.max_txn_staged_rows.max(1))
            {
                pushdown_metrics.record_txn_stage_limit_reject();
                return Err(err);
            }
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
    config: DmlRuntimeConfig,
    pushdown_metrics: &PushdownMetrics,
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
    ensure_txn_snapshot_for_table(
        tx_manager,
        session_id,
        session_context,
        table_name.as_str(),
        config,
        pushdown_metrics,
    )
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
            if let Err(err) = txn.stage_row_for_order(
                row.order_id,
                Some(row.clone()),
                config.max_txn_staged_rows.max(1),
            ) {
                pushdown_metrics.record_txn_stage_limit_reject();
                return Err(err);
            }
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
    let wire_format = hook_query_wire_format(protocol);
    let query = df::encode_dataframe(dataframe, &wire_format, Some(format_options))
        .await
        .map_err(|err| api_error(err.to_string()))?;
    Ok(Response::Query(query))
}

/// Executes one fast `MIN/MAX(primary_key)` query directly on provider scan fast path.
async fn execute_fast_primary_key_aggregate_query(
    provider: HoloStoreTableProvider,
    spec: FastPrimaryKeyAggregateSpec,
    client: &(dyn ClientInfo + Send + Sync),
    protocol: QueryProtocol,
) -> PgWireResult<Response> {
    let value = evaluate_fast_primary_key_aggregate(provider, &spec).await?;
    let column_name = fast_primary_key_aggregate_output_name(&spec);

    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name.as_str(),
        ArrowDataType::Int64,
        true,
    )]));
    let mut values = Int64Builder::new();
    if let Some(value) = value {
        values.append_value(value);
    } else {
        values.append_null();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values.finish()) as ArrayRef])
        .map_err(|err| api_error(err.to_string()))?;
    let mem =
        MemTable::try_new(schema, vec![vec![batch]]).map_err(|err| api_error(err.to_string()))?;

    let query_ctx = SessionContext::new();
    query_ctx
        .register_table("holo_fast_pk_aggregate", Arc::new(mem))
        .map_err(|err| api_error(err.to_string()))?;
    let dataframe = query_ctx
        .sql("SELECT * FROM holo_fast_pk_aggregate")
        .await
        .map_err(|err| api_error(err.to_string()))?;

    let format_options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
    let wire_format = hook_query_wire_format(protocol);
    let query = df::encode_dataframe(dataframe, &wire_format, Some(format_options))
        .await
        .map_err(|err| api_error(err.to_string()))?;
    Ok(Response::Query(query))
}

/// Executes grouped aggregate + top-k query using provider-native pushdown.
async fn execute_fast_grouped_aggregate_topk_query(
    provider: HoloStoreTableProvider,
    spec: FastGroupedAggregateTopKSpec,
    pushdown_metrics: &PushdownMetrics,
    query_execution_id: &str,
    client: &(dyn ClientInfo + Send + Sync),
    protocol: QueryProtocol,
) -> PgWireResult<Response> {
    let stage_id = pushdown_metrics.next_stage_execution_id();
    pushdown_metrics.record_stage_event(
        query_execution_id,
        stage_id,
        "aggregate_pushdown_entry",
        format!(
            "table={} groups={} filters={} having_min_count={} limit={}",
            spec.table_name,
            spec.group_columns.len(),
            spec.filters.len(),
            spec.having_min_count,
            spec.limit
        ),
    );

    let provider_spec = GroupedAggregateTopKSpec {
        group_columns: spec.group_columns.clone(),
        sum_column: spec.sum_column.clone(),
        filters: spec
            .filters
            .iter()
            .map(|filter| GroupedAggregateFilter {
                column: filter.column.clone(),
                allowed_values: filter.allowed_values.clone(),
            })
            .collect::<Vec<_>>(),
        having_min_count: spec.having_min_count,
        limit: spec.limit,
    };
    let rows = provider
        .execute_grouped_aggregate_topk(query_execution_id, stage_id, &provider_spec)
        .await
        .map_err(|err| api_error(err.to_string()))?;

    let batch = grouped_aggregate_rows_to_batch(&provider, &spec, rows.as_slice())?;
    let mem = MemTable::try_new(batch.schema(), vec![vec![batch]])
        .map_err(|err| api_error(err.to_string()))?;

    let query_ctx = SessionContext::new();
    query_ctx
        .register_table("holo_fast_grouped_aggregate", Arc::new(mem))
        .map_err(|err| api_error(err.to_string()))?;
    let dataframe = query_ctx
        .sql("SELECT * FROM holo_fast_grouped_aggregate")
        .await
        .map_err(|err| api_error(err.to_string()))?;

    let format_options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
    let wire_format = hook_query_wire_format(protocol);
    let query = df::encode_dataframe(dataframe, &wire_format, Some(format_options))
        .await
        .map_err(|err| api_error(err.to_string()))?;
    Ok(Response::Query(query))
}

fn grouped_aggregate_rows_to_batch(
    provider: &HoloStoreTableProvider,
    spec: &FastGroupedAggregateTopKSpec,
    rows: &[GroupedAggregateTopKRow],
) -> PgWireResult<RecordBatch> {
    let mut group_position_by_name = BTreeMap::<String, usize>::new();
    for (idx, column) in spec.group_columns.iter().enumerate() {
        group_position_by_name.insert(column.to_ascii_lowercase(), idx);
    }

    let output_layout = spec
        .output_columns
        .iter()
        .map(|column| match column {
            FastGroupedOutputColumn::GroupColumn {
                column,
                output_name,
            } => {
                let position = group_position_by_name
                    .get(column.to_ascii_lowercase().as_str())
                    .copied()
                    .ok_or_else(|| {
                        api_error(format!(
                            "grouped aggregate output references missing group column '{}'",
                            column
                        ))
                    })?;
                Ok(CompiledGroupedOutput::Group {
                    group_position: position,
                    output_name: output_name.clone(),
                    data_type: output_group_arrow_type(provider, column.as_str())?,
                })
            }
            FastGroupedOutputColumn::Count { output_name } => Ok(CompiledGroupedOutput::Count {
                output_name: output_name.clone(),
            }),
            FastGroupedOutputColumn::Sum { output_name } => Ok(CompiledGroupedOutput::Sum {
                output_name: output_name.clone(),
            }),
        })
        .collect::<PgWireResult<Vec<_>>>()?;

    if rows.is_empty() {
        let mut fields = Vec::<Field>::with_capacity(output_layout.len());
        for output in &output_layout {
            match output {
                CompiledGroupedOutput::Group {
                    output_name,
                    data_type,
                    ..
                } => fields.push(Field::new(output_name.as_str(), data_type.clone(), true)),
                CompiledGroupedOutput::Count { output_name }
                | CompiledGroupedOutput::Sum { output_name } => {
                    fields.push(Field::new(
                        output_name.as_str(),
                        ArrowDataType::Int64,
                        false,
                    ));
                }
            }
        }
        let schema = Arc::new(Schema::new(fields));
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut column_values =
        vec![Vec::<ScalarValue>::with_capacity(rows.len()); output_layout.len()];
    for row in rows {
        for (idx, output) in output_layout.iter().enumerate() {
            match output {
                CompiledGroupedOutput::Group { group_position, .. } => {
                    let value = row
                        .group_values
                        .get(*group_position)
                        .cloned()
                        .unwrap_or(ScalarValue::Null);
                    column_values[idx].push(value);
                }
                CompiledGroupedOutput::Count { .. } => {
                    let count = i64::try_from(row.count).map_err(|_| {
                        api_error(format!("aggregate COUNT overflow for value {}", row.count))
                    })?;
                    column_values[idx].push(ScalarValue::Int64(Some(count)));
                }
                CompiledGroupedOutput::Sum { .. } => {
                    column_values[idx].push(ScalarValue::Int64(Some(row.sum)));
                }
            }
        }
    }

    let mut arrays = Vec::<ArrayRef>::with_capacity(column_values.len());
    let mut fields = Vec::<Field>::with_capacity(output_layout.len());
    for (idx, values) in column_values.into_iter().enumerate() {
        let array = ScalarValue::iter_to_array(values.into_iter())
            .map_err(|err| api_error(err.to_string()))?;
        let field = match &output_layout[idx] {
            CompiledGroupedOutput::Group { output_name, .. } => {
                Field::new(output_name.as_str(), array.data_type().clone(), true)
            }
            CompiledGroupedOutput::Count { output_name }
            | CompiledGroupedOutput::Sum { output_name } => {
                Field::new(output_name.as_str(), ArrowDataType::Int64, false)
            }
        };
        arrays.push(array);
        fields.push(field);
    }
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).map_err(|err| api_error(err.to_string()))
}

#[derive(Debug, Clone)]
enum CompiledGroupedOutput {
    Group {
        group_position: usize,
        output_name: String,
        data_type: ArrowDataType,
    },
    Count {
        output_name: String,
    },
    Sum {
        output_name: String,
    },
}

fn output_group_arrow_type(
    provider: &HoloStoreTableProvider,
    column_name: &str,
) -> PgWireResult<ArrowDataType> {
    let column = provider
        .columns()
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| {
            api_error(format!(
                "grouped aggregate column '{}' not found in table metadata",
                column_name
            ))
        })?;
    Ok(match column.column_type {
        TableColumnType::Int8 => ArrowDataType::Int8,
        TableColumnType::Int16 => ArrowDataType::Int16,
        TableColumnType::Int32 => ArrowDataType::Int32,
        TableColumnType::Int64 => ArrowDataType::Int64,
        TableColumnType::UInt8 => ArrowDataType::UInt8,
        TableColumnType::UInt16 => ArrowDataType::UInt16,
        TableColumnType::UInt32 => ArrowDataType::UInt32,
        TableColumnType::UInt64 => ArrowDataType::UInt64,
        TableColumnType::Float64 => ArrowDataType::Float64,
        TableColumnType::Boolean => ArrowDataType::Boolean,
        TableColumnType::Utf8 => ArrowDataType::Utf8,
        TableColumnType::TimestampNanosecond => {
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None)
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExplainOutputFormat {
    Text,
    Json,
}

#[derive(Debug, Clone)]
struct ExplainRequest {
    analyze: bool,
    verbose: bool,
    dist: bool,
    format: ExplainOutputFormat,
    explained_sql: String,
}

#[derive(Debug, Clone)]
struct ExplainDistPlacementRow {
    stage: String,
    placement: String,
    detail: String,
    stage_id: u64,
}

#[derive(Debug, Clone)]
struct ExplainDistOutputRow {
    stage: String,
    placement: String,
    detail: String,
    query_execution_id: String,
    stage_id: u64,
}

#[derive(Debug, Default, Clone)]
struct ExplainDistRuntimeSummary {
    grpc_calls: u64,
    network_bytes: u64,
    retries: u64,
    reroutes: u64,
    involved_shards: BTreeSet<usize>,
    involved_targets: BTreeSet<String>,
}

/// Executes PostgreSQL-style `EXPLAIN` with optional distributed (`DIST`) extensions.
async fn execute_explain_statement(
    statement: &Statement,
    session_context: &SessionContext,
    holostore_client: &HoloStoreClient,
    pushdown_metrics: &PushdownMetrics,
    client: &(dyn ClientInfo + Send + Sync),
    protocol: QueryProtocol,
    query_execution_id: &str,
) -> PgWireResult<Response> {
    let explain = parse_explain_request(statement)?;
    let started_at_unix_ns = now_unix_nanos();
    let metrics_before = pushdown_metrics.snapshot();
    let mut plan_lines = collect_explain_plan_lines(session_context, &explain).await?;
    let finished_at_unix_ns = now_unix_nanos();
    let metrics_after = pushdown_metrics.snapshot();

    let timeline = collect_explain_timeline_events(
        pushdown_metrics,
        query_execution_id,
        started_at_unix_ns,
        finished_at_unix_ns,
    );

    if !explain.dist {
        if let Some(access_path_line) = derive_explain_access_path_annotation(timeline.as_slice()) {
            plan_lines.push(access_path_line);
        }
        return match explain.format {
            ExplainOutputFormat::Text => {
                let batch = explain_text_lines_to_batch(plan_lines.as_slice())?;
                encode_record_batch_query_response(batch, "holo_explain_text", client, protocol)
                    .await
            }
            ExplainOutputFormat::Json => {
                let doc = build_explain_json_document(
                    query_execution_id,
                    &explain,
                    plan_lines.as_slice(),
                    timeline.as_slice(),
                );
                let batch = explain_json_to_batch(doc.as_str())?;
                encode_record_batch_query_response(batch, "holo_explain_json", client, protocol)
                    .await
            }
        };
    }

    let topology = fetch_topology(holostore_client).await.ok();
    let runtime =
        summarize_explain_dist_runtime(timeline.as_slice(), &metrics_before, &metrics_after);
    let placement_rows = classify_explain_placement(plan_lines.as_slice(), query_execution_id);
    let dist_rows = build_explain_dist_output_rows(
        placement_rows.as_slice(),
        query_execution_id,
        holostore_client.target().to_string().as_str(),
        topology.as_ref(),
        &runtime,
        timeline.as_slice(),
    );

    match explain.format {
        ExplainOutputFormat::Text => {
            let batch = explain_dist_rows_to_batch(dist_rows.as_slice())?;
            encode_record_batch_query_response(batch, "holo_explain_dist", client, protocol).await
        }
        ExplainOutputFormat::Json => {
            let json = build_explain_dist_json_document(
                query_execution_id,
                &explain,
                plan_lines.as_slice(),
                dist_rows.as_slice(),
                timeline.as_slice(),
                topology.as_ref(),
                &runtime,
            );
            let batch = explain_json_to_batch(json.as_str())?;
            encode_record_batch_query_response(batch, "holo_explain_dist_json", client, protocol)
                .await
        }
    }
}

/// Executes one explain statement against DataFusion and extracts text plan lines.
async fn collect_explain_plan_lines(
    session_context: &SessionContext,
    explain: &ExplainRequest,
) -> PgWireResult<Vec<String>> {
    let explain_sql =
        build_engine_explain_sql(explain.analyze, explain.verbose, &explain.explained_sql);
    let explain_df = session_context
        .sql(explain_sql.as_str())
        .await
        .map_err(|err| api_error(err.to_string()))?;
    let batches = explain_df
        .collect()
        .await
        .map_err(|err| api_error(err.to_string()))?;

    let mut plan_lines = Vec::new();
    for batch in &batches {
        for row_idx in 0..batch.num_rows() {
            for col_idx in 0..batch.num_columns() {
                let scalar = ScalarValue::try_from_array(batch.column(col_idx).as_ref(), row_idx)
                    .map_err(|err| api_error(err.to_string()))?;
                match scalar {
                    ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)) => {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() {
                            plan_lines.push(trimmed.to_string());
                        }
                    }
                    ScalarValue::Null => {}
                    other => {
                        let rendered = other.to_string();
                        if !rendered.trim().is_empty() {
                            plan_lines.push(rendered);
                        }
                    }
                }
            }
        }
    }

    if plan_lines.is_empty() {
        plan_lines.push("empty explain plan".to_string());
    }
    Ok(plan_lines)
}

/// Builds one engine explain SQL string for a statement.
fn build_engine_explain_sql(analyze: bool, verbose: bool, explained_sql: &str) -> String {
    let mut out = String::from("EXPLAIN ");
    if analyze {
        out.push_str("ANALYZE ");
    }
    if verbose {
        out.push_str("VERBOSE ");
    }
    out.push_str(explained_sql);
    out
}

/// Parses one SQL `EXPLAIN` statement into execution options.
fn parse_explain_request(statement: &Statement) -> PgWireResult<ExplainRequest> {
    let Statement::Explain {
        statement: explained,
        analyze,
        verbose,
        format,
        options,
        ..
    } = statement
    else {
        return Err(api_error("expected EXPLAIN statement"));
    };

    let mut analyze_enabled = *analyze;
    let mut verbose_enabled = *verbose;
    let mut dist_enabled = false;
    let mut output_format = match format {
        None | Some(AnalyzeFormat::TEXT) => ExplainOutputFormat::Text,
        Some(AnalyzeFormat::JSON) => ExplainOutputFormat::Json,
        Some(AnalyzeFormat::GRAPHVIZ) => {
            return Err(to_user_error(
                "0A000",
                "EXPLAIN FORMAT GRAPHVIZ is not supported",
            ));
        }
    };

    if let Some(raw_options) = options {
        for option in raw_options {
            let option_name = normalize_ident(&option.name).to_ascii_lowercase();
            match option_name.as_str() {
                "analyze" => {
                    analyze_enabled = parse_explain_bool_option(option, true)?;
                }
                "verbose" => {
                    verbose_enabled = parse_explain_bool_option(option, true)?;
                }
                "dist" | "distributed" => {
                    dist_enabled = parse_explain_bool_option(option, true)?;
                }
                "format" => {
                    output_format = parse_explain_format_option(option)?;
                }
                _ => {}
            }
        }
    }

    Ok(ExplainRequest {
        analyze: analyze_enabled,
        verbose: verbose_enabled,
        dist: dist_enabled,
        format: output_format,
        explained_sql: explained.to_string(),
    })
}

/// Parses one boolean-style utility option (`ON/OFF/TRUE/FALSE`).
fn parse_explain_bool_option(
    option: &UtilityOption,
    default_if_missing: bool,
) -> PgWireResult<bool> {
    let Some(expr) = option.arg.as_ref() else {
        return Ok(default_if_missing);
    };
    parse_sql_bool_expr(expr).ok_or_else(|| {
        to_user_error(
            "42601",
            format!(
                "invalid EXPLAIN option value for '{}': {}",
                option.name, expr
            ),
        )
    })
}

/// Parses one `FORMAT` explain option.
fn parse_explain_format_option(option: &UtilityOption) -> PgWireResult<ExplainOutputFormat> {
    let Some(expr) = option.arg.as_ref() else {
        return Err(to_user_error(
            "42601",
            "EXPLAIN FORMAT option requires a value (TEXT or JSON)",
        ));
    };
    let Some(word) = parse_sql_identifierish(expr) else {
        return Err(to_user_error(
            "42601",
            format!("invalid EXPLAIN FORMAT option value: {}", expr),
        ));
    };
    match word.to_ascii_uppercase().as_str() {
        "TEXT" => Ok(ExplainOutputFormat::Text),
        "JSON" => Ok(ExplainOutputFormat::Json),
        "GRAPHVIZ" => Err(to_user_error(
            "0A000",
            "EXPLAIN FORMAT GRAPHVIZ is not supported",
        )),
        other => Err(to_user_error(
            "42601",
            format!("unsupported EXPLAIN FORMAT '{}'", other),
        )),
    }
}

/// Parses one SQL expression into a boolean value when possible.
fn parse_sql_bool_expr(expr: &SqlExpr) -> Option<bool> {
    match expr {
        SqlExpr::Value(value) => match &value.value {
            Value::Boolean(v) => Some(*v),
            Value::SingleQuotedString(raw)
            | Value::DoubleQuotedString(raw)
            | Value::EscapedStringLiteral(raw)
            | Value::NationalStringLiteral(raw) => parse_sql_bool_word(raw),
            _ => None,
        },
        SqlExpr::Identifier(ident) => parse_sql_bool_word(normalize_ident(ident).as_str()),
        SqlExpr::CompoundIdentifier(idents) => idents
            .last()
            .and_then(|ident| parse_sql_bool_word(normalize_ident(ident).as_str())),
        _ => None,
    }
}

/// Parses one SQL token-like value into TEXT/JSON keyword text.
fn parse_sql_identifierish(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(ident) => Some(normalize_ident(ident)),
        SqlExpr::CompoundIdentifier(idents) => idents.last().map(normalize_ident),
        SqlExpr::Value(value) => match &value.value {
            Value::SingleQuotedString(raw)
            | Value::DoubleQuotedString(raw)
            | Value::EscapedStringLiteral(raw)
            | Value::NationalStringLiteral(raw) => Some(raw.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Parses one SQL boolean word.
fn parse_sql_bool_word(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "on" | "yes" | "1" => Some(true),
        "false" | "off" | "no" | "0" => Some(false),
        _ => None,
    }
}

/// Builds one plain-text PostgreSQL-style explain batch (`QUERY PLAN` column).
fn explain_text_lines_to_batch(plan_lines: &[String]) -> PgWireResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "QUERY PLAN",
        ArrowDataType::Utf8,
        false,
    )]));
    let mut lines = StringBuilder::new();
    for line in plan_lines {
        lines.append_value(line.as_str());
    }
    RecordBatch::try_new(schema, vec![Arc::new(lines.finish()) as ArrayRef])
        .map_err(|err| api_error(err.to_string()))
}

/// Builds one JSON explain response batch (`QUERY PLAN` column).
fn explain_json_to_batch(json_payload: &str) -> PgWireResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "QUERY PLAN",
        ArrowDataType::Utf8,
        false,
    )]));
    let mut values = StringBuilder::new();
    values.append_value(json_payload);
    RecordBatch::try_new(schema, vec![Arc::new(values.finish()) as ArrayRef])
        .map_err(|err| api_error(err.to_string()))
}

/// Encodes one record batch into a query response.
async fn encode_record_batch_query_response(
    batch: RecordBatch,
    table_name: &str,
    client: &(dyn ClientInfo + Send + Sync),
    protocol: QueryProtocol,
) -> PgWireResult<Response> {
    let schema = batch.schema();
    let mem =
        MemTable::try_new(schema, vec![vec![batch]]).map_err(|err| api_error(err.to_string()))?;
    let query_ctx = SessionContext::new();
    query_ctx
        .register_table(table_name, Arc::new(mem))
        .map_err(|err| api_error(err.to_string()))?;
    let dataframe = query_ctx
        .sql(format!("SELECT * FROM {}", quote_sql_ident(table_name)).as_str())
        .await
        .map_err(|err| api_error(err.to_string()))?;
    let format_options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
    let wire_format = hook_query_wire_format(protocol);
    let query = df::encode_dataframe(dataframe, &wire_format, Some(format_options))
        .await
        .map_err(|err| api_error(err.to_string()))?;
    Ok(Response::Query(query))
}

/// Builds one quoted SQL identifier.
fn quote_sql_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Converts explain placement rows into a record batch.
fn explain_dist_rows_to_batch(rows: &[ExplainDistOutputRow]) -> PgWireResult<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("stage", ArrowDataType::Utf8, false),
        Field::new("placement", ArrowDataType::Utf8, false),
        Field::new("detail", ArrowDataType::Utf8, false),
        Field::new("query_execution_id", ArrowDataType::Utf8, false),
        Field::new("stage_id", ArrowDataType::UInt64, false),
    ]));

    let mut stage_builder = StringBuilder::new();
    let mut placement_builder = StringBuilder::new();
    let mut detail_builder = StringBuilder::new();
    let mut query_builder = StringBuilder::new();
    let mut stage_id_builder = datafusion::arrow::array::UInt64Builder::new();
    for row in rows {
        stage_builder.append_value(row.stage.as_str());
        placement_builder.append_value(row.placement.as_str());
        detail_builder.append_value(row.detail.as_str());
        query_builder.append_value(row.query_execution_id.as_str());
        stage_id_builder.append_value(row.stage_id);
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(stage_builder.finish()) as ArrayRef,
            Arc::new(placement_builder.finish()) as ArrayRef,
            Arc::new(detail_builder.finish()) as ArrayRef,
            Arc::new(query_builder.finish()) as ArrayRef,
            Arc::new(stage_id_builder.finish()) as ArrayRef,
        ],
    )
    .map_err(|err| api_error(err.to_string()))
}

/// Builds one PostgreSQL-style JSON explain payload for non-distributed mode.
fn build_explain_json_document(
    query_execution_id: &str,
    explain: &ExplainRequest,
    plan_lines: &[String],
    timeline: &[crate::metrics::QueryStageEvent],
) -> String {
    let timeline_json = build_explain_timeline_json(timeline);
    let mut plan_json = serde_json::Map::new();
    plan_json.insert(
        "query_execution_id".to_string(),
        serde_json::Value::String(query_execution_id.to_string()),
    );
    plan_json.insert(
        "analyze".to_string(),
        serde_json::Value::Bool(explain.analyze),
    );
    plan_json.insert(
        "verbose".to_string(),
        serde_json::Value::Bool(explain.verbose),
    );
    plan_json.insert(
        "plan_lines".to_string(),
        serde_json::Value::Array(
            plan_lines
                .iter()
                .map(|line| serde_json::Value::String(line.clone()))
                .collect::<Vec<_>>(),
        ),
    );
    plan_json.insert(
        "timeline".to_string(),
        serde_json::Value::Array(timeline_json),
    );

    if explain.verbose {
        let optimizer_trace = build_explain_optimizer_trace_json(timeline);
        if !optimizer_trace.is_empty() {
            plan_json.insert(
                "optimizer_trace".to_string(),
                serde_json::Value::Array(optimizer_trace),
            );
        }
    }

    serde_json::json!([
        {
            "Plan": serde_json::Value::Object(plan_json)
        }
    ])
    .to_string()
}

/// Builds one PostgreSQL-style JSON explain payload for distributed mode.
fn build_explain_dist_json_document(
    query_execution_id: &str,
    explain: &ExplainRequest,
    plan_lines: &[String],
    dist_rows: &[ExplainDistOutputRow],
    timeline: &[crate::metrics::QueryStageEvent],
    topology: Option<&ClusterTopology>,
    runtime: &ExplainDistRuntimeSummary,
) -> String {
    let topology_json = topology.map(|topology| {
        let nodes = topology
            .members
            .values()
            .map(|member| {
                serde_json::json!({
                    "node_id": member.node_id,
                    "grpc_addr": member.grpc_addr.to_string(),
                    "state": member.state,
                })
            })
            .collect::<Vec<_>>();
        let shards = topology
            .shards
            .iter()
            .map(|shard| {
                serde_json::json!({
                    "shard_id": shard.shard_id,
                    "shard_index": shard.shard_index,
                    "leaseholder": shard.leaseholder,
                    "start_key_hex": hex::encode(&shard.start_key),
                    "end_key_hex": hex::encode(&shard.end_key),
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "sql_nodes": nodes,
            "kv_nodes": nodes,
            "shards": shards,
        })
    });
    let timeline_json = build_explain_timeline_json(timeline);
    let dist_rows_json = dist_rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "stage": row.stage,
                "placement": row.placement,
                "detail": row.detail,
                "query_execution_id": row.query_execution_id,
                "stage_id": row.stage_id,
            })
        })
        .collect::<Vec<_>>();
    let mut plan_json = serde_json::Map::new();
    plan_json.insert(
        "query_execution_id".to_string(),
        serde_json::Value::String(query_execution_id.to_string()),
    );
    plan_json.insert(
        "analyze".to_string(),
        serde_json::Value::Bool(explain.analyze),
    );
    plan_json.insert(
        "verbose".to_string(),
        serde_json::Value::Bool(explain.verbose),
    );
    plan_json.insert("distributed".to_string(), serde_json::Value::Bool(true));
    plan_json.insert(
        "plan_lines".to_string(),
        serde_json::Value::Array(
            plan_lines
                .iter()
                .map(|line| serde_json::Value::String(line.clone()))
                .collect::<Vec<_>>(),
        ),
    );
    plan_json.insert(
        "distribution".to_string(),
        serde_json::json!({
            "grpc_calls": runtime.grpc_calls,
            "network_bytes": runtime.network_bytes,
            "retries": runtime.retries,
            "reroutes": runtime.reroutes,
            "involved_shards": runtime.involved_shards.iter().copied().collect::<Vec<_>>(),
            "involved_targets": runtime.involved_targets.iter().cloned().collect::<Vec<_>>(),
        }),
    );
    plan_json.insert(
        "topology".to_string(),
        topology_json.unwrap_or(serde_json::Value::Null),
    );
    plan_json.insert(
        "stages".to_string(),
        serde_json::Value::Array(dist_rows_json),
    );
    plan_json.insert(
        "timeline".to_string(),
        serde_json::Value::Array(timeline_json),
    );

    if explain.verbose {
        let optimizer_trace = build_explain_optimizer_trace_json(timeline);
        if !optimizer_trace.is_empty() {
            plan_json.insert(
                "optimizer_trace".to_string(),
                serde_json::Value::Array(optimizer_trace),
            );
        }
    }

    serde_json::json!([
        {
            "Plan": serde_json::Value::Object(plan_json)
        }
    ])
    .to_string()
}

/// Converts timeline events to JSON entries for EXPLAIN output.
fn build_explain_timeline_json(
    timeline: &[crate::metrics::QueryStageEvent],
) -> Vec<serde_json::Value> {
    timeline
        .iter()
        .map(|event| {
            serde_json::json!({
                "query_execution_id": event.query_execution_id,
                "stage_id": event.stage_id,
                "kind": event.kind,
                "detail": event.detail,
                "at_unix_ns": event.at_unix_ns,
            })
        })
        .collect::<Vec<_>>()
}

/// Builds a compact, typed optimizer trace from timeline events.
///
/// This is only emitted in EXPLAIN JSON when verbose mode is enabled.
fn build_explain_optimizer_trace_json(
    timeline: &[crate::metrics::QueryStageEvent],
) -> Vec<serde_json::Value> {
    timeline
        .iter()
        .filter(|event| is_optimizer_trace_event_kind(event.kind.as_str()))
        .map(|event| {
            let mut entry = serde_json::Map::new();
            entry.insert(
                "kind".to_string(),
                serde_json::Value::String(event.kind.clone()),
            );
            entry.insert(
                "stage_id".to_string(),
                serde_json::Value::Number(event.stage_id.into()),
            );
            entry.insert(
                "at_unix_ns".to_string(),
                serde_json::Value::Number(event.at_unix_ns.into()),
            );
            entry.insert(
                "detail".to_string(),
                serde_json::Value::String(event.detail.clone()),
            );
            let fields = parse_explain_kv_detail_typed(event.detail.as_str());
            if !fields.is_empty() {
                entry.insert("fields".to_string(), serde_json::Value::Object(fields));
            }
            serde_json::Value::Object(entry)
        })
        .collect::<Vec<_>>()
}

/// Returns true when one timeline event should appear in optimizer traces.
fn is_optimizer_trace_event_kind(kind: &str) -> bool {
    kind.starts_with("optimizer_")
        || kind.contains("_optimizer_")
        || kind == "aggregate_pushdown_plan_choice"
}

/// Parses explain event details into typed JSON key/value fields.
fn parse_explain_kv_detail_typed(detail: &str) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    let mut raw_fields = parse_explain_kv_detail(detail)
        .into_iter()
        .collect::<Vec<_>>();
    raw_fields.sort_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));
    for (key, value) in raw_fields {
        fields.insert(key, parse_explain_kv_value(value.as_str()));
    }

    // Preserve full access-path strings even when they contain spaces.
    if let Some(path) = extract_optimizer_path_token(detail) {
        fields.insert("path".to_string(), serde_json::Value::String(path));
    }
    fields
}

/// Converts one key/value token into a typed JSON scalar.
fn parse_explain_kv_value(raw: &str) -> serde_json::Value {
    let value = raw.trim().trim_matches(|ch| ch == '"' || ch == '\'');
    match value.to_ascii_lowercase().as_str() {
        "true" => return serde_json::Value::Bool(true),
        "false" => return serde_json::Value::Bool(false),
        _ => {}
    }

    if let Ok(parsed) = value.parse::<i64>() {
        return serde_json::Value::Number(parsed.into());
    }
    if let Ok(parsed) = value.parse::<u64>() {
        return serde_json::Value::Number(parsed.into());
    }
    if let Ok(parsed) = value.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(parsed) {
            return serde_json::Value::Number(number);
        }
    }
    serde_json::Value::String(value.to_string())
}

/// Collects timeline events associated with one explain request.
fn collect_explain_timeline_events(
    pushdown_metrics: &PushdownMetrics,
    query_execution_id: &str,
    started_at_unix_ns: u64,
    finished_at_unix_ns: u64,
) -> Vec<crate::metrics::QueryStageEvent> {
    let mut direct = pushdown_metrics
        .recent_stage_events(4096)
        .into_iter()
        .filter(|event| event.query_execution_id == query_execution_id)
        .collect::<Vec<_>>();
    direct.sort_by_key(|event| (event.at_unix_ns, event.stage_id));
    if direct
        .iter()
        .any(|event| event.kind.starts_with("scan_") || event.kind.starts_with("optimizer_"))
    {
        return direct;
    }

    let slack = 5_000_000u64;
    let window_start = started_at_unix_ns.saturating_sub(slack);
    let window_end = finished_at_unix_ns.saturating_add(slack);
    let mut fallback = pushdown_metrics
        .recent_stage_events(4096)
        .into_iter()
        .filter(|event| {
            event.at_unix_ns >= window_start
                && event.at_unix_ns <= window_end
                && (event.kind.starts_with("scan_") || event.kind.starts_with("optimizer_"))
        })
        .collect::<Vec<_>>();
    fallback.sort_by_key(|event| (event.at_unix_ns, event.stage_id));
    fallback
}

/// Derives one human-readable access-path line from optimizer timeline events.
fn derive_explain_access_path_annotation(
    timeline: &[crate::metrics::QueryStageEvent],
) -> Option<String> {
    let path = timeline.iter().rev().find_map(|event| {
        if !event.kind.ends_with("plan_choice") {
            return None;
        }
        extract_optimizer_path_token(event.detail.as_str())
    })?;
    Some(format!(
        "AccessPath: {}",
        render_access_path_summary(path.as_str())
    ))
}

/// Extracts the `path=...` token from one optimizer-plan detail string.
fn extract_optimizer_path_token(detail: &str) -> Option<String> {
    let marker = "path=";
    let start = detail.find(marker)? + marker.len();
    let tail = &detail[start..];
    if tail.starts_with("index_scan[") {
        let end = tail.find(']')?;
        return Some(tail[..=end].to_string());
    }
    let end = tail.find(char::is_whitespace).unwrap_or(tail.len());
    Some(tail[..end].to_string())
}

/// Renders one compact access-path summary for explain text.
fn render_access_path_summary(path: &str) -> String {
    if path == "table_scan" {
        return "table_scan".to_string();
    }
    if let Some(inner) = path
        .strip_prefix("index_scan[")
        .and_then(|token| token.strip_suffix(']'))
    {
        let mut index_name = None::<&str>;
        let mut covering = false;
        for token in inner.split_whitespace() {
            if let Some(value) = token.strip_prefix("index=") {
                index_name = Some(value);
            } else if let Some(value) = token.strip_prefix("covering=") {
                covering = value.eq_ignore_ascii_case("true");
            }
        }
        let kind = if covering { "index_only" } else { "index_scan" };
        if let Some(name) = index_name {
            return format!("{kind} index={name}");
        }
        return kind.to_string();
    }
    path.to_string()
}

/// Summarizes distributed explain runtime counters from timeline and metric deltas.
fn summarize_explain_dist_runtime(
    timeline: &[crate::metrics::QueryStageEvent],
    before: &crate::metrics::PushdownMetricsSnapshot,
    after: &crate::metrics::PushdownMetricsSnapshot,
) -> ExplainDistRuntimeSummary {
    let mut runtime = ExplainDistRuntimeSummary {
        grpc_calls: after.scan_rpc_pages.saturating_sub(before.scan_rpc_pages),
        network_bytes: after
            .scan_bytes_scanned
            .saturating_sub(before.scan_bytes_scanned),
        retries: after
            .scan_retry_count
            .saturating_sub(before.scan_retry_count),
        reroutes: after
            .scan_reroute_count
            .saturating_sub(before.scan_reroute_count),
        ..ExplainDistRuntimeSummary::default()
    };

    let mut grpc_calls_from_timeline = 0u64;
    let mut network_bytes_from_timeline = 0u64;
    let mut retries_from_timeline = 0u64;
    let mut reroutes_from_timeline = 0u64;
    for event in timeline {
        if event.kind == "scan_chunk" {
            grpc_calls_from_timeline = grpc_calls_from_timeline.saturating_add(1);
        } else if event.kind == "scan_retry" {
            retries_from_timeline = retries_from_timeline.saturating_add(1);
        } else if event.kind == "scan_reroute" {
            reroutes_from_timeline = reroutes_from_timeline.saturating_add(1);
        }

        let kv = parse_explain_kv_detail(event.detail.as_str());
        if let Some(bytes) = kv
            .get("bytes_scanned")
            .and_then(|raw| raw.parse::<u64>().ok())
        {
            network_bytes_from_timeline = network_bytes_from_timeline.saturating_add(bytes);
        }
        if let Some(shard) = kv.get("shard").and_then(|raw| raw.parse::<usize>().ok()) {
            runtime.involved_shards.insert(shard);
        }
        if let Some(shard) = kv
            .get("from_shard")
            .and_then(|raw| raw.parse::<usize>().ok())
        {
            runtime.involved_shards.insert(shard);
        }
        if let Some(shard) = kv.get("to_shard").and_then(|raw| raw.parse::<usize>().ok()) {
            runtime.involved_shards.insert(shard);
        }
        if let Some(target) = kv.get("target") {
            runtime.involved_targets.insert(target.to_string());
        }
        if let Some(target) = kv.get("from_target") {
            runtime.involved_targets.insert(target.to_string());
        }
        if let Some(target) = kv.get("to_target") {
            runtime.involved_targets.insert(target.to_string());
        }
    }

    runtime.grpc_calls = runtime.grpc_calls.max(grpc_calls_from_timeline);
    runtime.network_bytes = runtime.network_bytes.max(network_bytes_from_timeline);
    runtime.retries = runtime.retries.max(retries_from_timeline);
    runtime.reroutes = runtime.reroutes.max(reroutes_from_timeline);
    runtime
}

/// Extracts key-value tokens from one stage-event detail string.
fn parse_explain_kv_detail(detail: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for token in detail.split_whitespace() {
        let token = token.trim_matches(|ch: char| ch == ',' || ch == ';');
        let Some((key, value)) = token.split_once('=') else {
            continue;
        };
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim().trim_matches(',').to_string();
        if key.is_empty() || value.is_empty() {
            continue;
        }
        out.insert(key, value);
    }
    out
}

/// Builds distributed explain output rows including Cockroach-style sections.
fn build_explain_dist_output_rows(
    placement_rows: &[ExplainDistPlacementRow],
    query_execution_id: &str,
    gateway: &str,
    topology: Option<&ClusterTopology>,
    runtime: &ExplainDistRuntimeSummary,
    timeline: &[crate::metrics::QueryStageEvent],
) -> Vec<ExplainDistOutputRow> {
    let mut rows = Vec::new();
    let sql_node_count = topology.map(|topology| topology.members.len()).unwrap_or(1);
    let kv_node_count = sql_node_count;
    rows.push(ExplainDistOutputRow {
        stage: "distribution".to_string(),
        placement: "gateway".to_string(),
        detail: "mode=distributed".to_string(),
        query_execution_id: query_execution_id.to_string(),
        stage_id: 0,
    });
    rows.push(ExplainDistOutputRow {
        stage: "gateway".to_string(),
        placement: "gateway".to_string(),
        detail: format!("target={gateway}"),
        query_execution_id: query_execution_id.to_string(),
        stage_id: 0,
    });
    rows.push(ExplainDistOutputRow {
        stage: "sql_nodes".to_string(),
        placement: "control-plane".to_string(),
        detail: format!("count={sql_node_count}"),
        query_execution_id: query_execution_id.to_string(),
        stage_id: 0,
    });
    rows.push(ExplainDistOutputRow {
        stage: "kv_nodes".to_string(),
        placement: "control-plane".to_string(),
        detail: format!("count={kv_node_count}"),
        query_execution_id: query_execution_id.to_string(),
        stage_id: 0,
    });
    rows.push(ExplainDistOutputRow {
        stage: "regions".to_string(),
        placement: "control-plane".to_string(),
        detail: "regions=unknown".to_string(),
        query_execution_id: query_execution_id.to_string(),
        stage_id: 0,
    });
    rows.push(ExplainDistOutputRow {
        stage: "runtime_network".to_string(),
        placement: "rpc".to_string(),
        detail: format!(
            "grpc_calls={} network_bytes={}",
            runtime.grpc_calls, runtime.network_bytes
        ),
        query_execution_id: query_execution_id.to_string(),
        stage_id: 0,
    });
    rows.push(ExplainDistOutputRow {
        stage: "runtime_retries".to_string(),
        placement: "rpc".to_string(),
        detail: format!("retries={} reroutes={}", runtime.retries, runtime.reroutes),
        query_execution_id: query_execution_id.to_string(),
        stage_id: 0,
    });

    if let Some(topology) = topology {
        let involved = if runtime.involved_shards.is_empty() {
            topology
                .shards
                .iter()
                .map(|shard| shard.shard_index)
                .collect::<BTreeSet<_>>()
        } else {
            runtime.involved_shards.clone()
        };
        rows.push(ExplainDistOutputRow {
            stage: "range_spans".to_string(),
            placement: "kv".to_string(),
            detail: format!("count={}", involved.len()),
            query_execution_id: query_execution_id.to_string(),
            stage_id: 0,
        });
        for shard_index in involved {
            if let Some(shard) = topology
                .shards
                .iter()
                .find(|shard| shard.shard_index == shard_index)
            {
                rows.push(ExplainDistOutputRow {
                    stage: "range_span".to_string(),
                    placement: "kv".to_string(),
                    detail: format!(
                        "shard={} shard_id={} start={} end={}",
                        shard.shard_index,
                        shard.shard_id,
                        hex::encode(&shard.start_key),
                        hex::encode(&shard.end_key)
                    ),
                    query_execution_id: query_execution_id.to_string(),
                    stage_id: 0,
                });
            }
        }
    } else {
        rows.push(ExplainDistOutputRow {
            stage: "range_spans".to_string(),
            placement: "kv".to_string(),
            detail: "count=unknown topology=unavailable".to_string(),
            query_execution_id: query_execution_id.to_string(),
            stage_id: 0,
        });
    }

    for row in placement_rows {
        rows.push(ExplainDistOutputRow {
            stage: row.stage.clone(),
            placement: row.placement.clone(),
            detail: row.detail.clone(),
            query_execution_id: query_execution_id.to_string(),
            stage_id: row.stage_id,
        });
    }

    for event in timeline.iter().take(128) {
        rows.push(ExplainDistOutputRow {
            stage: "timeline".to_string(),
            placement: "runtime".to_string(),
            detail: format!(
                "kind={} at_unix_ns={} detail={}",
                event.kind, event.at_unix_ns, event.detail
            ),
            query_execution_id: query_execution_id.to_string(),
            stage_id: event.stage_id,
        });
    }
    rows
}

/// Inserts one placement row with de-duplication and stage-id assignment.
fn push_explain_placement_row(
    rows: &mut Vec<ExplainDistPlacementRow>,
    seen: &mut BTreeSet<(String, String, String)>,
    next_stage_id: &mut u64,
    stage: &str,
    placement: &str,
    detail: String,
) {
    let key = (stage.to_string(), placement.to_string(), detail.clone());
    if !seen.insert(key) {
        return;
    }
    rows.push(ExplainDistPlacementRow {
        stage: stage.to_string(),
        placement: placement.to_string(),
        detail,
        stage_id: *next_stage_id,
    });
    *next_stage_id = next_stage_id.saturating_add(1);
}

/// Classifies physical plan lines into placement-oriented stage rows.
fn classify_explain_placement(
    plan_lines: &[String],
    query_execution_id: &str,
) -> Vec<ExplainDistPlacementRow> {
    let mut seen = BTreeSet::<(String, String, String)>::new();
    let mut rows = Vec::<ExplainDistPlacementRow>::new();
    let mut next_stage_id = 1u64;

    push_explain_placement_row(
        &mut rows,
        &mut seen,
        &mut next_stage_id,
        "query",
        "gateway",
        format!("query_execution_id={query_execution_id}"),
    );

    for line in plan_lines {
        let lower = line.to_ascii_lowercase();
        if lower.contains("holostoretableprovider") || lower.contains("tablescan") {
            push_explain_placement_row(
                &mut rows,
                &mut seen,
                &mut next_stage_id,
                "scan",
                "executor+leaseholder",
                line.clone(),
            );
            continue;
        }
        if lower.contains("index") && lower.contains("scan") {
            push_explain_placement_row(
                &mut rows,
                &mut seen,
                &mut next_stage_id,
                "index_scan",
                "executor+leaseholder",
                line.clone(),
            );
            continue;
        }
        if lower.contains("filterexec") || lower.contains("projectionexec") {
            push_explain_placement_row(
                &mut rows,
                &mut seen,
                &mut next_stage_id,
                "filter_projection",
                "executor-local",
                line.clone(),
            );
            continue;
        }
        if lower.contains("aggregateexec") && lower.contains("mode=partial") {
            push_explain_placement_row(
                &mut rows,
                &mut seen,
                &mut next_stage_id,
                "aggregate_partial",
                "executor-local",
                line.clone(),
            );
            continue;
        }
        if lower.contains("aggregateexec") && lower.contains("mode=final") {
            push_explain_placement_row(
                &mut rows,
                &mut seen,
                &mut next_stage_id,
                "aggregate_final",
                "merge/gateway",
                line.clone(),
            );
            continue;
        }
        if (lower.contains("sortexec") && lower.contains("fetch="))
            || lower.contains("sortpreservingmergeexec")
        {
            push_explain_placement_row(
                &mut rows,
                &mut seen,
                &mut next_stage_id,
                "topk",
                "executor+merge",
                line.clone(),
            );
            continue;
        }
        if lower.contains("hashjoinexec") || lower.contains("sortmergejoinexec") {
            push_explain_placement_row(
                &mut rows,
                &mut seen,
                &mut next_stage_id,
                "join",
                "distributed-executor",
                line.clone(),
            );
            continue;
        }
        if lower.contains("repartitionexec")
            || lower.contains("coalescepartitionsexec")
            || lower.contains("coalescebatchesexec")
        {
            push_explain_placement_row(
                &mut rows,
                &mut seen,
                &mut next_stage_id,
                "exchange",
                "networked-executor",
                line.clone(),
            );
        }
    }

    if next_stage_id == 2 {
        push_explain_placement_row(
            &mut rows,
            &mut seen,
            &mut next_stage_id,
            "plan",
            "gateway",
            "no distributed physical operators detected".to_string(),
        );
    }
    rows
}

/// Returns current unix timestamp in nanoseconds.
fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
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
        SessionTxnState::Active { txn, .. } => f(txn),
        SessionTxnState::Aborted { .. } => Err(aborted_tx_error()),
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
        SessionTxnState::Active { txn, .. } => Ok(f(txn)),
        SessionTxnState::Aborted { .. } => Err(aborted_tx_error()),
    }
}

/// Executes `ensure txn snapshot for table` for this component.
async fn ensure_txn_snapshot_for_table(
    tx_manager: &TxnSessionManager,
    session_id: &str,
    session_context: &SessionContext,
    table_name: &str,
    config: DmlRuntimeConfig,
    pushdown_metrics: &PushdownMetrics,
) -> PgWireResult<()> {
    let needs_snapshot = {
        let sessions = tx_manager.sessions.lock().await;
        let Some(state) = sessions.get(session_id) else {
            return Err(no_active_tx_error("active transaction required"));
        };
        // Decision: evaluate `state` to choose the correct SQL/storage control path.
        match state {
            SessionTxnState::Active { txn, .. } => {
                txn.ensure_same_table(table_name)?;
                txn.bound_table_name().is_none()
            }
            SessionTxnState::Aborted { .. } => return Err(aborted_tx_error()),
        }
    };
    // Decision: evaluate `!needs_snapshot` to choose the correct SQL/storage control path.
    if !needs_snapshot {
        return Ok(());
    }

    let provider = get_provider_for_table(session_context, table_name).await?;
    let snapshot_limit = config.max_txn_staged_rows.max(1);
    let rows = provider
        .scan_orders_with_versions_by_order_id_bounds(None, None, Some(snapshot_limit + 1))
        .await
        .map_err(|err| api_error(err.to_string()))?;
    if rows.len() > snapshot_limit {
        pushdown_metrics.record_txn_stage_limit_reject();
        return Err(to_user_error(
            "54000",
            format!(
                "transaction snapshot exceeded max_txn_staged_rows={} (snapshot_rows={})",
                snapshot_limit,
                rows.len()
            ),
        ));
    }

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

/// Looks up a HoloStore table provider by table name, returning `None` when unavailable.
async fn try_get_holo_provider_for_table(
    session_context: &SessionContext,
    table_name: &str,
) -> Option<HoloStoreTableProvider> {
    let provider = session_context.table_provider(table_name).await.ok()?;
    provider
        .as_any()
        .downcast_ref::<HoloStoreTableProvider>()
        .cloned()
}

/// Executes `enforce scan row limit` for this component.
fn enforce_scan_row_limit(
    rows_len: usize,
    max_rows: usize,
    pushdown_metrics: &PushdownMetrics,
    context: &'static str,
) -> PgWireResult<()> {
    if rows_len > max_rows.max(1) {
        pushdown_metrics.record_scan_row_limit_reject();
        return Err(to_user_error(
            "54000",
            format!(
                "{context} exceeded max_scan_rows={} (observed_rows={rows_len})",
                max_rows.max(1)
            ),
        ));
    }
    Ok(())
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
        return placeholder_plan_for_schema(Arc::new(Schema::new(vec![Field::new(
            "__dml_placeholder__",
            ArrowDataType::Null,
            true,
        )])));
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

fn placeholder_plan_for_schema(schema: Arc<Schema>) -> PgWireResult<LogicalPlan> {
    let df_schema = schema
        .to_dfschema()
        .map_err(|err| api_error(err.to_string()))?;
    Ok(LogicalPlan::EmptyRelation(
        datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(df_schema),
        },
    ))
}

fn fast_primary_key_aggregate_output_name(spec: &FastPrimaryKeyAggregateSpec) -> String {
    spec.output_alias.clone().unwrap_or_else(|| {
        if spec.coalesce_fallback.is_some() {
            "coalesce".to_string()
        } else {
            match spec.extreme {
                PrimaryKeyExtreme::Min => "min".to_string(),
                PrimaryKeyExtreme::Max => "max".to_string(),
            }
        }
    })
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
    parse_pk_bounds_for_column(selection, params, "order_id")
}

/// Parses bounded PK predicates for the requested primary-key column.
fn parse_pk_bounds_for_column(
    selection: &SqlExpr,
    params: &ParamValues,
    primary_key_column: &str,
) -> PgWireResult<OrderIdBounds> {
    let mut bounds = OrderIdBounds::default();
    collect_pk_bounds(selection, params, primary_key_column, &mut bounds)?;
    // Decision: evaluate `bounds.predicate_count == 0` to choose the correct SQL/storage control path.
    if bounds.predicate_count == 0 {
        return Err(to_user_error(
            "0A000",
            format!("WHERE clause must include {primary_key_column} predicates"),
        ));
    }
    Ok(bounds)
}

/// Executes `collect pk bounds` for this component.
fn collect_pk_bounds(
    expr: &SqlExpr,
    params: &ParamValues,
    primary_key_column: &str,
    bounds: &mut OrderIdBounds,
) -> PgWireResult<()> {
    // Decision: evaluate `expr` to choose the correct SQL/storage control path.
    match expr {
        SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            collect_pk_bounds(left, params, primary_key_column, bounds)?;
            collect_pk_bounds(right, params, primary_key_column, bounds)?;
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
                                format!(
                                    "WHERE clause must contain only {primary_key_column} comparisons joined by AND"
                                ),
                            ));
                    }
                },
            };

            // Decision: evaluate `column != primary_key_column` to choose the correct SQL/storage control path.
            if !column.eq_ignore_ascii_case(primary_key_column) {
                return Err(to_user_error(
                    "0A000",
                    format!("WHERE clause must contain only {primary_key_column} comparisons"),
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
                    format!("WHERE clause supports only =, >, >=, <, <= on {primary_key_column}"),
                ));
            }

            bounds.apply(op, literal);
            Ok(())
        }
        SqlExpr::Nested(inner) => collect_pk_bounds(inner, params, primary_key_column, bounds),
        _ => Err(to_user_error(
            "0A000",
            format!(
                "WHERE clause must contain only {primary_key_column} comparisons joined by AND"
            ),
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

#[derive(Debug, Default)]
/// Represents a generic row_v1 update patch keyed by column index.
struct RowV1UpdatePatch {
    assignments: BTreeMap<usize, ScalarValue>,
}

impl RowV1UpdatePatch {
    /// Applies row_v1 patch assignments to one existing row.
    fn apply(&self, row: &[ScalarValue]) -> Vec<ScalarValue> {
        let mut updated = row.to_vec();
        for (index, value) in &self.assignments {
            if let Some(slot) = updated.get_mut(*index) {
                *slot = value.clone();
            }
        }
        updated
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

/// Parses a generic row_v1 UPDATE patch from assignments.
fn parse_row_v1_update_patch(
    assignments: &[Assignment],
    params: &ParamValues,
    columns: &[TableColumnRecord],
    primary_key_column: &str,
) -> PgWireResult<RowV1UpdatePatch> {
    if assignments.is_empty() {
        return Err(to_user_error(
            "42601",
            "UPDATE requires at least one assignment",
        ));
    }
    if columns.is_empty() {
        return Err(api_error("row_v1 table has empty column metadata"));
    }

    let mut by_name = HashMap::<String, usize>::new();
    for (idx, column) in columns.iter().enumerate() {
        by_name.insert(column.name.to_ascii_lowercase(), idx);
    }

    let mut patch = RowV1UpdatePatch::default();
    let mut seen = HashSet::<String>::new();
    for assignment in assignments {
        let column = extract_assignment_column(&assignment.target)?.to_ascii_lowercase();
        if !seen.insert(column.clone()) {
            return Err(to_user_error(
                "42601",
                format!("duplicate assignment for column '{column}'"),
            ));
        }
        if column == primary_key_column.to_ascii_lowercase() {
            return Err(to_user_error(
                "0A000",
                format!(
                    "updating primary key column '{}' is not supported",
                    primary_key_column
                ),
            ));
        }

        let Some(column_index) = by_name.get(column.as_str()).copied() else {
            return Err(to_user_error(
                "0A000",
                format!("updating column '{column}' is not supported"),
            ));
        };
        let value =
            parse_update_scalar_for_column(&assignment.value, params, &columns[column_index])?;
        patch.assignments.insert(column_index, value);
    }

    Ok(patch)
}

/// Parses one UPDATE assignment expression into a typed scalar for a row_v1 column.
fn parse_update_scalar_for_column(
    expr: &SqlExpr,
    params: &ParamValues,
    column: &TableColumnRecord,
) -> PgWireResult<ScalarValue> {
    let nullable_error = || {
        to_user_error(
            "23502",
            format!(
                "null value in column '{}' violates not-null constraint",
                column.name
            ),
        )
    };

    match column.column_type {
        TableColumnType::Int8 => {
            let value = match extract_i64_expr(expr, params)? {
                Some(value) => value,
                None => {
                    return null_assignment_or_type_error(
                        expr,
                        params,
                        column,
                        "invalid int8 assignment",
                    )
                }
            };
            let narrowed = i8::try_from(value).map_err(|_| {
                to_user_error(
                    "22003",
                    format!("value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::Int8(Some(narrowed)))
        }
        TableColumnType::Int16 => {
            let value = match extract_i64_expr(expr, params)? {
                Some(value) => value,
                None => {
                    return null_assignment_or_type_error(
                        expr,
                        params,
                        column,
                        "invalid int16 assignment",
                    )
                }
            };
            let narrowed = i16::try_from(value).map_err(|_| {
                to_user_error(
                    "22003",
                    format!("value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::Int16(Some(narrowed)))
        }
        TableColumnType::Int32 => {
            let value = match extract_i64_expr(expr, params)? {
                Some(value) => value,
                None => {
                    return null_assignment_or_type_error(
                        expr,
                        params,
                        column,
                        "invalid int32 assignment",
                    )
                }
            };
            let narrowed = i32::try_from(value).map_err(|_| {
                to_user_error(
                    "22003",
                    format!("value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::Int32(Some(narrowed)))
        }
        TableColumnType::Int64 => {
            let value = match extract_i64_expr(expr, params)? {
                Some(value) => value,
                None => {
                    return null_assignment_or_type_error(
                        expr,
                        params,
                        column,
                        "invalid int64 assignment",
                    )
                }
            };
            Ok(ScalarValue::Int64(Some(value)))
        }
        TableColumnType::UInt8 => {
            let value = match extract_u64_expr(expr, params)? {
                Some(value) => value,
                None => {
                    return null_assignment_or_type_error(
                        expr,
                        params,
                        column,
                        "invalid uint8 assignment",
                    )
                }
            };
            let narrowed = u8::try_from(value).map_err(|_| {
                to_user_error(
                    "22003",
                    format!("value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::UInt8(Some(narrowed)))
        }
        TableColumnType::UInt16 => {
            let value = match extract_u64_expr(expr, params)? {
                Some(value) => value,
                None => {
                    return null_assignment_or_type_error(
                        expr,
                        params,
                        column,
                        "invalid uint16 assignment",
                    )
                }
            };
            let narrowed = u16::try_from(value).map_err(|_| {
                to_user_error(
                    "22003",
                    format!("value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::UInt16(Some(narrowed)))
        }
        TableColumnType::UInt32 => {
            let value = match extract_u64_expr(expr, params)? {
                Some(value) => value,
                None => {
                    return null_assignment_or_type_error(
                        expr,
                        params,
                        column,
                        "invalid uint32 assignment",
                    )
                }
            };
            let narrowed = u32::try_from(value).map_err(|_| {
                to_user_error(
                    "22003",
                    format!("value out of range for column '{}'", column.name),
                )
            })?;
            Ok(ScalarValue::UInt32(Some(narrowed)))
        }
        TableColumnType::UInt64 => {
            let value = match extract_u64_expr(expr, params)? {
                Some(value) => value,
                None => {
                    return null_assignment_or_type_error(
                        expr,
                        params,
                        column,
                        "invalid uint64 assignment",
                    )
                }
            };
            Ok(ScalarValue::UInt64(Some(value)))
        }
        TableColumnType::Float64 => {
            let value = match extract_f64_expr(expr, params)? {
                Some(value) => value,
                None => {
                    return null_assignment_or_type_error(
                        expr,
                        params,
                        column,
                        "invalid float64 assignment",
                    )
                }
            };
            Ok(ScalarValue::Float64(Some(value)))
        }
        TableColumnType::Boolean => {
            let value = match extract_bool_expr(expr, params)? {
                Some(value) => value,
                None => {
                    return null_assignment_or_type_error(
                        expr,
                        params,
                        column,
                        "invalid boolean assignment",
                    )
                }
            };
            Ok(ScalarValue::Boolean(Some(value)))
        }
        TableColumnType::Utf8 => {
            let value = extract_optional_string_expr(expr, params)?;
            match value {
                Some(v) => Ok(ScalarValue::Utf8(Some(v))),
                None if column.nullable => Ok(ScalarValue::Null),
                None => Err(nullable_error()),
            }
        }
        TableColumnType::TimestampNanosecond => {
            let value = extract_optional_timestamp_ns_expr(expr, params)?;
            match value {
                Some(v) => Ok(ScalarValue::TimestampNanosecond(Some(v), None)),
                None if column.nullable => Ok(ScalarValue::Null),
                None => Err(nullable_error()),
            }
        }
    }
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

/// Executes `extract u64 expr` for this component.
fn extract_u64_expr(expr: &SqlExpr, params: &ParamValues) -> PgWireResult<Option<u64>> {
    match expr {
        SqlExpr::Value(value) => scalar_to_u64(resolve_sql_value(value, params)?)
            .ok_or_else(|| to_user_error("22023", "invalid uint64 literal"))
            .map(Some),
        SqlExpr::UnaryOp { op, expr } => {
            let Some(value) = extract_u64_expr(expr, params)? else {
                return Ok(None);
            };
            match op {
                UnaryOperator::Plus => Ok(Some(value)),
                UnaryOperator::Minus if value == 0 => Ok(Some(0)),
                _ => Ok(None),
            }
        }
        SqlExpr::Nested(inner) => extract_u64_expr(inner, params),
        SqlExpr::Cast { expr, .. } => extract_u64_expr(expr, params),
        _ => Ok(None),
    }
}

/// Executes `extract f64 expr` for this component.
fn extract_f64_expr(expr: &SqlExpr, params: &ParamValues) -> PgWireResult<Option<f64>> {
    match expr {
        SqlExpr::Value(value) => scalar_to_f64(resolve_sql_value(value, params)?)
            .ok_or_else(|| to_user_error("22023", "invalid float literal"))
            .map(Some),
        SqlExpr::UnaryOp { op, expr } => {
            let Some(value) = extract_f64_expr(expr, params)? else {
                return Ok(None);
            };
            match op {
                UnaryOperator::Plus => Ok(Some(value)),
                UnaryOperator::Minus => Ok(Some(-value)),
                _ => Ok(None),
            }
        }
        SqlExpr::Nested(inner) => extract_f64_expr(inner, params),
        SqlExpr::Cast { expr, .. } => extract_f64_expr(expr, params),
        _ => Ok(None),
    }
}

/// Executes `extract bool expr` for this component.
fn extract_bool_expr(expr: &SqlExpr, params: &ParamValues) -> PgWireResult<Option<bool>> {
    match expr {
        SqlExpr::Value(value) => scalar_to_bool(resolve_sql_value(value, params)?)
            .ok_or_else(|| to_user_error("22023", "invalid boolean literal"))
            .map(Some),
        SqlExpr::Nested(inner) => extract_bool_expr(inner, params),
        SqlExpr::Cast { expr, .. } => extract_bool_expr(expr, params),
        _ => Ok(None),
    }
}

/// Executes `extract optional timestamp ns expr` for this component.
fn extract_optional_timestamp_ns_expr(
    expr: &SqlExpr,
    params: &ParamValues,
) -> PgWireResult<Option<i64>> {
    match expr {
        SqlExpr::Value(value) => {
            let resolved = resolve_sql_value(value, params)?;
            if matches!(resolved, ScalarValue::Null) {
                Ok(None)
            } else {
                scalar_to_timestamp_ns(resolved).map(Some)
            }
        }
        SqlExpr::TypedString { data_type, value } => {
            if !is_timestamp_data_type(data_type) {
                return Err(to_user_error(
                    "22023",
                    format!("unsupported typed literal for timestamp: {data_type}"),
                ));
            }
            parse_timestamp_ns_from_string(value.value.to_string().trim_matches('\'')).map(Some)
        }
        SqlExpr::Nested(inner) => extract_optional_timestamp_ns_expr(inner, params),
        SqlExpr::Cast { expr, .. } => extract_optional_timestamp_ns_expr(expr, params),
        _ => Err(to_user_error(
            "22023",
            "timestamp assignment must be a timestamp literal",
        )),
    }
}

/// Resolves null-assignment behavior for typed row_v1 UPDATE expressions.
fn null_assignment_or_type_error(
    expr: &SqlExpr,
    params: &ParamValues,
    column: &TableColumnRecord,
    type_error_message: &'static str,
) -> PgWireResult<ScalarValue> {
    if is_null_assignment_expr(expr, params)? {
        if column.nullable {
            return Ok(ScalarValue::Null);
        }
        return Err(to_user_error(
            "23502",
            format!(
                "null value in column '{}' violates not-null constraint",
                column.name
            ),
        ));
    }
    Err(to_user_error("22023", type_error_message))
}

/// Returns `true` when assignment expression resolves to SQL NULL.
fn is_null_assignment_expr(expr: &SqlExpr, params: &ParamValues) -> PgWireResult<bool> {
    match expr {
        SqlExpr::Value(value) => Ok(matches!(
            resolve_sql_value(value, params)?,
            ScalarValue::Null
        )),
        SqlExpr::Nested(inner) => is_null_assignment_expr(inner, params),
        SqlExpr::Cast { expr, .. } => is_null_assignment_expr(expr, params),
        _ => Ok(false),
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

/// Executes `scalar to u64` for this component.
fn scalar_to_u64(value: ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::UInt64(v) => v,
        ScalarValue::UInt32(v) => v.map(u64::from),
        ScalarValue::UInt16(v) => v.map(u64::from),
        ScalarValue::UInt8(v) => v.map(u64::from),
        ScalarValue::Int64(v) => v.and_then(|v| u64::try_from(v).ok()),
        ScalarValue::Int32(v) => v.and_then(|v| u64::try_from(v).ok()),
        ScalarValue::Int16(v) => v.and_then(|v| u64::try_from(v).ok()),
        ScalarValue::Int8(v) => v.and_then(|v| u64::try_from(v).ok()),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => v.parse::<u64>().ok(),
        _ => None,
    }
}

/// Executes `scalar to f64` for this component.
fn scalar_to_f64(value: ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(v) => v,
        ScalarValue::Float32(v) => v.map(f64::from),
        ScalarValue::Int64(v) => v.map(|v| v as f64),
        ScalarValue::Int32(v) => v.map(|v| v as f64),
        ScalarValue::Int16(v) => v.map(|v| v as f64),
        ScalarValue::Int8(v) => v.map(|v| v as f64),
        ScalarValue::UInt64(v) => v.map(|v| v as f64),
        ScalarValue::UInt32(v) => v.map(|v| v as f64),
        ScalarValue::UInt16(v) => v.map(|v| v as f64),
        ScalarValue::UInt8(v) => v.map(|v| v as f64),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => v.parse::<f64>().ok(),
        _ => None,
    }
}

/// Executes `scalar to bool` for this component.
fn scalar_to_bool(value: ScalarValue) -> Option<bool> {
    match value {
        ScalarValue::Boolean(v) => v,
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
            match v.trim().to_ascii_lowercase().as_str() {
                "true" | "t" | "1" => Some(true),
                "false" | "f" | "0" => Some(false),
                _ => None,
            }
        }
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

/// Returns current wall-clock nanoseconds since Unix epoch.
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
    let message = message.into();
    if is_duplicate_key_violation_message(message.as_str()) {
        return to_user_error("23505", extract_duplicate_key_message(message.as_str()));
    }
    if is_overload_error_message(message.as_str()) {
        return to_user_error("53300", message);
    }
    PgWireError::ApiError(Box::new(std::io::Error::other(message)))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_explain_placement_detects_key_stages() {
        let lines = vec![
            "TableScan: orders".to_string(),
            "ProjectionExec: expr=[order_id@0]".to_string(),
            "AggregateExec: mode=Partial".to_string(),
            "AggregateExec: mode=Final".to_string(),
            "HashJoinExec: mode=Partitioned".to_string(),
            "RepartitionExec: partitioning=Hash([order_id@0], 8)".to_string(),
            "SortExec: fetch=10".to_string(),
        ];

        let rows = classify_explain_placement(lines.as_slice(), "q0000000000000001");
        assert!(rows.iter().any(|row| row.stage == "query"));
        assert!(rows.iter().any(|row| row.stage == "scan"));
        assert!(rows.iter().any(|row| row.stage == "aggregate_partial"));
        assert!(rows.iter().any(|row| row.stage == "aggregate_final"));
        assert!(rows.iter().any(|row| row.stage == "join"));
        assert!(rows.iter().any(|row| row.stage == "exchange"));
        assert!(rows.iter().any(|row| row.stage == "topk"));
        assert!(
            rows.iter().all(|row| row.stage_id > 0),
            "expected all placement rows to include stage ids"
        );
    }

    #[test]
    fn parse_explain_request_supports_dist_and_format_json_options() {
        let dialect = PostgreSqlDialect {};
        let mut statements =
            Parser::parse_sql(&dialect, "EXPLAIN (ANALYZE, DIST, FORMAT JSON) SELECT 1;")
                .expect("parse explain statement");
        assert_eq!(statements.len(), 1);
        let statement = statements.remove(0);
        let explain = parse_explain_request(&statement).expect("parse request");
        assert!(explain.analyze);
        assert!(explain.dist);
        assert_eq!(explain.format, ExplainOutputFormat::Json);
    }

    #[test]
    fn parse_explain_request_defaults_to_standard_text_mode() {
        let dialect = PostgreSqlDialect {};
        let mut statements =
            Parser::parse_sql(&dialect, "EXPLAIN SELECT 1;").expect("parse explain statement");
        assert_eq!(statements.len(), 1);
        let statement = statements.remove(0);
        let explain = parse_explain_request(&statement).expect("parse request");
        assert!(!explain.analyze);
        assert!(!explain.dist);
        assert_eq!(explain.format, ExplainOutputFormat::Text);
    }

    #[test]
    fn build_explain_json_document_omits_optimizer_trace_when_not_verbose() {
        let explain = ExplainRequest {
            analyze: true,
            verbose: false,
            dist: false,
            format: ExplainOutputFormat::Json,
            explained_sql: "SELECT 1".to_string(),
        };
        let timeline = vec![crate::metrics::QueryStageEvent {
            query_execution_id: "q1".to_string(),
            stage_id: 42,
            kind: "optimizer_plan_decision".to_string(),
            detail:
                "reason=lowest_total_cost winner=table_scan winner_cost=4272.866 candidate_count=2"
                    .to_string(),
            at_unix_ns: 100,
        }];
        let payload = build_explain_json_document(
            "q1",
            &explain,
            &["Plan with Metrics".to_string()],
            timeline.as_slice(),
        );
        let value: serde_json::Value = serde_json::from_str(payload.as_str()).expect("json value");
        let plan = value
            .as_array()
            .and_then(|items| items.first())
            .and_then(|item| item.get("Plan"))
            .expect("plan object");
        assert!(
            plan.get("optimizer_trace").is_none(),
            "non-verbose explain output should not include optimizer_trace"
        );
    }

    #[test]
    fn build_explain_json_document_includes_optimizer_trace_when_verbose() {
        let explain = ExplainRequest {
            analyze: true,
            verbose: true,
            dist: false,
            format: ExplainOutputFormat::Json,
            explained_sql: "SELECT 1".to_string(),
        };
        let timeline = vec![
            crate::metrics::QueryStageEvent {
                query_execution_id: "q1".to_string(),
                stage_id: 42,
                kind: "optimizer_plan_decision".to_string(),
                detail: "reason=lowest_total_cost winner=table_scan winner_cost=4272.866 runner_up=index_only:idx_sales_status_day_merchant_cover runner_up_cost=44107.980 cost_delta=39835.114 cost_ratio=10.322 candidate_count=2".to_string(),
                at_unix_ns: 100,
            },
            crate::metrics::QueryStageEvent {
                query_execution_id: "q1".to_string(),
                stage_id: 42,
                kind: "optimizer_candidate_eval".to_string(),
                detail: "rank=1 name=table_scan total_cost=4272.866 estimated_output_rows=2459.00 estimated_scan_rows=4096.00 uncertainty=0.024 feedback_multiplier=1.000".to_string(),
                at_unix_ns: 101,
            },
        ];
        let payload = build_explain_json_document(
            "q1",
            &explain,
            &["Plan with Metrics".to_string()],
            timeline.as_slice(),
        );
        let value: serde_json::Value = serde_json::from_str(payload.as_str()).expect("json value");
        let plan = value
            .as_array()
            .and_then(|items| items.first())
            .and_then(|item| item.get("Plan"))
            .expect("plan object");
        let trace = plan
            .get("optimizer_trace")
            .and_then(|node| node.as_array())
            .expect("optimizer trace array");
        assert_eq!(trace.len(), 2);
        assert_eq!(trace[0]["fields"]["reason"], "lowest_total_cost");
        assert_eq!(trace[0]["fields"]["candidate_count"], serde_json::json!(2));
        assert!(
            trace[0]["fields"]["winner_cost"]
                .as_f64()
                .expect("winner cost")
                > 4272.8
        );
        assert_eq!(trace[1]["fields"]["rank"], serde_json::json!(1));
    }

    #[test]
    fn build_explain_optimizer_trace_json_preserves_full_path_field() {
        let timeline = vec![crate::metrics::QueryStageEvent {
            query_execution_id: "q1".to_string(),
            stage_id: 9,
            kind: "optimizer_plan_choice".to_string(),
            detail: "path=index_scan[index=idx_sales_status_day_merchant_cover prefix_columns=2 probes=4 covering=true ordered=false] estimated_output_rows=10.00".to_string(),
            at_unix_ns: 88,
        }];
        let trace = build_explain_optimizer_trace_json(timeline.as_slice());
        assert_eq!(trace.len(), 1);
        assert_eq!(
            trace[0]["fields"]["path"],
            "index_scan[index=idx_sales_status_day_merchant_cover prefix_columns=2 probes=4 covering=true ordered=false]"
        );
    }

    #[test]
    fn derive_explain_access_path_annotation_prefers_latest_plan_choice() {
        let timeline = vec![
            crate::metrics::QueryStageEvent {
                query_execution_id: "q1".to_string(),
                stage_id: 1,
                kind: "optimizer_plan_choice".to_string(),
                detail: "path=table_scan estimated_output_rows=100".to_string(),
                at_unix_ns: 1,
            },
            crate::metrics::QueryStageEvent {
                query_execution_id: "q1".to_string(),
                stage_id: 1,
                kind: "optimizer_plan_choice".to_string(),
                detail: "path=index_scan[index=idx_sales_status_day_merchant_cover prefix_columns=1 probes=1 covering=true ordered=false] estimated_output_rows=10".to_string(),
                at_unix_ns: 2,
            },
        ];
        let line = derive_explain_access_path_annotation(timeline.as_slice())
            .expect("access path annotation");
        assert_eq!(
            line,
            "AccessPath: index_only index=idx_sales_status_day_merchant_cover"
        );
    }

    #[test]
    fn render_access_path_summary_formats_table_and_index_modes() {
        assert_eq!(render_access_path_summary("table_scan"), "table_scan");
        assert_eq!(
            render_access_path_summary(
                "index_scan[index=idx_orders covering=false prefix_columns=1 probes=1 ordered=false]"
            ),
            "index_scan index=idx_orders"
        );
        assert_eq!(
            render_access_path_summary(
                "index_scan[index=idx_orders covering=true prefix_columns=1 probes=1 ordered=false]"
            ),
            "index_only index=idx_orders"
        );
    }

    #[test]
    fn extract_fast_grouped_aggregate_topk_supports_sales_shape() {
        let dialect = PostgreSqlDialect {};
        let sql =
            "SELECT merchant_id, event_day, COUNT(*) AS orders, SUM(amount_cents) AS gross_cents
                   FROM sales_facts
                   WHERE event_day = 1 AND status IN ('paid', 'shipped')
                   GROUP BY merchant_id, event_day
                   HAVING COUNT(*) >= 1
                   ORDER BY gross_cents DESC
                   LIMIT 200;";
        let mut statements = Parser::parse_sql(&dialect, sql).expect("parse grouped query");
        let statement = statements.remove(0);
        let params = ParamValues::List(Vec::new());
        let spec = extract_fast_grouped_aggregate_topk_statement(&statement, &params)
            .expect("expected grouped aggregate top-k fast path");
        assert_eq!(spec.table_name, "sales_facts");
        assert_eq!(
            spec.group_columns,
            vec!["merchant_id".to_string(), "event_day".to_string()]
        );
        assert_eq!(spec.sum_column, "amount_cents");
        assert_eq!(spec.having_min_count, 1);
        assert_eq!(spec.limit, 200);
        assert_eq!(spec.filters.len(), 2);
    }

    #[test]
    fn extract_fast_grouped_aggregate_topk_rejects_missing_limit() {
        let dialect = PostgreSqlDialect {};
        let sql =
            "SELECT merchant_id, event_day, COUNT(*) AS orders, SUM(amount_cents) AS gross_cents
                   FROM sales_facts
                   WHERE event_day = 1
                   GROUP BY merchant_id, event_day
                   HAVING COUNT(*) >= 1
                   ORDER BY gross_cents DESC;";
        let mut statements = Parser::parse_sql(&dialect, sql).expect("parse grouped query");
        let statement = statements.remove(0);
        let params = ParamValues::List(Vec::new());
        assert!(
            extract_fast_grouped_aggregate_topk_statement(&statement, &params).is_none(),
            "query without LIMIT should stay on standard planner path"
        );
    }
}
