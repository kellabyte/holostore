//! Extra PostgreSQL catalog compatibility tables layered on top of `pg_catalog`.
//!
//! This module adds `pg_catalog.pg_locks` backed by live HoloFusion transaction
//! state so IDEs and tooling can run lock/introspection queries without
//! requiring a full upstream Postgres backend.

use std::any::Any;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result as AnyResult};
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::MemTable;
use datafusion::catalog::{SchemaProvider, Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::mutation::{TxnIntrospection, TxnLifecycleState};

const PG_LOCKS_TABLE_NAME: &str = "pg_locks";

/// Installs `pg_catalog.pg_locks` by layering a schema provider over `pg_catalog`.
pub fn install_pg_locks_table(
    session_context: &SessionContext,
    catalog_name: &str,
    txn_introspection: TxnIntrospection,
) -> AnyResult<()> {
    let catalog = session_context
        .catalog(catalog_name)
        .ok_or_else(|| anyhow!("catalog '{}' not found", catalog_name))?;
    let base_pg_catalog = catalog.schema("pg_catalog").ok_or_else(|| {
        anyhow!(
            "schema 'pg_catalog' not found in catalog '{}'",
            catalog_name
        )
    })?;

    if base_pg_catalog
        .as_any()
        .downcast_ref::<PgCatalogOverlaySchemaProvider>()
        .is_some()
    {
        return Ok(());
    }

    let pg_locks = Arc::new(PgLocksTableProvider::new(txn_introspection));
    let overlay = Arc::new(PgCatalogOverlaySchemaProvider::new(
        base_pg_catalog,
        pg_locks,
    ));
    catalog
        .register_schema("pg_catalog", overlay)
        .context("register pg_catalog overlay schema")?;
    Ok(())
}

#[derive(Debug)]
/// Wraps an existing `pg_catalog` provider while adding HoloFusion extras.
struct PgCatalogOverlaySchemaProvider {
    base: Arc<dyn SchemaProvider>,
    pg_locks: Arc<PgLocksTableProvider>,
}

impl PgCatalogOverlaySchemaProvider {
    /// Creates a new overlay provider.
    fn new(base: Arc<dyn SchemaProvider>, pg_locks: Arc<PgLocksTableProvider>) -> Self {
        Self { base, pg_locks }
    }
}

#[async_trait]
impl SchemaProvider for PgCatalogOverlaySchemaProvider {
    /// Returns this provider as `Any` for downcasting.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Lists schema table names, including overlay extras.
    fn table_names(&self) -> Vec<String> {
        let mut names = self.base.table_names();
        if !names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(PG_LOCKS_TABLE_NAME))
        {
            names.push(PG_LOCKS_TABLE_NAME.to_string());
        }
        names
    }

    /// Resolves one table by name.
    async fn table(
        &self,
        name: &str,
    ) -> std::result::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if name.eq_ignore_ascii_case(PG_LOCKS_TABLE_NAME) {
            return Ok(Some(self.pg_locks.clone()));
        }
        self.base.table(name).await
    }

    /// Reports table existence, including overlay extras.
    fn table_exist(&self, name: &str) -> bool {
        if name.eq_ignore_ascii_case(PG_LOCKS_TABLE_NAME) {
            true
        } else {
            self.base.table_exist(name)
        }
    }
}

#[derive(Debug, Clone)]
/// Backing table provider for `pg_catalog.pg_locks`.
struct PgLocksTableProvider {
    schema: SchemaRef,
    txn_introspection: TxnIntrospection,
}

impl PgLocksTableProvider {
    /// Creates a new provider bound to live transaction introspection.
    fn new(txn_introspection: TxnIntrospection) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("locktype", DataType::Utf8, false),
            Field::new("database", DataType::Int32, true),
            Field::new("relation", DataType::Int32, true),
            Field::new("page", DataType::Int32, true),
            Field::new("tuple", DataType::Int16, true),
            Field::new("virtualxid", DataType::Utf8, true),
            Field::new("transactionid", DataType::Int64, true),
            Field::new("classid", DataType::Int32, true),
            Field::new("objid", DataType::Int32, true),
            Field::new("objsubid", DataType::Int16, true),
            Field::new("virtualtransaction", DataType::Utf8, true),
            Field::new("pid", DataType::Int32, true),
            Field::new("mode", DataType::Utf8, false),
            Field::new("granted", DataType::Boolean, false),
            Field::new("fastpath", DataType::Boolean, false),
            Field::new(
                "waitstart",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        Self {
            schema,
            txn_introspection,
        }
    }
}

#[derive(Debug, Clone)]
/// One row in the `pg_locks` compatibility table.
struct PgLockRow {
    locktype: String,
    virtualxid: Option<String>,
    transactionid: Option<i64>,
    virtualtransaction: Option<String>,
    pid: Option<i32>,
    mode: String,
    granted: bool,
    fastpath: bool,
}

/// Converts internal transaction snapshots into `pg_locks` rows.
fn pg_locks_rows_from_transactions(txns: &[crate::mutation::ActiveTxnSnapshot]) -> Vec<PgLockRow> {
    let mut rows = Vec::with_capacity(txns.len().saturating_mul(2));
    for txn in txns {
        let virtual_xid = format!("{}/{}", txn.session_seq.max(1), txn.txid.max(1));
        let lock_mode = match txn.state {
            TxnLifecycleState::Active => "ExclusiveLock",
            TxnLifecycleState::Aborted => "ExclusiveLock",
        }
        .to_string();
        rows.push(PgLockRow {
            locktype: "virtualxid".to_string(),
            virtualxid: Some(virtual_xid.clone()),
            transactionid: None,
            virtualtransaction: Some(virtual_xid.clone()),
            pid: Some(txn.backend_pid),
            mode: lock_mode.clone(),
            granted: true,
            fastpath: false,
        });
        rows.push(PgLockRow {
            locktype: "transactionid".to_string(),
            virtualxid: None,
            transactionid: Some(txn.txid as i64),
            virtualtransaction: Some(virtual_xid),
            pid: Some(txn.backend_pid),
            mode: lock_mode,
            granted: true,
            fastpath: false,
        });
    }

    rows.sort_by(|left, right| {
        left.pid
            .cmp(&right.pid)
            .then_with(|| left.transactionid.cmp(&right.transactionid))
            .then_with(|| left.locktype.cmp(&right.locktype))
    });
    rows
}

/// Encodes `pg_locks` rows into an Arrow batch.
fn pg_locks_rows_to_batch(schema: SchemaRef, rows: &[PgLockRow]) -> DFResult<RecordBatch> {
    let mut locktype = StringBuilder::new();
    let mut database = Int32Builder::new();
    let mut relation = Int32Builder::new();
    let mut page = Int32Builder::new();
    let mut tuple = Int16Builder::new();
    let mut virtualxid = StringBuilder::new();
    let mut transactionid = Int64Builder::new();
    let mut classid = Int32Builder::new();
    let mut objid = Int32Builder::new();
    let mut objsubid = Int16Builder::new();
    let mut virtualtransaction = StringBuilder::new();
    let mut pid = Int32Builder::new();
    let mut mode = StringBuilder::new();
    let mut granted = BooleanBuilder::new();
    let mut fastpath = BooleanBuilder::new();
    let mut waitstart = TimestampNanosecondBuilder::new();

    for row in rows {
        locktype.append_value(row.locktype.as_str());
        database.append_null();
        relation.append_null();
        page.append_null();
        tuple.append_null();
        if let Some(value) = row.virtualxid.as_deref() {
            virtualxid.append_value(value);
        } else {
            virtualxid.append_null();
        }
        if let Some(value) = row.transactionid {
            transactionid.append_value(value);
        } else {
            transactionid.append_null();
        }
        classid.append_null();
        objid.append_null();
        objsubid.append_null();
        if let Some(value) = row.virtualtransaction.as_deref() {
            virtualtransaction.append_value(value);
        } else {
            virtualtransaction.append_null();
        }
        if let Some(value) = row.pid {
            pid.append_value(value);
        } else {
            pid.append_null();
        }
        mode.append_value(row.mode.as_str());
        granted.append_value(row.granted);
        fastpath.append_value(row.fastpath);
        waitstart.append_null();
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(locktype.finish()),
        Arc::new(database.finish()),
        Arc::new(relation.finish()),
        Arc::new(page.finish()),
        Arc::new(tuple.finish()),
        Arc::new(virtualxid.finish()),
        Arc::new(transactionid.finish()),
        Arc::new(classid.finish()),
        Arc::new(objid.finish()),
        Arc::new(objsubid.finish()),
        Arc::new(virtualtransaction.finish()),
        Arc::new(pid.finish()),
        Arc::new(mode.finish()),
        Arc::new(granted.finish()),
        Arc::new(fastpath.finish()),
        Arc::new(waitstart.finish()),
    ];
    Ok(RecordBatch::try_new(schema, arrays)?)
}

#[async_trait]
impl TableProvider for PgLocksTableProvider {
    /// Returns this provider as `Any` for downcasting.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the table schema.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Exposes `pg_locks` as a system-style view.
    fn table_type(&self) -> TableType {
        TableType::View
    }

    /// Scans current lock rows from live transaction state.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if matches!(limit, Some(0)) {
            let mem = MemTable::try_new(
                self.schema(),
                vec![vec![RecordBatch::new_empty(self.schema())]],
            )?;
            return mem.scan(state, projection, &[], limit).await;
        }

        let snapshot = self.txn_introspection.snapshot_active_transactions().await;
        let rows = pg_locks_rows_from_transactions(snapshot.as_slice());
        let batch = pg_locks_rows_to_batch(self.schema(), rows.as_slice())?;
        let mem = MemTable::try_new(self.schema(), vec![vec![batch]])?;
        mem.scan(state, projection, &[], limit).await
    }

    /// Reports unsupported pushdown for compatibility table filters.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    /// Advertises a compatibility DDL representation.
    fn get_table_definition(&self) -> Option<&str> {
        Some("CREATE VIEW pg_catalog.pg_locks AS SELECT lock metadata")
    }
}
