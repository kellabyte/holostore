//! Catalog bootstrap and synchronization from HoloStore metadata.
//!
//! This module translates persisted table metadata records into registered
//! DataFusion table providers at node startup and during periodic sync.

use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use holo_store::HoloStoreClient;
use tracing::warn;

use crate::metadata::{is_supported_table_model, list_table_metadata, TableMetadataRecord};
use crate::metrics::PushdownMetrics;
use crate::provider::HoloStoreTableProvider;

/// Summary of tables registered during one bootstrap pass.
#[derive(Debug, Clone)]
pub struct CatalogBootstrapResult {
    /// Fully qualified table names that were successfully registered.
    pub registered_tables: Vec<String>,
}

/// Loads table metadata from storage and registers each supported table.
pub async fn bootstrap_catalog(
    session_context: &SessionContext,
    holostore_client: HoloStoreClient,
    pushdown_metrics: Arc<PushdownMetrics>,
) -> Result<CatalogBootstrapResult> {
    let table_metadata = list_table_metadata(&holostore_client)
        .await
        .context("load table metadata from holostore")?;

    let mut registered_tables = Vec::new();
    for table in table_metadata {
        // Decision: include table names only when registration happened in this pass.
        if register_table_from_metadata(
            session_context,
            holostore_client.clone(),
            pushdown_metrics.clone(),
            &table,
        )
        .await?
        {
            registered_tables.push(table.table_name.clone());
        }
    }

    Ok(CatalogBootstrapResult { registered_tables })
}

/// Registers one table described by persisted metadata.
///
/// Returns `Ok(true)` if registration happened in this call and `Ok(false)` if
/// the table was skipped or already present.
pub async fn register_table_from_metadata(
    session_context: &SessionContext,
    holostore_client: HoloStoreClient,
    pushdown_metrics: Arc<PushdownMetrics>,
    table: &TableMetadataRecord,
) -> Result<bool> {
    // Decision: skip unsupported models to keep startup resilient across versions.
    if !is_supported_table_model(&table.table_model) {
        warn!(
            table_name = %table.table_name,
            table_model = %table.table_model,
            "skipping unsupported table model"
        );
        return Ok(false);
    }

    // Decision: treat already-present providers as idempotent no-op.
    if session_context
        .table_provider(table.table_name.as_str())
        .await
        .is_ok()
    {
        return Ok(false);
    }

    let provider = Arc::new(
        HoloStoreTableProvider::from_table_metadata(table, holostore_client, pushdown_metrics)
            .with_context(|| format!("build provider for table {}", table.table_name))?,
    );

    // Decision: map concurrent registrar races to `Ok(false)` instead of failure.
    match session_context.register_table(table.table_name.as_str(), provider) {
        Ok(_) => Ok(true),
        Err(err) => {
            // Decision: if table appears now, another registrar won the race.
            if session_context
                .table_provider(table.table_name.as_str())
                .await
                .is_ok()
            {
                // Another concurrent registrar inserted the same table.
                Ok(false)
            } else {
                Err(err).with_context(|| format!("register table {}", table.table_name))
            }
        }
    }
}

/// Periodically used helper that registers newly discovered metadata tables.
pub async fn sync_catalog_from_metadata(
    session_context: &SessionContext,
    holostore_client: HoloStoreClient,
    pushdown_metrics: Arc<PushdownMetrics>,
) -> Result<Vec<String>> {
    let table_metadata = list_table_metadata(&holostore_client)
        .await
        .context("load table metadata for catalog sync")?;

    let mut newly_registered = Vec::new();
    for table in table_metadata {
        // Decision: track only newly registered tables to keep sync output meaningful.
        if register_table_from_metadata(
            session_context,
            holostore_client.clone(),
            pushdown_metrics.clone(),
            &table,
        )
        .await?
        {
            newly_registered.push(table.table_name.clone());
        }
    }
    Ok(newly_registered)
}
