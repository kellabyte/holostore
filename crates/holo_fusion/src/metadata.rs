//! Table-metadata persistence and lookup on top of HoloStore keyspace.
//!
//! Metadata rows describe logical SQL tables and are used to bootstrap
//! `TableProvider` instances on each HoloFusion node.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use holo_store::{HoloStoreClient, ReplicatedConditionalWriteEntry, RpcVersion};
use serde::{Deserialize, Serialize};

use crate::topology::{fetch_topology, require_non_empty_topology, route_key, scan_targets};

/// Keyspace prefix for table metadata records.
const META_PREFIX_TABLE: u8 = 0x03;
/// Default catalog/database id currently used by HoloFusion.
const DEFAULT_DB_ID: u64 = 1;
/// Default schema id currently used by HoloFusion.
const DEFAULT_SCHEMA_ID: u64 = 1;
/// Storage model tag for the current orders table layout.
const TABLE_MODEL_ORDERS_V1: &str = "orders_v1";
/// Page size used when scanning metadata rows.
const DEFAULT_SCAN_PAGE_SIZE: usize = 1024;
/// Visibility wait timeout after metadata writes.
const METADATA_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(5);
/// Overall timeout budget for metadata creation retries.
const METADATA_CREATE_TIMEOUT: Duration = Duration::from_secs(15);
/// Delay between metadata creation retries under contention.
const METADATA_CREATE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// Result of attempting to create table metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableMetadataOutcome {
    /// Metadata record that exists after the operation.
    pub record: TableMetadataRecord,
    /// Whether this call created a new record (`true`) vs observed existing (`false`).
    pub created: bool,
}

/// Persisted metadata row describing one logical SQL table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        Ok(())
    }
}

/// Ensures metadata keyspace is reachable and returns current records.
pub async fn ensure_table_metadata(client: &HoloStoreClient) -> Result<Vec<TableMetadataRecord>> {
    list_table_metadata(client)
        .await
        .context("list table metadata")
}

/// Lists all table metadata records across relevant topology targets.
pub async fn list_table_metadata(client: &HoloStoreClient) -> Result<Vec<TableMetadataRecord>> {
    let topology = fetch_topology(client).await?;
    require_non_empty_topology(&topology)?;

    let start_key = vec![META_PREFIX_TABLE];
    let end_key = prefix_end(&start_key).unwrap_or_default();
    let targets = scan_targets(&topology, client.target(), &start_key, &end_key, &[]);

    let mut by_key = BTreeMap::<(u64, u64, u64), TableMetadataRecord>::new();
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
                let record: TableMetadataRecord = serde_json::from_slice(&entry.value)
                    .with_context(|| {
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
                by_key.insert((db_id, schema_id, table_id), record);
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
) -> Result<CreateTableMetadataOutcome> {
    let table_name = table_name.trim();
    // Decision: fail early on empty names to avoid creating unreachable entries.
    if table_name.is_empty() {
        return Err(anyhow!("table metadata create has empty table_name"));
    }

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
            table_model: TABLE_MODEL_ORDERS_V1.to_string(),
            preferred_shards: Vec::new(),
            page_size: default_page_size(),
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
    matches!(table_model, TABLE_MODEL_ORDERS_V1)
}

/// Returns the canonical `orders_v1` model tag.
pub fn table_model_orders_v1() -> &'static str {
    TABLE_MODEL_ORDERS_V1
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
