use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use holo_store::{HoloStoreClient, ReplicatedConditionalWriteEntry, RpcVersion};
use serde::{Deserialize, Serialize};

use crate::indexing::{index_tombstone_value, is_sql_tombstone_value};
use crate::metadata::TableMetadataRecord;
use crate::topology::{fetch_topology, require_non_empty_topology, route_key, scan_targets};

const META_PREFIX_SECONDARY_INDEX: u8 = 0x04;
const DEFAULT_INDEX_SCAN_PAGE_SIZE: usize = 1024;
const INDEX_METADATA_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(5);
const INDEX_METADATA_RETRY_LIMIT: usize = 64;
const INDEX_METADATA_RETRY_DELAY: Duration = Duration::from_millis(75);
const INDEX_METADATA_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);
const INDEX_METADATA_RETRY_MAX_ELAPSED: Duration = Duration::from_secs(30);

pub const DEFAULT_SECONDARY_INDEX_HASH_BUCKETS: usize = 32;
pub const MAX_SECONDARY_INDEX_HASH_BUCKETS: usize = u16::MAX as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SecondaryIndexDistribution {
    #[default]
    Range,
    Hash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SecondaryIndexState {
    #[default]
    WriteOnly,
    Public,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexRecord {
    pub db_id: u64,
    pub schema_id: u64,
    pub table_id: u64,
    pub index_id: u64,
    pub table_name: String,
    pub index_name: String,
    pub unique: bool,
    pub key_columns: Vec<String>,
    pub include_columns: Vec<String>,
    pub distribution: SecondaryIndexDistribution,
    pub hash_bucket_count: Option<usize>,
    pub state: SecondaryIndexState,
    #[serde(default)]
    pub created_at_unix_ms: u64,
    #[serde(default)]
    pub updated_at_unix_ms: u64,
}

impl SecondaryIndexRecord {
    pub fn validate(&self) -> Result<()> {
        if self.table_id == 0 {
            return Err(anyhow!("secondary index metadata has invalid table_id=0"));
        }
        if self.index_id == 0 {
            return Err(anyhow!("secondary index metadata has invalid index_id=0"));
        }
        if self.table_name.trim().is_empty() {
            return Err(anyhow!("secondary index metadata has empty table_name"));
        }
        if self.index_name.trim().is_empty() {
            return Err(anyhow!("secondary index metadata has empty index_name"));
        }
        if self.key_columns.is_empty() {
            return Err(anyhow!(
                "secondary index '{}' has no key columns",
                self.index_name
            ));
        }
        let mut key_seen = BTreeSet::<String>::new();
        for key in &self.key_columns {
            if key.trim().is_empty() {
                return Err(anyhow!(
                    "secondary index '{}' has empty key column",
                    self.index_name
                ));
            }
            let normalized = key.to_ascii_lowercase();
            if !key_seen.insert(normalized.clone()) {
                return Err(anyhow!(
                    "secondary index '{}' has duplicate key column '{}'",
                    self.index_name,
                    normalized
                ));
            }
        }
        let mut include_seen = BTreeSet::<String>::new();
        for column in &self.include_columns {
            if column.trim().is_empty() {
                return Err(anyhow!(
                    "secondary index '{}' has empty include column",
                    self.index_name
                ));
            }
            let normalized = column.to_ascii_lowercase();
            if key_seen.contains(&normalized) {
                return Err(anyhow!(
                    "secondary index '{}' include column '{}' duplicates key column",
                    self.index_name,
                    normalized
                ));
            }
            if !include_seen.insert(normalized.clone()) {
                return Err(anyhow!(
                    "secondary index '{}' has duplicate include column '{}'",
                    self.index_name,
                    normalized
                ));
            }
        }

        match self.distribution {
            SecondaryIndexDistribution::Range => {
                if self.hash_bucket_count.is_some() {
                    return Err(anyhow!(
                        "secondary index '{}' has hash bucket count for range distribution",
                        self.index_name
                    ));
                }
            }
            SecondaryIndexDistribution::Hash => {
                let buckets = self
                    .hash_bucket_count
                    .unwrap_or(DEFAULT_SECONDARY_INDEX_HASH_BUCKETS);
                if buckets == 0 || buckets > MAX_SECONDARY_INDEX_HASH_BUCKETS {
                    return Err(anyhow!(
                        "secondary index '{}' hash bucket count must be in [1, {}], got {}",
                        self.index_name,
                        MAX_SECONDARY_INDEX_HASH_BUCKETS,
                        buckets
                    ));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSecondaryIndexMetadataSpec {
    pub index_name: String,
    pub unique: bool,
    pub key_columns: Vec<String>,
    pub include_columns: Vec<String>,
    pub distribution: SecondaryIndexDistribution,
    pub hash_bucket_count: Option<usize>,
    pub if_not_exists: bool,
}

impl CreateSecondaryIndexMetadataSpec {
    pub fn validate_against_table(&self, table: &TableMetadataRecord) -> Result<()> {
        if self.index_name.trim().is_empty() {
            return Err(anyhow!("CREATE INDEX has empty index name"));
        }
        if self.key_columns.is_empty() {
            return Err(anyhow!("CREATE INDEX requires at least one key column"));
        }

        let by_name = table
            .columns
            .iter()
            .map(|column| (column.name.to_ascii_lowercase(), column))
            .collect::<BTreeMap<_, _>>();
        let mut key_seen = BTreeSet::<String>::new();
        for key in &self.key_columns {
            let normalized = key.to_ascii_lowercase();
            if !by_name.contains_key(normalized.as_str()) {
                return Err(anyhow!(
                    "CREATE INDEX column '{}' does not exist in table '{}'",
                    key,
                    table.table_name
                ));
            }
            if !key_seen.insert(normalized.clone()) {
                return Err(anyhow!("CREATE INDEX duplicate key column '{}'", key));
            }
        }

        let mut include_seen = BTreeSet::<String>::new();
        for column in &self.include_columns {
            let normalized = column.to_ascii_lowercase();
            if !by_name.contains_key(normalized.as_str()) {
                return Err(anyhow!(
                    "CREATE INDEX include column '{}' does not exist in table '{}'",
                    column,
                    table.table_name
                ));
            }
            if key_seen.contains(normalized.as_str()) {
                return Err(anyhow!(
                    "CREATE INDEX include column '{}' duplicates key column",
                    column
                ));
            }
            if !include_seen.insert(normalized) {
                return Err(anyhow!(
                    "CREATE INDEX duplicate include column '{}'",
                    column
                ));
            }
        }

        match self.distribution {
            SecondaryIndexDistribution::Range => {
                if self.hash_bucket_count.is_some() {
                    return Err(anyhow!(
                        "hash bucket count requires USING HASH distribution"
                    ));
                }
            }
            SecondaryIndexDistribution::Hash => {
                let buckets = self
                    .hash_bucket_count
                    .unwrap_or(DEFAULT_SECONDARY_INDEX_HASH_BUCKETS);
                if buckets == 0 || buckets > MAX_SECONDARY_INDEX_HASH_BUCKETS {
                    return Err(anyhow!(
                        "hash bucket count must be in [1, {}], got {}",
                        MAX_SECONDARY_INDEX_HASH_BUCKETS,
                        buckets
                    ));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSecondaryIndexMetadataOutcome {
    pub record: SecondaryIndexRecord,
    pub created: bool,
}

#[derive(Debug, Clone)]
struct SecondaryIndexMetadataEntry {
    key: Vec<u8>,
    version: RpcVersion,
    record: SecondaryIndexRecord,
}

pub async fn list_secondary_index_metadata(
    client: &HoloStoreClient,
) -> Result<Vec<SecondaryIndexRecord>> {
    let entries = list_secondary_index_entries(client).await?;
    Ok(entries.into_iter().map(|entry| entry.record).collect())
}

pub async fn list_secondary_index_metadata_for_table(
    client: &HoloStoreClient,
    table_id: u64,
) -> Result<Vec<SecondaryIndexRecord>> {
    let all = list_secondary_index_entries(client).await?;
    let mut out = all
        .into_iter()
        .filter(|entry| entry.record.table_id == table_id)
        .map(|entry| entry.record)
        .collect::<Vec<_>>();
    out.sort_by_key(|record| record.index_id);
    Ok(out)
}

pub async fn create_secondary_index_metadata(
    client: &HoloStoreClient,
    table: &TableMetadataRecord,
    spec: &CreateSecondaryIndexMetadataSpec,
) -> Result<CreateSecondaryIndexMetadataOutcome> {
    spec.validate_against_table(table)?;
    let retry_deadline = Instant::now() + INDEX_METADATA_RETRY_MAX_ELAPSED;
    let normalized_name = spec.index_name.to_ascii_lowercase();

    for attempt in 0..INDEX_METADATA_RETRY_LIMIT {
        let mut existing = list_secondary_index_metadata_for_table(client, table.table_id).await?;
        if let Some(record) = existing.iter().find(|record| {
            record
                .index_name
                .eq_ignore_ascii_case(normalized_name.as_str())
        }) {
            if spec.if_not_exists {
                return Ok(CreateSecondaryIndexMetadataOutcome {
                    record: record.clone(),
                    created: false,
                });
            }
            if attempt > 0 && secondary_index_record_matches_spec(record, spec) {
                // Decision: the index was absent on attempt 0 and appeared during
                // retries, which can happen when a prior conditional write was
                // committed but the client observed a retryable transport error.
                // Continue CREATE INDEX by treating write-only state as newly
                // created so the caller performs backfill/state transition.
                return Ok(CreateSecondaryIndexMetadataOutcome {
                    record: record.clone(),
                    created: record.state == SecondaryIndexState::WriteOnly,
                });
            }
            return Err(anyhow!(
                "index '{}' already exists on table '{}'",
                spec.index_name,
                table.table_name
            ));
        }

        existing.sort_by_key(|record| record.index_id);
        let next_index_id = existing
            .last()
            .map(|record| record.index_id.saturating_add(1))
            .unwrap_or(1);

        let now_ms = now_unix_epoch_millis();
        let record = SecondaryIndexRecord {
            db_id: table.db_id,
            schema_id: table.schema_id,
            table_id: table.table_id,
            index_id: next_index_id,
            table_name: table.table_name.clone(),
            index_name: normalized_name.clone(),
            unique: spec.unique,
            key_columns: spec
                .key_columns
                .iter()
                .map(|name| name.to_ascii_lowercase())
                .collect(),
            include_columns: spec
                .include_columns
                .iter()
                .map(|name| name.to_ascii_lowercase())
                .collect(),
            distribution: spec.distribution,
            hash_bucket_count: if spec.distribution == SecondaryIndexDistribution::Hash {
                Some(
                    spec.hash_bucket_count
                        .unwrap_or(DEFAULT_SECONDARY_INDEX_HASH_BUCKETS),
                )
            } else {
                None
            },
            state: SecondaryIndexState::WriteOnly,
            created_at_unix_ms: now_ms,
            updated_at_unix_ms: now_ms,
        };
        record.validate()?;

        let key = encode_secondary_index_metadata_key(
            record.db_id,
            record.schema_id,
            record.table_id,
            record.index_id,
        );
        let value = serde_json::to_vec(&record).context("encode secondary index metadata json")?;

        let topology = fetch_topology(client).await?;
        require_non_empty_topology(&topology)?;
        let Some(route) = route_key(&topology, client.target(), key.as_slice(), &[]) else {
            if should_retry_metadata_write(attempt, retry_deadline) {
                tokio::time::sleep(metadata_retry_delay(attempt)).await;
                continue;
            }
            return Err(anyhow!("failed to route secondary index metadata key"));
        };

        let write_client = HoloStoreClient::new(route.grpc_addr);
        let result = match write_client
            .range_write_latest_conditional(
                route.shard_index,
                route.start_key.as_slice(),
                route.end_key.as_slice(),
                vec![ReplicatedConditionalWriteEntry {
                    key: key.clone(),
                    value,
                    expected_version: RpcVersion::zero(),
                }],
            )
            .await
            .with_context(|| {
                format!(
                    "persist secondary index metadata '{}' on table '{}'",
                    record.index_name, record.table_name
                )
            }) {
            Ok(result) => result,
            Err(err) => {
                if is_retryable_metadata_write_error(&err)
                    && should_retry_metadata_write(attempt, retry_deadline)
                {
                    tokio::time::sleep(metadata_retry_delay(attempt)).await;
                    continue;
                }
                return Err(err);
            }
        };

        if result.applied == 1 && result.conflicts == 0 {
            let visible = wait_for_secondary_index_by_id(
                client,
                record.table_id,
                record.index_id,
                INDEX_METADATA_VISIBILITY_TIMEOUT,
            )
            .await?;
            if let Some(visible) = visible {
                return Ok(CreateSecondaryIndexMetadataOutcome {
                    record: visible,
                    created: true,
                });
            }
            return Err(anyhow!(
                "secondary index metadata write applied but not visible: table='{}' index='{}'",
                table.table_name,
                record.index_name
            ));
        }

        if should_retry_metadata_write(attempt, retry_deadline) {
            tokio::time::sleep(metadata_retry_delay(attempt)).await;
            continue;
        }
        break;
    }

    Err(anyhow!(
        "secondary index metadata create retries exhausted for table '{}' index '{}'",
        table.table_name,
        spec.index_name
    ))
}

pub async fn update_secondary_index_state(
    client: &HoloStoreClient,
    table_id: u64,
    index_id: u64,
    state: SecondaryIndexState,
) -> Result<SecondaryIndexRecord> {
    let retry_deadline = Instant::now() + INDEX_METADATA_RETRY_MAX_ELAPSED;
    for attempt in 0..INDEX_METADATA_RETRY_LIMIT {
        let Some(entry) = find_secondary_index_entry_by_id(client, table_id, index_id).await?
        else {
            return Err(anyhow!(
                "secondary index metadata missing for table_id={} index_id={}",
                table_id,
                index_id
            ));
        };
        if entry.record.state == state {
            return Ok(entry.record);
        }

        let mut updated = entry.record.clone();
        updated.state = state;
        updated.updated_at_unix_ms = now_unix_epoch_millis();
        updated.validate()?;

        let payload =
            serde_json::to_vec(&updated).context("encode updated secondary index metadata")?;

        let topology = fetch_topology(client).await?;
        require_non_empty_topology(&topology)?;
        let Some(route) = route_key(&topology, client.target(), entry.key.as_slice(), &[]) else {
            if should_retry_metadata_write(attempt, retry_deadline) {
                tokio::time::sleep(metadata_retry_delay(attempt)).await;
                continue;
            }
            return Err(anyhow!("failed to route secondary index metadata key"));
        };
        let write_client = HoloStoreClient::new(route.grpc_addr);
        let result = match write_client
            .range_write_latest_conditional(
                route.shard_index,
                route.start_key.as_slice(),
                route.end_key.as_slice(),
                vec![ReplicatedConditionalWriteEntry {
                    key: entry.key.clone(),
                    value: payload,
                    expected_version: entry.version,
                }],
            )
            .await
            .with_context(|| {
                format!(
                    "persist secondary index state update table_id={} index_id={} state={:?}",
                    table_id, index_id, state
                )
            }) {
            Ok(result) => result,
            Err(err) => {
                if is_retryable_metadata_write_error(&err)
                    && should_retry_metadata_write(attempt, retry_deadline)
                {
                    tokio::time::sleep(metadata_retry_delay(attempt)).await;
                    continue;
                }
                return Err(err);
            }
        };

        if result.applied == 1 && result.conflicts == 0 {
            let visible = wait_for_secondary_index_by_id(
                client,
                table_id,
                index_id,
                INDEX_METADATA_VISIBILITY_TIMEOUT,
            )
            .await?;
            if let Some(visible) = visible {
                if visible.state == state {
                    return Ok(visible);
                }
            }
        }

        if should_retry_metadata_write(attempt, retry_deadline) {
            tokio::time::sleep(metadata_retry_delay(attempt)).await;
            continue;
        }
        break;
    }

    Err(anyhow!(
        "secondary index state update retries exhausted for table_id={} index_id={}",
        table_id,
        index_id
    ))
}

pub async fn drop_secondary_index_metadata_by_name(
    client: &HoloStoreClient,
    index_name: &str,
    if_exists: bool,
) -> Result<bool> {
    let normalized = index_name.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(anyhow!("DROP INDEX has empty index name"));
    }
    let retry_deadline = Instant::now() + INDEX_METADATA_RETRY_MAX_ELAPSED;

    for attempt in 0..INDEX_METADATA_RETRY_LIMIT {
        let entries = list_secondary_index_entries(client).await?;
        let mut matches = entries
            .into_iter()
            .filter(|entry| entry.record.index_name == normalized)
            .collect::<Vec<_>>();

        if matches.is_empty() {
            if if_exists {
                return Ok(false);
            }
            return Err(anyhow!("index '{}' does not exist", normalized));
        }
        if matches.len() > 1 {
            return Err(anyhow!(
                "index name '{}' is ambiguous; qualify by table before dropping",
                normalized
            ));
        }

        let entry = matches.pop().expect("validated len=1");

        let topology = fetch_topology(client).await?;
        require_non_empty_topology(&topology)?;
        let Some(route) = route_key(&topology, client.target(), entry.key.as_slice(), &[]) else {
            if should_retry_metadata_write(attempt, retry_deadline) {
                tokio::time::sleep(metadata_retry_delay(attempt)).await;
                continue;
            }
            return Err(anyhow!(
                "failed to route secondary index metadata key for drop"
            ));
        };
        let write_client = HoloStoreClient::new(route.grpc_addr);
        let result = match write_client
            .range_write_latest_conditional(
                route.shard_index,
                route.start_key.as_slice(),
                route.end_key.as_slice(),
                vec![ReplicatedConditionalWriteEntry {
                    key: entry.key.clone(),
                    value: index_tombstone_value(),
                    expected_version: entry.version,
                }],
            )
            .await
            .with_context(|| {
                format!(
                    "drop secondary index metadata '{}' on table '{}'",
                    entry.record.index_name, entry.record.table_name
                )
            }) {
            Ok(result) => result,
            Err(err) => {
                if is_retryable_metadata_write_error(&err)
                    && should_retry_metadata_write(attempt, retry_deadline)
                {
                    tokio::time::sleep(metadata_retry_delay(attempt)).await;
                    continue;
                }
                return Err(err);
            }
        };

        if result.applied == 1 && result.conflicts == 0 {
            return Ok(true);
        }

        if should_retry_metadata_write(attempt, retry_deadline) {
            tokio::time::sleep(metadata_retry_delay(attempt)).await;
            continue;
        }
        break;
    }

    Err(anyhow!(
        "secondary index metadata drop retries exhausted for '{}'",
        normalized
    ))
}

async fn list_secondary_index_entries(
    client: &HoloStoreClient,
) -> Result<Vec<SecondaryIndexMetadataEntry>> {
    let topology = fetch_topology(client).await?;
    require_non_empty_topology(&topology)?;

    let start_key = vec![META_PREFIX_SECONDARY_INDEX];
    let end_key = prefix_end(start_key.as_slice()).unwrap_or_default();
    let targets = scan_targets(
        &topology,
        client.target(),
        start_key.as_slice(),
        end_key.as_slice(),
        &[],
    );

    let mut by_key = BTreeMap::<(u64, u64, u64, u64), SecondaryIndexMetadataEntry>::new();

    for target in targets {
        let mut cursor = Vec::new();
        loop {
            let scan_client = HoloStoreClient::new(target.grpc_addr);
            let (entries, next_cursor, done) = scan_client
                .range_snapshot_latest(
                    target.shard_index,
                    target.start_key.as_slice(),
                    target.end_key.as_slice(),
                    cursor.as_slice(),
                    DEFAULT_INDEX_SCAN_PAGE_SIZE,
                    false,
                )
                .await
                .with_context(|| {
                    format!(
                        "scan secondary index metadata shard={} target={}",
                        target.shard_index, target.grpc_addr
                    )
                })?;

            for entry in entries {
                let Some((db_id, schema_id, table_id, index_id)) =
                    decode_secondary_index_metadata_key(entry.key.as_slice())
                else {
                    continue;
                };
                if is_sql_tombstone_value(entry.value.as_slice()) {
                    continue;
                }
                let record: SecondaryIndexRecord = serde_json::from_slice(entry.value.as_slice())
                    .with_context(|| {
                    format!(
                        "decode secondary index metadata value key={}",
                        hex::encode(&entry.key)
                    )
                })?;
                record.validate()?;

                let key_tuple = (db_id, schema_id, table_id, index_id);
                match by_key.get(&key_tuple) {
                    Some(existing) if existing.version >= entry.version => {}
                    _ => {
                        by_key.insert(
                            key_tuple,
                            SecondaryIndexMetadataEntry {
                                key: entry.key,
                                version: entry.version,
                                record,
                            },
                        );
                    }
                }
            }

            if done || next_cursor.is_empty() {
                break;
            }
            cursor = next_cursor;
        }
    }

    Ok(by_key.into_values().collect())
}

async fn find_secondary_index_entry_by_id(
    client: &HoloStoreClient,
    table_id: u64,
    index_id: u64,
) -> Result<Option<SecondaryIndexMetadataEntry>> {
    let entries = list_secondary_index_entries(client).await?;
    Ok(entries
        .into_iter()
        .find(|entry| entry.record.table_id == table_id && entry.record.index_id == index_id))
}

async fn wait_for_secondary_index_by_id(
    client: &HoloStoreClient,
    table_id: u64,
    index_id: u64,
    timeout: Duration,
) -> Result<Option<SecondaryIndexRecord>> {
    let deadline = tokio::time::Instant::now() + timeout.max(Duration::from_millis(1));
    let mut last_err: Option<anyhow::Error> = None;

    loop {
        match find_secondary_index_entry_by_id(client, table_id, index_id).await {
            Ok(Some(entry)) => return Ok(Some(entry.record)),
            Ok(None) => {}
            Err(err) => last_err = Some(err),
        }

        if tokio::time::Instant::now() >= deadline {
            if let Some(err) = last_err {
                return Err(anyhow!(
                    "secondary index metadata did not become visible before timeout: {}",
                    err
                ));
            }
            return Ok(None);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn encode_secondary_index_metadata_key(
    db_id: u64,
    schema_id: u64,
    table_id: u64,
    index_id: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + 8 + 8 + 8);
    out.push(META_PREFIX_SECONDARY_INDEX);
    out.extend_from_slice(&db_id.to_be_bytes());
    out.extend_from_slice(&schema_id.to_be_bytes());
    out.extend_from_slice(&table_id.to_be_bytes());
    out.extend_from_slice(&index_id.to_be_bytes());
    out
}

fn decode_secondary_index_metadata_key(key: &[u8]) -> Option<(u64, u64, u64, u64)> {
    if key.len() != 1 + 8 + 8 + 8 + 8 {
        return None;
    }
    if key[0] != META_PREFIX_SECONDARY_INDEX {
        return None;
    }

    let mut db = [0u8; 8];
    db.copy_from_slice(&key[1..9]);
    let mut schema = [0u8; 8];
    schema.copy_from_slice(&key[9..17]);
    let mut table = [0u8; 8];
    table.copy_from_slice(&key[17..25]);
    let mut index = [0u8; 8];
    index.copy_from_slice(&key[25..33]);

    Some((
        u64::from_be_bytes(db),
        u64::from_be_bytes(schema),
        u64::from_be_bytes(table),
        u64::from_be_bytes(index),
    ))
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    for idx in (0..out.len()).rev() {
        if out[idx] != 0xFF {
            out[idx] = out[idx].saturating_add(1);
            out.truncate(idx + 1);
            return Some(out);
        }
    }
    None
}

fn now_unix_epoch_millis() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(u64::MAX as u128) as u64,
        Err(_) => 0,
    }
}

/// Returns exponential retry delay for secondary-index metadata writes.
///
/// Design:
/// - Metadata writes are routed and can observe transient route churn during
///   split/rebalance windows, so backoff should stretch past sub-second bursts.
/// - Keep delay bounded to avoid unbounded sleeps while still reducing retry
///   pressure under prolonged control-plane instability.
///
/// Inputs:
/// - `attempt`: zero-based metadata-write attempt index.
///
/// Outputs:
/// - Backoff duration in `[INDEX_METADATA_RETRY_DELAY, INDEX_METADATA_RETRY_MAX_DELAY]`.
fn metadata_retry_delay(attempt: usize) -> Duration {
    let shift = (attempt as u32).min(5);
    let factor = 1u128 << shift;
    let base_ms = INDEX_METADATA_RETRY_DELAY.as_millis();
    let capped_ms =
        (base_ms.saturating_mul(factor)).min(INDEX_METADATA_RETRY_MAX_DELAY.as_millis());
    Duration::from_millis(capped_ms as u64)
}

/// Returns `true` when metadata write path should schedule another retry.
fn should_retry_metadata_write(attempt: usize, deadline: Instant) -> bool {
    attempt + 1 < INDEX_METADATA_RETRY_LIMIT && Instant::now() < deadline
}

/// Returns whether an existing index metadata record matches CREATE INDEX spec.
///
/// Design:
/// - Distinguish "same logical index appeared during retries" from true
///   conflicting duplicates so CREATE INDEX can recover from ambiguous write
///   acknowledgments without sacrificing correctness.
///
/// Inputs:
/// - `record`: persisted metadata candidate with matching index name.
/// - `spec`: requested CREATE INDEX metadata spec.
///
/// Outputs:
/// - `true` when uniqueness/distribution/hash bucket and normalized key/include
///   columns match exactly.
fn secondary_index_record_matches_spec(
    record: &SecondaryIndexRecord,
    spec: &CreateSecondaryIndexMetadataSpec,
) -> bool {
    if record.unique != spec.unique || record.distribution != spec.distribution {
        return false;
    }
    let expected_hash_buckets = if spec.distribution == SecondaryIndexDistribution::Hash {
        Some(
            spec.hash_bucket_count
                .unwrap_or(DEFAULT_SECONDARY_INDEX_HASH_BUCKETS),
        )
    } else {
        None
    };
    if record.hash_bucket_count != expected_hash_buckets {
        return false;
    }

    let expected_keys = spec
        .key_columns
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if record.key_columns != expected_keys {
        return false;
    }

    let expected_include = spec
        .include_columns
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    record.include_columns == expected_include
}

fn is_retryable_metadata_write_error(err: &anyhow::Error) -> bool {
    let message = err.to_string();
    let lower = message.to_ascii_lowercase();
    lower.contains("key routed to shard")
        || lower.contains("request targeted shard")
        || lower.contains("no shard route found")
        || lower.contains("split key does not map")
        || lower.contains("split key must")
        || lower.contains("range metadata changed")
        || lower.contains("timed out")
        || lower.contains("deadline")
        || lower.contains("temporarily unavailable")
        || lower.contains("unavailable")
        || lower.contains("transport")
        || lower.contains("connection reset")
        || lower.contains("connection refused")
        || lower.contains("broken pipe")
        || lower.contains("network")
        || lower.contains("unknown group")
        || lower.contains("meta handle not available")
        || lower.contains("lost range controller lease")
        || lower.contains("lease_lost")
        || lower.contains("controller lease")
        || lower.contains("failed precondition")
        || lower.contains("failed_precondition")
        || lower.contains("fence")
        || lower.contains("retry after split")
        || lower.contains("split in progress")
}

#[cfg(test)]
mod tests {
    use super::{is_retryable_metadata_write_error, secondary_index_record_matches_spec};
    use crate::indexing::metadata::{
        CreateSecondaryIndexMetadataSpec, SecondaryIndexDistribution, SecondaryIndexRecord,
        SecondaryIndexState,
    };

    #[test]
    fn metadata_retry_classifier_matches_shard_route_mismatch() {
        let err = anyhow::anyhow!(
            "persist secondary index metadata 'idx' on table 'sales_facts': range_write_latest_conditional rpc failed for 127.0.0.1:15051: rpc status: key routed to shard 2, but request targeted shard 1"
        );
        assert!(is_retryable_metadata_write_error(&err));
    }

    #[test]
    fn metadata_retry_classifier_matches_transient_rpc_timeout() {
        let err = anyhow::anyhow!(
            "persist secondary index metadata 'idx' on table 'sales_facts': range_write_latest_conditional rpc timed out for 127.0.0.1:15051"
        );
        assert!(is_retryable_metadata_write_error(&err));
    }

    #[test]
    fn metadata_retry_classifier_rejects_non_retryable_errors() {
        let err = anyhow::anyhow!("secondary index metadata write applied but not visible");
        assert!(!is_retryable_metadata_write_error(&err));
    }

    #[test]
    fn metadata_retry_classifier_matches_split_lease_loss() {
        let err = anyhow::anyhow!(
            "persist secondary index metadata 'idx' on table 'sales_facts': range_write_latest_conditional rpc failed for 127.0.0.1:15051: rpc status: Internal: range conditional write failed: lost range controller lease during split operation"
        );
        assert!(is_retryable_metadata_write_error(&err));
    }

    #[test]
    fn metadata_retry_classifier_matches_unknown_group() {
        let err = anyhow::anyhow!(
            "persist secondary index metadata 'idx' on table 'sales_facts': range_write_latest_conditional rpc failed for 127.0.0.1:15051: rpc status: NotFound: unknown group"
        );
        assert!(is_retryable_metadata_write_error(&err));
    }

    #[test]
    fn secondary_index_record_matcher_detects_equal_spec() {
        let spec = CreateSecondaryIndexMetadataSpec {
            index_name: "idx_sales_status_day_merchant_cover".to_string(),
            unique: false,
            key_columns: vec!["status".to_string(), "event_day".to_string()],
            include_columns: vec!["merchant_id".to_string(), "amount_cents".to_string()],
            distribution: SecondaryIndexDistribution::Hash,
            hash_bucket_count: Some(64),
            if_not_exists: false,
        };
        let record = SecondaryIndexRecord {
            db_id: 1,
            schema_id: 1,
            table_id: 1,
            index_id: 1,
            table_name: "sales_facts".to_string(),
            index_name: "idx_sales_status_day_merchant_cover".to_string(),
            unique: false,
            key_columns: vec!["status".to_string(), "event_day".to_string()],
            include_columns: vec!["merchant_id".to_string(), "amount_cents".to_string()],
            distribution: SecondaryIndexDistribution::Hash,
            hash_bucket_count: Some(64),
            state: SecondaryIndexState::WriteOnly,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        assert!(secondary_index_record_matches_spec(&record, &spec));
    }

    #[test]
    fn secondary_index_record_matcher_rejects_mismatched_distribution() {
        let spec = CreateSecondaryIndexMetadataSpec {
            index_name: "idx_sales_status_day_merchant_cover".to_string(),
            unique: false,
            key_columns: vec!["status".to_string(), "event_day".to_string()],
            include_columns: vec!["merchant_id".to_string(), "amount_cents".to_string()],
            distribution: SecondaryIndexDistribution::Range,
            hash_bucket_count: None,
            if_not_exists: false,
        };
        let record = SecondaryIndexRecord {
            db_id: 1,
            schema_id: 1,
            table_id: 1,
            index_id: 1,
            table_name: "sales_facts".to_string(),
            index_name: "idx_sales_status_day_merchant_cover".to_string(),
            unique: false,
            key_columns: vec!["status".to_string(), "event_day".to_string()],
            include_columns: vec!["merchant_id".to_string(), "amount_cents".to_string()],
            distribution: SecondaryIndexDistribution::Hash,
            hash_bucket_count: Some(64),
            state: SecondaryIndexState::WriteOnly,
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
        };
        assert!(!secondary_index_record_matches_spec(&record, &spec));
    }
}
