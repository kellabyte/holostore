//! Key/value storage primitives and command encoding for the HoloStore node.
//!
//! This module provides the `KvEngine` abstraction, two engine implementations
//! (`KvStore` in-memory and `FjallEngine` on-disk), a sharded wrapper, and
//! utilities for encoding/decoding commands used by the Accord state machine.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use fjall::{Keyspace, PartitionCreateOptions};
use holo_accord::accord::{CommandKeys, ExecMeta, StateMachine, TxnId};
use tracing::warn;

/// Storage engine API used by the Accord state machine.
///
/// Implementations are responsible for storing per-version values, exposing
/// both historical reads (at a given version) and the latest visible value.
pub trait KvEngine: Send + Sync + 'static {
    /// Read the newest visible value for `key` that is <= `version`.
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>>;
    /// Read the latest visible value for `key` along with its version.
    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)>;
    /// Batch variant of `get_latest` that preserves input ordering.
    fn get_latest_batch(&self, keys: &[Vec<u8>]) -> Vec<Option<(Vec<u8>, Version)>>;
    /// Persist a value for `key` at `version` (initially invisible).
    fn set(&self, key: Vec<u8>, value: Vec<u8>, version: Version);
    /// Persist multiple values in one batch (initially invisible).
    fn set_batch(&self, items: &[(Vec<u8>, Vec<u8>, Version)]) {
        for (key, value, version) in items {
            self.set(key.clone(), value.clone(), *version);
        }
    }
    /// Mark a previously written `(key, version)` as visible to readers.
    fn mark_visible(&self, key: &[u8], version: Version);
}

#[allow(dead_code)]
/// Simple in-memory key/value store with per-version visibility.
pub struct KvStore {
    inner: RwLock<HashMap<Vec<u8>, Vec<VersionedValue>>>,
}

impl KvStore {
    #[allow(dead_code)]
    /// Create a new empty in-memory KV store.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl KvEngine for KvStore {
    /// Return the latest visible version <= `version` if present.
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>> {
        let guard = self.inner.read().ok()?;
        let versions = guard.get(key)?;
        find_visible_version(versions, version).map(|v| v.value.clone())
    }

    /// Return the most recently visible version for `key`.
    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)> {
        let guard = self.inner.read().ok()?;
        let versions = guard.get(key)?;
        let last = versions.iter().rev().find(|v| v.visible)?;
        Some((last.value.clone(), last.version))
    }

    /// Batch helper to read latest values for multiple keys.
    fn get_latest_batch(&self, keys: &[Vec<u8>]) -> Vec<Option<(Vec<u8>, Version)>> {
        keys.iter().map(|k| self.get_latest(k)).collect()
    }

    /// Insert or update a versioned value (initially invisible).
    fn set(&self, key: Vec<u8>, value: Vec<u8>, version: Version) {
        if let Ok(mut guard) = self.inner.write() {
            let entry = guard.entry(key).or_default();
            // Decide whether to replace an existing version or insert a new one in order.
            match entry.binary_search_by(|v| v.version.cmp(&version)) {
                // Existing version: overwrite the value in-place.
                Ok(idx) => entry[idx].value = value,
                // New version: insert in sorted order and mark invisible.
                Err(idx) => entry.insert(
                    idx,
                    VersionedValue {
                        version,
                        value,
                        visible: false,
                    },
                ),
            }
        }
    }

    /// Batch variant of `set` with one write lock.
    fn set_batch(&self, items: &[(Vec<u8>, Vec<u8>, Version)]) {
        if let Ok(mut guard) = self.inner.write() {
            for (key, value, version) in items {
                let entry = guard.entry(key.clone()).or_default();
                // Decide whether to replace an existing version or insert a new one in order.
                match entry.binary_search_by(|v| v.version.cmp(version)) {
                    // Existing version: overwrite the value in-place.
                    Ok(idx) => entry[idx].value = value.clone(),
                    // New version: insert in sorted order and mark invisible.
                    Err(idx) => entry.insert(
                        idx,
                        VersionedValue {
                            version: *version,
                            value: value.clone(),
                            visible: false,
                        },
                    ),
                }
            }
        }
    }

    /// Mark a version as visible so reads can observe it.
    fn mark_visible(&self, key: &[u8], version: Version) {
        if let Ok(mut guard) = self.inner.write() {
            let Some(entry) = guard.get_mut(key) else {
                // Nothing to mark if the key does not exist yet.
                return;
            };
            // Only update visibility if the exact version is present.
            if let Ok(idx) = entry.binary_search_by(|v| v.version.cmp(&version)) {
                entry[idx].visible = true;
            }
        }
    }
}

/// Fjall-backed key/value engine that stores versions and a latest index.
pub struct FjallEngine {
    keyspace: Arc<Keyspace>,
    versions: fjall::PartitionHandle,
    latest: fjall::PartitionHandle,
    lock: RwLock<()>,
}

/// Hash a key for shard selection and peer ordering.
pub fn hash_key(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

impl FjallEngine {
    /// Open the default (single-shard) Fjall partitions.
    pub fn open(keyspace: Arc<Keyspace>) -> anyhow::Result<Self> {
        let versions = keyspace.open_partition("kv_versions", PartitionCreateOptions::default())?;
        let latest = keyspace.open_partition("kv_latest", PartitionCreateOptions::default())?;
        Ok(Self {
            keyspace,
            versions,
            latest,
            lock: RwLock::new(()),
        })
    }

    /// Open shard-specific Fjall partitions by suffixing partition names.
    pub fn open_shard(keyspace: Arc<Keyspace>, shard: usize) -> anyhow::Result<Self> {
        let versions_name = format!("kv_versions_{shard}");
        let latest_name = format!("kv_latest_{shard}");
        let versions = keyspace.open_partition(&versions_name, PartitionCreateOptions::default())?;
        let latest = keyspace.open_partition(&latest_name, PartitionCreateOptions::default())?;
        Ok(Self {
            keyspace,
            versions,
            latest,
            lock: RwLock::new(()),
        })
    }
}

impl KvEngine for FjallEngine {
    /// Read the latest visible value <= `version` by consulting the latest index
    /// first and falling back to a reverse scan of the versioned partition.
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>> {
        let _guard = self.lock.read().ok()?;
        if let Ok(Some(bytes)) = self.latest.get(key) {
            if let Ok((latest_version, latest_value)) = decode_latest_value(&bytes) {
                // Fast path: latest value is already visible for this version.
                if latest_version <= version {
                    return Some(latest_value);
                }
            }
        }

        // Slow path: scan older versions until a visible one <= target version is found.
        let prefix = encode_key_prefix(key);
        let mut iter = self.versions.prefix(prefix).rev();
        while let Some(Ok((entry_key, entry_value))) = iter.next() {
            let entry_version = decode_version_from_key(key, &entry_key)?;
            // Skip versions that are newer than the read view.
            if entry_version > version {
                continue;
            }
            if let Ok((visible, value)) = decode_version_value(&entry_value) {
                if visible {
                    return Some(value);
                }
            }
        }
        None
    }

    /// Read the latest visible value for `key` from the latest index.
    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)> {
        let _guard = self.lock.read().ok()?;
        let bytes = self.latest.get(key).ok().flatten()?;
        let (version, value) = decode_latest_value(&bytes).ok()?;
        Some((value, version))
    }

    /// Batch helper to read latest values for multiple keys.
    fn get_latest_batch(&self, keys: &[Vec<u8>]) -> Vec<Option<(Vec<u8>, Version)>> {
        keys.iter().map(|k| self.get_latest(k)).collect()
    }

    /// Insert a versioned value (initially invisible) into the versions partition.
    fn set(&self, key: Vec<u8>, value: Vec<u8>, version: Version) {
        let _guard = match self.lock.write() {
            Ok(guard) => guard,
            // If the lock is poisoned, skip the write rather than panic.
            Err(_) => return,
        };
        let entry_key = encode_version_key(&key, version);
        let entry_value = encode_version_value(false, &value);
        if let Err(err) = self.versions.insert(entry_key, entry_value) {
            warn!(error = ?err, "fjall kv write failed");
        }
    }

    /// Batch insert multiple versioned values in one Fjall transaction.
    fn set_batch(&self, items: &[(Vec<u8>, Vec<u8>, Version)]) {
        let _guard = match self.lock.write() {
            Ok(guard) => guard,
            // If the lock is poisoned, skip the write rather than panic.
            Err(_) => return,
        };

        let mut batch = self.keyspace.batch();
        for (key, value, version) in items {
            let entry_key = encode_version_key(key, *version);
            let entry_value = encode_version_value(false, value);
            batch.insert(&self.versions, entry_key, entry_value);
        }

        if let Err(err) = batch.commit() {
            warn!(error = ?err, "fjall kv batch write failed");
        }
    }

    /// Mark a version visible and update the latest index if needed.
    fn mark_visible(&self, key: &[u8], version: Version) {
        let _guard = match self.lock.write() {
            Ok(guard) => guard,
            // If the lock is poisoned, skip the update rather than panic.
            Err(_) => return,
        };
        let entry_key = encode_version_key(key, version);
        let entry_value = match self.versions.get(&entry_key) {
            Ok(Some(bytes)) => bytes,
            // Nothing to mark if the version does not exist.
            Ok(None) => return,
            Err(err) => {
                warn!(error = ?err, "fjall kv read failed");
                return;
            }
        };

        let Ok((visible, value)) = decode_version_value(&entry_value) else {
            return;
        };
        // Skip updates if the entry is already visible.
        if visible {
            return;
        }

        let mut batch = self.keyspace.batch();
        batch.insert(
            &self.versions,
            entry_key,
            encode_version_value(true, &value),
        );

        let latest_update = match self.latest.get(key) {
            Ok(Some(bytes)) => decode_latest_value(&bytes).ok().map(|(cur, _)| cur),
            _ => None,
        };
        // Only update the latest index if this version is newer or no latest exists.
        if latest_update.map_or(true, |cur| version >= cur) {
            batch.insert(&self.latest, key.to_vec(), encode_latest_value(version, &value));
        }

        if let Err(err) = batch.commit() {
            warn!(error = ?err, "fjall kv mark visible failed");
        }
    }
}

/// Sharded wrapper that routes keys to per-shard `KvEngine` instances.
pub struct ShardedKvEngine {
    shards: Vec<Arc<dyn KvEngine>>,
}

impl ShardedKvEngine {
    /// Create a sharded engine from pre-built shard engines.
    pub fn new(shards: Vec<Arc<dyn KvEngine>>) -> anyhow::Result<Self> {
        anyhow::ensure!(!shards.is_empty(), "sharded kv engine requires at least one shard");
        Ok(Self { shards })
    }

    /// Hash a key to select which shard should serve it.
    fn shard_for_key(&self, key: &[u8]) -> usize {
        let hash = hash_key(key);
        (hash as usize) % self.shards.len()
    }
}

impl KvEngine for ShardedKvEngine {
    /// Delegate a versioned read to the chosen shard.
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>> {
        let shard = self.shard_for_key(key);
        self.shards[shard].get(key, version)
    }

    /// Delegate a latest read to the chosen shard.
    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)> {
        let shard = self.shard_for_key(key);
        self.shards[shard].get_latest(key)
    }

    /// Batch latest reads across shards while preserving input order.
    fn get_latest_batch(&self, keys: &[Vec<u8>]) -> Vec<Option<(Vec<u8>, Version)>> {
        if keys.is_empty() {
            // Empty input means empty output; avoid shard bookkeeping.
            return Vec::new();
        }
        let mut results = vec![None; keys.len()];
        let mut by_shard: Vec<Vec<(usize, Vec<u8>)>> = vec![Vec::new(); self.shards.len()];
        for (idx, key) in keys.iter().enumerate() {
            let shard = self.shard_for_key(key);
            by_shard[shard].push((idx, key.clone()));
        }
        for (shard, items) in by_shard.into_iter().enumerate() {
            if items.is_empty() {
                // Skip shards that have no keys assigned.
                continue;
            }
            let mut shard_keys = Vec::with_capacity(items.len());
            let mut indices = Vec::with_capacity(items.len());
            for (idx, key) in items {
                indices.push(idx);
                shard_keys.push(key);
            }
            let shard_values = self.shards[shard].get_latest_batch(&shard_keys);
            for (idx, value) in indices.into_iter().zip(shard_values.into_iter()) {
                results[idx] = value;
            }
        }
        results
    }

    /// Delegate a write to the chosen shard.
    fn set(&self, key: Vec<u8>, value: Vec<u8>, version: Version) {
        let shard = self.shard_for_key(&key);
        self.shards[shard].set(key, value, version);
    }

    /// Batch writes across shards while preserving per-shard ordering.
    fn set_batch(&self, items: &[(Vec<u8>, Vec<u8>, Version)]) {
        if items.is_empty() {
            // Nothing to write, so avoid shard work.
            return;
        }
        let mut by_shard: Vec<Vec<(Vec<u8>, Vec<u8>, Version)>> =
            vec![Vec::new(); self.shards.len()];
        for (key, value, version) in items {
            let shard = self.shard_for_key(key);
            by_shard[shard].push((key.clone(), value.clone(), *version));
        }
        for (shard, batch) in by_shard.into_iter().enumerate() {
            if batch.is_empty() {
                // Skip shards that have no items assigned.
                continue;
            }
            self.shards[shard].set_batch(&batch);
        }
    }

    /// Delegate visibility updates to the chosen shard.
    fn mark_visible(&self, key: &[u8], version: Version) {
        let shard = self.shard_for_key(key);
        self.shards[shard].mark_visible(key, version);
    }
}

/// Version identifier used for MVCC-style reads in the KV engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub seq: u64,
    pub txn_id: TxnId,
}

impl Version {
    /// Zero version used as a sentinel for "no data".
    pub const fn zero() -> Self {
        Self {
            seq: 0,
            txn_id: TxnId {
                node_id: 0,
                counter: 0,
            },
        }
    }
}

impl From<ExecMeta> for Version {
    /// Convert execution metadata into a KV version.
    fn from(meta: ExecMeta) -> Self {
        Self {
            seq: meta.seq,
            txn_id: meta.txn_id,
        }
    }
}

/// Internal storage representation of a versioned value.
#[derive(Clone, Debug)]
struct VersionedValue {
    version: Version,
    value: Vec<u8>,
    visible: bool,
}

/// Find the latest visible version <= `version` in an ordered version list.
fn find_visible_version(versions: &[VersionedValue], version: Version) -> Option<&VersionedValue> {
    if versions.is_empty() {
        // No versions recorded for the key.
        return None;
    }

    // Binary search to find the closest candidate version.
    let idx = match versions.binary_search_by(|v| v.version.cmp(&version)) {
        // Exact version hit.
        Ok(idx) => idx,
        // All versions are newer than the requested version.
        Err(0) => return None,
        // Use the previous entry as the newest <= version.
        Err(idx) => idx.saturating_sub(1),
    };

    // Walk backward until a visible version is found.
    for item in versions[..=idx].iter().rev() {
        if item.visible {
            return Some(item);
        }
    }
    None
}

#[allow(dead_code)]
/// Encode a list of versioned values to a compact binary format.
fn encode_versions(versions: &[VersionedValue]) -> Vec<u8> {
    let mut size = 4;
    for v in versions {
        size += 8 + 8 + 8 + 1 + 4 + v.value.len();
    }
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(&(versions.len() as u32).to_be_bytes());
    for v in versions {
        out.extend_from_slice(&v.version.seq.to_be_bytes());
        out.extend_from_slice(&v.version.txn_id.node_id.to_be_bytes());
        out.extend_from_slice(&v.version.txn_id.counter.to_be_bytes());
        out.push(v.visible as u8);
        out.extend_from_slice(&(v.value.len() as u32).to_be_bytes());
        out.extend_from_slice(&v.value);
    }
    out
}

#[allow(dead_code)]
/// Decode the versioned value list produced by `encode_versions`.
fn decode_versions(data: &[u8]) -> anyhow::Result<Vec<VersionedValue>> {
    let mut offset = 0usize;
    let count = read_u32(data, &mut offset)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        anyhow::ensure!(offset + 8 + 8 + 8 + 1 + 4 <= data.len(), "short version header");
        let seq = read_u64(data, &mut offset)?;
        let node_id = read_u64(data, &mut offset)?;
        let counter = read_u64(data, &mut offset)?;
        let visible = read_u8(data, &mut offset)? != 0;
        let len = read_u32(data, &mut offset)? as usize;
        anyhow::ensure!(offset + len <= data.len(), "short version value");
        let value = data[offset..offset + len].to_vec();
        offset += len;
        out.push(VersionedValue {
            version: Version {
                seq,
                txn_id: TxnId { node_id, counter },
            },
            value,
            visible,
        });
    }
    Ok(out)
}

/// Encode the key prefix used for range scans in the versions partition.
fn encode_key_prefix(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

/// Encode the composite key used in the versions partition.
fn encode_version_key(key: &[u8], version: Version) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len() + 8 + 8 + 8);
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&version.seq.to_be_bytes());
    out.extend_from_slice(&version.txn_id.node_id.to_be_bytes());
    out.extend_from_slice(&version.txn_id.counter.to_be_bytes());
    out
}

/// Decode the version from a composite key if it matches the requested key.
fn decode_version_from_key(key: &[u8], entry_key: &[u8]) -> Option<Version> {
    if entry_key.len() < 4 {
        // Entry key is too small to contain a length prefix.
        return None;
    }
    let mut offset = 0usize;
    let key_len = read_u32(entry_key, &mut offset).ok()? as usize;
    if offset + key_len + 8 + 8 + 8 > entry_key.len() {
        return None;
    }
    if key_len != key.len() || &entry_key[offset..offset + key_len] != key {
        // Prefix does not match the requested key.
        return None;
    }
    offset += key_len;
    let seq = read_u64(entry_key, &mut offset).ok()?;
    let node_id = read_u64(entry_key, &mut offset).ok()?;
    let counter = read_u64(entry_key, &mut offset).ok()?;
    Some(Version {
        seq,
        txn_id: TxnId { node_id, counter },
    })
}

/// Encode a versioned value with an explicit visibility flag.
fn encode_version_value(visible: bool, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + value.len());
    out.push(visible as u8);
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
    out
}

/// Decode a value payload that includes a visibility byte.
fn decode_version_value(data: &[u8]) -> anyhow::Result<(bool, Vec<u8>)> {
    let mut offset = 0usize;
    let visible = read_u8(data, &mut offset)? != 0;
    let len = read_u32(data, &mut offset)? as usize;
    anyhow::ensure!(offset + len <= data.len(), "short version value");
    Ok((visible, data[offset..offset + len].to_vec()))
}

/// Encode the "latest" index value (version + value bytes).
fn encode_latest_value(version: Version, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 8 + 8 + 4 + value.len());
    out.extend_from_slice(&version.seq.to_be_bytes());
    out.extend_from_slice(&version.txn_id.node_id.to_be_bytes());
    out.extend_from_slice(&version.txn_id.counter.to_be_bytes());
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
    out
}

/// Decode the "latest" index value.
fn decode_latest_value(data: &[u8]) -> anyhow::Result<(Version, Vec<u8>)> {
    let mut offset = 0usize;
    let seq = read_u64(data, &mut offset)?;
    let node_id = read_u64(data, &mut offset)?;
    let counter = read_u64(data, &mut offset)?;
    let len = read_u32(data, &mut offset)? as usize;
    anyhow::ensure!(offset + len <= data.len(), "short latest value");
    let value = data[offset..offset + len].to_vec();
    Ok((
        Version {
            seq,
            txn_id: TxnId { node_id, counter },
        },
        value,
    ))
}

/// State machine that ignores commands (used for membership group).
pub struct NoopStateMachine;

impl StateMachine for NoopStateMachine {
    /// No keys are tracked because commands are ignored.
    fn command_keys(&self, _data: &[u8]) -> anyhow::Result<CommandKeys> {
        Ok(CommandKeys::default())
    }

    /// No-op apply for ignored commands.
    fn apply(&self, _data: &[u8], _meta: ExecMeta) {}
}

/// State machine that applies KV commands to a `KvEngine`.
pub struct KvStateMachine {
    kv: Arc<dyn KvEngine>,
}

impl KvStateMachine {
    /// Create a new state machine wrapper around a KV engine.
    pub fn new(kv: Arc<dyn KvEngine>) -> Self {
        Self { kv }
    }
}

impl StateMachine for KvStateMachine {
    /// Parse command bytes to identify read/write keys for dependency tracking.
    fn command_keys(&self, data: &[u8]) -> anyhow::Result<CommandKeys> {
        command_keys(data)
    }

    /// Apply a single command to the KV engine, logging on failure.
    fn apply(&self, data: &[u8], meta: ExecMeta) {
        if let Err(err) = apply_kv_command(self.kv.as_ref(), data, meta.into()) {
            tracing::warn!(error = ?err, "failed to apply kv command");
        }
    }

    /// Apply a batch of commands, coalescing SETs for efficiency.
    fn apply_batch(&self, items: &[(Vec<u8>, ExecMeta)]) {
        let mut sets: Vec<(Vec<u8>, Vec<u8>, Version)> = Vec::new();
        for (data, meta) in items {
            if data.is_empty() {
                // Ignore empty commands to avoid parsing errors.
                continue;
            }
            let version = Version::from(*meta);
            match data[0] {
                CMD_SET => {
                    let mut offset = 1;
                    if let Ok(key_len) = read_u32(data, &mut offset) {
                        let key_len = key_len as usize;
                        if offset + key_len > data.len() {
                            // Malformed command; skip this entry.
                            continue;
                        }
                        let key = data[offset..offset + key_len].to_vec();
                        offset += key_len;
                        if let Ok(value_len) = read_u32(data, &mut offset) {
                            let value_len = value_len as usize;
                            if offset + value_len > data.len() {
                                // Malformed command; skip this entry.
                                continue;
                            }
                            let value = data[offset..offset + value_len].to_vec();
                            sets.push((key, value, version));
                        }
                    }
                }
                CMD_BATCH_SET => {
                    let mut offset = 1;
                    let Ok(count) = read_u32(data, &mut offset) else {
                        // Malformed batch header; skip this entry.
                        continue;
                    };
                    for _ in 0..count {
                        let Ok(key_len) = read_u32(data, &mut offset) else {
                            // Truncated entry; stop scanning this batch.
                            break;
                        };
                        let key_len = key_len as usize;
                        if offset + key_len > data.len() {
                            // Truncated entry; stop scanning this batch.
                            break;
                        }
                        let key = data[offset..offset + key_len].to_vec();
                        offset += key_len;
                        let Ok(value_len) = read_u32(data, &mut offset) else {
                            // Truncated entry; stop scanning this batch.
                            break;
                        };
                        let value_len = value_len as usize;
                        if offset + value_len > data.len() {
                            // Truncated entry; stop scanning this batch.
                            break;
                        }
                        let value = data[offset..offset + value_len].to_vec();
                        offset += value_len;
                        sets.push((key, value, version));
                    }
                }
                // Non-SET commands are ignored in batch apply.
                _ => {}
            }
        }

        if !sets.is_empty() {
            // Flush coalesced SETs in one engine batch.
            self.kv.set_batch(&sets);
        }
    }

    /// Execute a read command without mutating state.
    fn read(&self, data: &[u8], meta: ExecMeta) -> anyhow::Result<Option<Vec<u8>>> {
        anyhow::ensure!(!data.is_empty(), "empty command");
        let version = Version::from(meta);
        match data[0] {
            CMD_BATCH_GET => {
                let mut offset = 1;
                let count = read_u32(data, &mut offset)? as usize;
                let mut out = Vec::with_capacity(4 + (count * 4));
                out.extend_from_slice(&(count as u32).to_be_bytes());
                for _ in 0..count {
                    let key_len = read_u32(data, &mut offset)? as usize;
                    anyhow::ensure!(offset + key_len <= data.len(), "short key");
                    let key = &data[offset..offset + key_len];
                    offset += key_len;

                    match self.kv.get(key, version) {
                        // Encode missing keys as sentinel lengths.
                        None => out.extend_from_slice(&u32::MAX.to_be_bytes()),
                        Some(v) => {
                            out.extend_from_slice(&(v.len() as u32).to_be_bytes());
                            out.extend_from_slice(&v);
                        }
                    }
                }
                Ok(Some(out))
            }
            CMD_GET => {
                let mut offset = 1;
                let key_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + key_len <= data.len(), "short key");
                let key = &data[offset..offset + key_len];
                Ok(self.kv.get(key, version))
            }
            // Non-read commands are ignored by the read path.
            _ => Ok(None),
        }
    }

    /// Mark all written keys in a command as visible.
    fn mark_visible(&self, data: &[u8], meta: ExecMeta) {
        if data.is_empty() {
            // Ignore empty commands to avoid parsing errors.
            return;
        }
        let version = Version::from(meta);
        let keys = match command_keys(data) {
            Ok(keys) => keys,
            Err(err) => {
                tracing::warn!(error = ?err, "failed to parse command keys for mark_visible");
                return;
            }
        };
        for key in keys.writes {
            self.kv.mark_visible(&key, version);
        }
    }
}

/// Command tag for a single SET.
const CMD_SET: u8 = 1;
/// Command tag for a single GET.
const CMD_GET: u8 = 2;
/// Command tag for a multi-key SET batch.
const CMD_BATCH_SET: u8 = 3;
/// Command tag for a multi-key GET batch.
const CMD_BATCH_GET: u8 = 4;

/// Encode a single-key SET command.
pub fn encode_set(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
    out.push(CMD_SET);
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
    out
}

/// Encode a single-key GET command.
pub fn encode_get(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + key.len());
    out.push(CMD_GET);
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

/// Encode a batch SET command with multiple key/value pairs.
pub fn encode_batch_set(items: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut size = 1 + 4;
    for (k, v) in items {
        size += 4 + k.len();
        size += 4 + v.len();
    }

    let mut out = Vec::with_capacity(size);
    out.push(CMD_BATCH_SET);
    out.extend_from_slice(&(items.len() as u32).to_be_bytes());
    for (k, v) in items {
        out.extend_from_slice(&(k.len() as u32).to_be_bytes());
        out.extend_from_slice(k);
        out.extend_from_slice(&(v.len() as u32).to_be_bytes());
        out.extend_from_slice(v);
    }
    out
}

/// Encode a batch GET command with multiple keys.
pub fn encode_batch_get(keys: &[Vec<u8>]) -> Vec<u8> {
    let mut size = 1 + 4;
    for key in keys {
        size += 4 + key.len();
    }

    let mut out = Vec::with_capacity(size);
    out.push(CMD_BATCH_GET);
    out.extend_from_slice(&(keys.len() as u32).to_be_bytes());
    for key in keys {
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
    }
    out
}

/// Decode the result of a batch GET command into optional values.
pub fn decode_batch_get_result(data: &[u8]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
    let mut offset = 0usize;
    let count = read_u32(data, &mut offset)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32(data, &mut offset)?;
        if len == u32::MAX {
            // Sentinel for missing values.
            out.push(None);
            continue;
        }
        let len = len as usize;
        anyhow::ensure!(offset + len <= data.len(), "short batch get value");
        out.push(Some(data[offset..offset + len].to_vec()));
        offset += len;
    }
    Ok(out)
}

/// Apply a KV command to a `KvEngine` (write-only operations).
fn apply_kv_command(kv: &dyn KvEngine, data: &[u8], version: Version) -> anyhow::Result<()> {
    anyhow::ensure!(!data.is_empty(), "empty command");
    match data[0] {
        CMD_SET => {
            let mut offset = 1;
            let key_len = read_u32(data, &mut offset)? as usize;
            anyhow::ensure!(offset + key_len <= data.len(), "short key");
            let key = data[offset..offset + key_len].to_vec();
            offset += key_len;

            let value_len = read_u32(data, &mut offset)? as usize;
            anyhow::ensure!(offset + value_len <= data.len(), "short value");
            let value = data[offset..offset + value_len].to_vec();

            kv.set(key, value, version);
            Ok(())
        }
        CMD_BATCH_SET => {
            let mut offset = 1;
            let count = read_u32(data, &mut offset)? as usize;
            for _ in 0..count {
                let key_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + key_len <= data.len(), "short key");
                let key = data[offset..offset + key_len].to_vec();
                offset += key_len;

                let value_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + value_len <= data.len(), "short value");
                let value = data[offset..offset + value_len].to_vec();
                offset += value_len;

                kv.set(key, value, version);
            }
            Ok(())
        }
        // GET commands are no-ops for write application.
        CMD_BATCH_GET => Ok(()),
        CMD_GET => Ok(()),
        // Reject unknown command tags to avoid corrupting state.
        other => anyhow::bail!("unknown command tag {other}"),
    }
}

/// Extract the read/write key sets from a command for dependency tracking.
fn command_keys(data: &[u8]) -> anyhow::Result<CommandKeys> {
    anyhow::ensure!(!data.is_empty(), "empty command");
    match data[0] {
        CMD_SET => {
            let mut offset = 1;
            let key_len = read_u32(data, &mut offset)? as usize;
            anyhow::ensure!(offset + key_len <= data.len(), "short key");
            let key = data[offset..offset + key_len].to_vec();
            Ok(CommandKeys {
                reads: Vec::new(),
                writes: vec![key],
            })
        }
        CMD_BATCH_SET => {
            let mut offset = 1;
            let count = read_u32(data, &mut offset)? as usize;
            let mut writes = Vec::with_capacity(count);
            for _ in 0..count {
                let key_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + key_len <= data.len(), "short key");
                let key = data[offset..offset + key_len].to_vec();
                offset += key_len;

                let value_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + value_len <= data.len(), "short value");
                offset += value_len;

                writes.push(key);
            }
            Ok(CommandKeys {
                reads: Vec::new(),
                writes,
            })
        }
        CMD_BATCH_GET => {
            let mut offset = 1;
            let count = read_u32(data, &mut offset)? as usize;
            let mut reads = Vec::with_capacity(count);
            for _ in 0..count {
                let key_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + key_len <= data.len(), "short key");
                let key = data[offset..offset + key_len].to_vec();
                offset += key_len;
                reads.push(key);
            }
            Ok(CommandKeys {
                reads,
                writes: Vec::new(),
            })
        }
        CMD_GET => {
            let mut offset = 1;
            let key_len = read_u32(data, &mut offset)? as usize;
            anyhow::ensure!(offset + key_len <= data.len(), "short key");
            let key = data[offset..offset + key_len].to_vec();
            Ok(CommandKeys {
                reads: vec![key],
                writes: Vec::new(),
            })
        }
        // Reject unknown command tags to avoid corrupting state.
        other => anyhow::bail!("unknown command tag {other}"),
    }
}

/// Read a big-endian u32 from `data` at `offset`.
fn read_u32(data: &[u8], offset: &mut usize) -> anyhow::Result<u32> {
    anyhow::ensure!(*offset + 4 <= data.len(), "short u32");
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*offset..*offset + 4]);
    *offset += 4;
    Ok(u32::from_be_bytes(buf))
}

/// Read a single byte from `data` at `offset`.
fn read_u8(data: &[u8], offset: &mut usize) -> anyhow::Result<u8> {
    anyhow::ensure!(*offset + 1 <= data.len(), "short u8");
    let out = data[*offset];
    *offset += 1;
    Ok(out)
}

/// Read a big-endian u64 from `data` at `offset`.
fn read_u64(data: &[u8], offset: &mut usize) -> anyhow::Result<u64> {
    anyhow::ensure!(*offset + 8 <= data.len(), "short u64");
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*offset..*offset + 8]);
    *offset += 8;
    Ok(u64::from_be_bytes(buf))
}
