use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use fjall::{Keyspace, PartitionCreateOptions};
use holo_accord::accord::{CommandKeys, ExecMeta, StateMachine, TxnId};
use tracing::warn;

pub trait KvEngine: Send + Sync + 'static {
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>>;
    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)>;
    fn get_latest_batch(&self, keys: &[Vec<u8>]) -> Vec<Option<(Vec<u8>, Version)>>;
    fn set(&self, key: Vec<u8>, value: Vec<u8>, version: Version);
    fn set_batch(&self, items: &[(Vec<u8>, Vec<u8>, Version)]) {
        for (key, value, version) in items {
            self.set(key.clone(), value.clone(), *version);
        }
    }
    fn mark_visible(&self, key: &[u8], version: Version);
}

#[allow(dead_code)]
pub struct KvStore {
    inner: RwLock<HashMap<Vec<u8>, Vec<VersionedValue>>>,
}

impl KvStore {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl KvEngine for KvStore {
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>> {
        let guard = self.inner.read().ok()?;
        let versions = guard.get(key)?;
        find_visible_version(versions, version).map(|v| v.value.clone())
    }

    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)> {
        let guard = self.inner.read().ok()?;
        let versions = guard.get(key)?;
        let last = versions.iter().rev().find(|v| v.visible)?;
        Some((last.value.clone(), last.version))
    }

    fn get_latest_batch(&self, keys: &[Vec<u8>]) -> Vec<Option<(Vec<u8>, Version)>> {
        keys.iter().map(|k| self.get_latest(k)).collect()
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>, version: Version) {
        if let Ok(mut guard) = self.inner.write() {
            let entry = guard.entry(key).or_default();
            match entry.binary_search_by(|v| v.version.cmp(&version)) {
                Ok(idx) => entry[idx].value = value,
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

    fn set_batch(&self, items: &[(Vec<u8>, Vec<u8>, Version)]) {
        if let Ok(mut guard) = self.inner.write() {
            for (key, value, version) in items {
                let entry = guard.entry(key.clone()).or_default();
                match entry.binary_search_by(|v| v.version.cmp(version)) {
                    Ok(idx) => entry[idx].value = value.clone(),
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

    fn mark_visible(&self, key: &[u8], version: Version) {
        if let Ok(mut guard) = self.inner.write() {
            let Some(entry) = guard.get_mut(key) else {
                return;
            };
            if let Ok(idx) = entry.binary_search_by(|v| v.version.cmp(&version)) {
                entry[idx].visible = true;
            }
        }
    }
}

pub struct FjallEngine {
    keyspace: Arc<Keyspace>,
    versions: fjall::PartitionHandle,
    latest: fjall::PartitionHandle,
    lock: RwLock<()>,
}

pub fn hash_key(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

impl FjallEngine {
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
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>> {
        let _guard = self.lock.read().ok()?;
        if let Ok(Some(bytes)) = self.latest.get(key) {
            if let Ok((latest_version, latest_value)) = decode_latest_value(&bytes) {
                if latest_version <= version {
                    return Some(latest_value);
                }
            }
        }

        let prefix = encode_key_prefix(key);
        let mut iter = self.versions.prefix(prefix).rev();
        while let Some(Ok((entry_key, entry_value))) = iter.next() {
            let entry_version = decode_version_from_key(key, &entry_key)?;
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

    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)> {
        let _guard = self.lock.read().ok()?;
        let bytes = self.latest.get(key).ok().flatten()?;
        let (version, value) = decode_latest_value(&bytes).ok()?;
        Some((value, version))
    }

    fn get_latest_batch(&self, keys: &[Vec<u8>]) -> Vec<Option<(Vec<u8>, Version)>> {
        keys.iter().map(|k| self.get_latest(k)).collect()
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>, version: Version) {
        let _guard = match self.lock.write() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let entry_key = encode_version_key(&key, version);
        let entry_value = encode_version_value(false, &value);
        if let Err(err) = self.versions.insert(entry_key, entry_value) {
            warn!(error = ?err, "fjall kv write failed");
        }
    }

    fn set_batch(&self, items: &[(Vec<u8>, Vec<u8>, Version)]) {
        let _guard = match self.lock.write() {
            Ok(guard) => guard,
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

    fn mark_visible(&self, key: &[u8], version: Version) {
        let _guard = match self.lock.write() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let entry_key = encode_version_key(key, version);
        let entry_value = match self.versions.get(&entry_key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return,
            Err(err) => {
                warn!(error = ?err, "fjall kv read failed");
                return;
            }
        };

        let Ok((visible, value)) = decode_version_value(&entry_value) else {
            return;
        };
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
        if latest_update.map_or(true, |cur| version >= cur) {
            batch.insert(&self.latest, key.to_vec(), encode_latest_value(version, &value));
        }

        if let Err(err) = batch.commit() {
            warn!(error = ?err, "fjall kv mark visible failed");
        }
    }
}

pub struct ShardedKvEngine {
    shards: Vec<Arc<dyn KvEngine>>,
}

impl ShardedKvEngine {
    pub fn new(shards: Vec<Arc<dyn KvEngine>>) -> anyhow::Result<Self> {
        anyhow::ensure!(!shards.is_empty(), "sharded kv engine requires at least one shard");
        Ok(Self { shards })
    }

    fn shard_for_key(&self, key: &[u8]) -> usize {
        let hash = hash_key(key);
        (hash as usize) % self.shards.len()
    }
}

impl KvEngine for ShardedKvEngine {
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>> {
        let shard = self.shard_for_key(key);
        self.shards[shard].get(key, version)
    }

    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)> {
        let shard = self.shard_for_key(key);
        self.shards[shard].get_latest(key)
    }

    fn get_latest_batch(&self, keys: &[Vec<u8>]) -> Vec<Option<(Vec<u8>, Version)>> {
        if keys.is_empty() {
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

    fn set(&self, key: Vec<u8>, value: Vec<u8>, version: Version) {
        let shard = self.shard_for_key(&key);
        self.shards[shard].set(key, value, version);
    }

    fn set_batch(&self, items: &[(Vec<u8>, Vec<u8>, Version)]) {
        if items.is_empty() {
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
                continue;
            }
            self.shards[shard].set_batch(&batch);
        }
    }

    fn mark_visible(&self, key: &[u8], version: Version) {
        let shard = self.shard_for_key(key);
        self.shards[shard].mark_visible(key, version);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub seq: u64,
    pub txn_id: TxnId,
}

impl Version {
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
    fn from(meta: ExecMeta) -> Self {
        Self {
            seq: meta.seq,
            txn_id: meta.txn_id,
        }
    }
}

#[derive(Clone, Debug)]
struct VersionedValue {
    version: Version,
    value: Vec<u8>,
    visible: bool,
}

fn find_visible_version(versions: &[VersionedValue], version: Version) -> Option<&VersionedValue> {
    if versions.is_empty() {
        return None;
    }

    let idx = match versions.binary_search_by(|v| v.version.cmp(&version)) {
        Ok(idx) => idx,
        Err(0) => return None,
        Err(idx) => idx.saturating_sub(1),
    };

    for item in versions[..=idx].iter().rev() {
        if item.visible {
            return Some(item);
        }
    }
    None
}

#[allow(dead_code)]
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

fn encode_key_prefix(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

fn encode_version_key(key: &[u8], version: Version) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len() + 8 + 8 + 8);
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&version.seq.to_be_bytes());
    out.extend_from_slice(&version.txn_id.node_id.to_be_bytes());
    out.extend_from_slice(&version.txn_id.counter.to_be_bytes());
    out
}

fn decode_version_from_key(key: &[u8], entry_key: &[u8]) -> Option<Version> {
    if entry_key.len() < 4 {
        return None;
    }
    let mut offset = 0usize;
    let key_len = read_u32(entry_key, &mut offset).ok()? as usize;
    if offset + key_len + 8 + 8 + 8 > entry_key.len() {
        return None;
    }
    if key_len != key.len() || &entry_key[offset..offset + key_len] != key {
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

fn encode_version_value(visible: bool, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + value.len());
    out.push(visible as u8);
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
    out
}

fn decode_version_value(data: &[u8]) -> anyhow::Result<(bool, Vec<u8>)> {
    let mut offset = 0usize;
    let visible = read_u8(data, &mut offset)? != 0;
    let len = read_u32(data, &mut offset)? as usize;
    anyhow::ensure!(offset + len <= data.len(), "short version value");
    Ok((visible, data[offset..offset + len].to_vec()))
}

fn encode_latest_value(version: Version, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + 8 + 8 + 4 + value.len());
    out.extend_from_slice(&version.seq.to_be_bytes());
    out.extend_from_slice(&version.txn_id.node_id.to_be_bytes());
    out.extend_from_slice(&version.txn_id.counter.to_be_bytes());
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
    out
}

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

pub struct NoopStateMachine;

impl StateMachine for NoopStateMachine {
    fn command_keys(&self, _data: &[u8]) -> anyhow::Result<CommandKeys> {
        Ok(CommandKeys::default())
    }

    fn apply(&self, _data: &[u8], _meta: ExecMeta) {}
}

pub struct KvStateMachine {
    kv: Arc<dyn KvEngine>,
}

impl KvStateMachine {
    pub fn new(kv: Arc<dyn KvEngine>) -> Self {
        Self { kv }
    }
}

impl StateMachine for KvStateMachine {
    fn command_keys(&self, data: &[u8]) -> anyhow::Result<CommandKeys> {
        command_keys(data)
    }

    fn apply(&self, data: &[u8], meta: ExecMeta) {
        if let Err(err) = apply_kv_command(self.kv.as_ref(), data, meta.into()) {
            tracing::warn!(error = ?err, "failed to apply kv command");
        }
    }

    fn apply_batch(&self, items: &[(Vec<u8>, ExecMeta)]) {
        let mut sets: Vec<(Vec<u8>, Vec<u8>, Version)> = Vec::new();
        for (data, meta) in items {
            if data.is_empty() {
                continue;
            }
            let version = Version::from(*meta);
            match data[0] {
                CMD_SET => {
                    let mut offset = 1;
                    if let Ok(key_len) = read_u32(data, &mut offset) {
                        let key_len = key_len as usize;
                        if offset + key_len > data.len() {
                            continue;
                        }
                        let key = data[offset..offset + key_len].to_vec();
                        offset += key_len;
                        if let Ok(value_len) = read_u32(data, &mut offset) {
                            let value_len = value_len as usize;
                            if offset + value_len > data.len() {
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
                        continue;
                    };
                    for _ in 0..count {
                        let Ok(key_len) = read_u32(data, &mut offset) else {
                            break;
                        };
                        let key_len = key_len as usize;
                        if offset + key_len > data.len() {
                            break;
                        }
                        let key = data[offset..offset + key_len].to_vec();
                        offset += key_len;
                        let Ok(value_len) = read_u32(data, &mut offset) else {
                            break;
                        };
                        let value_len = value_len as usize;
                        if offset + value_len > data.len() {
                            break;
                        }
                        let value = data[offset..offset + value_len].to_vec();
                        offset += value_len;
                        sets.push((key, value, version));
                    }
                }
                _ => {}
            }
        }

        if !sets.is_empty() {
            self.kv.set_batch(&sets);
        }
    }

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
            _ => Ok(None),
        }
    }

    fn mark_visible(&self, data: &[u8], meta: ExecMeta) {
        if data.is_empty() {
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

const CMD_SET: u8 = 1;
const CMD_GET: u8 = 2;
const CMD_BATCH_SET: u8 = 3;
const CMD_BATCH_GET: u8 = 4;

pub fn encode_set(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
    out.push(CMD_SET);
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
    out
}

pub fn encode_get(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + key.len());
    out.push(CMD_GET);
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

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

pub fn decode_batch_get_result(data: &[u8]) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
    let mut offset = 0usize;
    let count = read_u32(data, &mut offset)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_u32(data, &mut offset)?;
        if len == u32::MAX {
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
        CMD_BATCH_GET => Ok(()),
        CMD_GET => Ok(()),
        other => anyhow::bail!("unknown command tag {other}"),
    }
}

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
        other => anyhow::bail!("unknown command tag {other}"),
    }
}

fn read_u32(data: &[u8], offset: &mut usize) -> anyhow::Result<u32> {
    anyhow::ensure!(*offset + 4 <= data.len(), "short u32");
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*offset..*offset + 4]);
    *offset += 4;
    Ok(u32::from_be_bytes(buf))
}

fn read_u8(data: &[u8], offset: &mut usize) -> anyhow::Result<u8> {
    anyhow::ensure!(*offset + 1 <= data.len(), "short u8");
    let out = data[*offset];
    *offset += 1;
    Ok(out)
}

fn read_u64(data: &[u8], offset: &mut usize) -> anyhow::Result<u64> {
    anyhow::ensure!(*offset + 8 <= data.len(), "short u64");
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*offset..*offset + 8]);
    *offset += 8;
    Ok(u64::from_be_bytes(buf))
}
