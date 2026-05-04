//! Lightweight per-shard load tracking for range management.
//!
//! This is intentionally simple: it tracks physical shard-index counters plus
//! descriptor-level logical counters for metadata-only split children.
//!
//! Notes:
//! - Counters are best-effort and node-local. Today the range manager runs on
//!   node 1, so it will only see load for traffic coordinated by node 1.
//! - Physical counters stay lock-free for the hot path.
//! - Logical counters are merged once per client batch, keeping metadata-only
//!   split telemetry cheap without duplicating the underlying storage group.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::kv;
/// Bucket count used for sampled hot-key concentration approximation.
pub const HOT_KEY_BUCKETS: usize = 32;
const HOT_KEY_SAMPLE_MASK: u64 = 0x0f; // 1 / 16 sampling
pub const LOGICAL_KEY_SAMPLE_CAP: usize = 128;
const LOGICAL_KEY_DELTA_SAMPLE_CAP: usize = 16;
const LOGICAL_KEY_SAMPLE_MASK: u64 = 0xff; // 1 / 256 sampling

/// Snapshot of per-shard counters.
///
/// Inputs:
/// - Point-in-time reads from `ShardLoadTracker` atomics.
///
/// Output:
/// - Immutable vectors consumed by split strategies for per-shard decisions.
#[derive(Clone, Debug)]
pub struct ShardLoadSnapshot {
    pub set_ops: Vec<u64>,
    pub get_ops: Vec<u64>,
    pub write_bytes: Vec<u64>,
    pub write_tail_latency_ms: Vec<f64>,
    pub hot_key_concentration_bps: Vec<u32>,
    pub write_hot_buckets: Vec<Vec<u64>>,
    pub read_hot_buckets: Vec<Vec<u64>>,
    pub logical_ranges: BTreeMap<u64, LogicalRangeLoadSnapshot>,
}

/// Point-in-time load counters for one logical range descriptor.
#[derive(Clone, Debug, Default)]
pub struct LogicalRangeLoadSnapshot {
    pub set_ops: u64,
    pub get_ops: u64,
    pub write_bytes: u64,
    pub write_tail_latency_ms: f64,
    pub hot_key_concentration_bps: u32,
    pub write_hot_buckets: Vec<u64>,
    pub read_hot_buckets: Vec<u64>,
    pub observed_min_key: Vec<u8>,
    pub observed_max_key: Vec<u8>,
    pub sampled_keys: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, Default)]
pub struct LogicalWriteDelta {
    pub ops: u64,
    pub bytes: u64,
    pub write_hot_buckets: [u64; HOT_KEY_BUCKETS],
    pub observed_min_key: Vec<u8>,
    pub observed_max_key: Vec<u8>,
    pub sampled_keys: Vec<Vec<u8>>,
}

impl LogicalWriteDelta {
    pub fn record(&mut self, key: &[u8], value_len: usize) {
        self.ops = self.ops.saturating_add(1);
        self.bytes = self
            .bytes
            .saturating_add((key.len().saturating_add(value_len)) as u64);
        if self.observed_min_key.is_empty() || key < self.observed_min_key.as_slice() {
            self.observed_min_key = key.to_vec();
        }
        if self.observed_max_key.is_empty() || key > self.observed_max_key.as_slice() {
            self.observed_max_key = key.to_vec();
        }
        if let Some(bucket) = sampled_hot_bucket(key) {
            if let Some(counter) = self.write_hot_buckets.get_mut(bucket) {
                *counter = counter.saturating_add(1);
            }
        }
        if self.sampled_keys.len() < LOGICAL_KEY_DELTA_SAMPLE_CAP && sampled_logical_split_key(key)
        {
            self.sampled_keys.push(key.to_vec());
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct LogicalReadDelta {
    pub ops: u64,
    pub read_hot_buckets: [u64; HOT_KEY_BUCKETS],
}

impl LogicalReadDelta {
    pub fn record(&mut self, key: &[u8]) {
        self.ops = self.ops.saturating_add(1);
        if let Some(bucket) = sampled_hot_bucket(key) {
            if let Some(counter) = self.read_hot_buckets.get_mut(bucket) {
                *counter = counter.saturating_add(1);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct LogicalRangeLoad {
    set_ops: u64,
    get_ops: u64,
    write_bytes: u64,
    write_tail_latency_us: u64,
    write_hot_buckets: [u64; HOT_KEY_BUCKETS],
    read_hot_buckets: [u64; HOT_KEY_BUCKETS],
    observed_min_key: Vec<u8>,
    observed_max_key: Vec<u8>,
    sampled_keys: Vec<Vec<u8>>,
    sampled_keys_seen: u64,
}

impl Default for LogicalRangeLoad {
    fn default() -> Self {
        Self {
            set_ops: 0,
            get_ops: 0,
            write_bytes: 0,
            write_tail_latency_us: 0,
            write_hot_buckets: [0; HOT_KEY_BUCKETS],
            read_hot_buckets: [0; HOT_KEY_BUCKETS],
            observed_min_key: Vec::new(),
            observed_max_key: Vec::new(),
            sampled_keys: Vec::new(),
            sampled_keys_seen: 0,
        }
    }
}

/// Tracks per-shard operation counters.
///
/// Design:
/// - All counters are lock-free atomics indexed by shard index.
/// - Hot-key tracking uses sampled hashed buckets to keep write-path overhead low.
///
/// Output:
/// - Provides fast `record_*` methods and periodic `snapshot()` reads.
#[derive(Clone, Debug)]
pub struct ShardLoadTracker {
    set_ops: Arc<Vec<AtomicU64>>,
    get_ops: Arc<Vec<AtomicU64>>,
    write_bytes: Arc<Vec<AtomicU64>>,
    // Decayed tail estimate in microseconds.
    write_tail_latency_us: Arc<Vec<AtomicU64>>,
    // Flattened [shard][bucket] sampled-key counters.
    write_hot_buckets: Arc<Vec<AtomicU64>>,
    read_hot_buckets: Arc<Vec<AtomicU64>>,
    logical_ranges: Arc<Mutex<BTreeMap<u64, LogicalRangeLoad>>>,
}

impl ShardLoadTracker {
    /// Build a tracker sized for `shards` data shards.
    ///
    /// Input:
    /// - `shards`: desired shard count (`0` is normalized to `1`).
    ///
    /// Output:
    /// - New tracker with zero-initialized counters.
    pub fn new(shards: usize) -> Self {
        let shards = shards.max(1);
        let set_ops = (0..shards).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        let get_ops = (0..shards).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        let write_bytes = (0..shards).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        let write_tail_latency_us = (0..shards).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        let write_hot_buckets = (0..(shards * HOT_KEY_BUCKETS))
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        let read_hot_buckets = (0..(shards * HOT_KEY_BUCKETS))
            .map(|_| AtomicU64::new(0))
            .collect::<Vec<_>>();
        Self {
            set_ops: Arc::new(set_ops),
            get_ops: Arc::new(get_ops),
            write_bytes: Arc::new(write_bytes),
            write_tail_latency_us: Arc::new(write_tail_latency_us),
            write_hot_buckets: Arc::new(write_hot_buckets),
            read_hot_buckets: Arc::new(read_hot_buckets),
            logical_ranges: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Return number of tracked shard indexes.
    pub fn shards(&self) -> usize {
        self.set_ops.len()
    }

    /// Add write operation count for one shard index.
    ///
    /// Inputs:
    /// - `shard_index`: target shard index.
    /// - `ops`: number of set operations to add.
    pub fn record_set_ops(&self, shard_index: usize, ops: u64) {
        if ops == 0 {
            return;
        }
        if let Some(counter) = self.set_ops.get(shard_index) {
            counter.fetch_add(ops, Ordering::Relaxed);
        }
    }

    /// Add logical write bytes for one shard index.
    ///
    /// Inputs:
    /// - `shard_index`: target shard index.
    /// - `bytes`: logical bytes written.
    pub fn record_set_bytes(&self, shard_index: usize, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if let Some(counter) = self.write_bytes.get(shard_index) {
            counter.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Add read operation count for one shard index.
    ///
    /// Inputs:
    /// - `shard_index`: target shard index.
    /// - `ops`: number of get operations to add.
    pub fn record_get_ops(&self, shard_index: usize, ops: u64) {
        if ops == 0 {
            return;
        }
        if let Some(counter) = self.get_ops.get(shard_index) {
            counter.fetch_add(ops, Ordering::Relaxed);
        }
    }

    /// Update decayed write-tail latency estimate for one shard.
    ///
    /// Inputs:
    /// - `shard_index`: target shard index.
    /// - `dur_us`: observed write latency in microseconds.
    ///
    /// Output:
    /// - Atomically updates per-shard conservative tail estimate.
    pub fn record_write_latency_us(&self, shard_index: usize, dur_us: u64) {
        let Some(counter) = self.write_tail_latency_us.get(shard_index) else {
            return;
        };
        // Conservative decayed tail estimator:
        // - Decay prior tail by 5% on each sample.
        // - Raise immediately to current latency if higher.
        loop {
            let cur = counter.load(Ordering::Relaxed);
            let decayed = cur.saturating_mul(95) / 100;
            let next = decayed.max(dur_us.max(1));
            if counter
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn record_logical_write_delta(&self, shard_id: u64, delta: LogicalWriteDelta) {
        if shard_id == 0 || delta.ops == 0 {
            return;
        }
        let Ok(mut logical_ranges) = self.logical_ranges.lock() else {
            return;
        };
        let entry = logical_ranges.entry(shard_id).or_default();
        entry.set_ops = entry.set_ops.saturating_add(delta.ops);
        entry.write_bytes = entry.write_bytes.saturating_add(delta.bytes);
        if !delta.observed_min_key.is_empty()
            && (entry.observed_min_key.is_empty()
                || delta.observed_min_key < entry.observed_min_key)
        {
            entry.observed_min_key = delta.observed_min_key;
        }
        if !delta.observed_max_key.is_empty()
            && (entry.observed_max_key.is_empty()
                || delta.observed_max_key > entry.observed_max_key)
        {
            entry.observed_max_key = delta.observed_max_key;
        }
        for idx in 0..HOT_KEY_BUCKETS {
            entry.write_hot_buckets[idx] =
                entry.write_hot_buckets[idx].saturating_add(delta.write_hot_buckets[idx]);
        }
        for key in delta.sampled_keys {
            entry.sampled_keys_seen = entry.sampled_keys_seen.saturating_add(1);
            if entry.sampled_keys.len() < LOGICAL_KEY_SAMPLE_CAP {
                entry.sampled_keys.push(key);
                continue;
            }
            let replace_at = logical_sample_replace_index(&key, entry.sampled_keys_seen);
            if replace_at < LOGICAL_KEY_SAMPLE_CAP {
                entry.sampled_keys[replace_at] = key;
            }
        }
    }

    pub fn record_logical_read_delta(&self, shard_id: u64, delta: LogicalReadDelta) {
        if shard_id == 0 || delta.ops == 0 {
            return;
        }
        let Ok(mut logical_ranges) = self.logical_ranges.lock() else {
            return;
        };
        let entry = logical_ranges.entry(shard_id).or_default();
        entry.get_ops = entry.get_ops.saturating_add(delta.ops);
        for idx in 0..HOT_KEY_BUCKETS {
            entry.read_hot_buckets[idx] =
                entry.read_hot_buckets[idx].saturating_add(delta.read_hot_buckets[idx]);
        }
    }

    /// Record a routed single-range write batch in one pass.
    ///
    /// Purpose:
    /// - Keep the no-split client SET fast path from paying separate physical
    ///   and logical telemetry passes over the same keys.
    ///
    /// Design:
    /// - Updates physical counters and logical range counters from one sampled
    ///   hash per key.
    /// - Preserves observed min/max and sampled split keys so automatic split
    ///   planning still has boundary hints after starting from one range.
    ///
    /// Inputs:
    /// - `shard_index`: physical shard index receiving the batch.
    /// - `shard_id`: logical descriptor id receiving the batch.
    /// - `items`: borrowed `(key, value_len)` pairs.
    pub fn record_single_range_write_batch<'a, I>(
        &self,
        shard_index: usize,
        shard_id: u64,
        items: I,
    ) where
        I: IntoIterator<Item = (&'a [u8], usize)>,
    {
        let mut ops = 0u64;
        let mut bytes = 0u64;
        let mut write_hot_buckets = [0u64; HOT_KEY_BUCKETS];
        let mut observed_min_key = Vec::new();
        let mut observed_max_key = Vec::new();
        let mut sampled_keys = Vec::new();

        for (key, value_len) in items {
            ops = ops.saturating_add(1);
            bytes = bytes.saturating_add((key.len().saturating_add(value_len)) as u64);
            if observed_min_key.is_empty() || key < observed_min_key.as_slice() {
                observed_min_key = key.to_vec();
            }
            if observed_max_key.is_empty() || key > observed_max_key.as_slice() {
                observed_max_key = key.to_vec();
            }

            let hash = kv::hash_key(key);
            if let Some(bucket) = sampled_hot_bucket_from_hash(hash) {
                if let Some(counter) = self
                    .write_hot_buckets
                    .get((shard_index * HOT_KEY_BUCKETS) + bucket)
                {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                write_hot_buckets[bucket] = write_hot_buckets[bucket].saturating_add(1);
            }
            if sampled_keys.len() < LOGICAL_KEY_DELTA_SAMPLE_CAP
                && sampled_logical_split_key_from_hash(hash)
            {
                sampled_keys.push(key.to_vec());
            }
        }

        self.record_set_ops(shard_index, ops);
        self.record_set_bytes(shard_index, bytes);
        if shard_id == 0 || ops == 0 {
            return;
        }
        let Ok(mut logical_ranges) = self.logical_ranges.lock() else {
            return;
        };
        let entry = logical_ranges.entry(shard_id).or_default();
        entry.set_ops = entry.set_ops.saturating_add(ops);
        entry.write_bytes = entry.write_bytes.saturating_add(bytes);
        if !observed_min_key.is_empty()
            && (entry.observed_min_key.is_empty() || observed_min_key < entry.observed_min_key)
        {
            entry.observed_min_key = observed_min_key;
        }
        if !observed_max_key.is_empty()
            && (entry.observed_max_key.is_empty() || observed_max_key > entry.observed_max_key)
        {
            entry.observed_max_key = observed_max_key;
        }
        for (idx, count) in write_hot_buckets.into_iter().enumerate() {
            entry.write_hot_buckets[idx] = entry.write_hot_buckets[idx].saturating_add(count);
        }
        for key in sampled_keys {
            entry.sampled_keys_seen = entry.sampled_keys_seen.saturating_add(1);
            if entry.sampled_keys.len() < LOGICAL_KEY_SAMPLE_CAP {
                entry.sampled_keys.push(key);
                continue;
            }
            let replace_at = logical_sample_replace_index(&key, entry.sampled_keys_seen);
            if replace_at < LOGICAL_KEY_SAMPLE_CAP {
                entry.sampled_keys[replace_at] = key;
            }
        }
    }

    /// Record a routed single-range read batch in one pass.
    ///
    /// Purpose:
    /// - Keep no-split GET telemetry cheap while retaining logical read counters
    ///   for the range manager.
    ///
    /// Inputs:
    /// - `shard_index`: physical shard index serving the batch.
    /// - `shard_id`: logical descriptor id serving the batch.
    /// - `keys`: borrowed read keys.
    pub fn record_single_range_read_batch<'a, I>(&self, shard_index: usize, shard_id: u64, keys: I)
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut ops = 0u64;
        let mut read_hot_buckets = [0u64; HOT_KEY_BUCKETS];
        for key in keys {
            ops = ops.saturating_add(1);
            let hash = kv::hash_key(key);
            if let Some(bucket) = sampled_hot_bucket_from_hash(hash) {
                if let Some(counter) = self
                    .read_hot_buckets
                    .get((shard_index * HOT_KEY_BUCKETS) + bucket)
                {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                read_hot_buckets[bucket] = read_hot_buckets[bucket].saturating_add(1);
            }
        }
        self.record_get_ops(shard_index, ops);
        if shard_id == 0 || ops == 0 {
            return;
        }
        let Ok(mut logical_ranges) = self.logical_ranges.lock() else {
            return;
        };
        let entry = logical_ranges.entry(shard_id).or_default();
        entry.get_ops = entry.get_ops.saturating_add(ops);
        for (idx, count) in read_hot_buckets.into_iter().enumerate() {
            entry.read_hot_buckets[idx] = entry.read_hot_buckets[idx].saturating_add(count);
        }
    }

    /// Record one sampled write key for hot-key concentration tracking.
    ///
    /// Inputs:
    /// - `shard_index`: target shard index.
    /// - `key`: written key bytes.
    pub fn record_write_key_sample(&self, shard_index: usize, key: &[u8]) {
        self.record_hot_key_sample(shard_index, key, true);
    }

    /// Record one sampled read key for hot-key concentration tracking.
    ///
    /// Inputs:
    /// - `shard_index`: target shard index.
    /// - `key`: read key bytes.
    pub fn record_read_key_sample(&self, shard_index: usize, key: &[u8]) {
        self.record_hot_key_sample(shard_index, key, false);
    }

    /// Internal hot-key sample recorder for read/write streams.
    ///
    /// Inputs:
    /// - `shard_index`: target shard index.
    /// - `key`: key bytes.
    /// - `write`: `true` routes to write buckets, `false` to read buckets.
    fn record_hot_key_sample(&self, shard_index: usize, key: &[u8], write: bool) {
        let Some(offset) = hot_bucket_offset(shard_index, key) else {
            return;
        };
        let buckets = if write {
            &self.write_hot_buckets
        } else {
            &self.read_hot_buckets
        };
        if let Some(counter) = buckets.get(offset) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Read a consistent point-in-time snapshot of all load counters.
    ///
    /// Output:
    /// - `ShardLoadSnapshot` containing per-shard totals, tail latency, and
    ///   sampled hot-key bucket summaries.
    pub fn snapshot(&self) -> ShardLoadSnapshot {
        let shards = self.shards();
        let set_ops = self
            .set_ops
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        let get_ops = self
            .get_ops
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        let write_bytes = self
            .write_bytes
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        let write_tail_latency_ms = self
            .write_tail_latency_us
            .iter()
            .map(|c| c.load(Ordering::Relaxed) as f64 / 1000.0)
            .collect::<Vec<_>>();

        let mut write_hot_buckets = vec![vec![0u64; HOT_KEY_BUCKETS]; shards];
        let mut read_hot_buckets = vec![vec![0u64; HOT_KEY_BUCKETS]; shards];
        let mut hot_key_concentration_bps = vec![0u32; shards];
        for shard_idx in 0..shards {
            let mut total = 0u64;
            let mut max_bucket = 0u64;
            for bucket in 0..HOT_KEY_BUCKETS {
                let flat = (shard_idx * HOT_KEY_BUCKETS) + bucket;
                let write_v = self
                    .write_hot_buckets
                    .get(flat)
                    .map(|c| c.load(Ordering::Relaxed))
                    .unwrap_or(0);
                let read_v = self
                    .read_hot_buckets
                    .get(flat)
                    .map(|c| c.load(Ordering::Relaxed))
                    .unwrap_or(0);
                write_hot_buckets[shard_idx][bucket] = write_v;
                read_hot_buckets[shard_idx][bucket] = read_v;
                let combined = write_v.saturating_add(read_v);
                total = total.saturating_add(combined);
                max_bucket = max_bucket.max(combined);
            }
            if total > 0 {
                hot_key_concentration_bps[shard_idx] =
                    ((max_bucket as u128 * 10_000u128) / total as u128) as u32;
            }
        }

        let logical_ranges = self
            .logical_ranges
            .lock()
            .ok()
            .map(|logical_ranges| {
                logical_ranges
                    .iter()
                    .map(|(shard_id, load)| {
                        let mut total = 0u64;
                        let mut max_bucket = 0u64;
                        for idx in 0..HOT_KEY_BUCKETS {
                            let combined = load.write_hot_buckets[idx]
                                .saturating_add(load.read_hot_buckets[idx]);
                            total = total.saturating_add(combined);
                            max_bucket = max_bucket.max(combined);
                        }
                        let hot_key_concentration_bps = if total > 0 {
                            ((max_bucket as u128 * 10_000u128) / total as u128) as u32
                        } else {
                            0
                        };
                        (
                            *shard_id,
                            LogicalRangeLoadSnapshot {
                                set_ops: load.set_ops,
                                get_ops: load.get_ops,
                                write_bytes: load.write_bytes,
                                write_tail_latency_ms: load.write_tail_latency_us as f64 / 1000.0,
                                hot_key_concentration_bps,
                                write_hot_buckets: load.write_hot_buckets.to_vec(),
                                read_hot_buckets: load.read_hot_buckets.to_vec(),
                                observed_min_key: load.observed_min_key.clone(),
                                observed_max_key: load.observed_max_key.clone(),
                                sampled_keys: load.sampled_keys.clone(),
                            },
                        )
                    })
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();

        ShardLoadSnapshot {
            set_ops,
            get_ops,
            write_bytes,
            write_tail_latency_ms,
            hot_key_concentration_bps,
            write_hot_buckets,
            read_hot_buckets,
            logical_ranges,
        }
    }
}

/// Map a sampled key to a flattened hot-bucket counter offset.
///
/// Inputs:
/// - `shard_index`: shard index namespace.
/// - `key`: key bytes to hash/sample.
///
/// Output:
/// - `Some(offset)` when the key is sampled, `None` when skipped by sampling mask.
fn hot_bucket_offset(shard_index: usize, key: &[u8]) -> Option<usize> {
    let bucket = sampled_hot_bucket(key)?;
    Some((shard_index * HOT_KEY_BUCKETS) + bucket)
}

pub fn sampled_hot_bucket(key: &[u8]) -> Option<usize> {
    let hash = kv::hash_key(key);
    sampled_hot_bucket_from_hash(hash)
}

fn sampled_hot_bucket_from_hash(hash: u64) -> Option<usize> {
    if (hash & HOT_KEY_SAMPLE_MASK) != 0 {
        return None;
    }
    Some(((hash >> 4) as usize) % HOT_KEY_BUCKETS)
}

fn sampled_logical_split_key(key: &[u8]) -> bool {
    sampled_logical_split_key_from_hash(kv::hash_key(key))
}

fn sampled_logical_split_key_from_hash(hash: u64) -> bool {
    (hash & LOGICAL_KEY_SAMPLE_MASK) == 0
}

fn logical_sample_replace_index(key: &[u8], seen: u64) -> usize {
    if seen == 0 {
        return usize::MAX;
    }
    let mixed = kv::hash_key(key) ^ seen.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ seen.rotate_left(17);
    (mixed % seen) as usize
}

#[cfg(test)]
mod tests {
    use super::ShardLoadTracker;

    #[test]
    fn single_range_write_batch_updates_physical_and_logical_counters() {
        let tracker = ShardLoadTracker::new(2);
        tracker.record_single_range_write_batch(
            1,
            42,
            [(b"k1".as_slice(), 3usize), (b"k2".as_slice(), 5usize)],
        );

        let snapshot = tracker.snapshot();
        assert_eq!(snapshot.set_ops[1], 2);
        assert_eq!(snapshot.write_bytes[1], 12);
        let logical = snapshot.logical_ranges.get(&42).expect("logical range");
        assert_eq!(logical.set_ops, 2);
        assert_eq!(logical.write_bytes, 12);
        assert_eq!(logical.observed_min_key, b"k1".to_vec());
        assert_eq!(logical.observed_max_key, b"k2".to_vec());
    }

    #[test]
    fn single_range_read_batch_updates_physical_and_logical_counters() {
        let tracker = ShardLoadTracker::new(2);
        tracker.record_single_range_read_batch(1, 42, [b"k1".as_slice(), b"k2".as_slice()]);

        let snapshot = tracker.snapshot();
        assert_eq!(snapshot.get_ops[1], 2);
        let logical = snapshot.logical_ranges.get(&42).expect("logical range");
        assert_eq!(logical.get_ops, 2);
    }
}
