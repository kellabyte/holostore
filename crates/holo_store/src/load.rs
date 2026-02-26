//! Lightweight per-shard load tracking for range management.
//!
//! This is intentionally simple: it tracks a few counters per *shard index* so
//! the (node-local) range manager can decide which range is "hot" and should
//! be split.
//!
//! Notes:
//! - Counters are best-effort and node-local. Today the range manager runs on
//!   node 1, so it will only see load for traffic coordinated by node 1.
//! - We track load by shard index (data group) rather than shard id (range
//!   descriptor). In the current implementation each range is backed by one
//!   shard index, so this is equivalent in practice.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Bucket count used for sampled hot-key concentration approximation.
pub const HOT_KEY_BUCKETS: usize = 32;
const HOT_KEY_SAMPLE_MASK: u64 = 0x0f; // 1 / 16 sampling

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

        ShardLoadSnapshot {
            set_ops,
            get_ops,
            write_bytes,
            write_tail_latency_ms,
            hot_key_concentration_bps,
            write_hot_buckets,
            read_hot_buckets,
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
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    if (hash & HOT_KEY_SAMPLE_MASK) != 0 {
        return None;
    }
    let bucket = ((hash >> 4) as usize) % HOT_KEY_BUCKETS;
    Some((shard_index * HOT_KEY_BUCKETS) + bucket)
}
