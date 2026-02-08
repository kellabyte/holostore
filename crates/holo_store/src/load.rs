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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Snapshot of per-shard counters.
#[derive(Clone, Debug)]
pub struct ShardLoadSnapshot {
    pub set_ops: Vec<u64>,
    pub get_ops: Vec<u64>,
}

/// Tracks per-shard operation counters.
#[derive(Clone, Debug)]
pub struct ShardLoadTracker {
    set_ops: Arc<Vec<AtomicU64>>,
    get_ops: Arc<Vec<AtomicU64>>,
}

impl ShardLoadTracker {
    pub fn new(shards: usize) -> Self {
        let shards = shards.max(1);
        let set_ops = (0..shards).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        let get_ops = (0..shards).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        Self {
            set_ops: Arc::new(set_ops),
            get_ops: Arc::new(get_ops),
        }
    }

    pub fn shards(&self) -> usize {
        self.set_ops.len()
    }

    pub fn record_set_ops(&self, shard_index: usize, ops: u64) {
        if ops == 0 {
            return;
        }
        if let Some(counter) = self.set_ops.get(shard_index) {
            counter.fetch_add(ops, Ordering::Relaxed);
        }
    }

    pub fn record_get_ops(&self, shard_index: usize, ops: u64) {
        if ops == 0 {
            return;
        }
        if let Some(counter) = self.get_ops.get(shard_index) {
            counter.fetch_add(ops, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> ShardLoadSnapshot {
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
        ShardLoadSnapshot { set_ops, get_ops }
    }
}

