//! gRPC transport layer used by the Accord consensus engine.
//!
//! Purpose:
//! - Provide low-latency, backpressure-aware RPC transport for Accord quorum
//!   rounds and read paths.
//!
//! Design:
//! - Build per-peer batching workers with bounded in-flight RPC concurrency.
//! - Keep worker loops single-task and cancellation-friendly by polling
//!   in-flight futures directly instead of spawning per-batch helper tasks.
//! - Surface detailed queue, wait, and latency counters for tuning.
//!
//! Inputs:
//! - Local transport method calls (`pre_accept`, `accept`, `commit`, `recover`,
//!   and read RPCs), membership updates, and environment-derived batching knobs.
//!
//! Outputs:
//! - Timed/batched gRPC requests to peers, fan-out responses to callers, and
//!   per-peer and global transport telemetry snapshots.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ahash::RandomState;
use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, StreamExt};
use hashbrown::HashMap as FastHashMap;
use holo_accord::accord::{
    AcceptRequest, AcceptResponse, Ballot, CommitRequest, CommitResponse, ExecutedPrefix, GroupId,
    NodeId, PreAcceptRequest, PreAcceptResponse, RecoverRequest, RecoverResponse,
    ReportExecutedRequest, ReportExecutedResponse, Transport, TxnId, TxnStatus,
};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio::time;

use crate::kv::Version;
use crate::packed_batch::{decode_items_from_parts, encode_items_to_parts, PackedBatchBuilder};
use crate::volo_gen::holo_store::rpc;

/// Capacity for each per-peer RPC queue.
const RPC_QUEUE_CAPACITY: usize = 4096;

/// Maximum number of reusable vectors cached per worker lane.
const REUSE_POOL_MAX_CACHED: usize = 64;

/// Upper bound multiplier for retained reusable vector capacities.
const REUSE_POOL_CAPACITY_MULTIPLIER: usize = 4;

/// Maximum multiplier from configured batch wait to enforced GET queue-age cap.
const KV_GET_MAX_QUEUE_WAIT_MULTIPLIER: u32 = 4;

/// Minimum enforced GET queue-age cap (microseconds).
const KV_GET_MIN_QUEUE_WAIT_US: u64 = 250;
/// Max coalescing delay for low-backlog GET batches (microseconds).
const KV_GET_LOW_BACKLOG_WAIT_US: u64 = 50;
/// Queue-depth threshold for immediate GET dispatch when limiter capacity exists.
const KV_GET_IMMEDIATE_DISPATCH_QUEUE_DEPTH: u64 = 1;

/// Packed-v2 lane capability state: enabled / disabled.
const V2_CAP_ENABLED: u8 = 1;
const V2_CAP_DISABLED: u8 = 2;

/// Histogram bucket boundaries for latency metrics (microseconds).
const LATENCY_BUCKETS_US: [u64; 12] = [
    100,     // 0.1ms
    250,     // 0.25ms
    500,     // 0.5ms
    1_000,   // 1ms
    2_000,   // 2ms
    5_000,   // 5ms
    10_000,  // 10ms
    20_000,  // 20ms
    50_000,  // 50ms
    100_000, // 100ms
    200_000, // 200ms
    500_000, // 500ms
];

type FastMap<K, V> = FastHashMap<K, V, RandomState>;

/// gRPC-based transport that implements Accord's `Transport` trait.
#[derive(Clone)]
pub struct GrpcTransport {
    peers: Arc<std::sync::RwLock<HashMap<NodeId, Peer>>>,
    rpc_timeout: Duration,
    commit_timeout: Duration,
    stats: Arc<RpcStats>,
    inflight_tuning: InflightTuning,
    rpc_batch_max: usize,
    rpc_batch_wait: Duration,
    inflight_limit: usize,
}

/// One latest-visible KV row used for replica backfill.
#[derive(Clone, Debug)]
pub struct RangeLatestEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: Version,
}

/// Per-range telemetry sampled from one node.
#[derive(Clone, Debug, Default)]
pub struct RangeTelemetryStat {
    pub shard_id: u64,
    pub shard_index: usize,
    pub record_count: u64,
    pub is_leaseholder: bool,
    pub write_ops_total: u64,
    pub read_ops_total: u64,
    pub write_bytes_total: u64,
    pub queue_depth: u64,
    pub write_tail_latency_ms: f64,
    pub hot_key_concentration_bps: u32,
    pub write_hot_buckets: Vec<u64>,
    pub read_hot_buckets: Vec<u64>,
}

/// Collect a batch of items from a channel with a size/time bound, reusing
/// caller-provided vector storage.
///
/// Purpose:
/// - Build one bounded batch while minimizing allocator churn on hot lanes.
///
/// Design:
/// - Clear and reuse the supplied `items` buffer.
/// - Include a required first item.
/// - Fill additional items via `try_recv` fast path.
/// - If temporarily empty, wait for either next item or batching deadline.
///
/// Inputs:
/// - `items`: caller-owned reusable vector buffer.
/// - `first`: first already-dequeued item.
/// - `rx`: lane receiver.
/// - `batch_max`: maximum batch size.
/// - `batch_wait`: soft wait window for coalescing.
///
/// Outputs:
/// - Batch vector with `1..=batch_max` items unless channel disconnects.
#[cfg(test)]
async fn collect_batch_reuse<T>(
    mut items: Vec<T>,
    first: T,
    rx: &mut mpsc::Receiver<T>,
    batch_max: usize,
    batch_wait: Duration,
) -> Vec<T> {
    items.clear();
    let target_capacity = batch_max.max(1);
    if items.capacity() < target_capacity {
        // Grow once up to the configured batch target; we intentionally avoid
        // shrinking so callers can keep reusing this allocation.
        items.reserve(target_capacity - items.capacity());
    }
    items.push(first);
    if batch_max <= 1 {
        return items;
    }

    // Decide whether to use a batching deadline based on the configured wait.
    let deadline = if batch_wait.is_zero() {
        None
    } else {
        Some(time::Instant::now() + batch_wait)
    };

    'outer: loop {
        // Stop when we hit the batch size limit.
        if items.len() >= batch_max {
            break;
        }
        match rx.try_recv() {
            Ok(item) => {
                items.push(item);
                continue;
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }

        // No deadline means we are done collecting.
        let Some(deadline) = deadline else {
            break;
        };
        let now = time::Instant::now();
        // Stop when the batching deadline expires.
        if now >= deadline {
            break;
        }

        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(item) => items.push(item),
                    None => break 'outer,
                }
            }
            _ = time::sleep_until(deadline) => {
                break;
            }
        }
    }

    items
}

/// Collect a batch while still polling already in-flight RPC futures.
///
/// Purpose:
/// - Preserve batch coalescing while preventing active RPCs from stalling during
///   the batching wait window.
///
/// Design:
/// - Reuse caller-provided vector storage to avoid per-batch allocation churn.
/// - Start from a required first item.
/// - Fill up to `batch_max` using immediate `try_recv`.
/// - When queue is briefly empty, wait on either: next item, batching deadline,
///   or one in-flight RPC completion.
/// - Forward each completed in-flight output to `on_in_flight` so caller-owned
///   buffer pools can reclaim memory immediately.
///
/// Inputs:
/// - `items`: reusable vector buffer supplied by caller.
/// - `first`: first work item already dequeued by caller.
/// - `rx`: work queue for the same peer+RPC lane.
/// - `batch_max`: hard cap on items in one batch.
/// - `batch_wait`: soft batching window.
/// - `in_flight`: currently running batch RPC futures.
/// - `on_in_flight`: callback for each completed in-flight output.
///
/// Outputs:
/// - A batch vector sized in `[1, batch_max]` unless channel is disconnected.
async fn collect_batch_with_progress<T, Fut, F>(
    mut items: Vec<T>,
    first: T,
    rx: &mut mpsc::Receiver<T>,
    batch_max: usize,
    batch_wait: Duration,
    in_flight: &mut FuturesUnordered<Fut>,
    mut on_in_flight: F,
) -> Vec<T>
where
    Fut: std::future::Future,
    F: FnMut(Fut::Output),
{
    items.clear();
    let target_capacity = batch_max.max(1);
    if items.capacity() < target_capacity {
        // Grow once up to the configured batch target; we intentionally avoid
        // shrinking here so the caller can reuse this allocation on later batches.
        items.reserve(target_capacity - items.capacity());
    }
    items.push(first);
    if batch_max <= 1 {
        return items;
    }

    let deadline = if batch_wait.is_zero() {
        None
    } else {
        Some(time::Instant::now() + batch_wait)
    };

    // Keep filling until size/deadline/channel constraints stop us.
    'outer: loop {
        if items.len() >= batch_max {
            break;
        }
        match rx.try_recv() {
            Ok(item) => {
                items.push(item);
                continue;
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }

        let Some(deadline) = deadline else {
            break;
        };
        if time::Instant::now() >= deadline {
            break;
        }

        // While waiting for another queue item, we also poll in-flight RPCs so
        // they can make forward progress and release limiter permits.
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(item) => items.push(item),
                    None => break 'outer,
                }
            }
            _ = time::sleep_until(deadline) => {
                break;
            }
            done = in_flight.next(), if !in_flight.is_empty() => {
                // Reclaim completion outputs immediately. This branch exists to
                // keep in-flight futures progressing; it does not dequeue extra
                // queue work beyond the current batch window.
                if let Some(output) = done {
                    on_in_flight(output);
                }
            }
        }
    }

    items
}

/// Bounded pool of reusable vectors.
///
/// Purpose:
/// - Reuse vector allocations across hot-path batch completions.
///
/// Design:
/// - Stores cleared vectors up to `max_cached`.
/// - Rejects oversized vectors (capacity above `max_capacity`) so one spike does
///   not pin excessive memory.
/// - Serves vectors by minimum required capacity, otherwise allocates.
///
/// Inputs:
/// - Capacity bounds and candidate vectors to cache/reuse.
///
/// Outputs:
/// - Reusable vectors with amortized allocation cost and bounded retained memory.
struct ReuseVecPool<T> {
    free: Vec<Vec<T>>,
    max_cached: usize,
    max_capacity: usize,
}

impl<T> ReuseVecPool<T> {
    /// Create a new reusable-vector pool.
    ///
    /// Purpose:
    /// - Configure bounded vector reuse for one worker lane.
    ///
    /// Design:
    /// - Clamps cache-size and capacity bounds to at least 1.
    ///
    /// Inputs:
    /// - `max_cached`: max vectors retained in the pool.
    /// - `max_capacity`: largest vector capacity allowed to remain cached.
    ///
    /// Outputs:
    /// - Empty pool ready for `take`/`put`.
    fn new(max_cached: usize, max_capacity: usize) -> Self {
        Self {
            free: Vec::new(),
            max_cached: max_cached.max(1),
            max_capacity: max_capacity.max(1),
        }
    }

    /// Take a vector with at least `min_capacity`, reusing one if possible.
    ///
    /// Purpose:
    /// - Avoid allocating for common batch sizes.
    ///
    /// Design:
    /// - Finds any cached vector with enough capacity and clears it.
    /// - Falls back to allocating exactly `min_capacity`.
    ///
    /// Inputs:
    /// - `min_capacity`: lower bound on returned vector capacity.
    ///
    /// Outputs:
    /// - Empty vector ready for caller writes.
    fn take(&mut self, min_capacity: usize) -> Vec<T> {
        let min_capacity = min_capacity.max(1);
        if let Some(idx) = self
            .free
            .iter()
            .position(|candidate| candidate.capacity() >= min_capacity)
        {
            let mut reused = self.free.swap_remove(idx);
            reused.clear();
            return reused;
        }
        Vec::with_capacity(min_capacity)
    }

    /// Return a vector to the pool if it is safe and useful to retain.
    ///
    /// Purpose:
    /// - Bound memory usage while still reusing typical batch allocations.
    ///
    /// Design:
    /// - Clears vector contents.
    /// - Drops vectors that are too large or when cache is already full.
    ///
    /// Inputs:
    /// - `vec`: candidate vector to recycle.
    ///
    /// Outputs:
    /// - Vector is cached for future `take` or dropped.
    fn put(&mut self, mut vec: Vec<T>) {
        vec.clear();
        if vec.capacity() == 0 {
            return;
        }
        // Do not cache pathological one-off spikes.
        if vec.capacity() > self.max_capacity {
            return;
        }
        if self.free.len() >= self.max_cached {
            return;
        }
        self.free.push(vec);
    }
}

/// Recycled waiter/request vectors for one completed quorum RPC batch.
///
/// Purpose:
/// - Return sender and request vector capacities to worker pools after one
///   batch completes.
///
/// Design:
/// - Holds drained sender and request storage only.
/// - Sender values are consumed before return.
///
/// Inputs:
/// - Drained sender and request vectors from one in-flight batch.
///
/// Outputs:
/// - Reusable sender/request vector capacities for subsequent batches.
struct WaiterBatchRecycle<T, Req> {
    txs: Vec<oneshot::Sender<anyhow::Result<T>>>,
    requests: Vec<Req>,
}

/// Recycled transaction-id/request vectors for one completed recover batch.
///
/// Purpose:
/// - Return txn-id and request vector capacities to worker pools after
///   completion.
///
/// Design:
/// - Holds drained txn-id and request storage only; values are consumed before
///   return.
///
/// Inputs:
/// - Drained txn-id and request vectors from one in-flight recover batch.
///
/// Outputs:
/// - Reusable txn-id/request vector capacities for subsequent recover batches.
struct RecoverBatchRecycle {
    txn_ids: Vec<TxnId>,
    requests: Vec<rpc::RecoverRequest>,
}

/// Per-peer RPC state and queues.
///
/// Purpose:
/// - Hold all peer-local channels, clients, and limiter handles.
///
/// Design:
/// - Separates read and consensus clients.
/// - Keeps dedicated GET limiter independent from consensus lane limiters.
///
/// Inputs:
/// - Built from membership and runtime tuning settings.
///
/// Outputs:
/// - Shared by transport call paths and peer snapshot collection.
#[derive(Clone)]
struct Peer {
    client: rpc::HoloRpcClient,
    read_client: rpc::HoloRpcClient,
    kv_get_tx: mpsc::Sender<KvGetWork>,
    pre_accept_tx: mpsc::Sender<PreAcceptWork>,
    accept_tx: mpsc::Sender<AcceptWork>,
    commit_tx: mpsc::Sender<CommitWork>,
    recover_tx: mpsc::Sender<RecoverWork>,
    stats: Arc<PeerStats>,
    recover_coalescer: Arc<RecoverCoalescer>,
    pre_accept_limiter: Arc<InflightLimiter>,
    accept_limiter: Arc<InflightLimiter>,
    commit_limiter: Arc<InflightLimiter>,
    recover_limiter: Arc<InflightLimiter>,
    kv_get_limiter: Arc<InflightLimiter>,
    kv_batch_get_v2_capability: Arc<BatchLaneCapability>,
}

/// Work item for pre-accept RPCs.
struct PreAcceptWork {
    req: PreAcceptRequest,
    tx: oneshot::Sender<anyhow::Result<PreAcceptResponse>>,
    enqueued_at: std::time::Instant,
}

/// Work item for accept RPCs.
struct AcceptWork {
    req: AcceptRequest,
    tx: oneshot::Sender<anyhow::Result<AcceptResponse>>,
    enqueued_at: std::time::Instant,
}

/// Work item for commit RPCs.
struct CommitWork {
    req: CommitRequest,
    tx: oneshot::Sender<anyhow::Result<CommitResponse>>,
    enqueued_at: std::time::Instant,
}

/// Work item for recover RPCs (no response channel; coalescer handles fan-out).
struct RecoverWork {
    req: RecoverRequest,
    enqueued_at: std::time::Instant,
}

/// Return current epoch time in microseconds (saturating).
fn epoch_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

type KvGetResult = anyhow::Result<Option<(Vec<u8>, Version)>>;
type KvGetReplyTx = oneshot::Sender<KvGetResult>;

/// Work item for per-key GET RPCs.
struct KvGetWork {
    key: Vec<u8>,
    tx: KvGetReplyTx,
    enqueued_at: std::time::Instant,
}

/// Coalesced recovery entry with a maximal ballot and waiting responders.
struct RecoverEntry {
    ballot: Ballot,
    waiters: Vec<oneshot::Sender<anyhow::Result<RecoverResponse>>>,
}

/// Tuning knobs for adaptive in-flight limits based on queue depth and wait time.
#[derive(Clone, Copy)]
pub struct InflightTuning {
    pub min: usize,
    pub max: usize,
    pub high_wait_ms: f64,
    pub low_wait_ms: f64,
    pub high_queue: u64,
    pub low_queue: u64,
}

/// Adaptive limiter that caps concurrent in-flight RPCs.
struct InflightLimiter {
    limit: AtomicUsize,
    in_flight: AtomicUsize,
    notify: Notify,
    min: usize,
    max: usize,
}

impl InflightLimiter {
    /// Create a limiter with an initial limit clamped within min/max.
    fn new(initial: usize, min: usize, max: usize) -> Self {
        let min = min.max(1);
        let max = max.max(min);
        let initial = initial.clamp(min, max);
        Self {
            limit: AtomicUsize::new(initial),
            in_flight: AtomicUsize::new(0),
            notify: Notify::new(),
            min,
            max,
        }
    }

    /// Acquire a permit, waiting until in-flight count is below the limit.
    async fn acquire(self: &Arc<Self>) -> InflightPermit {
        loop {
            let limit = self.limit.load(Ordering::Relaxed).max(1);
            let current = self.in_flight.load(Ordering::Relaxed);
            if current < limit {
                if self
                    .in_flight
                    .compare_exchange(current, current + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    return InflightPermit {
                        limiter: self.clone(),
                    };
                }
                // CAS failed, retry with updated counters.
                continue;
            }
            self.notify.notified().await;
        }
    }

    /// Attempt to acquire a permit without waiting.
    ///
    /// Purpose:
    /// - Let batching workers avoid blocking on limiter saturation while they
    ///   continue polling already in-flight RPC futures.
    ///
    /// Design:
    /// - Single compare-exchange against current in-flight count.
    /// - No retries or waits; caller decides whether to fall back to blocking.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Outputs:
    /// - `Some(InflightPermit)` when capacity is available.
    /// - `None` when current in-flight count is at/above limit.
    fn try_acquire(self: &Arc<Self>) -> Option<InflightPermit> {
        let limit = self.limit.load(Ordering::Relaxed).max(1);
        let current = self.in_flight.load(Ordering::Relaxed);
        if current >= limit {
            return None;
        }
        if self
            .in_flight
            .compare_exchange(current, current + 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            Some(InflightPermit {
                limiter: self.clone(),
            })
        } else {
            None
        }
    }

    /// Release a permit and wake one waiter.
    fn release(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        self.notify.notify_one();
    }

    /// Return the current in-flight limit.
    fn current(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    /// Adjust the in-flight limit, clamped to min/max.
    fn adjust(&self, desired: usize) -> bool {
        let desired = desired.clamp(self.min, self.max);
        let prev = self.limit.swap(desired, Ordering::Relaxed);
        if desired != prev {
            // Notify waiters so they can proceed under the new limit.
            self.notify.notify_waiters();
            true
        } else {
            false
        }
    }
}

/// Acquire an in-flight permit while keeping active RPC futures progressing.
///
/// Purpose:
/// - Avoid head-of-line stalls when limiter capacity is exhausted.
///
/// Design:
/// - Try fast-path non-blocking acquire.
/// - If saturated, wait for one in-flight future to finish, then retry.
/// - Fall back to blocking acquire only when no futures are registered.
///
/// Inputs:
/// - `limiter`: adaptive concurrency limiter for one RPC lane.
/// - `in_flight`: futures currently executing for that lane.
///
/// Outputs:
/// - One acquired permit.
async fn acquire_with_progress<Fut>(
    limiter: &Arc<InflightLimiter>,
    in_flight: &mut FuturesUnordered<Fut>,
) -> InflightPermit
where
    Fut: std::future::Future,
{
    loop {
        if let Some(permit) = limiter.try_acquire() {
            return permit;
        }

        // When saturated, wait for one completion before retrying. This does
        // not dequeue more work while the lane is fully utilized.
        if in_flight.next().await.is_none() {
            // Defensive fallback: if no tracked futures remain, use blocking
            // acquire to preserve correctness under transient races.
            return limiter.acquire().await;
        }
    }
}

/// Per-batch capability path counters.
///
/// Purpose:
/// - Surface packed-v2 attempt/disable decisions for telemetry.
///
/// Design:
/// - Mutable per-call accumulator that callers merge into lane stats.
///
/// Inputs:
/// - Updated inside v2-only batch call helpers.
///
/// Outputs:
/// - One summarized path decision for the executed batch.
#[derive(Default, Clone, Copy)]
struct BatchPathStats {
    v2_attempted: bool,
    v2_disabled_event: bool,
}

/// Packed-v2 capability state for one peer+RPC lane.
///
/// Purpose:
/// - Keep v2-only lanes on the packed fast path and suppress repeated retries
///   after explicit incompatibility responses.
///
/// Design:
/// - `state` tracks Enabled/Disabled capability.
/// - All non-disabled lanes attempt v2.
/// - Explicit `Unimplemented` marks lane disabled to fail fast on subsequent
///   calls.
///
/// Inputs:
/// - Updated on every batched RPC attempt.
///
/// Outputs:
/// - Deterministic lane-level capability decisions.
#[derive(Default)]
struct BatchLaneCapability {
    state: AtomicU8,
}

impl BatchLaneCapability {
    /// Return whether this lane should attempt packed-v2.
    ///
    /// Purpose:
    /// - Decide whether this call should attempt packed-v2.
    ///
    /// Design:
    /// - Enabled lanes always try v2.
    /// - Disabled lanes never try v2.
    /// - Unknown lanes also try v2 optimistically.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Outputs:
    /// - `true` when this call should attempt packed-v2.
    fn should_attempt_v2(&self) -> bool {
        // A disabled lane has already returned explicit Unimplemented; we
        // intentionally do not retry to avoid repeated compatibility stalls.
        !matches!(self.state.load(Ordering::Relaxed), V2_CAP_DISABLED)
    }

    /// Mark a successful packed-v2 call.
    ///
    /// Purpose:
    /// - Persist v2 capability after a successful packed-v2 call.
    ///
    /// Design:
    /// - Transitions state to enabled.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Outputs:
    /// - Lane remains on fast path for future batches.
    fn mark_v2_success(&self) {
        self.state.store(V2_CAP_ENABLED, Ordering::Relaxed);
    }

    /// Mark explicit v2 unavailability (`Unimplemented`).
    ///
    /// Purpose:
    /// - Disable future packed-v2 attempts for this lane.
    ///
    /// Design:
    /// - Writes disabled state once.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Outputs:
    /// - `true` when this call changed lane state to disabled.
    fn mark_v2_unimplemented(&self) -> bool {
        self.state.swap(V2_CAP_DISABLED, Ordering::Relaxed) != V2_CAP_DISABLED
    }
}

/// Compute a strict maximum GET queue-age bound from configured batching wait.
///
/// Purpose:
/// - Bound additional batching delay so stale queued GETs flush quickly.
///
/// Design:
/// - Uses a small floor for tiny waits.
/// - Scales with configured batch wait to preserve coalescing for healthy load.
///
/// Inputs:
/// - `batch_wait`: configured per-lane coalescing window.
///
/// Outputs:
/// - Maximum allowed head-of-queue age before immediate dispatch.
fn kv_get_max_queue_wait(batch_wait: Duration) -> Duration {
    let scaled = batch_wait
        .checked_mul(KV_GET_MAX_QUEUE_WAIT_MULTIPLIER)
        .unwrap_or(Duration::from_secs(1));
    scaled.max(Duration::from_micros(KV_GET_MIN_QUEUE_WAIT_US))
}

/// Clamp GET batch wait so head-of-queue age never exceeds a strict cap.
///
/// Purpose:
/// - Prevent split-stall amplification from waiting an extra full batch window
///   on already-stale queue heads.
///
/// Design:
/// - If current head age already exceeds `max_queue_wait`, return zero wait.
/// - Otherwise return the smaller of configured batch wait and remaining age
///   budget.
///
/// Inputs:
/// - `head_enqueued_at`: enqueue timestamp of first item in batch.
/// - `batch_start`: dequeue/build start timestamp.
/// - `batch_wait`: configured coalescing window.
/// - `max_queue_wait`: strict queue-age cap.
/// - `queued_items_hint`: queue depth estimate including the head item.
///
/// Outputs:
/// - Effective coalescing wait for this batch.
fn kv_get_effective_batch_wait(
    head_enqueued_at: std::time::Instant,
    batch_start: std::time::Instant,
    batch_wait: Duration,
    max_queue_wait: Duration,
    queued_items_hint: u64,
) -> Duration {
    if batch_wait.is_zero() || max_queue_wait.is_zero() {
        return Duration::ZERO;
    }
    let head_age = batch_start.saturating_duration_since(head_enqueued_at);
    if head_age >= max_queue_wait {
        // Head item already waited too long in queue: dispatch immediately and
        // do not wait for extra coalescing.
        return Duration::ZERO;
    }
    let remaining = max_queue_wait - head_age;
    let mut effective = batch_wait.min(remaining);
    if queued_items_hint <= 1 {
        // With only the head item queued, keep a tiny coalescing window so we
        // can capture immediately arriving neighbors without adding a full batch
        // wait to median latency.
        effective = effective.min(Duration::from_micros(KV_GET_LOW_BACKLOG_WAIT_US));
    }
    effective
}

/// Decide whether the GET lane should dispatch immediately without coalescing.
///
/// Purpose:
/// - Protect median read latency when backlog is tiny and limiter capacity is
///   available.
///
/// Design:
/// - Requires both low queue depth and immediate limiter capacity.
/// - Intentionally does not bypass batching under backlog/saturation.
///
/// Inputs:
/// - `queued_items_hint`: queue depth estimate including current head.
/// - `limiter_has_capacity`: non-blocking limiter capacity probe result.
///
/// Outputs:
/// - `true` when current head should skip batching wait.
fn kv_get_should_dispatch_immediately(queued_items_hint: u64, limiter_has_capacity: bool) -> bool {
    limiter_has_capacity && queued_items_hint <= KV_GET_IMMEDIATE_DISPATCH_QUEUE_DEPTH
}

/// Execute one batch RPC using packed-v2 only.
///
/// Purpose:
/// - Use compact packed unary RPCs on all peers in v2-only deployments.
///
/// Design:
/// - Attempt v2 unless lane capability is disabled.
/// - On `Unimplemented`, disable v2 for this lane and fail fast.
/// - Reuse caller-owned request vector on successful v2.
///
/// Inputs:
/// - `timeout`: RPC timeout for packed-v2 call.
/// - `lane_capability`: lane capability state.
/// - `requests`: caller-owned request vector for one batch.
/// - `rpc_name`: log/context name for error text.
/// - `v2_call`: closure invoking v2 packed RPC.
/// - `path_stats`: per-call capability path counters.
///
/// Outputs:
/// - `(responses, recyclable_requests)` where `recyclable_requests` retains
///   caller vector capacity when v2 succeeds.
async fn call_batch_v2_only<Req, Resp, V2Call, V2Future>(
    timeout: Duration,
    lane_capability: &BatchLaneCapability,
    requests: Vec<Req>,
    rpc_name: &'static str,
    v2_call: V2Call,
    path_stats: &mut BatchPathStats,
) -> anyhow::Result<(Vec<Resp>, Vec<Req>)>
where
    Req: pilota::pb::Message + Default,
    Resp: pilota::pb::Message + Default,
    V2Call: FnOnce(rpc::PackedBatchRequest) -> V2Future,
    V2Future: std::future::Future<
        Output = Result<volo_grpc::Response<rpc::PackedBatchResponse>, volo_grpc::Status>,
    >,
{
    if !lane_capability.should_attempt_v2() {
        // Fast-fail disabled lanes: this preserves correctness while avoiding
        // repeated incompatible RPC attempts that cannot succeed.
        return Err(anyhow::anyhow!(
            "{rpc_name} packed-v2 lane disabled after explicit unimplemented response"
        ));
    }
    path_stats.v2_attempted = true;
    let (frame, end_offsets) = encode_items_to_parts(&requests)
        .map_err(|err| anyhow::anyhow!("{rpc_name} packed request encode failed: {err}"))?;
    let v2_result = time::timeout(
        timeout,
        v2_call(rpc::PackedBatchRequest { frame, end_offsets }),
    )
    .await;
    match v2_result {
        Ok(Ok(resp)) => {
            let packed = resp.into_inner();
            let responses = decode_items_from_parts::<Resp>(packed.frame, &packed.end_offsets)
                .map_err(|err| {
                    anyhow::anyhow!("{rpc_name} packed response decode failed: {err}")
                })?;
            lane_capability.mark_v2_success();
            Ok((responses, requests))
        }
        Ok(Err(err)) if err.code() == volo_grpc::Code::Unimplemented => {
            // In v2-only mode, explicit unimplemented is a hard compatibility
            // failure; we mark lane disabled to avoid repeated incompatible calls.
            path_stats.v2_disabled_event = lane_capability.mark_v2_unimplemented();
            Err(anyhow::anyhow!("{rpc_name} packed-v2 unimplemented: {err}"))
        }
        Ok(Err(err)) => Err(anyhow::anyhow!("{rpc_name} rpc failed: {err}")),
        Err(_) => Err(anyhow::anyhow!("{rpc_name} rpc timed out")),
    }
}

/// Execute one KV batch-get RPC with packed-v2 only.
///
/// Purpose:
/// - Keep quorum-read hot paths on packed-v2 without materializing intermediate
///   `Vec<KvGetRequest>` wrappers.
///
/// Design:
/// - Encodes `keys` directly into packed `KvGetRequest` items with
///   `PackedBatchBuilder`.
/// - Attempts v2 unless lane capability is disabled.
/// - On `Unimplemented`, disables v2 for this lane and returns an error.
///
/// Inputs:
/// - `timeout`: RPC timeout for packed-v2 call.
/// - `lane_capability`: lane capability state.
/// - `keys`: ordered key payloads for one batch (borrowed so caller retains
///   vector ownership/capacity).
/// - `rpc_name`: log/context name for error text.
/// - `v2_call`: closure invoking packed-v2 RPC.
/// - `path_stats`: per-call capability path counters.
///
/// Outputs:
/// - Ordered `KvGetResponse` list aligned 1:1 with input keys.
async fn call_kv_batch_get_v2_only<V2Call, V2Future>(
    timeout: Duration,
    lane_capability: &BatchLaneCapability,
    keys: &[Bytes],
    rpc_name: &'static str,
    v2_call: V2Call,
    path_stats: &mut BatchPathStats,
) -> anyhow::Result<Vec<rpc::KvGetResponse>>
where
    V2Call: FnOnce(rpc::PackedBatchRequest) -> V2Future,
    V2Future: std::future::Future<
        Output = Result<volo_grpc::Response<rpc::PackedBatchResponse>, volo_grpc::Status>,
    >,
{
    if !lane_capability.should_attempt_v2() {
        // Fast-fail disabled lanes: this preserves correctness while avoiding
        // repeated incompatible RPC attempts that cannot succeed.
        return Err(anyhow::anyhow!(
            "{rpc_name} packed-v2 lane disabled after explicit unimplemented response"
        ));
    }
    path_stats.v2_attempted = true;
    let frame_hint = keys
        .iter()
        .map(|key| key.len().saturating_add(8))
        .sum::<usize>();
    let mut builder = PackedBatchBuilder::with_capacity(keys.len(), frame_hint);
    // Preserve key ordering exactly; this loop intentionally does not
    // reorder/deduplicate keys because caller order defines response routing.
    for key in keys {
        let req = rpc::KvGetRequest { key: key.clone() };
        builder
            .push(&req)
            .map_err(|err| anyhow::anyhow!("{rpc_name} packed request encode failed: {err}"))?;
    }
    let (frame, end_offsets) = builder.finish();
    let v2_result = time::timeout(
        timeout,
        v2_call(rpc::PackedBatchRequest { frame, end_offsets }),
    )
    .await;
    match v2_result {
        Ok(Ok(resp)) => {
            let packed = resp.into_inner();
            let responses =
                decode_items_from_parts::<rpc::KvGetResponse>(packed.frame, &packed.end_offsets)
                    .map_err(|err| {
                        anyhow::anyhow!("{rpc_name} packed response decode failed: {err}")
                    })?;
            lane_capability.mark_v2_success();
            Ok(responses)
        }
        Ok(Err(err)) if err.code() == volo_grpc::Code::Unimplemented => {
            path_stats.v2_disabled_event = lane_capability.mark_v2_unimplemented();
            Err(anyhow::anyhow!("{rpc_name} packed-v2 unimplemented: {err}"))
        }
        Ok(Err(err)) => Err(anyhow::anyhow!("{rpc_name} rpc failed: {err}")),
        Err(_) => Err(anyhow::anyhow!("{rpc_name} rpc timed out")),
    }
}

/// Guard that releases the in-flight permit on drop.
struct InflightPermit {
    limiter: Arc<InflightLimiter>,
}

impl Drop for InflightPermit {
    /// Release the permit when the guard is dropped.
    fn drop(&mut self) {
        self.limiter.release();
    }
}

/// Decision returned by the recover coalescer when a request arrives.
enum RecoverEnqueueDecision {
    Enqueue,
    Coalesced {
        waiters: usize,
        coalesced_count: u64,
        ballot: Ballot,
    },
}

/// Snapshot of coalescer metrics.
struct RecoverCoalescerSnapshot {
    inflight: u64,
    inflight_peak: u64,
    coalesced: u64,
    enqueued: u64,
    waiters_peak: u64,
    waiters_avg: f64,
}

/// Coalesces concurrent recover requests for the same transaction.
#[derive(Default)]
struct RecoverCoalescer {
    inner: Mutex<FastMap<TxnId, RecoverEntry>>,
    coalesced: AtomicU64,
    enqueued: AtomicU64,
    inflight_peak: AtomicU64,
    waiters_peak: AtomicU64,
    waiters_total: AtomicU64,
    waiters_events: AtomicU64,
}

impl RecoverCoalescer {
    /// Insert a recover request or coalesce it with an existing one.
    async fn add_or_coalesce(
        &self,
        txn_id: TxnId,
        ballot: Ballot,
        tx: oneshot::Sender<anyhow::Result<RecoverResponse>>,
    ) -> RecoverEnqueueDecision {
        let mut map = self.inner.lock().await;
        if let Some(entry) = map.get_mut(&txn_id) {
            // Track the highest ballot among coalesced requests.
            if ballot > entry.ballot {
                entry.ballot = ballot;
            }
            entry.waiters.push(tx);
            let waiters = entry.waiters.len();
            let coalesced_count = self.coalesced.fetch_add(1, Ordering::Relaxed) + 1;
            self.waiters_peak
                .fetch_max(waiters as u64, Ordering::Relaxed);
            self.waiters_total
                .fetch_add(waiters as u64, Ordering::Relaxed);
            self.waiters_events.fetch_add(1, Ordering::Relaxed);
            return RecoverEnqueueDecision::Coalesced {
                waiters,
                coalesced_count,
                ballot: entry.ballot,
            };
        }
        // First request for this txn id: store it as inflight.
        map.insert(
            txn_id,
            RecoverEntry {
                ballot,
                waiters: vec![tx],
            },
        );
        let inflight = map.len() as u64;
        self.inflight_peak.fetch_max(inflight, Ordering::Relaxed);
        self.enqueued.fetch_add(1, Ordering::Relaxed);
        RecoverEnqueueDecision::Enqueue
    }

    /// Complete a recover request successfully and fan out the response.
    async fn complete_ok(&self, txn_id: TxnId, resp: RecoverResponse) {
        let waiters = {
            let mut map = self.inner.lock().await;
            map.remove(&txn_id).map(|entry| entry.waiters)
        };
        if let Some(waiters) = waiters {
            for tx in waiters {
                let _ = tx.send(Ok(resp.clone()));
            }
        }
    }

    /// Complete a recover request with an error and fan out the failure.
    async fn complete_err(&self, txn_id: TxnId, err: &anyhow::Error) {
        let waiters = {
            let mut map = self.inner.lock().await;
            map.remove(&txn_id).map(|entry| entry.waiters)
        };
        if let Some(waiters) = waiters {
            let msg = err.to_string();
            for tx in waiters {
                let _ = tx.send(Err(anyhow::anyhow!(msg.clone())));
            }
        }
    }

    /// Snapshot and reset coalescer statistics.
    async fn snapshot_and_reset(&self) -> RecoverCoalescerSnapshot {
        let inflight = self.inner.lock().await.len() as u64;
        let inflight_peak = self.inflight_peak.swap(0, Ordering::Relaxed);
        let coalesced = self.coalesced.swap(0, Ordering::Relaxed);
        let enqueued = self.enqueued.swap(0, Ordering::Relaxed);
        let waiters_peak = self.waiters_peak.swap(0, Ordering::Relaxed);
        let waiters_total = self.waiters_total.swap(0, Ordering::Relaxed);
        let waiters_events = self.waiters_events.swap(0, Ordering::Relaxed);
        let waiters_avg = if waiters_events == 0 {
            0.0
        } else {
            waiters_total as f64 / waiters_events as f64
        };
        RecoverCoalescerSnapshot {
            inflight,
            inflight_peak,
            coalesced,
            enqueued,
            waiters_peak,
            waiters_avg,
        }
    }
}

impl GrpcTransport {
    /// Build a transport for the given membership set, wiring per-peer queues
    /// and batchers for each RPC type.
    pub fn new(
        members: &HashMap<NodeId, SocketAddr>,
        rpc_timeout: Duration,
        commit_timeout: Duration,
        rpc_inflight_limit: usize,
        inflight_tuning: InflightTuning,
        rpc_batch_max: usize,
        rpc_batch_wait: Duration,
    ) -> Self {
        let batch_max = rpc_batch_max.max(1);
        let batch_wait = rpc_batch_wait;
        let mut peers = HashMap::new();
        let stats = Arc::new(RpcStats::default());
        let inflight_limit = rpc_inflight_limit.max(1);
        for (node_id, addr) in members {
            let peer = Self::build_peer(
                *addr,
                rpc_timeout,
                commit_timeout,
                batch_max,
                batch_wait,
                inflight_limit,
                inflight_tuning,
                stats.clone(),
            );
            peers.insert(*node_id, peer);
        }
        Self {
            peers: Arc::new(std::sync::RwLock::new(peers)),
            rpc_timeout,
            commit_timeout,
            stats,
            inflight_tuning,
            rpc_batch_max: batch_max,
            rpc_batch_wait: batch_wait,
            inflight_limit,
        }
    }

    /// Build one peer transport entry and spawn all per-lane workers.
    ///
    /// Purpose:
    /// - Construct isolated per-peer queues, limiters, and worker tasks.
    ///
    /// Design:
    /// - Uses separate clients for consensus and read RPCs.
    /// - Uses distinct per-lane limiters so GET concurrency is isolated from
    ///   consensus tuning signals.
    ///
    /// Inputs:
    /// - Peer address plus batching/timeout/limiter knobs.
    ///
    /// Outputs:
    /// - Fully initialized `Peer` with active background workers.
    fn build_peer(
        addr: SocketAddr,
        rpc_timeout: Duration,
        commit_timeout: Duration,
        batch_max: usize,
        batch_wait: Duration,
        inflight_limit: usize,
        inflight_tuning: InflightTuning,
        stats: Arc<RpcStats>,
    ) -> Peer {
        // Build gRPC clients for consensus and read-only calls.
        let consensus_client = rpc::HoloRpcClientBuilder::new("holo_store.rpc.HoloRpc")
            .address(volo::net::Address::from(addr))
            .build();
        let read_client = rpc::HoloRpcClientBuilder::new("holo_store.rpc.HoloRpc")
            .address(volo::net::Address::from(addr))
            .build();

        let (kv_get_tx, kv_get_rx) = mpsc::channel(RPC_QUEUE_CAPACITY);
        let (pre_accept_tx, pre_accept_rx) = mpsc::channel(RPC_QUEUE_CAPACITY);
        let (accept_tx, accept_rx) = mpsc::channel(RPC_QUEUE_CAPACITY);
        let (commit_tx, commit_rx) = mpsc::channel(RPC_QUEUE_CAPACITY);
        let (recover_tx, recover_rx) = mpsc::channel(RPC_QUEUE_CAPACITY);

        let peer_stats = Arc::new(PeerStats::default());
        let recover_coalescer = Arc::new(RecoverCoalescer::default());
        // Start each limiter at the configured inflight limit.
        let pre_accept_limiter = Arc::new(InflightLimiter::new(
            inflight_limit,
            inflight_tuning.min,
            inflight_tuning.max,
        ));
        let accept_limiter = Arc::new(InflightLimiter::new(
            inflight_limit,
            inflight_tuning.min,
            inflight_tuning.max,
        ));
        let commit_limiter = Arc::new(InflightLimiter::new(
            inflight_limit,
            inflight_tuning.min,
            inflight_tuning.max,
        ));
        let recover_limiter = Arc::new(InflightLimiter::new(
            inflight_limit,
            inflight_tuning.min,
            inflight_tuning.max,
        ));
        let kv_get_limiter = Arc::new(InflightLimiter::new(
            inflight_limit,
            inflight_tuning.min,
            inflight_tuning.max,
        ));
        let kv_batch_get_v2_capability = Arc::new(BatchLaneCapability::default());

        spawn_kv_get_batcher(
            read_client.clone(),
            kv_get_rx,
            rpc_timeout,
            batch_max,
            batch_wait,
            stats.clone(),
            peer_stats.clone(),
            kv_get_limiter.clone(),
        );
        spawn_pre_accept_batcher(
            consensus_client.clone(),
            pre_accept_rx,
            rpc_timeout,
            batch_max,
            batch_wait,
            stats.clone(),
            peer_stats.clone(),
            pre_accept_limiter.clone(),
        );
        spawn_accept_batcher(
            consensus_client.clone(),
            accept_rx,
            rpc_timeout,
            batch_max,
            batch_wait,
            stats.clone(),
            peer_stats.clone(),
            accept_limiter.clone(),
        );
        spawn_commit_batcher(
            consensus_client.clone(),
            commit_rx,
            commit_timeout,
            batch_max,
            batch_wait,
            stats.clone(),
            peer_stats.clone(),
            commit_limiter.clone(),
        );
        spawn_recover_batcher(
            consensus_client.clone(),
            recover_rx,
            rpc_timeout,
            batch_max,
            batch_wait,
            stats.clone(),
            peer_stats.clone(),
            recover_coalescer.clone(),
            recover_limiter.clone(),
        );

        Peer {
            client: consensus_client,
            read_client,
            kv_get_tx,
            pre_accept_tx,
            accept_tx,
            commit_tx,
            recover_tx,
            stats: peer_stats,
            recover_coalescer,
            pre_accept_limiter,
            accept_limiter,
            commit_limiter,
            recover_limiter,
            kv_get_limiter,
            kv_batch_get_v2_capability,
        }
    }

    pub fn update_members(&self, members: &HashMap<NodeId, SocketAddr>) {
        let mut peers = self.peers.write().unwrap();
        peers.retain(|id, _| members.contains_key(id));
        for (id, addr) in members {
            if !peers.contains_key(id) {
                let peer = Self::build_peer(
                    *addr,
                    self.rpc_timeout,
                    self.commit_timeout,
                    self.rpc_batch_max,
                    self.rpc_batch_wait,
                    self.inflight_limit,
                    self.inflight_tuning,
                    self.stats.clone(),
                );
                peers.insert(*id, peer);
            }
        }
    }

    fn peer(&self, target: NodeId) -> anyhow::Result<Peer> {
        self.peers
            .read()
            .unwrap()
            .get(&target)
            .cloned()
            .with_context(|| format!("unknown target node {target}"))
    }

    /// Return the shared RPC statistics collector.
    pub fn stats(&self) -> Arc<RpcStats> {
        self.stats.clone()
    }

    /// Collect and reset per-peer stats snapshots, applying inflight tuning.
    ///
    /// Purpose:
    /// - Produce peer snapshots for logging/tuning while resetting rolling
    ///   counters.
    ///
    /// Design:
    /// - Applies adaptive tuning only to consensus lanes.
    /// - Reports GET lane limit separately to keep read tuning independent.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Outputs:
    /// - One `(peer_id, snapshot)` entry per known peer.
    pub async fn peer_stats_snapshots(&self) -> Vec<(NodeId, PeerStatsSnapshot)> {
        let now_us = epoch_micros();
        let peers = self.peers.read().unwrap().clone();
        let mut out = Vec::with_capacity(peers.len());
        for (id, peer) in peers {
            let mut snap = peer.stats.snapshot_and_reset(now_us);
            let coalescer = peer.recover_coalescer.snapshot_and_reset().await;
            snap.recover_inflight_txns = coalescer.inflight;
            snap.recover_inflight_peak = coalescer.inflight_peak;
            snap.recover_coalesced = coalescer.coalesced;
            snap.recover_enqueued = coalescer.enqueued;
            snap.recover_waiters_peak = coalescer.waiters_peak;
            snap.recover_waiters_avg = coalescer.waiters_avg;
            // Evaluate tuning signals based on queue depth and wait time.
            let wait_max_ms = snap
                .pre_accept_wait_max_ms
                .max(snap.accept_wait_max_ms)
                .max(snap.commit_wait_max_ms);
            let queue_total = snap.pre_accept_queue + snap.accept_queue + snap.commit_queue;
            let mut limit = peer.pre_accept_limiter.current();
            if queue_total > self.inflight_tuning.high_queue
                || wait_max_ms > self.inflight_tuning.high_wait_ms
            {
                // Back off when queues are deep or latency is high.
                limit = limit.saturating_sub(1).max(self.inflight_tuning.min);
            } else if queue_total < self.inflight_tuning.low_queue
                && wait_max_ms < self.inflight_tuning.low_wait_ms
            {
                // Increase concurrency when queues are short and latency is low.
                limit = (limit + 1).min(self.inflight_tuning.max);
            }
            let changed = peer.pre_accept_limiter.adjust(limit);
            peer.accept_limiter.adjust(limit);
            peer.commit_limiter.adjust(limit);
            peer.recover_limiter.adjust(limit);
            snap.rpc_inflight_limit = limit as u64;
            snap.kv_get_inflight_limit = peer.kv_get_limiter.current() as u64;
            if changed {
                tracing::info!(
                    peer = id,
                    inflight_limit = limit,
                    kv_get_inflight_limit = snap.kv_get_inflight_limit,
                    queue_total = queue_total,
                    wait_max_ms = wait_max_ms,
                    "rpc inflight tuned"
                );
            }
            out.push((id, snap));
        }
        out
    }

    /// Perform a batched KV GET via the peer's queue-based pipeline.
    ///
    /// Purpose:
    /// - Enqueue one key read into the per-peer GET batching lane.
    ///
    /// Design:
    /// - Uses queue-based batching worker for coalescing and bounded in-flight
    ///   execution.
    /// - Preserves per-request timeout semantics at call site.
    ///
    /// Inputs:
    /// - `target`: remote node id.
    /// - `key`: key bytes to read.
    ///
    /// Outputs:
    /// - Optional `(value, version)` or an error/timeout.
    pub async fn kv_get(
        &self,
        target: NodeId,
        key: Vec<u8>,
    ) -> anyhow::Result<Option<(Vec<u8>, Version)>> {
        let peer = self.peer(target)?;

        let (tx, rx) = oneshot::channel();
        peer.stats.kv_get_sent.fetch_add(1, Ordering::Relaxed);
        peer.stats
            .kv_get_last_enqueue_us
            .store(epoch_micros(), Ordering::Relaxed);
        peer.stats.kv_get_queue.fetch_add(1, Ordering::Relaxed);
        if let Err(_) = peer
            .kv_get_tx
            .send(KvGetWork {
                key,
                tx,
                enqueued_at: std::time::Instant::now(),
            })
            .await
        {
            // If the queue is closed, return a clear error.
            peer.stats.kv_get_queue.fetch_sub(1, Ordering::Relaxed);
            return Err(anyhow::anyhow!("kv_get queue closed"));
        }

        match time::timeout(self.rpc_timeout, rx).await {
            Ok(Ok(res)) => res,
            Ok(Err(_)) => Err(anyhow::anyhow!("kv_get response channel closed")),
            Err(_) => Err(anyhow::anyhow!("kv_get timed out")),
        }
    }

    /// Perform a direct batch-get RPC against a peer (no coalescing queue).
    ///
    /// Purpose:
    /// - Serve quorum-read fanout with low allocation overhead.
    ///
    /// Design:
    /// - Uses packed-v2 only and fails fast if peer capability is incompatible.
    /// - Preserves request order and converts responses to `(value, version)`
    ///   tuples expected by read-path merge logic.
    ///
    /// Inputs:
    /// - `target`: remote node id.
    /// - `keys`: ordered key bytes for one shard-local batch.
    ///
    /// Outputs:
    /// - Ordered optional `(value, version)` entries aligned with input keys.
    pub async fn kv_batch_get(
        &self,
        target: NodeId,
        keys: Vec<Bytes>,
    ) -> anyhow::Result<Vec<Option<(Vec<u8>, Version)>>> {
        let peer = self.peer(target)?;
        let start = std::time::Instant::now();
        let mut path_stats = BatchPathStats::default();
        let client_v2 = peer.read_client.clone();
        let result = call_kv_batch_get_v2_only(
            self.rpc_timeout,
            &peer.kv_batch_get_v2_capability,
            &keys,
            "kv_batch_get_v2",
            move |packed| {
                let client = client_v2.clone();
                async move { client.kv_batch_get_v2(packed).await }
            },
            &mut path_stats,
        )
        .await;
        let rpc_us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        peer.stats.record_kv_get_path_stats(path_stats);

        match result {
            Ok(responses) => {
                self.stats.record_kv_batch_get(rpc_us, false);
                let mut out = Vec::with_capacity(responses.len());
                for item in responses {
                    if !item.has_value {
                        out.push(None);
                    } else {
                        let version = from_rpc_version(item.version);
                        out.push(Some((item.value.to_vec(), version)));
                    }
                }
                Ok(out)
            }
            Err(err) => {
                self.stats.record_kv_batch_get(rpc_us, true);
                Err(err)
            }
        }
    }

    /// Fetch last committed (txn_id, seq) for a set of keys from a peer.
    pub async fn last_committed(
        &self,
        target: NodeId,
        group_id: u64,
        keys: Vec<Vec<u8>>,
    ) -> anyhow::Result<Vec<Option<(TxnId, u64)>>> {
        let peer = self.peer(target)?;
        let keys = keys.into_iter().map(|k| k.into()).collect::<Vec<_>>();
        let start = std::time::Instant::now();
        let resp = time::timeout(
            self.rpc_timeout,
            peer.read_client
                .last_committed(rpc::LastCommittedRequest { group_id, keys }),
        )
        .await;
        let _rpc_us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        match resp {
            Ok(Ok(resp)) => {
                let resp = resp.into_inner();
                let mut out = Vec::with_capacity(resp.items.len());
                for item in resp.items {
                    if item.present {
                        let txn_id = item.txn_id.map(from_rpc_txn_id).unwrap_or(TxnId {
                            node_id: 0,
                            counter: 0,
                        });
                        out.push(Some((txn_id, item.seq)));
                    } else {
                        out.push(None);
                    }
                }
                Ok(out)
            }
            Ok(Err(err)) => Err(anyhow::anyhow!("last_committed rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("last_committed rpc timed out")),
        }
    }

    /// Fetch the last executed prefix vector for a group from a peer.
    pub async fn last_executed_prefix(
        &self,
        target: NodeId,
        group_id: u64,
    ) -> anyhow::Result<Vec<ExecutedPrefix>> {
        let peer = self.peer(target)?;
        let resp = time::timeout(
            self.rpc_timeout,
            peer.read_client
                .last_executed_prefix(rpc::LastExecutedPrefixRequest { group_id }),
        )
        .await;
        match resp {
            Ok(Ok(resp)) => Ok(resp
                .into_inner()
                .prefixes
                .into_iter()
                .map(from_rpc_executed_prefix)
                .collect()),
            Ok(Err(err)) => Err(anyhow::anyhow!("last_executed_prefix rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("last_executed_prefix rpc timed out")),
        }
    }

    /// Seed executed-prefix floors on a peer for a group.
    pub async fn seed_executed_prefix(
        &self,
        target: NodeId,
        group_id: u64,
        prefixes: &[ExecutedPrefix],
    ) -> anyhow::Result<()> {
        let peer = self.peer(target)?;
        let req = rpc::SeedExecutedPrefixRequest {
            group_id,
            prefixes: prefixes
                .iter()
                .cloned()
                .map(to_rpc_executed_prefix)
                .collect(),
        };
        let resp =
            time::timeout(self.rpc_timeout, peer.read_client.seed_executed_prefix(req)).await;
        match resp {
            Ok(Ok(resp)) => {
                if resp.into_inner().ok {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("seed_executed_prefix rpc returned not-ok"))
                }
            }
            Ok(Err(err)) => Err(anyhow::anyhow!("seed_executed_prefix rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("seed_executed_prefix rpc timed out")),
        }
    }

    /// Fetch and decode the peer's current control-plane state snapshot.
    pub async fn cluster_state(
        &self,
        target: NodeId,
    ) -> anyhow::Result<crate::cluster::ClusterState> {
        let peer = self.peer(target)?;
        let resp = time::timeout(
            self.rpc_timeout,
            peer.read_client.cluster_state(rpc::ClusterStateRequest {}),
        )
        .await;
        match resp {
            Ok(Ok(resp)) => {
                let json = resp.into_inner().json.to_string();
                serde_json::from_str(&json)
                    .with_context(|| format!("failed to parse cluster_state from node {target}"))
            }
            Ok(Err(err)) => Err(anyhow::anyhow!("cluster_state rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("cluster_state rpc timed out")),
        }
    }

    /// Fetch per-range telemetry from a peer.
    pub async fn range_stats_detailed(
        &self,
        target: NodeId,
    ) -> anyhow::Result<Vec<RangeTelemetryStat>> {
        let peer = self.peer(target)?;
        let resp = time::timeout(
            self.rpc_timeout,
            peer.read_client.range_stats(rpc::RangeStatsRequest {}),
        )
        .await;
        match resp {
            Ok(Ok(resp)) => {
                let mut out = Vec::new();
                for range in resp.into_inner().ranges {
                    out.push(RangeTelemetryStat {
                        shard_id: range.shard_id,
                        shard_index: range.shard_index as usize,
                        record_count: range.record_count,
                        is_leaseholder: range.is_leaseholder,
                        write_ops_total: range.write_ops_total,
                        read_ops_total: range.read_ops_total,
                        write_bytes_total: range.write_bytes_total,
                        queue_depth: range.queue_depth,
                        write_tail_latency_ms: range.write_tail_latency_ms,
                        hot_key_concentration_bps: range.hot_key_concentration_bps,
                        write_hot_buckets: range.write_hot_buckets,
                        read_hot_buckets: range.read_hot_buckets,
                    });
                }
                Ok(out)
            }
            Ok(Err(err)) => Err(anyhow::anyhow!("range_stats rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("range_stats rpc timed out")),
        }
    }

    /// Fetch local range record counts from a peer keyed by shard id.
    pub async fn range_stats(&self, target: NodeId) -> anyhow::Result<HashMap<u64, u64>> {
        let detailed = self.range_stats_detailed(target).await?;
        let mut out = HashMap::new();
        for stat in detailed {
            out.insert(stat.shard_id, stat.record_count);
        }
        Ok(out)
    }

    /// Fetch one page of latest-visible rows from a peer for `[start, end)`.
    pub async fn range_snapshot_latest(
        &self,
        target: NodeId,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        cursor: &[u8],
        limit: usize,
        reverse: bool,
    ) -> anyhow::Result<(Vec<RangeLatestEntry>, Vec<u8>, bool)> {
        let peer = self.peer(target)?;
        let req = rpc::RangeSnapshotLatestRequest {
            shard_index: shard_index as u64,
            start_key: start_key.to_vec().into(),
            end_key: end_key.to_vec().into(),
            cursor: cursor.to_vec().into(),
            limit: limit.min(u32::MAX as usize) as u32,
            reverse,
        };
        let resp = time::timeout(
            self.rpc_timeout,
            peer.read_client.range_snapshot_latest(req),
        )
        .await;
        match resp {
            Ok(Ok(resp)) => {
                let resp = resp.into_inner();
                let mut entries = Vec::with_capacity(resp.entries.len());
                for item in resp.entries {
                    let version = from_rpc_version_required(item.version)?;
                    entries.push(RangeLatestEntry {
                        key: item.key.to_vec(),
                        value: item.value.to_vec(),
                        version,
                    });
                }
                Ok((entries, resp.next_cursor.to_vec(), resp.done))
            }
            Ok(Err(err)) => Err(anyhow::anyhow!("range_snapshot_latest rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("range_snapshot_latest rpc timed out")),
        }
    }

    /// Apply one page of latest-visible rows to a peer shard for `[start, end)`.
    pub async fn range_apply_latest(
        &self,
        target: NodeId,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: Vec<RangeLatestEntry>,
    ) -> anyhow::Result<u64> {
        let peer = self.peer(target)?;
        let rpc_entries = entries
            .into_iter()
            .map(|entry| rpc::RangeSnapshotLatestEntry {
                key: entry.key.into(),
                value: entry.value.into(),
                version: Some(to_rpc_version(entry.version)),
            })
            .collect();
        let req = rpc::RangeApplyLatestRequest {
            shard_index: shard_index as u64,
            start_key: start_key.to_vec().into(),
            end_key: end_key.to_vec().into(),
            entries: rpc_entries,
            admin: true,
        };
        let resp = time::timeout(self.rpc_timeout, peer.read_client.range_apply_latest(req)).await;
        match resp {
            Ok(Ok(resp)) => Ok(resp.into_inner().applied),
            Ok(Err(err)) => Err(anyhow::anyhow!("range_apply_latest rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("range_apply_latest rpc timed out")),
        }
    }

    /// Fetch a command payload for a specific transaction from a peer.
    pub async fn fetch_command(
        &self,
        target: NodeId,
        group_id: u64,
        txn_id: TxnId,
    ) -> anyhow::Result<Option<Bytes>> {
        let peer = self.peer(target)?;
        let resp = time::timeout(
            self.rpc_timeout,
            peer.read_client.fetch_command(rpc::FetchCommandRequest {
                group_id,
                txn_id: Some(to_rpc_txn_id(txn_id)),
            }),
        )
        .await;
        match resp {
            Ok(Ok(resp)) => {
                let resp = resp.into_inner();
                if resp.has_command {
                    Ok(Some(resp.command))
                } else {
                    Ok(None)
                }
            }
            Ok(Err(err)) => Err(anyhow::anyhow!("fetch_command rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("fetch_command rpc timed out")),
        }
    }

    /// Ask a peer whether a transaction has executed.
    pub async fn executed(
        &self,
        target: NodeId,
        group_id: u64,
        txn_id: TxnId,
    ) -> anyhow::Result<bool> {
        let peer = self.peer(target)?;
        let resp = time::timeout(
            self.rpc_timeout,
            peer.read_client.executed(rpc::ExecutedRequest {
                group_id,
                txn_id: Some(to_rpc_txn_id(txn_id)),
            }),
        )
        .await;
        match resp {
            Ok(Ok(resp)) => Ok(resp.into_inner().executed),
            Ok(Err(err)) => Err(anyhow::anyhow!("executed rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("executed rpc timed out")),
        }
    }

    /// Tell a peer to mark a transaction as visible to readers.
    pub async fn mark_visible(
        &self,
        target: NodeId,
        group_id: u64,
        txn_id: TxnId,
    ) -> anyhow::Result<bool> {
        let peer = self.peer(target)?;
        let resp = time::timeout(
            self.rpc_timeout,
            peer.client.mark_visible(rpc::MarkVisibleRequest {
                group_id,
                txn_id: Some(to_rpc_txn_id(txn_id)),
            }),
        )
        .await;
        match resp {
            Ok(Ok(resp)) => Ok(resp.into_inner().ok),
            Ok(Err(err)) => Err(anyhow::anyhow!("mark_visible rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("mark_visible rpc timed out")),
        }
    }
}

/// Spawn a task that batches KV GET requests per peer.
///
/// Purpose:
/// - Coalesce per-key reads into one RPC while preserving request order and
///   per-request completion fan-out.
///
/// Design:
/// - One long-lived worker owns dequeue, batch build, and RPC execution.
/// - Uses `FuturesUnordered` and a dedicated GET limiter to allow bounded
///   in-flight read batches without borrowing consensus-lane permits.
/// - Clamps effective batch wait by head queue age so stale reads flush
///   immediately instead of waiting an extra coalescing window.
/// - Reuses hot-path vectors (`KvGetWork` item buffers and waiter senders) via
///   bounded pools to reduce allocator churn on read-heavy workloads.
/// - Keeps response fan-out linear and deterministic by preserving original
///   queue order within each batch.
///
/// Inputs:
/// - Peer client, queue receiver, timeout/batching knobs, shared stats, and
///   one dedicated GET in-flight limiter.
///
/// Outputs:
/// - Batched `kv_batch_get` RPC calls and per-request response completion.
fn spawn_kv_get_batcher(
    client: rpc::HoloRpcClient,
    mut rx: mpsc::Receiver<KvGetWork>,
    timeout: Duration,
    batch_max: usize,
    batch_wait: Duration,
    stats: Arc<RpcStats>,
    peer_stats: Arc<PeerStats>,
    limiter: Arc<InflightLimiter>,
) {
    tokio::spawn(async move {
        let reuse_max_capacity = batch_max
            .saturating_mul(REUSE_POOL_CAPACITY_MULTIPLIER)
            .max(1);
        let reuse_max_cached = limiter.current().clamp(1, REUSE_POOL_MAX_CACHED);
        let mut item_pool = ReuseVecPool::<KvGetWork>::new(reuse_max_cached, reuse_max_capacity);
        let mut tx_pool = ReuseVecPool::<KvGetReplyTx>::new(reuse_max_cached, reuse_max_capacity);
        let mut key_pool = ReuseVecPool::<Bytes>::new(reuse_max_cached, reuse_max_capacity);
        let v2_capability = Arc::new(BatchLaneCapability::default());
        let max_queue_wait = kv_get_max_queue_wait(batch_wait);
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut rx_closed = false;
        loop {
            // Exit only after queue shutdown and all in-flight RPCs complete.
            if rx_closed && in_flight.is_empty() {
                break;
            }
            tokio::select! {
                maybe = rx.recv(), if !rx_closed => {
                    match maybe {
                        Some(first) => {
                            let dequeue_start = std::time::Instant::now();
                            let queued_items_hint = peer_stats.kv_get_queue.load(Ordering::Relaxed);
                            // Probe limiter capacity only on low-backlog shapes where
                            // immediate dispatch is potentially beneficial.
                            let mut preacquired_permit: Option<InflightPermit> =
                                if queued_items_hint <= KV_GET_IMMEDIATE_DISPATCH_QUEUE_DEPTH {
                                    limiter.try_acquire()
                                } else {
                                    None
                                };
                            let effective_wait = if kv_get_should_dispatch_immediately(
                                queued_items_hint,
                                preacquired_permit.is_some(),
                            ) {
                                // With minimal backlog and available capacity, bypass
                                // coalescing delay to protect p50/p75.
                                Duration::ZERO
                            } else {
                                // Backlog or saturation path: release speculative permit and
                                // retain queue-age-capped batching behavior.
                                preacquired_permit = None;
                                kv_get_effective_batch_wait(
                                    first.enqueued_at,
                                    dequeue_start,
                                    batch_wait,
                                    max_queue_wait,
                                    queued_items_hint,
                                )
                            };
                            let items_buf = item_pool.take(batch_max.max(1));
                            let mut items = collect_batch_with_progress(
                                items_buf,
                                first,
                                &mut rx,
                                batch_max,
                                effective_wait,
                                &mut in_flight,
                                |recycle: WaiterBatchRecycle<Option<(Vec<u8>, Version)>, Bytes>| {
                                    tx_pool.put(recycle.txs);
                                    key_pool.put(recycle.requests);
                                },
                            ).await;

                            let batch_len = items.len() as u64;
                            peer_stats
                                .kv_get_queue
                                .fetch_sub(batch_len, Ordering::Relaxed);
                            let mut txs = tx_pool.take(items.len());
                            let mut keys = key_pool.take(items.len());
                            let batch_start = std::time::Instant::now();
                            let mut wait_us_total = 0u64;
                            let mut wait_us_max = 0u64;
                            let mut head_wait_us = 0u64;
                            let mut first_in_batch = true;
                            // Build one batch request in dequeue order so response fan-out maps
                            // 1:1 to queued waiters. We intentionally do not re-order keys.
                            for KvGetWork {
                                key,
                                tx,
                                enqueued_at,
                            } in items.drain(..)
                            {
                                // Track queue wait time for each request in the batch.
                                let wait_us = batch_start
                                    .duration_since(enqueued_at)
                                    .as_micros()
                                    .min(u128::from(u64::MAX)) as u64;
                                if first_in_batch {
                                    // Head queue age approximates batch staleness at dispatch.
                                    head_wait_us = wait_us;
                                    first_in_batch = false;
                                }
                                wait_us_total = wait_us_total.saturating_add(wait_us);
                                wait_us_max = wait_us_max.max(wait_us);
                                txs.push(tx);
                                // Keep key payload transfer zero-copy at this layer.
                                keys.push(Bytes::from(key));
                            }
                            // Recycle the now-drained item buffer immediately.
                            item_pool.put(items);

                            peer_stats
                                .kv_get_last_dequeue_us
                                .store(epoch_micros(), Ordering::Relaxed);
                            peer_stats
                                .kv_get_wait_total_us
                                .fetch_add(wait_us_total, Ordering::Relaxed);
                            peer_stats
                                .kv_get_wait_max_us
                                .fetch_max(wait_us_max, Ordering::Relaxed);
                            peer_stats
                                .kv_get_wait_count
                                .fetch_add(batch_len, Ordering::Relaxed);
                            peer_stats
                                .kv_get_head_wait_total_us
                                .fetch_add(head_wait_us, Ordering::Relaxed);
                            peer_stats
                                .kv_get_head_wait_max_us
                                .fetch_max(head_wait_us, Ordering::Relaxed);
                            peer_stats
                                .kv_get_head_wait_count
                                .fetch_add(1, Ordering::Relaxed);

                            // Track limiter saturation separately for GET so read-tail tuning
                            // can be isolated from consensus lane behavior.
                            let (permit, saturated) = if let Some(permit) = preacquired_permit {
                                (permit, false)
                            } else if let Some(permit) = limiter.try_acquire() {
                                (permit, false)
                            } else {
                                (acquire_with_progress(&limiter, &mut in_flight).await, true)
                            };
                            if saturated {
                                peer_stats
                                    .kv_get_limiter_saturated
                                    .fetch_add(1, Ordering::Relaxed);
                            }

                            let client = client.clone();
                            let stats = stats.clone();
                            let peer_stats = peer_stats.clone();
                            let v2_capability = v2_capability.clone();
                            in_flight.push(async move {
                                let _permit = permit;
                                peer_stats.kv_get_inflight.fetch_add(1, Ordering::Relaxed);
                                let rpc_start = std::time::Instant::now();
                                let client_v2 = client.clone();
                                let mut path_stats = BatchPathStats::default();
                                let result = call_kv_batch_get_v2_only(
                                    timeout,
                                    &v2_capability,
                                    &keys,
                                    "kv_batch_get_v2",
                                    move |packed| {
                                        let client = client_v2.clone();
                                        async move { client.kv_batch_get_v2(packed).await }
                                    },
                                    &mut path_stats,
                                )
                                .await;
                                let rpc_us = rpc_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                                peer_stats.kv_get_inflight.fetch_sub(1, Ordering::Relaxed);
                                stats.record_kv_get_batch(batch_len, wait_us_total, wait_us_max, rpc_us);
                                peer_stats.kv_get_latency.record(rpc_us);
                                peer_stats.record_kv_get_path_stats(path_stats);

                                match result {
                                    Ok(responses) => {
                                        if responses.len() != txs.len() {
                                            // Reject if server returned a mismatched response count.
                                            let err = anyhow::anyhow!(
                                                "kv_batch_get response count mismatch (expected {}, got {})",
                                                txs.len(),
                                                responses.len()
                                            );
                                            peer_stats.kv_get_errors.fetch_add(1, Ordering::Relaxed);
                                            for tx in txs.drain(..) {
                                                let _ = tx.send(Err(anyhow::anyhow!("{err}")));
                                            }
                                            return WaiterBatchRecycle { txs, requests: keys };
                                        }

                                        for (tx, item) in txs.drain(..).zip(responses) {
                                            if !item.has_value {
                                                let _ = tx.send(Ok(None));
                                            } else {
                                                let version = from_rpc_version(item.version);
                                                let _ = tx.send(Ok(Some((item.value.to_vec(), version))));
                                            }
                                        }
                                        WaiterBatchRecycle { txs, requests: keys }
                                    }
                                    Err(err) => {
                                        // RPC error (or timeout): notify all waiters.
                                        stats.record_kv_get_error();
                                        peer_stats.kv_get_errors.fetch_add(1, Ordering::Relaxed);
                                        let err = anyhow::anyhow!("{err}");
                                        for tx in txs.drain(..) {
                                            let _ = tx.send(Err(anyhow::anyhow!("{err}")));
                                        }
                                        WaiterBatchRecycle { txs, requests: keys }
                                    }
                                }
                            });
                        }
                        None => {
                            rx_closed = true;
                        }
                    }
                }
                done = in_flight.next(), if !in_flight.is_empty() => {
                    // Keep driving in-flight futures even when queue load is low.
                    if let Some(recycle) = done {
                        tx_pool.put(recycle.txs);
                        key_pool.put(recycle.requests);
                    }
                }
            }
        }
    });
}

/// Spawn a pre-accept batching worker for one peer.
///
/// Purpose:
/// - Amortize per-request RPC overhead while keeping tail latency low under
///   bursty load.
///
/// Design:
/// - One long-lived worker task owns queue dequeue and in-flight RPC polling.
/// - Uses `FuturesUnordered` for bounded parallel batch RPCs without spawning a
///   second task per batch.
/// - Enforces concurrency via `InflightLimiter`.
///
/// Inputs:
/// - Peer client, queue receiver, timeout/batching knobs, shared stats, and
///   one limiter for this RPC lane.
///
/// Outputs:
/// - Batched pre-accept RPC calls with per-request response fan-out.
fn spawn_pre_accept_batcher(
    client: rpc::HoloRpcClient,
    mut rx: mpsc::Receiver<PreAcceptWork>,
    timeout: Duration,
    batch_max: usize,
    batch_wait: Duration,
    stats: Arc<RpcStats>,
    peer_stats: Arc<PeerStats>,
    limiter: Arc<InflightLimiter>,
) {
    tokio::spawn(async move {
        let reuse_max_capacity = batch_max
            .saturating_mul(REUSE_POOL_CAPACITY_MULTIPLIER)
            .max(1);
        let reuse_max_cached = limiter.current().clamp(1, REUSE_POOL_MAX_CACHED);
        let mut item_pool =
            ReuseVecPool::<PreAcceptWork>::new(reuse_max_cached, reuse_max_capacity);
        let mut tx_pool = ReuseVecPool::<oneshot::Sender<anyhow::Result<PreAcceptResponse>>>::new(
            reuse_max_cached,
            reuse_max_capacity,
        );
        let mut request_pool =
            ReuseVecPool::<rpc::PreAcceptRequest>::new(reuse_max_cached, reuse_max_capacity);
        let v2_capability = Arc::new(BatchLaneCapability::default());
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut rx_closed = false;
        loop {
            // Exit only after queue shutdown and all in-flight RPCs complete.
            if rx_closed && in_flight.is_empty() {
                break;
            }
            tokio::select! {
                maybe = rx.recv(), if !rx_closed => {
                    match maybe {
                        Some(first) => {
                            let items_buf = item_pool.take(batch_max.max(1));
                            let mut items = collect_batch_with_progress(
                                items_buf,
                                first,
                                &mut rx,
                                batch_max,
                                batch_wait,
                                &mut in_flight,
                                |recycle: WaiterBatchRecycle<PreAcceptResponse, rpc::PreAcceptRequest>| {
                                    tx_pool.put(recycle.txs);
                                    request_pool.put(recycle.requests);
                                },
                            ).await;

                            let batch_len = items.len() as u64;
                            peer_stats
                                .pre_accept_queue
                                .fetch_sub(batch_len, Ordering::Relaxed);
                            let mut txs = tx_pool.take(items.len());
                            let mut requests = request_pool.take(items.len());
                            let batch_start = std::time::Instant::now();
                            let mut wait_us_total = 0u64;
                            let mut wait_us_max = 0u64;
                            // Build one RPC payload for the current dequeued batch.
                            for PreAcceptWork {
                                req,
                                tx,
                                enqueued_at,
                            } in items.drain(..)
                            {
                                // Track queue wait time for each request in the batch.
                                let wait_us = batch_start
                                    .duration_since(enqueued_at)
                                    .as_micros()
                                    .min(u128::from(u64::MAX)) as u64;
                                wait_us_total = wait_us_total.saturating_add(wait_us);
                                wait_us_max = wait_us_max.max(wait_us);
                                txs.push(tx);
                                requests.push(to_rpc_pre_accept(req));
                            }
                            // Recycle item vector now that all items were drained.
                            item_pool.put(items);

                            peer_stats
                                .pre_accept_last_dequeue_us
                                .store(epoch_micros(), Ordering::Relaxed);
                            peer_stats
                                .pre_accept_wait_total_us
                                .fetch_add(wait_us_total, Ordering::Relaxed);
                            peer_stats
                                .pre_accept_wait_max_us
                                .fetch_max(wait_us_max, Ordering::Relaxed);
                            peer_stats
                                .pre_accept_wait_count
                                .fetch_add(batch_len, Ordering::Relaxed);

                            // Saturated lanes wait for one completion before dispatching more.
                            let permit = acquire_with_progress(&limiter, &mut in_flight).await;
                            let client = client.clone();
                            let stats = stats.clone();
                            let peer_stats = peer_stats.clone();
                            let v2_capability = v2_capability.clone();
                            in_flight.push(async move {
                                let _permit = permit;
                                peer_stats
                                    .pre_accept_inflight
                                    .fetch_add(1, Ordering::Relaxed);
                                let rpc_start = std::time::Instant::now();
                                let client_v2 = client.clone();
                                let mut path_stats = BatchPathStats::default();
                                let result = call_batch_v2_only(
                                    timeout,
                                    &v2_capability,
                                    requests,
                                    "pre_accept_batch_v2",
                                    move |packed| {
                                        let client = client_v2.clone();
                                        async move { client.pre_accept_batch_v2(packed).await }
                                    },
                                    &mut path_stats,
                                )
                                .await;
                                let rpc_us = rpc_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                                peer_stats
                                    .pre_accept_inflight
                                    .fetch_sub(1, Ordering::Relaxed);
                                stats.record_pre_accept_batch(batch_len, wait_us_total, wait_us_max, rpc_us);
                                peer_stats.pre_accept_latency.record(rpc_us);
                                peer_stats.record_pre_accept_path_stats(path_stats);

                                match result {
                                    Ok((responses, requests)) => {
                                        if responses.len() != txs.len() {
                                            // Reject if server returned a mismatched response count.
                                            let err = anyhow::anyhow!(
                                                "pre_accept_batch response count mismatch (expected {}, got {})",
                                                txs.len(),
                                                responses.len()
                                            );
                                            peer_stats.pre_accept_errors.fetch_add(1, Ordering::Relaxed);
                                            for tx in txs.drain(..) {
                                                let _ = tx.send(Err(anyhow::anyhow!("{err}")));
                                            }
                                            return WaiterBatchRecycle { txs, requests };
                                        }

                                        for (tx, r) in txs.drain(..).zip(responses) {
                                            let _ = tx.send(Ok(from_rpc_pre_accept(r)));
                                        }
                                        WaiterBatchRecycle { txs, requests }
                                    }
                                    Err(err) => {
                                        // RPC error: notify all waiters.
                                        stats.record_pre_accept_error();
                                        if err
                                            .to_string()
                                            .contains("timed out")
                                        {
                                            peer_stats
                                                .pre_accept_timeouts
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                        peer_stats.pre_accept_errors.fetch_add(1, Ordering::Relaxed);
                                        let err = anyhow::anyhow!("{err}");
                                        for tx in txs.drain(..) {
                                            let _ = tx.send(Err(anyhow::anyhow!("{err}")));
                                        }
                                        // Preserve pool capacity after errors so the next batch
                                        // can reuse request storage without fresh allocation.
                                        let requests = Vec::with_capacity(batch_len as usize);
                                        WaiterBatchRecycle { txs, requests }
                                    }
                                }
                            });
                        }
                        None => {
                            rx_closed = true;
                        }
                    }
                }
                done = in_flight.next(), if !in_flight.is_empty() => {
                    // Keep driving in-flight futures even when queue load is low.
                    if let Some(recycle) = done {
                        tx_pool.put(recycle.txs);
                        request_pool.put(recycle.requests);
                    }
                }
            }
        }
    });
}

/// Spawn an accept batching worker for one peer.
///
/// Purpose:
/// - Batch accept RPC calls while maintaining bounded concurrency.
///
/// Design:
/// - Single worker task owns queue dequeue and in-flight batch polling.
/// - Uses `FuturesUnordered` to avoid per-batch spawned tasks.
/// - Applies limiter permits before launching each in-flight batch future.
///
/// Inputs:
/// - Peer client, queue receiver, timeout/batching knobs, shared stats, and
///   one limiter for this RPC lane.
///
/// Outputs:
/// - Batched accept RPC calls with response fan-out to original waiters.
fn spawn_accept_batcher(
    client: rpc::HoloRpcClient,
    mut rx: mpsc::Receiver<AcceptWork>,
    timeout: Duration,
    batch_max: usize,
    batch_wait: Duration,
    stats: Arc<RpcStats>,
    peer_stats: Arc<PeerStats>,
    limiter: Arc<InflightLimiter>,
) {
    tokio::spawn(async move {
        let reuse_max_capacity = batch_max
            .saturating_mul(REUSE_POOL_CAPACITY_MULTIPLIER)
            .max(1);
        let reuse_max_cached = limiter.current().clamp(1, REUSE_POOL_MAX_CACHED);
        let mut item_pool = ReuseVecPool::<AcceptWork>::new(reuse_max_cached, reuse_max_capacity);
        let mut tx_pool = ReuseVecPool::<oneshot::Sender<anyhow::Result<AcceptResponse>>>::new(
            reuse_max_cached,
            reuse_max_capacity,
        );
        let mut request_pool =
            ReuseVecPool::<rpc::AcceptRequest>::new(reuse_max_cached, reuse_max_capacity);
        let v2_capability = Arc::new(BatchLaneCapability::default());
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut rx_closed = false;
        loop {
            // Exit only after queue shutdown and all in-flight RPCs complete.
            if rx_closed && in_flight.is_empty() {
                break;
            }
            tokio::select! {
                maybe = rx.recv(), if !rx_closed => {
                    match maybe {
                        Some(first) => {
                            let items_buf = item_pool.take(batch_max.max(1));
                            let mut items = collect_batch_with_progress(
                                items_buf,
                                first,
                                &mut rx,
                                batch_max,
                                batch_wait,
                                &mut in_flight,
                                |recycle: WaiterBatchRecycle<AcceptResponse, rpc::AcceptRequest>| {
                                    tx_pool.put(recycle.txs);
                                    request_pool.put(recycle.requests);
                                },
                            ).await;

                            let batch_len = items.len() as u64;
                            peer_stats
                                .accept_queue
                                .fetch_sub(batch_len, Ordering::Relaxed);
                            let mut txs = tx_pool.take(items.len());
                            let mut requests = request_pool.take(items.len());
                            let batch_start = std::time::Instant::now();
                            let mut wait_us_total = 0u64;
                            let mut wait_us_max = 0u64;
                            // Build one RPC payload for the current dequeued batch.
                            for AcceptWork {
                                req,
                                tx,
                                enqueued_at,
                            } in items.drain(..)
                            {
                                // Track queue wait time for each request in the batch.
                                let wait_us = batch_start
                                    .duration_since(enqueued_at)
                                    .as_micros()
                                    .min(u128::from(u64::MAX)) as u64;
                                wait_us_total = wait_us_total.saturating_add(wait_us);
                                wait_us_max = wait_us_max.max(wait_us);
                                txs.push(tx);
                                requests.push(to_rpc_accept(req));
                            }
                            // Recycle item vector now that all items were drained.
                            item_pool.put(items);

                            peer_stats
                                .accept_last_dequeue_us
                                .store(epoch_micros(), Ordering::Relaxed);
                            peer_stats
                                .accept_wait_total_us
                                .fetch_add(wait_us_total, Ordering::Relaxed);
                            peer_stats
                                .accept_wait_max_us
                                .fetch_max(wait_us_max, Ordering::Relaxed);
                            peer_stats
                                .accept_wait_count
                                .fetch_add(batch_len, Ordering::Relaxed);

                            // Saturated lanes wait for one completion before dispatching more.
                            let permit = acquire_with_progress(&limiter, &mut in_flight).await;
                            let client = client.clone();
                            let stats = stats.clone();
                            let peer_stats = peer_stats.clone();
                            let v2_capability = v2_capability.clone();
                            in_flight.push(async move {
                                let _permit = permit;
                                peer_stats.accept_inflight.fetch_add(1, Ordering::Relaxed);
                                let rpc_start = std::time::Instant::now();
                                let client_v2 = client.clone();
                                let mut path_stats = BatchPathStats::default();
                                let result = call_batch_v2_only(
                                    timeout,
                                    &v2_capability,
                                    requests,
                                    "accept_batch_v2",
                                    move |packed| {
                                        let client = client_v2.clone();
                                        async move { client.accept_batch_v2(packed).await }
                                    },
                                    &mut path_stats,
                                )
                                .await;
                                let rpc_us = rpc_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                                peer_stats.accept_inflight.fetch_sub(1, Ordering::Relaxed);
                                stats.record_accept_batch(batch_len, wait_us_total, wait_us_max, rpc_us);
                                peer_stats.accept_latency.record(rpc_us);
                                peer_stats.record_accept_path_stats(path_stats);

                                match result {
                                    Ok((responses, requests)) => {
                                        if responses.len() != txs.len() {
                                            // Reject if server returned a mismatched response count.
                                            let err = anyhow::anyhow!(
                                                "accept_batch response count mismatch (expected {}, got {})",
                                                txs.len(),
                                                responses.len()
                                            );
                                            peer_stats.accept_errors.fetch_add(1, Ordering::Relaxed);
                                            for tx in txs.drain(..) {
                                                let _ = tx.send(Err(anyhow::anyhow!("{err}")));
                                            }
                                            return WaiterBatchRecycle { txs, requests };
                                        }

                                        for (tx, r) in txs.drain(..).zip(responses) {
                                            let _ = tx.send(Ok(from_rpc_accept(r)));
                                        }
                                        WaiterBatchRecycle { txs, requests }
                                    }
                                    Err(err) => {
                                        // RPC error: notify all waiters.
                                        stats.record_accept_error();
                                        if err
                                            .to_string()
                                            .contains("timed out")
                                        {
                                            peer_stats.accept_timeouts.fetch_add(1, Ordering::Relaxed);
                                        }
                                        peer_stats.accept_errors.fetch_add(1, Ordering::Relaxed);
                                        let err = anyhow::anyhow!("{err}");
                                        for tx in txs.drain(..) {
                                            let _ = tx.send(Err(anyhow::anyhow!("{err}")));
                                        }
                                        // Preserve pool capacity after errors so the next batch
                                        // can reuse request storage without fresh allocation.
                                        let requests = Vec::with_capacity(batch_len as usize);
                                        WaiterBatchRecycle { txs, requests }
                                    }
                                }
                            });
                        }
                        None => {
                            rx_closed = true;
                        }
                    }
                }
                done = in_flight.next(), if !in_flight.is_empty() => {
                    // Keep driving in-flight futures even when queue load is low.
                    if let Some(recycle) = done {
                        tx_pool.put(recycle.txs);
                        request_pool.put(recycle.requests);
                    }
                }
            }
        }
    });
}

/// Spawn a commit batching worker for one peer.
///
/// Purpose:
/// - Batch commit RPC calls with bounded concurrency and predictable queue
///   behavior.
///
/// Design:
/// - One worker task handles dequeue + in-flight polling in one loop.
/// - Uses `FuturesUnordered` rather than per-batch spawned helper tasks.
/// - Enforces concurrency with `InflightLimiter`.
///
/// Inputs:
/// - Peer client, queue receiver, timeout/batching knobs, shared stats, and
///   one limiter for this RPC lane.
///
/// Outputs:
/// - Batched commit RPC calls with response fan-out.
fn spawn_commit_batcher(
    client: rpc::HoloRpcClient,
    mut rx: mpsc::Receiver<CommitWork>,
    timeout: Duration,
    batch_max: usize,
    batch_wait: Duration,
    stats: Arc<RpcStats>,
    peer_stats: Arc<PeerStats>,
    limiter: Arc<InflightLimiter>,
) {
    tokio::spawn(async move {
        let reuse_max_capacity = batch_max
            .saturating_mul(REUSE_POOL_CAPACITY_MULTIPLIER)
            .max(1);
        let reuse_max_cached = limiter.current().clamp(1, REUSE_POOL_MAX_CACHED);
        let mut item_pool = ReuseVecPool::<CommitWork>::new(reuse_max_cached, reuse_max_capacity);
        let mut tx_pool = ReuseVecPool::<oneshot::Sender<anyhow::Result<CommitResponse>>>::new(
            reuse_max_cached,
            reuse_max_capacity,
        );
        let mut request_pool =
            ReuseVecPool::<rpc::CommitRequest>::new(reuse_max_cached, reuse_max_capacity);
        let v2_capability = Arc::new(BatchLaneCapability::default());
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut rx_closed = false;
        loop {
            // Exit only after queue shutdown and all in-flight RPCs complete.
            if rx_closed && in_flight.is_empty() {
                break;
            }
            tokio::select! {
                maybe = rx.recv(), if !rx_closed => {
                    match maybe {
                        Some(first) => {
                            let items_buf = item_pool.take(batch_max.max(1));
                            let mut items = collect_batch_with_progress(
                                items_buf,
                                first,
                                &mut rx,
                                batch_max,
                                batch_wait,
                                &mut in_flight,
                                |recycle: WaiterBatchRecycle<CommitResponse, rpc::CommitRequest>| {
                                    tx_pool.put(recycle.txs);
                                    request_pool.put(recycle.requests);
                                },
                            ).await;

                            let batch_len = items.len() as u64;
                            peer_stats
                                .commit_queue
                                .fetch_sub(batch_len, Ordering::Relaxed);
                            let mut txs = tx_pool.take(items.len());
                            let mut requests = request_pool.take(items.len());
                            let batch_start = std::time::Instant::now();
                            let mut wait_us_total = 0u64;
                            let mut wait_us_max = 0u64;
                            // Build one RPC payload for the current dequeued batch.
                            for CommitWork {
                                req,
                                tx,
                                enqueued_at,
                            } in items.drain(..)
                            {
                                // Track queue wait time for each request in the batch.
                                let wait_us = batch_start
                                    .duration_since(enqueued_at)
                                    .as_micros()
                                    .min(u128::from(u64::MAX)) as u64;
                                wait_us_total = wait_us_total.saturating_add(wait_us);
                                wait_us_max = wait_us_max.max(wait_us);
                                txs.push(tx);
                                requests.push(to_rpc_commit(req));
                            }
                            // Recycle item vector now that all items were drained.
                            item_pool.put(items);

                            peer_stats
                                .commit_last_dequeue_us
                                .store(epoch_micros(), Ordering::Relaxed);
                            peer_stats
                                .commit_wait_total_us
                                .fetch_add(wait_us_total, Ordering::Relaxed);
                            peer_stats
                                .commit_wait_max_us
                                .fetch_max(wait_us_max, Ordering::Relaxed);
                            peer_stats
                                .commit_wait_count
                                .fetch_add(batch_len, Ordering::Relaxed);

                            // Saturated lanes wait for one completion before dispatching more.
                            let permit = acquire_with_progress(&limiter, &mut in_flight).await;
                            let client = client.clone();
                            let stats = stats.clone();
                            let peer_stats = peer_stats.clone();
                            let v2_capability = v2_capability.clone();
                            in_flight.push(async move {
                                let _permit = permit;
                                peer_stats.commit_inflight.fetch_add(1, Ordering::Relaxed);
                                let rpc_start = std::time::Instant::now();
                                let client_v2 = client.clone();
                                let mut path_stats = BatchPathStats::default();
                                let result = call_batch_v2_only(
                                    timeout,
                                    &v2_capability,
                                    requests,
                                    "commit_batch_v2",
                                    move |packed| {
                                        let client = client_v2.clone();
                                        async move { client.commit_batch_v2(packed).await }
                                    },
                                    &mut path_stats,
                                )
                                .await;
                                let rpc_us = rpc_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                                peer_stats.commit_inflight.fetch_sub(1, Ordering::Relaxed);
                                stats.record_commit_batch(batch_len, wait_us_total, wait_us_max, rpc_us);
                                peer_stats.commit_latency.record(rpc_us);
                                peer_stats.record_commit_path_stats(path_stats);

                                match result {
                                    Ok((responses, requests)) => {
                                        if responses.len() != txs.len() {
                                            // Reject if server returned a mismatched response count.
                                            let err = anyhow::anyhow!(
                                                "commit_batch response count mismatch (expected {}, got {})",
                                                txs.len(),
                                                responses.len()
                                            );
                                            peer_stats.commit_errors.fetch_add(1, Ordering::Relaxed);
                                            for tx in txs.drain(..) {
                                                let _ = tx.send(Err(anyhow::anyhow!("{err}")));
                                            }
                                            return WaiterBatchRecycle { txs, requests };
                                        }

                                        for (tx, r) in txs.drain(..).zip(responses) {
                                            let _ = tx.send(Ok(from_rpc_commit(r)));
                                        }
                                        WaiterBatchRecycle { txs, requests }
                                    }
                                    Err(err) => {
                                        // RPC error: notify all waiters.
                                        stats.record_commit_error();
                                        if err
                                            .to_string()
                                            .contains("timed out")
                                        {
                                            peer_stats.commit_timeouts.fetch_add(1, Ordering::Relaxed);
                                        }
                                        peer_stats.commit_errors.fetch_add(1, Ordering::Relaxed);
                                        let err = anyhow::anyhow!("{err}");
                                        for tx in txs.drain(..) {
                                            let _ = tx.send(Err(anyhow::anyhow!("{err}")));
                                        }
                                        // Preserve pool capacity after errors so the next batch
                                        // can reuse request storage without fresh allocation.
                                        let requests = Vec::with_capacity(batch_len as usize);
                                        WaiterBatchRecycle { txs, requests }
                                    }
                                }
                            });
                        }
                        None => {
                            rx_closed = true;
                        }
                    }
                }
                done = in_flight.next(), if !in_flight.is_empty() => {
                    // Keep driving in-flight futures even when queue load is low.
                    if let Some(recycle) = done {
                        tx_pool.put(recycle.txs);
                        request_pool.put(recycle.requests);
                    }
                }
            }
        }
    });
}

/// Spawn a recover batching worker for one peer.
///
/// Purpose:
/// - Batch recover RPC calls and complete all coalesced waiters for each txn.
///
/// Design:
/// - Single worker task owns dequeue and in-flight batch polling.
/// - Uses `FuturesUnordered` to avoid per-batch helper task churn.
/// - Applies limiter permits before launching each recover batch future.
///
/// Inputs:
/// - Peer client, queue receiver, timeout/batching knobs, shared stats, the
///   recover coalescer, and one limiter for this RPC lane.
///
/// Outputs:
/// - Batched recover RPC calls and completion fan-out through coalescer.
fn spawn_recover_batcher(
    client: rpc::HoloRpcClient,
    mut rx: mpsc::Receiver<RecoverWork>,
    timeout: Duration,
    batch_max: usize,
    batch_wait: Duration,
    stats: Arc<RpcStats>,
    peer_stats: Arc<PeerStats>,
    recover_coalescer: Arc<RecoverCoalescer>,
    limiter: Arc<InflightLimiter>,
) {
    tokio::spawn(async move {
        let reuse_max_capacity = batch_max
            .saturating_mul(REUSE_POOL_CAPACITY_MULTIPLIER)
            .max(1);
        let reuse_max_cached = limiter.current().clamp(1, REUSE_POOL_MAX_CACHED);
        let mut item_pool = ReuseVecPool::<RecoverWork>::new(reuse_max_cached, reuse_max_capacity);
        let mut txn_id_pool = ReuseVecPool::<TxnId>::new(reuse_max_cached, reuse_max_capacity);
        let mut request_pool =
            ReuseVecPool::<rpc::RecoverRequest>::new(reuse_max_cached, reuse_max_capacity);
        let v2_capability = Arc::new(BatchLaneCapability::default());
        let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();
        let mut rx_closed = false;
        loop {
            // Exit only after queue shutdown and all in-flight RPCs complete.
            if rx_closed && in_flight.is_empty() {
                break;
            }
            tokio::select! {
                maybe = rx.recv(), if !rx_closed => {
                    match maybe {
                        Some(first) => {
                            let items_buf = item_pool.take(batch_max.max(1));
                            let mut items = collect_batch_with_progress(
                                items_buf,
                                first,
                                &mut rx,
                                batch_max,
                                batch_wait,
                                &mut in_flight,
                                |recycle: RecoverBatchRecycle| {
                                    txn_id_pool.put(recycle.txn_ids);
                                    request_pool.put(recycle.requests);
                                },
                            ).await;

                            let batch_len = items.len() as u64;
                            peer_stats
                                .recover_queue
                                .fetch_sub(batch_len, Ordering::Relaxed);
                            let mut requests = request_pool.take(items.len());
                            let mut txn_ids = txn_id_pool.take(items.len());
                            let batch_start = std::time::Instant::now();
                            let mut wait_us_total = 0u64;
                            let mut wait_us_max = 0u64;
                            // Build one recover RPC payload for this dequeued batch.
                            for RecoverWork { req, enqueued_at } in items.drain(..) {
                                // Track queue wait time for each request in the batch.
                                let wait_us = batch_start
                                    .duration_since(enqueued_at)
                                    .as_micros()
                                    .min(u128::from(u64::MAX)) as u64;
                                wait_us_total = wait_us_total.saturating_add(wait_us);
                                wait_us_max = wait_us_max.max(wait_us);
                                txn_ids.push(req.txn_id);
                                requests.push(to_rpc_recover(req));
                            }
                            // Recycle item vector now that all items were drained.
                            item_pool.put(items);

                            peer_stats
                                .recover_last_dequeue_us
                                .store(epoch_micros(), Ordering::Relaxed);
                            peer_stats
                                .recover_wait_total_us
                                .fetch_add(wait_us_total, Ordering::Relaxed);
                            peer_stats
                                .recover_wait_max_us
                                .fetch_max(wait_us_max, Ordering::Relaxed);
                            peer_stats
                                .recover_wait_count
                                .fetch_add(batch_len, Ordering::Relaxed);

                            // Saturated lanes wait for one completion before dispatching more.
                            let permit = acquire_with_progress(&limiter, &mut in_flight).await;
                            let client = client.clone();
                            let stats = stats.clone();
                            let peer_stats = peer_stats.clone();
                            let recover_coalescer = recover_coalescer.clone();
                            let v2_capability = v2_capability.clone();
                            in_flight.push(async move {
                                let _permit = permit;
                                peer_stats.recover_inflight.fetch_add(1, Ordering::Relaxed);
                                let rpc_start = std::time::Instant::now();
                                let client_v2 = client.clone();
                                let mut path_stats = BatchPathStats::default();
                                let result = call_batch_v2_only(
                                    timeout,
                                    &v2_capability,
                                    requests,
                                    "recover_batch_v2",
                                    move |packed| {
                                        let client = client_v2.clone();
                                        async move { client.recover_batch_v2(packed).await }
                                    },
                                    &mut path_stats,
                                )
                                .await;
                                let rpc_us = rpc_start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                                peer_stats.recover_inflight.fetch_sub(1, Ordering::Relaxed);
                                stats.record_recover_batch(batch_len, wait_us_total, wait_us_max, rpc_us);
                                peer_stats.recover_latency.record(rpc_us);
                                peer_stats.record_recover_path_stats(path_stats);

                                match result {
                                    Ok((responses, requests)) => {
                                        if responses.len() != txn_ids.len() {
                                            // Reject if server returned a mismatched response count.
                                            let err = anyhow::anyhow!(
                                                "recover_batch response count mismatch (expected {}, got {})",
                                                txn_ids.len(),
                                                responses.len()
                                            );
                                            peer_stats.recover_errors.fetch_add(1, Ordering::Relaxed);
                                            for txn_id in txn_ids.drain(..) {
                                                recover_coalescer.complete_err(txn_id, &err).await;
                                            }
                                            return RecoverBatchRecycle { txn_ids, requests };
                                        }

                                        for (txn_id, r) in txn_ids.drain(..).zip(responses) {
                                            recover_coalescer
                                                .complete_ok(txn_id, from_rpc_recover(r))
                                                .await;
                                        }
                                        RecoverBatchRecycle { txn_ids, requests }
                                    }
                                    Err(err) => {
                                        // RPC error: notify all waiters.
                                        stats.record_recover_error();
                                        peer_stats.recover_errors.fetch_add(1, Ordering::Relaxed);
                                        let err = anyhow::anyhow!("{err}");
                                        for txn_id in txn_ids.drain(..) {
                                            recover_coalescer.complete_err(txn_id, &err).await;
                                        }
                                        // Preserve pool capacity after errors so the next batch
                                        // can reuse request storage without fresh allocation.
                                        let requests = Vec::with_capacity(batch_len as usize);
                                        RecoverBatchRecycle { txn_ids, requests }
                                    }
                                }
                            });
                        }
                        None => {
                            rx_closed = true;
                        }
                    }
                }
                done = in_flight.next(), if !in_flight.is_empty() => {
                    // Keep driving in-flight futures even when queue load is low.
                    if let Some(recycle) = done {
                        txn_id_pool.put(recycle.txn_ids);
                        request_pool.put(recycle.requests);
                    }
                }
            }
        }
    });
}

/// Aggregated RPC stats across all peers.
#[derive(Default)]
pub struct RpcStats {
    kv_get_batches: AtomicU64,
    kv_get_items: AtomicU64,
    kv_get_rpc_us: AtomicU64,
    kv_get_wait_us: AtomicU64,
    kv_get_max_batch: AtomicU64,
    kv_get_max_wait_us: AtomicU64,
    kv_get_errors: AtomicU64,

    kv_batch_get_calls: AtomicU64,
    kv_batch_get_rpc_us: AtomicU64,
    kv_batch_get_errors: AtomicU64,

    pre_accept_batches: AtomicU64,
    pre_accept_items: AtomicU64,
    pre_accept_rpc_us: AtomicU64,
    pre_accept_wait_us: AtomicU64,
    pre_accept_max_batch: AtomicU64,
    pre_accept_max_wait_us: AtomicU64,
    pre_accept_errors: AtomicU64,

    accept_batches: AtomicU64,
    accept_items: AtomicU64,
    accept_rpc_us: AtomicU64,
    accept_wait_us: AtomicU64,
    accept_max_batch: AtomicU64,
    accept_max_wait_us: AtomicU64,
    accept_errors: AtomicU64,

    commit_batches: AtomicU64,
    commit_items: AtomicU64,
    commit_rpc_us: AtomicU64,
    commit_wait_us: AtomicU64,
    commit_max_batch: AtomicU64,
    commit_max_wait_us: AtomicU64,
    commit_errors: AtomicU64,

    recover_batches: AtomicU64,
    recover_items: AtomicU64,
    recover_rpc_us: AtomicU64,
    recover_wait_us: AtomicU64,
    recover_max_batch: AtomicU64,
    recover_max_wait_us: AtomicU64,
    recover_errors: AtomicU64,
}

/// Snapshot of `RpcStats` for logging, with counters reset on read.
#[derive(Default, Debug, Clone)]
pub struct RpcStatsSnapshot {
    pub kv_get_batches: u64,
    pub kv_get_items: u64,
    pub kv_get_rpc_us: u64,
    pub kv_get_wait_us: u64,
    pub kv_get_max_batch: u64,
    pub kv_get_max_wait_us: u64,
    pub kv_get_errors: u64,

    pub kv_batch_get_calls: u64,
    pub kv_batch_get_rpc_us: u64,
    pub kv_batch_get_errors: u64,

    pub pre_accept_batches: u64,
    pub pre_accept_items: u64,
    pub pre_accept_rpc_us: u64,
    pub pre_accept_wait_us: u64,
    pub pre_accept_max_batch: u64,
    pub pre_accept_max_wait_us: u64,
    pub pre_accept_errors: u64,

    pub accept_batches: u64,
    pub accept_items: u64,
    pub accept_rpc_us: u64,
    pub accept_wait_us: u64,
    pub accept_max_batch: u64,
    pub accept_max_wait_us: u64,
    pub accept_errors: u64,

    pub commit_batches: u64,
    pub commit_items: u64,
    pub commit_rpc_us: u64,
    pub commit_wait_us: u64,
    pub commit_max_batch: u64,
    pub commit_max_wait_us: u64,
    pub commit_errors: u64,

    pub recover_batches: u64,
    pub recover_items: u64,
    pub recover_rpc_us: u64,
    pub recover_wait_us: u64,
    pub recover_max_batch: u64,
    pub recover_max_wait_us: u64,
    pub recover_errors: u64,
}

/// Simple fixed-bucket histogram for latency tracking.
struct LatencyHistogram {
    counts: [AtomicU64; LATENCY_BUCKETS_US.len() + 1],
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

/// Snapshot of latency distribution.
#[derive(Default, Debug, Clone)]
pub struct LatencySnapshot {
    pub count: u64,
    pub avg_ms: f64,
    #[allow(dead_code)]
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl Default for LatencyHistogram {
    /// Initialize a histogram with empty buckets.
    fn default() -> Self {
        Self {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }
}

impl LatencyHistogram {
    /// Record a single latency observation.
    fn record(&self, us: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(us, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
        let mut idx = LATENCY_BUCKETS_US.len();
        for (i, upper) in LATENCY_BUCKETS_US.iter().enumerate() {
            if us <= *upper {
                idx = i;
                break;
            }
        }
        self.counts[idx].fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot and reset the histogram, computing percentiles.
    fn snapshot_and_reset(&self) -> LatencySnapshot {
        let count = self.count.swap(0, Ordering::Relaxed);
        let total_us = self.total_us.swap(0, Ordering::Relaxed);
        let max_us = self.max_us.swap(0, Ordering::Relaxed);

        let mut buckets = [0u64; LATENCY_BUCKETS_US.len() + 1];
        for (i, c) in self.counts.iter().enumerate() {
            buckets[i] = c.swap(0, Ordering::Relaxed);
        }

        let p50 = percentile_us(&buckets, 50.0, max_us);
        let p95 = percentile_us(&buckets, 95.0, max_us);
        let p99 = percentile_us(&buckets, 99.0, max_us);

        LatencySnapshot {
            count,
            avg_ms: if count == 0 {
                // Avoid divide-by-zero.
                0.0
            } else {
                (total_us as f64 / count as f64) / 1000.0
            },
            p50_ms: p50 as f64 / 1000.0,
            p95_ms: p95 as f64 / 1000.0,
            p99_ms: p99 as f64 / 1000.0,
            max_ms: max_us as f64 / 1000.0,
        }
    }
}

/// Compute a percentile value from bucketed counts.
fn percentile_us(counts: &[u64; LATENCY_BUCKETS_US.len() + 1], p: f64, max_us: u64) -> u64 {
    let total: u64 = counts.iter().sum();
    if total == 0 {
        // No samples recorded.
        return 0;
    }
    let target = (total as f64 * (p / 100.0)).ceil() as u64;
    let mut cumulative = 0u64;
    for (i, count) in counts.iter().enumerate() {
        cumulative += *count;
        if cumulative >= target {
            if i < LATENCY_BUCKETS_US.len() {
                return LATENCY_BUCKETS_US[i];
            }
            // Overflow bucket: return max observed or last boundary.
            return max_us.max(LATENCY_BUCKETS_US[LATENCY_BUCKETS_US.len() - 1]);
        }
    }
    max_us
}

/// Per-peer counters and histograms used to build `PeerStatsSnapshot`.
///
/// Purpose:
/// - Track queue, wait, latency, and packed-v2 capability telemetry per peer.
///
/// Design:
/// - Uses atomics for low-overhead concurrent updates.
/// - Maintains separate read-lane and consensus-lane counters.
///
/// Inputs:
/// - Updated by transport enqueue paths and worker completions.
///
/// Outputs:
/// - Reset-on-read snapshots for logging and adaptive tuning.
#[derive(Default)]
struct PeerStats {
    kv_get_queue: AtomicU64,
    pre_accept_queue: AtomicU64,
    accept_queue: AtomicU64,
    commit_queue: AtomicU64,
    recover_queue: AtomicU64,
    kv_get_inflight: AtomicU64,
    pre_accept_inflight: AtomicU64,
    accept_inflight: AtomicU64,
    commit_inflight: AtomicU64,
    recover_inflight: AtomicU64,
    kv_get_sent: AtomicU64,
    pre_accept_sent: AtomicU64,
    accept_sent: AtomicU64,
    commit_sent: AtomicU64,
    recover_sent: AtomicU64,
    kv_get_latency: LatencyHistogram,
    pre_accept_latency: LatencyHistogram,
    accept_latency: LatencyHistogram,
    commit_latency: LatencyHistogram,
    recover_latency: LatencyHistogram,
    kv_get_last_enqueue_us: AtomicU64,
    kv_get_last_dequeue_us: AtomicU64,
    kv_get_wait_total_us: AtomicU64,
    kv_get_wait_max_us: AtomicU64,
    kv_get_wait_count: AtomicU64,
    kv_get_head_wait_total_us: AtomicU64,
    kv_get_head_wait_max_us: AtomicU64,
    kv_get_head_wait_count: AtomicU64,
    kv_get_limiter_saturated: AtomicU64,
    kv_get_v2_attempted: AtomicU64,
    kv_get_v2_disabled_events: AtomicU64,
    pre_accept_v2_attempted: AtomicU64,
    pre_accept_v2_disabled_events: AtomicU64,
    accept_v2_attempted: AtomicU64,
    accept_v2_disabled_events: AtomicU64,
    commit_v2_attempted: AtomicU64,
    commit_v2_disabled_events: AtomicU64,
    recover_v2_attempted: AtomicU64,
    recover_v2_disabled_events: AtomicU64,
    pre_accept_last_enqueue_us: AtomicU64,
    accept_last_enqueue_us: AtomicU64,
    commit_last_enqueue_us: AtomicU64,
    pre_accept_last_dequeue_us: AtomicU64,
    accept_last_dequeue_us: AtomicU64,
    commit_last_dequeue_us: AtomicU64,
    pre_accept_wait_total_us: AtomicU64,
    pre_accept_wait_max_us: AtomicU64,
    pre_accept_wait_count: AtomicU64,
    accept_wait_total_us: AtomicU64,
    accept_wait_max_us: AtomicU64,
    accept_wait_count: AtomicU64,
    commit_wait_total_us: AtomicU64,
    commit_wait_max_us: AtomicU64,
    commit_wait_count: AtomicU64,
    recover_queue_peak: AtomicU64,
    recover_last_enqueue_us: AtomicU64,
    recover_last_dequeue_us: AtomicU64,
    recover_wait_total_us: AtomicU64,
    recover_wait_max_us: AtomicU64,
    recover_wait_count: AtomicU64,
    kv_get_errors: AtomicU64,
    pre_accept_errors: AtomicU64,
    accept_errors: AtomicU64,
    commit_errors: AtomicU64,
    recover_errors: AtomicU64,
    pre_accept_timeouts: AtomicU64,
    accept_timeouts: AtomicU64,
    commit_timeouts: AtomicU64,
    pre_accept_queue_full: AtomicU64,
    accept_queue_full: AtomicU64,
    commit_queue_full: AtomicU64,
    recover_queue_full: AtomicU64,
}

/// Snapshot of per-peer stats for logging and tuning.
///
/// Purpose:
/// - Provide one immutable per-peer telemetry view for logs/CSV/tuning loops.
///
/// Design:
/// - Aggregates queue, wait, latency, capability, and limiter fields.
/// - Values are computed from reset-on-read counters in `PeerStats`.
///
/// Inputs:
/// - Produced by `PeerStats::snapshot_and_reset`.
///
/// Outputs:
/// - Stable, serializable metrics payload used by observability code.
#[derive(Default, Debug, Clone)]
pub struct PeerStatsSnapshot {
    pub kv_get_queue: u64,
    pub pre_accept_queue: u64,
    pub accept_queue: u64,
    pub commit_queue: u64,
    pub recover_queue: u64,
    pub kv_get_inflight: u64,
    pub pre_accept_inflight: u64,
    pub accept_inflight: u64,
    pub commit_inflight: u64,
    pub recover_inflight: u64,
    pub kv_get_sent: u64,
    pub pre_accept_sent: u64,
    pub accept_sent: u64,
    pub commit_sent: u64,
    pub recover_sent: u64,
    pub kv_get_latency: LatencySnapshot,
    pub kv_get_wait_count: u64,
    pub kv_get_wait_avg_ms: f64,
    pub kv_get_wait_max_ms: f64,
    pub kv_get_head_wait_count: u64,
    pub kv_get_head_wait_avg_ms: f64,
    pub kv_get_head_wait_max_ms: f64,
    pub kv_get_last_enqueue_age_ms: f64,
    pub kv_get_last_dequeue_age_ms: f64,
    pub kv_get_limiter_saturated: u64,
    pub kv_get_v2_attempted: u64,
    pub kv_get_v2_disabled_events: u64,
    pub pre_accept_v2_attempted: u64,
    pub pre_accept_v2_disabled_events: u64,
    pub accept_v2_attempted: u64,
    pub accept_v2_disabled_events: u64,
    pub commit_v2_attempted: u64,
    pub commit_v2_disabled_events: u64,
    pub recover_v2_attempted: u64,
    pub recover_v2_disabled_events: u64,
    pub pre_accept_latency: LatencySnapshot,
    pub accept_latency: LatencySnapshot,
    pub commit_latency: LatencySnapshot,
    pub recover_latency: LatencySnapshot,
    pub pre_accept_wait_count: u64,
    pub pre_accept_wait_avg_ms: f64,
    pub pre_accept_wait_max_ms: f64,
    pub pre_accept_last_enqueue_age_ms: f64,
    pub pre_accept_last_dequeue_age_ms: f64,
    pub accept_wait_count: u64,
    pub accept_wait_avg_ms: f64,
    pub accept_wait_max_ms: f64,
    pub accept_last_enqueue_age_ms: f64,
    pub accept_last_dequeue_age_ms: f64,
    pub commit_wait_count: u64,
    pub commit_wait_avg_ms: f64,
    pub commit_wait_max_ms: f64,
    pub commit_last_enqueue_age_ms: f64,
    pub commit_last_dequeue_age_ms: f64,
    pub recover_queue_peak: u64,
    pub recover_wait_count: u64,
    pub recover_wait_avg_ms: f64,
    pub recover_wait_max_ms: f64,
    pub recover_last_enqueue_age_ms: f64,
    pub recover_last_dequeue_age_ms: f64,
    pub recover_inflight_txns: u64,
    pub recover_inflight_peak: u64,
    pub recover_coalesced: u64,
    pub recover_enqueued: u64,
    pub recover_waiters_peak: u64,
    pub recover_waiters_avg: f64,
    pub rpc_inflight_limit: u64,
    pub kv_get_inflight_limit: u64,
    pub kv_get_errors: u64,
    pub pre_accept_errors: u64,
    pub accept_errors: u64,
    pub commit_errors: u64,
    pub recover_errors: u64,
    pub pre_accept_timeouts: u64,
    pub accept_timeouts: u64,
    pub commit_timeouts: u64,
    pub pre_accept_queue_full: u64,
    pub accept_queue_full: u64,
    pub commit_queue_full: u64,
    pub recover_queue_full: u64,
}

impl PeerStats {
    /// Snapshot and reset counters, computing averages and ages.
    ///
    /// Purpose:
    /// - Convert mutable per-peer counters into one log-friendly snapshot.
    ///
    /// Design:
    /// - Uses atomic swap/load to avoid allocation and minimize contention.
    /// - Computes wait/age aggregates for read and consensus lanes.
    ///
    /// Inputs:
    /// - `now_us`: current epoch microseconds for age calculations.
    ///
    /// Outputs:
    /// - One `PeerStatsSnapshot` with reset rolling counters.
    fn snapshot_and_reset(&self, now_us: u64) -> PeerStatsSnapshot {
        let kv_get_last_enqueue_us = self.kv_get_last_enqueue_us.load(Ordering::Relaxed);
        let kv_get_last_dequeue_us = self.kv_get_last_dequeue_us.load(Ordering::Relaxed);
        let kv_get_wait_count = self.kv_get_wait_count.swap(0, Ordering::Relaxed);
        let kv_get_wait_total_us = self.kv_get_wait_total_us.swap(0, Ordering::Relaxed);
        let kv_get_wait_max_us = self.kv_get_wait_max_us.swap(0, Ordering::Relaxed);
        let kv_get_wait_avg_ms = if kv_get_wait_count == 0 {
            // No samples recorded.
            0.0
        } else {
            (kv_get_wait_total_us as f64 / kv_get_wait_count as f64) / 1000.0
        };
        let kv_get_wait_max_ms = kv_get_wait_max_us as f64 / 1000.0;
        let kv_get_head_wait_count = self.kv_get_head_wait_count.swap(0, Ordering::Relaxed);
        let kv_get_head_wait_total_us = self.kv_get_head_wait_total_us.swap(0, Ordering::Relaxed);
        let kv_get_head_wait_max_us = self.kv_get_head_wait_max_us.swap(0, Ordering::Relaxed);
        let kv_get_head_wait_avg_ms = if kv_get_head_wait_count == 0 {
            // No samples recorded.
            0.0
        } else {
            (kv_get_head_wait_total_us as f64 / kv_get_head_wait_count as f64) / 1000.0
        };
        let kv_get_head_wait_max_ms = kv_get_head_wait_max_us as f64 / 1000.0;
        let kv_get_last_enqueue_age_ms = if kv_get_last_enqueue_us == 0 {
            // No enqueue has happened yet.
            0.0
        } else {
            now_us.saturating_sub(kv_get_last_enqueue_us) as f64 / 1000.0
        };
        let kv_get_last_dequeue_age_ms = if kv_get_last_dequeue_us == 0 {
            // No dequeue has happened yet.
            0.0
        } else {
            now_us.saturating_sub(kv_get_last_dequeue_us) as f64 / 1000.0
        };

        let pre_accept_last_enqueue_us = self.pre_accept_last_enqueue_us.load(Ordering::Relaxed);
        let accept_last_enqueue_us = self.accept_last_enqueue_us.load(Ordering::Relaxed);
        let commit_last_enqueue_us = self.commit_last_enqueue_us.load(Ordering::Relaxed);
        let pre_accept_last_dequeue_us = self.pre_accept_last_dequeue_us.load(Ordering::Relaxed);
        let accept_last_dequeue_us = self.accept_last_dequeue_us.load(Ordering::Relaxed);
        let commit_last_dequeue_us = self.commit_last_dequeue_us.load(Ordering::Relaxed);

        let pre_accept_wait_count = self.pre_accept_wait_count.swap(0, Ordering::Relaxed);
        let pre_accept_wait_total_us = self.pre_accept_wait_total_us.swap(0, Ordering::Relaxed);
        let pre_accept_wait_max_us = self.pre_accept_wait_max_us.swap(0, Ordering::Relaxed);
        let pre_accept_wait_avg_ms = if pre_accept_wait_count == 0 {
            // No samples recorded.
            0.0
        } else {
            (pre_accept_wait_total_us as f64 / pre_accept_wait_count as f64) / 1000.0
        };
        let pre_accept_wait_max_ms = pre_accept_wait_max_us as f64 / 1000.0;
        let pre_accept_last_enqueue_age_ms = if pre_accept_last_enqueue_us == 0 {
            // No enqueue has happened yet.
            0.0
        } else {
            now_us.saturating_sub(pre_accept_last_enqueue_us) as f64 / 1000.0
        };
        let pre_accept_last_dequeue_age_ms = if pre_accept_last_dequeue_us == 0 {
            // No dequeue has happened yet.
            0.0
        } else {
            now_us.saturating_sub(pre_accept_last_dequeue_us) as f64 / 1000.0
        };

        let accept_wait_count = self.accept_wait_count.swap(0, Ordering::Relaxed);
        let accept_wait_total_us = self.accept_wait_total_us.swap(0, Ordering::Relaxed);
        let accept_wait_max_us = self.accept_wait_max_us.swap(0, Ordering::Relaxed);
        let accept_wait_avg_ms = if accept_wait_count == 0 {
            // No samples recorded.
            0.0
        } else {
            (accept_wait_total_us as f64 / accept_wait_count as f64) / 1000.0
        };
        let accept_wait_max_ms = accept_wait_max_us as f64 / 1000.0;
        let accept_last_enqueue_age_ms = if accept_last_enqueue_us == 0 {
            // No enqueue has happened yet.
            0.0
        } else {
            now_us.saturating_sub(accept_last_enqueue_us) as f64 / 1000.0
        };
        let accept_last_dequeue_age_ms = if accept_last_dequeue_us == 0 {
            // No dequeue has happened yet.
            0.0
        } else {
            now_us.saturating_sub(accept_last_dequeue_us) as f64 / 1000.0
        };

        let commit_wait_count = self.commit_wait_count.swap(0, Ordering::Relaxed);
        let commit_wait_total_us = self.commit_wait_total_us.swap(0, Ordering::Relaxed);
        let commit_wait_max_us = self.commit_wait_max_us.swap(0, Ordering::Relaxed);
        let commit_wait_avg_ms = if commit_wait_count == 0 {
            // No samples recorded.
            0.0
        } else {
            (commit_wait_total_us as f64 / commit_wait_count as f64) / 1000.0
        };
        let commit_wait_max_ms = commit_wait_max_us as f64 / 1000.0;
        let commit_last_enqueue_age_ms = if commit_last_enqueue_us == 0 {
            // No enqueue has happened yet.
            0.0
        } else {
            now_us.saturating_sub(commit_last_enqueue_us) as f64 / 1000.0
        };
        let commit_last_dequeue_age_ms = if commit_last_dequeue_us == 0 {
            // No dequeue has happened yet.
            0.0
        } else {
            now_us.saturating_sub(commit_last_dequeue_us) as f64 / 1000.0
        };

        let recover_last_enqueue_us = self.recover_last_enqueue_us.load(Ordering::Relaxed);
        let recover_last_dequeue_us = self.recover_last_dequeue_us.load(Ordering::Relaxed);
        let recover_wait_count = self.recover_wait_count.swap(0, Ordering::Relaxed);
        let recover_wait_total_us = self.recover_wait_total_us.swap(0, Ordering::Relaxed);
        let recover_wait_max_us = self.recover_wait_max_us.swap(0, Ordering::Relaxed);
        let recover_wait_avg_ms = if recover_wait_count == 0 {
            // No samples recorded.
            0.0
        } else {
            (recover_wait_total_us as f64 / recover_wait_count as f64) / 1000.0
        };
        let recover_wait_max_ms = recover_wait_max_us as f64 / 1000.0;
        let recover_last_enqueue_age_ms = if recover_last_enqueue_us == 0 {
            // No enqueue has happened yet.
            0.0
        } else {
            now_us.saturating_sub(recover_last_enqueue_us) as f64 / 1000.0
        };
        let recover_last_dequeue_age_ms = if recover_last_dequeue_us == 0 {
            // No dequeue has happened yet.
            0.0
        } else {
            now_us.saturating_sub(recover_last_dequeue_us) as f64 / 1000.0
        };

        PeerStatsSnapshot {
            kv_get_queue: self.kv_get_queue.load(Ordering::Relaxed),
            pre_accept_queue: self.pre_accept_queue.load(Ordering::Relaxed),
            accept_queue: self.accept_queue.load(Ordering::Relaxed),
            commit_queue: self.commit_queue.load(Ordering::Relaxed),
            recover_queue: self.recover_queue.load(Ordering::Relaxed),
            kv_get_inflight: self.kv_get_inflight.load(Ordering::Relaxed),
            pre_accept_inflight: self.pre_accept_inflight.load(Ordering::Relaxed),
            accept_inflight: self.accept_inflight.load(Ordering::Relaxed),
            commit_inflight: self.commit_inflight.load(Ordering::Relaxed),
            recover_inflight: self.recover_inflight.load(Ordering::Relaxed),
            kv_get_sent: self.kv_get_sent.swap(0, Ordering::Relaxed),
            pre_accept_sent: self.pre_accept_sent.swap(0, Ordering::Relaxed),
            accept_sent: self.accept_sent.swap(0, Ordering::Relaxed),
            commit_sent: self.commit_sent.swap(0, Ordering::Relaxed),
            recover_sent: self.recover_sent.swap(0, Ordering::Relaxed),
            kv_get_latency: self.kv_get_latency.snapshot_and_reset(),
            kv_get_wait_count,
            kv_get_wait_avg_ms,
            kv_get_wait_max_ms,
            kv_get_head_wait_count,
            kv_get_head_wait_avg_ms,
            kv_get_head_wait_max_ms,
            kv_get_last_enqueue_age_ms,
            kv_get_last_dequeue_age_ms,
            kv_get_limiter_saturated: self.kv_get_limiter_saturated.swap(0, Ordering::Relaxed),
            kv_get_v2_attempted: self.kv_get_v2_attempted.swap(0, Ordering::Relaxed),
            kv_get_v2_disabled_events: self.kv_get_v2_disabled_events.swap(0, Ordering::Relaxed),
            pre_accept_v2_attempted: self.pre_accept_v2_attempted.swap(0, Ordering::Relaxed),
            pre_accept_v2_disabled_events: self
                .pre_accept_v2_disabled_events
                .swap(0, Ordering::Relaxed),
            accept_v2_attempted: self.accept_v2_attempted.swap(0, Ordering::Relaxed),
            accept_v2_disabled_events: self.accept_v2_disabled_events.swap(0, Ordering::Relaxed),
            commit_v2_attempted: self.commit_v2_attempted.swap(0, Ordering::Relaxed),
            commit_v2_disabled_events: self.commit_v2_disabled_events.swap(0, Ordering::Relaxed),
            recover_v2_attempted: self.recover_v2_attempted.swap(0, Ordering::Relaxed),
            recover_v2_disabled_events: self.recover_v2_disabled_events.swap(0, Ordering::Relaxed),
            pre_accept_latency: self.pre_accept_latency.snapshot_and_reset(),
            accept_latency: self.accept_latency.snapshot_and_reset(),
            commit_latency: self.commit_latency.snapshot_and_reset(),
            recover_latency: self.recover_latency.snapshot_and_reset(),
            pre_accept_wait_count,
            pre_accept_wait_avg_ms,
            pre_accept_wait_max_ms,
            pre_accept_last_enqueue_age_ms,
            pre_accept_last_dequeue_age_ms,
            accept_wait_count,
            accept_wait_avg_ms,
            accept_wait_max_ms,
            accept_last_enqueue_age_ms,
            accept_last_dequeue_age_ms,
            commit_wait_count,
            commit_wait_avg_ms,
            commit_wait_max_ms,
            commit_last_enqueue_age_ms,
            commit_last_dequeue_age_ms,
            recover_queue_peak: self.recover_queue_peak.swap(0, Ordering::Relaxed),
            recover_wait_count,
            recover_wait_avg_ms,
            recover_wait_max_ms,
            recover_last_enqueue_age_ms,
            recover_last_dequeue_age_ms,
            recover_inflight_txns: 0,
            recover_inflight_peak: 0,
            recover_coalesced: 0,
            recover_enqueued: 0,
            recover_waiters_peak: 0,
            recover_waiters_avg: 0.0,
            rpc_inflight_limit: 0,
            kv_get_inflight_limit: 0,
            kv_get_errors: self.kv_get_errors.swap(0, Ordering::Relaxed),
            pre_accept_errors: self.pre_accept_errors.swap(0, Ordering::Relaxed),
            accept_errors: self.accept_errors.swap(0, Ordering::Relaxed),
            commit_errors: self.commit_errors.swap(0, Ordering::Relaxed),
            recover_errors: self.recover_errors.swap(0, Ordering::Relaxed),
            pre_accept_timeouts: self.pre_accept_timeouts.swap(0, Ordering::Relaxed),
            accept_timeouts: self.accept_timeouts.swap(0, Ordering::Relaxed),
            commit_timeouts: self.commit_timeouts.swap(0, Ordering::Relaxed),
            pre_accept_queue_full: self.pre_accept_queue_full.swap(0, Ordering::Relaxed),
            accept_queue_full: self.accept_queue_full.swap(0, Ordering::Relaxed),
            commit_queue_full: self.commit_queue_full.swap(0, Ordering::Relaxed),
            recover_queue_full: self.recover_queue_full.swap(0, Ordering::Relaxed),
        }
    }

    /// Record packed-v2 path counters for one completed GET batch.
    ///
    /// Purpose:
    /// - Expose packed-v2 attempt/disable behavior for read-tail diagnosis.
    ///
    /// Design:
    /// - Increment only the counters relevant to the observed path.
    ///
    /// Inputs:
    /// - `path`: per-batch capability path flags.
    ///
    /// Outputs:
    /// - Updated per-peer counters consumed by snapshot logging/CSV.
    fn record_kv_get_path_stats(&self, path: BatchPathStats) {
        if path.v2_attempted {
            self.kv_get_v2_attempted.fetch_add(1, Ordering::Relaxed);
        }
        if path.v2_disabled_event {
            self.kv_get_v2_disabled_events
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record packed-v2 path counters for one completed pre-accept batch.
    ///
    /// Purpose:
    /// - Expose packed-v2 attempt/disable behavior for pre-accept lane diagnosis.
    ///
    /// Design:
    /// - Increments only counters corresponding to observed packed-v2 path flags.
    ///
    /// Inputs:
    /// - `path`: per-batch capability path flags.
    ///
    /// Outputs:
    /// - Updated pre-accept lane counters consumed by snapshots.
    fn record_pre_accept_path_stats(&self, path: BatchPathStats) {
        if path.v2_attempted {
            self.pre_accept_v2_attempted.fetch_add(1, Ordering::Relaxed);
        }
        if path.v2_disabled_event {
            self.pre_accept_v2_disabled_events
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record packed-v2 path counters for one completed accept batch.
    ///
    /// Purpose:
    /// - Expose packed-v2 attempt/disable behavior for accept lane diagnosis.
    ///
    /// Design:
    /// - Increments only counters corresponding to observed packed-v2 path flags.
    ///
    /// Inputs:
    /// - `path`: per-batch capability path flags.
    ///
    /// Outputs:
    /// - Updated accept lane counters consumed by snapshots.
    fn record_accept_path_stats(&self, path: BatchPathStats) {
        if path.v2_attempted {
            self.accept_v2_attempted.fetch_add(1, Ordering::Relaxed);
        }
        if path.v2_disabled_event {
            self.accept_v2_disabled_events
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record packed-v2 path counters for one completed commit batch.
    ///
    /// Purpose:
    /// - Expose packed-v2 attempt/disable behavior for commit lane diagnosis.
    ///
    /// Design:
    /// - Increments only counters corresponding to observed packed-v2 path flags.
    ///
    /// Inputs:
    /// - `path`: per-batch capability path flags.
    ///
    /// Outputs:
    /// - Updated commit lane counters consumed by snapshots.
    fn record_commit_path_stats(&self, path: BatchPathStats) {
        if path.v2_attempted {
            self.commit_v2_attempted.fetch_add(1, Ordering::Relaxed);
        }
        if path.v2_disabled_event {
            self.commit_v2_disabled_events
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record packed-v2 path counters for one completed recover batch.
    ///
    /// Purpose:
    /// - Expose packed-v2 attempt/disable behavior for recover lane diagnosis.
    ///
    /// Design:
    /// - Increments only counters corresponding to observed packed-v2 path flags.
    ///
    /// Inputs:
    /// - `path`: per-batch capability path flags.
    ///
    /// Outputs:
    /// - Updated recover lane counters consumed by snapshots.
    fn record_recover_path_stats(&self, path: BatchPathStats) {
        if path.v2_attempted {
            self.recover_v2_attempted.fetch_add(1, Ordering::Relaxed);
        }
        if path.v2_disabled_event {
            self.recover_v2_disabled_events
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl RpcStats {
    /// Record a batched KV GET request.
    fn record_kv_get_batch(&self, items: u64, wait_us_total: u64, wait_us_max: u64, rpc_us: u64) {
        self.kv_get_batches.fetch_add(1, Ordering::Relaxed);
        self.kv_get_items.fetch_add(items, Ordering::Relaxed);
        self.kv_get_wait_us
            .fetch_add(wait_us_total, Ordering::Relaxed);
        self.kv_get_rpc_us.fetch_add(rpc_us, Ordering::Relaxed);
        self.kv_get_max_batch.fetch_max(items, Ordering::Relaxed);
        self.kv_get_max_wait_us
            .fetch_max(wait_us_max, Ordering::Relaxed);
    }

    /// Record an error for KV GET batching.
    fn record_kv_get_error(&self) {
        self.kv_get_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a direct batch GET RPC call.
    fn record_kv_batch_get(&self, rpc_us: u64, error: bool) {
        self.kv_batch_get_calls.fetch_add(1, Ordering::Relaxed);
        self.kv_batch_get_rpc_us
            .fetch_add(rpc_us, Ordering::Relaxed);
        if error {
            self.kv_batch_get_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a batched pre-accept RPC call.
    fn record_pre_accept_batch(
        &self,
        items: u64,
        wait_us_total: u64,
        wait_us_max: u64,
        rpc_us: u64,
    ) {
        self.pre_accept_batches.fetch_add(1, Ordering::Relaxed);
        self.pre_accept_items.fetch_add(items, Ordering::Relaxed);
        self.pre_accept_wait_us
            .fetch_add(wait_us_total, Ordering::Relaxed);
        self.pre_accept_rpc_us.fetch_add(rpc_us, Ordering::Relaxed);
        self.pre_accept_max_batch
            .fetch_max(items, Ordering::Relaxed);
        self.pre_accept_max_wait_us
            .fetch_max(wait_us_max, Ordering::Relaxed);
    }

    /// Record a pre-accept RPC error.
    fn record_pre_accept_error(&self) {
        self.pre_accept_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a batched accept RPC call.
    fn record_accept_batch(&self, items: u64, wait_us_total: u64, wait_us_max: u64, rpc_us: u64) {
        self.accept_batches.fetch_add(1, Ordering::Relaxed);
        self.accept_items.fetch_add(items, Ordering::Relaxed);
        self.accept_wait_us
            .fetch_add(wait_us_total, Ordering::Relaxed);
        self.accept_rpc_us.fetch_add(rpc_us, Ordering::Relaxed);
        self.accept_max_batch.fetch_max(items, Ordering::Relaxed);
        self.accept_max_wait_us
            .fetch_max(wait_us_max, Ordering::Relaxed);
    }

    /// Record an accept RPC error.
    fn record_accept_error(&self) {
        self.accept_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a batched commit RPC call.
    fn record_commit_batch(&self, items: u64, wait_us_total: u64, wait_us_max: u64, rpc_us: u64) {
        self.commit_batches.fetch_add(1, Ordering::Relaxed);
        self.commit_items.fetch_add(items, Ordering::Relaxed);
        self.commit_wait_us
            .fetch_add(wait_us_total, Ordering::Relaxed);
        self.commit_rpc_us.fetch_add(rpc_us, Ordering::Relaxed);
        self.commit_max_batch.fetch_max(items, Ordering::Relaxed);
        self.commit_max_wait_us
            .fetch_max(wait_us_max, Ordering::Relaxed);
    }

    /// Record a commit RPC error.
    fn record_commit_error(&self) {
        self.commit_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a batched recover RPC call.
    fn record_recover_batch(&self, items: u64, wait_us_total: u64, wait_us_max: u64, rpc_us: u64) {
        self.recover_batches.fetch_add(1, Ordering::Relaxed);
        self.recover_items.fetch_add(items, Ordering::Relaxed);
        self.recover_wait_us
            .fetch_add(wait_us_total, Ordering::Relaxed);
        self.recover_rpc_us.fetch_add(rpc_us, Ordering::Relaxed);
        self.recover_max_batch.fetch_max(items, Ordering::Relaxed);
        self.recover_max_wait_us
            .fetch_max(wait_us_max, Ordering::Relaxed);
    }

    /// Record a recover RPC error.
    fn record_recover_error(&self) {
        self.recover_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot and reset aggregated stats.
    pub fn snapshot_and_reset(&self) -> RpcStatsSnapshot {
        RpcStatsSnapshot {
            kv_get_batches: self.kv_get_batches.swap(0, Ordering::Relaxed),
            kv_get_items: self.kv_get_items.swap(0, Ordering::Relaxed),
            kv_get_rpc_us: self.kv_get_rpc_us.swap(0, Ordering::Relaxed),
            kv_get_wait_us: self.kv_get_wait_us.swap(0, Ordering::Relaxed),
            kv_get_max_batch: self.kv_get_max_batch.swap(0, Ordering::Relaxed),
            kv_get_max_wait_us: self.kv_get_max_wait_us.swap(0, Ordering::Relaxed),
            kv_get_errors: self.kv_get_errors.swap(0, Ordering::Relaxed),

            kv_batch_get_calls: self.kv_batch_get_calls.swap(0, Ordering::Relaxed),
            kv_batch_get_rpc_us: self.kv_batch_get_rpc_us.swap(0, Ordering::Relaxed),
            kv_batch_get_errors: self.kv_batch_get_errors.swap(0, Ordering::Relaxed),

            pre_accept_batches: self.pre_accept_batches.swap(0, Ordering::Relaxed),
            pre_accept_items: self.pre_accept_items.swap(0, Ordering::Relaxed),
            pre_accept_rpc_us: self.pre_accept_rpc_us.swap(0, Ordering::Relaxed),
            pre_accept_wait_us: self.pre_accept_wait_us.swap(0, Ordering::Relaxed),
            pre_accept_max_batch: self.pre_accept_max_batch.swap(0, Ordering::Relaxed),
            pre_accept_max_wait_us: self.pre_accept_max_wait_us.swap(0, Ordering::Relaxed),
            pre_accept_errors: self.pre_accept_errors.swap(0, Ordering::Relaxed),

            accept_batches: self.accept_batches.swap(0, Ordering::Relaxed),
            accept_items: self.accept_items.swap(0, Ordering::Relaxed),
            accept_rpc_us: self.accept_rpc_us.swap(0, Ordering::Relaxed),
            accept_wait_us: self.accept_wait_us.swap(0, Ordering::Relaxed),
            accept_max_batch: self.accept_max_batch.swap(0, Ordering::Relaxed),
            accept_max_wait_us: self.accept_max_wait_us.swap(0, Ordering::Relaxed),
            accept_errors: self.accept_errors.swap(0, Ordering::Relaxed),

            commit_batches: self.commit_batches.swap(0, Ordering::Relaxed),
            commit_items: self.commit_items.swap(0, Ordering::Relaxed),
            commit_rpc_us: self.commit_rpc_us.swap(0, Ordering::Relaxed),
            commit_wait_us: self.commit_wait_us.swap(0, Ordering::Relaxed),
            commit_max_batch: self.commit_max_batch.swap(0, Ordering::Relaxed),
            commit_max_wait_us: self.commit_max_wait_us.swap(0, Ordering::Relaxed),
            commit_errors: self.commit_errors.swap(0, Ordering::Relaxed),

            recover_batches: self.recover_batches.swap(0, Ordering::Relaxed),
            recover_items: self.recover_items.swap(0, Ordering::Relaxed),
            recover_rpc_us: self.recover_rpc_us.swap(0, Ordering::Relaxed),
            recover_wait_us: self.recover_wait_us.swap(0, Ordering::Relaxed),
            recover_max_batch: self.recover_max_batch.swap(0, Ordering::Relaxed),
            recover_max_wait_us: self.recover_max_wait_us.swap(0, Ordering::Relaxed),
            recover_errors: self.recover_errors.swap(0, Ordering::Relaxed),
        }
    }
}

/// Convert a local txn id to its RPC representation.
fn to_rpc_txn_id(txn_id: TxnId) -> rpc::TxnId {
    rpc::TxnId {
        node_id: txn_id.node_id,
        counter: txn_id.counter,
    }
}

/// Convert a local version to its RPC representation.
fn to_rpc_version(version: Version) -> rpc::Version {
    rpc::Version {
        seq: version.seq,
        txn_id: Some(to_rpc_txn_id(version.txn_id)),
    }
}

/// Convert a local ballot to its RPC representation.
fn to_rpc_ballot(ballot: Ballot) -> rpc::Ballot {
    rpc::Ballot {
        counter: ballot.counter,
        node_id: ballot.node_id,
    }
}

/// Decode an RPC version into a local `Version`, supplying defaults if missing.
fn from_rpc_version(version: Option<rpc::Version>) -> Version {
    let version = version.unwrap_or(rpc::Version {
        seq: 0,
        txn_id: None,
    });
    let txn_id = version.txn_id.map(from_rpc_txn_id).unwrap_or(TxnId {
        node_id: 0,
        counter: 0,
    });
    Version {
        seq: version.seq,
        txn_id,
    }
}

/// Decode an RPC version, rejecting missing fields.
fn from_rpc_version_required(version: Option<rpc::Version>) -> anyhow::Result<Version> {
    let version = version.ok_or_else(|| anyhow::anyhow!("missing version"))?;
    let txn_id = version
        .txn_id
        .ok_or_else(|| anyhow::anyhow!("missing version.txn_id"))?;
    Ok(Version {
        seq: version.seq,
        txn_id: from_rpc_txn_id(txn_id),
    })
}

/// Convert an RPC txn id into a local representation.
fn from_rpc_txn_id(txn_id: rpc::TxnId) -> TxnId {
    TxnId {
        node_id: txn_id.node_id,
        counter: txn_id.counter,
    }
}

/// Decode an optional RPC ballot into a local ballot.
fn from_rpc_ballot(ballot: Option<rpc::Ballot>) -> Ballot {
    let ballot = ballot.unwrap_or(rpc::Ballot {
        counter: 0,
        node_id: 0,
    });
    Ballot {
        counter: ballot.counter,
        node_id: ballot.node_id,
    }
}

/// Convert a pre-accept request into its RPC representation.
fn to_rpc_pre_accept(req: PreAcceptRequest) -> rpc::PreAcceptRequest {
    let deps = req.deps.into_iter().map(to_rpc_txn_id).collect();
    rpc::PreAcceptRequest {
        group_id: req.group_id,
        txn_id: Some(to_rpc_txn_id(req.txn_id)),
        command: req.command,
        seq: req.seq,
        deps,
        ballot: Some(to_rpc_ballot(req.ballot)),
    }
}

/// Convert an RPC pre-accept response into a local response.
fn from_rpc_pre_accept(resp: rpc::PreAcceptResponse) -> PreAcceptResponse {
    let deps = resp
        .deps
        .into_iter()
        .map(|d| TxnId {
            node_id: d.node_id,
            counter: d.counter,
        })
        .collect();
    PreAcceptResponse {
        ok: resp.ok,
        promised: from_rpc_ballot(resp.promised),
        seq: resp.seq,
        deps,
    }
}

/// Convert an accept request into its RPC representation.
fn to_rpc_accept(req: AcceptRequest) -> rpc::AcceptRequest {
    let deps = req.deps.into_iter().map(to_rpc_txn_id).collect();
    rpc::AcceptRequest {
        group_id: req.group_id,
        txn_id: Some(to_rpc_txn_id(req.txn_id)),
        command: req.command,
        command_digest: req.command_digest.to_vec().into(),
        has_command: req.has_command,
        seq: req.seq,
        deps,
        ballot: Some(to_rpc_ballot(req.ballot)),
    }
}

/// Convert an RPC accept response into a local response.
fn from_rpc_accept(resp: rpc::AcceptResponse) -> AcceptResponse {
    AcceptResponse {
        ok: resp.ok,
        promised: from_rpc_ballot(resp.promised),
    }
}

/// Convert a commit request into its RPC representation.
fn to_rpc_commit(req: CommitRequest) -> rpc::CommitRequest {
    let deps = req.deps.into_iter().map(to_rpc_txn_id).collect();
    rpc::CommitRequest {
        group_id: req.group_id,
        txn_id: Some(to_rpc_txn_id(req.txn_id)),
        command: req.command,
        command_digest: req.command_digest.to_vec().into(),
        has_command: req.has_command,
        seq: req.seq,
        deps,
        ballot: Some(to_rpc_ballot(req.ballot)),
    }
}

/// Convert an RPC commit response into a local response.
fn from_rpc_commit(resp: rpc::CommitResponse) -> CommitResponse {
    CommitResponse { ok: resp.ok }
}

/// Convert a recover request into its RPC representation.
fn to_rpc_recover(req: RecoverRequest) -> rpc::RecoverRequest {
    rpc::RecoverRequest {
        group_id: req.group_id,
        txn_id: Some(to_rpc_txn_id(req.txn_id)),
        ballot: Some(to_rpc_ballot(req.ballot)),
    }
}

/// Convert an RPC recover response into a local response.
fn from_rpc_recover(resp: rpc::RecoverResponse) -> RecoverResponse {
    let has_command = resp.has_command || !resp.command.is_empty();
    let promised = from_rpc_ballot(resp.promised);
    let accepted_ballot = resp.accepted_ballot.map(|b| Ballot {
        counter: b.counter,
        node_id: b.node_id,
    });

    let deps = resp
        .deps
        .into_iter()
        .map(|d| TxnId {
            node_id: d.node_id,
            counter: d.counter,
        })
        .collect();

    RecoverResponse {
        ok: resp.ok,
        promised,
        status: match resp.status {
            // Map wire status values to the local enum.
            s if s == rpc::TxnStatus::TXN_STATUS_UNKNOWN => TxnStatus::Unknown,
            s if s == rpc::TxnStatus::TXN_STATUS_PREACCEPTED => TxnStatus::PreAccepted,
            s if s == rpc::TxnStatus::TXN_STATUS_ACCEPTED => TxnStatus::Accepted,
            s if s == rpc::TxnStatus::TXN_STATUS_COMMITTED => TxnStatus::Committed,
            s if s == rpc::TxnStatus::TXN_STATUS_EXECUTED => TxnStatus::Executed,
            _ => TxnStatus::Unknown,
        },
        accepted_ballot,
        command: resp.command,
        command_digest: if resp.command_digest.is_empty() {
            None
        } else if resp.command_digest.len() == 32 {
            let mut digest = [0u8; 32];
            digest.copy_from_slice(resp.command_digest.as_ref());
            Some(digest)
        } else {
            None
        },
        has_command,
        seq: resp.seq,
        deps,
    }
}

/// Convert a local executed prefix to its RPC representation.
fn to_rpc_executed_prefix(item: holo_accord::accord::ExecutedPrefix) -> rpc::ExecutedPrefix {
    rpc::ExecutedPrefix {
        node_id: item.node_id,
        counter: item.counter,
    }
}

/// Convert an RPC executed prefix into a local representation.
fn from_rpc_executed_prefix(item: rpc::ExecutedPrefix) -> ExecutedPrefix {
    ExecutedPrefix {
        node_id: item.node_id,
        counter: item.counter,
    }
}

/// Convert a report executed request into its RPC representation.
fn to_rpc_report_executed(req: ReportExecutedRequest) -> rpc::ReportExecutedRequest {
    rpc::ReportExecutedRequest {
        group_id: req.group_id,
        from_node_id: req.from_node_id,
        prefixes: req
            .prefixes
            .into_iter()
            .map(to_rpc_executed_prefix)
            .collect(),
    }
}

/// Convert an RPC report executed response into a local response.
fn from_rpc_report_executed(resp: rpc::ReportExecutedResponse) -> ReportExecutedResponse {
    ReportExecutedResponse { ok: resp.ok }
}

#[async_trait]
impl Transport for GrpcTransport {
    /// Send a pre-accept RPC via the peer's batching queue.
    async fn pre_accept(
        &self,
        target: NodeId,
        req: PreAcceptRequest,
    ) -> anyhow::Result<PreAcceptResponse> {
        let peer = self.peer(target)?;

        let (tx, rx) = oneshot::channel();
        let work = PreAcceptWork {
            req,
            tx,
            enqueued_at: std::time::Instant::now(),
        };
        match peer.pre_accept_tx.try_send(work) {
            Ok(()) => {
                peer.stats.pre_accept_sent.fetch_add(1, Ordering::Relaxed);
                peer.stats
                    .pre_accept_last_enqueue_us
                    .store(epoch_micros(), Ordering::Relaxed);
                peer.stats.pre_accept_queue.fetch_add(1, Ordering::Relaxed);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                // Backpressure: queue is full.
                peer.stats
                    .pre_accept_queue_full
                    .fetch_add(1, Ordering::Relaxed);
                return Err(anyhow::anyhow!("pre_accept queue full"));
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                // Transport is shutting down.
                return Err(anyhow::anyhow!("pre_accept queue closed"));
            }
        }
        rx.await.context("pre_accept response dropped")?
    }

    /// Send an accept RPC via the peer's batching queue.
    async fn accept(&self, target: NodeId, req: AcceptRequest) -> anyhow::Result<AcceptResponse> {
        let peer = self.peer(target)?;

        let (tx, rx) = oneshot::channel();
        let work = AcceptWork {
            req,
            tx,
            enqueued_at: std::time::Instant::now(),
        };
        match peer.accept_tx.try_send(work) {
            Ok(()) => {
                peer.stats.accept_sent.fetch_add(1, Ordering::Relaxed);
                peer.stats
                    .accept_last_enqueue_us
                    .store(epoch_micros(), Ordering::Relaxed);
                peer.stats.accept_queue.fetch_add(1, Ordering::Relaxed);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                // Backpressure: queue is full.
                peer.stats.accept_queue_full.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow::anyhow!("accept queue full"));
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                // Transport is shutting down.
                return Err(anyhow::anyhow!("accept queue closed"));
            }
        }
        rx.await.context("accept response dropped")?
    }

    /// Send a commit RPC via the peer's batching queue.
    async fn commit(&self, target: NodeId, req: CommitRequest) -> anyhow::Result<CommitResponse> {
        let peer = self.peer(target)?;

        let (tx, rx) = oneshot::channel();
        let work = CommitWork {
            req,
            tx,
            enqueued_at: std::time::Instant::now(),
        };
        match peer.commit_tx.try_send(work) {
            Ok(()) => {
                peer.stats.commit_sent.fetch_add(1, Ordering::Relaxed);
                peer.stats
                    .commit_last_enqueue_us
                    .store(epoch_micros(), Ordering::Relaxed);
                peer.stats.commit_queue.fetch_add(1, Ordering::Relaxed);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                // Backpressure: queue is full.
                peer.stats.commit_queue_full.fetch_add(1, Ordering::Relaxed);
                return Err(anyhow::anyhow!("commit queue full"));
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                // Transport is shutting down.
                return Err(anyhow::anyhow!("commit queue closed"));
            }
        }
        rx.await.context("commit response dropped")?
    }

    /// Send a recover RPC, coalescing concurrent requests for the same txn.
    async fn recover(
        &self,
        target: NodeId,
        req: RecoverRequest,
    ) -> anyhow::Result<RecoverResponse> {
        let peer = self.peer(target)?;

        let (tx, rx) = oneshot::channel();
        let txn_id = req.txn_id;
        let decision = peer
            .recover_coalescer
            .add_or_coalesce(txn_id, req.ballot, tx)
            .await;
        match decision {
            RecoverEnqueueDecision::Enqueue => {
                // First request for this txn id: enqueue to peer worker.
                let work = RecoverWork {
                    req,
                    enqueued_at: std::time::Instant::now(),
                };
                match peer.recover_tx.try_send(work) {
                    Ok(()) => {
                        peer.stats.recover_sent.fetch_add(1, Ordering::Relaxed);
                        let now_us = epoch_micros();
                        peer.stats
                            .recover_last_enqueue_us
                            .store(now_us, Ordering::Relaxed);
                        let new_queue =
                            peer.stats.recover_queue.fetch_add(1, Ordering::Relaxed) + 1;
                        peer.stats
                            .recover_queue_peak
                            .fetch_max(new_queue, Ordering::Relaxed);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        // Backpressure: queue is full, propagate error to waiters.
                        peer.stats
                            .recover_queue_full
                            .fetch_add(1, Ordering::Relaxed);
                        let err = anyhow::anyhow!("recover queue full");
                        peer.recover_coalescer.complete_err(txn_id, &err).await;
                        return Err(err);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        // Transport is shutting down, propagate error to waiters.
                        let err = anyhow::anyhow!("recover queue closed");
                        peer.recover_coalescer.complete_err(txn_id, &err).await;
                        return Err(err);
                    }
                }
            }
            RecoverEnqueueDecision::Coalesced {
                waiters,
                coalesced_count,
                ballot,
            } => {
                // Periodically log coalescing to avoid noisy logs on hot keys.
                if coalesced_count % 1000 == 0 {
                    tracing::info!(
                        peer = target,
                        txn_id = ?txn_id,
                        ballot = ?ballot,
                        waiters = waiters,
                        coalesced = coalesced_count,
                        "recover coalesced"
                    );
                }
            }
        }
        rx.await.context("recover response dropped")?
    }

    /// Forward to the peer's direct fetch_command RPC.
    async fn fetch_command(
        &self,
        target: NodeId,
        group_id: GroupId,
        txn_id: TxnId,
    ) -> anyhow::Result<Option<Bytes>> {
        GrpcTransport::fetch_command(self, target, group_id, txn_id).await
    }

    /// Send a report_executed RPC via the direct client.
    async fn report_executed(
        &self,
        target: NodeId,
        req: ReportExecutedRequest,
    ) -> anyhow::Result<ReportExecutedResponse> {
        let peer = self.peer(target)?;

        let result = time::timeout(
            self.rpc_timeout,
            peer.client.report_executed(to_rpc_report_executed(req)),
        )
        .await;

        match result {
            Ok(Ok(resp)) => Ok(from_rpc_report_executed(resp.into_inner())),
            Ok(Err(err)) => Err(anyhow::anyhow!("report_executed rpc failed: {err}")),
            Err(_) => Err(anyhow::anyhow!("report_executed rpc timed out")),
        }
    }

    /// Forward to the peer's direct last_executed_prefix RPC.
    async fn last_executed_prefix(
        &self,
        target: NodeId,
        group_id: GroupId,
    ) -> anyhow::Result<Vec<ExecutedPrefix>> {
        GrpcTransport::last_executed_prefix(self, target, group_id).await
    }

    /// Forward to the peer's direct executed RPC.
    async fn executed(
        &self,
        target: NodeId,
        group_id: GroupId,
        txn_id: TxnId,
    ) -> anyhow::Result<bool> {
        GrpcTransport::executed(self, target, group_id, txn_id).await
    }

    /// Forward to the peer's direct mark_visible RPC.
    async fn mark_visible(
        &self,
        target: NodeId,
        group_id: GroupId,
        txn_id: TxnId,
    ) -> anyhow::Result<bool> {
        GrpcTransport::mark_visible(self, target, group_id, txn_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    /// Verify reusable batching collects ready queue items up to the configured
    /// batch cap.
    ///
    /// Purpose:
    /// - Validate normal-path behavior for `collect_batch_reuse`.
    ///
    /// Design:
    /// - Seed two queued items and provide one `first` item.
    /// - Assert dequeue order and batch-size limit behavior.
    ///
    /// Inputs:
    /// - One `first` item and two queued channel items.
    ///
    /// Outputs:
    /// - Batch contains all three items in queue order.
    #[tokio::test]
    async fn collect_batch_reuse_collects_ready_items() {
        let (tx, mut rx) = mpsc::channel(8);
        tx.send(2u64).await.expect("send second item");
        tx.send(3u64).await.expect("send third item");

        let batch = collect_batch_reuse(
            Vec::with_capacity(4),
            1u64,
            &mut rx,
            3,
            Duration::from_millis(5),
        )
        .await;

        assert_eq!(batch, vec![1, 2, 3]);
    }

    /// Verify reusable batching keeps caller allocation when capacity is
    /// already sufficient.
    ///
    /// Purpose:
    /// - Validate edge-path reuse where no extra reserve is required.
    ///
    /// Design:
    /// - Pass a buffer with enough capacity and enforce immediate return.
    /// - Compare allocation pointer before/after.
    ///
    /// Inputs:
    /// - Pre-sized vector buffer and one `first` item.
    ///
    /// Outputs:
    /// - Returned batch reuses the same allocation.
    #[tokio::test]
    async fn collect_batch_reuse_reuses_caller_buffer() {
        let (_tx, mut rx) = mpsc::channel(8);
        let items = Vec::<u64>::with_capacity(8);
        let original_ptr = items.as_ptr();

        let batch = collect_batch_reuse(items, 1u64, &mut rx, 4, Duration::from_millis(0)).await;

        assert_eq!(batch, vec![1]);
        assert_eq!(
            batch.as_ptr(),
            original_ptr,
            "batch builder should retain caller allocation when capacity fits"
        );
    }

    /// Verify reusable batching exits cleanly when the input channel is
    /// disconnected.
    ///
    /// Purpose:
    /// - Validate failure-path behavior for queue shutdown.
    ///
    /// Design:
    /// - Drop sender before collection and assert no hang / no extra items.
    ///
    /// Inputs:
    /// - Disconnected receiver and one `first` item.
    ///
    /// Outputs:
    /// - Returned batch contains only the first item.
    #[tokio::test]
    async fn collect_batch_reuse_handles_disconnected_channel() {
        let (tx, mut rx) = mpsc::channel::<u64>(1);
        drop(tx);

        let batch = collect_batch_reuse(
            Vec::with_capacity(2),
            42u64,
            &mut rx,
            8,
            Duration::from_millis(50),
        )
        .await;

        assert_eq!(batch, vec![42]);
    }

    /// Verify progress-aware batching collects immediately available items up to
    /// the configured batch size.
    ///
    /// Purpose:
    /// - Validate normal-path batching behavior for `collect_batch_with_progress`.
    ///
    /// Design:
    /// - Seed two queued items, then provide one `first` item and check exact
    ///   batch shape.
    ///
    /// Inputs:
    /// - One `first` item and two queued channel items.
    ///
    /// Outputs:
    /// - Batch contains exactly `batch_max` items in dequeue order.
    #[tokio::test]
    async fn collect_batch_with_progress_collects_ready_items() {
        let (tx, mut rx) = mpsc::channel(8);
        tx.send(2u64).await.expect("send second item");
        tx.send(3u64).await.expect("send third item");
        let mut in_flight: FuturesUnordered<futures_util::future::Ready<()>> =
            FuturesUnordered::new();
        let items = Vec::new();

        let batch = collect_batch_with_progress(
            items,
            1,
            &mut rx,
            3,
            Duration::from_millis(5),
            &mut in_flight,
            |_ready| {},
        )
        .await;

        assert_eq!(batch, vec![1, 2, 3]);
    }

    /// Verify progress-aware batching keeps in-flight futures moving while
    /// waiting for additional batch items.
    ///
    /// Purpose:
    /// - Validate the low-tail-latency behavior that avoids stalling active RPCs
    ///   during batching windows.
    ///
    /// Design:
    /// - Start one short in-flight future and keep the queue empty, forcing the
    ///   batching function to wait on timeout.
    /// - Assert that in-flight completion happened before batching returns.
    ///
    /// Inputs:
    /// - Empty queue after first item plus one in-flight completion future.
    ///
    /// Outputs:
    /// - Returned batch has one item and the in-flight future completed.
    #[tokio::test]
    async fn collect_batch_with_progress_polls_in_flight_while_waiting() {
        let (_tx, mut rx) = mpsc::channel(8);
        let progressed = Arc::new(AtomicBool::new(false));
        let mut in_flight = FuturesUnordered::new();
        let progressed_flag = progressed.clone();
        let completions = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let completions_seen = completions.clone();
        let items = Vec::new();
        in_flight.push(async move {
            time::sleep(Duration::from_millis(10)).await;
            progressed_flag.store(true, AtomicOrdering::Relaxed);
        });

        let batch = collect_batch_with_progress(
            items,
            1u64,
            &mut rx,
            4,
            Duration::from_millis(40),
            &mut in_flight,
            move |_ready| {
                completions_seen.fetch_add(1, AtomicOrdering::Relaxed);
            },
        )
        .await;

        assert_eq!(batch, vec![1]);
        assert!(
            progressed.load(AtomicOrdering::Relaxed),
            "in-flight future should complete while batcher waits"
        );
        assert_eq!(
            completions.load(AtomicOrdering::Relaxed),
            1,
            "batch collection should observe one in-flight completion callback"
        );
    }

    /// Verify reusable vector pool returns previously cached allocation with
    /// preserved capacity.
    ///
    /// Purpose:
    /// - Validate normal-path reuse behavior for `ReuseVecPool`.
    ///
    /// Design:
    /// - Cache one vector, then request a smaller capacity and assert reuse.
    ///
    /// Inputs:
    /// - One cached vector and one take request.
    ///
    /// Outputs:
    /// - Returned vector is empty and keeps the prior allocation capacity.
    #[test]
    fn reuse_vec_pool_reuses_cached_capacity() {
        let mut pool = ReuseVecPool::<u64>::new(2, 64);
        let mut buf = Vec::with_capacity(16);
        let original_ptr = buf.as_ptr();
        buf.extend_from_slice(&[1, 2, 3]);
        pool.put(buf);

        let out = pool.take(8);
        assert_eq!(out.len(), 0);
        assert_eq!(
            out.as_ptr(),
            original_ptr,
            "pool should hand back the same allocation when capacity fits"
        );
        assert!(
            out.capacity() >= 16,
            "pooled vector should keep prior capacity for reuse"
        );
    }

    /// Verify reusable vector pool allocates when empty.
    ///
    /// Purpose:
    /// - Validate edge-path behavior when no vector has been cached yet.
    ///
    /// Design:
    /// - Request capacity from an empty pool and verify minimum allocation.
    ///
    /// Inputs:
    /// - Empty pool and one take request.
    ///
    /// Outputs:
    /// - Fresh vector with requested minimum capacity.
    #[test]
    fn reuse_vec_pool_allocates_when_empty() {
        let mut pool = ReuseVecPool::<u64>::new(2, 64);
        let out = pool.take(11);
        assert_eq!(out.len(), 0);
        assert!(
            out.capacity() >= 11,
            "empty pool should allocate requested minimum capacity"
        );
    }

    /// Verify reusable vector pool rejects oversized vectors.
    ///
    /// Purpose:
    /// - Validate failure-path protection against retaining pathological memory spikes.
    ///
    /// Design:
    /// - Return one oversized vector to the pool and then request a small one.
    /// - Assert the pool did not hand back the oversized capacity.
    ///
    /// Inputs:
    /// - One oversized vector where `capacity > max_capacity`.
    ///
    /// Outputs:
    /// - Next `take` allocates a right-sized buffer instead of reusing oversized one.
    #[test]
    fn reuse_vec_pool_drops_oversized_vector() {
        let mut pool = ReuseVecPool::<u64>::new(2, 32);
        let oversized = Vec::with_capacity(256);
        pool.put(oversized);

        let out = pool.take(8);
        assert!(
            out.capacity() < 256,
            "oversized vectors should not be retained in reuse pool"
        );
    }

    /// Verify `acquire_with_progress` blocks on saturation until one in-flight
    /// future completes and releases a permit.
    ///
    /// Purpose:
    /// - Validate saturation behavior for the non-spawn in-flight worker model.
    ///
    /// Design:
    /// - Limit concurrency to 1.
    /// - Hold the only permit inside one in-flight future for a fixed delay.
    /// - Assert that `acquire_with_progress` does not return before delay.
    ///
    /// Inputs:
    /// - Saturated limiter and one delayed in-flight completion future.
    ///
    /// Outputs:
    /// - One new permit returned only after prior permit is released.
    #[tokio::test]
    async fn acquire_with_progress_waits_for_in_flight_completion() {
        let limiter = Arc::new(InflightLimiter::new(1, 1, 1));
        let held = limiter.try_acquire().expect("initial permit");
        let mut in_flight = FuturesUnordered::new();
        in_flight.push(async move {
            let _hold = held;
            time::sleep(Duration::from_millis(20)).await;
        });

        let start = std::time::Instant::now();
        let permit = acquire_with_progress(&limiter, &mut in_flight).await;
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(10),
            "acquire should wait for saturated permit release (elapsed={elapsed:?})"
        );
        drop(permit);
    }

    /// Verify `acquire_with_progress` correctly falls back to blocking acquire
    /// when saturation exists but no futures are currently tracked.
    ///
    /// Purpose:
    /// - Validate failure/edge behavior in the defensive fallback path.
    ///
    /// Design:
    /// - Saturate limiter outside the in-flight set, then release permit later.
    /// - Keep `in_flight` empty to force the fallback branch.
    ///
    /// Inputs:
    /// - Saturated limiter with delayed external permit release.
    ///
    /// Outputs:
    /// - Permit returned after delayed release.
    #[tokio::test]
    async fn acquire_with_progress_falls_back_when_in_flight_is_empty() {
        let limiter = Arc::new(InflightLimiter::new(1, 1, 1));
        let held = limiter.try_acquire().expect("initial permit");
        let mut in_flight: FuturesUnordered<futures_util::future::Ready<()>> =
            FuturesUnordered::new();

        tokio::spawn(async move {
            let _hold = held;
            time::sleep(Duration::from_millis(15)).await;
        });

        let start = std::time::Instant::now();
        let permit = acquire_with_progress(&limiter, &mut in_flight).await;
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(10),
            "fallback acquire should wait for externally held permit (elapsed={elapsed:?})"
        );
        drop(permit);
    }

    /// Verify GET batch wait is clamped to zero when the head request already
    /// exceeded the strict queue-age budget.
    ///
    /// Purpose:
    /// - Validate stall-prevention behavior for stale queued reads.
    ///
    /// Design:
    /// - Build a synthetic enqueue/dequeue timeline where head age exceeds the
    ///   max queue wait and assert immediate dispatch.
    ///
    /// Inputs:
    /// - One stale head enqueue timestamp and non-zero configured batch wait.
    ///
    /// Outputs:
    /// - Effective wait equals zero.
    #[test]
    fn kv_get_effective_batch_wait_flushes_stale_head_immediately() {
        let now = std::time::Instant::now();
        let head_enqueued = now - Duration::from_millis(5);
        let batch_wait = Duration::from_millis(2);
        let max_queue_wait = Duration::from_millis(3);

        let effective =
            kv_get_effective_batch_wait(head_enqueued, now, batch_wait, max_queue_wait, 8);
        assert_eq!(
            effective,
            Duration::ZERO,
            "stale head item should bypass additional batching wait"
        );
    }

    /// Verify GET batch wait is clamped to remaining queue-age budget when the
    /// head item is partially aged but not yet stale.
    ///
    /// Purpose:
    /// - Validate normal-path queue-age clamping behavior.
    ///
    /// Design:
    /// - Construct a timeline where head age consumes part of the max budget.
    /// - Assert returned wait equals remaining budget (not full configured wait).
    ///
    /// Inputs:
    /// - Head age below cap, with configured wait larger than remaining budget.
    ///
    /// Outputs:
    /// - Effective wait equals remaining queue-age budget.
    #[test]
    fn kv_get_effective_batch_wait_clamps_to_remaining_budget() {
        let now = std::time::Instant::now();
        let head_enqueued = now - Duration::from_millis(3);
        let batch_wait = Duration::from_millis(4);
        let max_queue_wait = Duration::from_millis(5);

        let effective =
            kv_get_effective_batch_wait(head_enqueued, now, batch_wait, max_queue_wait, 8);
        assert_eq!(effective, Duration::from_millis(2));
    }

    /// Verify low-backlog GET batches cap coalescing wait to protect p50.
    ///
    /// Purpose:
    /// - Ensure read batching stays latency-friendly when queue depth is one.
    ///
    /// Design:
    /// - Use non-zero configured wait and queue-depth hint of one.
    /// - Assert wait is clamped to the low-backlog cap.
    ///
    /// Inputs:
    /// - Fresh head item with healthy queue-age budget.
    ///
    /// Outputs:
    /// - Effective wait is capped at `KV_GET_LOW_BACKLOG_WAIT_US`.
    #[test]
    fn kv_get_effective_batch_wait_caps_low_backlog_delay() {
        let now = std::time::Instant::now();
        let head_enqueued = now;
        let batch_wait = Duration::from_millis(1);
        let max_queue_wait = Duration::from_millis(8);

        let effective =
            kv_get_effective_batch_wait(head_enqueued, now, batch_wait, max_queue_wait, 1);
        assert_eq!(
            effective,
            Duration::from_micros(KV_GET_LOW_BACKLOG_WAIT_US),
            "single-item backlog should not wait a full batching window"
        );
    }

    /// Verify immediate-dispatch gate enables low-backlog GET fast path when
    /// limiter capacity is available.
    ///
    /// Purpose:
    /// - Validate p50-focused fast path behavior under uncongested load.
    ///
    /// Design:
    /// - Uses queue depth at low-backlog threshold with available capacity.
    ///
    /// Inputs:
    /// - `queued_items_hint=1`, `limiter_has_capacity=true`.
    ///
    /// Outputs:
    /// - `true` immediate-dispatch decision.
    #[test]
    fn kv_get_should_dispatch_immediately_low_backlog_with_capacity() {
        assert!(kv_get_should_dispatch_immediately(1, true));
    }

    /// Verify immediate-dispatch gate remains disabled when limiter capacity is
    /// unavailable.
    ///
    /// Purpose:
    /// - Preserve backpressure behavior under saturation.
    ///
    /// Design:
    /// - Uses low queue depth but no limiter capacity.
    ///
    /// Inputs:
    /// - `queued_items_hint=1`, `limiter_has_capacity=false`.
    ///
    /// Outputs:
    /// - `false` immediate-dispatch decision.
    #[test]
    fn kv_get_should_not_dispatch_immediately_without_capacity() {
        assert!(!kv_get_should_dispatch_immediately(1, false));
    }

    /// Verify immediate-dispatch gate remains disabled when backlog exceeds the
    /// low-latency threshold.
    ///
    /// Purpose:
    /// - Keep batching active when queue depth indicates useful coalescing.
    ///
    /// Design:
    /// - Uses queue depth above threshold with capacity available.
    ///
    /// Inputs:
    /// - `queued_items_hint=2`, `limiter_has_capacity=true`.
    ///
    /// Outputs:
    /// - `false` immediate-dispatch decision.
    #[test]
    fn kv_get_should_not_dispatch_immediately_for_backlog() {
        assert!(!kv_get_should_dispatch_immediately(2, true));
    }

    /// Verify unknown lanes optimistically attempt packed-v2.
    ///
    /// Purpose:
    /// - Ensure startup traffic on upgraded peers stays on the v2 fast path.
    ///
    /// Design:
    /// - Leaves lane state as default.
    /// - Returns one successful packed-v2 response.
    ///
    /// Inputs:
    /// - One `KvGetRequest` and default capability state.
    ///
    /// Outputs:
    /// - v2 response is returned and lane is promoted to enabled.
    #[tokio::test]
    async fn call_batch_v2_unknown_lane_attempts_v2() {
        let lane = BatchLaneCapability::default();
        let requests = vec![rpc::KvGetRequest {
            key: Bytes::from_static(b"k0"),
        }];
        let mut path_stats = BatchPathStats::default();
        let (responses, _recyclable_requests): (Vec<rpc::KvGetResponse>, Vec<rpc::KvGetRequest>) =
            call_batch_v2_only(
                Duration::from_millis(100),
                &lane,
                requests,
                "kv_batch_get",
                move |packed| async move {
                    let decoded = decode_items_from_parts::<rpc::KvGetRequest>(
                        packed.frame,
                        &packed.end_offsets,
                    )
                    .expect("decode packed v2 requests");
                    let responses = decoded
                        .into_iter()
                        .map(|req| rpc::KvGetResponse {
                            has_value: true,
                            value: req.key,
                            version: None,
                        })
                        .collect::<Vec<_>>();
                    let (frame, end_offsets) =
                        encode_items_to_parts(&responses).expect("encode packed v2 responses");
                    Ok(volo_grpc::Response::new(rpc::PackedBatchResponse {
                        frame,
                        end_offsets,
                    }))
                },
                &mut path_stats,
            )
            .await
            .expect("unknown lane should succeed via v2");

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].value.as_ref(), b"k0");
        assert_eq!(lane.state.load(AtomicOrdering::Relaxed), V2_CAP_ENABLED);
        assert!(path_stats.v2_attempted);
        assert!(!path_stats.v2_disabled_event);
    }

    /// Verify packed-v2 batch helper returns v2 responses and preserves request
    /// vector capacity on the fast path.
    ///
    /// Purpose:
    /// - Validate normal-path behavior for `call_batch_v2_only`.
    ///
    /// Design:
    /// - Encode two request items into a packed v2 call.
    /// - Decode them inside the v2 closure and return packed responses.
    ///
    /// Inputs:
    /// - Two `KvGetRequest` items and v2-enabled capability flag.
    ///
    /// Outputs:
    /// - Two decoded v2 responses and recyclable request vector with original
    ///   item count retained.
    #[tokio::test]
    async fn call_batch_v2_uses_fast_path_and_reuses_requests() {
        let lane = BatchLaneCapability::default();
        lane.state.store(V2_CAP_ENABLED, AtomicOrdering::Relaxed);
        let requests = vec![
            rpc::KvGetRequest {
                key: Bytes::from_static(b"k0"),
            },
            rpc::KvGetRequest {
                key: Bytes::from_static(b"k1"),
            },
        ];

        let mut path_stats = BatchPathStats::default();
        let (responses, recyclable_requests): (Vec<rpc::KvGetResponse>, Vec<rpc::KvGetRequest>) =
            call_batch_v2_only(
                Duration::from_millis(100),
                &lane,
                requests,
                "kv_batch_get",
                move |packed| async move {
                    let decoded = decode_items_from_parts::<rpc::KvGetRequest>(
                        packed.frame,
                        &packed.end_offsets,
                    )
                    .expect("decode packed v2 requests");
                    let responses = decoded
                        .into_iter()
                        .map(|req| rpc::KvGetResponse {
                            has_value: true,
                            value: req.key,
                            version: Some(rpc::Version {
                                seq: 1,
                                txn_id: Some(rpc::TxnId {
                                    node_id: 7,
                                    counter: 9,
                                }),
                            }),
                        })
                        .collect::<Vec<_>>();
                    let (frame, end_offsets) =
                        encode_items_to_parts(&responses).expect("encode packed v2 responses");
                    Ok(volo_grpc::Response::new(rpc::PackedBatchResponse {
                        frame,
                        end_offsets,
                    }))
                },
                &mut path_stats,
            )
            .await
            .expect("v2 call should succeed");

        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].value.as_ref(), b"k0");
        assert_eq!(responses[1].value.as_ref(), b"k1");
        assert_eq!(recyclable_requests.len(), 2);
        assert_eq!(lane.state.load(AtomicOrdering::Relaxed), V2_CAP_ENABLED);
        assert!(path_stats.v2_attempted);
        assert!(!path_stats.v2_disabled_event);
    }

    /// Verify packed-v2 helper disables lane and errors on `Unimplemented`.
    ///
    /// Purpose:
    /// - Validate v2-only failure behavior for incompatible peers.
    ///
    /// Design:
    /// - Return `Unimplemented` from v2 closure.
    /// - Assert helper returns error and lane transitions to disabled.
    ///
    /// Inputs:
    /// - Two `KvGetRequest` items and v2-enabled capability flag.
    ///
    /// Outputs:
    /// - Error returned and disabled-lane marker recorded.
    #[tokio::test]
    async fn call_batch_v2_disables_lane_on_unimplemented_and_errors() {
        let lane = BatchLaneCapability::default();
        lane.state.store(V2_CAP_ENABLED, AtomicOrdering::Relaxed);
        let requests = vec![
            rpc::KvGetRequest {
                key: Bytes::from_static(b"k0"),
            },
            rpc::KvGetRequest {
                key: Bytes::from_static(b"k1"),
            },
        ];

        let mut path_stats = BatchPathStats::default();
        let err = call_batch_v2_only::<rpc::KvGetRequest, rpc::KvGetResponse, _, _>(
            Duration::from_millis(100),
            &lane,
            requests,
            "kv_batch_get",
            move |_packed| async move { Err(volo_grpc::Status::unimplemented("no v2")) },
            &mut path_stats,
        )
        .await
        .expect_err("v2-only mode should fail on unimplemented");

        assert!(
            err.to_string().contains("packed-v2 unimplemented"),
            "error should report unimplemented incompatibility"
        );
        assert_eq!(lane.state.load(AtomicOrdering::Relaxed), V2_CAP_DISABLED);
        assert!(path_stats.v2_attempted);
        assert!(path_stats.v2_disabled_event);
    }

    /// Verify packed-v2 helper returns an error when response payload decode
    /// fails.
    ///
    /// Purpose:
    /// - Validate failure-path handling for malformed v2 response envelopes.
    ///
    /// Design:
    /// - Return a packed response with one trailing unindexed byte so decode
    ///   fails with frame-length mismatch.
    ///
    /// Inputs:
    /// - One `KvGetRequest` item and malformed packed v2 response.
    ///
    /// Outputs:
    /// - Error from packed decode and unchanged v2 capability flag.
    #[tokio::test]
    async fn call_batch_v2_returns_error_on_malformed_response() {
        let lane = BatchLaneCapability::default();
        lane.state.store(V2_CAP_ENABLED, AtomicOrdering::Relaxed);
        let requests = vec![rpc::KvGetRequest {
            key: Bytes::from_static(b"k0"),
        }];

        let mut path_stats = BatchPathStats::default();
        let err = call_batch_v2_only::<rpc::KvGetRequest, rpc::KvGetResponse, _, _>(
            Duration::from_millis(100),
            &lane,
            requests,
            "kv_batch_get",
            move |_packed| async move {
                let responses = vec![rpc::KvGetResponse {
                    has_value: true,
                    value: Bytes::from_static(b"v0"),
                    version: Some(rpc::Version {
                        seq: 1,
                        txn_id: Some(rpc::TxnId {
                            node_id: 1,
                            counter: 1,
                        }),
                    }),
                }];
                let (frame, end_offsets) =
                    encode_items_to_parts(&responses).expect("encode packed response");
                let mut malformed = frame.to_vec();
                malformed.push(0);
                Ok(volo_grpc::Response::new(rpc::PackedBatchResponse {
                    frame: Bytes::from(malformed),
                    end_offsets,
                }))
            },
            &mut path_stats,
        )
        .await
        .expect_err("malformed packed response should fail");

        assert!(
            err.to_string().contains("packed response decode failed"),
            "error should report packed decode failure; got: {err}"
        );
        assert_eq!(
            lane.state.load(AtomicOrdering::Relaxed),
            V2_CAP_ENABLED,
            "non-unimplemented errors should not disable v2 capability"
        );
        assert!(path_stats.v2_attempted);
        assert!(!path_stats.v2_disabled_event);
    }

    /// Verify direct KV batch-get helper uses packed-v2 on unknown lanes.
    ///
    /// Purpose:
    /// - Validate normal-path behavior for read-fanout v2 encoding.
    ///
    /// Design:
    /// - Starts with default capability state.
    /// - Decodes packed request keys inside the v2 closure.
    /// - Returns packed responses.
    ///
    /// Inputs:
    /// - Two raw key payloads.
    ///
    /// Outputs:
    /// - Two decoded responses with matching values and enabled lane state.
    #[tokio::test]
    async fn call_kv_batch_get_v2_only_uses_v2_on_unknown_lane() {
        let lane = BatchLaneCapability::default();
        let keys = vec![Bytes::from_static(b"k0"), Bytes::from_static(b"k1")];
        let mut path_stats = BatchPathStats::default();
        let responses = call_kv_batch_get_v2_only(
            Duration::from_millis(100),
            &lane,
            &keys,
            "kv_batch_get",
            move |packed| async move {
                let decoded =
                    decode_items_from_parts::<rpc::KvGetRequest>(packed.frame, &packed.end_offsets)
                        .expect("decode packed kv_get requests");
                assert_eq!(decoded.len(), 2);
                assert_eq!(decoded[0].key.as_ref(), b"k0");
                assert_eq!(decoded[1].key.as_ref(), b"k1");
                let responses = vec![
                    rpc::KvGetResponse {
                        has_value: true,
                        value: Bytes::from_static(b"v0"),
                        version: None,
                    },
                    rpc::KvGetResponse {
                        has_value: true,
                        value: Bytes::from_static(b"v1"),
                        version: None,
                    },
                ];
                let (frame, end_offsets) =
                    encode_items_to_parts(&responses).expect("encode packed kv_get responses");
                Ok(volo_grpc::Response::new(rpc::PackedBatchResponse {
                    frame,
                    end_offsets,
                }))
            },
            &mut path_stats,
        )
        .await
        .expect("v2 path should succeed");

        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].value.as_ref(), b"v0");
        assert_eq!(responses[1].value.as_ref(), b"v1");
        assert_eq!(lane.state.load(AtomicOrdering::Relaxed), V2_CAP_ENABLED);
        assert!(path_stats.v2_attempted);
        assert!(!path_stats.v2_disabled_event);
    }

    /// Verify direct KV batch-get helper errors on `Unimplemented`.
    ///
    /// Purpose:
    /// - Validate v2-only behavior for incompatible peers.
    ///
    /// Design:
    /// - Returns `Unimplemented` from v2 closure.
    /// - Asserts helper reports error and disables the lane.
    ///
    /// Inputs:
    /// - Two raw key payloads and v2-enabled capability state.
    ///
    /// Outputs:
    /// - Error is returned and lane state becomes disabled.
    #[tokio::test]
    async fn call_kv_batch_get_v2_only_unimplemented_errors() {
        let lane = BatchLaneCapability::default();
        lane.state.store(V2_CAP_ENABLED, AtomicOrdering::Relaxed);
        let keys = vec![Bytes::from_static(b"k0"), Bytes::from_static(b"k1")];
        let mut path_stats = BatchPathStats::default();
        let err = call_kv_batch_get_v2_only(
            Duration::from_millis(100),
            &lane,
            &keys,
            "kv_batch_get",
            move |_packed| async move { Err(volo_grpc::Status::unimplemented("no v2")) },
            &mut path_stats,
        )
        .await
        .expect_err("v2-only mode should fail on unimplemented");

        assert!(
            err.to_string().contains("packed-v2 unimplemented"),
            "error should report unimplemented incompatibility"
        );
        assert_eq!(lane.state.load(AtomicOrdering::Relaxed), V2_CAP_DISABLED);
        assert!(path_stats.v2_attempted);
        assert!(path_stats.v2_disabled_event);
    }

    /// Verify per-lane packed-v2 counters are recorded and reset.
    ///
    /// Purpose:
    /// - Validate uniform visibility across GET and consensus lanes.
    ///
    /// Design:
    /// - Applies one synthetic path update per lane.
    /// - Snapshots once to verify increments, then again to verify reset.
    ///
    /// Inputs:
    /// - Synthetic `BatchPathStats` values for all lanes.
    ///
    /// Outputs:
    /// - First snapshot includes expected counters; second snapshot zeros them.
    #[test]
    fn peer_stats_snapshot_includes_all_lane_path_counters() {
        let stats = PeerStats::default();
        stats.record_kv_get_path_stats(BatchPathStats {
            v2_attempted: true,
            v2_disabled_event: true,
        });
        stats.record_pre_accept_path_stats(BatchPathStats {
            v2_attempted: true,
            v2_disabled_event: true,
        });
        stats.record_accept_path_stats(BatchPathStats {
            v2_attempted: false,
            v2_disabled_event: false,
        });
        // Edge path: no flags set should not increment commit counters.
        stats.record_commit_path_stats(BatchPathStats::default());
        stats.record_recover_path_stats(BatchPathStats {
            v2_attempted: true,
            // Failure-path shape: lane disabled after unimplemented response.
            v2_disabled_event: true,
        });

        let snap = stats.snapshot_and_reset(epoch_micros());
        assert_eq!(snap.kv_get_v2_attempted, 1);
        assert_eq!(snap.kv_get_v2_disabled_events, 1);
        assert_eq!(snap.pre_accept_v2_attempted, 1);
        assert_eq!(snap.pre_accept_v2_disabled_events, 1);
        assert_eq!(snap.accept_v2_attempted, 0);
        assert_eq!(snap.accept_v2_disabled_events, 0);
        assert_eq!(snap.commit_v2_attempted, 0);
        assert_eq!(snap.commit_v2_disabled_events, 0);
        assert_eq!(snap.recover_v2_attempted, 1);
        assert_eq!(snap.recover_v2_disabled_events, 1);

        let reset = stats.snapshot_and_reset(epoch_micros());
        assert_eq!(reset.kv_get_v2_attempted, 0);
        assert_eq!(reset.pre_accept_v2_attempted, 0);
        assert_eq!(reset.accept_v2_disabled_events, 0);
        assert_eq!(reset.commit_v2_disabled_events, 0);
        assert_eq!(reset.recover_v2_attempted, 0);
    }
}
