//! Key/value storage primitives and command encoding for the HoloStore node.
//!
//! Purpose:
//! - Provide the storage abstraction and command codec used by the Accord KV
//!   state machine.
//!
//! Design:
//! - Expose a `KvEngine` trait with in-memory, Fjall-backed, and sharded/routed
//!   implementations.
//! - Keep hot command apply/read paths zero-copy where possible by using
//!   borrowed key/value slices.
//! - Publish committed Fjall writes either directly or through a shared
//!   non-sleeping publish worker that batches already queued shard publishes
//!   into one keyspace commit.
//!
//! Inputs:
//! - Encoded KV commands and membership commands from Accord apply/read hooks.
//! - Key/value byte slices passed to engine methods.
//!
//! Outputs:
//! - Versioned KV reads/writes, visibility updates, and decoded command
//!   key-dependency sets.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, RwLock};
use std::thread;

use ahash::RandomState;
use anyhow::Context;
use bytes::Bytes;
use fjall::{Keyspace, PartitionCreateOptions};
use holo_accord::accord::{CommandKeys, ExecMeta, NodeId, StateMachine, TxnId};
use tracing::warn;

/// Default range generation for data written before range-generation metadata.
pub const DEFAULT_RANGE_GENERATION: u64 = 1;
/// Marker byte for version-key rows that include range generation.
const VERSION_KEY_V2_MARKER: u8 = 0xFF;
/// Magic prefix for latest-index rows that include range generation.
const LATEST_VALUE_V2_MAGIC: &[u8; 4] = b"\xFFHV2";
/// Marker byte for in-memory version-list rows that include range generation.
const VERSION_LIST_V2_MARKER: u8 = 0xFF;

/// Storage engine API used by the Accord state machine.
///
/// Implementations are responsible for storing per-version values, exposing
/// both historical reads (at a given version) and the latest visible value.
pub trait KvEngine: Send + Sync + 'static {
    /// Read the newest visible value for `key` that is <= `version`.
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>>;
    /// Read the newest visible value for `key` that is <= `version`, returning
    /// the stored version as well as the value.
    fn get_versioned(&self, key: &[u8], version: Version) -> Option<(Vec<u8>, Version)>;
    /// Read the latest visible value for `key` along with its version.
    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)>;
    /// Batch variant of `get_latest` that preserves input ordering.
    fn get_latest_batch(&self, keys: &[&[u8]]) -> Vec<Option<(Vec<u8>, Version)>>;
    /// Persist a value for `key` at `version` (initially invisible).
    fn set(&self, key: &[u8], value: &[u8], version: Version) -> anyhow::Result<()>;
    /// Persist multiple values in one batch (initially invisible).
    fn set_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<()> {
        for (key, value, version) in items {
            self.set(key, value, *version)?;
        }
        Ok(())
    }
    /// Persist committed values and publish their visible/latest state atomically.
    ///
    /// Returns the number of keys whose latest index transitioned from missing
    /// to present.
    fn apply_committed_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<u64> {
        self.set_batch(items)?;
        let mut inserted_latest = 0u64;
        for (key, _, version) in items {
            if self.mark_visible(key, *version)? {
                inserted_latest = inserted_latest.saturating_add(1);
            }
        }
        Ok(inserted_latest)
    }
    /// Mark a previously written `(key, version)` as visible to readers.
    ///
    /// Returns `true` when this update created a new latest index entry for
    /// the key (i.e. the key did not previously have a visible latest value).
    fn mark_visible(&self, key: &[u8], version: Version) -> anyhow::Result<bool>;
    /// Batch variant of `mark_visible`.
    ///
    /// Purpose:
    /// - Mark many keys visible in one call while preserving key-order
    ///   semantics for deterministic behavior.
    ///
    /// Design:
    /// - Accepts borrowed key slices to avoid forcing caller-side key cloning.
    /// - Default implementation delegates to per-key `mark_visible`.
    ///
    /// Inputs:
    /// - `keys`: borrowed key slices to mark visible at `version`.
    /// - `version`: committed version to mark visible.
    ///
    /// Outputs:
    /// - Count of keys whose latest index transitioned from missing to present.
    fn mark_visible_batch(&self, keys: &[&[u8]], version: Version) -> anyhow::Result<u64> {
        let mut inserted_latest = 0u64;
        for key in keys {
            if self.mark_visible(key, version)? {
                inserted_latest = inserted_latest.saturating_add(1);
            }
        }
        Ok(inserted_latest)
    }
}

/// Routing policy for selecting a shard given a key.
pub trait ShardRouter: Send + Sync + 'static {
    fn shard_for_key(&self, key: &[u8]) -> usize;

    fn may_have_fallback_shards(&self) -> bool {
        true
    }

    fn fallback_shard_for_key(&self, _key: &[u8]) -> Option<usize> {
        None
    }
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
        self.get_versioned(key, version).map(|(value, _)| value)
    }

    /// Return the latest visible version <= `version` if present.
    fn get_versioned(&self, key: &[u8], version: Version) -> Option<(Vec<u8>, Version)> {
        let guard = self.inner.read().ok()?;
        let versions = guard.get(key)?;
        find_visible_version(versions, version).map(|v| (v.value.clone(), v.version))
    }

    /// Return the most recently visible version for `key`.
    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)> {
        let guard = self.inner.read().ok()?;
        let versions = guard.get(key)?;
        let last = versions.iter().rev().find(|v| v.visible)?;
        Some((last.value.clone(), last.version))
    }

    /// Batch helper to read latest values for multiple keys.
    fn get_latest_batch(&self, keys: &[&[u8]]) -> Vec<Option<(Vec<u8>, Version)>> {
        let Ok(guard) = self.inner.read() else {
            return vec![None; keys.len()];
        };
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let value = guard.get(*key).and_then(|versions| {
                versions
                    .iter()
                    .rev()
                    .find(|versioned| versioned.visible)
                    .map(|versioned| (versioned.value.clone(), versioned.version))
            });
            out.push(value);
        }
        out
    }

    /// Insert or update a versioned value (initially invisible).
    fn set(&self, key: &[u8], value: &[u8], version: Version) -> anyhow::Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| anyhow::anyhow!("kv store lock poisoned"))?;
        let entry = guard.entry(key.to_vec()).or_default();
        // Decide whether to replace an existing version or insert a new one in order.
        match entry.binary_search_by(|v| v.version.cmp(&version)) {
            // Existing version: overwrite the value in-place.
            Ok(idx) => entry[idx].value = value.to_vec(),
            // New version: insert in sorted order and mark invisible.
            Err(idx) => entry.insert(
                idx,
                VersionedValue {
                    version,
                    value: value.to_vec(),
                    visible: false,
                },
            ),
        }
        Ok(())
    }

    /// Batch variant of `set` with one write lock.
    fn set_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| anyhow::anyhow!("kv store lock poisoned"))?;
        for (key, value, version) in items {
            let entry = guard.entry(key.to_vec()).or_default();
            // Decide whether to replace an existing version or insert a new one in order.
            match entry.binary_search_by(|v| v.version.cmp(version)) {
                // Existing version: overwrite the value in-place.
                Ok(idx) => entry[idx].value = value.to_vec(),
                // New version: insert in sorted order and mark invisible.
                Err(idx) => entry.insert(
                    idx,
                    VersionedValue {
                        version: *version,
                        value: value.to_vec(),
                        visible: false,
                    },
                ),
            }
        }
        Ok(())
    }

    /// Apply committed values as visible with one write lock.
    fn apply_committed_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<u64> {
        let mut inserted_latest = 0u64;
        let mut guard = self
            .inner
            .write()
            .map_err(|_| anyhow::anyhow!("kv store lock poisoned"))?;
        for (key, value, version) in items {
            let entry = guard.entry(key.to_vec()).or_default();
            let had_visible = entry.iter().any(|v| v.visible);
            match entry.binary_search_by(|v| v.version.cmp(version)) {
                Ok(idx) => {
                    entry[idx].value = value.to_vec();
                    entry[idx].visible = true;
                }
                Err(idx) => entry.insert(
                    idx,
                    VersionedValue {
                        version: *version,
                        value: value.to_vec(),
                        visible: true,
                    },
                ),
            }
            if !had_visible {
                inserted_latest = inserted_latest.saturating_add(1);
            }
        }
        Ok(inserted_latest)
    }

    /// Mark a version as visible so reads can observe it.
    fn mark_visible(&self, key: &[u8], version: Version) -> anyhow::Result<bool> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| anyhow::anyhow!("kv store lock poisoned"))?;
        let Some(entry) = guard.get_mut(key) else {
            // Nothing to mark if the key does not exist yet.
            return Ok(false);
        };
        let had_visible = entry.iter().any(|v| v.visible);
        // Only update visibility if the exact version is present.
        if let Ok(idx) = entry.binary_search_by(|v| v.version.cmp(&version)) {
            if entry[idx].visible {
                return Ok(false);
            }
            entry[idx].visible = true;
            return Ok(!had_visible);
        }
        Ok(false)
    }

    /// Batch visibility update with one write lock.
    ///
    /// Purpose:
    /// - Amortize lock overhead for many visibility updates.
    ///
    /// Design:
    /// - Uses borrowed key slices from caller to avoid key cloning.
    ///
    /// Inputs:
    /// - `keys`: borrowed key slices.
    /// - `version`: committed version to expose.
    ///
    /// Outputs:
    /// - Count of keys that gained a latest entry.
    fn mark_visible_batch(&self, keys: &[&[u8]], version: Version) -> anyhow::Result<u64> {
        let mut inserted_latest = 0u64;
        let mut guard = self
            .inner
            .write()
            .map_err(|_| anyhow::anyhow!("kv store lock poisoned"))?;
        for key in keys {
            let Some(entry) = guard.get_mut(*key) else {
                continue;
            };
            let had_visible = entry.iter().any(|v| v.visible);
            let Ok(idx) = entry.binary_search_by(|v| v.version.cmp(&version)) else {
                continue;
            };
            if entry[idx].visible {
                continue;
            }
            entry[idx].visible = true;
            if !had_visible {
                inserted_latest = inserted_latest.saturating_add(1);
            }
        }
        Ok(inserted_latest)
    }
}

/// Maximum number of queued publish requests folded into one Fjall commit.
const FJALL_PUBLISH_MAX_REQUESTS_PER_BATCH: usize = 64;
/// Maximum number of committed writes folded into one Fjall commit.
const FJALL_PUBLISH_MAX_ITEMS_PER_BATCH: usize = 4096;
/// Approximate key/value bytes folded into one Fjall commit before draining stops.
const FJALL_PUBLISH_MAX_BYTES_PER_BATCH: usize = 8 * 1024 * 1024;

static NEXT_FJALL_ENGINE_ID: AtomicUsize = AtomicUsize::new(1);

/// One owned committed KV write handed to the Fjall publish worker.
///
/// Purpose:
/// - Carry write data across the worker channel without borrowing from the
///   Accord apply stack.
///
/// Design:
/// - Stores key/value bytes and the committed version exactly once per queued
///   publish request.
///
/// Inputs:
/// - Borrowed `KvEngine::apply_committed_batch` items copied at the Fjall
///   boundary when a shared publisher is enabled.
///
/// Outputs:
/// - Visible version rows and latest-index candidates during worker commit.
#[derive(Clone, Debug)]
struct OwnedCommittedWrite {
    key: Vec<u8>,
    value: Vec<u8>,
    version: Version,
}

/// Bounded drain limits for one Fjall publish worker commit.
///
/// Purpose:
/// - Prevent one busy keyspace from building unbounded in-memory batches.
///
/// Design:
/// - Uses fixed internal caps instead of public runtime knobs; the worker only
///   drains requests that are already queued.
///
/// Inputs:
/// - Queue contents observed by the worker after the first ready request.
///
/// Outputs:
/// - Upper bounds on request count, item count, and approximate key/value bytes.
#[derive(Clone, Copy, Debug)]
struct FjallPublishBatchLimits {
    max_requests: usize,
    max_items: usize,
    max_bytes: usize,
}

impl Default for FjallPublishBatchLimits {
    /// Build the production default publish-drain limits.
    ///
    /// Purpose:
    /// - Centralize the caps used by the publish worker.
    ///
    /// Design:
    /// - Keeps caps large enough to amortize keyspace commits while small enough
    ///   to bound latency and memory for one worker turn.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Outputs:
    /// - A `FjallPublishBatchLimits` value used by worker drain logic.
    fn default() -> Self {
        Self {
            max_requests: FJALL_PUBLISH_MAX_REQUESTS_PER_BATCH,
            max_items: FJALL_PUBLISH_MAX_ITEMS_PER_BATCH,
            max_bytes: FJALL_PUBLISH_MAX_BYTES_PER_BATCH,
        }
    }
}

/// Runtime counters for a shared Fjall publish worker.
///
/// Purpose:
/// - Track whether the worker is actually combining publish requests and
///   surface error activity in tests/diagnostics.
///
/// Design:
/// - Uses atomics so producers and the worker can update/read without locks.
///
/// Inputs:
/// - Worker batch completions and failed publish commits.
///
/// Outputs:
/// - Snapshot values returned by `FjallPublishBatcher::snapshot`.
#[derive(Default)]
struct FjallPublishBatcherStatsInner {
    batches: AtomicU64,
    requests: AtomicU64,
    items: AtomicU64,
    max_requests_per_batch: AtomicU64,
    max_items_per_batch: AtomicU64,
    errors: AtomicU64,
}

impl FjallPublishBatcherStatsInner {
    /// Record one publish worker turn.
    ///
    /// Purpose:
    /// - Maintain aggregate and maximum batch sizes for observability.
    ///
    /// Design:
    /// - Uses relaxed atomics because these counters are advisory and do not
    ///   participate in correctness.
    ///
    /// Inputs:
    /// - `request_count`: number of caller requests in the committed batch.
    /// - `item_count`: number of committed writes in the committed batch.
    /// - `error`: whether the worker failed the batch.
    ///
    /// Outputs:
    /// - Updated stats visible through snapshots.
    fn record_batch(&self, request_count: usize, item_count: usize, error: bool) {
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.requests
            .fetch_add(request_count as u64, Ordering::Relaxed);
        self.items.fetch_add(item_count as u64, Ordering::Relaxed);
        update_max_atomic(&self.max_requests_per_batch, request_count as u64);
        update_max_atomic(&self.max_items_per_batch, item_count as u64);
        if error {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Return a stable point-in-time view of publish worker counters.
    ///
    /// Purpose:
    /// - Let tests and diagnostics inspect worker behavior without locking.
    ///
    /// Design:
    /// - Loads each relaxed counter independently; exact simultaneity is not
    ///   required for advisory stats.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Outputs:
    /// - A `FjallPublishBatcherStats` snapshot.
    #[allow(dead_code)]
    fn snapshot(&self) -> FjallPublishBatcherStats {
        FjallPublishBatcherStats {
            batches: self.batches.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            items: self.items.load(Ordering::Relaxed),
            max_requests_per_batch: self.max_requests_per_batch.load(Ordering::Relaxed),
            max_items_per_batch: self.max_items_per_batch.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

/// Public snapshot of a Fjall publish worker's batching counters.
///
/// Purpose:
/// - Expose lightweight visibility into committed publish batching.
///
/// Design:
/// - Contains copyable aggregate counters captured from atomics.
///
/// Inputs:
/// - Worker stats maintained by `FjallPublishBatcherStatsInner`.
///
/// Outputs:
/// - Read-only values for tests, diagnostics, or future metrics wiring.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(dead_code)]
pub struct FjallPublishBatcherStats {
    pub batches: u64,
    pub requests: u64,
    pub items: u64,
    pub max_requests_per_batch: u64,
    pub max_items_per_batch: u64,
    pub errors: u64,
}

/// Shared per-keyspace Fjall publish worker.
///
/// Purpose:
/// - Batch naturally queued committed KV publishes from multiple shard engines
///   into one Fjall keyspace commit.
///
/// Design:
/// - A single worker thread blocks for the first request, then drains only
///   already-queued requests up to bounded caps; no artificial sleep is used.
///
/// Inputs:
/// - Owned committed writes submitted by `FjallEngine` instances sharing a
///   keyspace.
///
/// Outputs:
/// - Per-request inserted-latest counts or storage errors returned to callers.
#[derive(Clone)]
pub struct FjallPublishBatcher {
    tx: mpsc::Sender<FjallPublishRequest>,
    #[allow(dead_code)]
    stats: Arc<FjallPublishBatcherStatsInner>,
}

impl FjallPublishBatcher {
    /// Start a publish worker for one Fjall keyspace.
    ///
    /// Purpose:
    /// - Create the shared commit path used by multi-shard KV engines.
    ///
    /// Design:
    /// - Spawns a named worker thread and shares only a sender plus atomic
    ///   stats with producers; per-commit drain limits bound worker batches.
    ///
    /// Inputs:
    /// - `keyspace`: Fjall keyspace whose partitions are committed by worker
    ///   batches.
    ///
    /// Outputs:
    /// - A cloneable `FjallPublishBatcher`.
    pub fn new(keyspace: Arc<Keyspace>) -> Self {
        Self::with_limits(keyspace, FjallPublishBatchLimits::default())
    }

    /// Return current publish worker counters.
    ///
    /// Purpose:
    /// - Make batching behavior observable without exposing worker internals.
    ///
    /// Design:
    /// - Delegates to the atomic stats snapshot.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Outputs:
    /// - A `FjallPublishBatcherStats` snapshot.
    #[allow(dead_code)]
    pub fn snapshot(&self) -> FjallPublishBatcherStats {
        self.stats.snapshot()
    }

    /// Start a publish worker with explicit drain limits.
    ///
    /// Purpose:
    /// - Support production defaults and deterministic tests with the same
    ///   worker implementation.
    ///
    /// Design:
    /// - Private constructor keeps limit tuning internal to the storage layer.
    ///
    /// Inputs:
    /// - `keyspace`: Fjall keyspace to commit.
    /// - `limits`: max requests/items/bytes per worker turn.
    ///
    /// Outputs:
    /// - A cloneable publisher.
    fn with_limits(keyspace: Arc<Keyspace>, limits: FjallPublishBatchLimits) -> Self {
        let (tx, rx) = mpsc::channel();
        let stats = Arc::new(FjallPublishBatcherStatsInner::default());
        let worker_stats = stats.clone();
        thread::Builder::new()
            .name("holo-fjall-publish".to_string())
            .spawn(move || run_fjall_publish_worker(keyspace, rx, limits, worker_stats))
            .expect("spawn fjall publish worker");
        Self { tx, stats }
    }

    /// Publish owned committed writes through the worker.
    ///
    /// Purpose:
    /// - Let callers wait for the durable Fjall batch before Accord marks a
    ///   command executed.
    ///
    /// Design:
    /// - Sends one request and blocks on its one-shot reply; send failure means
    ///   the worker has stopped and apply must fail.
    ///
    /// Inputs:
    /// - `request`: one engine's owned committed writes and partition handles.
    ///
    /// Outputs:
    /// - Inserted-latest count on success, or a storage/worker error.
    fn publish(
        &self,
        engine_id: usize,
        versions: fjall::PartitionHandle,
        latest: fjall::PartitionHandle,
        lock: Arc<RwLock<()>>,
        items: Vec<OwnedCommittedWrite>,
        bytes: usize,
    ) -> anyhow::Result<u64> {
        let (done_tx, done_rx) = mpsc::channel();
        let request = FjallPublishRequest {
            engine_id,
            versions,
            latest,
            lock,
            items,
            bytes,
            done: done_tx,
        };
        self.tx
            .send(request)
            .map_err(|_| anyhow::anyhow!("fjall publish worker stopped"))?;
        done_rx
            .recv()
            .map_err(|_| anyhow::anyhow!("fjall publish worker stopped"))?
    }
}

/// One caller request sent to the shared Fjall publish worker.
///
/// Purpose:
/// - Preserve the partition/lock identity and owned writes needed for a
///   keyspace-level commit.
///
/// Design:
/// - Carries cloned Fjall partition handles plus the engine write lock so the
///   worker maintains the same read/write exclusion as direct engine commits.
///
/// Inputs:
/// - An engine id, Fjall partitions, lock, owned writes, and a reply sender.
///
/// Outputs:
/// - A worker-committed batch contribution and one reply to the caller.
struct FjallPublishRequest {
    engine_id: usize,
    versions: fjall::PartitionHandle,
    latest: fjall::PartitionHandle,
    lock: Arc<RwLock<()>>,
    items: Vec<OwnedCommittedWrite>,
    bytes: usize,
    done: mpsc::Sender<anyhow::Result<u64>>,
}

/// Per-request latest-index candidate selected during a combined publish.
///
/// Purpose:
/// - Track which item should update `kv_latest` for one `(engine, key)` pair.
///
/// Design:
/// - Stores indexes back into the request/item arrays to avoid cloning values
///   while deciding the latest write.
///
/// Inputs:
/// - Existing latest version and committed writes in the combined publish.
///
/// Outputs:
/// - One optional latest-index update and inserted-latest accounting owner.
struct FjallLatestCandidate {
    current: Option<Version>,
    request_idx: usize,
    item_idx: usize,
    version: Option<Version>,
    had_latest: bool,
}

/// Unique key for latest-index candidates in a combined Fjall publish.
///
/// Purpose:
/// - Keep latest-index decisions separate for shard partitions that may contain
///   the same user key bytes.
///
/// Design:
/// - Combines the engine id with the user key bytes.
///
/// Inputs:
/// - Engine id and user key from a publish request.
///
/// Outputs:
/// - Hashable map key for candidate tracking.
#[derive(Hash, PartialEq, Eq)]
struct FjallLatestKey {
    engine_id: usize,
    key: Vec<u8>,
}

/// Update an atomic maximum counter.
///
/// Purpose:
/// - Record peak batch sizes without locking.
///
/// Design:
/// - Uses a compare/exchange loop and exits early when the current max is
///   already greater than or equal to `value`.
///
/// Inputs:
/// - `slot`: atomic maximum counter.
/// - `value`: observed candidate max.
///
/// Outputs:
/// - `slot` is increased if `value` is larger.
fn update_max_atomic(slot: &AtomicU64, value: u64) {
    let mut current = slot.load(Ordering::Relaxed);
    while value > current {
        match slot.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

/// Run the shared Fjall publish worker loop.
///
/// Purpose:
/// - Serialize and opportunistically batch committed KV publishes for one
///   Fjall keyspace.
///
/// Design:
/// - Blocks for the first request, then drains only immediately available
///   queued requests while under internal caps; it never sleeps to manufacture
///   batching.
///
/// Inputs:
/// - `keyspace`: keyspace used to create combined Fjall batches.
/// - `rx`: request receiver owned by this worker.
/// - `limits`: bounded drain limits per worker turn.
/// - `stats`: shared advisory counters.
///
/// Outputs:
/// - Sends one success/error reply per request and exits when all senders drop.
fn run_fjall_publish_worker(
    keyspace: Arc<Keyspace>,
    rx: mpsc::Receiver<FjallPublishRequest>,
    limits: FjallPublishBatchLimits,
    stats: Arc<FjallPublishBatcherStatsInner>,
) {
    while let Ok(first) = rx.recv() {
        let mut requests = Vec::with_capacity(limits.max_requests.clamp(1, 8));
        let mut item_count = first.items.len();
        let mut byte_count = first.bytes;
        requests.push(first);

        loop {
            if requests.len() >= limits.max_requests
                || item_count >= limits.max_items
                || byte_count >= limits.max_bytes
            {
                // Stop draining once the current batch is large enough; this
                // deliberately leaves later requests queued for the next turn
                // instead of waiting for more work or growing memory further.
                break;
            }
            match rx.try_recv() {
                Ok(next) => {
                    item_count = item_count.saturating_add(next.items.len());
                    byte_count = byte_count.saturating_add(next.bytes);
                    requests.push(next);
                }
                Err(mpsc::TryRecvError::Empty) => {
                    // No already-queued request is available, so commit now
                    // rather than adding artificial latency.
                    break;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    // Finish the requests already collected; the outer loop
                    // will exit after replying to callers.
                    break;
                }
            }
        }

        let result = publish_fjall_requests(&keyspace, &requests);
        let failed = result.is_err();
        stats.record_batch(requests.len(), item_count, failed);
        match result {
            Ok(counts) => {
                for (request, inserted_latest) in requests.into_iter().zip(counts.into_iter()) {
                    let _ = request.done.send(Ok(inserted_latest));
                }
            }
            Err(err) => {
                let message = err.to_string();
                for request in requests {
                    let _ = request.done.send(Err(anyhow::anyhow!(message.clone())));
                }
            }
        }
    }
}

/// Commit a set of Fjall publish requests in one keyspace batch.
///
/// Purpose:
/// - Share one Fjall commit across multiple shard-engine publish requests while
///   preserving each caller's inserted-latest accounting.
///
/// Design:
/// - Acquires participating engine write locks in engine-id order, writes all
///   visible version rows, deduplicates latest-index decisions per
///   `(engine_id, key)`, then commits once.
///
/// Inputs:
/// - `keyspace`: keyspace used to allocate the Fjall batch.
/// - `requests`: non-empty publish requests already selected by the worker.
///
/// Outputs:
/// - Per-request inserted-latest counts or a storage error.
fn publish_fjall_requests(
    keyspace: &Keyspace,
    requests: &[FjallPublishRequest],
) -> anyhow::Result<Vec<u64>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let mut lock_by_engine: HashMap<usize, Arc<RwLock<()>>> =
        HashMap::with_capacity(requests.len());
    for request in requests {
        lock_by_engine
            .entry(request.engine_id)
            .or_insert_with(|| request.lock.clone());
    }
    let mut locks = lock_by_engine.into_iter().collect::<Vec<_>>();
    locks.sort_unstable_by_key(|(engine_id, _)| *engine_id);

    let mut guards = Vec::with_capacity(locks.len());
    for (_, lock) in &locks {
        // Hold every participating engine lock through latest reads and commit
        // so direct reads cannot observe half-published rows. Lock ordering by
        // engine id avoids multi-engine deadlocks; there is no attempt to merge
        // or bypass the per-engine read/write exclusion contract.
        guards.push(
            lock.write()
                .map_err(|_| anyhow::anyhow!("fjall kv lock poisoned"))?,
        );
    }

    let total_items = requests
        .iter()
        .map(|request| request.items.len())
        .sum::<usize>();
    let mut batch = keyspace.batch();
    let mut latest_by_key: HashMap<FjallLatestKey, FjallLatestCandidate> =
        HashMap::with_capacity(total_items);

    for (request_idx, request) in requests.iter().enumerate() {
        for (item_idx, item) in request.items.iter().enumerate() {
            let entry_key = encode_version_key(&item.key, item.version);
            let entry_value = encode_version_value(true, &item.value);
            batch.insert(&request.versions, entry_key, entry_value);

            let latest_key = FjallLatestKey {
                engine_id: request.engine_id,
                key: item.key.clone(),
            };
            match latest_by_key.entry(latest_key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let candidate = entry.get_mut();
                    let base = candidate.version.or(candidate.current);
                    if should_update_latest(base, item.version) {
                        candidate.request_idx = request_idx;
                        candidate.item_idx = item_idx;
                        candidate.version = Some(item.version);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let current = match request.latest.get(&item.key) {
                        Ok(Some(bytes)) => Some(decode_latest_version(&bytes)?),
                        Ok(None) => None,
                        Err(err) => {
                            return Err(anyhow::anyhow!(err).context("fjall kv latest read failed"));
                        }
                    };
                    let mut candidate = FjallLatestCandidate {
                        current,
                        request_idx,
                        item_idx,
                        version: None,
                        had_latest: current.is_some(),
                    };
                    if should_update_latest(current, item.version) {
                        candidate.version = Some(item.version);
                    }
                    entry.insert(candidate);
                }
            }
        }
    }

    let mut inserted_by_request = vec![0u64; requests.len()];
    for (latest_key, candidate) in latest_by_key {
        let Some(version) = candidate.version else {
            // The committed version row still exists; this write simply did not
            // supersede the current latest index entry.
            continue;
        };
        let request = &requests[candidate.request_idx];
        let item = &request.items[candidate.item_idx];
        if !candidate.had_latest {
            inserted_by_request[candidate.request_idx] =
                inserted_by_request[candidate.request_idx].saturating_add(1);
        }
        batch.insert(
            &request.latest,
            latest_key.key,
            encode_latest_value(version, &item.value),
        );
    }

    batch
        .commit()
        .map_err(|err| anyhow::anyhow!(err).context("fjall kv committed publish batch failed"))?;
    drop(guards);
    Ok(inserted_by_request)
}

/// Fjall-backed key/value engine that stores versions and a latest index.
///
/// Purpose:
/// - Persist committed KV versions and serve latest/historical reads.
///
/// Design:
/// - Owns one versions partition and one latest-index partition, guarded by a
///   per-engine RW lock; committed publishes can run directly or through an
///   optional shared keyspace publish worker.
///
/// Inputs:
/// - Encoded command writes from the KV state machine and read keys from
///   client/query paths.
///
/// Outputs:
/// - Visible version rows, latest-index rows, and decoded read values.
pub struct FjallEngine {
    engine_id: usize,
    keyspace: Arc<Keyspace>,
    versions: fjall::PartitionHandle,
    latest: fjall::PartitionHandle,
    lock: Arc<RwLock<()>>,
    publish_batcher: Option<FjallPublishBatcher>,
}

// Deterministic, process-stable seeds for shard selection and peer ordering.
const SHARD_HASH_STATE: RandomState = RandomState::with_seeds(
    0x243f_6a88_85a3_08d3,
    0x1319_8a2e_0370_7344,
    0xa409_3822_299f_31d0,
    0x082e_fa98_ec4e_6c89,
);

/// Hash a key for shard selection and peer ordering.
pub fn hash_key(bytes: &[u8]) -> u64 {
    let mut hasher = SHARD_HASH_STATE.build_hasher();
    hasher.write(bytes);
    hasher.finish()
}

impl FjallEngine {
    /// Open the default (single-shard) Fjall partitions.
    pub fn open(keyspace: Arc<Keyspace>) -> anyhow::Result<Self> {
        Self::open_with_publisher(keyspace, None)
    }

    /// Open the default Fjall partitions with an optional shared publisher.
    ///
    /// Purpose:
    /// - Allow production code to opt into keyspace-level publish batching while
    ///   keeping direct single-shard operation allocation-light.
    ///
    /// Design:
    /// - Creates the default version/latest partitions and stores a cloneable
    ///   publisher used only by `apply_committed_batch`.
    ///
    /// Inputs:
    /// - `keyspace`: Fjall keyspace containing KV partitions.
    /// - `publish_batcher`: optional shared publish worker.
    ///
    /// Outputs:
    /// - A ready `FjallEngine`.
    pub fn open_with_publisher(
        keyspace: Arc<Keyspace>,
        publish_batcher: Option<FjallPublishBatcher>,
    ) -> anyhow::Result<Self> {
        let versions = keyspace.open_partition("kv_versions", PartitionCreateOptions::default())?;
        let latest = keyspace.open_partition("kv_latest", PartitionCreateOptions::default())?;
        Ok(Self {
            engine_id: NEXT_FJALL_ENGINE_ID.fetch_add(1, Ordering::Relaxed),
            keyspace,
            versions,
            latest,
            lock: Arc::new(RwLock::new(())),
            publish_batcher,
        })
    }

    /// Open shard-specific Fjall partitions by suffixing partition names.
    pub fn open_shard(keyspace: Arc<Keyspace>, shard: usize) -> anyhow::Result<Self> {
        Self::open_shard_with_publisher(keyspace, shard, None)
    }

    /// Open shard-specific Fjall partitions with an optional shared publisher.
    ///
    /// Purpose:
    /// - Attach many shard engines to one keyspace-level publish worker.
    ///
    /// Design:
    /// - Suffixes partition names by shard and shares only the publisher, not
    ///   shard partition state.
    ///
    /// Inputs:
    /// - `keyspace`: Fjall keyspace containing shard partitions.
    /// - `shard`: shard index used in partition names.
    /// - `publish_batcher`: optional shared publish worker.
    ///
    /// Outputs:
    /// - A ready shard `FjallEngine`.
    pub fn open_shard_with_publisher(
        keyspace: Arc<Keyspace>,
        shard: usize,
        publish_batcher: Option<FjallPublishBatcher>,
    ) -> anyhow::Result<Self> {
        let versions_name = format!("kv_versions_{shard}");
        let latest_name = format!("kv_latest_{shard}");
        let versions =
            keyspace.open_partition(&versions_name, PartitionCreateOptions::default())?;
        let latest = keyspace.open_partition(&latest_name, PartitionCreateOptions::default())?;
        Ok(Self {
            engine_id: NEXT_FJALL_ENGINE_ID.fetch_add(1, Ordering::Relaxed),
            keyspace,
            versions,
            latest,
            lock: Arc::new(RwLock::new(())),
            publish_batcher,
        })
    }
}

impl KvEngine for FjallEngine {
    /// Read the latest visible value <= `version` by consulting the latest index
    /// first and falling back to a reverse scan of the versioned partition.
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>> {
        self.get_versioned(key, version).map(|(value, _)| value)
    }

    /// Read the latest visible value <= `version` by consulting the latest index
    /// first and falling back to a reverse scan of the versioned partition.
    fn get_versioned(&self, key: &[u8], version: Version) -> Option<(Vec<u8>, Version)> {
        let _guard = self.lock.read().ok()?;
        if let Ok(Some(bytes)) = self.latest.get(key) {
            if let Ok((latest_version, latest_value)) = decode_latest_value(&bytes) {
                // Fast path: latest value is already visible for this version.
                if latest_version <= version {
                    return Some((latest_value, latest_version));
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
                    return Some((value, entry_version));
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
    fn get_latest_batch(&self, keys: &[&[u8]]) -> Vec<Option<(Vec<u8>, Version)>> {
        let _guard = match self.lock.read() {
            Ok(guard) => guard,
            // If the lock is poisoned, preserve ordering with missing entries.
            Err(_) => return vec![None; keys.len()],
        };
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let value = match self.latest.get(*key) {
                Ok(Some(bytes)) => decode_latest_value(&bytes).ok().map(|(version, value)| {
                    // Keep `(value, version)` ordering expected by the trait.
                    (value, version)
                }),
                Ok(None) => None,
                Err(err) => {
                    warn!(error = ?err, "fjall kv latest batch read failed");
                    None
                }
            };
            out.push(value);
        }
        out
    }

    /// Insert a versioned value (initially invisible) into the versions partition.
    fn set(&self, key: &[u8], value: &[u8], version: Version) -> anyhow::Result<()> {
        let _guard = self
            .lock
            .write()
            .map_err(|_| anyhow::anyhow!("fjall kv lock poisoned"))?;
        let entry_key = encode_version_key(key, version);
        let entry_value = encode_version_value(false, value);
        self.versions
            .insert(entry_key, entry_value)
            .map_err(|err| anyhow::anyhow!(err).context("fjall kv write failed"))?;
        Ok(())
    }

    /// Batch insert multiple versioned values in one Fjall transaction.
    fn set_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<()> {
        let _guard = self
            .lock
            .write()
            .map_err(|_| anyhow::anyhow!("fjall kv lock poisoned"))?;

        let mut batch = self.keyspace.batch();
        for (key, value, version) in items {
            let entry_key = encode_version_key(key, *version);
            let entry_value = encode_version_value(false, value);
            batch.insert(&self.versions, entry_key, entry_value);
        }

        batch
            .commit()
            .map_err(|err| anyhow::anyhow!(err).context("fjall kv batch write failed"))?;
        Ok(())
    }

    /// Apply committed values as visible and update the latest index.
    ///
    /// Purpose:
    /// - Publish committed writes atomically before Accord execution advances.
    ///
    /// Design:
    /// - Uses the shared publish worker when configured, otherwise commits one
    ///   direct Fjall batch for this engine.
    ///
    /// Inputs:
    /// - Borrowed `(key, value, version)` tuples from the state-machine apply.
    ///
    /// Outputs:
    /// - Count of keys whose latest index transitioned from missing to present.
    fn apply_committed_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<u64> {
        if items.is_empty() {
            return Ok(0);
        }

        if let Some(publisher) = &self.publish_batcher {
            let mut owned = Vec::with_capacity(items.len());
            let mut bytes = 0usize;
            for (key, value, version) in items {
                // Clone only at the asynchronous worker boundary; direct
                // engines keep the borrowed zero-copy path.
                bytes = bytes.saturating_add(key.len()).saturating_add(value.len());
                owned.push(OwnedCommittedWrite {
                    key: (*key).to_vec(),
                    value: (*value).to_vec(),
                    version: *version,
                });
            }
            return publisher.publish(
                self.engine_id,
                self.versions.clone(),
                self.latest.clone(),
                self.lock.clone(),
                owned,
                bytes,
            );
        }

        let _guard = self
            .lock
            .write()
            .map_err(|_| anyhow::anyhow!("fjall kv lock poisoned"))?;

        struct LatestCandidate {
            current: Option<Version>,
            item_idx: usize,
            version: Option<Version>,
            had_latest: bool,
        }

        let mut batch = self.keyspace.batch();
        let mut latest_by_key: HashMap<Vec<u8>, LatestCandidate> =
            HashMap::with_capacity(items.len());

        for (idx, (key, value, version)) in items.iter().enumerate() {
            let entry_key = encode_version_key(key, *version);
            let entry_value = encode_version_value(true, value);
            batch.insert(&self.versions, entry_key, entry_value);

            match latest_by_key.entry((*key).to_vec()) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let candidate = entry.get_mut();
                    let base = candidate.version.or(candidate.current);
                    if should_update_latest(base, *version) {
                        candidate.item_idx = idx;
                        candidate.version = Some(*version);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let current = match self.latest.get(*key) {
                        Ok(Some(bytes)) => Some(decode_latest_version(&bytes)?),
                        Ok(None) => None,
                        Err(err) => {
                            return Err(anyhow::anyhow!(err).context("fjall kv latest read failed"));
                        }
                    };
                    let mut candidate = LatestCandidate {
                        current,
                        item_idx: idx,
                        version: None,
                        had_latest: current.is_some(),
                    };
                    if should_update_latest(current, *version) {
                        candidate.version = Some(*version);
                    }
                    entry.insert(candidate);
                }
            }
        }

        let mut inserted_latest = 0u64;
        for (key, candidate) in latest_by_key {
            let Some(version) = candidate.version else {
                continue;
            };
            let value = items[candidate.item_idx].1;
            if !candidate.had_latest {
                inserted_latest = inserted_latest.saturating_add(1);
            }
            batch.insert(&self.latest, key, encode_latest_value(version, value));
        }

        batch
            .commit()
            .map_err(|err| anyhow::anyhow!(err).context("fjall kv committed batch failed"))?;
        Ok(inserted_latest)
    }

    /// Mark a version visible and update the latest index if needed.
    fn mark_visible(&self, key: &[u8], version: Version) -> anyhow::Result<bool> {
        let _guard = self
            .lock
            .write()
            .map_err(|_| anyhow::anyhow!("fjall kv lock poisoned"))?;
        let entry_key = encode_version_key(key, version);
        let entry_value = match self.versions.get(&entry_key) {
            Ok(Some(bytes)) => bytes,
            // Nothing to mark if the version does not exist.
            Ok(None) => return Ok(false),
            Err(err) => {
                return Err(anyhow::anyhow!(err).context("fjall kv read failed"));
            }
        };

        let (visible, value) = decode_version_value(&entry_value)?;
        // Skip updates if the entry is already visible.
        if visible {
            return Ok(false);
        }

        let mut batch = self.keyspace.batch();
        batch.insert(
            &self.versions,
            entry_key,
            encode_version_value(true, &value),
        );

        let latest_current = match self.latest.get(key) {
            Ok(Some(bytes)) => Some(decode_latest_version(&bytes)?),
            Ok(None) => None,
            Err(err) => {
                return Err(anyhow::anyhow!(err).context("fjall kv latest read failed"));
            }
        };
        let had_latest = latest_current.is_some();
        // Only update the latest index if this version should supersede the
        // current latest entry.
        //
        // `seq` is only ordered *within* an Accord group. During range splits a
        // key can move to a different group, so comparing versions across
        // groups using `(seq, txn_id)` is not meaningful and can pin stale
        // entries in `kv_latest`.
        //
        // If group ids differ, treat the newly visible value as newer for the
        // purpose of the latest index.
        let should_update_latest = should_update_latest(latest_current, version);
        let mut inserted_latest = false;
        if should_update_latest {
            inserted_latest = !had_latest;
            batch.insert(
                &self.latest,
                key.to_vec(),
                encode_latest_value(version, &value),
            );
        }

        batch
            .commit()
            .map_err(|err| anyhow::anyhow!(err).context("fjall kv mark visible failed"))?;
        Ok(inserted_latest)
    }

    /// Mark many keys visible with one Fjall batch commit.
    ///
    /// Purpose:
    /// - Batch visibility updates in one durable commit.
    ///
    /// Design:
    /// - Deduplicates borrowed keys to avoid duplicate writes/counting.
    /// - Returns storage errors so the executor can retry without advancing.
    ///
    /// Inputs:
    /// - `keys`: borrowed key slices.
    /// - `version`: committed version to expose.
    ///
    /// Outputs:
    /// - Count of keys that gained a latest entry.
    fn mark_visible_batch(&self, keys: &[&[u8]], version: Version) -> anyhow::Result<u64> {
        if keys.is_empty() {
            return Ok(0);
        }

        // Skip duplicate keys in the same command to avoid duplicate work and
        // double-counting inserted-latest transitions.
        let mut unique_keys = Vec::with_capacity(keys.len());
        let mut seen: HashSet<&[u8]> = HashSet::with_capacity(keys.len());
        for &key in keys {
            // Keep only the first occurrence for each key in this batch. This
            // does not alter correctness because mark-visible is idempotent.
            if seen.insert(key) {
                unique_keys.push(key);
            }
        }
        if unique_keys.is_empty() {
            return Ok(0);
        }

        let mut inserted_latest = 0u64;
        let _guard = self
            .lock
            .write()
            .map_err(|_| anyhow::anyhow!("fjall kv lock poisoned"))?;

        let mut batch = self.keyspace.batch();
        let mut updates = 0usize;
        for &key in &unique_keys {
            let entry_key = encode_version_key(key, version);
            let entry_value = match self.versions.get(&entry_key) {
                Ok(Some(bytes)) => bytes,
                // Nothing to mark if the version does not exist.
                Ok(None) => continue,
                Err(err) => {
                    return Err(anyhow::anyhow!(err).context("fjall kv read failed"));
                }
            };

            let (visible, value) = decode_version_value(&entry_value)?;
            // Skip updates if the entry is already visible.
            if visible {
                continue;
            }

            let latest_current = match self.latest.get(key) {
                Ok(Some(bytes)) => Some(decode_latest_version(&bytes)?),
                Ok(None) => None,
                Err(err) => {
                    return Err(anyhow::anyhow!(err).context("fjall kv latest read failed"));
                }
            };
            let had_latest = latest_current.is_some();
            let should_update_latest = should_update_latest(latest_current, version);

            batch.insert(
                &self.versions,
                entry_key,
                encode_version_value(true, &value),
            );
            if should_update_latest {
                if !had_latest {
                    inserted_latest = inserted_latest.saturating_add(1);
                }
                batch.insert(
                    &self.latest,
                    key.to_vec(),
                    encode_latest_value(version, &value),
                );
            }
            updates = updates.saturating_add(1);
        }

        if updates == 0 {
            return Ok(0);
        }
        batch
            .commit()
            .map_err(|err| anyhow::anyhow!(err).context("fjall kv batch mark visible failed"))?;
        Ok(inserted_latest)
    }
}

/// Choose the newest value from two optional versioned read results.
///
/// Purpose:
/// - Merge primary and fallback storage reads during deferred range
///   materialization.
///
/// Design:
/// - `Version` ordering is the correctness contract: newer range generations
///   beat old-owner sequence numbers, and same-generation sequence ordering
///   still handles delayed old-owner replays.
///
/// Inputs:
/// - Primary and fallback latest values for the same key.
///
/// Outputs:
/// - The latest visible value/version pair, or `None` if both are missing.
fn latest_versioned_pair(
    left: Option<(Vec<u8>, Version)>,
    right: Option<(Vec<u8>, Version)>,
) -> Option<(Vec<u8>, Version)> {
    match (left, right) {
        (Some(left), Some(right)) => {
            if right.1 > left.1 {
                Some(right)
            } else {
                Some(left)
            }
        }
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

/// Decode one committed write command as a versioned value candidate for a key.
///
/// Purpose:
/// - Give future Accord-owned read paths a reusable way to inspect committed
///   write bytes after consensus has established the read's visibility point.
///
/// Design:
/// - Uses the same command decoding and generation override rules as normal KV
///   apply, then returns the newest write in the command for `key`.
/// - Unknown or malformed commands return an error so callers can fail closed
///   to the ordinary execution wait path.
///
/// Inputs:
/// - `data`: committed KV command bytes.
/// - `meta`: Accord execution metadata for the committed transaction.
/// - `key`: key being read.
///
/// Outputs:
/// - The committed value/version for `key`, or `None` if the command does not
///   write that key.
pub fn committed_write_value_for_key(
    data: &[u8],
    meta: ExecMeta,
    key: &[u8],
) -> anyhow::Result<Option<(Vec<u8>, Version)>> {
    let mut values = committed_write_values_for_keys(data, meta, &[key])?;
    Ok(values.pop().unwrap_or(None))
}

/// Decode one committed write command as versioned value candidates for many keys.
///
/// Purpose:
/// - Serve a read batch from committed command bytes without reparsing the same
///   command once per requested key.
///
/// Design:
/// - Builds a borrowed key-index map for the requested read keys.
/// - Decodes the command once, then fills all matching output slots using the
///   same version ordering as normal latest-value reads.
///
/// Inputs:
/// - `data`: committed KV command bytes.
/// - `meta`: Accord execution metadata for the committed transaction.
/// - `keys`: read keys in requested output order.
///
/// Outputs:
/// - One optional committed value/version per input key, preserving order.
pub fn committed_write_values_for_keys(
    data: &[u8],
    meta: ExecMeta,
    keys: &[&[u8]],
) -> anyhow::Result<Vec<Option<(Vec<u8>, Version)>>> {
    let mut out = vec![None; keys.len()];
    if keys.is_empty() {
        return Ok(out);
    }

    let mut key_indices: HashMap<&[u8], Vec<usize>> = HashMap::with_capacity(keys.len());
    for (idx, key) in keys.iter().copied().enumerate() {
        key_indices.entry(key).or_default().push(idx);
    }

    let base_version = Version::from(meta);
    for item in decode_write_items(data, base_version)? {
        let Some(indices) = key_indices.get(item.key) else {
            continue;
        };
        for idx in indices.iter().copied() {
            let next = Some((item.value.to_vec(), item.version));
            out[idx] = latest_versioned_pair(out[idx].take(), next);
        }
    }
    Ok(out)
}

/// Sharded wrapper that routes keys to per-shard `KvEngine` instances.
pub struct RoutedKvEngine {
    shards: Vec<Arc<dyn KvEngine>>,
    router: Arc<dyn ShardRouter>,
}

impl RoutedKvEngine {
    /// Create a routed engine from pre-built shard engines and a router.
    pub fn new(
        shards: Vec<Arc<dyn KvEngine>>,
        router: Arc<dyn ShardRouter>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !shards.is_empty(),
            "routed kv engine requires at least one shard"
        );
        Ok(Self { shards, router })
    }

    fn shard_for_key(&self, key: &[u8]) -> usize {
        let idx = self.router.shard_for_key(key);
        idx.min(self.shards.len().saturating_sub(1))
    }

    fn fallback_shard_for_key(&self, key: &[u8], primary: usize) -> Option<usize> {
        let idx = self.router.fallback_shard_for_key(key)?;
        let idx = idx.min(self.shards.len().saturating_sub(1));
        (idx != primary).then_some(idx)
    }
}

impl KvEngine for RoutedKvEngine {
    fn get(&self, key: &[u8], version: Version) -> Option<Vec<u8>> {
        self.get_versioned(key, version).map(|(value, _)| value)
    }

    fn get_versioned(&self, key: &[u8], version: Version) -> Option<(Vec<u8>, Version)> {
        let shard = self.shard_for_key(key);
        let primary = self.shards[shard].get_versioned(key, version);
        let fallback = self
            .router
            .may_have_fallback_shards()
            .then(|| {
                self.fallback_shard_for_key(key, shard)
                    .and_then(|fallback| self.shards[fallback].get_versioned(key, version))
            })
            .flatten();
        latest_versioned_pair(primary, fallback)
    }

    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)> {
        let shard = self.shard_for_key(key);
        let primary = self.shards[shard].get_latest(key);
        let fallback = self
            .router
            .may_have_fallback_shards()
            .then(|| {
                self.fallback_shard_for_key(key, shard)
                    .and_then(|fallback| self.shards[fallback].get_latest(key))
            })
            .flatten();
        latest_versioned_pair(primary, fallback)
    }

    fn get_latest_batch(&self, keys: &[&[u8]]) -> Vec<Option<(Vec<u8>, Version)>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let mut results = vec![None; keys.len()];
        let mut primary_shards = vec![0usize; keys.len()];
        let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, key) in keys.iter().enumerate() {
            let shard = self.shard_for_key(key);
            primary_shards[idx] = shard;
            by_shard[shard].push(idx);
        }
        for (shard, indices) in by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut shard_keys = Vec::with_capacity(indices.len());
            for idx in &indices {
                shard_keys.push(keys[*idx]);
            }
            let shard_values = self.shards[shard].get_latest_batch(&shard_keys);
            for (idx, value) in indices.into_iter().zip(shard_values.into_iter()) {
                results[idx] = value;
            }
        }
        if !self.router.may_have_fallback_shards() {
            return results;
        }

        let mut fallback_by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, key) in keys.iter().enumerate() {
            if let Some(fallback) = self.fallback_shard_for_key(key, primary_shards[idx]) {
                fallback_by_shard[fallback].push(idx);
            }
        }
        for (shard, indices) in fallback_by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut shard_keys = Vec::with_capacity(indices.len());
            for idx in &indices {
                shard_keys.push(keys[*idx]);
            }
            let shard_values = self.shards[shard].get_latest_batch(&shard_keys);
            for (idx, value) in indices.into_iter().zip(shard_values.into_iter()) {
                results[idx] = latest_versioned_pair(results[idx].take(), value);
            }
        }
        results
    }

    fn set(&self, key: &[u8], value: &[u8], version: Version) -> anyhow::Result<()> {
        let shard = self.shard_for_key(key);
        self.shards[shard].set(key, value, version)?;
        if self.router.may_have_fallback_shards() {
            if let Some(fallback) = self.fallback_shard_for_key(key, shard) {
                self.shards[fallback].set(key, value, version)?;
            }
        }
        Ok(())
    }

    fn set_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, (key, _, _)) in items.iter().enumerate() {
            let shard = self.shard_for_key(key);
            by_shard[shard].push(idx);
        }
        for (shard, indices) in by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut batch = Vec::with_capacity(indices.len());
            for idx in indices {
                let (key, value, version) = items[idx];
                batch.push((key, value, version));
            }
            self.shards[shard].set_batch(&batch)?;
        }
        if !self.router.may_have_fallback_shards() {
            return Ok(());
        }
        let mut fallback_by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, (key, _, _)) in items.iter().enumerate() {
            let shard = self.shard_for_key(key);
            if let Some(fallback) = self.fallback_shard_for_key(key, shard) {
                fallback_by_shard[fallback].push(idx);
            }
        }
        for (shard, indices) in fallback_by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut batch = Vec::with_capacity(indices.len());
            for idx in indices {
                let (key, value, version) = items[idx];
                batch.push((key, value, version));
            }
            self.shards[shard].set_batch(&batch)?;
        }
        Ok(())
    }

    fn apply_committed_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<u64> {
        if items.is_empty() {
            return Ok(0);
        }
        if self.shards.len() == 1 {
            return self.shards[0].apply_committed_batch(items);
        }

        let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, (key, _, _)) in items.iter().enumerate() {
            let shard = self.shard_for_key(key);
            by_shard[shard].push(idx);
        }

        let mut inserted = 0u64;
        for (shard, indices) in by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut batch = Vec::with_capacity(indices.len());
            for idx in indices {
                let (key, value, version) = items[idx];
                batch.push((key, value, version));
            }
            inserted = inserted.saturating_add(self.shards[shard].apply_committed_batch(&batch)?);
        }
        if !self.router.may_have_fallback_shards() {
            return Ok(inserted);
        }
        let mut fallback_by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, (key, _, _)) in items.iter().enumerate() {
            let shard = self.shard_for_key(key);
            if let Some(fallback) = self.fallback_shard_for_key(key, shard) {
                fallback_by_shard[fallback].push(idx);
            }
        }
        for (shard, indices) in fallback_by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut batch = Vec::with_capacity(indices.len());
            for idx in indices {
                let (key, value, version) = items[idx];
                batch.push((key, value, version));
            }
            let _ = self.shards[shard].apply_committed_batch(&batch)?;
        }
        Ok(inserted)
    }

    fn mark_visible(&self, key: &[u8], version: Version) -> anyhow::Result<bool> {
        let shard = self.shard_for_key(key);
        let inserted = self.shards[shard].mark_visible(key, version)?;
        if self.router.may_have_fallback_shards() {
            if let Some(fallback) = self.fallback_shard_for_key(key, shard) {
                let _ = self.shards[fallback].mark_visible(key, version)?;
            }
        }
        Ok(inserted)
    }

    /// Batch visibility updates across routed shards.
    ///
    /// Purpose:
    /// - Route visibility updates to the owning shard without cloning keys.
    ///
    /// Design:
    /// - Buckets input indices per shard, then passes borrowed key slices to
    ///   shard engines.
    ///
    /// Inputs:
    /// - `keys`: borrowed key slices.
    /// - `version`: committed version to expose.
    ///
    /// Outputs:
    /// - Total inserted-latest count across all shards.
    fn mark_visible_batch(&self, keys: &[&[u8]], version: Version) -> anyhow::Result<u64> {
        if keys.is_empty() {
            return Ok(0);
        }
        if self.shards.len() == 1 {
            return self.shards[0].mark_visible_batch(keys, version);
        }

        let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, key) in keys.iter().enumerate() {
            // Bucket indices to avoid cloning keys into per-shard vectors.
            let shard = self.shard_for_key(key);
            by_shard[shard].push(idx);
        }

        let mut inserted = 0u64;
        let use_fallbacks = self.router.may_have_fallback_shards();
        let mut fallback_by_shard: Vec<Vec<usize>> = if use_fallbacks {
            vec![Vec::new(); self.shards.len()]
        } else {
            Vec::new()
        };
        for (shard, indices) in by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut shard_keys = Vec::with_capacity(indices.len());
            for idx in &indices {
                shard_keys.push(keys[*idx]);
            }
            inserted = inserted
                .saturating_add(self.shards[shard].mark_visible_batch(&shard_keys, version)?);
            if use_fallbacks {
                for idx in indices {
                    if let Some(fallback) = self.fallback_shard_for_key(keys[idx], shard) {
                        fallback_by_shard[fallback].push(idx);
                    }
                }
            }
        }
        if !use_fallbacks {
            return Ok(inserted);
        }

        for (shard, indices) in fallback_by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut shard_keys = Vec::with_capacity(indices.len());
            for idx in indices {
                shard_keys.push(keys[idx]);
            }
            let _ = self.shards[shard].mark_visible_batch(&shard_keys, version)?;
        }
        Ok(inserted)
    }
}

/// Sharded wrapper that routes keys to per-shard `KvEngine` instances.
pub struct ShardedKvEngine {
    shards: Vec<Arc<dyn KvEngine>>,
}

impl ShardedKvEngine {
    /// Create a sharded engine from pre-built shard engines.
    pub fn new(shards: Vec<Arc<dyn KvEngine>>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !shards.is_empty(),
            "sharded kv engine requires at least one shard"
        );
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
        self.get_versioned(key, version).map(|(value, _)| value)
    }

    /// Delegate a versioned read to the chosen shard.
    fn get_versioned(&self, key: &[u8], version: Version) -> Option<(Vec<u8>, Version)> {
        let shard = self.shard_for_key(key);
        self.shards[shard].get_versioned(key, version)
    }

    /// Delegate a latest read to the chosen shard.
    fn get_latest(&self, key: &[u8]) -> Option<(Vec<u8>, Version)> {
        let shard = self.shard_for_key(key);
        self.shards[shard].get_latest(key)
    }

    /// Batch latest reads across shards while preserving input order.
    fn get_latest_batch(&self, keys: &[&[u8]]) -> Vec<Option<(Vec<u8>, Version)>> {
        if keys.is_empty() {
            // Empty input means empty output; avoid shard bookkeeping.
            return Vec::new();
        }
        let mut results = vec![None; keys.len()];
        let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, key) in keys.iter().enumerate() {
            let shard = self.shard_for_key(key);
            by_shard[shard].push(idx);
        }
        for (shard, indices) in by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                // Skip shards that have no keys assigned.
                continue;
            }
            let mut shard_keys = Vec::with_capacity(indices.len());
            for idx in &indices {
                shard_keys.push(keys[*idx]);
            }
            let shard_values = self.shards[shard].get_latest_batch(&shard_keys);
            for (idx, value) in indices.into_iter().zip(shard_values.into_iter()) {
                results[idx] = value;
            }
        }
        results
    }

    /// Delegate a write to the chosen shard.
    fn set(&self, key: &[u8], value: &[u8], version: Version) -> anyhow::Result<()> {
        let shard = self.shard_for_key(key);
        self.shards[shard].set(key, value, version)
    }

    /// Batch writes across shards while preserving per-shard ordering.
    fn set_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<()> {
        if items.is_empty() {
            // Nothing to write, so avoid shard work.
            return Ok(());
        }
        let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, (key, _, _)) in items.iter().enumerate() {
            let shard = self.shard_for_key(key);
            by_shard[shard].push(idx);
        }
        for (shard, indices) in by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                // Skip shards that have no items assigned.
                continue;
            }
            let mut batch = Vec::with_capacity(indices.len());
            for idx in indices {
                let (key, value, version) = items[idx];
                batch.push((key, value, version));
            }
            self.shards[shard].set_batch(&batch)?;
        }
        Ok(())
    }

    fn apply_committed_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<u64> {
        if items.is_empty() {
            return Ok(0);
        }
        if self.shards.len() == 1 {
            return self.shards[0].apply_committed_batch(items);
        }

        let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, (key, _, _)) in items.iter().enumerate() {
            let shard = self.shard_for_key(key);
            by_shard[shard].push(idx);
        }

        let mut inserted = 0u64;
        for (shard, indices) in by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut batch = Vec::with_capacity(indices.len());
            for idx in indices {
                let (key, value, version) = items[idx];
                batch.push((key, value, version));
            }
            inserted = inserted.saturating_add(self.shards[shard].apply_committed_batch(&batch)?);
        }
        Ok(inserted)
    }

    /// Delegate visibility updates to the chosen shard.
    fn mark_visible(&self, key: &[u8], version: Version) -> anyhow::Result<bool> {
        let shard = self.shard_for_key(key);
        self.shards[shard].mark_visible(key, version)
    }

    /// Batch visibility updates across shards.
    ///
    /// Purpose:
    /// - Route visibility updates to owning shards while preserving aggregate
    ///   inserted-latest accounting.
    ///
    /// Design:
    /// - Buckets input indices by shard and forwards borrowed slices, avoiding
    ///   key cloning.
    ///
    /// Inputs:
    /// - `keys`: borrowed key slices.
    /// - `version`: committed version to expose.
    ///
    /// Outputs:
    /// - Total inserted-latest count across all shards.
    fn mark_visible_batch(&self, keys: &[&[u8]], version: Version) -> anyhow::Result<u64> {
        if keys.is_empty() {
            return Ok(0);
        }
        if self.shards.len() == 1 {
            return self.shards[0].mark_visible_batch(keys, version);
        }

        let mut by_shard: Vec<Vec<usize>> = vec![Vec::new(); self.shards.len()];
        for (idx, key) in keys.iter().enumerate() {
            // Bucket indices to avoid cloning keys into per-shard vectors.
            let shard = self.shard_for_key(key);
            by_shard[shard].push(idx);
        }

        let mut inserted = 0u64;
        for (shard, indices) in by_shard.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut shard_keys = Vec::with_capacity(indices.len());
            for idx in indices {
                shard_keys.push(keys[idx]);
            }
            inserted = inserted
                .saturating_add(self.shards[shard].mark_visible_batch(&shard_keys, version)?);
        }
        Ok(inserted)
    }
}

/// Version identifier used for MVCC-style reads in the KV engine.
///
/// `range_generation` is a durable range-ownership epoch. It is bumped by
/// range split/merge/cutover metadata, not by ordinary writes. Ordering by
/// generation first prevents an old range owner replay/apply from superseding a
/// newer owner whose Accord sequence is only group-local.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub range_generation: u64,
    pub seq: u64,
    pub txn_id: TxnId,
}

impl Version {
    /// Zero version used as a sentinel for "no data".
    pub const fn zero() -> Self {
        Self {
            range_generation: 0,
            seq: 0,
            txn_id: TxnId {
                node_id: 0,
                counter: 0,
            },
        }
    }

    /// Return this version with a specific range generation.
    pub const fn with_range_generation(self, range_generation: u64) -> Self {
        Self {
            range_generation,
            seq: self.seq,
            txn_id: self.txn_id,
        }
    }
}

impl From<ExecMeta> for Version {
    /// Convert execution metadata into a KV version.
    fn from(meta: ExecMeta) -> Self {
        Self {
            range_generation: DEFAULT_RANGE_GENERATION,
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
        size += 1 + 8 + 8 + 8 + 8 + 1 + 4 + v.value.len();
    }
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(&(versions.len() as u32).to_be_bytes());
    for v in versions {
        out.push(VERSION_LIST_V2_MARKER);
        out.extend_from_slice(&v.version.range_generation.to_be_bytes());
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
        anyhow::ensure!(
            offset + 8 + 8 + 8 + 1 + 4 <= data.len(),
            "short version header"
        );
        let range_generation = if data[offset] == VERSION_LIST_V2_MARKER {
            offset += 1;
            read_u64(data, &mut offset)?
        } else {
            DEFAULT_RANGE_GENERATION
        };
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
                range_generation,
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
pub(crate) fn encode_key_prefix(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len());
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out
}

/// Encode the composite key used in the versions partition.
pub(crate) fn encode_version_key(key: &[u8], version: Version) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len() + 1 + 8 + 8 + 8 + 8);
    out.extend_from_slice(&(key.len() as u32).to_be_bytes());
    out.extend_from_slice(key);
    out.push(VERSION_KEY_V2_MARKER);
    out.extend_from_slice(&version.range_generation.to_be_bytes());
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
    if offset + key_len > entry_key.len() {
        return None;
    }
    if key_len != key.len() || &entry_key[offset..offset + key_len] != key {
        // Prefix does not match the requested key.
        return None;
    }
    offset += key_len;
    let suffix_len = entry_key.len().saturating_sub(offset);
    let range_generation = match suffix_len {
        24 => DEFAULT_RANGE_GENERATION,
        33 if entry_key[offset] == VERSION_KEY_V2_MARKER => {
            offset += 1;
            read_u64(entry_key, &mut offset).ok()?
        }
        _ => return None,
    };
    let seq = read_u64(entry_key, &mut offset).ok()?;
    let node_id = read_u64(entry_key, &mut offset).ok()?;
    let counter = read_u64(entry_key, &mut offset).ok()?;
    if offset != entry_key.len() {
        return None;
    }
    Some(Version {
        range_generation,
        seq,
        txn_id: TxnId { node_id, counter },
    })
}

/// Encode a versioned value with an explicit visibility flag.
pub(crate) fn encode_version_value(visible: bool, value: &[u8]) -> Vec<u8> {
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
pub(crate) fn encode_latest_value(version: Version, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(LATEST_VALUE_V2_MAGIC.len() + 8 + 8 + 8 + 8 + 4 + value.len());
    out.extend_from_slice(LATEST_VALUE_V2_MAGIC);
    out.extend_from_slice(&version.range_generation.to_be_bytes());
    out.extend_from_slice(&version.seq.to_be_bytes());
    out.extend_from_slice(&version.txn_id.node_id.to_be_bytes());
    out.extend_from_slice(&version.txn_id.counter.to_be_bytes());
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(value);
    out
}

/// Decode the "latest" index value.
pub(crate) fn decode_latest_value(data: &[u8]) -> anyhow::Result<(Version, Vec<u8>)> {
    let mut offset = 0usize;
    let range_generation = if data.starts_with(LATEST_VALUE_V2_MAGIC) {
        offset += LATEST_VALUE_V2_MAGIC.len();
        read_u64(data, &mut offset)?
    } else {
        DEFAULT_RANGE_GENERATION
    };
    let seq = read_u64(data, &mut offset)?;
    let node_id = read_u64(data, &mut offset)?;
    let counter = read_u64(data, &mut offset)?;
    let len = read_u32(data, &mut offset)? as usize;
    anyhow::ensure!(offset + len <= data.len(), "short latest value");
    let value = data[offset..offset + len].to_vec();
    Ok((
        Version {
            range_generation,
            seq,
            txn_id: TxnId { node_id, counter },
        },
        value,
    ))
}

/// Decode only the version prefix from a latest-index value.
fn decode_latest_version(data: &[u8]) -> anyhow::Result<Version> {
    let mut offset = 0usize;
    let range_generation = if data.starts_with(LATEST_VALUE_V2_MAGIC) {
        offset += LATEST_VALUE_V2_MAGIC.len();
        read_u64(data, &mut offset)?
    } else {
        DEFAULT_RANGE_GENERATION
    };
    let seq = read_u64(data, &mut offset)?;
    let node_id = read_u64(data, &mut offset)?;
    let counter = read_u64(data, &mut offset)?;
    let _len = read_u32(data, &mut offset)?;
    Ok(Version {
        range_generation,
        seq,
        txn_id: TxnId { node_id, counter },
    })
}

/// Return whether `candidate` should replace the current latest index version.
fn should_update_latest(current: Option<Version>, candidate: Version) -> bool {
    current.map_or(true, |cur| candidate > cur)
}

/// State machine that ignores commands (used for membership group).
pub struct NoopStateMachine;

impl StateMachine for NoopStateMachine {
    /// No keys are tracked because commands are ignored.
    fn command_keys(&self, _data: &[u8]) -> anyhow::Result<CommandKeys> {
        Ok(CommandKeys::default())
    }

    /// No-op apply for ignored commands.
    fn apply(&self, _data: &[u8], _meta: ExecMeta) -> anyhow::Result<()> {
        Ok(())
    }
}

/// State machine that applies KV commands to a `KvEngine`.
pub struct KvStateMachine {
    kv: Arc<dyn KvEngine>,
    split_lock: Option<Arc<std::sync::RwLock<()>>>,
    membership_hook: Option<Arc<MembershipUpdateHook>>,
    visibility_hook: Option<Arc<VisibilityDeltaHook>>,
}

/// Committed membership update payload replicated through a shard Accord log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipReconfig {
    pub members: Vec<NodeId>,
    pub voters: Vec<NodeId>,
}

/// Callback used by `KvStateMachine` to apply committed membership updates.
pub type MembershipUpdateHook =
    dyn Fn(MembershipReconfig) -> anyhow::Result<()> + Send + Sync + 'static;
/// Callback used by `KvStateMachine` to publish visible-row count deltas.
pub type VisibilityDeltaHook = dyn Fn(i64) + Send + Sync + 'static;

impl KvStateMachine {
    /// Create a new state machine wrapper around a KV engine.
    pub fn new(kv: Arc<dyn KvEngine>, split_lock: Option<Arc<std::sync::RwLock<()>>>) -> Self {
        Self {
            kv,
            split_lock,
            membership_hook: None,
            visibility_hook: None,
        }
    }

    /// Create a new state machine wrapper with an optional membership callback.
    pub fn with_membership_hook(
        kv: Arc<dyn KvEngine>,
        split_lock: Option<Arc<std::sync::RwLock<()>>>,
        membership_hook: Option<Arc<MembershipUpdateHook>>,
        visibility_hook: Option<Arc<VisibilityDeltaHook>>,
    ) -> Self {
        Self {
            kv,
            split_lock,
            membership_hook,
            visibility_hook,
        }
    }

    fn emit_visibility_delta(&self, inserted_latest: u64) {
        let visibility_delta = inserted_latest.min(i64::MAX as u64) as i64;
        if visibility_delta != 0 {
            if let Some(hook) = &self.visibility_hook {
                hook(visibility_delta);
            }
        }
    }
}

impl StateMachine for KvStateMachine {
    /// Parse command bytes to identify read/write keys for dependency tracking.
    fn command_keys(&self, data: &[u8]) -> anyhow::Result<CommandKeys> {
        command_keys(data)
    }

    /// Apply a single command to the KV engine.
    fn apply(&self, data: &[u8], meta: ExecMeta) -> anyhow::Result<()> {
        let _guard = self.split_lock.as_ref().map(|l| l.read().unwrap());
        match decode_membership_reconfig_command(data) {
            Ok(Some(reconfig)) => {
                if let Some(hook) = &self.membership_hook {
                    hook(reconfig)?;
                }
                return Ok(());
            }
            Ok(None) => {}
            Err(err) => {
                return Err(err).context("failed to decode membership reconfiguration command")
            }
        }
        let version = Version::from(meta);
        let writes = decode_write_items(data, version)?;
        if !writes.is_empty() {
            let sets = writes
                .iter()
                .map(|item| (item.key, item.value, item.version))
                .collect::<Vec<_>>();
            let inserted_latest = self.kv.apply_committed_batch(&sets)?;
            self.emit_visibility_delta(inserted_latest);
        }
        Ok(())
    }

    /// Apply a batch of commands, coalescing SETs for efficiency.
    fn apply_batch(&self, items: &[(Bytes, ExecMeta)]) -> anyhow::Result<()> {
        let _guard = self.split_lock.as_ref().map(|l| l.read().unwrap());
        let mut sets: Vec<(&[u8], &[u8], Version)> = Vec::new();
        for (data, meta) in items {
            if data.is_empty() {
                // Ignore empty commands to avoid parsing errors.
                continue;
            }
            let version = Version::from(*meta);
            for item in decode_write_items(data, version)? {
                sets.push((item.key, item.value, item.version));
            }
        }

        if !sets.is_empty() {
            let inserted_latest = self.kv.apply_committed_batch(&sets)?;
            self.emit_visibility_delta(inserted_latest);
        }
        Ok(())
    }

    /// Execute a read command without mutating state.
    fn read(&self, data: &[u8], meta: ExecMeta) -> anyhow::Result<Option<Vec<u8>>> {
        let _guard = self.split_lock.as_ref().map(|l| l.read().unwrap());
        anyhow::ensure!(!data.is_empty(), "empty command");
        let base_version = Version::from(meta);
        match data[0] {
            CMD_BATCH_GET | CMD_BATCH_GET_V2 | CMD_BATCH_GET_V3 => {
                let reads = decode_read_items(data)?;
                let count = reads.len();
                let mut out = Vec::with_capacity(4 + (count * 4));
                out.extend_from_slice(&(count as u32).to_be_bytes());
                for item in reads {
                    let read_version = item
                        .range_generation
                        .map(|generation| base_version.with_range_generation(generation))
                        .unwrap_or(base_version);
                    let value = self.kv.get(item.key, read_version);
                    match value {
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
                Ok(self.kv.get(key, base_version))
            }
            CMD_MEMBERSHIP_RECONFIG => Ok(None),
            // Non-read commands are ignored by the read path.
            _ => Ok(None),
        }
    }

    /// Mark all written keys in a command as visible.
    ///
    /// Purpose:
    /// - Publish committed write versions so reads can observe them.
    ///
    /// Design:
    /// - Parses command keys once, then passes borrowed key slices into the
    ///   engine batch-visibility API.
    ///
    /// Inputs:
    /// - `data`: encoded committed command.
    /// - `meta`: execution metadata providing committed version.
    ///
    /// Outputs:
    /// - Updates engine visibility state and emits optional visibility delta.
    fn mark_visible(&self, data: &[u8], meta: ExecMeta) -> anyhow::Result<()> {
        let _guard = self.split_lock.as_ref().map(|l| l.read().unwrap());
        if data.is_empty() {
            // Ignore empty commands to avoid parsing errors.
            return Ok(());
        }
        let version = Version::from(meta);
        let writes = decode_write_items(data, version)
            .context("failed to parse command keys for mark_visible")?;
        let mut grouped: Vec<(Version, Vec<&[u8]>)> = Vec::new();
        for item in writes {
            if let Some((_, keys)) = grouped
                .iter_mut()
                .find(|(group_version, _)| *group_version == item.version)
            {
                keys.push(item.key);
            } else {
                grouped.push((item.version, vec![item.key]));
            }
        }

        let mut visibility_delta = 0i64;
        for (version, keys) in grouped {
            let inserted = self
                .kv
                .mark_visible_batch(&keys, version)?
                .min(i64::MAX as u64) as i64;
            visibility_delta = visibility_delta.saturating_add(inserted);
        }
        if visibility_delta != 0 {
            if let Some(hook) = &self.visibility_hook {
                hook(visibility_delta);
            }
        }
        Ok(())
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
/// Internal command tag for committed shard-membership reconfiguration.
const CMD_MEMBERSHIP_RECONFIG: u8 = 5;
/// Command tag for a single SET carrying an explicit range generation.
const CMD_SET_V2: u8 = 6;
/// Command tag for a multi-key SET batch carrying per-key range generations.
const CMD_BATCH_SET_V2: u8 = 7;
/// Command tag for a multi-key GET batch carrying per-key range generations.
const CMD_BATCH_GET_V2: u8 = 8;
/// Command tag for a multi-key SET batch carrying one shared range generation.
const CMD_BATCH_SET_V3: u8 = 9;
/// Command tag for a multi-key GET batch carrying one shared range generation.
const CMD_BATCH_GET_V3: u8 = 10;

struct DecodedWrite<'a> {
    key: &'a [u8],
    value: &'a [u8],
    version: Version,
}

struct DecodedRead<'a> {
    key: &'a [u8],
    range_generation: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchGenerationShape {
    Default,
    Shared(u64),
    Mixed,
}

fn observe_batch_generation(first: &mut Option<u64>, mixed: &mut bool, generation: u64) {
    if *mixed {
        return;
    }
    match *first {
        Some(first_generation) if first_generation != generation => {
            *mixed = true;
        }
        Some(_) => {}
        None => {
            *first = Some(generation);
        }
    }
}

fn batch_generation_shape(first: Option<u64>, mixed: bool) -> BatchGenerationShape {
    if mixed {
        return BatchGenerationShape::Mixed;
    }
    match first {
        Some(DEFAULT_RANGE_GENERATION) | None => BatchGenerationShape::Default,
        Some(generation) => BatchGenerationShape::Shared(generation),
    }
}

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

/// Encode a single-key SET command with an explicit range generation.
pub fn encode_set_with_generation(key: &[u8], value: &[u8], range_generation: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 8 + 4 + key.len() + 4 + value.len());
    out.push(CMD_SET_V2);
    out.extend_from_slice(&range_generation.to_be_bytes());
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

/// Encode a batch SET command with per-key range generations.
///
/// Purpose:
/// - Persist range ownership generation in the committed command bytes so WAL
///   replay reuses the original cutover epoch instead of consulting whatever
///   descriptor is current at replay time.
pub fn encode_batch_set_with_generations(items: &[(Vec<u8>, Vec<u8>, u64)]) -> Vec<u8> {
    let mut first_generation = None;
    let mut mixed_generations = false;
    let mut compact_size = 1 + 4;
    let mut shared_size = 1 + 8 + 4;
    let mut per_key_size = 1 + 4;
    for (key, value, generation) in items {
        let item_size = 4 + key.len() + 4 + value.len();
        compact_size += item_size;
        shared_size += item_size;
        per_key_size += 8 + item_size;
        observe_batch_generation(&mut first_generation, &mut mixed_generations, *generation);
    }

    match batch_generation_shape(first_generation, mixed_generations) {
        BatchGenerationShape::Default => encode_batch_set_versioned_default(items, compact_size),
        BatchGenerationShape::Shared(shared_generation) => {
            encode_batch_set_with_shared_generation(items, shared_generation, shared_size)
        }
        BatchGenerationShape::Mixed => {
            encode_batch_set_with_per_key_generations(items, per_key_size)
        }
    }
}

fn encode_batch_set_versioned_default(items: &[(Vec<u8>, Vec<u8>, u64)], size: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    out.push(CMD_BATCH_SET);
    out.extend_from_slice(&(items.len() as u32).to_be_bytes());
    for (k, v, _) in items {
        out.extend_from_slice(&(k.len() as u32).to_be_bytes());
        out.extend_from_slice(k);
        out.extend_from_slice(&(v.len() as u32).to_be_bytes());
        out.extend_from_slice(v);
    }
    out
}

fn encode_batch_set_with_shared_generation(
    items: &[(Vec<u8>, Vec<u8>, u64)],
    range_generation: u64,
    size: usize,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    out.push(CMD_BATCH_SET_V3);
    out.extend_from_slice(&range_generation.to_be_bytes());
    out.extend_from_slice(&(items.len() as u32).to_be_bytes());
    for (k, v, _) in items {
        out.extend_from_slice(&(k.len() as u32).to_be_bytes());
        out.extend_from_slice(k);
        out.extend_from_slice(&(v.len() as u32).to_be_bytes());
        out.extend_from_slice(v);
    }
    out
}

fn encode_batch_set_with_per_key_generations(
    items: &[(Vec<u8>, Vec<u8>, u64)],
    size: usize,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    out.push(CMD_BATCH_SET_V2);
    out.extend_from_slice(&(items.len() as u32).to_be_bytes());
    for (k, v, range_generation) in items {
        out.extend_from_slice(&range_generation.to_be_bytes());
        out.extend_from_slice(&(k.len() as u32).to_be_bytes());
        out.extend_from_slice(k);
        out.extend_from_slice(&(v.len() as u32).to_be_bytes());
        out.extend_from_slice(v);
    }
    out
}

/// Encode a batch GET command with multiple keys.
pub fn encode_batch_get(keys: &[Vec<u8>]) -> Vec<u8> {
    let key_refs = keys.iter().map(|key| key.as_slice()).collect::<Vec<_>>();
    encode_batch_get_slices(&key_refs)
}

/// Encode a batch GET command from borrowed key slices.
pub fn encode_batch_get_slices(keys: &[&[u8]]) -> Vec<u8> {
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

/// Encode a batch GET command with per-key routed range generations.
///
/// The generation preserves the logical route used by the coordinator so
/// candidate-read compatibility checks can reason about ownership transitions.
/// The current byte-returning read still returns the latest visible value, just
/// like legacy batch GET, because group-local read sequence numbers are not a
/// safe global cap while ranges can move between Accord groups.
pub fn encode_batch_get_slices_with_generations(keys: &[(&[u8], u64)]) -> Vec<u8> {
    let mut first_generation = None;
    let mut mixed_generations = false;
    let mut compact_size = 1 + 4;
    let mut shared_size = 1 + 8 + 4;
    let mut per_key_size = 1 + 4;
    for (key, generation) in keys {
        let item_size = 4 + key.len();
        compact_size += item_size;
        shared_size += item_size;
        per_key_size += 8 + item_size;
        observe_batch_generation(&mut first_generation, &mut mixed_generations, *generation);
    }

    match batch_generation_shape(first_generation, mixed_generations) {
        BatchGenerationShape::Default => {
            encode_batch_get_generation_items_default(keys, compact_size)
        }
        BatchGenerationShape::Shared(shared_generation) => {
            encode_batch_get_generation_items_shared(keys, shared_generation, shared_size)
        }
        BatchGenerationShape::Mixed => {
            encode_batch_get_slices_with_per_key_generations(keys, per_key_size)
        }
    }
}

/// Encode a batch GET command where all keys share one routed generation.
///
/// Purpose:
/// - Preserve generation-capped reads for stable non-default ranges without
///   paying an 8-byte generation field per key.
///
/// Inputs:
/// - Borrowed keys routed to one logical generation.
/// - `range_generation`: generation captured from the routing snapshot.
///
/// Outputs:
/// - A compact internal read command decoded as a generation-aware batch GET.
pub fn encode_batch_get_slices_with_shared_generation(
    keys: &[&[u8]],
    range_generation: u64,
) -> Vec<u8> {
    if range_generation == DEFAULT_RANGE_GENERATION {
        return encode_batch_get_slices(keys);
    }
    let mut size = 1 + 8 + 4;
    for key in keys {
        size += 4 + key.len();
    }

    encode_batch_get_slices_shared(keys, range_generation, size)
}

fn encode_batch_get_slices_shared(keys: &[&[u8]], range_generation: u64, size: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    out.push(CMD_BATCH_GET_V3);
    out.extend_from_slice(&range_generation.to_be_bytes());
    out.extend_from_slice(&(keys.len() as u32).to_be_bytes());
    for key in keys {
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
    }
    out
}

fn encode_batch_get_generation_items_default(keys: &[(&[u8], u64)], size: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    out.push(CMD_BATCH_GET);
    out.extend_from_slice(&(keys.len() as u32).to_be_bytes());
    for (key, _) in keys {
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
    }
    out
}

fn encode_batch_get_generation_items_shared(
    keys: &[(&[u8], u64)],
    range_generation: u64,
    size: usize,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    out.push(CMD_BATCH_GET_V3);
    out.extend_from_slice(&range_generation.to_be_bytes());
    out.extend_from_slice(&(keys.len() as u32).to_be_bytes());
    for (key, _) in keys {
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
    }
    out
}

fn encode_batch_get_slices_with_per_key_generations(keys: &[(&[u8], u64)], size: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(size);
    out.push(CMD_BATCH_GET_V2);
    out.extend_from_slice(&(keys.len() as u32).to_be_bytes());
    for (key, range_generation) in keys {
        out.extend_from_slice(&range_generation.to_be_bytes());
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
    }
    out
}

/// Encode an internal command that updates shard runtime membership after commit.
///
/// This command is only issued by the rebalancer/controller; clients never send it.
pub fn encode_membership_reconfig(members: &[NodeId], voters: &[NodeId]) -> Vec<u8> {
    let mut members = members.to_vec();
    members.sort_unstable();
    members.dedup();

    let mut voters = voters.to_vec();
    voters.sort_unstable();
    voters.dedup();

    let mut out = Vec::with_capacity(1 + 4 + members.len() * 8 + 4 + voters.len() * 8);
    out.push(CMD_MEMBERSHIP_RECONFIG);
    out.extend_from_slice(&(members.len() as u32).to_be_bytes());
    for id in members {
        out.extend_from_slice(&id.to_be_bytes());
    }
    out.extend_from_slice(&(voters.len() as u32).to_be_bytes());
    for id in voters {
        out.extend_from_slice(&id.to_be_bytes());
    }
    out
}

fn decode_membership_reconfig_command(data: &[u8]) -> anyhow::Result<Option<MembershipReconfig>> {
    if data.is_empty() {
        anyhow::bail!("empty command");
    }
    if data[0] != CMD_MEMBERSHIP_RECONFIG {
        return Ok(None);
    }
    let mut offset = 1usize;
    let members_len = read_u32(data, &mut offset)? as usize;
    let mut members = Vec::with_capacity(members_len);
    for _ in 0..members_len {
        members.push(read_u64(data, &mut offset)?);
    }
    let voters_len = read_u32(data, &mut offset)? as usize;
    let mut voters = Vec::with_capacity(voters_len);
    for _ in 0..voters_len {
        voters.push(read_u64(data, &mut offset)?);
    }
    anyhow::ensure!(
        voters_len > 0,
        "membership reconfiguration voter set cannot be empty"
    );
    Ok(Some(MembershipReconfig { members, voters }))
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

/// Decode write commands into borrowed key/value slices and stable versions.
///
/// Legacy commands inherit `base_version`'s default generation. Generation-aware
/// commands override only `range_generation`, keeping Accord's committed
/// `seq/txn_id` from the execution metadata.
fn decode_write_items<'a>(
    data: &'a [u8],
    base_version: Version,
) -> anyhow::Result<Vec<DecodedWrite<'a>>> {
    anyhow::ensure!(!data.is_empty(), "empty command");
    match data[0] {
        CMD_SET => {
            let mut offset = 1;
            let key_len = read_u32(data, &mut offset)? as usize;
            anyhow::ensure!(offset + key_len <= data.len(), "short key");
            let key = &data[offset..offset + key_len];
            offset += key_len;

            let value_len = read_u32(data, &mut offset)? as usize;
            anyhow::ensure!(offset + value_len <= data.len(), "short value");
            let value = &data[offset..offset + value_len];

            Ok(vec![DecodedWrite {
                key,
                value,
                version: base_version,
            }])
        }
        CMD_SET_V2 => {
            let mut offset = 1;
            let range_generation = read_u64(data, &mut offset)?;
            let key_len = read_u32(data, &mut offset)? as usize;
            anyhow::ensure!(offset + key_len <= data.len(), "short key");
            let key = &data[offset..offset + key_len];
            offset += key_len;

            let value_len = read_u32(data, &mut offset)? as usize;
            anyhow::ensure!(offset + value_len <= data.len(), "short value");
            let value = &data[offset..offset + value_len];

            Ok(vec![DecodedWrite {
                key,
                value,
                version: base_version.with_range_generation(range_generation),
            }])
        }
        CMD_BATCH_SET => {
            let mut offset = 1;
            let count = read_u32(data, &mut offset)? as usize;
            let mut sets = Vec::with_capacity(count);
            for _ in 0..count {
                let key_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + key_len <= data.len(), "short key");
                let key = &data[offset..offset + key_len];
                offset += key_len;

                let value_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + value_len <= data.len(), "short value");
                let value = &data[offset..offset + value_len];
                offset += value_len;

                sets.push(DecodedWrite {
                    key,
                    value,
                    version: base_version,
                });
            }
            Ok(sets)
        }
        CMD_BATCH_SET_V2 => {
            let mut offset = 1;
            let count = read_u32(data, &mut offset)? as usize;
            let mut sets = Vec::with_capacity(count);
            for _ in 0..count {
                let range_generation = read_u64(data, &mut offset)?;
                let key_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + key_len <= data.len(), "short key");
                let key = &data[offset..offset + key_len];
                offset += key_len;

                let value_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + value_len <= data.len(), "short value");
                let value = &data[offset..offset + value_len];
                offset += value_len;

                sets.push(DecodedWrite {
                    key,
                    value,
                    version: base_version.with_range_generation(range_generation),
                });
            }
            Ok(sets)
        }
        CMD_BATCH_SET_V3 => {
            let mut offset = 1;
            let range_generation = read_u64(data, &mut offset)?;
            let version = base_version.with_range_generation(range_generation);
            let count = read_u32(data, &mut offset)? as usize;
            let mut sets = Vec::with_capacity(count);
            for _ in 0..count {
                let key_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + key_len <= data.len(), "short key");
                let key = &data[offset..offset + key_len];
                offset += key_len;

                let value_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + value_len <= data.len(), "short value");
                let value = &data[offset..offset + value_len];
                offset += value_len;

                sets.push(DecodedWrite {
                    key,
                    value,
                    version,
                });
            }
            Ok(sets)
        }
        _ => Ok(Vec::new()),
    }
}

fn decode_read_items(data: &[u8]) -> anyhow::Result<Vec<DecodedRead<'_>>> {
    anyhow::ensure!(!data.is_empty(), "empty command");
    match data[0] {
        CMD_BATCH_GET | CMD_BATCH_GET_V2 => {
            let mut offset = 1;
            let count = read_u32(data, &mut offset)? as usize;
            let mut reads = Vec::with_capacity(count);
            for _ in 0..count {
                let range_generation = if data[0] == CMD_BATCH_GET_V2 {
                    Some(read_u64(data, &mut offset)?)
                } else {
                    None
                };
                let key_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + key_len <= data.len(), "short key");
                let key = &data[offset..offset + key_len];
                offset += key_len;
                reads.push(DecodedRead {
                    key,
                    range_generation,
                });
            }
            Ok(reads)
        }
        CMD_BATCH_GET_V3 => {
            let mut offset = 1;
            let range_generation = Some(read_u64(data, &mut offset)?);
            let count = read_u32(data, &mut offset)? as usize;
            let mut reads = Vec::with_capacity(count);
            for _ in 0..count {
                let key_len = read_u32(data, &mut offset)? as usize;
                anyhow::ensure!(offset + key_len <= data.len(), "short key");
                let key = &data[offset..offset + key_len];
                offset += key_len;
                reads.push(DecodedRead {
                    key,
                    range_generation,
                });
            }
            Ok(reads)
        }
        CMD_GET => {
            let mut offset = 1;
            let key_len = read_u32(data, &mut offset)? as usize;
            anyhow::ensure!(offset + key_len <= data.len(), "short key");
            Ok(vec![DecodedRead {
                key: &data[offset..offset + key_len],
                range_generation: None,
            }])
        }
        _ => Ok(Vec::new()),
    }
}

/// Extract the read/write key sets from a command for dependency tracking.
fn command_keys(data: &[u8]) -> anyhow::Result<CommandKeys> {
    anyhow::ensure!(!data.is_empty(), "empty command");
    match data[0] {
        CMD_SET | CMD_SET_V2 => {
            let mut offset = 1;
            if data[0] == CMD_SET_V2 {
                let _range_generation = read_u64(data, &mut offset)?;
            }
            let key_len = read_u32(data, &mut offset)? as usize;
            anyhow::ensure!(offset + key_len <= data.len(), "short key");
            let key = data[offset..offset + key_len].to_vec();
            Ok(CommandKeys {
                reads: Vec::new(),
                writes: vec![key],
            })
        }
        CMD_BATCH_SET | CMD_BATCH_SET_V2 | CMD_BATCH_SET_V3 => {
            let mut offset = 1;
            if data[0] == CMD_BATCH_SET_V3 {
                let _range_generation = read_u64(data, &mut offset)?;
            }
            let count = read_u32(data, &mut offset)? as usize;
            let mut writes = Vec::with_capacity(count);
            for _ in 0..count {
                if data[0] == CMD_BATCH_SET_V2 {
                    let _range_generation = read_u64(data, &mut offset)?;
                }
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
        CMD_BATCH_GET | CMD_BATCH_GET_V2 | CMD_BATCH_GET_V3 => {
            let mut offset = 1;
            if data[0] == CMD_BATCH_GET_V3 {
                let _range_generation = read_u64(data, &mut offset)?;
            }
            let count = read_u32(data, &mut offset)? as usize;
            let mut reads = Vec::with_capacity(count);
            for _ in 0..count {
                if data[0] == CMD_BATCH_GET_V2 {
                    let _range_generation = read_u64(data, &mut offset)?;
                }
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
        CMD_MEMBERSHIP_RECONFIG => Ok(CommandKeys {
            reads: Vec::new(),
            writes: Vec::new(),
        }),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    use holo_accord::accord::StateMachine;

    /// Build a unique temporary directory for a KV storage test.
    ///
    /// Purpose:
    /// - Keep Fjall test keyspaces isolated from each other.
    ///
    /// Design:
    /// - Uses process id plus nanosecond timestamp under the OS temp directory.
    ///
    /// Inputs:
    /// - `name`: human-readable test name suffix.
    ///
    /// Outputs:
    /// - Created temporary directory path.
    fn temp_dir(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "holo_store_kv_{name}_{}_{}",
            std::process::id(),
            nanos
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    /// Open a temporary Fjall keyspace for a KV storage test.
    ///
    /// Purpose:
    /// - Provide isolated Fjall state for direct and worker publish tests.
    ///
    /// Design:
    /// - Creates a unique temp directory and opens a normal Fjall keyspace.
    ///
    /// Inputs:
    /// - `name`: human-readable test name suffix.
    ///
    /// Outputs:
    /// - Temporary directory path kept alive by the caller and keyspace handle.
    fn open_test_keyspace(name: &str) -> (std::path::PathBuf, Arc<Keyspace>) {
        let dir = temp_dir(name);
        let keyspace = Arc::new(
            fjall::Config::new(&dir)
                .open()
                .expect("open temporary keyspace"),
        );
        (dir, keyspace)
    }

    /// Build a deterministic test MVCC version.
    ///
    /// Purpose:
    /// - Keep publish tests readable while constructing comparable versions.
    ///
    /// Design:
    /// - Uses node id 1 and varies the sequence/counter supplied by the test.
    ///
    /// Inputs:
    /// - `seq`: committed sequence number.
    /// - `counter`: transaction counter.
    ///
    /// Outputs:
    /// - A `Version` value.
    fn test_version(seq: u64, counter: u64) -> Version {
        Version {
            range_generation: DEFAULT_RANGE_GENERATION,
            seq,
            txn_id: TxnId {
                node_id: 1,
                counter,
            },
        }
    }

    /// Build a deterministic test MVCC version whose transaction id encodes
    /// a specific Accord group id.
    fn test_group_version(group_id: u64, seq: u64, counter_seq: u64) -> Version {
        Version {
            range_generation: group_id,
            seq,
            txn_id: TxnId {
                node_id: 1,
                counter: holo_accord::accord::make_txn_counter(group_id, 1, counter_seq)
                    .expect("test txn counter"),
            },
        }
    }

    /// Build execution metadata whose transaction id encodes an Accord group.
    fn test_group_meta(group_id: u64, seq: u64, counter_seq: u64) -> ExecMeta {
        ExecMeta {
            seq,
            txn_id: TxnId {
                node_id: 1,
                counter: holo_accord::accord::make_txn_counter(group_id, 1, counter_seq)
                    .expect("test txn counter"),
            },
        }
    }

    struct OverlayRouter;

    impl ShardRouter for OverlayRouter {
        fn shard_for_key(&self, key: &[u8]) -> usize {
            if key >= b"m".as_slice() {
                1
            } else {
                0
            }
        }

        fn fallback_shard_for_key(&self, key: &[u8]) -> Option<usize> {
            (key >= b"m".as_slice()).then_some(0)
        }
    }

    #[test]
    fn routed_kv_engine_reads_fallback_and_mirrors_writes() {
        let parent = Arc::new(KvStore::new());
        let child = Arc::new(KvStore::new());
        let old_version = test_version(1, 1);
        parent
            .apply_committed_batch(&[(b"n".as_slice(), b"old".as_slice(), old_version)])
            .expect("seed fallback parent");
        let engine =
            RoutedKvEngine::new(vec![parent.clone(), child.clone()], Arc::new(OverlayRouter))
                .expect("routed engine");

        assert_eq!(
            engine.get_latest(b"n"),
            Some((b"old".to_vec(), old_version))
        );
        assert_eq!(
            engine.get_latest_batch(&[b"n".as_slice()]),
            vec![Some((b"old".to_vec(), old_version))]
        );

        let new_version = test_version(2, 2);
        engine
            .apply_committed_batch(&[(b"n".as_slice(), b"new".as_slice(), new_version)])
            .expect("write child primary and fallback");

        assert_eq!(child.get_latest(b"n"), Some((b"new".to_vec(), new_version)));
        assert_eq!(
            parent.get_latest(b"n"),
            Some((b"new".to_vec(), new_version))
        );
        assert_eq!(
            engine.get_latest(b"n"),
            Some((b"new".to_vec(), new_version))
        );
    }

    #[test]
    fn routed_kv_engine_picks_latest_across_primary_and_fallback() {
        let parent = Arc::new(KvStore::new());
        let child = Arc::new(KvStore::new());
        let fallback_newer = test_version(10, 10);
        let primary_older = test_version(2, 2);
        parent
            .apply_committed_batch(&[(
                b"n".as_slice(),
                b"fallback-newer".as_slice(),
                fallback_newer,
            )])
            .expect("seed fallback parent");
        child
            .apply_committed_batch(&[(b"n".as_slice(), b"primary-older".as_slice(), primary_older)])
            .expect("seed child primary");
        let engine =
            RoutedKvEngine::new(vec![parent.clone(), child.clone()], Arc::new(OverlayRouter))
                .expect("routed engine");

        assert_eq!(
            engine.get_latest(b"n"),
            Some((b"fallback-newer".to_vec(), fallback_newer))
        );
        assert_eq!(
            engine.get_latest_batch(&[b"n".as_slice()]),
            vec![Some((b"fallback-newer".to_vec(), fallback_newer))]
        );
        assert_eq!(
            engine.get(b"n", test_version(20, 20)),
            Some(b"fallback-newer".to_vec())
        );
    }

    #[test]
    fn latest_rejects_late_parent_group_write_after_child_owner_write() {
        let engine = KvStore::new();
        let child_version = test_group_version(2, 1, 1);
        let parent_version = test_group_version(1, 999, 999);

        engine
            .apply_committed_batch(&[(
                b"split-key".as_slice(),
                b"child-new".as_slice(),
                child_version,
            )])
            .expect("child owner write should apply");
        engine
            .apply_committed_batch(&[(
                b"split-key".as_slice(),
                b"parent-old".as_slice(),
                parent_version,
            )])
            .expect("late parent write should apply as historical version");

        assert_eq!(
            engine.get_latest(b"split-key"),
            Some((b"child-new".to_vec(), child_version))
        );
    }

    #[test]
    fn state_machine_replay_uses_command_range_generation_for_latest_ordering() {
        let engine = Arc::new(KvStore::new());
        let sm = KvStateMachine::new(engine.clone(), None);
        let child_cmd =
            encode_batch_set_with_generations(&[(b"split-key".to_vec(), b"child-new".to_vec(), 2)]);
        let parent_cmd = encode_batch_set_with_generations(&[(
            b"split-key".to_vec(),
            b"parent-old".to_vec(),
            1,
        )]);

        sm.apply(&child_cmd, test_group_meta(2, 1, 1))
            .expect("child owner command should apply");
        sm.mark_visible(&child_cmd, test_group_meta(2, 1, 1))
            .expect("child owner command should become visible");
        sm.apply(&parent_cmd, test_group_meta(1, 999, 999))
            .expect("late parent owner command should apply as history");
        sm.mark_visible(&parent_cmd, test_group_meta(1, 999, 999))
            .expect("late parent owner command should become visible");

        assert_eq!(
            engine.get_latest(b"split-key"),
            Some((b"child-new".to_vec(), test_group_version(2, 1, 1)))
        );
    }

    #[test]
    fn default_generation_batch_set_uses_legacy_command() {
        let cmd = encode_batch_set_with_generations(&[
            (b"a".to_vec(), b"one".to_vec(), DEFAULT_RANGE_GENERATION),
            (b"b".to_vec(), b"two".to_vec(), DEFAULT_RANGE_GENERATION),
        ]);

        assert_eq!(cmd[0], CMD_BATCH_SET);
        let writes = decode_write_items(&cmd, test_version(42, 42))
            .expect("default-generation batch set should decode");
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].version.range_generation, DEFAULT_RANGE_GENERATION);
        assert_eq!(writes[1].version.range_generation, DEFAULT_RANGE_GENERATION);
    }

    #[test]
    fn shared_generation_batch_set_replays_with_command_generation() {
        let engine = Arc::new(KvStore::new());
        let sm = KvStateMachine::new(engine.clone(), None);
        let cmd = encode_batch_set_with_generations(&[
            (b"a".to_vec(), b"one".to_vec(), 7),
            (b"b".to_vec(), b"two".to_vec(), 7),
        ]);

        assert_eq!(cmd[0], CMD_BATCH_SET_V3);
        sm.apply(&cmd, test_group_meta(7, 3, 3))
            .expect("shared-generation batch should apply");
        sm.mark_visible(&cmd, test_group_meta(7, 3, 3))
            .expect("shared-generation batch should publish");

        assert_eq!(
            engine.get_latest(b"a").map(|(_, v)| v.range_generation),
            Some(7)
        );
        assert_eq!(
            engine.get_latest(b"b").map(|(_, v)| v.range_generation),
            Some(7)
        );
    }

    #[test]
    fn mixed_generation_batch_set_uses_per_key_command() {
        let cmd = encode_batch_set_with_generations(&[
            (b"a".to_vec(), b"one".to_vec(), DEFAULT_RANGE_GENERATION),
            (b"b".to_vec(), b"two".to_vec(), 2),
        ]);

        assert_eq!(cmd[0], CMD_BATCH_SET_V2);
        let writes = decode_write_items(&cmd, test_version(42, 42))
            .expect("mixed-generation batch set should decode");
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].version.range_generation, DEFAULT_RANGE_GENERATION);
        assert_eq!(writes[1].version.range_generation, 2);
    }

    #[test]
    fn committed_write_value_decodes_generation_aware_batch() {
        let cmd = encode_batch_set_with_generations(&[
            (b"a".to_vec(), b"one".to_vec(), 7),
            (b"b".to_vec(), b"two".to_vec(), 7),
        ]);
        let value = committed_write_value_for_key(&cmd, test_group_meta(7, 5, 5), b"b")
            .expect("committed decode should succeed")
            .expect("key should be present");

        assert_eq!(value.0, b"two".to_vec());
        assert_eq!(value.1, test_group_version(7, 5, 5));
        assert!(
            committed_write_value_for_key(&cmd, test_group_meta(7, 5, 5), b"missing")
                .expect("committed decode should succeed")
                .is_none()
        );
    }

    #[test]
    fn committed_write_values_preserve_read_order_and_duplicates() {
        let cmd = encode_batch_set_with_generations(&[
            (b"a".to_vec(), b"one".to_vec(), 4),
            (b"b".to_vec(), b"two".to_vec(), 4),
            (b"a".to_vec(), b"three".to_vec(), 5),
        ]);
        let values = committed_write_values_for_keys(
            &cmd,
            test_group_meta(4, 9, 9),
            &[
                b"b".as_slice(),
                b"missing".as_slice(),
                b"a".as_slice(),
                b"a".as_slice(),
            ],
        )
        .expect("committed decode should succeed");

        assert_eq!(
            values[0].as_ref().map(|(value, _)| value.as_slice()),
            Some(b"two".as_slice())
        );
        assert!(values[1].is_none());
        assert_eq!(
            values[2].as_ref().map(|(value, _)| value.as_slice()),
            Some(b"three".as_slice())
        );
        assert_eq!(values[2], values[3]);
        assert_eq!(
            values[2]
                .as_ref()
                .map(|(_, version)| version.range_generation),
            Some(5)
        );
    }

    #[test]
    fn default_generation_batch_get_uses_legacy_command() {
        let payload = encode_batch_get_slices_with_generations(&[
            (b"a".as_slice(), DEFAULT_RANGE_GENERATION),
            (b"b".as_slice(), DEFAULT_RANGE_GENERATION),
        ]);

        assert_eq!(payload[0], CMD_BATCH_GET);
        let reads =
            decode_read_items(&payload).expect("default-generation batch get should decode");
        assert_eq!(reads.len(), 2);
        assert!(reads.iter().all(|read| read.range_generation.is_none()));
    }

    #[test]
    fn mixed_generation_batch_get_uses_per_key_command() {
        let payload = encode_batch_get_slices_with_generations(&[
            (b"a".as_slice(), DEFAULT_RANGE_GENERATION),
            (b"b".as_slice(), 2),
        ]);

        assert_eq!(payload[0], CMD_BATCH_GET_V2);
        let reads = decode_read_items(&payload).expect("mixed-generation batch get should decode");
        assert_eq!(reads.len(), 2);
        assert_eq!(reads[0].range_generation, Some(DEFAULT_RANGE_GENERATION));
        assert_eq!(reads[1].range_generation, Some(2));
    }

    #[test]
    fn batch_get_v2_is_sequence_bounded_within_routed_generation() {
        let engine = Arc::new(KvStore::new());
        let sm = KvStateMachine::new(engine.clone(), None);
        let old_version = test_version(10, 10);
        let new_version = test_version(50, 50);

        engine
            .apply_committed_batch(&[(b"k".as_slice(), b"old".as_slice(), old_version)])
            .expect("seed old visible value");
        engine
            .apply_committed_batch(&[(b"k".as_slice(), b"new".as_slice(), new_version)])
            .expect("seed new visible value");

        let payload = encode_batch_get_slices_with_generations(&[(
            b"k".as_slice(),
            DEFAULT_RANGE_GENERATION,
        )]);
        let result = sm
            .read(
                &payload,
                ExecMeta {
                    seq: 20,
                    txn_id: TxnId {
                        node_id: 1,
                        counter: 20,
                    },
                },
            )
            .expect("batch get v2 should read")
            .expect("batch get v2 should return bytes");

        assert_eq!(
            decode_batch_get_result(&result).expect("decode result"),
            vec![Some(b"old".to_vec())]
        );
    }

    #[test]
    fn shared_generation_batch_get_is_read_only_and_generation_capped() {
        let engine = Arc::new(KvStore::new());
        let sm = KvStateMachine::new(engine.clone(), None);
        let visible = Version {
            range_generation: 9,
            seq: 5,
            txn_id: TxnId {
                node_id: 1,
                counter: 5,
            },
        };
        engine
            .apply_committed_batch(&[(b"k".as_slice(), b"visible".as_slice(), visible)])
            .expect("seed shared-generation value");

        let payload = encode_batch_get_slices_with_shared_generation(&[b"k".as_slice()], 9);
        assert_eq!(payload[0], CMD_BATCH_GET_V3);
        let keys = command_keys(&payload).expect("command keys");
        assert_eq!(keys.reads, vec![b"k".to_vec()]);
        assert!(keys.writes.is_empty());

        let result = sm
            .read(
                &payload,
                ExecMeta {
                    seq: 10,
                    txn_id: TxnId {
                        node_id: 1,
                        counter: 10,
                    },
                },
            )
            .expect("shared-generation batch get should read")
            .expect("shared-generation batch get should return bytes");
        assert_eq!(
            decode_batch_get_result(&result).expect("decode result"),
            vec![Some(b"visible".to_vec())]
        );
    }

    #[test]
    fn batch_get_v2_can_fall_back_to_older_generation_history() {
        let engine = Arc::new(KvStore::new());
        let sm = KvStateMachine::new(engine.clone(), None);
        let old_owner_version = Version {
            range_generation: 1,
            seq: 999,
            txn_id: TxnId {
                node_id: 1,
                counter: 999,
            },
        };
        let future_child_version = Version {
            range_generation: 2,
            seq: 50,
            txn_id: TxnId {
                node_id: 1,
                counter: 50,
            },
        };

        engine
            .apply_committed_batch(&[(
                b"k".as_slice(),
                b"parent-visible".as_slice(),
                old_owner_version,
            )])
            .expect("seed old-owner visible value");
        engine
            .apply_committed_batch(&[(
                b"k".as_slice(),
                b"future-child".as_slice(),
                future_child_version,
            )])
            .expect("seed future child visible value");

        let payload = encode_batch_get_slices_with_generations(&[(b"k".as_slice(), 2)]);
        let result = sm
            .read(
                &payload,
                ExecMeta {
                    seq: 20,
                    txn_id: TxnId {
                        node_id: 1,
                        counter: 20,
                    },
                },
            )
            .expect("batch get v2 should read")
            .expect("batch get v2 should return bytes");

        assert_eq!(
            decode_batch_get_result(&result).expect("decode result"),
            vec![Some(b"parent-visible".to_vec())]
        );
    }

    /// Build a worker request for direct combined-publish tests.
    ///
    /// Purpose:
    /// - Exercise `publish_fjall_requests` without relying on scheduler timing.
    ///
    /// Design:
    /// - Reuses the engine partition handles and supplies a reply channel that
    ///   direct helper tests do not consume.
    ///
    /// Inputs:
    /// - `engine`: target shard engine.
    /// - `items`: owned committed writes for this request.
    ///
    /// Outputs:
    /// - A `FjallPublishRequest` suitable for `publish_fjall_requests`.
    fn test_publish_request(
        engine: &FjallEngine,
        items: Vec<OwnedCommittedWrite>,
    ) -> FjallPublishRequest {
        let bytes = items
            .iter()
            .map(|item| item.key.len().saturating_add(item.value.len()))
            .sum();
        let (done, _rx) = std::sync::mpsc::channel();
        FjallPublishRequest {
            engine_id: engine.engine_id,
            versions: engine.versions.clone(),
            latest: engine.latest.clone(),
            lock: engine.lock.clone(),
            items,
            bytes,
            done,
        }
    }

    #[test]
    fn membership_reconfig_roundtrip() {
        let payload = encode_membership_reconfig(&[3, 1, 2, 2], &[3, 1, 1]);
        let parsed = decode_membership_reconfig_command(&payload)
            .expect("decode")
            .expect("membership payload");
        assert_eq!(parsed.members, vec![1, 2, 3]);
        assert_eq!(parsed.voters, vec![1, 3]);
    }

    #[test]
    fn membership_reconfig_has_empty_command_keys() {
        let payload = encode_membership_reconfig(&[1, 2, 3], &[1, 2]);
        let keys = command_keys(&payload).expect("command keys");
        assert!(keys.reads.is_empty());
        assert!(keys.writes.is_empty());
    }

    #[test]
    fn generation_batch_get_is_read_only_for_accord_dependencies() {
        let payload = encode_batch_get_slices_with_generations(&[
            (b"k1".as_slice(), 7),
            (b"k2".as_slice(), 8),
        ]);
        let keys = command_keys(&payload).expect("command keys");
        assert_eq!(keys.reads, vec![b"k1".to_vec(), b"k2".to_vec()]);
        assert!(keys.writes.is_empty());
    }

    #[test]
    fn kv_state_machine_applies_membership_hook() {
        let seen: Arc<Mutex<Vec<MembershipReconfig>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_hook = seen.clone();
        let hook: Arc<MembershipUpdateHook> = Arc::new(move |cfg| {
            let mut guard = seen_hook.lock().expect("lock");
            guard.push(cfg);
            Ok(())
        });
        let sm =
            KvStateMachine::with_membership_hook(Arc::new(KvStore::new()), None, Some(hook), None);
        let payload = encode_membership_reconfig(&[1, 2, 4], &[1, 2]);
        sm.apply(
            &payload,
            ExecMeta {
                seq: 1,
                txn_id: TxnId {
                    node_id: 1,
                    counter: 1,
                },
            },
        )
        .expect("apply membership");

        let guard = seen.lock().expect("lock");
        assert_eq!(guard.len(), 1);
        assert_eq!(
            guard[0],
            MembershipReconfig {
                members: vec![1, 2, 4],
                voters: vec![1, 2],
            }
        );
    }

    #[derive(Default)]
    struct BatchTrackingEngine {
        set_batch_calls: AtomicU64,
        set_batch_items: AtomicU64,
        apply_committed_batch_calls: AtomicU64,
        apply_committed_batch_items: AtomicU64,
        mark_visible_calls: AtomicU64,
        mark_visible_batch_calls: AtomicU64,
        mark_visible_batch_items: AtomicU64,
    }

    impl KvEngine for BatchTrackingEngine {
        fn get(&self, _key: &[u8], _version: Version) -> Option<Vec<u8>> {
            None
        }

        fn get_versioned(&self, _key: &[u8], _version: Version) -> Option<(Vec<u8>, Version)> {
            None
        }

        fn get_latest(&self, _key: &[u8]) -> Option<(Vec<u8>, Version)> {
            None
        }

        fn get_latest_batch(&self, keys: &[&[u8]]) -> Vec<Option<(Vec<u8>, Version)>> {
            vec![None; keys.len()]
        }

        fn set(&self, _key: &[u8], _value: &[u8], _version: Version) -> anyhow::Result<()> {
            Ok(())
        }

        fn set_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<()> {
            self.set_batch_calls.fetch_add(1, Ordering::Relaxed);
            self.set_batch_items
                .fetch_add(items.len() as u64, Ordering::Relaxed);
            Ok(())
        }

        fn apply_committed_batch(&self, items: &[(&[u8], &[u8], Version)]) -> anyhow::Result<u64> {
            self.apply_committed_batch_calls
                .fetch_add(1, Ordering::Relaxed);
            self.apply_committed_batch_items
                .fetch_add(items.len() as u64, Ordering::Relaxed);
            Ok(items.len() as u64)
        }

        fn mark_visible(&self, _key: &[u8], _version: Version) -> anyhow::Result<bool> {
            self.mark_visible_calls.fetch_add(1, Ordering::Relaxed);
            Ok(true)
        }

        fn mark_visible_batch(&self, keys: &[&[u8]], _version: Version) -> anyhow::Result<u64> {
            self.mark_visible_batch_calls
                .fetch_add(1, Ordering::Relaxed);
            self.mark_visible_batch_items
                .fetch_add(keys.len() as u64, Ordering::Relaxed);
            Ok(keys.len() as u64)
        }
    }

    #[test]
    fn kv_state_machine_apply_batch_publishes_with_batch_api() {
        let engine = Arc::new(BatchTrackingEngine::default());
        let sm = KvStateMachine::new(engine.clone(), None);
        let payload = Bytes::from(encode_batch_set(&[
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
        ]));
        sm.apply_batch(&[(
            payload,
            ExecMeta {
                seq: 7,
                txn_id: TxnId {
                    node_id: 1,
                    counter: 99,
                },
            },
        )])
        .expect("apply batch");

        assert_eq!(engine.set_batch_calls.load(Ordering::Relaxed), 0);
        assert_eq!(engine.set_batch_items.load(Ordering::Relaxed), 0);
        assert_eq!(
            engine.apply_committed_batch_calls.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            engine.apply_committed_batch_items.load(Ordering::Relaxed),
            3
        );
        assert_eq!(engine.mark_visible_batch_calls.load(Ordering::Relaxed), 0);
        assert_eq!(engine.mark_visible_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn kv_state_machine_mark_visible_uses_engine_batch_api() {
        let engine = Arc::new(BatchTrackingEngine::default());
        let sm = KvStateMachine::new(engine.clone(), None);
        let payload = encode_batch_set(&[
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
            (b"k3".to_vec(), b"v3".to_vec()),
        ]);
        sm.mark_visible(
            &payload,
            ExecMeta {
                seq: 7,
                txn_id: TxnId {
                    node_id: 1,
                    counter: 99,
                },
            },
        )
        .expect("mark visible");

        assert_eq!(engine.mark_visible_batch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(engine.mark_visible_batch_items.load(Ordering::Relaxed), 3);
        assert_eq!(engine.mark_visible_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn kv_state_machine_mark_visible_ignores_empty_command() {
        let engine = Arc::new(BatchTrackingEngine::default());
        let sm = KvStateMachine::new(engine.clone(), None);
        sm.mark_visible(
            &[],
            ExecMeta {
                seq: 1,
                txn_id: TxnId {
                    node_id: 1,
                    counter: 1,
                },
            },
        )
        .expect("mark visible empty");

        assert_eq!(engine.mark_visible_batch_calls.load(Ordering::Relaxed), 0);
        assert_eq!(engine.mark_visible_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn kv_state_machine_mark_visible_rejects_malformed_batch_set() {
        let engine = Arc::new(BatchTrackingEngine::default());
        let sm = KvStateMachine::new(engine.clone(), None);

        // Malformed command: declares one batch item but truncates before
        // value bytes. This should not call into the engine visibility path.
        let mut malformed = Vec::new();
        malformed.push(CMD_BATCH_SET);
        malformed.extend_from_slice(&1u32.to_be_bytes()); // item count
        malformed.extend_from_slice(&2u32.to_be_bytes()); // key len
        malformed.extend_from_slice(b"k1");
        malformed.extend_from_slice(&4u32.to_be_bytes()); // value len

        let err = sm
            .mark_visible(
                &malformed,
                ExecMeta {
                    seq: 2,
                    txn_id: TxnId {
                        node_id: 1,
                        counter: 2,
                    },
                },
            )
            .expect_err("malformed mark_visible should fail");
        assert!(err.to_string().contains("failed to parse command keys"));

        assert_eq!(engine.mark_visible_batch_calls.load(Ordering::Relaxed), 0);
        assert_eq!(engine.mark_visible_calls.load(Ordering::Relaxed), 0);
    }

    /// Verify the shared worker publishes data and records worker stats.
    ///
    /// Purpose:
    /// - Cover the normal path through `FjallEngine::apply_committed_batch`
    ///   when a publisher is configured.
    ///
    /// Design:
    /// - Uses one shard engine with a small worker and waits on the blocking
    ///   apply call, which only returns after the worker replies.
    ///
    /// Inputs:
    /// - One committed key/value/version item.
    ///
    /// Outputs:
    /// - Latest read returns the value and worker stats show one committed item.
    #[test]
    fn fjall_publish_batcher_applies_committed_batch_and_tracks_stats() {
        let (_dir, keyspace) = open_test_keyspace("publish_worker_normal");
        let publisher = FjallPublishBatcher::with_limits(
            keyspace.clone(),
            FjallPublishBatchLimits {
                max_requests: 8,
                max_items: 32,
                max_bytes: 4096,
            },
        );
        let engine = FjallEngine::open_shard_with_publisher(keyspace, 0, Some(publisher.clone()))
            .expect("open shard engine");
        let version = test_version(1, 1);

        let inserted = engine
            .apply_committed_batch(&[(b"k1".as_slice(), b"v1".as_slice(), version)])
            .expect("apply committed batch");

        assert_eq!(inserted, 1);
        assert_eq!(engine.get_latest(b"k1"), Some((b"v1".to_vec(), version)));
        let stats = publisher.snapshot();
        assert_eq!(stats.batches, 1);
        assert_eq!(stats.requests, 1);
        assert_eq!(stats.items, 1);
        assert_eq!(stats.errors, 0);
    }

    /// Verify combined publishes count duplicate-key latest insertion once.
    ///
    /// Purpose:
    /// - Cover the edge case where two queued requests for the same shard/key
    ///   publish different versions in one Fjall commit.
    ///
    /// Design:
    /// - Calls the combined publish helper directly so the test is deterministic
    ///   and independent of thread scheduling.
    ///
    /// Inputs:
    /// - Two requests for one key with increasing versions.
    ///
    /// Outputs:
    /// - One total inserted-latest count and the highest version in latest.
    #[test]
    fn fjall_combined_publish_counts_duplicate_key_latest_once() {
        let (_dir, keyspace) = open_test_keyspace("publish_duplicate_key");
        let engine = FjallEngine::open_shard(keyspace.clone(), 0).expect("open shard engine");
        let old_version = test_version(1, 1);
        let new_version = test_version(2, 2);

        let req1 = test_publish_request(
            &engine,
            vec![OwnedCommittedWrite {
                key: b"dup".to_vec(),
                value: b"old".to_vec(),
                version: old_version,
            }],
        );
        let req2 = test_publish_request(
            &engine,
            vec![OwnedCommittedWrite {
                key: b"dup".to_vec(),
                value: b"new".to_vec(),
                version: new_version,
            }],
        );

        let counts = publish_fjall_requests(&keyspace, &[req1, req2]).expect("publish requests");

        assert_eq!(counts, vec![0, 1]);
        assert_eq!(
            engine.get_latest(b"dup"),
            Some((b"new".to_vec(), new_version))
        );
        assert_eq!(engine.get(b"dup", old_version), Some(b"old".to_vec()));
    }

    /// Verify combined publishes isolate same key bytes across shard engines.
    ///
    /// Purpose:
    /// - Cover the edge case where two shard partitions contain identical user
    ///   key bytes and both need latest-index updates.
    ///
    /// Design:
    /// - Combines requests from two engines sharing a keyspace and checks each
    ///   shard's latest partition separately.
    ///
    /// Inputs:
    /// - Two requests with the same key bytes but different shard engines.
    ///
    /// Outputs:
    /// - Each request receives its own inserted-latest count.
    #[test]
    fn fjall_combined_publish_keeps_shard_latest_candidates_separate() {
        let (_dir, keyspace) = open_test_keyspace("publish_cross_shard_same_key");
        let shard0 = FjallEngine::open_shard(keyspace.clone(), 0).expect("open shard 0");
        let shard1 = FjallEngine::open_shard(keyspace.clone(), 1).expect("open shard 1");
        let version0 = test_version(1, 1);
        let version1 = test_version(1, 2);

        let req0 = test_publish_request(
            &shard0,
            vec![OwnedCommittedWrite {
                key: b"same".to_vec(),
                value: b"s0".to_vec(),
                version: version0,
            }],
        );
        let req1 = test_publish_request(
            &shard1,
            vec![OwnedCommittedWrite {
                key: b"same".to_vec(),
                value: b"s1".to_vec(),
                version: version1,
            }],
        );

        let counts = publish_fjall_requests(&keyspace, &[req0, req1]).expect("publish requests");

        assert_eq!(counts, vec![1, 1]);
        assert_eq!(shard0.get_latest(b"same"), Some((b"s0".to_vec(), version0)));
        assert_eq!(shard1.get_latest(b"same"), Some((b"s1".to_vec(), version1)));
    }

    /// Verify combined publish failures are returned instead of swallowed.
    ///
    /// Purpose:
    /// - Cover the failure path that must stop Accord execution from advancing
    ///   after an unapplied storage write.
    ///
    /// Design:
    /// - Supplies a poisoned engine lock to the helper and checks for an error
    ///   before any Fjall commit is attempted.
    ///
    /// Inputs:
    /// - One publish request with a poisoned lock.
    ///
    /// Outputs:
    /// - An error containing the storage lock failure.
    #[test]
    fn fjall_combined_publish_returns_lock_poison_error() {
        let (_dir, keyspace) = open_test_keyspace("publish_poisoned_lock");
        let engine = FjallEngine::open_shard(keyspace.clone(), 0).expect("open shard engine");
        let poisoned_lock = Arc::new(RwLock::new(()));
        let lock_for_panic = poisoned_lock.clone();
        let previous_hook = std::panic::take_hook();
        // Suppress the expected panic output; this poisons only the test lock
        // and does not exercise Fjall or the engine's normal lock.
        std::panic::set_hook(Box::new(|_| {}));
        let poison_result = std::panic::catch_unwind(move || {
            let _guard = lock_for_panic.write().expect("lock before poison");
            panic!("poison test lock");
        });
        std::panic::set_hook(previous_hook);
        assert!(poison_result.is_err());
        let mut req = test_publish_request(
            &engine,
            vec![OwnedCommittedWrite {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                version: test_version(1, 1),
            }],
        );
        req.lock = poisoned_lock;

        let err = publish_fjall_requests(&keyspace, &[req]).expect_err("publish should fail");

        assert!(err.to_string().contains("fjall kv lock poisoned"));
        assert_eq!(engine.get_latest(b"k"), None);
    }
}
