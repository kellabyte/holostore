//! Write-ahead log (WAL) implementation for durable Accord commits.
//!
//! This module provides a file-backed `CommitLog` implementation that batches
//! writes, optionally fsyncs, and supports compaction based on executed txns.

use std::collections::HashSet;
#[cfg(feature = "raft-engine")]
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
#[cfg(feature = "raft-engine")]
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};
use std::{env, str::FromStr};

use anyhow::Context;
use crc32fast::Hasher;
#[cfg(feature = "raft-engine")]
use holo_accord::accord::{txn_group_id, GroupId};
use holo_accord::accord::{
    CommitLog, CommitLogAppendOptions, CommitLogCheckpointStatus, CommitLogEntry, TxnId,
};

#[cfg(feature = "raft-engine")]
use raft_engine::{Config as RaftConfig, Engine as RaftEngine, LogBatch};

/// Default number of appended entries before forcing a sync.
const WAL_PERSIST_EVERY: u64 = 256;
/// Default max time between forced syncs (microseconds).
const WAL_PERSIST_INTERVAL_US: u64 = 2_000;
/// Default maximum number of commit entries to batch together.
const WAL_COMMIT_BATCH_MAX: usize = 64;
/// Default batching window for commits (microseconds).
const WAL_COMMIT_BATCH_WAIT_US: u64 = 200;

/// File name used for the WAL log within the WAL directory.
const WAL_LOG_FILE: &str = "wal.log";
/// Sidecar file storing the last fsync-confirmed WAL byte offset.
const WAL_SYNC_STATE_FILE: &str = "wal.synced";
/// Fault-injection flag: truncate WAL to last synced offset when opening.
const WAL_FAULT_TRUNCATE_UNSYNCED_ON_OPEN: &str = "HOLO_WAL_FAULT_TRUNCATE_UNSYNCED_ON_OPEN";

/// Snapshot of WAL performance statistics for logging/monitoring.
#[derive(Default, Debug, Clone, Copy)]
pub struct WalStatsSnapshot {
    pub fsync_count: u64,
    pub fsync_total_us: u64,
    pub fsync_max_us: u64,
    pub batch_count: u64,
    pub batch_items: u64,
    pub batch_max_items: u64,
    pub batch_total_bytes: u64,
    pub batch_max_bytes: u64,
}

/// Internal counters used to build `WalStatsSnapshot`.
struct WalStats {
    fsync_count: AtomicU64,
    fsync_total_us: AtomicU64,
    fsync_max_us: AtomicU64,
    batch_count: AtomicU64,
    batch_items: AtomicU64,
    batch_max_items: AtomicU64,
    batch_total_bytes: AtomicU64,
    batch_max_bytes: AtomicU64,
}

impl WalStats {
    /// Initialize a zeroed statistics struct.
    const fn new() -> Self {
        Self {
            fsync_count: AtomicU64::new(0),
            fsync_total_us: AtomicU64::new(0),
            fsync_max_us: AtomicU64::new(0),
            batch_count: AtomicU64::new(0),
            batch_items: AtomicU64::new(0),
            batch_max_items: AtomicU64::new(0),
            batch_total_bytes: AtomicU64::new(0),
            batch_max_bytes: AtomicU64::new(0),
        }
    }

    /// Record the cost of a single fsync.
    fn record_fsync(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
        self.fsync_total_us.fetch_add(us, Ordering::Relaxed);
        self.fsync_max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Record batch sizing stats for a commit write.
    fn record_batch(&self, items: u64, bytes: u64) {
        self.batch_count.fetch_add(1, Ordering::Relaxed);
        self.batch_items.fetch_add(items, Ordering::Relaxed);
        self.batch_max_items.fetch_max(items, Ordering::Relaxed);
        self.batch_total_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.batch_max_bytes.fetch_max(bytes, Ordering::Relaxed);
    }

    /// Return the current snapshot and reset counters.
    fn snapshot_and_reset(&self) -> WalStatsSnapshot {
        WalStatsSnapshot {
            fsync_count: self.fsync_count.swap(0, Ordering::Relaxed),
            fsync_total_us: self.fsync_total_us.swap(0, Ordering::Relaxed),
            fsync_max_us: self.fsync_max_us.swap(0, Ordering::Relaxed),
            batch_count: self.batch_count.swap(0, Ordering::Relaxed),
            batch_items: self.batch_items.swap(0, Ordering::Relaxed),
            batch_max_items: self.batch_max_items.swap(0, Ordering::Relaxed),
            batch_total_bytes: self.batch_total_bytes.swap(0, Ordering::Relaxed),
            batch_max_bytes: self.batch_max_bytes.swap(0, Ordering::Relaxed),
        }
    }
}

/// Global WAL statistics collector.
static WAL_STATS: WalStats = WalStats::new();

/// Fetch and reset WAL stats for logging/monitoring.
pub fn stats_snapshot() -> WalStatsSnapshot {
    WAL_STATS.snapshot_and_reset()
}

/// Single commit append work item sent to the WAL worker.
struct CommitWork {
    /// Commit record to encode and append.
    entry: CommitLogEntry,
    /// When `true`, this request requires fsync-complete durability before ACK.
    require_durable: bool,
    /// One-shot response channel for append result propagation.
    tx: mpsc::Sender<anyhow::Result<()>>,
}

/// Compaction request sent to the WAL worker.
struct CompactWork {
    max_delete: usize,
    tx: mpsc::Sender<anyhow::Result<usize>>,
}

/// Commands supported by the WAL worker thread.
enum WalCommand {
    Append(CommitWork),
    AppendBatch(Vec<CommitWork>),
    MarkExecuted(TxnId),
    Compact(CompactWork),
}

/// Sync strategy used when persisting WAL data.
#[derive(Clone, Copy, Debug)]
enum SyncMode {
    None,
    Data,
    All,
}

/// File-backed WAL implementation with a dedicated worker thread.
pub struct FileWal {
    dir: PathBuf,
    log_path: PathBuf,
    tx: mpsc::Sender<WalCommand>,
}

impl FileWal {
    /// Open or create a WAL directory and spawn the worker thread.
    ///
    /// Input:
    /// - `path`: WAL root directory for this group.
    ///
    /// Output:
    /// - `FileWal` with live worker channels when startup succeeds.
    ///
    /// Design:
    /// - Reads env knobs for batching, persistence mode, and async fsync worker.
    /// - Optionally applies fault-injection truncation before writers start.
    /// - Separates `persist_mode` (periodic background policy) from
    ///   `configured_persist_mode` (capability used for forced durable appends).
    pub fn open_dir(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir).context("create wal dir")?;
        let log_path = dir.join(WAL_LOG_FILE);
        let sync_state_path = dir.join(WAL_SYNC_STATE_FILE);

        // Optional fault injection for crash tests: emulate power-loss behavior
        // by dropping any bytes not known to be fsync-confirmed.
        maybe_fault_truncate_unsynced_tail(&log_path, &sync_state_path)?;

        // Read persistence and batching knobs from environment variables.
        let persist_every = read_env_u64("HOLO_WAL_PERSIST_EVERY", WAL_PERSIST_EVERY);
        let persist_interval_us =
            read_env_u64("HOLO_WAL_PERSIST_INTERVAL_US", WAL_PERSIST_INTERVAL_US);
        let configured_persist_mode =
            parse_persist_mode(env::var("HOLO_WAL_PERSIST_MODE").ok().as_deref())
                .unwrap_or(SyncMode::None);
        let persist_async = read_env_bool("HOLO_WAL_PERSIST_ASYNC", false);

        let commit_batch_max =
            read_env_usize("HOLO_WAL_COMMIT_BATCH_MAX", WAL_COMMIT_BATCH_MAX).max(1);
        let commit_batch_wait_us =
            read_env_u64("HOLO_WAL_COMMIT_BATCH_WAIT_US", WAL_COMMIT_BATCH_WAIT_US);

        // Decide whether periodic fsync work is inline or offloaded.
        let (persist_mode, persist_tx) = if persist_async {
            match configured_persist_mode {
                SyncMode::Data | SyncMode::All => {
                    let (tx, rx) = mpsc::channel();
                    let path = log_path.clone();
                    let sync_state_path = sync_state_path.clone();
                    thread::Builder::new()
                        .name("wal-persist".to_string())
                        .spawn(move || {
                            persist_loop(&path, &sync_state_path, configured_persist_mode, rx)
                        })
                        .context("spawn wal persist thread")?;
                    // In async-persist mode the commit worker skips periodic fsync and
                    // signals this dedicated thread instead.
                    (SyncMode::None, Some(tx))
                }
                // No sync mode configured; both periodic and forced durability will fail.
                SyncMode::None => (SyncMode::None, None),
            }
        } else {
            // Inline persistence in the commit worker thread.
            (configured_persist_mode, None)
        };

        let (tx, rx) = mpsc::channel();
        let worker_log_path = log_path.clone();
        thread::Builder::new()
            .name("wal-commit".to_string())
            .spawn(move || {
                // Commit worker batches appends, handles fsyncs, and serves compaction.
                wal_worker(
                    &worker_log_path,
                    rx,
                    persist_every,
                    persist_interval_us,
                    persist_mode,
                    persist_tx,
                    configured_persist_mode,
                    sync_state_path,
                    commit_batch_max,
                    Duration::from_micros(commit_batch_wait_us),
                )
            })
            .context("spawn wal commit thread")?;

        Ok(Self { dir, log_path, tx })
    }

    /// Read all WAL entries for replay.
    fn load_entries(&self) -> anyhow::Result<Vec<CommitLogEntry>> {
        read_log_entries(&self.log_path)
    }

    /// Append multiple commits with explicit per-request durability options.
    ///
    /// Inputs:
    /// - `entries`: encoded commit records to append to the WAL.
    /// - `options`: append controls (notably `require_durable`).
    ///
    /// Output:
    /// - `Ok(())` when the worker confirms all items in this request.
    /// - `Err(...)` when append/sync fails or the worker channel closes.
    pub fn append_commits_with_options(
        &self,
        entries: Vec<CommitLogEntry>,
        options: CommitLogAppendOptions,
    ) -> anyhow::Result<()> {
        if entries.is_empty() {
            // Nothing to append.
            return Ok(());
        }
        let (tx, rx) = mpsc::channel();
        let mut works = Vec::with_capacity(entries.len());
        for entry in entries {
            works.push(CommitWork {
                entry,
                require_durable: options.require_durable,
                tx: tx.clone(),
            });
        }
        drop(tx);
        let work_count = works.len();
        self.tx
            .send(WalCommand::AppendBatch(works))
            .map_err(|_| anyhow::anyhow!("wal worker closed"))?;
        for _ in 0..work_count {
            rx.recv().context("wal append batch response dropped")??;
        }
        Ok(())
    }
}

impl CommitLog for FileWal {
    /// Append a single commit entry to the WAL.
    ///
    /// Inputs:
    /// - `entry`: commit record to append.
    /// - `options`: durability requirements for this append.
    ///
    /// Output:
    /// - `Ok(())` once append (and required fsync) has completed.
    fn append_commit_with_options(
        &self,
        entry: CommitLogEntry,
        options: CommitLogAppendOptions,
    ) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(WalCommand::Append(CommitWork {
                entry,
                require_durable: options.require_durable,
                tx,
            }))
            .map_err(|_| anyhow::anyhow!("wal worker closed"))?;
        rx.recv().context("wal append response dropped")?
    }

    /// Append multiple commits in one batch.
    ///
    /// Inputs:
    /// - `entries`: commit records to append.
    /// - `options`: durability requirements shared by this batch.
    ///
    /// Output:
    /// - `Ok(())` when every item is acknowledged by the worker.
    fn append_commits_with_options(
        &self,
        entries: Vec<CommitLogEntry>,
        options: CommitLogAppendOptions,
    ) -> anyhow::Result<()> {
        FileWal::append_commits_with_options(self, entries, options)
    }

    /// Record that a transaction has executed (for compaction eligibility).
    fn mark_executed(&self, txn_id: TxnId) -> anyhow::Result<()> {
        let _ = self.tx.send(WalCommand::MarkExecuted(txn_id));
        Ok(())
    }

    /// Load all commit entries for replay.
    fn load(&self) -> anyhow::Result<Vec<CommitLogEntry>> {
        self.load_entries()
    }

    /// Compact the WAL by removing up to `max_delete` executed entries.
    fn compact(&self, max_delete: usize) -> anyhow::Result<usize> {
        if max_delete == 0 {
            // No compaction requested.
            return Ok(0);
        }
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(WalCommand::Compact(CompactWork { max_delete, tx }))
            .map_err(|_| anyhow::anyhow!("wal worker closed"))?;
        rx.recv().context("wal compact response dropped")?
    }
}

#[cfg(feature = "raft-engine")]
const WAL_META_REGION_ID: GroupId = 0;
#[cfg(feature = "raft-engine")]
const WAL_META_GROUPS_KEY: &[u8] = b"meta/groups";
#[cfg(feature = "raft-engine")]
const WAL_META_LAST_INDEX_KEY: &[u8] = b"meta/last_index";
#[cfg(feature = "raft-engine")]
const WAL_META_COMPACTED_FLOOR_KEY: &[u8] = b"meta/compacted_floor";
#[cfg(feature = "raft-engine")]
const WAL_META_DURABLE_FLOOR_KEY: &[u8] = b"meta/durable_floor";
#[cfg(feature = "raft-engine")]
const WAL_ENTRY_PREFIX: u8 = b'e';

#[cfg(feature = "raft-engine")]
#[derive(Debug)]
struct RaftWalState {
    last_index: HashMap<GroupId, u64>,
    groups: HashSet<GroupId>,
    txn_index: HashMap<TxnId, (GroupId, u64)>,
    txn_by_index: HashMap<GroupId, HashMap<u64, TxnId>>,
    // Highest contiguous executed log index by group.
    executed_floor: HashMap<GroupId, u64>,
    // Executed indices that are above the contiguous floor and waiting for gaps.
    executed_pending: HashMap<GroupId, BTreeSet<u64>>,
    // Highest log index durably compacted away by group.
    compacted_floor: HashMap<GroupId, u64>,
    // Highest log index known durable in KV/meta storage checkpoints.
    durable_floor: HashMap<GroupId, u64>,
    checkpoint_count: u64,
    checkpoint_error_count: u64,
    checkpoint_last_us: u64,
    pending_count: u64,
    last_persist_us: u64,
}

#[cfg(feature = "raft-engine")]
impl RaftWalState {
    fn new() -> Self {
        Self {
            last_index: HashMap::new(),
            groups: HashSet::new(),
            txn_index: HashMap::new(),
            txn_by_index: HashMap::new(),
            executed_floor: HashMap::new(),
            executed_pending: HashMap::new(),
            compacted_floor: HashMap::new(),
            durable_floor: HashMap::new(),
            checkpoint_count: 0,
            checkpoint_error_count: 0,
            checkpoint_last_us: 0,
            pending_count: 0,
            last_persist_us: epoch_micros(),
        }
    }
}

/// Raft-engine backed WAL implementation (optional).
#[cfg(feature = "raft-engine")]
pub struct RaftEngineWal {
    engine: RaftEngine,
    state: Mutex<RaftWalState>,
    persist_every: u64,
    persist_interval_us: u64,
    persist_mode: SyncMode,
}

#[cfg(feature = "raft-engine")]
impl RaftEngineWal {
    /// Open or create a raft-engine WAL directory.
    pub fn open_dir(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir).context("create raft wal dir")?;

        let mut cfg = RaftConfig::default();
        cfg.dir = dir.to_string_lossy().to_string();
        let engine = RaftEngine::open(cfg).context("open raft-engine")?;

        let persist_every = read_env_u64("HOLO_WAL_PERSIST_EVERY", WAL_PERSIST_EVERY);
        let persist_interval_us =
            read_env_u64("HOLO_WAL_PERSIST_INTERVAL_US", WAL_PERSIST_INTERVAL_US);
        let persist_mode = parse_persist_mode(env::var("HOLO_WAL_PERSIST_MODE").ok().as_deref())
            .unwrap_or(SyncMode::None);

        let mut state = RaftWalState::new();
        if let Some(buf) = engine.get(WAL_META_REGION_ID, WAL_META_GROUPS_KEY) {
            let groups = decode_groups(&buf)?;
            for group_id in groups {
                state.groups.insert(group_id);
                if let Some(last) = read_last_index(&engine, group_id)? {
                    state.last_index.insert(group_id, last);
                }
                if let Some(floor) = read_compacted_floor(&engine, group_id)? {
                    state.compacted_floor.insert(group_id, floor);
                }
                if let Some(floor) = read_durable_floor(&engine, group_id)? {
                    state.durable_floor.insert(group_id, floor);
                }
            }
        }

        Ok(Self {
            engine,
            state: Mutex::new(state),
            persist_every,
            persist_interval_us,
            persist_mode,
        })
    }

    /// Decide persistence behavior for one raft-engine append call.
    ///
    /// Inputs:
    /// - `state`: current persistence counters.
    /// - `batch_len`: number of entries being appended now.
    /// - `now`: current timestamp in microseconds.
    /// - `options`: caller durability requirements.
    ///
    /// Output:
    /// - `(sync, pending_count, last_persist_us)` persistence decision/state.
    ///
    /// Design:
    /// - `require_durable` forces `sync=true` and validates durability support.
    /// - Otherwise uses periodic count/time thresholds.
    fn persist_should_sync(
        &self,
        state: &RaftWalState,
        batch_len: usize,
        now: u64,
        options: CommitLogAppendOptions,
    ) -> anyhow::Result<(bool, u64, u64)> {
        // Per-request durable ACK has priority over periodic thresholds.
        if options.require_durable {
            anyhow::ensure!(
                !matches!(self.persist_mode, SyncMode::None),
                "durable append requested but wal persist mode is non-durable"
            );
            return Ok((true, 0, now));
        }
        // With non-durable mode there is no sync thresholding.
        if matches!(self.persist_mode, SyncMode::None) {
            return Ok((false, state.pending_count, state.last_persist_us));
        }
        let mut pending = state.pending_count.saturating_add(batch_len as u64);
        let mut last_persist_us = state.last_persist_us;
        let hit_count = self.persist_every > 0 && pending >= self.persist_every;
        let hit_interval = self.persist_interval_us > 0
            && now.saturating_sub(state.last_persist_us) >= self.persist_interval_us;
        let sync = hit_count || hit_interval;
        if sync {
            pending = 0;
            last_persist_us = now;
        }
        Ok((sync, pending, last_persist_us))
    }
}

#[cfg(feature = "raft-engine")]
impl CommitLog for RaftEngineWal {
    /// Append one commit entry with explicit durability options.
    ///
    /// Inputs:
    /// - `entry`: commit record to append.
    /// - `options`: durability requirements for this append.
    ///
    /// Output:
    /// - `Ok(())` when raft-engine write completes under requested sync policy.
    fn append_commit_with_options(
        &self,
        entry: CommitLogEntry,
        options: CommitLogAppendOptions,
    ) -> anyhow::Result<()> {
        self.append_commits_with_options(vec![entry], options)
    }

    /// Append multiple commit entries with explicit durability options.
    ///
    /// Inputs:
    /// - `entries`: commit records to append.
    /// - `options`: durability requirements for this append batch.
    ///
    /// Output:
    /// - `Ok(())` when metadata + entries are written consistently.
    fn append_commits_with_options(
        &self,
        entries: Vec<CommitLogEntry>,
        options: CommitLogAppendOptions,
    ) -> anyhow::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut state = self.state.lock().expect("raft wal state lock");
        let now = epoch_micros();

        let mut assigned = Vec::with_capacity(entries.len());
        let mut per_group_last = HashMap::<GroupId, u64>::new();
        let mut new_groups = Vec::new();
        let mut prev_last = HashMap::<GroupId, u64>::new();

        for entry in entries {
            let group_id = txn_group_id(entry.txn_id);
            let base_last = match state.last_index.get(&group_id).copied() {
                Some(last) => last,
                None => read_last_index(&self.engine, group_id)?.unwrap_or(0),
            };
            prev_last.entry(group_id).or_insert(base_last);
            let next_index = base_last.saturating_add(1);
            state.last_index.insert(group_id, next_index);
            per_group_last.insert(group_id, next_index);
            if state.groups.insert(group_id) {
                new_groups.push(group_id);
            }
            state.compacted_floor.entry(group_id).or_insert(0);
            let compacted_floor = state.compacted_floor.get(&group_id).copied().unwrap_or(0);
            state
                .durable_floor
                .entry(group_id)
                .or_insert(compacted_floor);
            assigned.push((group_id, next_index, entry));
        }

        let (sync, pending_count, last_persist_us) =
            self.persist_should_sync(&state, assigned.len(), now, options)?;

        let mut batch = LogBatch::with_capacity(assigned.len() + per_group_last.len() + 1);
        for (group_id, index, entry) in &assigned {
            let key = entry_key(*index);
            let payload = encode_entry(entry);
            batch
                .put(*group_id, key.to_vec(), payload)
                .context("raft wal batch put entry")?;
        }
        for (group_id, last_index) in &per_group_last {
            let payload = last_index.to_be_bytes();
            batch
                .put(
                    *group_id,
                    WAL_META_LAST_INDEX_KEY.to_vec(),
                    payload.to_vec(),
                )
                .context("raft wal batch put last index")?;
        }
        if !new_groups.is_empty() {
            let payload = encode_groups(&state.groups);
            batch
                .put(WAL_META_REGION_ID, WAL_META_GROUPS_KEY.to_vec(), payload)
                .context("raft wal batch put group list")?;
        }

        if let Err(err) = self.engine.write(&mut batch, sync) {
            for (group_id, last) in prev_last {
                state.last_index.insert(group_id, last);
            }
            for group_id in new_groups {
                state.groups.remove(&group_id);
            }
            return Err(err).context("raft wal write batch");
        }

        for (group_id, index, entry) in assigned {
            state.txn_index.insert(entry.txn_id, (group_id, index));
            state
                .txn_by_index
                .entry(group_id)
                .or_default()
                .insert(index, entry.txn_id);
        }
        state.pending_count = pending_count;
        state.last_persist_us = last_persist_us;
        Ok(())
    }

    fn mark_executed(&self, txn_id: TxnId) -> anyhow::Result<()> {
        let mut state = self.state.lock().expect("raft wal state lock");
        let Some((group_id, index)) = state.txn_index.get(&txn_id).copied() else {
            return Ok(());
        };
        let base_floor = state.compacted_floor.get(&group_id).copied().unwrap_or(0);
        let mut floor = state
            .executed_floor
            .get(&group_id)
            .copied()
            .unwrap_or(base_floor);
        if index <= floor {
            return Ok(());
        }
        let pending = state.executed_pending.entry(group_id).or_default();
        pending.insert(index);
        while pending.remove(&floor.saturating_add(1)) {
            floor = floor.saturating_add(1);
        }
        state.executed_floor.insert(group_id, floor);
        Ok(())
    }

    fn load(&self) -> anyhow::Result<Vec<CommitLogEntry>> {
        let mut state = self.state.lock().expect("raft wal state lock");
        if state.groups.is_empty() {
            if let Some(buf) = self.engine.get(WAL_META_REGION_ID, WAL_META_GROUPS_KEY) {
                for group_id in decode_groups(&buf)? {
                    state.groups.insert(group_id);
                }
            }
        }

        state.txn_index.clear();
        state.txn_by_index.clear();
        state.executed_floor.clear();
        state.executed_pending.clear();

        let mut entries = Vec::new();
        let groups: Vec<GroupId> = state.groups.iter().copied().collect();
        for group_id in groups {
            let last_index = match state.last_index.get(&group_id).copied() {
                Some(last) => last,
                None => {
                    let last = read_last_index(&self.engine, group_id)?.unwrap_or(0);
                    state.last_index.insert(group_id, last);
                    last
                }
            };
            let compacted_floor = match state.compacted_floor.get(&group_id).copied() {
                Some(floor) => floor.min(last_index),
                None => {
                    let floor = read_compacted_floor(&self.engine, group_id)?
                        .unwrap_or(0)
                        .min(last_index);
                    state.compacted_floor.insert(group_id, floor);
                    floor
                }
            };
            let durable_floor = match state.durable_floor.get(&group_id).copied() {
                Some(floor) => floor.min(last_index),
                None => {
                    let floor = read_durable_floor(&self.engine, group_id)?
                        .unwrap_or(compacted_floor)
                        .max(compacted_floor)
                        .min(last_index);
                    state.durable_floor.insert(group_id, floor);
                    floor
                }
            };
            if last_index == 0 {
                continue;
            }
            for index in compacted_floor.saturating_add(1)..=last_index {
                let key = entry_key(index);
                let Some(payload) = self.engine.get(group_id, &key) else {
                    continue;
                };
                let entry = decode_entry(&payload)?;
                state.txn_index.insert(entry.txn_id, (group_id, index));
                state
                    .txn_by_index
                    .entry(group_id)
                    .or_default()
                    .insert(index, entry.txn_id);
                entries.push(entry);
            }
            state.executed_floor.insert(group_id, compacted_floor);
            state
                .durable_floor
                .insert(group_id, durable_floor.max(compacted_floor));
        }
        Ok(entries)
    }

    fn compact(&self, max_delete: usize) -> anyhow::Result<usize> {
        if max_delete == 0 {
            return Ok(0);
        }

        let mut state = self.state.lock().expect("raft wal state lock");
        let mut groups: Vec<GroupId> = state.groups.iter().copied().collect();
        groups.sort_unstable();

        let mut removed = 0usize;
        let mut compacted_updates = HashMap::<GroupId, u64>::new();
        let mut progress = true;
        while removed < max_delete && progress {
            progress = false;
            for group_id in &groups {
                if removed >= max_delete {
                    break;
                }
                let current_floor = compacted_updates
                    .get(group_id)
                    .copied()
                    .unwrap_or_else(|| state.compacted_floor.get(group_id).copied().unwrap_or(0));
                let executed_floor = state
                    .executed_floor
                    .get(group_id)
                    .copied()
                    .unwrap_or(current_floor);
                let durable_floor = state
                    .durable_floor
                    .get(group_id)
                    .copied()
                    .unwrap_or(current_floor);
                let gc_floor = executed_floor.min(durable_floor);
                if gc_floor <= current_floor {
                    continue;
                }
                compacted_updates.insert(*group_id, current_floor.saturating_add(1));
                removed += 1;
                progress = true;
            }
        }

        if compacted_updates.is_empty() {
            return Ok(0);
        }

        let mut batch =
            LogBatch::with_capacity((removed * 2).saturating_add(compacted_updates.len()));
        for (group_id, new_floor) in &compacted_updates {
            let old_floor = state.compacted_floor.get(group_id).copied().unwrap_or(0);
            for index in old_floor.saturating_add(1)..=*new_floor {
                let key = entry_key(index);
                batch.delete(*group_id, key.to_vec());
            }
            batch
                .put(
                    *group_id,
                    WAL_META_COMPACTED_FLOOR_KEY.to_vec(),
                    new_floor.to_be_bytes().to_vec(),
                )
                .context("raft wal batch put compacted floor")?;
        }

        self.engine
            .write(&mut batch, false)
            .context("raft wal compact write")?;

        for (group_id, new_floor) in &compacted_updates {
            let old_floor = state.compacted_floor.get(group_id).copied().unwrap_or(0);
            let mut removed_txns = Vec::new();
            if let Some(by_index) = state.txn_by_index.get_mut(group_id) {
                for index in old_floor.saturating_add(1)..=*new_floor {
                    if let Some(txn_id) = by_index.remove(&index) {
                        removed_txns.push(txn_id);
                    }
                }
            }
            for txn_id in removed_txns {
                state.txn_index.remove(&txn_id);
            }
            if let Some(pending) = state.executed_pending.get_mut(group_id) {
                for index in old_floor.saturating_add(1)..=*new_floor {
                    pending.remove(&index);
                }
            }
            state.compacted_floor.insert(*group_id, *new_floor);
            let durable = state.durable_floor.entry(*group_id).or_insert(*new_floor);
            if *durable < *new_floor {
                *durable = *new_floor;
            }
            let executed = state.executed_floor.entry(*group_id).or_insert(*new_floor);
            if *executed < *new_floor {
                *executed = *new_floor;
            }
        }

        Ok(removed)
    }

    fn mark_durable_checkpoint(&self) -> anyhow::Result<()> {
        let mut state = self.state.lock().expect("raft wal state lock");
        if state.groups.is_empty() {
            if let Some(buf) = self.engine.get(WAL_META_REGION_ID, WAL_META_GROUPS_KEY) {
                for group_id in decode_groups(&buf)? {
                    state.groups.insert(group_id);
                }
            }
        }

        let mut groups: Vec<GroupId> = state.groups.iter().copied().collect();
        groups.sort_unstable();
        if groups.is_empty() {
            state.checkpoint_last_us = epoch_micros();
            state.checkpoint_count = state.checkpoint_count.saturating_add(1);
            return Ok(());
        }

        let mut updates = Vec::<(GroupId, u64)>::new();
        for group_id in groups {
            let compacted = state.compacted_floor.get(&group_id).copied().unwrap_or(0);
            let executed = state
                .executed_floor
                .get(&group_id)
                .copied()
                .unwrap_or(compacted);
            let durable = executed.max(compacted);
            updates.push((group_id, durable));
        }

        let mut batch = LogBatch::with_capacity(updates.len());
        for (group_id, floor) in &updates {
            batch
                .put(
                    *group_id,
                    WAL_META_DURABLE_FLOOR_KEY.to_vec(),
                    floor.to_be_bytes().to_vec(),
                )
                .context("raft wal batch put durable floor")?;
        }

        if let Err(err) = self.engine.write(&mut batch, false) {
            state.checkpoint_error_count = state.checkpoint_error_count.saturating_add(1);
            return Err(err).context("raft wal durable checkpoint write");
        }

        for (group_id, floor) in updates {
            state.durable_floor.insert(group_id, floor);
        }
        state.checkpoint_count = state.checkpoint_count.saturating_add(1);
        state.checkpoint_last_us = epoch_micros();
        Ok(())
    }

    fn checkpoint_status(&self) -> CommitLogCheckpointStatus {
        let state = self.state.lock().expect("raft wal state lock");
        let mut durable = std::collections::BTreeMap::new();
        let mut executed = std::collections::BTreeMap::new();
        let mut compacted = std::collections::BTreeMap::new();
        let mut groups: Vec<GroupId> = state.groups.iter().copied().collect();
        groups.sort_unstable();
        for group_id in groups {
            durable.insert(
                group_id,
                state.durable_floor.get(&group_id).copied().unwrap_or(0),
            );
            executed.insert(
                group_id,
                state.executed_floor.get(&group_id).copied().unwrap_or(0),
            );
            compacted.insert(
                group_id,
                state.compacted_floor.get(&group_id).copied().unwrap_or(0),
            );
        }
        CommitLogCheckpointStatus {
            durable_floor_by_group: durable,
            executed_floor_by_group: executed,
            compacted_floor_by_group: compacted,
        }
    }
}

/// Worker loop that batches WAL commands and persists log entries.
///
/// Inputs:
/// - `log_path`: WAL file path.
/// - `rx`: command stream from WAL API callers.
/// - `persist_every`/`persist_interval_us`: periodic sync thresholds.
/// - `persist_mode`/`persist_tx`: periodic sync execution strategy.
/// - `durable_sync_mode`: fsync capability used by `require_durable` requests.
/// - `sync_state_path`: sidecar storing fsync-confirmed byte offset.
/// - `batch_max`/`batch_wait`: command coalescing controls.
///
/// Output:
/// - No direct return value; responds to each command via per-request channels.
///
/// Design:
/// - Coalesces appends for throughput, but escalates to durable sync when any
///   request in the batch requires fsync-on-ack.
/// - Separates append errors from persist errors so callers receive a precise
///   failure regardless of which phase failed.
fn wal_worker(
    log_path: &Path,
    rx: mpsc::Receiver<WalCommand>,
    persist_every: u64,
    persist_interval_us: u64,
    persist_mode: SyncMode,
    persist_tx: Option<mpsc::Sender<()>>,
    durable_sync_mode: SyncMode,
    sync_state_path: PathBuf,
    batch_max: usize,
    batch_wait: Duration,
) {
    let mut executed = HashSet::<TxnId>::new();
    let mut pending_count = 0u64;
    let mut last_persist_us = epoch_micros();
    let mut file = match open_log_for_append(log_path) {
        Ok(file) => file,
        Err(err) => {
            tracing::error!(error = ?err, "open wal log failed");
            return;
        }
    };

    let batch_max = batch_max.max(1);
    let mut disconnected = false;
    while !disconnected {
        let first = match rx.recv() {
            Ok(cmd) => cmd,
            Err(_) => break,
        };

        let mut commands = Vec::with_capacity(batch_max);
        commands.push(first);

        if batch_max > 1 {
            // Try to coalesce multiple commands into a batch up to the limit or wait window.
            let deadline = if batch_wait.is_zero() {
                None
            } else {
                Some(Instant::now() + batch_wait)
            };

            loop {
                if commands.len() >= batch_max {
                    break;
                }
                match rx.try_recv() {
                    Ok(cmd) => {
                        commands.push(cmd);
                        continue;
                    }
                    Err(mpsc::TryRecvError::Empty) => {}
                    Err(mpsc::TryRecvError::Disconnected) => {
                        // Stop after draining pending commands.
                        disconnected = true;
                        break;
                    }
                }

                let Some(deadline) = deadline else {
                    break;
                };
                let now = Instant::now();
                if now >= deadline {
                    // Batching window expired.
                    break;
                }
                let remaining = deadline.saturating_duration_since(now);
                match rx.recv_timeout(remaining) {
                    Ok(cmd) => commands.push(cmd),
                    Err(mpsc::RecvTimeoutError::Timeout) => break,
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // Stop after draining pending commands.
                        disconnected = true;
                        break;
                    }
                }
            }
        }

        let mut append_batch = Vec::new();
        let mut append_resps = Vec::new();
        let mut batch_require_durable = false;
        let mut compact_req: Option<CompactWork> = None;

        for cmd in commands {
            match cmd {
                WalCommand::Append(work) => {
                    batch_require_durable |= work.require_durable;
                    append_batch.push(work.entry);
                    append_resps.push(work.tx);
                }
                WalCommand::AppendBatch(works) => {
                    for work in works {
                        batch_require_durable |= work.require_durable;
                        append_batch.push(work.entry);
                        append_resps.push(work.tx);
                    }
                }
                WalCommand::MarkExecuted(txn_id) => {
                    // Track executed txns so compaction can drop them later.
                    executed.insert(txn_id);
                }
                WalCommand::Compact(work) => {
                    // Defer compaction until after current batch flush.
                    compact_req = Some(work);
                }
            }
        }

        let mut append_result: anyhow::Result<()> = Ok(());
        if !append_batch.is_empty() {
            if let Err(err) = append_entries(&mut file, &append_batch) {
                append_result = Err(err.into());
            }
        }

        let mut persist_error: Option<anyhow::Error> = None;
        if append_result.is_ok() && !append_batch.is_empty() {
            let batch_items = append_batch.len() as u64;
            let mut batch_bytes = 0u64;
            for entry in &append_batch {
                batch_bytes = batch_bytes.saturating_add((encoded_len(entry) + 8) as u64);
            }
            WAL_STATS.record_batch(batch_items, batch_bytes);
            pending_count = pending_count.saturating_add(append_batch.len() as u64);
            let now = epoch_micros();
            // Decide whether to sync based on count or elapsed time thresholds.
            let hit_count = persist_every > 0 && pending_count >= persist_every;
            let hit_interval = persist_interval_us > 0
                && now.saturating_sub(last_persist_us) >= persist_interval_us;
            if batch_require_durable || hit_count || hit_interval {
                pending_count = 0;
                last_persist_us = now;
                if batch_require_durable {
                    // Strict-ACK path: fail immediately if durable fsync is impossible.
                    if matches!(durable_sync_mode, SyncMode::None) {
                        persist_error = Some(anyhow::anyhow!(
                            "durable append requested but wal persist mode is non-durable"
                        ));
                    } else if let Err(err) =
                        sync_file_with_state(&file, durable_sync_mode, &sync_state_path)
                    {
                        // Capture sync errors so we can propagate to callers.
                        persist_error = Some(err.into());
                    }
                } else if let Some(tx) = &persist_tx {
                    // Signal the async persist thread.
                    let _ = tx.send(());
                } else if let Err(err) = sync_file_with_state(&file, persist_mode, &sync_state_path)
                {
                    // Capture sync errors so we can propagate to callers.
                    persist_error = Some(err.into());
                }
            }
        }

        let err_msg = append_result
            .err()
            .or(persist_error)
            .map(|err| err.to_string());
        for tx in append_resps {
            let res = match &err_msg {
                None => Ok(()),
                Some(msg) => Err(anyhow::anyhow!(msg.clone())),
            };
            let _ = tx.send(res);
        }

        if let Some(work) = compact_req {
            let res = compact_log(log_path, &mut executed, work.max_delete);
            let _ = work.tx.send(res);
            match open_log_for_append(log_path) {
                Ok(new_file) => file = new_file,
                Err(err) => {
                    tracing::error!(error = ?err, "reopen wal log failed after compact");
                    return;
                }
            }
        }
    }
}

/// Append a sequence of commit entries to the WAL file.
fn append_entries(file: &mut File, entries: &[CommitLogEntry]) -> std::io::Result<()> {
    for entry in entries {
        let payload = encode_entry(entry);
        write_record(file, &payload)?;
    }
    file.flush()
}

/// Write a single length-prefixed record with CRC32 checksum.
fn write_record(file: &mut File, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len() as u32;
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let checksum = hasher.finalize();
    file.write_all(&len.to_be_bytes())?;
    file.write_all(&checksum.to_be_bytes())?;
    file.write_all(payload)?;
    Ok(())
}

/// Read and decode all WAL records from disk.
fn read_log_entries(path: &Path) -> anyhow::Result<Vec<CommitLogEntry>> {
    let file = match File::open(path) {
        Ok(file) => file,
        // Missing WAL means no entries to replay.
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut reader = std::io::BufReader::new(file);
    let mut entries = Vec::new();
    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            // EOF means we have read all complete records.
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf)?;
        let expected_crc = u32::from_be_bytes(crc_buf);
        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload)?;
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let actual_crc = hasher.finalize();
        // Detect partial/corrupted records.
        anyhow::ensure!(actual_crc == expected_crc, "wal checksum mismatch");
        entries.push(decode_entry(&payload)?);
    }
    Ok(entries)
}

/// Compact the WAL by removing up to `max_delete` entries whose txns executed.
fn compact_log(
    log_path: &Path,
    executed: &mut HashSet<TxnId>,
    max_delete: usize,
) -> anyhow::Result<usize> {
    if max_delete == 0 {
        // Nothing to delete.
        return Ok(0);
    }

    let entries = read_log_entries(log_path)?;
    if entries.is_empty() {
        // No data to compact.
        return Ok(0);
    }

    // Write a new temp log file containing retained entries.
    let tmp_path = log_path.with_extension("log.tmp");
    let mut out = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .context("open wal compact temp")?;

    let mut removed = 0usize;
    let mut retained = 0usize;
    for entry in &entries {
        // Only remove entries that were executed, up to max_delete.
        let remove = removed < max_delete && executed.contains(&entry.txn_id);
        if remove {
            removed += 1;
            continue;
        }
        let payload = encode_entry(entry);
        write_record(&mut out, &payload)?;
        retained += 1;
    }

    out.flush()?;
    out.sync_all()?;
    fs::rename(&tmp_path, log_path).context("replace wal log")?;

    if removed > 0 {
        // Prune executed set so it does not grow unbounded.
        let mut pruned = 0usize;
        executed.retain(|txn_id| {
            if pruned >= removed {
                return true;
            }
            if entries.iter().any(|entry| entry.txn_id == *txn_id) {
                pruned += 1;
                return false;
            }
            true
        });
    }

    if retained == 0 {
        // Ensure the log file exists after compaction.
        let _ = open_log_for_append(log_path)?;
    }

    Ok(removed)
}

/// Open the WAL log file for appending.
fn open_log_for_append(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

/// Background thread that performs fsyncs on demand.
///
/// Inputs:
/// - `path`: WAL file to fsync.
/// - `sync_state_path`: sidecar file that tracks fsync-confirmed WAL offset.
/// - `mode`: sync strategy used for each persist signal.
/// - `rx`: trigger channel fed by the WAL writer thread.
fn persist_loop(path: &Path, sync_state_path: &Path, mode: SyncMode, rx: mpsc::Receiver<()>) {
    while rx.recv().is_ok() {
        // Drain any extra signals so we only sync once per burst.
        while rx.try_recv().is_ok() {}
        let Ok(file) = File::open(path) else {
            continue;
        };
        let _ = sync_file_with_state(&file, mode, sync_state_path);
    }
}

/// Perform the configured fsync mode on the WAL file.
fn sync_file(file: &File, mode: SyncMode) -> std::io::Result<()> {
    match mode {
        SyncMode::None => Ok(()),
        SyncMode::Data => {
            let start = Instant::now();
            let res = file.sync_data();
            WAL_STATS.record_fsync(start.elapsed());
            res
        }
        SyncMode::All => {
            let start = Instant::now();
            let res = file.sync_all();
            WAL_STATS.record_fsync(start.elapsed());
            res
        }
    }
}

/// Perform fsync and persist the confirmed WAL offset to a sidecar file.
///
/// Inputs:
/// - `file`: opened WAL file descriptor.
/// - `mode`: fsync mode to apply.
/// - `sync_state_path`: sidecar path storing last fsync-confirmed WAL length.
///
/// Design:
/// - Only records an offset when real fsync work is requested.
/// - `SyncMode::None` intentionally performs no sidecar update.
fn sync_file_with_state(
    file: &File,
    mode: SyncMode,
    sync_state_path: &Path,
) -> std::io::Result<()> {
    if matches!(mode, SyncMode::None) {
        return Ok(());
    }
    sync_file(file, mode)?;
    let synced_len = file.metadata()?.len();
    write_synced_offset(sync_state_path, synced_len)
}

/// Write the most recently fsync-confirmed WAL byte offset to the sidecar file.
///
/// Inputs:
/// - `path`: sidecar file path.
/// - `offset`: last WAL length known to be durable.
///
/// Design:
/// - Writes through a temp file + rename to avoid torn state files.
fn write_synced_offset(path: &Path, offset: u64) -> std::io::Result<()> {
    let tmp_path = path.with_extension("synced.tmp");
    let mut out = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)?;
    out.write_all(&offset.to_be_bytes())?;
    out.sync_all()?;
    fs::rename(tmp_path, path)?;
    Ok(())
}

/// Read the fsync-confirmed WAL byte offset from the sidecar file.
///
/// Inputs:
/// - `path`: sidecar file path.
///
/// Output:
/// - `Ok(0)` when the sidecar is missing or malformed.
/// - `Ok(offset)` when a previously recorded durable offset exists.
fn read_synced_offset(path: &Path) -> std::io::Result<u64> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };
    let mut buf = [0u8; 8];
    match file.read_exact(&mut buf) {
        Ok(()) => Ok(u64::from_be_bytes(buf)),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(0),
        Err(err) => Err(err),
    }
}

/// Fault-injection hook: truncate unsynced WAL tail before opening the worker.
///
/// Inputs:
/// - `log_path`: WAL log file path.
/// - `sync_state_path`: sidecar that records durable WAL offset.
///
/// Design:
/// - Enabled only when `HOLO_WAL_FAULT_TRUNCATE_UNSYNCED_ON_OPEN=true`.
/// - Simulates power-loss by dropping bytes beyond the last durable offset.
fn maybe_fault_truncate_unsynced_tail(
    log_path: &Path,
    sync_state_path: &Path,
) -> anyhow::Result<()> {
    if !read_env_bool(WAL_FAULT_TRUNCATE_UNSYNCED_ON_OPEN, false) {
        return Ok(());
    }
    let current_len = match fs::metadata(log_path) {
        Ok(meta) => meta.len(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err).context("wal fault-injection stat log"),
    };
    if current_len == 0 {
        return Ok(());
    }
    let synced_len = read_synced_offset(sync_state_path)
        .context("wal fault-injection read synced offset")?
        .min(current_len);
    if synced_len >= current_len {
        return Ok(());
    }
    let file = OpenOptions::new()
        .write(true)
        .open(log_path)
        .context("wal fault-injection open log for truncate")?;
    file.set_len(synced_len)
        .context("wal fault-injection truncate unsynced tail")?;
    tracing::warn!(
        current_len = current_len,
        synced_len = synced_len,
        "wal fault injection truncated unsynced tail"
    );
    Ok(())
}

/// Encode a commit log entry to a compact binary representation.
fn encode_entry(entry: &CommitLogEntry) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(8 + 8 + 8 + 4 + (entry.deps.len() * 16) + 4 + entry.command.len());
    out.extend_from_slice(&entry.txn_id.node_id.to_be_bytes());
    out.extend_from_slice(&entry.txn_id.counter.to_be_bytes());
    out.extend_from_slice(&entry.seq.to_be_bytes());
    out.extend_from_slice(&(entry.deps.len() as u32).to_be_bytes());
    for dep in &entry.deps {
        out.extend_from_slice(&dep.node_id.to_be_bytes());
        out.extend_from_slice(&dep.counter.to_be_bytes());
    }
    out.extend_from_slice(&(entry.command.len() as u32).to_be_bytes());
    out.extend_from_slice(&entry.command);
    out
}

/// Return the encoded size for a commit entry.
fn encoded_len(entry: &CommitLogEntry) -> usize {
    8 + 8 + 8 + 4 + (entry.deps.len() * 16) + 4 + entry.command.len()
}

/// Decode a commit entry from the WAL record payload.
fn decode_entry(buf: &[u8]) -> anyhow::Result<CommitLogEntry> {
    let mut offset = 0usize;
    let node_id = read_u64_at(buf, &mut offset)?;
    let counter = read_u64_at(buf, &mut offset)?;
    let seq = read_u64_at(buf, &mut offset)?;
    let dep_len = read_u32_at(buf, &mut offset)? as usize;
    let mut deps = Vec::with_capacity(dep_len);
    for _ in 0..dep_len {
        let dep_node = read_u64_at(buf, &mut offset)?;
        let dep_counter = read_u64_at(buf, &mut offset)?;
        deps.push(TxnId {
            node_id: dep_node,
            counter: dep_counter,
        });
    }
    let cmd_len = read_u32_at(buf, &mut offset)? as usize;
    anyhow::ensure!(offset + cmd_len <= buf.len(), "wal entry short command");
    let command = buf[offset..offset + cmd_len].to_vec();
    Ok(CommitLogEntry {
        txn_id: TxnId { node_id, counter },
        seq,
        deps,
        command,
    })
}

#[cfg(feature = "raft-engine")]
fn entry_key(index: u64) -> [u8; 9] {
    let mut out = [0u8; 9];
    out[0] = WAL_ENTRY_PREFIX;
    out[1..].copy_from_slice(&index.to_be_bytes());
    out
}

#[cfg(feature = "raft-engine")]
fn decode_u64(buf: &[u8]) -> anyhow::Result<u64> {
    anyhow::ensure!(buf.len() == 8, "raft wal short u64");
    let mut raw = [0u8; 8];
    raw.copy_from_slice(buf);
    Ok(u64::from_be_bytes(raw))
}

#[cfg(feature = "raft-engine")]
fn encode_groups(groups: &HashSet<GroupId>) -> Vec<u8> {
    let mut ids: Vec<GroupId> = groups.iter().copied().collect();
    ids.sort_unstable();
    let mut out = Vec::with_capacity(ids.len() * 8);
    for id in ids {
        out.extend_from_slice(&id.to_be_bytes());
    }
    out
}

#[cfg(feature = "raft-engine")]
fn decode_groups(buf: &[u8]) -> anyhow::Result<Vec<GroupId>> {
    anyhow::ensure!(buf.len() % 8 == 0, "raft wal group list length");
    let mut groups = Vec::with_capacity(buf.len() / 8);
    for chunk in buf.chunks_exact(8) {
        groups.push(decode_u64(chunk)?);
    }
    Ok(groups)
}

#[cfg(feature = "raft-engine")]
fn read_last_index(engine: &RaftEngine, group_id: GroupId) -> anyhow::Result<Option<u64>> {
    let Some(buf) = engine.get(group_id, WAL_META_LAST_INDEX_KEY) else {
        return Ok(None);
    };
    Ok(Some(decode_u64(&buf)?))
}

#[cfg(feature = "raft-engine")]
fn read_compacted_floor(engine: &RaftEngine, group_id: GroupId) -> anyhow::Result<Option<u64>> {
    let Some(buf) = engine.get(group_id, WAL_META_COMPACTED_FLOOR_KEY) else {
        return Ok(None);
    };
    Ok(Some(decode_u64(&buf)?))
}

#[cfg(feature = "raft-engine")]
fn read_durable_floor(engine: &RaftEngine, group_id: GroupId) -> anyhow::Result<Option<u64>> {
    let Some(buf) = engine.get(group_id, WAL_META_DURABLE_FLOOR_KEY) else {
        return Ok(None);
    };
    Ok(Some(decode_u64(&buf)?))
}

/// Read an env var as u64 with a default.
fn read_env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| u64::from_str(&v).ok())
        .unwrap_or(default)
}

/// Read an env var as usize with a default.
fn read_env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| usize::from_str(&v).ok())
        .unwrap_or(default)
}

/// Read an env var as bool with a default, accepting common truthy values.
fn read_env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|v| {
            matches!(
                v.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "y" | "on"
            )
        })
        .unwrap_or(default)
}

/// Parse the requested persistence mode from a string.
fn parse_persist_mode(value: Option<&str>) -> Option<SyncMode> {
    match value.map(|v| v.to_ascii_lowercase()) {
        // "none" disables syncing entirely.
        Some(v) if v == "none" => None,
        // "buffer" means rely on OS buffering (no explicit sync).
        Some(v) if v == "buffer" => Some(SyncMode::None),
        // "sync_data" maps to `sync_data`.
        Some(v) if v == "sync_data" => Some(SyncMode::Data),
        // "sync_all" maps to `sync_all`.
        Some(v) if v == "sync_all" => Some(SyncMode::All),
        // Unknown strings default to safest sync mode.
        Some(_) => Some(SyncMode::All),
        // Unset defaults to safest sync mode.
        None => Some(SyncMode::All),
    }
}

/// Return current epoch time in microseconds (saturating).
fn epoch_micros() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.as_micros().min(u128::from(u64::MAX)) as u64
}

/// Read a u64 from a buffer at the current offset.
fn read_u64_at(data: &[u8], offset: &mut usize) -> anyhow::Result<u64> {
    anyhow::ensure!(*offset + 8 <= data.len(), "wal entry short u64");
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*offset..*offset + 8]);
    *offset += 8;
    Ok(u64::from_be_bytes(buf))
}

/// Read a u32 from a buffer at the current offset.
fn read_u32_at(data: &[u8], offset: &mut usize) -> anyhow::Result<u32> {
    anyhow::ensure!(*offset + 4 <= data.len(), "wal entry short u32");
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*offset..*offset + 4]);
    *offset += 4;
    Ok(u32::from_be_bytes(buf))
}

#[cfg(all(test, feature = "raft-engine"))]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use holo_accord::accord::TXN_COUNTER_SHARD_SHIFT;

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_wal_dir(name: &str) -> PathBuf {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("holostore-{name}-{id}"));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp wal dir");
        path
    }

    fn make_txn(group_id: GroupId, seq: u64) -> TxnId {
        TxnId {
            node_id: 1,
            counter: (group_id << TXN_COUNTER_SHARD_SHIFT) | seq,
        }
    }

    fn make_entry(group_id: GroupId, seq: u64) -> CommitLogEntry {
        CommitLogEntry {
            txn_id: make_txn(group_id, seq),
            seq,
            deps: Vec::new(),
            command: format!("set:{seq}").into_bytes(),
        }
    }

    #[test]
    fn raft_wal_persists_compaction_floor_across_restart() {
        let dir = temp_wal_dir("wal-floor");
        let group_id = 7;

        let wal = RaftEngineWal::open_dir(&dir).expect("open raft wal");
        let entries: Vec<CommitLogEntry> = (1..=8).map(|i| make_entry(group_id, i)).collect();
        wal.append_commits(entries).expect("append entries");

        for seq in 1..=5 {
            wal.mark_executed(make_txn(group_id, seq))
                .expect("mark executed");
        }
        wal.mark_durable_checkpoint()
            .expect("mark durable checkpoint");
        let removed = wal.compact(100).expect("compact");
        assert_eq!(
            removed, 5,
            "expected prefix compaction to remove first 5 entries"
        );
        assert_eq!(
            read_compacted_floor(&wal.engine, group_id).expect("read compacted floor"),
            Some(5)
        );
        drop(wal);

        let wal = RaftEngineWal::open_dir(&dir).expect("re-open raft wal");
        {
            let state = wal.state.lock().expect("state lock");
            assert_eq!(state.compacted_floor.get(&group_id).copied(), Some(5));
        }
        let loaded = wal.load().expect("load entries");
        let mut seqs: Vec<u64> = loaded
            .into_iter()
            .filter(|entry| txn_group_id(entry.txn_id) == group_id)
            .map(|entry| entry.txn_id.counter & ((1u64 << TXN_COUNTER_SHARD_SHIFT) - 1))
            .collect();
        seqs.sort_unstable();
        assert_eq!(
            seqs,
            vec![6, 7, 8],
            "replay must start at compacted floor + 1"
        );

        for seq in 6..=8 {
            wal.mark_executed(make_txn(group_id, seq))
                .expect("mark executed after restart");
        }
        wal.mark_durable_checkpoint()
            .expect("mark durable checkpoint after restart");
        let removed = wal.compact(100).expect("second compact");
        assert_eq!(removed, 3);
        assert_eq!(
            read_compacted_floor(&wal.engine, group_id).expect("read compacted floor"),
            Some(8)
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
