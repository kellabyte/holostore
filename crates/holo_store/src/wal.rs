//! Write-ahead log (WAL) implementation for durable Accord commits.
//!
//! This module provides a file-backed `CommitLog` implementation that batches
//! writes, optionally fsyncs, and supports compaction based on executed txns.

#[cfg(feature = "raft-engine")]
use std::collections::HashMap;
use std::collections::HashSet;
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
use holo_accord::accord::{CommitLog, CommitLogEntry, TxnId};

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
    entry: CommitLogEntry,
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
    /// This configures batching and persistence based on env overrides.
    pub fn open_dir(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir).context("create wal dir")?;
        let log_path = dir.join(WAL_LOG_FILE);

        // Read persistence and batching knobs from environment variables.
        let persist_every = read_env_u64("HOLO_WAL_PERSIST_EVERY", WAL_PERSIST_EVERY);
        let persist_interval_us =
            read_env_u64("HOLO_WAL_PERSIST_INTERVAL_US", WAL_PERSIST_INTERVAL_US);
        let persist_mode = parse_persist_mode(env::var("HOLO_WAL_PERSIST_MODE").ok().as_deref());
        let persist_async = read_env_bool("HOLO_WAL_PERSIST_ASYNC", false);

        let commit_batch_max =
            read_env_usize("HOLO_WAL_COMMIT_BATCH_MAX", WAL_COMMIT_BATCH_MAX).max(1);
        let commit_batch_wait_us =
            read_env_u64("HOLO_WAL_COMMIT_BATCH_WAIT_US", WAL_COMMIT_BATCH_WAIT_US);

        // Decide whether to sync inline or via a background thread.
        let (persist_mode, persist_tx) = if persist_async {
            match persist_mode {
                Some(mode) => {
                    let (tx, rx) = mpsc::channel();
                    let path = log_path.clone();
                    thread::Builder::new()
                        .name("wal-persist".to_string())
                        .spawn(move || persist_loop(&path, mode, rx))
                        .context("spawn wal persist thread")?;
                    // When async, the worker thread owns syncs and the writer skips them.
                    (SyncMode::None, Some(tx))
                }
                // Async requested but no persist mode provided means no syncing.
                None => (SyncMode::None, None),
            }
        } else {
            // Inline persistence in the commit worker thread.
            (persist_mode.unwrap_or(SyncMode::None), None)
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

    /// Append multiple commits using a single batch command to the worker.
    pub fn append_commits(&self, entries: Vec<CommitLogEntry>) -> anyhow::Result<()> {
        if entries.is_empty() {
            // Nothing to append.
            return Ok(());
        }
        let (tx, rx) = mpsc::channel();
        let mut works = Vec::with_capacity(entries.len());
        for entry in entries {
            works.push(CommitWork {
                entry,
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
    fn append_commit(&self, entry: CommitLogEntry) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(WalCommand::Append(CommitWork { entry, tx }))
            .map_err(|_| anyhow::anyhow!("wal worker closed"))?;
        rx.recv().context("wal append response dropped")?
    }

    /// Append multiple commits in one batch.
    fn append_commits(&self, entries: Vec<CommitLogEntry>) -> anyhow::Result<()> {
        FileWal::append_commits(self, entries)
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
const WAL_ENTRY_PREFIX: u8 = b'e';

#[cfg(feature = "raft-engine")]
#[derive(Debug)]
struct RaftWalState {
    last_index: HashMap<GroupId, u64>,
    groups: HashSet<GroupId>,
    txn_index: HashMap<TxnId, u64>,
    executed: HashSet<TxnId>,
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
            executed: HashSet::new(),
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

    fn persist_should_sync(
        &self,
        state: &RaftWalState,
        batch_len: usize,
        now: u64,
    ) -> (bool, u64, u64) {
        if matches!(self.persist_mode, SyncMode::None) {
            return (false, state.pending_count, state.last_persist_us);
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
        (sync, pending, last_persist_us)
    }
}

#[cfg(feature = "raft-engine")]
impl CommitLog for RaftEngineWal {
    fn append_commit(&self, entry: CommitLogEntry) -> anyhow::Result<()> {
        self.append_commits(vec![entry])
    }

    fn append_commits(&self, entries: Vec<CommitLogEntry>) -> anyhow::Result<()> {
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
            assigned.push((group_id, next_index, entry));
        }

        let (sync, pending_count, last_persist_us) =
            self.persist_should_sync(&state, assigned.len(), now);

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

        for (_, index, entry) in assigned {
            state.txn_index.insert(entry.txn_id, index);
        }
        state.pending_count = pending_count;
        state.last_persist_us = last_persist_us;
        Ok(())
    }

    fn mark_executed(&self, txn_id: TxnId) -> anyhow::Result<()> {
        let mut state = self.state.lock().expect("raft wal state lock");
        state.executed.insert(txn_id);
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
            if last_index == 0 {
                continue;
            }
            for index in 1..=last_index {
                let key = entry_key(index);
                let Some(payload) = self.engine.get(group_id, &key) else {
                    continue;
                };
                let entry = decode_entry(&payload)?;
                state.txn_index.insert(entry.txn_id, index);
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    fn compact(&self, max_delete: usize) -> anyhow::Result<usize> {
        if max_delete == 0 {
            return Ok(0);
        }

        let mut state = self.state.lock().expect("raft wal state lock");
        if state.executed.is_empty() {
            return Ok(0);
        }

        let mut candidates = Vec::new();
        for txn_id in &state.executed {
            if let Some(index) = state.txn_index.get(txn_id).copied() {
                candidates.push((index, *txn_id));
            }
        }
        candidates.sort_by_key(|(index, _)| *index);
        let to_remove: Vec<(u64, TxnId)> = candidates.into_iter().take(max_delete).collect();
        if to_remove.is_empty() {
            return Ok(0);
        }

        let mut batch = LogBatch::with_capacity(to_remove.len());
        for (index, txn_id) in &to_remove {
            let group_id = txn_group_id(*txn_id);
            let key = entry_key(*index);
            batch.delete(group_id, key.to_vec());
        }

        self.engine
            .write(&mut batch, false)
            .context("raft wal compact write")?;

        for (_, txn_id) in &to_remove {
            state.executed.remove(txn_id);
            state.txn_index.remove(txn_id);
        }

        Ok(to_remove.len())
    }
}

/// Worker loop that batches WAL commands and persists log entries.
fn wal_worker(
    log_path: &Path,
    rx: mpsc::Receiver<WalCommand>,
    persist_every: u64,
    persist_interval_us: u64,
    persist_mode: SyncMode,
    persist_tx: Option<mpsc::Sender<()>>,
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
        let mut compact_req: Option<CompactWork> = None;

        for cmd in commands {
            match cmd {
                WalCommand::Append(work) => {
                    append_batch.push(work.entry);
                    append_resps.push(work.tx);
                }
                WalCommand::AppendBatch(works) => {
                    for work in works {
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
            if hit_count || hit_interval {
                pending_count = 0;
                last_persist_us = now;
                if let Some(tx) = &persist_tx {
                    // Signal the async persist thread.
                    let _ = tx.send(());
                } else if let Err(err) = sync_file(&file, persist_mode) {
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
fn persist_loop(path: &Path, mode: SyncMode, rx: mpsc::Receiver<()>) {
    while rx.recv().is_ok() {
        // Drain any extra signals so we only sync once per burst.
        while rx.try_recv().is_ok() {}
        let Ok(file) = File::open(path) else {
            continue;
        };
        let _ = sync_file(&file, mode);
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
