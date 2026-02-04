use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use std::{env, str::FromStr};

use anyhow::Context;
use crc32fast::Hasher;
use holo_accord::accord::{CommitLog, CommitLogEntry, TxnId};

const WAL_PERSIST_EVERY: u64 = 256;
const WAL_PERSIST_INTERVAL_US: u64 = 2_000;
const WAL_COMMIT_BATCH_MAX: usize = 64;
const WAL_COMMIT_BATCH_WAIT_US: u64 = 200;

const WAL_LOG_FILE: &str = "wal.log";

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

    fn record_fsync(&self, dur: Duration) {
        let us = dur.as_micros().min(u128::from(u64::MAX)) as u64;
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
        self.fsync_total_us.fetch_add(us, Ordering::Relaxed);
        self.fsync_max_us.fetch_max(us, Ordering::Relaxed);
    }

    fn record_batch(&self, items: u64, bytes: u64) {
        self.batch_count.fetch_add(1, Ordering::Relaxed);
        self.batch_items.fetch_add(items, Ordering::Relaxed);
        self.batch_max_items.fetch_max(items, Ordering::Relaxed);
        self.batch_total_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.batch_max_bytes.fetch_max(bytes, Ordering::Relaxed);
    }

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

static WAL_STATS: WalStats = WalStats::new();

pub fn stats_snapshot() -> WalStatsSnapshot {
    WAL_STATS.snapshot_and_reset()
}

struct CommitWork {
    entry: CommitLogEntry,
    tx: mpsc::Sender<anyhow::Result<()>>,
}

struct CompactWork {
    max_delete: usize,
    tx: mpsc::Sender<anyhow::Result<usize>>,
}

enum WalCommand {
    Append(CommitWork),
    AppendBatch(Vec<CommitWork>),
    MarkExecuted(TxnId),
    Compact(CompactWork),
}

#[derive(Clone, Copy, Debug)]
enum SyncMode {
    None,
    Data,
    All,
}

pub struct FileWal {
    dir: PathBuf,
    log_path: PathBuf,
    tx: mpsc::Sender<WalCommand>,
}

impl FileWal {
    pub fn open_dir(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir).context("create wal dir")?;
        let log_path = dir.join(WAL_LOG_FILE);

        let persist_every = read_env_u64("HOLO_WAL_PERSIST_EVERY", WAL_PERSIST_EVERY);
        let persist_interval_us =
            read_env_u64("HOLO_WAL_PERSIST_INTERVAL_US", WAL_PERSIST_INTERVAL_US);
        let persist_mode = parse_persist_mode(
            env::var("HOLO_WAL_PERSIST_MODE")
                .ok()
                .as_deref(),
        );
        let persist_async = read_env_bool("HOLO_WAL_PERSIST_ASYNC", false);

        let commit_batch_max =
            read_env_usize("HOLO_WAL_COMMIT_BATCH_MAX", WAL_COMMIT_BATCH_MAX).max(1);
        let commit_batch_wait_us =
            read_env_u64("HOLO_WAL_COMMIT_BATCH_WAIT_US", WAL_COMMIT_BATCH_WAIT_US);

        let (persist_mode, persist_tx) = if persist_async {
            match persist_mode {
                Some(mode) => {
                    let (tx, rx) = mpsc::channel();
                    let path = log_path.clone();
                    thread::Builder::new()
                        .name("wal-persist".to_string())
                        .spawn(move || persist_loop(&path, mode, rx))
                        .context("spawn wal persist thread")?;
                    (SyncMode::None, Some(tx))
                }
                None => (SyncMode::None, None),
            }
        } else {
            (persist_mode.unwrap_or(SyncMode::None), None)
        };

        let (tx, rx) = mpsc::channel();
        let worker_log_path = log_path.clone();
        thread::Builder::new()
            .name("wal-commit".to_string())
            .spawn(move || {
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

    fn load_entries(&self) -> anyhow::Result<Vec<CommitLogEntry>> {
        read_log_entries(&self.log_path)
    }

    pub fn append_commits(&self, entries: Vec<CommitLogEntry>) -> anyhow::Result<()> {
        if entries.is_empty() {
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
    fn append_commit(&self, entry: CommitLogEntry) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(WalCommand::Append(CommitWork { entry, tx }))
            .map_err(|_| anyhow::anyhow!("wal worker closed"))?;
        rx.recv().context("wal append response dropped")?
    }

    fn append_commits(&self, entries: Vec<CommitLogEntry>) -> anyhow::Result<()> {
        FileWal::append_commits(self, entries)
    }

    fn mark_executed(&self, txn_id: TxnId) -> anyhow::Result<()> {
        let _ = self.tx.send(WalCommand::MarkExecuted(txn_id));
        Ok(())
    }

    fn load(&self) -> anyhow::Result<Vec<CommitLogEntry>> {
        self.load_entries()
    }

    fn compact(&self, max_delete: usize) -> anyhow::Result<usize> {
        if max_delete == 0 {
            return Ok(0);
        }
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(WalCommand::Compact(CompactWork { max_delete, tx }))
            .map_err(|_| anyhow::anyhow!("wal worker closed"))?;
        rx.recv().context("wal compact response dropped")?
    }
}

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
                        disconnected = true;
                        break;
                    }
                }

                let Some(deadline) = deadline else {
                    break;
                };
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let remaining = deadline.saturating_duration_since(now);
                match rx.recv_timeout(remaining) {
                    Ok(cmd) => commands.push(cmd),
                    Err(mpsc::RecvTimeoutError::Timeout) => break,
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
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
                    executed.insert(txn_id);
                }
                WalCommand::Compact(work) => {
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
            let hit_count = persist_every > 0 && pending_count >= persist_every;
            let hit_interval =
                persist_interval_us > 0 && now.saturating_sub(last_persist_us) >= persist_interval_us;
            if hit_count || hit_interval {
                pending_count = 0;
                last_persist_us = now;
                if let Some(tx) = &persist_tx {
                    let _ = tx.send(());
                } else if let Err(err) = sync_file(&file, persist_mode) {
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

fn append_entries(file: &mut File, entries: &[CommitLogEntry]) -> std::io::Result<()> {
    for entry in entries {
        let payload = encode_entry(entry);
        write_record(file, &payload)?;
    }
    file.flush()
}

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

fn read_log_entries(path: &Path) -> anyhow::Result<Vec<CommitLogEntry>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    };
    let mut reader = std::io::BufReader::new(file);
    let mut entries = Vec::new();
    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
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
        anyhow::ensure!(actual_crc == expected_crc, "wal checksum mismatch");
        entries.push(decode_entry(&payload)?);
    }
    Ok(entries)
}

fn compact_log(
    log_path: &Path,
    executed: &mut HashSet<TxnId>,
    max_delete: usize,
) -> anyhow::Result<usize> {
    if max_delete == 0 {
        return Ok(0);
    }

    let entries = read_log_entries(log_path)?;
    if entries.is_empty() {
        return Ok(0);
    }

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

fn open_log_for_append(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

fn persist_loop(path: &Path, mode: SyncMode, rx: mpsc::Receiver<()>) {
    while rx.recv().is_ok() {
        while rx.try_recv().is_ok() {}
        let Ok(file) = File::open(path) else {
            continue;
        };
        let _ = sync_file(&file, mode);
    }
}

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

fn encode_entry(entry: &CommitLogEntry) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        8 + 8 + 8 + 4 + (entry.deps.len() * 16) + 4 + entry.command.len(),
    );
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

fn encoded_len(entry: &CommitLogEntry) -> usize {
    8 + 8 + 8 + 4 + (entry.deps.len() * 16) + 4 + entry.command.len()
}

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

fn read_env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| u64::from_str(&v).ok())
        .unwrap_or(default)
}

fn read_env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| usize::from_str(&v).ok())
        .unwrap_or(default)
}

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

fn parse_persist_mode(value: Option<&str>) -> Option<SyncMode> {
    match value.map(|v| v.to_ascii_lowercase()) {
        Some(v) if v == "none" => None,
        Some(v) if v == "buffer" => Some(SyncMode::None),
        Some(v) if v == "sync_data" => Some(SyncMode::Data),
        Some(v) if v == "sync_all" => Some(SyncMode::All),
        Some(_) => Some(SyncMode::All),
        None => Some(SyncMode::All),
    }
}

fn epoch_micros() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.as_micros().min(u128::from(u64::MAX)) as u64
}

fn read_u64_at(data: &[u8], offset: &mut usize) -> anyhow::Result<u64> {
    anyhow::ensure!(*offset + 8 <= data.len(), "wal entry short u64");
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[*offset..*offset + 8]);
    *offset += 8;
    Ok(u64::from_be_bytes(buf))
}

fn read_u32_at(data: &[u8], offset: &mut usize) -> anyhow::Result<u32> {
    anyhow::ensure!(*offset + 4 <= data.len(), "wal entry short u32");
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&data[*offset..*offset + 4]);
    *offset += 4;
    Ok(u32::from_be_bytes(buf))
}
