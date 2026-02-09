//! HoloStore node binary entry point.
//!
//! This file wires together the Accord consensus groups, KV engine, WAL, gRPC
//! transport, and Redis protocol server. It also hosts the CLI and runtime
//! configuration, as well as performance/statistics logging.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, IsTerminal, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use clap::{Parser, Subcommand};
use fjall::{Keyspace, PartitionCreateOptions};
use futures_util::stream::{FuturesUnordered, StreamExt};
use holo_accord::accord::{self, GroupId, NodeId, TxnId};
use sysinfo::{Pid, System};
use tokio::sync::{mpsc, oneshot, Semaphore};

include!(concat!(env!("OUT_DIR"), "/volo_gen.rs"));

mod cluster;
mod kv;
mod load;
mod range_manager;
mod rebalance_manager;
mod redis_server;
mod rpc_service;
mod transport;
mod wal;

use cluster::{
    ClusterState, ClusterStateMachine, ClusterStateStore, MemberInfo, MemberState, ReplicaRole,
    ShardDesc,
};
use kv::{FjallEngine, KvEngine};
use load::ShardLoadTracker;
use range_manager::RangeManagerConfig;
use rebalance_manager::RebalanceManagerConfig;
use rpc_service::RpcService;
use transport::GrpcTransport;

/// Group id used for the membership/control group.
const GROUP_MEMBERSHIP: GroupId = 0;
/// Base group id for data shards (shard index is added to this base).
const GROUP_DATA_BASE: GroupId = 1;

/// CLI entry point wrapper.
#[derive(Parser, Debug)]
#[command(name = "holo-store")]
struct Args {
    #[command(subcommand)]
    cmd: Command,
}

/// Top-level CLI subcommands.
#[derive(Subcommand, Debug)]
enum Command {
    Node(NodeArgs),
}

/// CLI options for running a node.
#[derive(Parser, Debug)]
struct NodeArgs {
    #[arg(long)]
    node_id: u64,

    #[arg(long)]
    listen_redis: SocketAddr,

    #[arg(long)]
    listen_grpc: SocketAddr,

    #[arg(long)]
    bootstrap: bool,

    #[arg(long)]
    join: Option<SocketAddr>,

    /// Comma-separated list like: `1@127.0.0.1:50051,2@127.0.0.1:50052,3@127.0.0.1:50053`
    #[arg(long)]
    initial_members: String,

    #[arg(long)]
    data_dir: String,

    /// WAL engine: raft-engine (default) or file.
    #[arg(long, env = "HOLO_WAL_ENGINE", default_value = "raft-engine")]
    wal_engine: WalEngine,

    /// Maximum shard slots for dynamic ranges.
    ///
    /// The node pre-creates this many data group slots at startup; the range
    /// manager can then split ranges dynamically up to this cap.
    #[arg(long = "max-shards", env = "HOLO_MAX_SHARDS", default_value_t = 1)]
    data_shards: usize,

    /// Initial number of key ranges to create in cluster metadata.
    ///
    /// CockroachDB boots with a small number of ranges and splits dynamically.
    /// We default to 1 full-keyspace range.
    #[arg(long, env = "HOLO_INITIAL_RANGES", default_value_t = 1)]
    initial_ranges: usize,

    /// Range manager evaluation interval (ms).
    #[arg(long, env = "HOLO_RANGE_MGR_INTERVAL_MS", default_value_t = 500)]
    range_mgr_interval_ms: u64,

    /// Split a range when it grows beyond this threshold.
    ///
    /// Implementation note: this is an approximation today. We use "SET ops
    /// coordinated by this node for the shard index since its last split" as a
    /// proxy for key growth, to avoid expensive keyspace scans.
    ///
    /// Set to 0 to disable growth-based splitting.
    #[arg(long, env = "HOLO_RANGE_SPLIT_MIN_KEYS", default_value_t = 50_000)]
    range_split_min_keys: usize,

    /// Split a hot range when its sustained SET QPS exceeds this threshold.
    ///
    /// Set to 0 to disable load-based splitting (size-based splitting may still apply).
    #[arg(long, env = "HOLO_RANGE_SPLIT_MIN_QPS", default_value_t = 100_000)]
    range_split_min_qps: u64,

    /// Require the QPS threshold to be exceeded for this many consecutive
    /// evaluations before proposing a split.
    #[arg(long, env = "HOLO_RANGE_SPLIT_QPS_SUSTAIN", default_value_t = 3)]
    range_split_qps_sustain: u8,

    /// Per-shard cooldown after a split (ms) to avoid rapid re-splitting.
    #[arg(long, env = "HOLO_RANGE_SPLIT_COOLDOWN_MS", default_value_t = 10_000)]
    range_split_cooldown_ms: u64,

    /// Enable the background replica rebalancer and decommission drain workflow.
    #[arg(long, env = "HOLO_REBALANCE_ENABLED", default_value_t = true)]
    rebalance_enabled: bool,

    /// Rebalance manager evaluation interval (ms).
    #[arg(long, env = "HOLO_REBALANCE_INTERVAL_MS", default_value_t = 1000)]
    rebalance_interval_ms: u64,

    /// Max idle time for an in-flight replica move before the manager
    /// auto-aborts (or force-finalizes post-lease-transfer) in milliseconds.
    ///
    /// Set to 0 to disable timeout-based recovery.
    #[arg(long, env = "HOLO_REBALANCE_MOVE_TIMEOUT_MS", default_value_t = 180_000)]
    rebalance_move_timeout_ms: u64,

    /// How to map keys to data shards.
    ///
    /// `hash` preserves parallelism for workloads with common key prefixes
    /// (e.g. redis-benchmark's default `key:...`), because the varying suffix
    /// still spreads across shards.
    ///
    /// `range` uses the control-plane's lexicographic key ranges. This is
    /// Cockroach-style, but without real per-range data movement yet it can
    /// concentrate traffic into a single shard for prefix-heavy workloads.
    #[arg(long, env = "HOLO_ROUTING_MODE", default_value = "range")]
    routing_mode: RoutingMode,

    /// Disable automatic Fjall journal persistence (commit log is the durability source).
    #[arg(
        long,
        env = "HOLO_FJALL_MANUAL_JOURNAL_PERSIST",
        default_value_t = true
    )]
    fjall_manual_journal_persist: bool,

    /// Periodic Fjall journal fsync (ms). Omit or 0 to disable.
    #[arg(long, env = "HOLO_FJALL_FSYNC_MS")]
    fjall_fsync_ms: Option<u16>,

    /// RPC timeout for pre-accept/accept/recover (milliseconds).
    #[arg(long, env = "HOLO_RPC_TIMEOUT_MS", default_value_t = 2000)]
    rpc_timeout_ms: u64,

    /// Commit RPC timeout (milliseconds).
    #[arg(long, env = "HOLO_COMMIT_TIMEOUT_MS", default_value_t = 4000)]
    commit_timeout_ms: u64,

    /// End-to-end propose timeout (milliseconds).
    #[arg(long, env = "HOLO_PROPOSE_TIMEOUT_MS", default_value_t = 10000)]
    propose_timeout_ms: u64,

    /// Read mode: accord (default), quorum, or local.
    #[arg(long, env = "HOLO_READ_MODE", default_value = "accord")]
    read_mode: ReadMode,

    /// Read barrier timeout for accord reads (milliseconds).
    #[arg(long, env = "HOLO_READ_BARRIER_TIMEOUT_MS", default_value_t = 2000)]
    read_barrier_timeout_ms: u64,

    /// Fall back to quorum reads if read barrier times out.
    #[arg(
        long,
        env = "HOLO_READ_BARRIER_FALLBACK_QUORUM",
        default_value_t = false
    )]
    read_barrier_fallback_quorum: bool,

    /// Require last-committed responses from all peers (stronger read barrier).
    #[arg(long, env = "HOLO_READ_BARRIER_ALL_PEERS", default_value_t = true)]
    read_barrier_all_peers: bool,

    /// Log RPC batching stats every N milliseconds (0 disables).
    #[arg(long, env = "HOLO_RPC_STATS_INTERVAL_MS", default_value_t = 0)]
    rpc_stats_interval_ms: u64,

    /// Log per-peer RPC queue/inflight gauges every N milliseconds (0 disables).
    #[arg(long, env = "HOLO_RPC_QUEUE_STATS_INTERVAL_MS", default_value_t = 0)]
    rpc_queue_stats_interval_ms: u64,

    /// Log Accord executor stats every N milliseconds (0 disables).
    #[arg(long, env = "HOLO_ACCORD_STATS_INTERVAL_MS", default_value_t = 0)]
    accord_stats_interval_ms: u64,

    /// Log proposal timing stats every N milliseconds (0 disables).
    #[arg(long, env = "HOLO_PROPOSAL_STATS_INTERVAL_MS", default_value_t = 0)]
    proposal_stats_interval_ms: u64,

    /// Artificial delay injected into RPC handlers (milliseconds).
    #[arg(long, env = "HOLO_RPC_HANDLER_DELAY_MS", default_value_t = 0)]
    rpc_handler_delay_ms: u64,

    /// Default replication factor for new shards.
    #[arg(long, env = "HOLO_REPLICATION_FACTOR", default_value_t = 3)]
    replication_factor: usize,

    /// Consecutive execute-stall hits on a PreAccepted blocker before triggering recovery.
    #[arg(long, env = "HOLO_PREACCEPT_STALL_HITS", default_value_t = 3)]
    preaccept_stall_hits: u32,

    /// Max number of committed txns to apply in one executor batch.
    #[arg(long, env = "HOLO_ACCORD_EXECUTE_BATCH_MAX", default_value_t = 32)]
    accord_execute_batch_max: usize,

    /// Inline commands in accept/commit RPCs (disabling reduces payload size).
    #[arg(
        long,
        env = "HOLO_ACCORD_INLINE_COMMAND_IN_ACCEPT_COMMIT",
        default_value_t = false
    )]
    accord_inline_command_in_accept_commit: bool,

    /// Log process CPU/RSS stats every N milliseconds (0 disables).
    #[arg(long, env = "HOLO_PROC_STATS_INTERVAL_MS", default_value_t = 0)]
    proc_stats_interval_ms: u64,

    /// Also sample RSS via `ps` for unit cross-checking.
    #[arg(long, env = "HOLO_PROC_STATS_USE_PS", default_value_t = false)]
    proc_stats_use_ps: bool,

    /// Write per-peer RPC queue/sent/latency stats to CSV (appends).
    #[arg(long, env = "HOLO_PEER_STATS_CSV")]
    peer_stats_csv: Option<String>,

    /// Max in-flight RPC batches per peer (pre/accept/commit/recover).
    #[arg(long, env = "HOLO_RPC_INFLIGHT_LIMIT", default_value_t = 4)]
    rpc_inflight_limit: usize,

    /// Max number of consensus RPCs to coalesce into a batch.
    #[arg(long, env = "HOLO_RPC_BATCH_MAX", default_value_t = 64)]
    rpc_batch_max: usize,

    /// How long to wait to coalesce consensus RPCs into a batch (microseconds).
    #[arg(long, env = "HOLO_RPC_BATCH_WAIT_US", default_value_t = 200)]
    rpc_batch_wait_us: u64,

    /// Max number of SET operations to coalesce into one proposal.
    #[arg(long, env = "HOLO_CLIENT_SET_BATCH_MAX", default_value_t = 256)]
    client_set_batch_max: usize,

    /// Target max SET operations per proposal (used to split large batches).
    #[arg(long, env = "HOLO_CLIENT_SET_BATCH_TARGET", default_value_t = 128)]
    client_set_batch_target: usize,

    /// Force single-key proposals for SET (debug perf mode).
    #[arg(long, env = "HOLO_CLIENT_SINGLE_KEY_TXN", default_value_t = false)]
    client_single_key_txn: bool,

    /// Max number of commit-log entries to batch per write.
    #[arg(long, env = "HOLO_COMMIT_LOG_BATCH_MAX", default_value_t = 256)]
    commit_log_batch_max: usize,

    /// How long to wait to coalesce commit-log entries (microseconds).
    #[arg(long, env = "HOLO_COMMIT_LOG_BATCH_WAIT_US", default_value_t = 200)]
    commit_log_batch_wait_us: u64,

    /// Max number of GET keys to coalesce into one read batch.
    #[arg(long, env = "HOLO_CLIENT_GET_BATCH_MAX", default_value_t = 256)]
    client_get_batch_max: usize,

    /// How long to wait to coalesce client ops into a batch (microseconds).
    #[arg(long, env = "HOLO_CLIENT_BATCH_WAIT_US", default_value_t = 200)]
    client_batch_wait_us: u64,

    /// Max in-flight client batches.
    #[arg(long, env = "HOLO_CLIENT_BATCH_INFLIGHT", default_value_t = 4)]
    client_batch_inflight: usize,

    /// Queue depth for client batchers.
    #[arg(long, env = "HOLO_CLIENT_BATCH_QUEUE", default_value_t = 8192)]
    client_batch_queue: usize,

    /// Min adaptive in-flight limit.
    #[arg(long, env = "HOLO_RPC_INFLIGHT_MIN", default_value_t = 1)]
    rpc_inflight_min: usize,

    /// Max adaptive in-flight limit.
    #[arg(long, env = "HOLO_RPC_INFLIGHT_MAX", default_value_t = 16)]
    rpc_inflight_max: usize,

    /// Decrease inflight limit when wait max exceeds this (ms).
    #[arg(long, env = "HOLO_RPC_INFLIGHT_HIGH_WAIT_MS", default_value_t = 100.0)]
    rpc_inflight_high_wait_ms: f64,

    /// Increase inflight limit when wait max is below this (ms).
    #[arg(long, env = "HOLO_RPC_INFLIGHT_LOW_WAIT_MS", default_value_t = 5.0)]
    rpc_inflight_low_wait_ms: f64,

    /// Decrease inflight limit when queue exceeds this.
    #[arg(long, env = "HOLO_RPC_INFLIGHT_HIGH_QUEUE", default_value_t = 1024)]
    rpc_inflight_high_queue: u64,

    /// Increase inflight limit when queue is below this.
    #[arg(long, env = "HOLO_RPC_INFLIGHT_LOW_QUEUE", default_value_t = 64)]
    rpc_inflight_low_queue: u64,
}

/// Shared state used by both RPC and Redis servers.
#[derive(Clone)]
struct NodeState {
    initial_members: String,

    groups: HashMap<GroupId, Arc<accord::Group>>,
    data_handles: Vec<accord::Handle>,
    data_shards: usize,
    routing_mode: RoutingMode,
    node_id: NodeId,
    kv_engine: Arc<dyn KvEngine>,
    transport: Arc<GrpcTransport>,
    member_ids: Vec<NodeId>,
    read_mode: ReadMode,
    read_barrier_timeout: Duration,
    read_barrier_fallback_quorum: bool,
    read_barrier_all_peers: bool,
    proposal_stats: Arc<ProposalStats>,
    proposal_stats_enabled: bool,
    rpc_handler_stats: Arc<RpcHandlerStats>,
    rpc_handler_delay: Duration,
    client_set_tx: mpsc::Sender<BatchSetWork>,
    client_get_tx: mpsc::Sender<BatchGetWork>,
    client_batch_stats: Arc<ClientBatchStats>,
    client_set_batch_target: usize,
    client_inflight_ops: Arc<AtomicU64>,
    cluster_store: ClusterStateStore,
    meta_handle: accord::Handle,
    split_lock: Arc<std::sync::RwLock<()>>,
    shard_load: ShardLoadTracker,
    range_stats: Arc<LocalRangeStats>,
}

#[derive(Clone, Debug)]
struct LocalRangeStat {
    shard_id: u64,
    shard_index: usize,
    record_count: u64,
    is_leaseholder: bool,
}

/// One latest-visible KV entry used for replica backfill.
#[derive(Clone, Debug)]
pub(crate) struct RangeSnapshotLatestEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: kv::Version,
}

/// Local range statistics provider backed by Fjall latest-index partitions.
#[derive(Clone)]
struct LocalRangeStats {
    keyspace: Arc<Keyspace>,
    max_shards: usize,
}

impl LocalRangeStats {
    fn new(keyspace: Arc<Keyspace>, max_shards: usize) -> Self {
        Self {
            keyspace,
            max_shards: max_shards.max(1),
        }
    }

    fn latest_partition_name(&self, shard_index: usize) -> String {
        if self.max_shards <= 1 {
            "kv_latest".to_string()
        } else {
            format!("kv_latest_{shard_index}")
        }
    }

    /// Count current visible records for a shard range from the latest index.
    fn count_range(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
    ) -> anyhow::Result<u64> {
        let latest_name = self.latest_partition_name(shard_index);
        let latest = self
            .keyspace
            .open_partition(&latest_name, PartitionCreateOptions::default())?;
        let start = start_key.to_vec();
        let mut iter: Box<dyn Iterator<Item = fjall::Result<fjall::KvPair>>> = if end_key.is_empty()
        {
            Box::new(latest.range(start..))
        } else {
            Box::new(latest.range(start..end_key.to_vec()))
        };
        let mut count = 0u64;
        while let Some(item) = iter.next() {
            let _ = item?;
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    /// Read one paged snapshot of latest-visible rows for `[start_key, end_key)`.
    fn snapshot_latest_page(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        cursor: &[u8],
        limit: usize,
    ) -> anyhow::Result<(Vec<RangeSnapshotLatestEntry>, Vec<u8>, bool)> {
        let limit = limit.clamp(1, 10_000);
        let latest_name = self.latest_partition_name(shard_index);
        let latest = self
            .keyspace
            .open_partition(&latest_name, PartitionCreateOptions::default())?;

        let mut lower = start_key.to_vec();
        if !cursor.is_empty() && cursor > lower.as_slice() {
            lower = cursor.to_vec();
        }
        let mut iter: Box<dyn Iterator<Item = fjall::Result<fjall::KvPair>>> = if end_key.is_empty()
        {
            Box::new(latest.range(lower..))
        } else {
            Box::new(latest.range(lower..end_key.to_vec()))
        };

        let mut out = Vec::with_capacity(limit);
        let mut last_key = Vec::new();
        let mut has_more = false;
        while let Some(item) = iter.next() {
            let (key, value) = item?;
            let key = key.to_vec();
            if !cursor.is_empty() && key.as_slice() <= cursor {
                continue;
            }
            let (version, row) = kv::decode_latest_value(&value)?;
            last_key = key.clone();
            out.push(RangeSnapshotLatestEntry {
                key,
                value: row,
                version,
            });
            if out.len() >= limit {
                has_more = true;
                break;
            }
        }

        let done = !has_more;
        let next_cursor = if has_more { last_key } else { Vec::new() };
        Ok((out, next_cursor, done))
    }

    /// Apply backfilled latest rows into a local shard's KV partitions.
    fn apply_latest_entries(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: &[RangeSnapshotLatestEntry],
    ) -> anyhow::Result<u64> {
        let engine: Arc<dyn KvEngine> = if self.max_shards <= 1 {
            Arc::new(FjallEngine::open(self.keyspace.clone())?)
        } else {
            Arc::new(FjallEngine::open_shard(self.keyspace.clone(), shard_index)?)
        };
        let mut applied = 0u64;
        for entry in entries {
            let in_start = start_key.is_empty() || entry.key.as_slice() >= start_key;
            let in_end = end_key.is_empty() || entry.key.as_slice() < end_key;
            if !in_start || !in_end {
                continue;
            }
            engine.set(entry.key.clone(), entry.value.clone(), entry.version);
            engine.mark_visible(&entry.key, entry.version);
            applied = applied.saturating_add(1);
        }
        Ok(applied)
    }
}

/// Read mode options for GETs.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum ReadMode {
    Accord,
    Quorum,
    Local,
}

/// Data shard routing mode.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum RoutingMode {
    Hash,
    Range,
}

/// WAL engine options.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum WalEngine {
    File,
    RaftEngine,
}

/// Classification of proposals for stats aggregation.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
enum ProposalKind {
    Get,
    Set,
    BatchGet,
    BatchSet,
}

/// Aggregated proposal latency stats.
#[derive(Default)]
struct ProposalStats {
    get: OpStats,
    set: OpStats,
    batch_get: OpStats,
    batch_set: OpStats,
}

/// Per-operation stats counters.
#[derive(Default)]
struct OpStats {
    count: AtomicU64,
    errors: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

/// Snapshot of proposal stats.
#[derive(Default, Debug, Clone)]
struct ProposalStatsSnapshot {
    get: OpStatsSnapshot,
    set: OpStatsSnapshot,
    batch_get: OpStatsSnapshot,
    batch_set: OpStatsSnapshot,
}

/// Server-side RPC handler latency counters.
#[derive(Default)]
struct RpcHandlerStats {
    pre_accept_count: AtomicU64,
    pre_accept_total_us: AtomicU64,
    pre_accept_max_us: AtomicU64,
    pre_accept_batch_count: AtomicU64,
    pre_accept_batch_total_us: AtomicU64,
    pre_accept_batch_max_us: AtomicU64,
    pre_accept_inflight: AtomicU64,
    pre_accept_inflight_max: AtomicU64,
    accept_count: AtomicU64,
    accept_total_us: AtomicU64,
    accept_max_us: AtomicU64,
    accept_batch_count: AtomicU64,
    accept_batch_total_us: AtomicU64,
    accept_batch_max_us: AtomicU64,
    accept_inflight: AtomicU64,
    accept_inflight_max: AtomicU64,
    commit_count: AtomicU64,
    commit_total_us: AtomicU64,
    commit_max_us: AtomicU64,
    commit_batch_count: AtomicU64,
    commit_batch_total_us: AtomicU64,
    commit_batch_max_us: AtomicU64,
    commit_inflight: AtomicU64,
    commit_inflight_max: AtomicU64,
}

/// Snapshot of server-side RPC handler stats.
#[derive(Default, Debug, Clone)]
struct RpcHandlerStatsSnapshot {
    pre_accept_avg_ms: f64,
    pre_accept_p99_ms: f64,
    pre_accept_batch_avg_ms: f64,
    pre_accept_batch_p99_ms: f64,
    pre_accept_inflight: u64,
    pre_accept_inflight_peak: u64,
    accept_avg_ms: f64,
    accept_p99_ms: f64,
    accept_batch_avg_ms: f64,
    accept_batch_p99_ms: f64,
    accept_inflight: u64,
    accept_inflight_peak: u64,
    commit_avg_ms: f64,
    commit_p99_ms: f64,
    commit_batch_avg_ms: f64,
    commit_batch_p99_ms: f64,
    commit_inflight: u64,
    commit_inflight_peak: u64,
}

impl RpcHandlerStats {
    /// Track an inflight pre-accept handler invocation.
    fn track_pre_accept(&self) -> InflightGuard<'_> {
        let current = self.pre_accept_inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.pre_accept_inflight_max
            .fetch_max(current, Ordering::Relaxed);
        InflightGuard {
            counter: &self.pre_accept_inflight,
        }
    }

    /// Record the latency of a pre-accept handler invocation.
    fn record_pre_accept(&self, us: u64) {
        self.pre_accept_count.fetch_add(1, Ordering::Relaxed);
        self.pre_accept_total_us.fetch_add(us, Ordering::Relaxed);
        self.pre_accept_max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Record the latency of a pre-accept batch handler invocation.
    fn record_pre_accept_batch(&self, us: u64) {
        self.pre_accept_batch_count.fetch_add(1, Ordering::Relaxed);
        self.pre_accept_batch_total_us
            .fetch_add(us, Ordering::Relaxed);
        self.pre_accept_batch_max_us
            .fetch_max(us, Ordering::Relaxed);
    }

    /// Track an inflight accept handler invocation.
    fn track_accept(&self) -> InflightGuard<'_> {
        let current = self.accept_inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.accept_inflight_max
            .fetch_max(current, Ordering::Relaxed);
        InflightGuard {
            counter: &self.accept_inflight,
        }
    }

    /// Record the latency of an accept handler invocation.
    fn record_accept(&self, us: u64) {
        self.accept_count.fetch_add(1, Ordering::Relaxed);
        self.accept_total_us.fetch_add(us, Ordering::Relaxed);
        self.accept_max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Record the latency of an accept batch handler invocation.
    fn record_accept_batch(&self, us: u64) {
        self.accept_batch_count.fetch_add(1, Ordering::Relaxed);
        self.accept_batch_total_us.fetch_add(us, Ordering::Relaxed);
        self.accept_batch_max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Track an inflight commit handler invocation.
    fn track_commit(&self) -> InflightGuard<'_> {
        let current = self.commit_inflight.fetch_add(1, Ordering::Relaxed) + 1;
        self.commit_inflight_max
            .fetch_max(current, Ordering::Relaxed);
        InflightGuard {
            counter: &self.commit_inflight,
        }
    }

    /// Record the latency of a commit handler invocation.
    fn record_commit(&self, us: u64) {
        self.commit_count.fetch_add(1, Ordering::Relaxed);
        self.commit_total_us.fetch_add(us, Ordering::Relaxed);
        self.commit_max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Record the latency of a commit batch handler invocation.
    fn record_commit_batch(&self, us: u64) {
        self.commit_batch_count.fetch_add(1, Ordering::Relaxed);
        self.commit_batch_total_us.fetch_add(us, Ordering::Relaxed);
        self.commit_batch_max_us.fetch_max(us, Ordering::Relaxed);
    }

    /// Snapshot and reset all handler stats.
    fn snapshot_and_reset(&self) -> RpcHandlerStatsSnapshot {
        let pre_cnt = self.pre_accept_count.swap(0, Ordering::Relaxed);
        let pre_tot = self.pre_accept_total_us.swap(0, Ordering::Relaxed);
        let pre_max = self.pre_accept_max_us.swap(0, Ordering::Relaxed);
        let pre_batch_cnt = self.pre_accept_batch_count.swap(0, Ordering::Relaxed);
        let pre_batch_tot = self.pre_accept_batch_total_us.swap(0, Ordering::Relaxed);
        let pre_batch_max = self.pre_accept_batch_max_us.swap(0, Ordering::Relaxed);
        let pre_inflight = self.pre_accept_inflight.load(Ordering::Relaxed);
        let pre_inflight_peak = self.pre_accept_inflight_max.swap(0, Ordering::Relaxed);

        let acc_cnt = self.accept_count.swap(0, Ordering::Relaxed);
        let acc_tot = self.accept_total_us.swap(0, Ordering::Relaxed);
        let acc_max = self.accept_max_us.swap(0, Ordering::Relaxed);
        let acc_batch_cnt = self.accept_batch_count.swap(0, Ordering::Relaxed);
        let acc_batch_tot = self.accept_batch_total_us.swap(0, Ordering::Relaxed);
        let acc_batch_max = self.accept_batch_max_us.swap(0, Ordering::Relaxed);
        let acc_inflight = self.accept_inflight.load(Ordering::Relaxed);
        let acc_inflight_peak = self.accept_inflight_max.swap(0, Ordering::Relaxed);

        let com_cnt = self.commit_count.swap(0, Ordering::Relaxed);
        let com_tot = self.commit_total_us.swap(0, Ordering::Relaxed);
        let com_max = self.commit_max_us.swap(0, Ordering::Relaxed);
        let com_batch_cnt = self.commit_batch_count.swap(0, Ordering::Relaxed);
        let com_batch_tot = self.commit_batch_total_us.swap(0, Ordering::Relaxed);
        let com_batch_max = self.commit_batch_max_us.swap(0, Ordering::Relaxed);
        let com_inflight = self.commit_inflight.load(Ordering::Relaxed);
        let com_inflight_peak = self.commit_inflight_max.swap(0, Ordering::Relaxed);

        RpcHandlerStatsSnapshot {
            pre_accept_avg_ms: avg_us_per(pre_tot, pre_cnt),
            pre_accept_p99_ms: pre_max as f64 / 1000.0,
            pre_accept_batch_avg_ms: avg_us_per(pre_batch_tot, pre_batch_cnt),
            pre_accept_batch_p99_ms: pre_batch_max as f64 / 1000.0,
            pre_accept_inflight: pre_inflight,
            pre_accept_inflight_peak: pre_inflight_peak,
            accept_avg_ms: avg_us_per(acc_tot, acc_cnt),
            accept_p99_ms: acc_max as f64 / 1000.0,
            accept_batch_avg_ms: avg_us_per(acc_batch_tot, acc_batch_cnt),
            accept_batch_p99_ms: acc_batch_max as f64 / 1000.0,
            accept_inflight: acc_inflight,
            accept_inflight_peak: acc_inflight_peak,
            commit_avg_ms: avg_us_per(com_tot, com_cnt),
            commit_p99_ms: com_max as f64 / 1000.0,
            commit_batch_avg_ms: avg_us_per(com_batch_tot, com_batch_cnt),
            commit_batch_p99_ms: com_batch_max as f64 / 1000.0,
            commit_inflight: com_inflight,
            commit_inflight_peak: com_inflight_peak,
        }
    }
}

/// RAII guard that decrements an inflight counter when dropped.
struct InflightGuard<'a> {
    counter: &'a AtomicU64,
}

impl Drop for InflightGuard<'_> {
    /// Decrement the inflight counter when the guard is dropped.
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Snapshot of per-operation stats.
#[derive(Default, Debug, Clone)]
struct OpStatsSnapshot {
    count: u64,
    errors: u64,
    total_us: u64,
    max_us: u64,
}

/// Client batcher statistics.
#[derive(Default)]
struct ClientBatchStats {
    set_batches: AtomicU64,
    set_items: AtomicU64,
    set_max_items: AtomicU64,
    set_wait_total_us: AtomicU64,
    set_wait_max_us: AtomicU64,
    get_batches: AtomicU64,
    get_items: AtomicU64,
    get_max_items: AtomicU64,
    get_wait_total_us: AtomicU64,
    get_wait_max_us: AtomicU64,
}

/// Snapshot of client batcher stats.
#[derive(Default, Debug, Clone)]
struct ClientBatchStatsSnapshot {
    set_batches: u64,
    set_items: u64,
    set_max_items: u64,
    set_wait_total_us: u64,
    set_wait_max_us: u64,
    get_batches: u64,
    get_items: u64,
    get_max_items: u64,
    get_wait_total_us: u64,
    get_wait_max_us: u64,
}

impl ClientBatchStats {
    /// Record a completed SET batch with item count and queue wait time.
    fn record_set(&self, items: u64, wait_us: u64) {
        self.set_batches.fetch_add(1, Ordering::Relaxed);
        self.set_items.fetch_add(items, Ordering::Relaxed);
        self.set_max_items.fetch_max(items, Ordering::Relaxed);
        self.set_wait_total_us.fetch_add(wait_us, Ordering::Relaxed);
        self.set_wait_max_us.fetch_max(wait_us, Ordering::Relaxed);
    }

    /// Record a completed GET batch with item count and queue wait time.
    fn record_get(&self, items: u64, wait_us: u64) {
        self.get_batches.fetch_add(1, Ordering::Relaxed);
        self.get_items.fetch_add(items, Ordering::Relaxed);
        self.get_max_items.fetch_max(items, Ordering::Relaxed);
        self.get_wait_total_us.fetch_add(wait_us, Ordering::Relaxed);
        self.get_wait_max_us.fetch_max(wait_us, Ordering::Relaxed);
    }

    /// Snapshot and reset client batch stats.
    fn snapshot_and_reset(&self) -> ClientBatchStatsSnapshot {
        ClientBatchStatsSnapshot {
            set_batches: self.set_batches.swap(0, Ordering::Relaxed),
            set_items: self.set_items.swap(0, Ordering::Relaxed),
            set_max_items: self.set_max_items.swap(0, Ordering::Relaxed),
            set_wait_total_us: self.set_wait_total_us.swap(0, Ordering::Relaxed),
            set_wait_max_us: self.set_wait_max_us.swap(0, Ordering::Relaxed),
            get_batches: self.get_batches.swap(0, Ordering::Relaxed),
            get_items: self.get_items.swap(0, Ordering::Relaxed),
            get_max_items: self.get_max_items.swap(0, Ordering::Relaxed),
            get_wait_total_us: self.get_wait_total_us.swap(0, Ordering::Relaxed),
            get_wait_max_us: self.get_wait_max_us.swap(0, Ordering::Relaxed),
        }
    }
}

/// Configuration for client request batching.
#[derive(Clone, Copy)]
struct ClientBatchConfig {
    set_max: usize,
    get_max: usize,
    wait: Duration,
    inflight: usize,
}

/// Work item representing a batched SET request.
struct BatchSetWork {
    items: Vec<(Vec<u8>, Vec<u8>)>,
    tx: oneshot::Sender<anyhow::Result<()>>,
    enqueued_at: Instant,
}

/// Work item representing a batched GET request.
struct BatchGetWork {
    keys: Vec<Vec<u8>>,
    tx: oneshot::Sender<anyhow::Result<Vec<Option<Vec<u8>>>>>,
    enqueued_at: Instant,
}

/// RAII guard for client ops currently executing through the batch-direct path.
struct InflightClientOpGuard {
    counter: Arc<AtomicU64>,
}

impl Drop for InflightClientOpGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl ProposalStats {
    /// Record latency for a proposal kind.
    fn record(&self, kind: ProposalKind, dur_us: u64, ok: bool) {
        match kind {
            ProposalKind::Get => self.get.record(dur_us, ok),
            ProposalKind::Set => self.set.record(dur_us, ok),
            ProposalKind::BatchGet => self.batch_get.record(dur_us, ok),
            ProposalKind::BatchSet => self.batch_set.record(dur_us, ok),
        }
    }

    /// Snapshot and reset all proposal stats.
    fn snapshot_and_reset(&self) -> ProposalStatsSnapshot {
        ProposalStatsSnapshot {
            get: self.get.snapshot(),
            set: self.set.snapshot(),
            batch_get: self.batch_get.snapshot(),
            batch_set: self.batch_set.snapshot(),
        }
    }
}

impl OpStats {
    /// Record a single operation's latency and success/failure.
    fn record(&self, dur_us: u64, ok: bool) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(dur_us, Ordering::Relaxed);
        self.max_us.fetch_max(dur_us, Ordering::Relaxed);
        if !ok {
            // Track failures separately from total count.
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Snapshot and reset counters.
    fn snapshot(&self) -> OpStatsSnapshot {
        OpStatsSnapshot {
            count: self.count.swap(0, Ordering::Relaxed),
            errors: self.errors.swap(0, Ordering::Relaxed),
            total_us: self.total_us.swap(0, Ordering::Relaxed),
            max_us: self.max_us.swap(0, Ordering::Relaxed),
        }
    }
}

impl NodeState {
    fn enter_client_op(&self) -> InflightClientOpGuard {
        self.client_inflight_ops.fetch_add(1, Ordering::Relaxed);
        InflightClientOpGuard {
            counter: self.client_inflight_ops.clone(),
        }
    }

    pub(crate) async fn wait_for_client_ops_drained(&self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let inflight = self.client_inflight_ops.load(Ordering::Relaxed);
            if inflight == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for client ops to drain (inflight={inflight})");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Return the Accord group for a given group id.
    fn group(&self, group_id: GroupId) -> Option<Arc<accord::Group>> {
        self.groups.get(&group_id).cloned()
    }

    /// Apply control-plane replica membership to runtime Accord data groups.
    fn refresh_group_memberships(&self) -> anyhow::Result<()> {
        let snapshot = self.cluster_store.state();

        // Meta/control group tracks all non-removed nodes.
        if let Some(meta) = self.group(GROUP_MEMBERSHIP) {
            let mut members = snapshot
                .members
                .values()
                .filter(|m| m.state != MemberState::Removed)
                .map(|m| accord::Member { id: m.node_id })
                .collect::<Vec<_>>();
            members.sort_by_key(|m| m.id);
            members.dedup_by_key(|m| m.id);
            if !members.is_empty() {
                meta.update_members(members)?;
            }
        }

        // Per-range data groups track voting replicas only (learners excluded).
        //
        // During an in-flight replica move, runtime membership transitions are
        // driven by committed shard-log reconfiguration commands. Skipping
        // control-plane refresh for those shards prevents the background
        // refresher from racing and reverting an in-progress stage.
        for shard in &snapshot.shards {
            if snapshot.shard_rebalances.contains_key(&shard.shard_id) {
                continue;
            }
            let group_id = GROUP_DATA_BASE + shard.shard_index as u64;
            let Some(group) = self.group(group_id) else {
                continue;
            };
            let (members, voters) = shard_membership_sets(&snapshot, shard);
            if members.is_empty() || voters.is_empty() {
                continue;
            }
            let members = members
                .into_iter()
                .map(|id| accord::Member { id })
                .collect::<Vec<_>>();
            group.update_membership(members, voters)?;
        }

        Ok(())
    }

    /// Map a key to a shard index.
    fn shard_for_key(&self, key: &[u8]) -> usize {
        // Block shard routing while a split is actively migrating keys.
        let _guard = self.split_lock.read().unwrap();
        self.shard_for_key_with_snapshot(key, None)
    }

    /// Map a key to a shard index, optionally using a pre-fetched range snapshot.
    ///
    /// The snapshot path avoids repeated lock acquisitions in hot batch loops.
    fn shard_for_key_with_snapshot(&self, key: &[u8], ranges: Option<&[ShardDesc]>) -> usize {
        match self.routing_mode {
            RoutingMode::Hash => {
                let hash = kv::hash_key(key);
                (hash as usize) % self.data_shards.max(1)
            }
            RoutingMode::Range => {
                if let Some(shards) = ranges {
                    shard_index_for_key_in_ranges(key, shards)
                } else {
                    self.cluster_store.shard_index_for_key(key)
                }
            }
        }
    }

    /// Resolve the Accord handle for a shard index.
    fn handle_for_shard(&self, shard: usize) -> accord::Handle {
        let idx = shard.min(self.data_handles.len().saturating_sub(1));
        self.data_handles[idx].clone()
    }

    /// Compare runtime Accord membership against the expected member/voter sets.
    fn shard_membership_matches(
        &self,
        shard_index: usize,
        members: &[NodeId],
        voters: &[NodeId],
    ) -> bool {
        let group_id = GROUP_DATA_BASE + shard_index as u64;
        let Some(group) = self.group(group_id) else {
            return false;
        };

        let mut expected_members = members.to_vec();
        expected_members.sort_unstable();
        expected_members.dedup();
        let mut expected_voters = voters.to_vec();
        expected_voters.sort_unstable();
        expected_voters.dedup();

        let mut current_members = group.members().into_iter().map(|m| m.id).collect::<Vec<_>>();
        current_members.sort_unstable();
        current_members.dedup();
        let mut current_voters = group.voters();
        current_voters.sort_unstable();
        current_voters.dedup();

        current_members == expected_members && current_voters == expected_voters
    }

    /// Propose an internal shard-membership reconfiguration command.
    async fn propose_shard_membership_reconfig(
        &self,
        shard_index: usize,
        members: &[NodeId],
        voters: &[NodeId],
    ) -> anyhow::Result<()> {
        if self.shard_membership_matches(shard_index, members, voters) {
            return Ok(());
        }
        let payload = kv::encode_membership_reconfig(members, voters);
        let _ = self.handle_for_shard(shard_index).propose(payload).await?;
        Ok(())
    }

    /// Read the latest visible value for a key from the local KV engine.
    fn kv_latest(&self, key: &[u8]) -> Option<(Vec<u8>, kv::Version)> {
        self.kv_engine.get_latest(key)
    }

    /// Batch read latest visible values from the local KV engine.
    fn kv_latest_batch(&self, keys: &[Vec<u8>]) -> Vec<Option<(Vec<u8>, kv::Version)>> {
        self.kv_engine.get_latest_batch(keys)
    }

    /// Collect local per-range record counts for ranges this node serves.
    fn local_range_stats(&self) -> anyhow::Result<Vec<LocalRangeStat>> {
        let shards = self.cluster_store.shards_snapshot();
        let mut out = Vec::new();
        for shard in shards {
            if !shard.replicas.contains(&self.node_id) {
                continue;
            }
            let record_count =
                self.range_stats
                    .count_range(shard.shard_index, &shard.start_key, &shard.end_key)?;
            out.push(LocalRangeStat {
                shard_id: shard.shard_id,
                shard_index: shard.shard_index,
                record_count,
                is_leaseholder: shard.leaseholder == self.node_id,
            });
        }
        Ok(out)
    }

    /// Read one paged snapshot of latest rows for a shard range.
    fn snapshot_range_latest_page(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        cursor: &[u8],
        limit: usize,
    ) -> anyhow::Result<(Vec<RangeSnapshotLatestEntry>, Vec<u8>, bool)> {
        self.range_stats
            .snapshot_latest_page(shard_index, start_key, end_key, cursor, limit)
    }

    /// Apply one page of latest rows into a local shard range.
    fn apply_range_latest_page(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: &[RangeSnapshotLatestEntry],
    ) -> anyhow::Result<u64> {
        self.range_stats
            .apply_latest_entries(shard_index, start_key, end_key, entries)
    }

    /// Snapshot and reset client batch stats.
    fn client_batch_stats_snapshot(&self) -> ClientBatchStatsSnapshot {
        self.client_batch_stats.snapshot_and_reset()
    }

    /// Block client request execution while range operations are in progress.
    async fn wait_until_unfrozen(&self) {
        while self.cluster_store.frozen() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Enqueue a batch SET request to the client batcher.
    async fn execute_batch_set(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> anyhow::Result<()> {
        self.wait_until_unfrozen().await;
        if items.is_empty() {
            // Nothing to do.
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        let work = BatchSetWork {
            items,
            tx,
            enqueued_at: Instant::now(),
        };
        self.client_set_tx
            .send(work)
            .await
            .map_err(|_| anyhow!("set batch queue closed"))?;
        rx.await.context("set batch response dropped")?
    }

    /// Execute a batch SET directly against Accord without client batching.
    async fn execute_batch_set_direct(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> anyhow::Result<()> {
        self.wait_until_unfrozen().await;
        let _inflight_guard = self.enter_client_op();
        if items.is_empty() {
            // Nothing to do.
            return Ok(());
        }
        let mut by_shard: Vec<Vec<(Vec<u8>, Vec<u8>)>> = vec![Vec::new(); self.data_shards];
        {
            // Take split lock and range snapshot once for the whole routing pass.
            let _guard = self.split_lock.read().unwrap();
            let range_snapshot = if matches!(self.routing_mode, RoutingMode::Range) {
                Some(self.cluster_store.shards_snapshot())
            } else {
                None
            };
            let ranges = range_snapshot.as_deref();
            for (key, value) in items {
                let shard = self.shard_for_key_with_snapshot(&key, ranges);
                by_shard[shard].push((key, value));
            }
        }

        let target = self.client_set_batch_target.max(1);
        let mut futs = FuturesUnordered::new();
        for (shard, batch) in by_shard.into_iter().enumerate() {
            if batch.is_empty() {
                // Skip shards with no work.
                continue;
            }
            // Track client-visible write load per shard (best-effort).
            self.shard_load.record_set_ops(shard, batch.len() as u64);
            let handle = self.handle_for_shard(shard);
            for chunk in batch.chunks(target) {
                // Split large batches into target-sized chunks for bounded proposal size.
                let cmd = kv::encode_batch_set(chunk);
                let handle = handle.clone();
                futs.push(self.propose_timed_on(handle, ProposalKind::BatchSet, cmd));
            }
        }

        while let Some(res) = futs.next().await {
            res?;
        }
        Ok(())
    }

    /// Enqueue a batch GET request to the client batcher.
    async fn execute_batch_get(&self, keys: Vec<Vec<u8>>) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        self.wait_until_unfrozen().await;
        if keys.is_empty() {
            // Nothing to do.
            return Ok(Vec::new());
        }

        let (tx, rx) = oneshot::channel();
        let work = BatchGetWork {
            keys,
            tx,
            enqueued_at: Instant::now(),
        };
        self.client_get_tx
            .send(work)
            .await
            .map_err(|_| anyhow!("get batch queue closed"))?;
        rx.await.context("get batch response dropped")?
    }

    /// Execute a batch GET directly with the selected read mode.
    async fn execute_batch_get_direct(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        self.wait_until_unfrozen().await;
        let _inflight_guard = self.enter_client_op();
        if keys.is_empty() {
            // Nothing to do.
            return Ok(Vec::new());
        }
        let mut by_shard: Vec<Vec<(usize, Vec<u8>)>> = vec![Vec::new(); self.data_shards];
        {
            // Take split lock and range snapshot once for the whole routing pass.
            let _guard = self.split_lock.read().unwrap();
            let range_snapshot = if matches!(self.routing_mode, RoutingMode::Range) {
                Some(self.cluster_store.shards_snapshot())
            } else {
                None
            };
            let ranges = range_snapshot.as_deref();
            for (idx, key) in keys.iter().enumerate() {
                let shard = self.shard_for_key_with_snapshot(key, ranges);
                by_shard[shard].push((idx, key.clone()));
            }
        }

        let mut out = vec![None; keys.len()];
        let mut futs = FuturesUnordered::new();

        for (shard, entries) in by_shard.into_iter().enumerate() {
            if entries.is_empty() {
                // Skip shards with no work.
                continue;
            }
            // Track client-visible read load per shard (best-effort).
            self.shard_load.record_get_ops(shard, entries.len() as u64);
            let mut shard_keys = Vec::with_capacity(entries.len());
            let mut shard_indices = Vec::with_capacity(entries.len());
            for (idx, key) in entries {
                shard_indices.push(idx);
                shard_keys.push(key);
            }

            let handle = self.handle_for_shard(shard);
            let read_mode = self.read_mode;
            let fallback = self.read_barrier_fallback_quorum;
            let this = self.clone();
            futs.push(async move {
                let values = match read_mode {
                    ReadMode::Accord => {
                        // Accord reads: enforce read barriers before reading.
                        let deps = match this
                            .ensure_read_barrier_shard(GROUP_DATA_BASE + shard as u64, &shard_keys)
                            .await
                        {
                            Ok(versions) => versions,
                            Err(err) => {
                                // Optionally fall back to quorum reads on barrier failures.
                                if fallback {
                                    tracing::warn!(
                                        error = ?err,
                                        "read barrier failed; falling back to quorum batch get"
                                    );
                                    return this
                                        .quorum_batch_get_shard(shard_keys)
                                        .await
                                        .map(|vals| (shard_indices, vals));
                                }
                                return Err(err);
                            }
                        };
                        let expected = shard_keys.len();
                        let cmd = kv::encode_batch_get(&shard_keys);
                        let res = handle.propose_read_with_deps(cmd, deps).await?;
                        let accord::ProposalResult::Read(value) = res else {
                            anyhow::bail!("BATCH_GET expected read result, got {res:?}");
                        };
                        let value = value.unwrap_or_default();
                        let decoded = kv::decode_batch_get_result(&value)?;
                        anyhow::ensure!(
                            decoded.len() == expected,
                            "batch get result count mismatch (expected {expected}, got {})",
                            decoded.len()
                        );
                        decoded
                    }
                    // Local reads use the local KV engine without barriers.
                    ReadMode::Local => this
                        .kv_engine
                        .get_latest_batch(&shard_keys)
                        .into_iter()
                        .map(|item| item.map(|(v, _)| v))
                        .collect(),
                    // Quorum reads consult a quorum of peers and pick latest values.
                    ReadMode::Quorum => this.quorum_batch_get_shard(shard_keys).await?,
                };
                Ok::<_, anyhow::Error>((shard_indices, values))
            });
        }

        while let Some(res) = futs.next().await {
            let (indices, values) = res?;
            anyhow::ensure!(
                indices.len() == values.len(),
                "batch get shard result mismatch"
            );
            for (idx, value) in indices.into_iter().zip(values.into_iter()) {
                out[idx] = value;
            }
        }

        Ok(out)
    }

    /// Execute a single Redis operation by delegating to batching helpers.
    async fn execute(&self, op: redis_server::KvOp) -> anyhow::Result<redis_server::KvResult> {
        // If a range operation is in progress, block client traffic until it completes.
        self.wait_until_unfrozen().await;

        match op {
            redis_server::KvOp::HoloStats => {
                anyhow::bail!("HOLOSTATS is handled at the Redis layer")
            }
            redis_server::KvOp::Ping => Ok(redis_server::KvResult::Pong),
            redis_server::KvOp::Get { key } => {
                let mut values = self.execute_batch_get(vec![key]).await?;
                let value = values.pop().unwrap_or(None);
                Ok(redis_server::KvResult::Value(value))
            }
            redis_server::KvOp::Set { key, value } => {
                self.execute_batch_set(vec![(key, value)]).await?;
                Ok(redis_server::KvResult::Ok)
            }
        }
    }

    /// Propose a command and record timing stats when enabled.
    async fn propose_timed_on(
        &self,
        handle: accord::Handle,
        kind: ProposalKind,
        command: Vec<u8>,
    ) -> anyhow::Result<accord::ProposalResult> {
        let start = Instant::now();
        let res = handle.propose(command).await;
        if self.proposal_stats_enabled {
            // Record duration even on error to capture tail latencies.
            let dur_us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            self.proposal_stats.record(kind, dur_us, res.is_ok());
        }
        res
    }

    /// Ensure read barriers for the keys in a shard by waiting for latest commits.
    async fn ensure_read_barrier_shard(
        &self,
        group_id: GroupId,
        keys: &[Vec<u8>],
    ) -> anyhow::Result<Vec<(TxnId, u64)>> {
        if keys.is_empty() {
            // No keys means no barrier required.
            return Ok(Vec::new());
        }
        let responses = self.quorum_last_committed_shard(group_id, keys).await?;
        let max_by_key = merge_last_committed(&responses, keys.len());
        let mut unique = std::collections::HashMap::<TxnId, u64>::new();
        for replica in responses {
            for item in replica {
                if let Some((txn_id, _)) = item {
                    // Track each unique transaction observed in responses.
                    unique.entry(txn_id).or_insert(0);
                }
            }
        }
        if unique.is_empty() {
            // No outstanding commits to wait for.
            return Ok(Vec::new());
        }
        let group = self
            .group(group_id)
            .ok_or_else(|| anyhow::anyhow!("data group missing"))?;
        group.observe_last_committed(keys, &max_by_key).await;
        for item in &max_by_key {
            if let Some((txn_id, seq)) = item {
                // Update sequences to the max observed per key.
                unique.insert(*txn_id, *seq);
            }
        }
        for (txn_id, seq) in unique.iter() {
            let _ = seq;
            match tokio::time::timeout(self.read_barrier_timeout, group.wait_executed(*txn_id))
                .await
            {
                // Success path: the txn has executed.
                Ok(Ok(())) => {}
                // Propagate execution errors.
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    // Timeout waiting for execution.
                    anyhow::bail!("read barrier timed out waiting for {:?}", txn_id);
                }
            }
        }
        Ok(unique.into_iter().collect())
    }

    /// Fetch last-committed info from a quorum (or all peers if configured).
    async fn quorum_last_committed_shard(
        &self,
        group_id: GroupId,
        keys: &[Vec<u8>],
    ) -> anyhow::Result<Vec<Vec<Option<(TxnId, u64)>>>> {
        let group = self
            .group(group_id)
            .ok_or_else(|| anyhow::anyhow!("data group missing"))?;
        let mut results: Vec<Vec<Option<(TxnId, u64)>>> = Vec::new();
        results.push(group.last_committed_for_keys(keys).await);

        let quorum = (self.member_ids.len() / 2) + 1;
        let mut ok = 1usize;
        if ok >= quorum && !self.read_barrier_all_peers {
            // Local response is enough for quorum when all-peers isn't required.
            return Ok(results);
        }

        let seed = keys.first().map(|k| kv::hash_key(k)).unwrap_or(0);
        let peers = self.peers_for_seed(seed);
        let mut futs = FuturesUnordered::new();

        let launch_next = |futs: &mut FuturesUnordered<_>,
                           peer: NodeId,
                           keys: &[Vec<u8>],
                           transport: &Arc<GrpcTransport>| {
            let transport = transport.clone();
            let keys = keys.to_vec();
            let group_id = group_id;
            futs.push(async move { transport.last_committed(peer, group_id, keys).await });
        };

        for peer in &peers {
            launch_next(&mut futs, *peer, keys, &self.transport);
        }

        while let Some(resp) = futs.next().await {
            match resp {
                Ok(values) => {
                    results.push(values);
                    ok += 1;
                }
                Err(err) => {
                    // Log and ignore failed peers; quorum will decide success.
                    tracing::debug!(error = ?err, "last_committed rpc failed");
                }
            }
        }

        // Enforce all-peers or quorum based on configuration.
        if self.read_barrier_all_peers {
            let expected = self.member_ids.len();
            anyhow::ensure!(
                ok >= expected,
                "last_committed all-peers failed (ok={ok}, expected={expected})"
            );
        } else {
            anyhow::ensure!(
                ok >= quorum,
                "last_committed quorum failed (ok={ok}, quorum={quorum})"
            );
        }
        Ok(results)
    }

    /// Perform a quorum GET and return the latest value.
    async fn quorum_get(&self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        let mut results: Vec<Option<(Vec<u8>, kv::Version)>> = Vec::new();
        results.push(self.kv_engine.get_latest(&key));

        let quorum = (self.member_ids.len() / 2) + 1;
        let mut ok = 1usize;

        if ok >= quorum {
            // Local read already satisfies quorum.
            return Ok(pick_latest(results));
        }

        let peers = self.peers_for_seed(kv::hash_key(&key));
        let mut next_idx = 0usize;

        let mut futs = FuturesUnordered::new();
        let launch_next = |futs: &mut FuturesUnordered<_>,
                           peer: NodeId,
                           key: &Vec<u8>,
                           transport: &Arc<GrpcTransport>| {
            let transport = transport.clone();
            let key = key.clone();
            futs.push(async move { transport.kv_get(peer, key).await });
        };

        let needed = (quorum - ok).min(peers.len());
        // Only compute and log debug output when enabled.
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                op = "quorum_get",
                node_id = self.node_id,
                quorum = quorum,
                needed = needed,
                peers = ?peers,
                "quorum read peers"
            );
        }
        while next_idx < needed {
            let peer = peers[next_idx];
            next_idx += 1;
            launch_next(&mut futs, peer, &key, &self.transport);
        }

        while let Some(res) = futs.next().await {
            if let Ok(value) = res {
                // Only count successful responses toward quorum.
                results.push(value);
                ok += 1;
                if ok >= quorum {
                    // Stop once quorum responses have been collected.
                    break;
                }
            }

            if ok < quorum && next_idx < peers.len() {
                // Continue sending requests to additional peers if needed.
                let peer = peers[next_idx];
                next_idx += 1;
                launch_next(&mut futs, peer, &key, &self.transport);
            }
        }

        anyhow::ensure!(
            ok >= quorum,
            "quorum read failed (ok={ok}, quorum={quorum})"
        );
        Ok(pick_latest(results))
    }

    /// Perform a quorum batch GET for a shard and merge latest results.
    async fn quorum_batch_get_shard(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let quorum = (self.member_ids.len() / 2) + 1;
        let mut ok = 1usize;

        let mut results: Vec<Vec<Option<(Vec<u8>, kv::Version)>>> = Vec::new();
        results.push(self.kv_engine.get_latest_batch(&keys));

        if ok >= quorum {
            // Local read already satisfies quorum.
            return Ok(merge_quorum_results(&results, keys.len()));
        }

        let seed = keys.first().map(|k| kv::hash_key(k)).unwrap_or(0);
        let peers = self.peers_for_seed(seed);
        let mut next_idx = 0usize;

        let mut futs = FuturesUnordered::new();
        let launch_next = |futs: &mut FuturesUnordered<_>,
                           peer: NodeId,
                           keys: &Vec<Vec<u8>>,
                           transport: &Arc<GrpcTransport>| {
            let transport = transport.clone();
            let keys = keys.clone();
            futs.push(async move { transport.kv_batch_get(peer, keys).await });
        };

        let needed = (quorum - ok).min(peers.len());
        // Only compute and log debug output when enabled.
        if tracing::enabled!(tracing::Level::DEBUG) {
            tracing::debug!(
                op = "quorum_batch_get",
                node_id = self.node_id,
                quorum = quorum,
                needed = needed,
                peers = ?peers,
                "quorum read peers"
            );
        }
        while next_idx < needed {
            let peer = peers[next_idx];
            next_idx += 1;
            launch_next(&mut futs, peer, &keys, &self.transport);
        }

        while let Some(res) = futs.next().await {
            if let Ok(values) = res {
                // Only count successful responses toward quorum.
                results.push(values);
                ok += 1;
                if ok >= quorum {
                    // Stop once quorum responses have been collected.
                    break;
                }
            }

            if ok < quorum && next_idx < peers.len() {
                // Continue sending requests to additional peers if needed.
                let peer = peers[next_idx];
                next_idx += 1;
                launch_next(&mut futs, peer, &keys, &self.transport);
            }
        }

        anyhow::ensure!(
            ok >= quorum,
            "quorum read failed (ok={ok}, quorum={quorum})"
        );
        Ok(merge_quorum_results(&results, keys.len()))
    }

    /// Return a deterministic ordering of peers for a given seed.
    fn peers_for_seed(&self, seed: u64) -> Vec<NodeId> {
        let mut peers = self
            .member_ids
            .iter()
            .copied()
            .filter(|id| *id != self.node_id)
            .collect::<Vec<_>>();
        if peers.len() > 1 {
            // Rotate to spread load across peers deterministically.
            let start = (seed as usize) % peers.len();
            peers.rotate_left(start);
        }
        peers
    }
}

/// Resolve a key to a shard index using an in-memory range snapshot.
fn shard_index_for_key_in_ranges(key: &[u8], shards: &[ShardDesc]) -> usize {
    if shards.is_empty() {
        return 0;
    }
    if shards.len() == 1 {
        return shards[0].shard_index;
    }
    let has_key_ranges = shards
        .iter()
        .any(|s| !s.start_key.is_empty() || !s.end_key.is_empty());
    if has_key_ranges {
        for shard in shards {
            let in_start = shard.start_key.is_empty() || key >= shard.start_key.as_slice();
            let in_end = shard.end_key.is_empty() || key < shard.end_key.as_slice();
            if in_start && in_end {
                return shard.shard_index;
            }
        }
        return 0;
    }
    let hash = kv::hash_key(key);
    for shard in shards {
        if hash >= shard.start_hash && hash <= shard.end_hash {
            return shard.shard_index;
        }
    }
    0
}

/// Resolve voting members for a shard from staged replica-role metadata.
fn shard_membership_sets(snapshot: &ClusterState, shard: &ShardDesc) -> (Vec<NodeId>, Vec<NodeId>) {
    let mut members = shard.replicas.clone();
    members.sort_unstable();
    members.dedup();

    let mut voters = if let Some(roles) = snapshot.shard_replica_roles.get(&shard.shard_id) {
        shard
            .replicas
            .iter()
            .copied()
            .filter(|id| {
                roles
                    .get(id)
                    .copied()
                    .unwrap_or(ReplicaRole::Voter)
                    != ReplicaRole::Learner
            })
            .collect::<Vec<_>>()
    } else {
        shard.replicas.clone()
    };

    if voters.is_empty() {
        voters = members.clone();
    }
    voters.sort_unstable();
    voters.dedup();

    (members, voters)
}

/// Collect a batch of SET work items up to size/time limits.
async fn collect_set_batch(
    first: BatchSetWork,
    rx: &mut mpsc::Receiver<BatchSetWork>,
    max_items: usize,
    wait: Duration,
) -> Vec<BatchSetWork> {
    let mut batch = Vec::with_capacity(max_items.max(1));
    let mut items = 0usize;
    items += first.items.len();
    batch.push(first);

    // Decide whether to wait for more items based on the configured wait time.
    let deadline = if wait.is_zero() {
        None
    } else {
        Some(Instant::now() + wait)
    };

    'outer: loop {
        // Stop when we hit the maximum batch size.
        if items >= max_items {
            break;
        }
        match rx.try_recv() {
            Ok(work) => {
                items += work.items.len();
                batch.push(work);
                continue;
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }

        // No deadline means we are done collecting.
        let Some(deadline) = deadline else {
            break;
        };
        let now = Instant::now();
        // Stop when the batching window expires.
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(work) => {
                        items += work.items.len();
                        batch.push(work);
                    }
                    None => break 'outer,
                }
            }
            _ = tokio::time::sleep(remaining) => {
                break;
            }
        }
    }

    batch
}

/// Collect a batch of GET work items up to size/time limits.
async fn collect_get_batch(
    first: BatchGetWork,
    rx: &mut mpsc::Receiver<BatchGetWork>,
    max_items: usize,
    wait: Duration,
) -> Vec<BatchGetWork> {
    let mut batch = Vec::with_capacity(max_items.max(1));
    let mut items = 0usize;
    items += first.keys.len();
    batch.push(first);

    // Decide whether to wait for more items based on the configured wait time.
    let deadline = if wait.is_zero() {
        None
    } else {
        Some(Instant::now() + wait)
    };

    'outer: loop {
        // Stop when we hit the maximum batch size.
        if items >= max_items {
            break;
        }
        match rx.try_recv() {
            Ok(work) => {
                items += work.keys.len();
                batch.push(work);
                continue;
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }

        // No deadline means we are done collecting.
        let Some(deadline) = deadline else {
            break;
        };
        let now = Instant::now();
        // Stop when the batching window expires.
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(work) => {
                        items += work.keys.len();
                        batch.push(work);
                    }
                    None => break 'outer,
                }
            }
            _ = tokio::time::sleep(remaining) => {
                break;
            }
        }
    }

    batch
}

/// Spawn the SET batcher that coalesces client requests.
fn spawn_set_batcher(
    state: Arc<NodeState>,
    mut rx: mpsc::Receiver<BatchSetWork>,
    cfg: ClientBatchConfig,
) {
    let max_items = cfg.set_max.max(1);
    let wait = cfg.wait;
    let inflight = Arc::new(Semaphore::new(cfg.inflight.max(1)));
    tokio::spawn(async move {
        while let Some(first) = rx.recv().await {
            let batch = collect_set_batch(first, &mut rx, max_items, wait).await;
            let permit = match inflight.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return,
            };
            let state = state.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let wait_us = batch
                    .first()
                    .map(|w| {
                        w.enqueued_at
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64
                    })
                    .unwrap_or(0);
                let total_items: usize = batch.iter().map(|w| w.items.len()).sum();
                let mut responders = Vec::with_capacity(batch.len());
                let mut items = Vec::with_capacity(total_items);
                for work in batch {
                    items.extend(work.items);
                    responders.push(work.tx);
                }

                if total_items == 0 {
                    // Respond immediately to empty batches.
                    for tx in responders {
                        let _ = tx.send(Ok(()));
                    }
                    return;
                }

                state
                    .client_batch_stats
                    .record_set(total_items as u64, wait_us);
                let res = state.execute_batch_set_direct(items).await;
                match res {
                    Ok(()) => {
                        for tx in responders {
                            let _ = tx.send(Ok(()));
                        }
                    }
                    Err(err) => {
                        // Send the same error to all batch participants.
                        let msg = err.to_string();
                        for tx in responders {
                            let _ = tx.send(Err(anyhow!(msg.clone())));
                        }
                    }
                }
            });
        }
    });
}

/// Spawn the GET batcher that coalesces client requests.
fn spawn_get_batcher(
    state: Arc<NodeState>,
    mut rx: mpsc::Receiver<BatchGetWork>,
    cfg: ClientBatchConfig,
) {
    let max_items = cfg.get_max.max(1);
    let wait = cfg.wait;
    let inflight = Arc::new(Semaphore::new(cfg.inflight.max(1)));
    tokio::spawn(async move {
        while let Some(first) = rx.recv().await {
            let batch = collect_get_batch(first, &mut rx, max_items, wait).await;
            let permit = match inflight.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => return,
            };
            let state = state.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let wait_us = batch
                    .first()
                    .map(|w| {
                        w.enqueued_at
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64
                    })
                    .unwrap_or(0);
                let total_items: usize = batch.iter().map(|w| w.keys.len()).sum();
                let mut responders = Vec::with_capacity(batch.len());
                let mut splits = Vec::with_capacity(batch.len());
                let mut keys = Vec::with_capacity(total_items);
                for work in batch {
                    splits.push(work.keys.len());
                    keys.extend(work.keys);
                    responders.push(work.tx);
                }

                if total_items == 0 {
                    // Respond immediately to empty batches.
                    for tx in responders {
                        let _ = tx.send(Ok(Vec::new()));
                    }
                    return;
                }

                state
                    .client_batch_stats
                    .record_get(total_items as u64, wait_us);
                let res = state.execute_batch_get_direct(keys).await;
                match res {
                    Ok(values) => {
                        if values.len() != total_items {
                            // Guard against mismatched batch sizing.
                            let msg = format!(
                                "batch get result count mismatch (expected {total_items}, got {})",
                                values.len()
                            );
                            for tx in responders {
                                let _ = tx.send(Err(anyhow!(msg.clone())));
                            }
                            return;
                        }
                        let mut offset = 0usize;
                        for (tx, count) in responders.into_iter().zip(splits) {
                            let end = offset + count;
                            let slice = values[offset..end].to_vec();
                            offset = end;
                            let _ = tx.send(Ok(slice));
                        }
                    }
                    Err(err) => {
                        // Send the same error to all batch participants.
                        let msg = err.to_string();
                        for tx in responders {
                            let _ = tx.send(Err(anyhow!(msg.clone())));
                        }
                    }
                }
            });
        }
    });
}

/// Pick the highest-version value from a list of optional versions.
fn pick_latest(values: Vec<Option<(Vec<u8>, kv::Version)>>) -> Option<Vec<u8>> {
    let mut best: Option<(Vec<u8>, kv::Version)> = None;
    for item in values.into_iter().flatten() {
        let replace = match &best {
            None => true,
            Some((_, cur)) => item.1 > *cur,
        };
        if replace {
            // Keep the highest-version value.
            best = Some(item);
        }
    }
    best.map(|(v, _)| v)
}

/// Merge per-replica latest values by choosing the highest version per key.
fn merge_quorum_results(
    results: &[Vec<Option<(Vec<u8>, kv::Version)>>],
    key_count: usize,
) -> Vec<Option<Vec<u8>>> {
    let mut merged = Vec::with_capacity(key_count);
    for idx in 0..key_count {
        let mut best: Option<(Vec<u8>, kv::Version)> = None;
        for replica in results {
            if let Some((v, ver)) = replica.get(idx).and_then(|o| o.as_ref()) {
                let replace = match &best {
                    None => true,
                    Some((_, cur)) => *ver > *cur,
                };
                if replace {
                    // Keep the highest-version value across replicas.
                    best = Some((v.clone(), *ver));
                }
            }
        }
        merged.push(best.map(|(v, _)| v));
    }
    merged
}

/// Merge per-replica last-committed (txn_id, seq) by highest seq per key.
fn merge_last_committed(
    results: &[Vec<Option<(TxnId, u64)>>],
    key_count: usize,
) -> Vec<Option<(TxnId, u64)>> {
    let mut merged = Vec::with_capacity(key_count);
    for idx in 0..key_count {
        let mut best: Option<(TxnId, u64)> = None;
        for replica in results {
            if let Some((txn_id, seq)) = replica.get(idx).and_then(|o| o.as_ref()) {
                let replace = match &best {
                    None => true,
                    Some((_, cur_seq)) => *seq > *cur_seq,
                };
                if replace {
                    // Keep the highest sequence number across replicas.
                    best = Some((*txn_id, *seq));
                }
            }
        }
        merged.push(best);
    }
    merged
}

/// Log aggregated RPC stats from the transport layer.
fn log_rpc_stats(snap: &transport::RpcStatsSnapshot) {
    log_rpc_batch(
        "kv_get",
        snap.kv_get_batches,
        snap.kv_get_items,
        snap.kv_get_rpc_us,
        snap.kv_get_wait_us,
        snap.kv_get_max_batch,
        snap.kv_get_max_wait_us,
        snap.kv_get_errors,
    );

    // Only log batch get stats if there was activity.
    if snap.kv_batch_get_calls > 0 || snap.kv_batch_get_errors > 0 {
        let avg_rpc_ms = avg_us_per(snap.kv_batch_get_rpc_us, snap.kv_batch_get_calls);
        tracing::info!(
            rpc = "kv_batch_get",
            calls = snap.kv_batch_get_calls,
            avg_rpc_ms = avg_rpc_ms,
            errors = snap.kv_batch_get_errors,
            "rpc stats"
        );
    }

    log_rpc_batch(
        "pre_accept",
        snap.pre_accept_batches,
        snap.pre_accept_items,
        snap.pre_accept_rpc_us,
        snap.pre_accept_wait_us,
        snap.pre_accept_max_batch,
        snap.pre_accept_max_wait_us,
        snap.pre_accept_errors,
    );
    log_rpc_batch(
        "accept",
        snap.accept_batches,
        snap.accept_items,
        snap.accept_rpc_us,
        snap.accept_wait_us,
        snap.accept_max_batch,
        snap.accept_max_wait_us,
        snap.accept_errors,
    );
    log_rpc_batch(
        "commit",
        snap.commit_batches,
        snap.commit_items,
        snap.commit_rpc_us,
        snap.commit_wait_us,
        snap.commit_max_batch,
        snap.commit_max_wait_us,
        snap.commit_errors,
    );
    log_rpc_batch(
        "recover",
        snap.recover_batches,
        snap.recover_items,
        snap.recover_rpc_us,
        snap.recover_wait_us,
        snap.recover_max_batch,
        snap.recover_max_wait_us,
        snap.recover_errors,
    );
}

/// Log Accord consensus stats for a given group.
fn log_accord_stats(group_id: GroupId, stats: &accord::DebugStats) {
    let exec_progress_avg_ms = avg_us_per(stats.exec_progress_total_us, stats.exec_progress_count);
    let exec_recover_avg_ms = avg_us_per(stats.exec_recover_total_us, stats.exec_recover_count);
    let apply_write_avg_ms = avg_us_per(stats.apply_write_total_us, stats.apply_write_count);
    let apply_read_avg_ms = avg_us_per(stats.apply_read_total_us, stats.apply_read_count);
    let apply_batch_avg_ms = avg_us_per(stats.apply_batch_total_us, stats.apply_batch_count);
    let mark_visible_avg_ms = avg_us_per(stats.mark_visible_total_us, stats.mark_visible_count);
    let state_update_avg_ms = avg_us_per(stats.state_update_total_us, stats.state_update_count);
    tracing::info!(
        group_id = group_id,
        records = stats.records_len,
        records_committed = stats.records_status_committed_len,
        records_executing = stats.records_status_executing_len,
        records_executed = stats.records_status_executed_len,
        committed_queue = stats.committed_queue_len,
        committed_queue_ghost = stats.committed_queue_ghost_len,
        executed_out_of_order = stats.executed_out_of_order_len,
        read_waiters = stats.read_waiters_len,
        proposal_timeouts = stats.proposal_timeouts,
        execute_timeouts = stats.execute_timeouts,
        recovery_attempts = stats.recovery_attempts,
        recovery_failures = stats.recovery_failures,
        recovery_timeouts = stats.recovery_timeouts,
        exec_progress_avg_ms = exec_progress_avg_ms,
        exec_progress_max_ms = stats.exec_progress_max_us as f64 / 1000.0,
        exec_progress_errors = stats.exec_progress_errors,
        exec_recover_avg_ms = exec_recover_avg_ms,
        exec_recover_max_ms = stats.exec_recover_max_us as f64 / 1000.0,
        exec_recover_errors = stats.exec_recover_errors,
        apply_write_avg_ms = apply_write_avg_ms,
        apply_write_max_ms = stats.apply_write_max_us as f64 / 1000.0,
        apply_read_avg_ms = apply_read_avg_ms,
        apply_read_max_ms = stats.apply_read_max_us as f64 / 1000.0,
        apply_batch_avg_ms = apply_batch_avg_ms,
        apply_batch_max_ms = stats.apply_batch_max_us as f64 / 1000.0,
        mark_visible_avg_ms = mark_visible_avg_ms,
        mark_visible_max_ms = stats.mark_visible_max_us as f64 / 1000.0,
        state_update_avg_ms = state_update_avg_ms,
        state_update_max_ms = stats.state_update_max_us as f64 / 1000.0,
        fast_path = stats.fast_path_count,
        slow_path = stats.slow_path_count,
        "accord stats"
    );
}

/// Log proposal stats by kind.
fn log_proposal_stats(snap: &ProposalStatsSnapshot) {
    log_proposal_kind("get", &snap.get);
    log_proposal_kind("set", &snap.set);
    log_proposal_kind("batch_get", &snap.batch_get);
    log_proposal_kind("batch_set", &snap.batch_set);
}

/// Log proposal stats for a specific proposal kind.
fn log_proposal_kind(kind: &str, snap: &OpStatsSnapshot) {
    if snap.count == 0 && snap.errors == 0 {
        // Skip logging empty stats.
        return;
    }
    let avg_ms = avg_us_per(snap.total_us, snap.count);
    let max_ms = snap.max_us as f64 / 1000.0;
    tracing::info!(
        proposal = kind,
        count = snap.count,
        avg_ms = avg_ms,
        max_ms = max_ms,
        errors = snap.errors,
        "proposal stats"
    );
}

/// Log per-peer RPC queue and latency stats.
fn log_peer_queue_stats(peer_id: NodeId, snap: &transport::PeerStatsSnapshot) {
    let pre = &snap.pre_accept_latency;
    let acc = &snap.accept_latency;
    let com = &snap.commit_latency;
    let kv = &snap.kv_get_latency;
    tracing::info!(
        peer = peer_id,
        kv_get_queue = snap.kv_get_queue,
        kv_get_inflight = snap.kv_get_inflight,
        kv_get_sent = snap.kv_get_sent,
        kv_get_errors = snap.kv_get_errors,
        kv_get_count = kv.count,
        kv_get_avg_ms = kv.avg_ms,
        kv_get_p95_ms = kv.p95_ms,
        kv_get_p99_ms = kv.p99_ms,
        pre_accept_queue = snap.pre_accept_queue,
        pre_accept_inflight = snap.pre_accept_inflight,
        pre_accept_sent = snap.pre_accept_sent,
        pre_accept_errors = snap.pre_accept_errors,
        pre_accept_timeouts = snap.pre_accept_timeouts,
        pre_accept_queue_full = snap.pre_accept_queue_full,
        pre_accept_count = pre.count,
        pre_accept_avg_ms = pre.avg_ms,
        pre_accept_p95_ms = pre.p95_ms,
        pre_accept_p99_ms = pre.p99_ms,
        pre_accept_max_ms = pre.max_ms,
        pre_accept_wait_count = snap.pre_accept_wait_count,
        pre_accept_wait_avg_ms = snap.pre_accept_wait_avg_ms,
        pre_accept_wait_max_ms = snap.pre_accept_wait_max_ms,
        pre_accept_last_enqueue_age_ms = snap.pre_accept_last_enqueue_age_ms,
        pre_accept_last_dequeue_age_ms = snap.pre_accept_last_dequeue_age_ms,
        accept_queue = snap.accept_queue,
        accept_inflight = snap.accept_inflight,
        accept_sent = snap.accept_sent,
        accept_errors = snap.accept_errors,
        accept_timeouts = snap.accept_timeouts,
        accept_queue_full = snap.accept_queue_full,
        accept_count = acc.count,
        accept_avg_ms = acc.avg_ms,
        accept_p95_ms = acc.p95_ms,
        accept_p99_ms = acc.p99_ms,
        accept_max_ms = acc.max_ms,
        accept_wait_count = snap.accept_wait_count,
        accept_wait_avg_ms = snap.accept_wait_avg_ms,
        accept_wait_max_ms = snap.accept_wait_max_ms,
        accept_last_enqueue_age_ms = snap.accept_last_enqueue_age_ms,
        accept_last_dequeue_age_ms = snap.accept_last_dequeue_age_ms,
        commit_queue = snap.commit_queue,
        commit_inflight = snap.commit_inflight,
        commit_sent = snap.commit_sent,
        commit_errors = snap.commit_errors,
        commit_timeouts = snap.commit_timeouts,
        commit_queue_full = snap.commit_queue_full,
        commit_count = com.count,
        commit_avg_ms = com.avg_ms,
        commit_p95_ms = com.p95_ms,
        commit_p99_ms = com.p99_ms,
        commit_max_ms = com.max_ms,
        commit_wait_count = snap.commit_wait_count,
        commit_wait_avg_ms = snap.commit_wait_avg_ms,
        commit_wait_max_ms = snap.commit_wait_max_ms,
        commit_last_enqueue_age_ms = snap.commit_last_enqueue_age_ms,
        commit_last_dequeue_age_ms = snap.commit_last_dequeue_age_ms,
        recover_queue = snap.recover_queue,
        recover_queue_peak = snap.recover_queue_peak,
        recover_inflight = snap.recover_inflight,
        recover_sent = snap.recover_sent,
        recover_errors = snap.recover_errors,
        recover_queue_full = snap.recover_queue_full,
        recover_count = snap.recover_latency.count,
        recover_avg_ms = snap.recover_latency.avg_ms,
        recover_p95_ms = snap.recover_latency.p95_ms,
        recover_p99_ms = snap.recover_latency.p99_ms,
        recover_max_ms = snap.recover_latency.max_ms,
        recover_wait_count = snap.recover_wait_count,
        recover_wait_avg_ms = snap.recover_wait_avg_ms,
        recover_wait_max_ms = snap.recover_wait_max_ms,
        recover_last_enqueue_age_ms = snap.recover_last_enqueue_age_ms,
        recover_last_dequeue_age_ms = snap.recover_last_dequeue_age_ms,
        recover_inflight_txns = snap.recover_inflight_txns,
        recover_inflight_peak = snap.recover_inflight_peak,
        recover_coalesced = snap.recover_coalesced,
        recover_enqueued = snap.recover_enqueued,
        recover_waiters_peak = snap.recover_waiters_peak,
        recover_waiters_avg = snap.recover_waiters_avg,
        rpc_inflight_limit = snap.rpc_inflight_limit,
        "rpc peer queues"
    );
}

/// Log a short summary identifying the slowest peer.
fn log_peer_summary(peers: &[(NodeId, transport::PeerStatsSnapshot)]) {
    if peers.is_empty() {
        // Nothing to summarize.
        return;
    }
    let mut slow_peer = peers[0].0;
    let mut slow_queue = 0u64;
    let mut slow_sent = 0u64;
    for (peer_id, snap) in peers {
        let queue_total = snap.pre_accept_queue + snap.accept_queue + snap.commit_queue;
        if queue_total > slow_queue {
            // Track the peer with the largest combined queue.
            slow_queue = queue_total;
            slow_peer = *peer_id;
            slow_sent = snap.pre_accept_sent + snap.accept_sent + snap.commit_sent;
        }
    }
    tracing::info!(
        slow_peer = slow_peer,
        slow_queue_total = slow_queue,
        slow_sent_total = slow_sent,
        "rpc peer summary"
    );
}

/// CSV writer for periodic peer RPC stats.
struct PeerStatsCsv {
    writer: BufWriter<std::fs::File>,
}

impl PeerStatsCsv {
    /// Open or create the CSV file, writing a header if the file is empty.
    fn open(path: &str) -> Option<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()?;
        let mut writer = BufWriter::new(file);
        let need_header = writer
            .get_ref()
            .metadata()
            .map(|m| m.len() == 0)
            .unwrap_or(false);
        if need_header {
            // Write a header row so downstream tools can parse columns.
            let header = concat!(
                "ts_ms,node_id,peer_id,queue_total,inflight_total,sent_total,",
                "kv_get_queue,kv_get_inflight,kv_get_sent,kv_get_errors,kv_get_count,kv_get_avg_ms,kv_get_p95_ms,kv_get_p99_ms,kv_get_max_ms,",
                "pre_accept_queue,pre_accept_inflight,pre_accept_sent,pre_accept_errors,pre_accept_timeouts,pre_accept_queue_full,pre_accept_count,pre_accept_avg_ms,pre_accept_p95_ms,pre_accept_p99_ms,pre_accept_max_ms,pre_accept_wait_count,pre_accept_wait_avg_ms,pre_accept_wait_max_ms,pre_accept_last_enqueue_age_ms,pre_accept_last_dequeue_age_ms,",
                "accept_queue,accept_inflight,accept_sent,accept_errors,accept_timeouts,accept_queue_full,accept_count,accept_avg_ms,accept_p95_ms,accept_p99_ms,accept_max_ms,accept_wait_count,accept_wait_avg_ms,accept_wait_max_ms,accept_last_enqueue_age_ms,accept_last_dequeue_age_ms,",
                "commit_queue,commit_inflight,commit_sent,commit_errors,commit_timeouts,commit_queue_full,commit_count,commit_avg_ms,commit_p95_ms,commit_p99_ms,commit_max_ms,commit_wait_count,commit_wait_avg_ms,commit_wait_max_ms,commit_last_enqueue_age_ms,commit_last_dequeue_age_ms,",
                "recover_queue,recover_queue_peak,recover_inflight,recover_sent,recover_errors,recover_queue_full,",
                "recover_count,recover_avg_ms,recover_p95_ms,recover_p99_ms,recover_max_ms,",
                "recover_wait_count,recover_wait_avg_ms,recover_wait_max_ms,",
                "recover_last_enqueue_age_ms,recover_last_dequeue_age_ms,",
                "recover_inflight_txns,recover_inflight_peak,recover_coalesced,recover_enqueued,",
                "recover_waiters_peak,recover_waiters_avg,",
                "rpc_inflight_limit\n",
            );
            if writer.write_all(header.as_bytes()).is_err() {
                // Bail out if we cannot write the header.
                return None;
            }
            let _ = writer.flush();
        }
        Some(Self { writer })
    }

    /// Append a single per-peer snapshot row to the CSV file.
    fn write_snapshot(
        &mut self,
        ts_ms: u128,
        node_id: NodeId,
        peer_id: NodeId,
        snap: &transport::PeerStatsSnapshot,
    ) {
        let queue_total = snap.kv_get_queue
            + snap.pre_accept_queue
            + snap.accept_queue
            + snap.commit_queue
            + snap.recover_queue;
        let inflight_total = snap.kv_get_inflight
            + snap.pre_accept_inflight
            + snap.accept_inflight
            + snap.commit_inflight
            + snap.recover_inflight;
        let sent_total = snap.kv_get_sent
            + snap.pre_accept_sent
            + snap.accept_sent
            + snap.commit_sent
            + snap.recover_sent;

        let kv = &snap.kv_get_latency;
        let pre = &snap.pre_accept_latency;
        let acc = &snap.accept_latency;
        let com = &snap.commit_latency;
        let rec = &snap.recover_latency;

        let _ = writeln!(
            self.writer,
            "{ts_ms},{node_id},{peer_id},{queue_total},{inflight_total},{sent_total},\
{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},\
{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3},\
{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3},\
{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{},{:.3},{:.3},{:.3},{:.3},\
{},{},{},{},{},{},\
{},{:.3},{:.3},{:.3},{:.3},\
{},{:.3},{:.3},{:.3},{:.3},\
{},{},{},{},{},{:.3},{}",
            snap.kv_get_queue,
            snap.kv_get_inflight,
            snap.kv_get_sent,
            snap.kv_get_errors,
            kv.count,
            kv.avg_ms,
            kv.p95_ms,
            kv.p99_ms,
            kv.max_ms,
            snap.pre_accept_queue,
            snap.pre_accept_inflight,
            snap.pre_accept_sent,
            snap.pre_accept_errors,
            snap.pre_accept_timeouts,
            snap.pre_accept_queue_full,
            pre.count,
            pre.avg_ms,
            pre.p95_ms,
            pre.p99_ms,
            pre.max_ms,
            snap.pre_accept_wait_count,
            snap.pre_accept_wait_avg_ms,
            snap.pre_accept_wait_max_ms,
            snap.pre_accept_last_enqueue_age_ms,
            snap.pre_accept_last_dequeue_age_ms,
            snap.accept_queue,
            snap.accept_inflight,
            snap.accept_sent,
            snap.accept_errors,
            snap.accept_timeouts,
            snap.accept_queue_full,
            acc.count,
            acc.avg_ms,
            acc.p95_ms,
            acc.p99_ms,
            acc.max_ms,
            snap.accept_wait_count,
            snap.accept_wait_avg_ms,
            snap.accept_wait_max_ms,
            snap.accept_last_enqueue_age_ms,
            snap.accept_last_dequeue_age_ms,
            snap.commit_queue,
            snap.commit_inflight,
            snap.commit_sent,
            snap.commit_errors,
            snap.commit_timeouts,
            snap.commit_queue_full,
            com.count,
            com.avg_ms,
            com.p95_ms,
            com.p99_ms,
            com.max_ms,
            snap.commit_wait_count,
            snap.commit_wait_avg_ms,
            snap.commit_wait_max_ms,
            snap.commit_last_enqueue_age_ms,
            snap.commit_last_dequeue_age_ms,
            snap.recover_queue,
            snap.recover_queue_peak,
            snap.recover_inflight,
            snap.recover_sent,
            snap.recover_errors,
            snap.recover_queue_full,
            rec.count,
            rec.avg_ms,
            rec.p95_ms,
            rec.p99_ms,
            rec.max_ms,
            snap.recover_wait_count,
            snap.recover_wait_avg_ms,
            snap.recover_wait_max_ms,
            snap.recover_last_enqueue_age_ms,
            snap.recover_last_dequeue_age_ms,
            snap.recover_inflight_txns,
            snap.recover_inflight_peak,
            snap.recover_coalesced,
            snap.recover_enqueued,
            snap.recover_waiters_peak,
            snap.recover_waiters_avg,
            snap.rpc_inflight_limit,
        );
    }
}

/// Read RSS in kilobytes from `ps` for cross-checking sysinfo units.
fn ps_rss_kb(pid: u32) -> Option<u64> {
    let output = std::process::Command::new("ps")
        .arg("-o")
        .arg("rss=")
        .arg("-p")
        .arg(pid.to_string())
        .output()
        .ok()?;
    if !output.status.success() {
        // `ps` failed; skip the cross-check.
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let value = text.split_whitespace().next()?;
    value.parse::<u64>().ok()
}

/// Log a single RPC batch category summary.
fn log_rpc_batch(
    kind: &str,
    batches: u64,
    items: u64,
    rpc_us: u64,
    wait_us: u64,
    max_batch: u64,
    max_wait_us: u64,
    errors: u64,
) {
    if batches == 0 && errors == 0 {
        // Skip empty stats.
        return;
    }
    let avg_batch = if batches > 0 {
        // Average items per batch when we have samples.
        items as f64 / batches as f64
    } else {
        // No batches recorded.
        0.0
    };
    let avg_rpc_ms = avg_us_per(rpc_us, batches);
    let avg_wait_ms = avg_us_per(wait_us, items);
    let max_wait_ms = max_wait_us as f64 / 1000.0;

    tracing::info!(
        rpc = kind,
        batches = batches,
        items = items,
        avg_batch = avg_batch,
        avg_rpc_ms = avg_rpc_ms,
        avg_wait_ms = avg_wait_ms,
        max_batch = max_batch,
        max_wait_ms = max_wait_ms,
        errors = errors,
        "rpc stats"
    );
}

/// Compute average duration in milliseconds from total microseconds and count.
fn avg_us_per(total_us: u64, count: u64) -> f64 {
    if count == 0 {
        // Avoid divide-by-zero.
        return 0.0;
    }
    (total_us as f64 / count as f64) / 1000.0
}

#[tokio::main]
/// Parse CLI args, initialize logging, and run the requested subcommand.
async fn main() -> anyhow::Result<()> {
    // Enable ANSI colors only when stdout is a terminal and NO_COLOR is unset.
    let ansi = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    tracing_subscriber::fmt()
        .with_ansi(ansi)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,h2=warn,hyper=warn".into()),
        )
        .init();

    let args = Args::parse();
    match args.cmd {
        // Dispatch to the node runtime.
        Command::Node(args) => run_node(args).await,
    }
}

/// Initialize storage, transport, consensus groups, and servers for a node.
async fn run_node(args: NodeArgs) -> anyhow::Result<()> {
    let data_dir = PathBuf::from(&args.data_dir);
    fs::create_dir_all(&data_dir).context("create data dir")?;
    let storage_dir = data_dir.join("storage");
    fs::create_dir_all(&storage_dir).context("create storage dir")?;
    let wal_dir = data_dir.join("wal");
    fs::create_dir_all(&wal_dir).context("create wal dir")?;

    let mut fjall_cfg = fjall::Config::new(&storage_dir);
    if args.fjall_manual_journal_persist {
        // Allow WAL to be the durability source by disabling auto journal persistence.
        fjall_cfg = fjall_cfg.manual_journal_persist(true);
    }
    if let Some(ms) = args.fjall_fsync_ms {
        if ms > 0 {
            // Configure periodic fsync if explicitly enabled.
            fjall_cfg = fjall_cfg.fsync_ms(Some(ms));
        }
    }
    let keyspace = Arc::new(fjall_cfg.open().context("open fjall keyspace")?);
    let shared_wal_dir = match args.wal_engine {
        WalEngine::File => wal_dir.join("commitlog"),
        WalEngine::RaftEngine => wal_dir.join("raft-engine"),
    };
    let shared_wal: Arc<dyn accord::CommitLog> = match args.wal_engine {
        WalEngine::File => Arc::new(wal::FileWal::open_dir(&shared_wal_dir)?),
        WalEngine::RaftEngine => {
            #[cfg(feature = "raft-engine")]
            {
                Arc::new(wal::RaftEngineWal::open_dir(&shared_wal_dir)?)
            }
            #[cfg(not(feature = "raft-engine"))]
            {
                anyhow::bail!(
                    "raft-engine WAL selected but feature is not enabled (build with --features raft-engine)"
                );
            }
        }
    };
    let kv_engine_impl = Arc::new(FjallEngine::open(keyspace.clone())?);

    if args.bootstrap == args.join.is_some() {
        // Exactly one of --bootstrap or --join must be selected.
        anyhow::bail!("exactly one of --bootstrap or --join must be set");
    }

    let seed_members = parse_members(&args.initial_members)?;
    let join_snapshot = if let Some(seed) = args.join {
        // Join first and fetch a control-plane snapshot before initializing
        // transport/groups so this node starts from the current cluster view.
        Some(
            join_seed_and_fetch_snapshot(
                args.node_id,
                seed,
                args.listen_grpc,
                args.listen_redis,
            )
            .await?,
        )
    } else {
        None
    };

    let member_infos: BTreeMap<NodeId, MemberInfo> = seed_members
        .iter()
        .map(|(id, addr)| {
            (
                *id,
                MemberInfo {
                    node_id: *id,
                    grpc_addr: addr.to_string(),
                    // `initial_members` only contains gRPC addrs; seed our own Redis addr.
                    redis_addr: if *id == args.node_id {
                        args.listen_redis.to_string()
                    } else {
                        String::new()
                    },
                    state: MemberState::Active,
                },
            )
        })
        .collect();

    let cluster_state_path = data_dir.join("meta").join("cluster_state.json");
    let cluster_store = ClusterStateStore::load_or_init(
        cluster_state_path,
        member_infos,
        args.replication_factor.max(1),
        args.initial_ranges.max(1).min(args.data_shards.max(1)),
    )?;
    if let Some(snapshot) = join_snapshot {
        cluster_store.install_snapshot(snapshot)?;
    }
    let runtime_members = cluster_store.members_map()?;

    // Clamp timeouts to at least 1ms to avoid zero-duration timeouts.
    let rpc_timeout = Duration::from_millis(args.rpc_timeout_ms.max(1));
    let commit_timeout = Duration::from_millis(args.commit_timeout_ms.max(1));
    let propose_timeout = Duration::from_millis(args.propose_timeout_ms.max(1));

    let inflight_tuning = transport::InflightTuning {
        min: args.rpc_inflight_min,
        max: args.rpc_inflight_max,
        high_wait_ms: args.rpc_inflight_high_wait_ms,
        low_wait_ms: args.rpc_inflight_low_wait_ms,
        high_queue: args.rpc_inflight_high_queue,
        low_queue: args.rpc_inflight_low_queue,
    };
    let transport = Arc::new(GrpcTransport::new(
        &runtime_members,
        rpc_timeout,
        commit_timeout,
        args.rpc_inflight_limit,
        inflight_tuning,
        args.rpc_batch_max,
        Duration::from_micros(args.rpc_batch_wait_us),
    ));
    if args.rpc_stats_interval_ms > 0 {
        // Periodically log transport-level RPC stats.
        let stats = transport.stats();
        let interval = Duration::from_millis(args.rpc_stats_interval_ms.max(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let snap = stats.snapshot_and_reset();
                log_rpc_stats(&snap);
            }
        });
    }

    if args.rpc_queue_stats_interval_ms > 0 {
        // Periodically log per-peer queue stats (and optionally CSV).
        let transport = transport.clone();
        let interval = Duration::from_millis(args.rpc_queue_stats_interval_ms.max(1));
        let node_id = args.node_id;
        let csv_path = args
            .peer_stats_csv
            .clone()
            .map(|path| path.replace("{node_id}", &args.node_id.to_string()));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            let mut csv = csv_path.and_then(|path| {
                let writer = PeerStatsCsv::open(&path);
                if writer.is_none() {
                    // Warn if CSV output could not be initialized.
                    tracing::warn!(path = %path, "failed to open peer stats csv");
                }
                writer
            });
            loop {
                ticker.tick().await;
                let snaps = transport.peer_stats_snapshots().await;
                for (peer_id, snap) in &snaps {
                    log_peer_queue_stats(*peer_id, snap);
                }
                log_peer_summary(&snaps);
                if let Some(writer) = csv.as_mut() {
                    // Append per-peer stats to the CSV file when enabled.
                    let ts_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis();
                    for (peer_id, snap) in &snaps {
                        writer.write_snapshot(ts_ms, node_id, *peer_id, snap);
                    }
                    let _ = writer.writer.flush();
                }
            }
        });
    }

    let rpc_handler_stats = Arc::new(RpcHandlerStats::default());
    let proposal_stats = Arc::new(ProposalStats::default());
    let proposal_stats_enabled = args.proposal_stats_interval_ms > 0;
    if proposal_stats_enabled {
        // Periodically log proposal stats.
        let stats = proposal_stats.clone();
        let interval = Duration::from_millis(args.proposal_stats_interval_ms.max(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let snap = stats.snapshot_and_reset();
                log_proposal_stats(&snap);
            }
        });
    }

    if args.rpc_queue_stats_interval_ms > 0 {
        // Periodically log server-side RPC handler stats.
        let stats = rpc_handler_stats.clone();
        let interval = Duration::from_millis(args.rpc_queue_stats_interval_ms.max(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let snap = stats.snapshot_and_reset();
                tracing::info!(
                    pre_accept_avg_ms = snap.pre_accept_avg_ms,
                    pre_accept_p99_ms = snap.pre_accept_p99_ms,
                    pre_accept_batch_avg_ms = snap.pre_accept_batch_avg_ms,
                    pre_accept_batch_p99_ms = snap.pre_accept_batch_p99_ms,
                    pre_accept_inflight = snap.pre_accept_inflight,
                    pre_accept_inflight_peak = snap.pre_accept_inflight_peak,
                    accept_avg_ms = snap.accept_avg_ms,
                    accept_p99_ms = snap.accept_p99_ms,
                    accept_batch_avg_ms = snap.accept_batch_avg_ms,
                    accept_batch_p99_ms = snap.accept_batch_p99_ms,
                    accept_inflight = snap.accept_inflight,
                    accept_inflight_peak = snap.accept_inflight_peak,
                    commit_avg_ms = snap.commit_avg_ms,
                    commit_p99_ms = snap.commit_p99_ms,
                    commit_batch_avg_ms = snap.commit_batch_avg_ms,
                    commit_batch_p99_ms = snap.commit_batch_p99_ms,
                    commit_inflight = snap.commit_inflight,
                    commit_inflight_peak = snap.commit_inflight_peak,
                    "rpc handler stats"
                );
            }
        });
    }

    if args.proc_stats_interval_ms > 0 {
        // Periodically log process CPU and memory usage.
        let interval = Duration::from_millis(args.proc_stats_interval_ms.max(1));
        let node_id = args.node_id;
        let use_ps = args.proc_stats_use_ps;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            let mut system = System::new();
            let pid = Pid::from(std::process::id() as usize);
            loop {
                ticker.tick().await;
                system.refresh_process(pid);
                if let Some(proc) = system.process(pid) {
                    // Only log stats when the process info is available.
                    let cpu = proc.cpu_usage();
                    let rss_raw = proc.memory();
                    let vmem_raw = proc.virtual_memory();
                    // Optionally sample RSS via `ps` for unit cross-checking.
                    let ps_rss = if use_ps {
                        ps_rss_kb(std::process::id())
                    } else {
                        None
                    };
                    let sysinfo_is_bytes = ps_rss
                        .map(|ps_kb| {
                            let ps_bytes = ps_kb.saturating_mul(1024);
                            let lower = ps_bytes / 2;
                            let upper = ps_bytes.saturating_mul(2);
                            rss_raw >= lower && rss_raw <= upper
                        })
                        .unwrap_or(false);
                    // Normalize sysinfo units to bytes for consistent logging.
                    let rss_bytes = if sysinfo_is_bytes {
                        rss_raw
                    } else {
                        rss_raw.saturating_mul(1024)
                    };
                    let vmem_bytes = if sysinfo_is_bytes {
                        vmem_raw
                    } else {
                        vmem_raw.saturating_mul(1024)
                    };
                    let rss_mb = rss_bytes as f64 / (1024.0 * 1024.0);
                    let vmem_mb = vmem_bytes as f64 / (1024.0 * 1024.0);
                    let run_s = proc.run_time();
                    tracing::info!(
                        node_id = node_id,
                        cpu_pct = cpu,
                        rss_raw = rss_raw,
                        vmem_raw = vmem_raw,
                        sysinfo_units = if sysinfo_is_bytes { "bytes" } else { "kb" },
                        rss_bytes = rss_bytes,
                        rss_mb = rss_mb,
                        vmem_bytes = vmem_bytes,
                        vmem_mb = vmem_mb,
                        ps_rss_kb = ps_rss,
                        run_s = run_s,
                        "proc stats"
                    );
                }
            }
        });
    }

    let data_shards = args.data_shards.max(1);
    let mut kv_shards: Vec<Arc<dyn KvEngine>> = Vec::with_capacity(data_shards);
    if data_shards == 1 {
        // Single shard: reuse the default Fjall engine.
        kv_shards.push(kv_engine_impl.clone());
    } else {
        // Multiple shards: open separate Fjall partitions per shard.
        for shard in 0..data_shards {
            let engine = Arc::new(FjallEngine::open_shard(keyspace.clone(), shard)?);
            kv_shards.push(engine);
        }
    }
    let kv_engine: Arc<dyn KvEngine> = if data_shards == 1 {
        kv_shards[0].clone()
    } else {
        match args.routing_mode {
            RoutingMode::Hash => Arc::new(kv::ShardedKvEngine::new(kv_shards.clone())?),
            RoutingMode::Range => Arc::new(kv::RoutedKvEngine::new(
                kv_shards.clone(),
                Arc::new(cluster_store.clone()),
            )?),
        }
    };
    let accord_members = runtime_members
        .keys()
        .copied()
        .map(|id| accord::Member { id })
        .collect::<Vec<_>>();

    let mk_cfg = |group_id: GroupId| accord::Config {
        group_id,
        node_id: args.node_id,
        members: accord_members.clone(),
        rpc_timeout,
        propose_timeout,
        preaccept_stall_hits: args.preaccept_stall_hits.max(1),
        execute_batch_max: args.accord_execute_batch_max.max(1),
        inline_command_in_accept_commit: args.accord_inline_command_in_accept_commit,
        commit_log_batch_max: args.commit_log_batch_max.max(1),
        commit_log_batch_wait: Duration::from_micros(args.commit_log_batch_wait_us),
    };

    let meta_wal_dir = match args.wal_engine {
        WalEngine::File => wal_dir.join("meta"),
        WalEngine::RaftEngine => wal_dir.join("meta-raft-engine"),
    };
    let meta_wal: Arc<dyn accord::CommitLog> = match args.wal_engine {
        WalEngine::File => Arc::new(wal::FileWal::open_dir(&meta_wal_dir)?),
        WalEngine::RaftEngine => {
            #[cfg(feature = "raft-engine")]
            {
                Arc::new(wal::RaftEngineWal::open_dir(&meta_wal_dir)?)
            }
            #[cfg(not(feature = "raft-engine"))]
            {
                anyhow::bail!(
                    "raft-engine WAL selected but feature is not enabled (build with --features raft-engine)"
                );
            }
        }
    };

    let cluster_sm = Arc::new(ClusterStateMachine::new(
        cluster_store.clone(),
        Some(Arc::new(cluster::FjallRangeMigrator::new(keyspace.clone()))),
        data_shards,
    ));
    let membership_group = Arc::new(accord::Group::new(
        mk_cfg(GROUP_MEMBERSHIP),
        transport.clone(),
        cluster_sm,
        Some(meta_wal),
    ));

    let mut data_groups: Vec<Arc<accord::Group>> = Vec::with_capacity(data_shards);
    let mut data_handles: Vec<accord::Handle> = Vec::with_capacity(data_shards);
    let mut groups = HashMap::new();
    groups.insert(GROUP_MEMBERSHIP, membership_group.clone());

    let split_lock = cluster_store.split_lock();
    let data_group_slots = (0..data_shards)
        .map(|_| Arc::new(std::sync::RwLock::new(None::<std::sync::Weak<accord::Group>>)))
        .collect::<Vec<_>>();

    for shard in 0..data_shards {
        let group_id = GROUP_DATA_BASE + shard as u64;
        let wal = shared_wal.clone();
        let group_slot = data_group_slots[shard].clone();
        let membership_hook: Arc<kv::MembershipUpdateHook> = Arc::new(move |reconfig| {
            let maybe_group = group_slot
                .read()
                .ok()
                .and_then(|slot| slot.as_ref().and_then(|weak| weak.upgrade()));
            let group = maybe_group
                .ok_or_else(|| anyhow::anyhow!("data group {group_id} is not initialized"))?;
            let members = reconfig
                .members
                .into_iter()
                .map(|id| accord::Member { id })
                .collect::<Vec<_>>();
            group.update_membership(members, reconfig.voters)
        });
        let kv_sm = Arc::new(kv::KvStateMachine::with_membership_hook(
            kv_shards[shard].clone(),
            Some(split_lock.clone()),
            Some(membership_hook),
        ));
        let data_group = Arc::new(accord::Group::new(
            mk_cfg(group_id),
            transport.clone(),
            kv_sm,
            Some(wal),
        ));
        if let Ok(mut slot) = data_group_slots[shard].write() {
            *slot = Some(Arc::downgrade(&data_group));
        }
        groups.insert(group_id, data_group.clone());
        let replayed = data_group
            .replay_commits()
            .await
            .context("replay commit log")?;
        if replayed > 0 {
            // Log replay to help track startup recovery cost.
            tracing::info!(
                group_id = group_id,
                replayed = replayed,
                "replayed commit log entries"
            );
        }
        data_group.spawn_executor();
        data_handles.push(data_group.handle());
        data_groups.push(data_group);
    }

    let _ = membership_group
        .replay_commits()
        .await
        .context("replay meta commit log")?;
    membership_group.spawn_executor();

    if args.accord_stats_interval_ms > 0 {
        // Periodically log Accord executor stats per data group.
        let data_groups = data_groups.clone();
        let interval = Duration::from_millis(args.accord_stats_interval_ms.max(1));
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                for (idx, group) in data_groups.iter().enumerate() {
                    let stats = group.debug_stats().await;
                    let group_id = GROUP_DATA_BASE + idx as u64;
                    log_accord_stats(group_id, &stats);
                }
            }
        });
    }

    let member_ids = runtime_members.keys().copied().collect::<Vec<_>>();

    let client_batch_stats = Arc::new(ClientBatchStats::default());
    let client_batch_queue = args.client_batch_queue.max(1);
    let (client_set_tx, client_set_rx) = mpsc::channel(client_batch_queue);
    let (client_get_tx, client_get_rx) = mpsc::channel(client_batch_queue);
    let client_batch_cfg = ClientBatchConfig {
        set_max: args.client_set_batch_max.max(1),
        get_max: args.client_get_batch_max.max(1),
        wait: Duration::from_micros(args.client_batch_wait_us),
        inflight: args.client_batch_inflight.max(1),
    };

    let meta_handle = membership_group.handle();
    let range_stats = Arc::new(LocalRangeStats::new(keyspace.clone(), data_shards));
    let state = Arc::new(NodeState {
        initial_members: cluster_store.members_string(),
        groups,
        data_handles,
        data_shards,
        routing_mode: args.routing_mode,
        node_id: args.node_id,
        kv_engine,
        transport: transport.clone(),
        member_ids,
        read_mode: args.read_mode,
        read_barrier_timeout: Duration::from_millis(args.read_barrier_timeout_ms.max(1)),
        read_barrier_fallback_quorum: args.read_barrier_fallback_quorum,
        read_barrier_all_peers: args.read_barrier_all_peers,
        proposal_stats,
        proposal_stats_enabled,
        rpc_handler_stats: rpc_handler_stats.clone(),
        rpc_handler_delay: Duration::from_millis(args.rpc_handler_delay_ms),
        client_set_tx,
        client_get_tx,
        client_batch_stats: client_batch_stats.clone(),
        client_set_batch_target: if args.client_single_key_txn {
            // Force single-key proposals for debug or perf experiments.
            1
        } else {
            args.client_set_batch_target.max(1)
        },
        client_inflight_ops: Arc::new(AtomicU64::new(0)),
        cluster_store,
        meta_handle,
        split_lock: split_lock.clone(),
        shard_load: ShardLoadTracker::new(data_shards),
        range_stats,
    });

    let refresh_state = state.clone();
    tokio::spawn(async move {
        // Force one refresh on startup, then apply on each metadata epoch bump.
        let mut last_epoch = 0u64;
        loop {
            let epoch = refresh_state.cluster_store.epoch();
            if epoch != last_epoch {
                match refresh_state.cluster_store.members_map() {
                    Ok(members) => {
                        refresh_state.transport.update_members(&members);
                    }
                    Err(err) => {
                        tracing::warn!(error = ?err, "failed to refresh cluster members");
                    }
                }
                if let Err(err) = refresh_state.refresh_group_memberships() {
                    tracing::warn!(error = ?err, "failed to refresh group memberships");
                }
                last_epoch = epoch;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    if args.join.is_some() {
        let sync_state = state.clone();
        tokio::spawn(async move {
            // Best-effort control-plane pull for joiners. This keeps late-joining
            // nodes converged even if they temporarily miss meta-group traffic.
            loop {
                let local = sync_state.cluster_store.state();
                let source = local
                    .members
                    .iter()
                    .filter_map(|(id, member)| {
                        (member.state != MemberState::Removed && *id != sync_state.node_id)
                            .then_some(*id)
                    })
                    .min();
                drop(local);

                if let Some(source) = source {
                    match sync_state.transport.cluster_state(source).await {
                        Ok(remote) => {
                            if remote.epoch > sync_state.cluster_store.epoch() {
                                if let Err(err) = sync_state.cluster_store.install_snapshot(remote)
                                {
                                    tracing::warn!(
                                        error = ?err,
                                        source,
                                        "failed to install pulled cluster snapshot"
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            tracing::debug!(
                                error = ?err,
                                source,
                                "failed to pull cluster snapshot from source"
                            );
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    spawn_set_batcher(state.clone(), client_set_rx, client_batch_cfg);
    spawn_get_batcher(state.clone(), client_get_rx, client_batch_cfg);

    // Range manager (Cockroach-style) for dynamic splits.
    range_manager::spawn(
        state.clone(),
        keyspace.clone(),
        RangeManagerConfig {
            interval: Duration::from_millis(args.range_mgr_interval_ms.max(50)),
            split_min_keys: args.range_split_min_keys,
            split_min_qps: args.range_split_min_qps,
            split_qps_sustain: args.range_split_qps_sustain.max(1),
            split_cooldown: Duration::from_millis(args.range_split_cooldown_ms.max(1)),
        },
    );

    if args.rebalance_enabled {
        rebalance_manager::spawn(
            state.clone(),
            RebalanceManagerConfig {
                interval: Duration::from_millis(args.rebalance_interval_ms.max(100)),
                move_timeout: Duration::from_millis(args.rebalance_move_timeout_ms),
            },
        );
    }

    let grpc_addr = args.listen_grpc;
    tokio::spawn({
        let service = RpcService {
            state: state.clone(),
        };
        async move {
            let svc = volo_gen::holo_store::rpc::HoloRpcServer::new(service);
            let svc = volo_grpc::server::ServiceBuilder::new(svc).build::<
                volo_gen::holo_store::rpc::HoloRpcRequestRecv,
                volo_gen::holo_store::rpc::HoloRpcResponseSend,
            >();
            let result = volo_grpc::server::Server::new()
                .add_service(svc)
                .run(volo::net::Address::from(grpc_addr))
                .await;
            if let Err(err) = result {
                // Log server failures without crashing the node task.
                tracing::error!(error = ?err, "gRPC server failed");
            }
        }
    });

    let redis_addr = args.listen_redis;
    tokio::spawn({
        let state = state.clone();
        async move {
            if let Err(err) = redis_server::run(redis_addr, state).await {
                // Log server failures without crashing the node task.
                tracing::error!(error = ?err, "redis server failed");
            }
        }
    });

    tracing::info!(
        node_id = args.node_id,
        redis = %redis_addr,
        grpc = %grpc_addr,
        "node started"
    );

    tokio::signal::ctrl_c().await?;
    Ok(())
}

/// Attempt to join a seed node, then fetch and return a converged control-plane
/// snapshot that includes this joining node as Active.
async fn join_seed_and_fetch_snapshot(
    node_id: u64,
    seed: SocketAddr,
    listen_grpc: SocketAddr,
    listen_redis: SocketAddr,
) -> anyhow::Result<ClusterState> {
    let client = volo_gen::holo_store::rpc::HoloRpcClientBuilder::new("holo_store.rpc.HoloRpc")
        .address(volo::net::Address::from(seed))
        .build();

    // Fast path: if the node is already present in the seed snapshot (typical
    // for preconfigured bootstrap members), do not issue AddNode.
    for _ in 0..300 {
        let resp = client
            .cluster_state(volo_gen::holo_store::rpc::ClusterStateRequest {})
            .await;
        match resp {
            Ok(resp) => {
                let json = resp.into_inner().json.to_string();
                let state: ClusterState = serde_json::from_str(&json)
                    .with_context(|| format!("failed to decode cluster state from seed {seed}"))?;
                let already_active = state
                    .members
                    .get(&node_id)
                    .map(|m| m.state == MemberState::Active)
                    .unwrap_or(false);
                if already_active {
                    return Ok(state);
                }
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut joined = false;
    for _ in 0..300 {
        match client
            .join(volo_gen::holo_store::rpc::JoinRequest {
                node_id,
                grpc_addr: listen_grpc.to_string().into(),
                redis_addr: listen_redis.to_string().into(),
            })
            .await
        {
            Ok(_) => {
                joined = true;
                break;
            }
            // Retry after a short delay if the seed is not ready.
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    if !joined {
        anyhow::bail!("failed to join seed {seed}");
    }

    for _ in 0..300 {
        let resp = client
            .cluster_state(volo_gen::holo_store::rpc::ClusterStateRequest {})
            .await;
        match resp {
            Ok(resp) => {
                let json = resp.into_inner().json.to_string();
                let state: ClusterState = serde_json::from_str(&json)
                    .with_context(|| format!("failed to decode cluster state from seed {seed}"))?;
                let joined = state
                    .members
                    .get(&node_id)
                    .map(|m| m.state == MemberState::Active)
                    .unwrap_or(false);
                if joined {
                    return Ok(state);
                }
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    anyhow::bail!("joined seed {seed} but failed to observe node {node_id} as Active");
}

/// Parse a comma-separated `id@host:port` membership string.
fn parse_members(input: &str) -> anyhow::Result<HashMap<NodeId, SocketAddr>> {
    let mut out = HashMap::new();
    for part in input.split(',').filter(|s| !s.trim().is_empty()) {
        let (id, addr) = part
            .split_once('@')
            .with_context(|| format!("invalid member entry (expected id@host:port): {part}"))?;
        let id: NodeId = id.parse().context("invalid member id")?;
        let addr: SocketAddr = addr.parse().context("invalid member addr")?;
        out.insert(id, addr);
    }
    anyhow::ensure!(!out.is_empty(), "initial_members is empty");
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_state() -> ClusterState {
        ClusterState {
            epoch: 1,
            frozen: false,
            replication_factor: 3,
            members: BTreeMap::new(),
            shards: vec![],
            shard_replica_roles: BTreeMap::new(),
            shard_rebalances: BTreeMap::new(),
        }
    }

    fn sample_shard() -> ShardDesc {
        ShardDesc {
            shard_id: 10,
            shard_index: 2,
            start_hash: 0,
            end_hash: 0,
            start_key: vec![],
            end_key: vec![],
            replicas: vec![1, 2, 3],
            leaseholder: 1,
        }
    }

    #[test]
    fn shard_membership_sets_excludes_learners_from_voters() {
        let mut state = sample_state();
        let shard = sample_shard();
        state.shards.push(shard.clone());
        state.shard_replica_roles.insert(
            shard.shard_id,
            BTreeMap::from([
                (1, ReplicaRole::Voter),
                (2, ReplicaRole::Learner),
                (3, ReplicaRole::Outgoing),
            ]),
        );

        let (members, voters) = shard_membership_sets(&state, &shard);
        assert_eq!(members, vec![1, 2, 3]);
        assert_eq!(voters, vec![1, 3]);
    }

    #[test]
    fn shard_membership_sets_fallbacks_when_all_roles_are_learners() {
        let mut state = sample_state();
        let shard = sample_shard();
        state.shards.push(shard.clone());
        state.shard_replica_roles.insert(
            shard.shard_id,
            BTreeMap::from([
                (1, ReplicaRole::Learner),
                (2, ReplicaRole::Learner),
                (3, ReplicaRole::Learner),
            ]),
        );

        let (members, voters) = shard_membership_sets(&state, &shard);
        assert_eq!(members, vec![1, 2, 3]);
        assert_eq!(voters, vec![1, 2, 3]);
    }
}

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
