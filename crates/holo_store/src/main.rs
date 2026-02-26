// HoloStore node binary entry point.
//
// This file wires together the Accord consensus groups, KV engine, WAL, gRPC
// transport, and Redis protocol server. It also hosts the CLI and runtime
// configuration, as well as performance/statistics logging.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, IsTerminal, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context};
use clap::{Parser, Subcommand};
use fjall::{Keyspace, PartitionCreateOptions, PersistMode};
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use holo_accord::accord::{self, GroupId, NodeId, TxnId};
use serde::{Deserialize, Serialize};
use sysinfo::{Disks, Pid, System};
use tokio::sync::{mpsc, oneshot, Semaphore};

include!(concat!(env!("OUT_DIR"), "/volo_gen.rs"));

mod cluster;
mod kv;
mod load;
mod meta_manager;
mod range_manager;
mod rebalance_manager;
mod redis_server;
mod rpc_service;
mod transport;
mod wal;

use cluster::{
    ClusterState, ClusterStateMachine, ClusterStateStore, ControllerDomain, ControllerFence,
    MemberInfo, MemberState, ReplicaRole, ShardDesc,
};
use kv::{FjallEngine, KvEngine};
use load::ShardLoadTracker;
use meta_manager::MetaManagerConfig;
use range_manager::RangeManagerConfig;
use rebalance_manager::RebalanceManagerConfig;
use rpc_service::RpcService;
use transport::GrpcTransport;

/// Group id used for the membership/control group.
const GROUP_MEMBERSHIP: GroupId = 0;
/// Base group id for additional metadata groups (meta index >= 1).
const GROUP_META_EXTRA_BASE: GroupId = 10_000;
/// Base group id for data shards (shard index is added to this base).
const GROUP_DATA_BASE: GroupId = 1;
/// Auto shard-slot target used when `--max-shards=0` ("no explicit cap").
///
/// The current runtime pre-creates shard groups at startup, so this is a
/// practical upper bound for automatic splitting rather than true infinity.
const AUTO_MAX_SHARDS: usize = 64;

type LocalNodeStateRegistry = HashMap<SocketAddr, Weak<NodeState>>;
static LOCAL_NODE_STATE_REGISTRY: OnceLock<RwLock<LocalNodeStateRegistry>> = OnceLock::new();

fn local_node_state_registry() -> &'static RwLock<LocalNodeStateRegistry> {
    LOCAL_NODE_STATE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

pub(crate) fn register_local_node_state(grpc_addr: SocketAddr, state: &Arc<NodeState>) {
    if let Ok(mut registry) = local_node_state_registry().write() {
        registry.insert(grpc_addr, Arc::downgrade(state));
    }
}

pub(crate) fn unregister_local_node_state(grpc_addr: SocketAddr) {
    if let Ok(mut registry) = local_node_state_registry().write() {
        registry.remove(&grpc_addr);
    }
}

#[allow(dead_code)]
pub(crate) fn lookup_local_node_state(grpc_addr: SocketAddr) -> Option<Arc<NodeState>> {
    if let Ok(registry) = local_node_state_registry().read() {
        let Some(entry) = registry.get(&grpc_addr) else {
            return None;
        };
        if let Some(state) = entry.upgrade() {
            return Some(state);
        }
    } else {
        return None;
    }

    // Clean up stale entries when embedded nodes have already shut down.
    if let Ok(mut registry) = local_node_state_registry().write() {
        let stale = registry
            .get(&grpc_addr)
            .and_then(std::sync::Weak::upgrade)
            .is_none();
        if stale {
            registry.remove(&grpc_addr);
        }
    }
    None
}

fn resolve_data_shards(max_shards: usize) -> usize {
    if max_shards == 0 {
        AUTO_MAX_SHARDS
    } else {
        max_shards.max(1)
    }
}

fn parse_positive_env_usize(var_name: &str) -> Option<usize> {
    std::env::var(var_name)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn resolve_range_write_batch_target(configured: usize) -> usize {
    if std::env::var_os("HOLO_RANGE_WRITE_BATCH_TARGET").is_some() {
        return configured.max(1);
    }
    if let Some(legacy) = parse_positive_env_usize("HOLO_SQL_WRITE_BATCH_TARGET") {
        tracing::warn!(
            legacy_env = "HOLO_SQL_WRITE_BATCH_TARGET",
            replacement_env = "HOLO_RANGE_WRITE_BATCH_TARGET",
            "legacy range-write batch target env is deprecated; use replacement env"
        );
        return legacy;
    }
    configured.max(1)
}

fn resolve_range_write_batch_max_bytes(configured: usize) -> usize {
    if std::env::var_os("HOLO_RANGE_WRITE_BATCH_MAX_BYTES").is_some() {
        return configured.max(1);
    }
    if let Some(legacy) = parse_positive_env_usize("HOLO_SQL_WRITE_BATCH_MAX_BYTES") {
        tracing::warn!(
            legacy_env = "HOLO_SQL_WRITE_BATCH_MAX_BYTES",
            replacement_env = "HOLO_RANGE_WRITE_BATCH_MAX_BYTES",
            "legacy range-write batch byte-cap env is deprecated; use replacement env"
        );
        return legacy;
    }
    configured.max(1)
}

fn meta_group_id_for_index(meta_index: usize) -> GroupId {
    if meta_index == 0 {
        GROUP_MEMBERSHIP
    } else {
        GROUP_META_EXTRA_BASE + meta_index as u64
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

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
pub struct NodeArgs {
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
    ///
    /// Use `0` to run in auto mode (no explicit operator cap).
    #[arg(long = "max-shards", env = "HOLO_MAX_SHARDS", default_value_t = 1)]
    data_shards: usize,

    /// Initial number of key ranges to create in cluster metadata.
    ///
    /// CockroachDB boots with a small number of ranges and splits dynamically.
    /// We default to 1 full-keyspace range.
    #[arg(long, env = "HOLO_INITIAL_RANGES", default_value_t = 1)]
    initial_ranges: usize,

    /// Maximum number of metadata consensus groups.
    ///
    /// `1` keeps the legacy single meta group. Values >1 allow meta-range
    /// splitting and routed control-plane proposals across independent groups.
    #[arg(long, env = "HOLO_META_GROUPS", default_value_t = 1)]
    meta_groups: usize,

    /// Enable automatic metadata-range splitting.
    #[arg(long, env = "HOLO_META_SPLIT_ENABLED", default_value_t = true)]
    meta_split_enabled: bool,

    /// Meta manager evaluation interval (ms).
    #[arg(long, env = "HOLO_META_MGR_INTERVAL_MS", default_value_t = 1000)]
    meta_mgr_interval_ms: u64,

    /// Split a metadata range when its sustained routed command QPS exceeds this threshold.
    #[arg(long, env = "HOLO_META_SPLIT_MIN_QPS", default_value_t = 200)]
    meta_split_min_qps: u64,

    /// Minimum cumulative command count for a metadata range before splitting.
    #[arg(long, env = "HOLO_META_SPLIT_MIN_OPS", default_value_t = 500)]
    meta_split_min_ops: u64,

    /// Require the meta QPS threshold for this many consecutive evaluations.
    #[arg(long, env = "HOLO_META_SPLIT_QPS_SUSTAIN", default_value_t = 5)]
    meta_split_qps_sustain: u8,

    /// Per-meta-index cooldown after a split (ms).
    #[arg(long, env = "HOLO_META_SPLIT_COOLDOWN_MS", default_value_t = 30_000)]
    meta_split_cooldown_ms: u64,

    /// Enable automatic metadata range balancing/reconfiguration.
    #[arg(long, env = "HOLO_META_BALANCE_ENABLED", default_value_t = true)]
    meta_balance_enabled: bool,

    /// Require at least this replica-count skew before moving a meta range replica.
    #[arg(long, env = "HOLO_META_REPLICA_SKEW_THRESHOLD", default_value_t = 1)]
    meta_replica_skew_threshold: usize,

    /// Require at least this leaseholder-count skew before moving a meta lease.
    #[arg(long, env = "HOLO_META_LEASE_SKEW_THRESHOLD", default_value_t = 1)]
    meta_lease_skew_threshold: usize,

    /// Cooldown between automatic meta balancing actions for the same range (ms).
    #[arg(long, env = "HOLO_META_BALANCE_COOLDOWN_MS", default_value_t = 10_000)]
    meta_balance_cooldown_ms: u64,

    /// Warn if a meta move has no progress for longer than this duration (ms).
    #[arg(long, env = "HOLO_META_MOVE_STUCK_WARN_MS", default_value_t = 30_000)]
    meta_move_stuck_warn_ms: u64,

    /// Abort stalled pre-cutover meta moves (or force-finalize post-lease moves) after this duration (ms).
    #[arg(long, env = "HOLO_META_MOVE_TIMEOUT_MS", default_value_t = 180_000)]
    meta_move_timeout_ms: u64,

    /// Warn when a meta group's executed-prefix lag exceeds this threshold.
    #[arg(long, env = "HOLO_META_LAG_WARN_OPS", default_value_t = 128)]
    meta_lag_warn_ops: u64,

    /// Skip meta manager proposals while client inflight ops exceed this threshold.
    #[arg(long, env = "HOLO_META_MAX_CLIENT_INFLIGHT", default_value_t = 10_000)]
    meta_max_client_inflight: u64,

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
    #[arg(long, env = "HOLO_RANGE_SPLIT_MIN_KEYS", default_value_t = 100_000)]
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

    /// Merge adjacent ranges when their combined key count is below this threshold.
    ///
    /// Set to 0 to disable automatic merge proposals.
    #[arg(long, env = "HOLO_RANGE_MERGE_MAX_KEYS", default_value_t = 8_000)]
    range_merge_max_keys: usize,

    /// Merge only when combined sustained SET QPS is below this threshold.
    ///
    /// Set to 0 to disable QPS-based cold gating for merges.
    #[arg(long, env = "HOLO_RANGE_MERGE_MAX_QPS", default_value_t = 5_000)]
    range_merge_max_qps: u64,

    /// Require low merge QPS for this many consecutive evaluations.
    #[arg(long, env = "HOLO_RANGE_MERGE_QPS_SUSTAIN", default_value_t = 3)]
    range_merge_qps_sustain: u8,

    /// Merge key hysteresis threshold as percentage of `range_merge_max_keys`.
    #[arg(
        long,
        env = "HOLO_RANGE_MERGE_KEY_HYSTERESIS_PCT",
        default_value_t = 80
    )]
    range_merge_key_hysteresis_pct: u8,

    /// Cooldown after auto-merge proposals (ms).
    #[arg(long, env = "HOLO_RANGE_MERGE_COOLDOWN_MS", default_value_t = 20_000)]
    range_merge_cooldown_ms: u64,

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
    #[arg(
        long,
        env = "HOLO_REBALANCE_MOVE_TIMEOUT_MS",
        default_value_t = 180_000
    )]
    rebalance_move_timeout_ms: u64,

    /// Metadata controller lease TTL (ms) used for split/rebalance managers.
    #[arg(long, env = "HOLO_CONTROLLER_LEASE_TTL_MS", default_value_t = 5000)]
    controller_lease_ttl_ms: u64,

    /// Renew the controller lease when remaining TTL falls below this (ms).
    #[arg(
        long,
        env = "HOLO_CONTROLLER_LEASE_RENEW_MARGIN_MS",
        default_value_t = 1000
    )]
    controller_lease_renew_margin_ms: u64,

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

    /// Enable periodic durable checkpoints that gate WAL GC.
    #[arg(long, env = "HOLO_RECOVERY_CHECKPOINT_ENABLED", default_value_t = true)]
    recovery_checkpoint_enabled: bool,

    /// Interval for durability checkpoints that couple snapshots to WAL GC (ms).
    #[arg(
        long,
        env = "HOLO_RECOVERY_CHECKPOINT_INTERVAL_MS",
        default_value_t = 5000
    )]
    recovery_checkpoint_interval_ms: u64,

    /// Warn when executed-vs-durable lag exceeds this many WAL entries.
    #[arg(
        long,
        env = "HOLO_RECOVERY_CHECKPOINT_WARN_LAG_ENTRIES",
        default_value_t = 50000
    )]
    recovery_checkpoint_warn_lag_entries: u64,

    /// Skip checkpoint advancement when free disk bytes are below this threshold.
    ///
    /// This is a safety circuit-breaker: if checkpoints are skipped, WAL GC
    /// naturally stalls because durable floors stop advancing.
    #[arg(
        long,
        env = "HOLO_RECOVERY_CHECKPOINT_MIN_FREE_BYTES",
        default_value_t = 268_435_456
    )]
    recovery_checkpoint_min_free_bytes: u64,

    /// Skip checkpoint advancement when free disk percentage is below this threshold.
    #[arg(
        long,
        env = "HOLO_RECOVERY_CHECKPOINT_MIN_FREE_PCT",
        default_value_t = 1.0
    )]
    recovery_checkpoint_min_free_pct: f64,

    /// Durability mode used when checkpointing Fjall state.
    #[arg(
        long,
        env = "HOLO_RECOVERY_CHECKPOINT_PERSIST_MODE",
        default_value = "sync-data"
    )]
    recovery_checkpoint_persist_mode: RecoveryCheckpointPersistMode,

    /// RPC timeout for pre-accept/accept/recover (milliseconds).
    #[arg(long, env = "HOLO_RPC_TIMEOUT_MS", default_value_t = 2000)]
    rpc_timeout_ms: u64,

    /// Commit RPC timeout (milliseconds).
    #[arg(long, env = "HOLO_COMMIT_TIMEOUT_MS", default_value_t = 4000)]
    commit_timeout_ms: u64,

    /// End-to-end propose timeout (milliseconds).
    #[arg(long, env = "HOLO_PROPOSE_TIMEOUT_MS", default_value_t = 10000)]
    propose_timeout_ms: u64,

    /// Minimum delay before retrying stalled transaction recovery (milliseconds).
    #[arg(long, env = "HOLO_RECOVERY_MIN_DELAY_MS", default_value_t = 200)]
    recovery_min_delay_ms: u64,

    /// Minimum interval between executor stall-recovery probes (milliseconds).
    #[arg(long, env = "HOLO_STALL_RECOVER_INTERVAL_MS", default_value_t = 100)]
    stall_recover_interval_ms: u64,

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

    /// Max in-flight proposals per shard for direct SET batch execution.
    #[arg(
        long,
        env = "HOLO_CLIENT_SET_PROPOSAL_PIPELINE_DEPTH",
        default_value_t = 8
    )]
    client_set_proposal_pipeline_depth: usize,

    /// Target max range-write operations per replicated proposal.
    ///
    /// This is intentionally decoupled from `HOLO_CLIENT_SET_BATCH_TARGET` so
    /// Redis/SET latency tuning does not throttle range-write throughput.
    #[arg(
        long,
        env = "HOLO_RANGE_WRITE_BATCH_TARGET",
        visible_alias = "sql-write-batch-target",
        default_value_t = 1_024
    )]
    range_write_batch_target: usize,

    /// Approximate byte cap for range-write operations per proposal.
    ///
    /// A value of `0` is treated as `1` internally to preserve bounded chunks.
    #[arg(
        long,
        env = "HOLO_RANGE_WRITE_BATCH_MAX_BYTES",
        visible_alias = "sql-write-batch-max-bytes",
        default_value_t = 1_048_576
    )]
    range_write_batch_max_bytes: usize,

    /// Max in-flight proposals per shard for replicated range-write execution.
    #[arg(
        long,
        env = "HOLO_RANGE_WRITE_PROPOSAL_PIPELINE_DEPTH",
        default_value_t = 8
    )]
    range_write_proposal_pipeline_depth: usize,

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
    meta_proposal_stats: Arc<MetaProposalStats>,
    rpc_handler_stats: Arc<RpcHandlerStats>,
    rpc_handler_delay: Duration,
    client_set_tx: mpsc::Sender<BatchSetWork>,
    client_get_tx: mpsc::Sender<BatchGetWork>,
    client_batch_stats: Arc<ClientBatchStats>,
    client_set_batch_target: usize,
    /// Per-shard max in-flight proposals for direct client SET execution.
    ///
    /// Design: bounded pipelining keeps throughput high without unbounded queue growth.
    client_set_proposal_pipeline_depth: usize,
    /// Entry target for range-write proposal chunking (SQL write path).
    range_write_batch_target: usize,
    /// Byte cap for range-write proposal chunking.
    range_write_batch_max_bytes: usize,
    /// Per-shard max in-flight proposals for range-write execution.
    range_write_proposal_pipeline_depth: usize,
    client_inflight_ops: Arc<AtomicU64>,
    cluster_store: ClusterStateStore,
    meta_handles: BTreeMap<usize, accord::Handle>,
    controller_lease_ttl: Duration,
    controller_lease_renew_margin: Duration,
    meta_move_stuck_warn: Duration,
    split_lock: Arc<std::sync::RwLock<()>>,
    sql_write_locks: Arc<Vec<tokio::sync::Mutex<()>>>,
    shard_load: ShardLoadTracker,
    split_health: Arc<SplitHealthStats>,
    range_stats: Arc<LocalRangeStats>,
    recovery_checkpoints: Arc<RecoveryCheckpointStats>,
    recovery_checkpoint_controller: Option<RecoveryCheckpointController>,
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

/// One conditional latest-visible KV write with optimistic version checking.
#[derive(Clone, Debug)]
pub(crate) struct RangeConditionalLatestEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: kv::Version,
    pub expected_version: kv::Version,
}

/// Result from a conditional range-apply operation.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RangeConditionalApplyResult {
    pub applied: u64,
    pub conflicts: u64,
}

/// One replicated range-write entry used by SQL mutation paths.
#[derive(Clone, Debug)]
pub(crate) struct RangeWriteEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// One conditional replicated range-write entry.
#[derive(Clone, Debug)]
pub(crate) struct RangeConditionalWriteEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expected_version: kv::Version,
}

/// A key/version pair applied during a successful conditional range write.
#[derive(Clone, Debug)]
pub(crate) struct RangeConditionalAppliedVersion {
    pub key: Vec<u8>,
    pub version: kv::Version,
}

/// Result from a replicated conditional range-write operation.
#[derive(Clone, Debug, Default)]
pub(crate) struct RangeConditionalWriteResult {
    pub applied: u64,
    pub conflicts: u64,
    pub applied_versions: Vec<RangeConditionalAppliedVersion>,
}

/// Local range statistics provider backed by Fjall latest-index partitions.
#[derive(Clone)]
struct LocalRangeStats {
    keyspace: Arc<Keyspace>,
    max_shards: usize,
    apply_lock: Arc<std::sync::Mutex<()>>,
    record_counts: Arc<Vec<AtomicI64>>,
}

impl LocalRangeStats {
    fn new(keyspace: Arc<Keyspace>, max_shards: usize) -> anyhow::Result<Self> {
        let max_shards = max_shards.max(1);
        let mut counters = Vec::with_capacity(max_shards);
        let probe = Self {
            keyspace: keyspace.clone(),
            max_shards,
            apply_lock: Arc::new(std::sync::Mutex::new(())),
            record_counts: Arc::new(Vec::new()),
        };
        for shard_index in 0..max_shards {
            let count = probe.scan_partition_count(shard_index)?;
            counters.push(AtomicI64::new(count as i64));
        }
        Ok(Self {
            keyspace,
            max_shards,
            apply_lock: Arc::new(std::sync::Mutex::new(())),
            record_counts: Arc::new(counters),
        })
    }

    fn latest_partition_name(&self, shard_index: usize) -> String {
        if self.max_shards <= 1 {
            "kv_latest".to_string()
        } else {
            format!("kv_latest_{shard_index}")
        }
    }

    fn versions_partition_name(&self, shard_index: usize) -> String {
        if self.max_shards <= 1 {
            "kv_versions".to_string()
        } else {
            format!("kv_versions_{shard_index}")
        }
    }

    /// Count all latest-index entries for one shard partition.
    fn scan_partition_count(&self, shard_index: usize) -> anyhow::Result<u64> {
        let latest_name = self.latest_partition_name(shard_index);
        let latest = self
            .keyspace
            .open_partition(&latest_name, PartitionCreateOptions::default())?;
        let mut iter: Box<dyn Iterator<Item = fjall::Result<fjall::KvPair>>> =
            Box::new(latest.range(Vec::<u8>::new()..));
        let mut count = 0u64;
        while let Some(item) = iter.next() {
            let _ = item?;
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    fn record_count(&self, shard_index: usize) -> u64 {
        self.record_counts
            .get(shard_index)
            .map(|v| v.load(Ordering::Relaxed).max(0) as u64)
            .unwrap_or(0)
    }

    fn adjust_record_count(&self, shard_index: usize, delta: i64) {
        if delta == 0 {
            return;
        }
        let Some(counter) = self.record_counts.get(shard_index) else {
            return;
        };
        loop {
            let cur = counter.load(Ordering::Relaxed);
            let next = (i128::from(cur) + i128::from(delta)).clamp(0, i128::from(i64::MAX)) as i64;
            if counter
                .compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    fn on_visible_insertions(&self, shard_index: usize, inserted_keys: i64) {
        self.adjust_record_count(shard_index, inserted_keys);
    }

    fn on_range_migration(&self, from_shard: usize, to_shard: usize, moved_keys: u64) {
        if moved_keys == 0 || from_shard == to_shard {
            return;
        }
        let moved = moved_keys.min(i64::MAX as u64) as i64;
        self.adjust_record_count(from_shard, -moved);
        self.adjust_record_count(to_shard, moved);
    }

    /// Read one paged snapshot of latest-visible rows for `[start_key, end_key)`.
    fn snapshot_latest_page(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        cursor: &[u8],
        limit: usize,
        reverse: bool,
    ) -> anyhow::Result<(Vec<RangeSnapshotLatestEntry>, Vec<u8>, bool)> {
        let limit = limit.clamp(1, 10_000);
        let latest_name = self.latest_partition_name(shard_index);
        let latest = self
            .keyspace
            .open_partition(&latest_name, PartitionCreateOptions::default())?;

        let mut iter: Box<dyn Iterator<Item = fjall::Result<fjall::KvPair>>> = if reverse {
            let lower = start_key.to_vec();
            let mut upper = if end_key.is_empty() {
                Vec::new()
            } else {
                end_key.to_vec()
            };
            if !cursor.is_empty() && (upper.is_empty() || cursor < upper.as_slice()) {
                upper = cursor.to_vec();
            }
            if !upper.is_empty() && lower >= upper {
                return Ok((Vec::new(), Vec::new(), true));
            }
            if upper.is_empty() {
                Box::new(latest.range(lower..).rev())
            } else {
                Box::new(latest.range(lower..upper).rev())
            }
        } else {
            let mut lower = start_key.to_vec();
            if !cursor.is_empty() && cursor > lower.as_slice() {
                lower = cursor.to_vec();
            }
            if end_key.is_empty() {
                Box::new(latest.range(lower..))
            } else {
                Box::new(latest.range(lower..end_key.to_vec()))
            }
        };

        let mut out = Vec::with_capacity(limit);
        let mut last_key = Vec::new();
        let mut has_more = false;
        while let Some(item) = iter.next() {
            let (key, value) = item?;
            let key = key.to_vec();
            if !cursor.is_empty() {
                if reverse && key.as_slice() >= cursor {
                    continue;
                }
                if !reverse && key.as_slice() <= cursor {
                    continue;
                }
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
        let _guard = self
            .apply_lock
            .lock()
            .map_err(|_| anyhow!("range apply lock poisoned"))?;
        let engine: Arc<dyn KvEngine> = if self.max_shards <= 1 {
            Arc::new(FjallEngine::open(self.keyspace.clone())?)
        } else {
            Arc::new(FjallEngine::open_shard(self.keyspace.clone(), shard_index)?)
        };
        let mut applied = 0u64;
        let mut inserted_latest = 0i64;
        for entry in entries {
            let in_start = start_key.is_empty() || entry.key.as_slice() >= start_key;
            let in_end = end_key.is_empty() || entry.key.as_slice() < end_key;
            if !in_start || !in_end {
                continue;
            }
            engine.set(entry.key.clone(), entry.value.clone(), entry.version);
            if engine.mark_visible(&entry.key, entry.version) {
                inserted_latest += 1;
            }
            applied = applied.saturating_add(1);
        }
        if inserted_latest != 0 {
            self.on_visible_insertions(shard_index, inserted_latest);
        }
        Ok(applied)
    }

    /// Apply rows only if each key still matches the expected latest version.
    ///
    /// This operation is all-or-nothing for the shard request:
    /// - if any key conflicts, no writes are applied and `conflicts > 0`.
    /// - otherwise all in-range entries are committed in one batch.
    fn apply_latest_entries_conditional(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: &[RangeConditionalLatestEntry],
    ) -> anyhow::Result<RangeConditionalApplyResult> {
        let _guard = self
            .apply_lock
            .lock()
            .map_err(|_| anyhow!("range apply lock poisoned"))?;

        let latest_name = self.latest_partition_name(shard_index);
        let latest = self
            .keyspace
            .open_partition(&latest_name, PartitionCreateOptions::default())?;
        let versions_name = self.versions_partition_name(shard_index);
        let versions = self
            .keyspace
            .open_partition(&versions_name, PartitionCreateOptions::default())?;

        let in_range = entries
            .iter()
            .filter(|entry| {
                let in_start = start_key.is_empty() || entry.key.as_slice() >= start_key;
                let in_end = end_key.is_empty() || entry.key.as_slice() < end_key;
                in_start && in_end
            })
            .collect::<Vec<_>>();

        if in_range.is_empty() {
            return Ok(RangeConditionalApplyResult::default());
        }

        let mut conflicts = 0u64;
        let mut inserted_latest = 0i64;
        for entry in &in_range {
            let Some(cur_bytes) = latest.get(entry.key.as_slice())? else {
                // Allow insert-on-missing when the caller expects the zero version.
                if entry.expected_version != kv::Version::zero() {
                    conflicts = conflicts.saturating_add(1);
                }
                inserted_latest += 1;
                continue;
            };
            let (cur_version, cur_value) = kv::decode_latest_value(&cur_bytes)?;
            if entry.expected_version == kv::Version::zero()
                && latest_value_is_sql_tombstone(cur_value.as_slice())
            {
                continue;
            }
            if cur_version != entry.expected_version {
                conflicts = conflicts.saturating_add(1);
            }
        }

        if conflicts > 0 {
            return Ok(RangeConditionalApplyResult {
                applied: 0,
                conflicts,
            });
        }

        let mut batch = self.keyspace.batch();
        for entry in &in_range {
            let version_key = kv::encode_version_key(entry.key.as_slice(), entry.version);
            batch.insert(
                &versions,
                version_key,
                kv::encode_version_value(true, entry.value.as_slice()),
            );
            batch.insert(
                &latest,
                entry.key.clone(),
                kv::encode_latest_value(entry.version, entry.value.as_slice()),
            );
        }

        batch.commit()?;
        if inserted_latest != 0 {
            self.on_visible_insertions(shard_index, inserted_latest);
        }
        Ok(RangeConditionalApplyResult {
            applied: in_range.len() as u64,
            conflicts: 0,
        })
    }
}

#[derive(Clone, Debug, Default)]
struct RecoveryCheckpointSnapshot {
    success_count: u64,
    failure_count: u64,
    manual_trigger_count: u64,
    manual_trigger_failure_count: u64,
    pressure_skip_count: u64,
    manifest_parse_error_count: u64,
    last_attempt_ms: u64,
    last_success_ms: u64,
    max_lag_entries: u64,
    blocked_groups: u64,
    paused: bool,
    last_run_reason: String,
    last_free_bytes: u64,
    last_free_pct: f64,
    last_error: String,
}

#[derive(Default)]
struct RecoveryCheckpointStats {
    success_count: AtomicU64,
    failure_count: AtomicU64,
    manual_trigger_count: AtomicU64,
    manual_trigger_failure_count: AtomicU64,
    pressure_skip_count: AtomicU64,
    manifest_parse_error_count: AtomicU64,
    last_attempt_ms: AtomicU64,
    last_success_ms: AtomicU64,
    max_lag_entries: AtomicU64,
    blocked_groups: AtomicU64,
    paused: AtomicBool,
    last_free_bytes: AtomicU64,
    last_free_pct_x100: AtomicU64,
    last_run_reason: std::sync::Mutex<String>,
    last_error: std::sync::Mutex<String>,
}

impl RecoveryCheckpointStats {
    fn record_success(
        &self,
        now_ms: u64,
        max_lag_entries: u64,
        blocked_groups: u64,
        run_reason: &str,
    ) {
        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.last_attempt_ms.store(now_ms, Ordering::Relaxed);
        self.last_success_ms.store(now_ms, Ordering::Relaxed);
        self.max_lag_entries
            .store(max_lag_entries, Ordering::Relaxed);
        self.blocked_groups.store(blocked_groups, Ordering::Relaxed);
        if let Ok(mut reason) = self.last_run_reason.lock() {
            *reason = run_reason.to_string();
        }
        if let Ok(mut err) = self.last_error.lock() {
            err.clear();
        }
    }

    fn record_failure(&self, now_ms: u64, err: &str, run_reason: &str) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        self.last_attempt_ms.store(now_ms, Ordering::Relaxed);
        if let Ok(mut reason) = self.last_run_reason.lock() {
            *reason = run_reason.to_string();
        }
        if let Ok(mut last) = self.last_error.lock() {
            *last = err.to_string();
        }
    }

    fn record_manual_trigger(&self, ok: bool) {
        self.manual_trigger_count.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.manual_trigger_failure_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_pressure_skip(&self) {
        self.pressure_skip_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_manifest_parse_error(&self) {
        self.manifest_parse_error_count
            .fetch_add(1, Ordering::Relaxed);
    }

    fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
    }

    fn set_last_disk_sample(&self, free_bytes: u64, free_pct: f64) {
        self.last_free_bytes.store(free_bytes, Ordering::Relaxed);
        let pct_x100 = if free_pct.is_finite() && free_pct > 0.0 {
            (free_pct * 100.0).round() as u64
        } else {
            0
        };
        self.last_free_pct_x100.store(pct_x100, Ordering::Relaxed);
    }

    fn snapshot(&self) -> RecoveryCheckpointSnapshot {
        let last_error = self
            .last_error
            .lock()
            .map(|v| v.clone())
            .unwrap_or_else(|_| "lock-poisoned".to_string());
        let last_run_reason = self
            .last_run_reason
            .lock()
            .map(|v| v.clone())
            .unwrap_or_else(|_| "lock-poisoned".to_string());
        RecoveryCheckpointSnapshot {
            success_count: self.success_count.load(Ordering::Relaxed),
            failure_count: self.failure_count.load(Ordering::Relaxed),
            manual_trigger_count: self.manual_trigger_count.load(Ordering::Relaxed),
            manual_trigger_failure_count: self.manual_trigger_failure_count.load(Ordering::Relaxed),
            pressure_skip_count: self.pressure_skip_count.load(Ordering::Relaxed),
            manifest_parse_error_count: self.manifest_parse_error_count.load(Ordering::Relaxed),
            last_attempt_ms: self.last_attempt_ms.load(Ordering::Relaxed),
            last_success_ms: self.last_success_ms.load(Ordering::Relaxed),
            max_lag_entries: self.max_lag_entries.load(Ordering::Relaxed),
            blocked_groups: self.blocked_groups.load(Ordering::Relaxed),
            paused: self.paused.load(Ordering::Relaxed),
            last_run_reason,
            last_free_bytes: self.last_free_bytes.load(Ordering::Relaxed),
            last_free_pct: self.last_free_pct_x100.load(Ordering::Relaxed) as f64 / 100.0,
            last_error,
        }
    }
}

#[derive(Clone)]
struct RecoveryCheckpointConfig {
    persist_mode: PersistMode,
    warn_lag_entries: u64,
    min_free_bytes: u64,
    min_free_pct: f64,
}

#[derive(Clone)]
struct RecoveryCheckpointController {
    tx: mpsc::UnboundedSender<RecoveryCheckpointControl>,
}

enum RecoveryCheckpointControl {
    SetPaused {
        paused: bool,
        done: oneshot::Sender<()>,
    },
    Trigger {
        done: oneshot::Sender<anyhow::Result<()>>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RecoveryCheckpointManifest {
    version: u8,
    updated_unix_ms: u64,
    persist_mode: String,
    wal_count: usize,
    success_count: u64,
    failure_count: u64,
    blocked_groups: u64,
    max_lag_entries: u64,
    durable_floor_by_group: BTreeMap<u64, u64>,
    executed_floor_by_group: BTreeMap<u64, u64>,
    compacted_floor_by_group: BTreeMap<u64, u64>,
    last_error: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RecoveryCheckpointManifestEnvelope {
    version: u8,
    checksum_crc32: u32,
    payload: RecoveryCheckpointManifest,
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

/// Persistence mode for periodic recovery checkpoints.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum RecoveryCheckpointPersistMode {
    #[value(alias = "sync_data")]
    SyncData,
    #[value(alias = "sync_all")]
    SyncAll,
}

impl RecoveryCheckpointPersistMode {
    fn to_fjall(self) -> PersistMode {
        match self {
            RecoveryCheckpointPersistMode::SyncData => PersistMode::SyncData,
            RecoveryCheckpointPersistMode::SyncAll => PersistMode::SyncAll,
        }
    }
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

/// Aggregated control-plane proposal latency stats.
#[derive(Default)]
struct MetaProposalStats {
    total: OpStats,
    by_index: std::sync::RwLock<BTreeMap<usize, OpStats>>,
}

/// Snapshot of control-plane proposal stats.
#[derive(Default, Debug, Clone)]
struct MetaProposalStatsSnapshot {
    total: OpStatsSnapshot,
    by_index: BTreeMap<usize, OpStatsSnapshot>,
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

#[derive(Debug, Default)]
struct SplitHealthStats {
    attempts: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    backoff_active: AtomicU64,
    last_attempt_ms: AtomicU64,
    last_success_ms: AtomicU64,
    last_failure_ms: AtomicU64,
    last_failure_reason: std::sync::RwLock<String>,
    attempt_reasons: std::sync::RwLock<BTreeMap<String, u64>>,
    failure_reasons: std::sync::RwLock<BTreeMap<String, u64>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub(crate) struct SplitHealthSnapshot {
    attempts: u64,
    successes: u64,
    failures: u64,
    backoff_active: u64,
    last_attempt_ms: u64,
    last_success_ms: u64,
    last_failure_ms: u64,
    last_failure_reason: String,
    attempt_reasons: BTreeMap<String, u64>,
    failure_reasons: BTreeMap<String, u64>,
}

impl SplitHealthStats {
    fn record_attempt(&self, reason: &str) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        self.last_attempt_ms
            .store(unix_time_ms(), Ordering::Relaxed);
        let mut reasons = self.attempt_reasons.write().unwrap();
        reasons
            .entry(reason.to_string())
            .and_modify(|v| *v = v.saturating_add(1))
            .or_insert(1);
    }

    fn record_success(&self) {
        self.successes.fetch_add(1, Ordering::Relaxed);
        self.last_success_ms
            .store(unix_time_ms(), Ordering::Relaxed);
    }

    fn record_failure(&self, reason: &str, detail: &str) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        self.last_failure_ms
            .store(unix_time_ms(), Ordering::Relaxed);
        let mut reasons = self.failure_reasons.write().unwrap();
        reasons
            .entry(format!("failure:{reason}"))
            .and_modify(|v| *v = v.saturating_add(1))
            .or_insert(1);
        drop(reasons);

        let detail_trimmed = detail.trim();
        let mut last = self.last_failure_reason.write().unwrap();
        if detail_trimmed.is_empty() {
            *last = reason.to_string();
        } else {
            *last = format!("{reason}: {}", detail_trimmed);
        }
    }

    fn set_backoff_active(&self, count: u64) {
        self.backoff_active.store(count, Ordering::Relaxed);
    }

    fn snapshot(&self) -> SplitHealthSnapshot {
        SplitHealthSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            successes: self.successes.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            backoff_active: self.backoff_active.load(Ordering::Relaxed),
            last_attempt_ms: self.last_attempt_ms.load(Ordering::Relaxed),
            last_success_ms: self.last_success_ms.load(Ordering::Relaxed),
            last_failure_ms: self.last_failure_ms.load(Ordering::Relaxed),
            last_failure_reason: self.last_failure_reason.read().unwrap().clone(),
            attempt_reasons: self.attempt_reasons.read().unwrap().clone(),
            failure_reasons: self.failure_reasons.read().unwrap().clone(),
        }
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

impl MetaProposalStats {
    /// Record latency for a routed control-plane proposal.
    fn record(&self, meta_index: usize, dur_us: u64, ok: bool) {
        self.total.record(dur_us, ok);
        let mut by_index = self.by_index.write().unwrap();
        let entry = by_index.entry(meta_index).or_default();
        entry.record(dur_us, ok);
    }

    /// Snapshot and reset all control-plane proposal stats.
    fn snapshot_and_reset(&self) -> MetaProposalStatsSnapshot {
        let mut by_index = self.by_index.write().unwrap();
        let mut out = BTreeMap::new();
        for (idx, stats) in by_index.iter() {
            out.insert(*idx, stats.snapshot());
        }
        by_index.clear();
        MetaProposalStatsSnapshot {
            total: self.total.snapshot(),
            by_index: out,
        }
    }

    /// Snapshot control-plane proposal stats without resetting counters.
    fn snapshot(&self) -> MetaProposalStatsSnapshot {
        let by_index = self.by_index.read().unwrap();
        let mut out = BTreeMap::new();
        for (idx, stats) in by_index.iter() {
            out.insert(*idx, stats.peek());
        }
        MetaProposalStatsSnapshot {
            total: self.total.peek(),
            by_index: out,
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

    /// Snapshot counters without resetting them.
    fn peek(&self) -> OpStatsSnapshot {
        OpStatsSnapshot {
            count: self.count.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            total_us: self.total_us.load(Ordering::Relaxed),
            max_us: self.max_us.load(Ordering::Relaxed),
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

    pub(crate) async fn wait_for_client_ops_drained(
        &self,
        timeout: Duration,
    ) -> anyhow::Result<()> {
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

    fn meta_route_for_key(&self, key: &[u8]) -> Option<(usize, accord::Handle)> {
        let idx = self.cluster_store.meta_range_index_for_key(key);
        self.meta_handles
            .get(&idx)
            .cloned()
            .map(|h| (idx, h))
            .or_else(|| self.meta_handles.get(&0).cloned().map(|h| (0, h)))
    }

    pub(crate) async fn propose_meta_command(
        &self,
        cmd: cluster::ClusterCommand,
    ) -> anyhow::Result<()> {
        let key = cluster::cluster_command_key(&cmd);
        let payload = ClusterStateMachine::encode_command(&cmd)?;
        let explicit_meta_index = self.cluster_store.meta_range_index_for_command(&cmd);
        let (meta_index, handle) = if let Some(idx) = explicit_meta_index {
            self.meta_handles
                .get(&idx)
                .cloned()
                .map(|h| (idx, h))
                .or_else(|| self.meta_handles.get(&0).cloned().map(|h| (0, h)))
                .ok_or_else(|| anyhow!("meta handle not available for index {idx}"))?
        } else {
            self.meta_route_for_key(&key)
                .ok_or_else(|| anyhow!("meta handle not available for routed key"))?
        };
        let start = Instant::now();
        let res = handle.propose(payload).await;
        let dur_us = start.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.meta_proposal_stats
            .record(meta_index, dur_us, res.is_ok());
        let _ = res?;
        Ok(())
    }

    fn controller_fence_for_domain(&self, domain: ControllerDomain) -> Option<ControllerFence> {
        let now = unix_time_ms();
        self.cluster_store
            .controller_lease(domain)
            .and_then(|lease| {
                (lease.holder == self.node_id && lease.lease_until_ms > now).then_some(
                    ControllerFence {
                        domain,
                        holder: lease.holder,
                        term: lease.term,
                    },
                )
            })
    }

    pub(crate) async fn propose_meta_command_guarded(
        &self,
        domain: ControllerDomain,
        cmd: cluster::ClusterCommand,
    ) -> anyhow::Result<()> {
        let fence = self
            .controller_fence_for_domain(domain)
            .ok_or_else(|| anyhow!("not current controller leader for {domain:?}"))?;
        self.propose_meta_command(cluster::ClusterCommand::Guarded {
            fence,
            command: Box::new(cmd),
        })
        .await
    }

    pub(crate) fn client_inflight_ops(&self) -> u64 {
        self.client_inflight_ops.load(Ordering::Relaxed)
    }

    pub(crate) fn record_split_attempt(&self, reason: &str) {
        self.split_health.record_attempt(reason);
    }

    pub(crate) fn record_split_success(&self) {
        self.split_health.record_success();
    }

    pub(crate) fn record_split_failure(&self, reason: &str, detail: &str) {
        self.split_health.record_failure(reason, detail);
    }

    pub(crate) fn set_split_backoff_active(&self, count: u64) {
        self.split_health.set_backoff_active(count);
    }

    pub(crate) fn split_health_snapshot(&self) -> SplitHealthSnapshot {
        self.split_health.snapshot()
    }

    fn meta_proposal_stats_snapshot(&self) -> MetaProposalStatsSnapshot {
        self.meta_proposal_stats.snapshot_and_reset()
    }

    pub(crate) fn meta_proposal_stats_peek(&self) -> MetaProposalStatsSnapshot {
        self.meta_proposal_stats.snapshot()
    }

    pub(crate) fn recovery_checkpoint_snapshot(&self) -> RecoveryCheckpointSnapshot {
        self.recovery_checkpoints.snapshot()
    }

    pub(crate) async fn recovery_checkpoint_set_paused(&self, paused: bool) -> anyhow::Result<()> {
        let controller = self
            .recovery_checkpoint_controller
            .as_ref()
            .ok_or_else(|| anyhow!("recovery checkpoint manager is disabled"))?;
        let (tx, rx) = oneshot::channel();
        controller
            .tx
            .send(RecoveryCheckpointControl::SetPaused { paused, done: tx })
            .map_err(|_| anyhow!("recovery checkpoint manager is unavailable"))?;
        rx.await
            .map_err(|_| anyhow!("recovery checkpoint manager did not acknowledge pause update"))?;
        Ok(())
    }

    pub(crate) async fn recovery_checkpoint_trigger(&self) -> anyhow::Result<()> {
        let controller = self
            .recovery_checkpoint_controller
            .as_ref()
            .ok_or_else(|| anyhow!("recovery checkpoint manager is disabled"))?;
        let (tx, rx) = oneshot::channel();
        controller
            .tx
            .send(RecoveryCheckpointControl::Trigger { done: tx })
            .map_err(|_| anyhow!("recovery checkpoint manager is unavailable"))?;
        rx.await
            .map_err(|_| anyhow!("recovery checkpoint manager trigger did not complete"))?
    }

    pub(crate) fn meta_move_stuck_warn_ms(&self) -> u64 {
        self.meta_move_stuck_warn
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }

    /// Compute per-meta-index executed-prefix lag across each range's replicas.
    ///
    /// Lag is the summed spread (`max-min`) per source node counter.
    pub(crate) async fn meta_group_lag_by_index(&self) -> BTreeMap<usize, u64> {
        let snapshot = self.cluster_store.state();
        let mut lag_by_index = BTreeMap::<usize, u64>::new();
        for range in snapshot.meta_ranges {
            let gid = meta_group_id_for_index(range.meta_index);
            let mut by_source = BTreeMap::<NodeId, (u64, u64)>::new();
            let mut complete = true;
            for node in range.replicas {
                let prefixes = match self.transport.last_executed_prefix(node, gid).await {
                    Ok(p) => p,
                    Err(_) => {
                        complete = false;
                        break;
                    }
                };
                for p in prefixes {
                    let entry = by_source.entry(p.node_id).or_insert((p.counter, p.counter));
                    entry.0 = entry.0.min(p.counter);
                    entry.1 = entry.1.max(p.counter);
                }
            }
            let range_lag = if complete {
                by_source
                    .into_iter()
                    .fold(0u64, |acc, (_, (min_c, max_c))| {
                        acc.saturating_add(max_c.saturating_sub(min_c))
                    })
            } else {
                0
            };
            let entry = lag_by_index.entry(range.meta_index).or_insert(0);
            *entry = entry.saturating_add(range_lag);
        }
        lag_by_index
    }

    pub(crate) async fn ensure_controller_leader(&self, domain: ControllerDomain) -> bool {
        let now = unix_time_ms();
        let renew_margin = self
            .controller_lease_renew_margin
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let snapshot = self.cluster_store.state();

        // Some control loops require the holder to be an eligible participant
        // in the in-flight workflow (for example, not the learner currently
        // being staged in a shard move). Refuse leadership when ineligible.
        if !Self::controller_lease_eligible_for_domain(&snapshot, domain, self.node_id) {
            return false;
        }
        if self.cluster_store.is_controller_leader(
            domain,
            self.node_id,
            now.saturating_add(renew_margin),
        ) {
            return true;
        }

        let current = self.cluster_store.controller_lease(domain);
        let current_holder_active = current
            .as_ref()
            .map(|lease| {
                snapshot
                    .members
                    .get(&lease.holder)
                    .map(|m| m.state == MemberState::Active)
                    .unwrap_or(false)
            })
            .unwrap_or(false);
        let current_holder_eligible = current
            .as_ref()
            .map(|lease| {
                Self::controller_lease_eligible_for_domain(&snapshot, domain, lease.holder)
            })
            .unwrap_or(false);

        // Respect a healthy incumbent while its lease is still valid.
        // This avoids unnecessary term churn across competing managers.
        if let Some(cur) = current.as_ref() {
            if cur.holder != self.node_id
                && cur.lease_until_ms > now.saturating_add(renew_margin)
                && current_holder_active
                && current_holder_eligible
            {
                return false;
            }
        }

        let term = current
            .as_ref()
            .map(|lease| {
                if lease.holder == self.node_id {
                    lease.term
                } else if lease.lease_until_ms > now && current_holder_active {
                    lease.term
                } else {
                    lease.term.saturating_add(1)
                }
            })
            .unwrap_or(1);
        let ttl_ms = self
            .controller_lease_ttl
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let lease_until_ms = now.saturating_add(ttl_ms.max(1));

        if self
            .propose_meta_command(cluster::ClusterCommand::AcquireControllerLease {
                domain,
                node_id: self.node_id,
                term,
                lease_until_ms,
            })
            .await
            .is_err()
        {
            return false;
        }
        self.cluster_store
            .is_controller_leader(domain, self.node_id, unix_time_ms())
    }

    fn controller_lease_eligible_for_domain(
        snapshot: &ClusterState,
        domain: ControllerDomain,
        node_id: NodeId,
    ) -> bool {
        match domain {
            ControllerDomain::Rebalance => {
                if let Some((&shard_id, _)) = snapshot.shard_rebalances.iter().next() {
                    let Some(shard) = snapshot.shards.iter().find(|s| s.shard_id == shard_id)
                    else {
                        return false;
                    };
                    if !shard.replicas.contains(&node_id) {
                        return false;
                    }
                    let role = snapshot
                        .shard_replica_roles
                        .get(&shard_id)
                        .and_then(|roles| roles.get(&node_id))
                        .copied()
                        .unwrap_or(ReplicaRole::Voter);
                    return role != ReplicaRole::Learner;
                }

                if let Some((&meta_range_id, _)) = snapshot.meta_rebalances.iter().next() {
                    let Some(range) = snapshot
                        .meta_ranges
                        .iter()
                        .find(|r| r.meta_range_id == meta_range_id)
                    else {
                        return false;
                    };
                    if !range.replicas.contains(&node_id) {
                        return false;
                    }
                    let role = snapshot
                        .meta_replica_roles
                        .get(&meta_range_id)
                        .and_then(|roles| roles.get(&node_id))
                        .copied()
                        .unwrap_or(ReplicaRole::Voter);
                    return role != ReplicaRole::Learner;
                }
                true
            }
            _ => true,
        }
    }

    /// Apply control-plane replica membership to runtime Accord data groups.
    fn refresh_group_memberships(&self) -> anyhow::Result<()> {
        let snapshot = self.cluster_store.state();

        // Meta/control groups track metadata-range replica sets.
        let mut default_members = snapshot
            .members
            .values()
            .filter(|m| m.state != MemberState::Removed)
            .map(|m| accord::Member { id: m.node_id })
            .collect::<Vec<_>>();
        default_members.sort_by_key(|m| m.id);
        default_members.dedup_by_key(|m| m.id);
        for meta_range in &snapshot.meta_ranges {
            let gid = meta_group_id_for_index(meta_range.meta_index);
            let Some(meta) = self.group(gid) else {
                continue;
            };
            let (members, voters) = meta_membership_sets(&snapshot, meta_range);
            let members = members
                .into_iter()
                .map(|id| accord::Member { id })
                .collect::<Vec<_>>();
            if !members.is_empty() && !voters.is_empty() {
                meta.update_membership(members, voters)?;
            }
        }

        // Fallback for very early bootstrap only: if no descriptor owns meta
        // index 0 yet, keep group 0 aligned with currently known members.
        let has_meta_index_zero = snapshot.meta_ranges.iter().any(|r| r.meta_index == 0);
        if !has_meta_index_zero {
            if let Some(meta) = self.group(GROUP_MEMBERSHIP) {
                if !default_members.is_empty() {
                    meta.update_members(default_members)?;
                }
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

        let mut current_members = group
            .members()
            .into_iter()
            .map(|m| m.id)
            .collect::<Vec<_>>();
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
    pub(crate) fn local_range_stats(&self) -> anyhow::Result<Vec<LocalRangeStat>> {
        let shards = self.cluster_store.shards_snapshot();
        let mut out = Vec::new();
        for shard in shards {
            if !shard.replicas.contains(&self.node_id) {
                continue;
            }
            let record_count = self.range_stats.record_count(shard.shard_index);
            out.push(LocalRangeStat {
                shard_id: shard.shard_id,
                shard_index: shard.shard_index,
                record_count,
                is_leaseholder: shard.leaseholder == self.node_id,
            });
        }
        Ok(out)
    }

    /// Local range record counts keyed by shard id.
    pub(crate) fn local_range_record_counts(
        &self,
    ) -> anyhow::Result<std::collections::HashMap<u64, u64>> {
        let mut out = std::collections::HashMap::new();
        for stat in self.local_range_stats()? {
            out.insert(stat.shard_id, stat.record_count);
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
        reverse: bool,
    ) -> anyhow::Result<(Vec<RangeSnapshotLatestEntry>, Vec<u8>, bool)> {
        self.range_stats.snapshot_latest_page(
            shard_index,
            start_key,
            end_key,
            cursor,
            limit,
            reverse,
        )
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

    /// Apply one page of latest rows conditionally into a local shard range.
    fn apply_range_latest_page_conditional(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: &[RangeConditionalLatestEntry],
    ) -> anyhow::Result<RangeConditionalApplyResult> {
        self.range_stats
            .apply_latest_entries_conditional(shard_index, start_key, end_key, entries)
    }

    fn ensure_keys_route_to_shard(
        &self,
        shard_index: usize,
        keys: &[Vec<u8>],
    ) -> anyhow::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let _guard = self.split_lock.read().unwrap();
        let range_snapshot = if matches!(self.routing_mode, RoutingMode::Range) {
            Some(self.cluster_store.shards_snapshot())
        } else {
            None
        };
        let ranges = range_snapshot.as_deref();

        for key in keys {
            let actual = self.shard_for_key_with_snapshot(key, ranges);
            anyhow::ensure!(
                actual == shard_index,
                "key routed to shard {actual}, but request targeted shard {shard_index}"
            );
        }
        Ok(())
    }

    /// Returns the per-shard SQL write lock used to serialize writes within one shard only.
    fn sql_write_lock_for_shard(&self, shard_index: usize) -> &tokio::sync::Mutex<()> {
        self.sql_write_locks
            .get(shard_index)
            .unwrap_or_else(|| &self.sql_write_locks[0])
    }

    async fn write_range_latest_replicated(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: &[RangeWriteEntry],
    ) -> anyhow::Result<u64> {
        let in_range = entries
            .iter()
            .filter(|entry| {
                let in_start = start_key.is_empty() || entry.key.as_slice() >= start_key;
                let in_end = end_key.is_empty() || entry.key.as_slice() < end_key;
                in_start && in_end
            })
            .cloned()
            .collect::<Vec<_>>();

        if in_range.is_empty() {
            return Ok(0);
        }

        let _write_guard = self.sql_write_lock_for_shard(shard_index).lock().await;

        let keys = in_range
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>();
        self.ensure_keys_route_to_shard(shard_index, &keys)?;

        let items = in_range
            .iter()
            .map(|entry| (entry.key.clone(), entry.value.clone()))
            .collect::<Vec<_>>();
        self.execute_range_write_batch_set_direct(items).await?;
        let committed = in_range
            .iter()
            .map(|entry| (entry.key.clone(), entry.value.clone()))
            .collect::<Vec<_>>();
        let _ = self.wait_for_latest_values(&committed).await?;
        Ok(in_range.len() as u64)
    }

    async fn write_range_latest_conditional_replicated(
        &self,
        shard_index: usize,
        start_key: &[u8],
        end_key: &[u8],
        entries: &[RangeConditionalWriteEntry],
    ) -> anyhow::Result<RangeConditionalWriteResult> {
        let in_range = entries
            .iter()
            .filter(|entry| {
                let in_start = start_key.is_empty() || entry.key.as_slice() >= start_key;
                let in_end = end_key.is_empty() || entry.key.as_slice() < end_key;
                in_start && in_end
            })
            .cloned()
            .collect::<Vec<_>>();

        if in_range.is_empty() {
            return Ok(RangeConditionalWriteResult::default());
        }

        let _write_guard = self.sql_write_lock_for_shard(shard_index).lock().await;

        let keys = in_range
            .iter()
            .map(|entry| entry.key.clone())
            .collect::<Vec<_>>();
        self.ensure_keys_route_to_shard(shard_index, &keys)?;

        let latest = self.kv_latest_batch(&keys);
        let mut conflicts = 0u64;
        for (entry, latest_item) in in_range.iter().zip(latest.into_iter()) {
            match latest_item {
                Some((value, version)) => {
                    if entry.expected_version == kv::Version::zero()
                        && latest_value_is_sql_tombstone(value.as_slice())
                    {
                        continue;
                    }
                    if version != entry.expected_version {
                        conflicts = conflicts.saturating_add(1);
                    }
                }
                None => {
                    if entry.expected_version != kv::Version::zero() {
                        conflicts = conflicts.saturating_add(1);
                    }
                }
            }
        }

        if conflicts > 0 {
            return Ok(RangeConditionalWriteResult {
                applied: 0,
                conflicts,
                applied_versions: Vec::new(),
            });
        }

        let items = in_range
            .iter()
            .map(|entry| (entry.key.clone(), entry.value.clone()))
            .collect::<Vec<_>>();
        self.execute_range_write_batch_set_direct(items).await?;
        let committed = in_range
            .iter()
            .map(|entry| (entry.key.clone(), entry.value.clone()))
            .collect::<Vec<_>>();
        let latest_versions = self.wait_for_latest_values(&committed).await?;
        let mut applied_versions = Vec::with_capacity(in_range.len());
        for (entry, version) in in_range.iter().zip(latest_versions.into_iter()) {
            applied_versions.push(RangeConditionalAppliedVersion {
                key: entry.key.clone(),
                version,
            });
        }

        Ok(RangeConditionalWriteResult {
            applied: in_range.len() as u64,
            conflicts: 0,
            applied_versions,
        })
    }

    async fn wait_for_latest_values(
        &self,
        items: &[(Vec<u8>, Vec<u8>)],
    ) -> anyhow::Result<Vec<kv::Version>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        // Track unresolved keys across polls so each retry only re-reads keys
        // that are not yet visible with their expected values.
        let mut pending_indices = (0..items.len()).collect::<Vec<_>>();
        let mut pending_keys = items.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
        let mut versions = vec![None; items.len()];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let latest = self.kv_latest_batch(&pending_keys);
            let mut next_pending_indices = Vec::with_capacity(pending_indices.len());
            let mut next_pending_keys = Vec::with_capacity(pending_keys.len());
            for ((idx, key), latest_item) in pending_indices
                .into_iter()
                .zip(pending_keys.into_iter())
                .zip(latest.into_iter())
            {
                let expected_value = &items[idx].1;
                match latest_item {
                    Some((value, version)) if value == *expected_value => {
                        versions[idx] = Some(version);
                    }
                    _ => {
                        next_pending_indices.push(idx);
                        next_pending_keys.push(key);
                    }
                }
            }
            pending_indices = next_pending_indices;
            pending_keys = next_pending_keys;

            if pending_indices.is_empty() {
                let mut out = Vec::with_capacity(versions.len());
                for version in versions {
                    out.push(version.context("missing latest version after visibility wait")?);
                }
                return Ok(out);
            }

            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("timed out waiting for replicated write visibility");
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
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

    /// Block while any key in the request maps to a currently fenced shard.
    async fn wait_until_keys_available(&self, keys: &[Vec<u8>]) {
        if keys.is_empty() {
            self.wait_until_unfrozen().await;
            return;
        }
        loop {
            if self.cluster_store.frozen() {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            let snapshot = self.cluster_store.state();
            if snapshot.shard_fences.is_empty() {
                return;
            }
            let blocked = keys.iter().find_map(|key| {
                let shard_id = match self.routing_mode {
                    RoutingMode::Hash => {
                        let idx = (kv::hash_key(key) as usize) % self.data_shards.max(1);
                        snapshot
                            .shards
                            .iter()
                            .find(|s| s.shard_index == idx)
                            .map(|s| s.shard_id)
                            .unwrap_or(0)
                    }
                    RoutingMode::Range => shard_id_for_key_in_ranges(key, &snapshot.shards),
                };
                snapshot
                    .shard_fences
                    .get(&shard_id)
                    .map(|reason| (shard_id, reason.clone()))
            });
            if blocked.is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Enqueue a batch SET request to the client batcher.
    async fn execute_batch_set(&self, items: Vec<(Vec<u8>, Vec<u8>)>) -> anyhow::Result<()> {
        if items.is_empty() {
            // Nothing to do.
            return Ok(());
        }
        let keys = items.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
        self.wait_until_keys_available(&keys).await;
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
        self.execute_batch_set_direct_with_limits(
            items,
            self.client_set_batch_target.max(1),
            usize::MAX,
            self.client_set_proposal_pipeline_depth.max(1),
        )
        .await
    }

    /// Execute a range-write batch directly with range-write-specific batching limits.
    async fn execute_range_write_batch_set_direct(
        &self,
        items: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> anyhow::Result<()> {
        self.execute_batch_set_direct_with_limits(
            items,
            self.range_write_batch_target.max(1),
            self.range_write_batch_max_bytes.max(1),
            self.range_write_proposal_pipeline_depth.max(1),
        )
        .await
    }

    /// Applies one shard's chunked SET workload with bounded in-flight proposals.
    ///
    /// Input:
    /// - `handle`: shard/group handle to propose on.
    /// - `chunks`: pre-chunked key/value batches in submit order.
    /// - `pipeline_depth`: max number of concurrent proposals for this shard.
    ///
    /// Output:
    /// - `Ok(())` when all chunks are committed.
    /// - First proposal error encountered, preserving fail-fast semantics.
    ///
    /// Design: keep multiple proposals in flight to overlap network/consensus
    /// latency, while preserving bounded memory and natural backpressure.
    async fn execute_batch_set_chunks_on_shard(
        &self,
        handle: accord::Handle,
        chunks: Vec<Vec<(Vec<u8>, Vec<u8>)>>,
        pipeline_depth: usize,
    ) -> anyhow::Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }
        let state = self.clone();
        let inflight_limit = pipeline_depth.max(1).min(chunks.len().max(1));
        let mut pending = chunks.into_iter().enumerate().collect::<VecDeque<_>>();
        let mut inflight =
            FuturesUnordered::<BoxFuture<'static, anyhow::Result<accord::ProposalResult>>>::new();

        // Prime pipeline up to the configured in-flight bound.
        while inflight.len() < inflight_limit {
            let Some((_chunk_idx, chunk)) = pending.pop_front() else {
                break;
            };
            let handle = handle.clone();
            let state = state.clone();
            inflight.push(Box::pin(async move {
                let cmd = kv::encode_batch_set(chunk.as_slice());
                state
                    .propose_timed_on(handle, ProposalKind::BatchSet, cmd)
                    .await
            }));
        }

        // As each proposal completes, refill pipeline until all chunks are drained.
        while let Some(res) = inflight.next().await {
            res?;
            while inflight.len() < inflight_limit {
                let Some((_chunk_idx, chunk)) = pending.pop_front() else {
                    break;
                };
                let handle = handle.clone();
                let state = state.clone();
                inflight.push(Box::pin(async move {
                    let cmd = kv::encode_batch_set(chunk.as_slice());
                    state
                        .propose_timed_on(handle, ProposalKind::BatchSet, cmd)
                        .await
                }));
            }
        }

        Ok(())
    }

    /// Directly routes and proposes a multi-key SET workload with explicit limits.
    ///
    /// Input:
    /// - `items`: key/value writes to apply.
    /// - `max_ops_per_proposal`: entry cap per proposal chunk.
    /// - `max_bytes_per_proposal`: byte cap per proposal chunk.
    /// - `proposal_pipeline_depth`: in-flight proposal bound per shard.
    ///
    /// Output:
    /// - `Ok(())` after all shard workloads are committed.
    /// - Error if any shard proposal fails.
    ///
    /// Design: route once, chunk once, then pipeline per shard. This keeps small
    /// writes low-latency while letting large writes amortize proposal overhead.
    async fn execute_batch_set_direct_with_limits(
        &self,
        items: Vec<(Vec<u8>, Vec<u8>)>,
        max_ops_per_proposal: usize,
        max_bytes_per_proposal: usize,
        proposal_pipeline_depth: usize,
    ) -> anyhow::Result<()> {
        if items.is_empty() {
            // Nothing to do.
            return Ok(());
        }
        let keys = items.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
        self.wait_until_keys_available(&keys).await;
        let _inflight_guard = self.enter_client_op();
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

        let target = max_ops_per_proposal.max(1);
        let max_bytes = max_bytes_per_proposal.max(1);
        let pipeline_depth = proposal_pipeline_depth.max(1);
        let mut shard_futs = FuturesUnordered::new();
        for (shard, batch) in by_shard.into_iter().enumerate() {
            if batch.is_empty() {
                // Skip shards with no work.
                continue;
            }
            // Track client-visible write load per shard (best-effort).
            self.shard_load.record_set_ops(shard, batch.len() as u64);
            let handle = self.handle_for_shard(shard);
            // Chunk by entry/byte budgets once, then let per-shard pipelining
            // overlap proposal round-trips behind a bounded concurrency gate.
            let chunks = chunk_batch_set_items_by_limits(batch, target, max_bytes);
            let state = self.clone();
            shard_futs.push(async move {
                state
                    .execute_batch_set_chunks_on_shard(handle, chunks, pipeline_depth)
                    .await
            });
        }

        while let Some(res) = shard_futs.next().await {
            res?;
        }
        Ok(())
    }

    /// Enqueue a batch GET request to the client batcher.
    async fn execute_batch_get(&self, keys: Vec<Vec<u8>>) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            // Nothing to do.
            return Ok(Vec::new());
        }
        self.wait_until_keys_available(&keys).await;

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
        if keys.is_empty() {
            // Nothing to do.
            return Ok(Vec::new());
        }
        self.wait_until_keys_available(&keys).await;
        let _inflight_guard = self.enter_client_op();
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
            redis_server::KvOp::HoloMetrics => {
                anyhow::bail!("HOLOMETRICS is handled at the Redis layer")
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

/// Resolve a key to a shard id using an in-memory range snapshot.
fn shard_id_for_key_in_ranges(key: &[u8], shards: &[ShardDesc]) -> u64 {
    if shards.is_empty() {
        return 0;
    }
    if shards.len() == 1 {
        return shards[0].shard_id;
    }
    let has_key_ranges = shards
        .iter()
        .any(|s| !s.start_key.is_empty() || !s.end_key.is_empty());
    if has_key_ranges {
        for shard in shards {
            let in_start = shard.start_key.is_empty() || key >= shard.start_key.as_slice();
            let in_end = shard.end_key.is_empty() || key < shard.end_key.as_slice();
            if in_start && in_end {
                return shard.shard_id;
            }
        }
        return shards[0].shard_id;
    }
    let hash = kv::hash_key(key);
    for shard in shards {
        if hash >= shard.start_hash && hash <= shard.end_hash {
            return shard.shard_id;
        }
    }
    shards[0].shard_id
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
                roles.get(id).copied().unwrap_or(ReplicaRole::Voter) != ReplicaRole::Learner
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

/// Resolve voting members for a metadata range from staged replica-role metadata.
fn meta_membership_sets(
    snapshot: &ClusterState,
    range: &cluster::MetaRangeDesc,
) -> (Vec<NodeId>, Vec<NodeId>) {
    let mut members = range.replicas.clone();
    members.sort_unstable();
    members.dedup();

    let mut voters = if let Some(roles) = snapshot.meta_replica_roles.get(&range.meta_range_id) {
        range
            .replicas
            .iter()
            .copied()
            .filter(|id| {
                roles.get(id).copied().unwrap_or(ReplicaRole::Voter) != ReplicaRole::Learner
            })
            .collect::<Vec<_>>()
    } else {
        range.replicas.clone()
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

fn chunk_batch_set_items_by_limits(
    batch: Vec<(Vec<u8>, Vec<u8>)>,
    max_ops_per_proposal: usize,
    max_bytes_per_proposal: usize,
) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let target = max_ops_per_proposal.max(1);
    let byte_cap = max_bytes_per_proposal.max(1);
    let mut chunks = Vec::new();
    let mut current = Vec::with_capacity(target);
    let mut current_bytes = 0usize;

    for (key, value) in batch {
        let item_bytes = key.len().saturating_add(value.len());
        let exceeds_ops = current.len() >= target;
        let exceeds_bytes =
            !current.is_empty() && current_bytes.saturating_add(item_bytes) > byte_cap;
        if exceeds_ops || exceeds_bytes {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
            current.reserve(target);
        }
        current_bytes = current_bytes.saturating_add(item_bytes);
        current.push((key, value));
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
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

/// Returns whether a latest value is a SQL row-model tombstone payload.
fn latest_value_is_sql_tombstone(value: &[u8]) -> bool {
    const ROW_FORMAT_V1: u8 = 1;
    const ROW_FORMAT_V2: u8 = 2;
    const ROW_FLAG_TOMBSTONE: u8 = 0x01;

    if value.len() < 2 {
        return false;
    }
    matches!(value[0], ROW_FORMAT_V1 | ROW_FORMAT_V2) && (value[1] & ROW_FLAG_TOMBSTONE != 0)
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

fn manifest_payload_checksum(manifest: &RecoveryCheckpointManifest) -> anyhow::Result<u32> {
    let payload = serde_json::to_vec(manifest).context("serialize recovery manifest payload")?;
    Ok(crc32fast::hash(&payload))
}

fn load_recovery_manifest(path: &Path) -> anyhow::Result<Option<RecoveryCheckpointManifest>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes =
        fs::read(path).with_context(|| format!("read recovery manifest: {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse recovery manifest json: {}", path.display()))?;

    if value.get("payload").is_some() && value.get("checksum_crc32").is_some() {
        let envelope: RecoveryCheckpointManifestEnvelope = serde_json::from_value(value)
            .with_context(|| format!("parse recovery manifest envelope: {}", path.display()))?;
        if envelope.version != 2 {
            anyhow::bail!(
                "unsupported recovery manifest envelope version {} at {}",
                envelope.version,
                path.display()
            );
        }
        let actual = manifest_payload_checksum(&envelope.payload)?;
        if actual != envelope.checksum_crc32 {
            anyhow::bail!(
                "recovery manifest checksum mismatch at {} (expected={}, actual={})",
                path.display(),
                envelope.checksum_crc32,
                actual
            );
        }
        return Ok(Some(envelope.payload));
    }

    let manifest: RecoveryCheckpointManifest = serde_json::from_value(value)
        .with_context(|| format!("parse legacy recovery manifest: {}", path.display()))?;
    Ok(Some(manifest))
}

fn write_recovery_manifest(
    path: &Path,
    manifest: &RecoveryCheckpointManifest,
) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("invalid recovery manifest path: {}", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create recovery manifest dir: {}", parent.display()))?;
    let envelope = RecoveryCheckpointManifestEnvelope {
        version: 2,
        checksum_crc32: manifest_payload_checksum(manifest)?,
        payload: manifest.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&envelope).context("serialize recovery manifest")?;
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, bytes)
        .with_context(|| format!("write recovery manifest temp: {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "replace recovery manifest {} -> {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn disk_space_for_path(path: &Path) -> anyhow::Result<(u64, f64)> {
    let resolved = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let disks = Disks::new_with_refreshed_list();
    let mut best: Option<(usize, u64, u64)> = None;
    for disk in disks.list() {
        let mount = disk.mount_point();
        if !resolved.starts_with(mount) {
            continue;
        }
        let depth = mount.components().count();
        let free = disk.available_space();
        let total = disk.total_space();
        match best {
            Some((best_depth, _, _)) if depth < best_depth => {}
            _ => best = Some((depth, free, total)),
        }
    }
    let (_, free_bytes, total_bytes) = best.ok_or_else(|| {
        anyhow!(
            "unable to resolve disk space for checkpoint path {}",
            resolved.display()
        )
    })?;
    let free_pct = if total_bytes == 0 {
        0.0
    } else {
        (free_bytes as f64 * 100.0) / total_bytes as f64
    };
    Ok((free_bytes, free_pct))
}

fn checkpoint_lag_snapshot(
    statuses: &[accord::CommitLogCheckpointStatus],
) -> (
    BTreeMap<u64, u64>,
    BTreeMap<u64, u64>,
    BTreeMap<u64, u64>,
    u64,
    u64,
) {
    let mut durable = BTreeMap::<u64, u64>::new();
    let mut executed = BTreeMap::<u64, u64>::new();
    let mut compacted = BTreeMap::<u64, u64>::new();
    for status in statuses {
        for (group, floor) in &status.durable_floor_by_group {
            durable.insert(
                *group,
                (*floor).max(durable.get(group).copied().unwrap_or(0)),
            );
        }
        for (group, floor) in &status.executed_floor_by_group {
            executed.insert(
                *group,
                (*floor).max(executed.get(group).copied().unwrap_or(0)),
            );
        }
        for (group, floor) in &status.compacted_floor_by_group {
            compacted.insert(
                *group,
                (*floor).max(compacted.get(group).copied().unwrap_or(0)),
            );
        }
    }

    let mut max_lag = 0u64;
    let mut blocked = 0u64;
    for (group, exec_floor) in &executed {
        let durable_floor = durable.get(group).copied().unwrap_or(0);
        let lag = exec_floor.saturating_sub(durable_floor);
        max_lag = max_lag.max(lag);
        if lag > 0 {
            blocked = blocked.saturating_add(1);
        }
    }
    (durable, executed, compacted, blocked, max_lag)
}

fn run_recovery_checkpoint_once(
    keyspace: &Arc<Keyspace>,
    wals: &[Arc<dyn accord::CommitLog>],
    config: RecoveryCheckpointConfig,
    disk_path: &Path,
    manifest_path: &Path,
    stats: &Arc<RecoveryCheckpointStats>,
    run_reason: &str,
) -> anyhow::Result<()> {
    let (free_bytes, free_pct) = disk_space_for_path(disk_path)?;
    stats.set_last_disk_sample(free_bytes, free_pct);
    let low_bytes = config.min_free_bytes > 0 && free_bytes < config.min_free_bytes;
    let low_pct = config.min_free_pct > 0.0 && free_pct < config.min_free_pct;
    if low_bytes || low_pct {
        stats.record_pressure_skip();
        anyhow::bail!(
            "disk pressure: free_bytes={} min_free_bytes={} free_pct={:.2} min_free_pct={:.2}",
            free_bytes,
            config.min_free_bytes,
            free_pct,
            config.min_free_pct
        );
    }

    keyspace
        .persist(config.persist_mode)
        .context("persist keyspace during recovery checkpoint")?;

    for wal in wals {
        wal.mark_durable_checkpoint()
            .context("mark durable checkpoint on wal")?;
    }

    let statuses = wals
        .iter()
        .map(|w| w.checkpoint_status())
        .collect::<Vec<_>>();
    let (durable, executed, compacted, blocked, max_lag) = checkpoint_lag_snapshot(&statuses);
    if max_lag >= config.warn_lag_entries.max(1) {
        tracing::warn!(
            max_lag_entries = max_lag,
            blocked_groups = blocked,
            warn_lag_entries = config.warn_lag_entries,
            "recovery checkpoint lag is above warning threshold"
        );
    }

    let snapshot = stats.snapshot();
    let now_ms = unix_time_ms();
    let manifest = RecoveryCheckpointManifest {
        version: 1,
        updated_unix_ms: now_ms,
        persist_mode: match config.persist_mode {
            PersistMode::Buffer => "buffer".to_string(),
            PersistMode::SyncData => "sync_data".to_string(),
            PersistMode::SyncAll => "sync_all".to_string(),
        },
        wal_count: wals.len(),
        success_count: snapshot.success_count.saturating_add(1),
        failure_count: snapshot.failure_count,
        blocked_groups: blocked,
        max_lag_entries: max_lag,
        durable_floor_by_group: durable,
        executed_floor_by_group: executed,
        compacted_floor_by_group: compacted,
        last_error: String::new(),
    };
    write_recovery_manifest(manifest_path, &manifest)?;
    stats.record_success(now_ms, max_lag, blocked, run_reason);
    Ok(())
}

fn spawn_recovery_checkpoint_manager(
    keyspace: Arc<Keyspace>,
    wals: Vec<Arc<dyn accord::CommitLog>>,
    config: RecoveryCheckpointConfig,
    interval: Duration,
    disk_path: PathBuf,
    manifest_path: PathBuf,
    stats: Arc<RecoveryCheckpointStats>,
) -> RecoveryCheckpointController {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
    let controller = RecoveryCheckpointController { tx: cmd_tx };
    if let Err(err) = load_recovery_manifest(&manifest_path) {
        stats.record_manifest_parse_error();
        let now_ms = unix_time_ms();
        stats.record_failure(now_ms, &err.to_string(), "startup");
        tracing::warn!(error = ?err, path = %manifest_path.display(), "recovery manifest parse failed");
    }

    stats.set_paused(false);
    tokio::spawn(async move {
        let mut paused = false;
        let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(100)));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if paused {
                        continue;
                    }
                    let keyspace = keyspace.clone();
                    let wals = wals.clone();
                    let path = manifest_path.clone();
                    let stats_ref = stats.clone();
                    let disk_path = disk_path.clone();
                    let cfg = config.clone();
                    let res = tokio::task::spawn_blocking(move || {
                        run_recovery_checkpoint_once(
                            &keyspace,
                            &wals,
                            cfg,
                            &disk_path,
                            &path,
                            &stats_ref,
                            "interval",
                        )
                    })
                    .await;

                    match res {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            let now_ms = unix_time_ms();
                            stats.record_failure(now_ms, &err.to_string(), "interval");
                            tracing::warn!(error = ?err, "recovery checkpoint iteration failed");
                        }
                        Err(err) => {
                            let now_ms = unix_time_ms();
                            stats.record_failure(now_ms, &err.to_string(), "interval");
                            tracing::warn!(error = ?err, "recovery checkpoint task join failure");
                        }
                    }
                }
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        RecoveryCheckpointControl::SetPaused { paused: new_paused, done } => {
                            paused = new_paused;
                            stats.set_paused(new_paused);
                            let _ = done.send(());
                        }
                        RecoveryCheckpointControl::Trigger { done } => {
                            let keyspace = keyspace.clone();
                            let wals = wals.clone();
                            let path = manifest_path.clone();
                            let stats_ref = stats.clone();
                            let disk_path = disk_path.clone();
                            let cfg = config.clone();
                            let res = tokio::task::spawn_blocking(move || {
                                run_recovery_checkpoint_once(
                                    &keyspace,
                                    &wals,
                                    cfg,
                                    &disk_path,
                                    &path,
                                    &stats_ref,
                                    "manual",
                                )
                            })
                            .await;

                            let outcome = match res {
                                Ok(Ok(())) => Ok(()),
                                Ok(Err(err)) => {
                                    let now_ms = unix_time_ms();
                                    stats.record_failure(now_ms, &err.to_string(), "manual");
                                    Err(err)
                                }
                                Err(err) => {
                                    let now_ms = unix_time_ms();
                                    let msg = err.to_string();
                                    stats.record_failure(now_ms, &msg, "manual");
                                    Err(anyhow!(msg))
                                }
                            };
                            stats.record_manual_trigger(outcome.is_ok());
                            let _ = done.send(outcome);
                        }
                    }
                }
            }
        }
    });
    controller
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
pub async fn run_node(args: NodeArgs) -> anyhow::Result<()> {
    run_node_with_shutdown(args, tokio::signal::ctrl_c()).await
}

/// Initialize storage, transport, consensus groups, and servers for a node,
/// and run until `shutdown` resolves.
pub async fn run_node_with_shutdown<F>(args: NodeArgs, shutdown: F) -> anyhow::Result<()>
where
    F: std::future::Future<Output = Result<(), std::io::Error>> + Send,
{
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
    let mut checkpoint_wals: Vec<Arc<dyn accord::CommitLog>> = vec![shared_wal.clone()];
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
            join_seed_and_fetch_snapshot(args.node_id, seed, args.listen_grpc, args.listen_redis)
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

    let data_shards = resolve_data_shards(args.data_shards);

    let cluster_state_path = data_dir.join("meta").join("cluster_state.json");
    let cluster_store = ClusterStateStore::load_or_init(
        cluster_state_path,
        member_infos,
        args.replication_factor.max(1),
        args.initial_ranges.max(1).min(data_shards),
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
        recovery_min_delay: Duration::from_millis(args.recovery_min_delay_ms.max(1)),
        stall_recover_interval: Duration::from_millis(args.stall_recover_interval_ms.max(1)),
        preaccept_stall_hits: args.preaccept_stall_hits.max(1),
        execute_batch_max: args.accord_execute_batch_max.max(1),
        inline_command_in_accept_commit: args.accord_inline_command_in_accept_commit,
        commit_log_batch_max: args.commit_log_batch_max.max(1),
        commit_log_batch_wait: Duration::from_micros(args.commit_log_batch_wait_us),
    };

    let meta_wal_root = match args.wal_engine {
        WalEngine::File => wal_dir.join("meta"),
        WalEngine::RaftEngine => wal_dir.join("meta-raft-engine"),
    };
    let open_meta_wal = |meta_index: usize| -> anyhow::Result<Arc<dyn accord::CommitLog>> {
        let meta_wal_dir = meta_wal_root.join(format!("group-{meta_index}"));
        let wal: Arc<dyn accord::CommitLog> = match args.wal_engine {
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
        Ok(wal)
    };

    let range_stats = Arc::new(LocalRangeStats::new(keyspace.clone(), data_shards)?);
    let migration_stats_hook: Arc<cluster::MigrationStatsHook> = {
        let range_stats = range_stats.clone();
        Arc::new(move |from_shard, to_shard, moved_keys| {
            range_stats.on_range_migration(from_shard, to_shard, moved_keys);
        })
    };
    let cluster_sm = Arc::new(ClusterStateMachine::new(
        cluster_store.clone(),
        Some(Arc::new(cluster::FjallRangeMigrator::with_migration_hook(
            keyspace.clone(),
            Some(migration_stats_hook),
        ))),
        data_shards,
    ));
    let meta_group_count = args.meta_groups.max(1);
    let mut meta_groups: Vec<(usize, GroupId, Arc<accord::Group>)> =
        Vec::with_capacity(meta_group_count);
    let mut meta_handles = BTreeMap::new();
    let mut data_groups: Vec<Arc<accord::Group>> = Vec::with_capacity(data_shards);
    let mut data_handles: Vec<accord::Handle> = Vec::with_capacity(data_shards);
    let mut groups = HashMap::new();
    for meta_index in 0..meta_group_count {
        let group_id = meta_group_id_for_index(meta_index);
        let meta_wal = open_meta_wal(meta_index)?;
        checkpoint_wals.push(meta_wal.clone());
        let meta_group = Arc::new(accord::Group::new(
            mk_cfg(group_id),
            transport.clone(),
            cluster_sm.clone(),
            Some(meta_wal),
        ));
        meta_handles.insert(meta_index, meta_group.handle());
        groups.insert(group_id, meta_group.clone());
        meta_groups.push((meta_index, group_id, meta_group));
    }

    let split_lock = cluster_store.split_lock();
    let data_group_slots = (0..data_shards)
        .map(|_| {
            Arc::new(std::sync::RwLock::new(
                None::<std::sync::Weak<accord::Group>>,
            ))
        })
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
        let visibility_hook: Arc<kv::VisibilityDeltaHook> = {
            let range_stats = range_stats.clone();
            Arc::new(move |delta| {
                range_stats.on_visible_insertions(shard, delta);
            })
        };
        let kv_sm = Arc::new(kv::KvStateMachine::with_membership_hook(
            kv_shards[shard].clone(),
            Some(split_lock.clone()),
            Some(membership_hook),
            Some(visibility_hook),
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

    for (meta_index, group_id, meta_group) in &meta_groups {
        let _ = meta_group.replay_commits().await.with_context(|| {
            format!("replay meta commit log (meta_index={meta_index}, group={group_id})")
        })?;
        meta_group.spawn_executor();
    }

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
    let meta_proposal_stats = Arc::new(MetaProposalStats::default());
    let recovery_checkpoint_stats = Arc::new(RecoveryCheckpointStats::default());
    let recovery_checkpoint_controller = if args.recovery_checkpoint_enabled {
        let interval = Duration::from_millis(args.recovery_checkpoint_interval_ms.max(100));
        let manifest_path = data_dir.join("meta").join("recovery_checkpoints.json");
        let config = RecoveryCheckpointConfig {
            persist_mode: args.recovery_checkpoint_persist_mode.to_fjall(),
            warn_lag_entries: args.recovery_checkpoint_warn_lag_entries,
            min_free_bytes: args.recovery_checkpoint_min_free_bytes,
            min_free_pct: args.recovery_checkpoint_min_free_pct.max(0.0),
        };
        Some(spawn_recovery_checkpoint_manager(
            keyspace.clone(),
            checkpoint_wals.clone(),
            config,
            interval,
            data_dir.clone(),
            manifest_path,
            recovery_checkpoint_stats.clone(),
        ))
    } else {
        None
    };
    let client_batch_queue = args.client_batch_queue.max(1);
    let (client_set_tx, client_set_rx) = mpsc::channel(client_batch_queue);
    let (client_get_tx, client_get_rx) = mpsc::channel(client_batch_queue);
    let client_batch_cfg = ClientBatchConfig {
        set_max: args.client_set_batch_max.max(1),
        get_max: args.client_get_batch_max.max(1),
        wait: Duration::from_micros(args.client_batch_wait_us),
        inflight: args.client_batch_inflight.max(1),
    };
    let range_write_batch_target = resolve_range_write_batch_target(args.range_write_batch_target);
    let range_write_batch_max_bytes =
        resolve_range_write_batch_max_bytes(args.range_write_batch_max_bytes);

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
        meta_proposal_stats,
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
        client_set_proposal_pipeline_depth: args.client_set_proposal_pipeline_depth.max(1),
        range_write_batch_target,
        range_write_batch_max_bytes,
        range_write_proposal_pipeline_depth: args.range_write_proposal_pipeline_depth.max(1),
        client_inflight_ops: Arc::new(AtomicU64::new(0)),
        cluster_store,
        meta_handles,
        controller_lease_ttl: Duration::from_millis(args.controller_lease_ttl_ms.max(1)),
        controller_lease_renew_margin: Duration::from_millis(
            args.controller_lease_renew_margin_ms.max(1),
        ),
        meta_move_stuck_warn: Duration::from_millis(args.meta_move_stuck_warn_ms.max(1)),
        split_lock: split_lock.clone(),
        sql_write_locks: Arc::new(
            (0..data_shards.max(1))
                .map(|_| tokio::sync::Mutex::new(()))
                .collect(),
        ),
        shard_load: ShardLoadTracker::new(data_shards),
        split_health: Arc::new(SplitHealthStats::default()),
        range_stats,
        recovery_checkpoints: recovery_checkpoint_stats.clone(),
        recovery_checkpoint_controller,
    });

    struct LocalNodeStateRegistration {
        grpc_addr: SocketAddr,
    }

    impl Drop for LocalNodeStateRegistration {
        fn drop(&mut self) {
            unregister_local_node_state(self.grpc_addr);
        }
    }

    let grpc_addr = args.listen_grpc;
    register_local_node_state(grpc_addr, &state);
    let _local_state_registration = LocalNodeStateRegistration { grpc_addr };

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
            merge_max_keys: args.range_merge_max_keys,
            merge_cooldown: Duration::from_millis(args.range_merge_cooldown_ms.max(1)),
            merge_max_qps: args.range_merge_max_qps,
            merge_qps_sustain: args.range_merge_qps_sustain.max(1),
            merge_key_hysteresis_pct: args.range_merge_key_hysteresis_pct.clamp(1, 100),
        },
    );

    rebalance_manager::spawn(
        state.clone(),
        RebalanceManagerConfig {
            enable_balancing: args.rebalance_enabled,
            interval: Duration::from_millis(args.rebalance_interval_ms.max(100)),
            move_timeout: Duration::from_millis(args.rebalance_move_timeout_ms),
        },
    );

    meta_manager::spawn(
        state.clone(),
        MetaManagerConfig {
            enable_split: args.meta_split_enabled,
            enable_balance: args.meta_balance_enabled,
            interval: Duration::from_millis(args.meta_mgr_interval_ms.max(100)),
            split_min_qps: args.meta_split_min_qps,
            split_min_ops: args.meta_split_min_ops,
            split_qps_sustain: args.meta_split_qps_sustain.max(1),
            split_cooldown: Duration::from_millis(args.meta_split_cooldown_ms.max(1)),
            max_meta_groups: args.meta_groups.max(1),
            replica_skew_threshold: args.meta_replica_skew_threshold.max(1),
            lease_skew_threshold: args.meta_lease_skew_threshold.max(1),
            balance_cooldown: Duration::from_millis(args.meta_balance_cooldown_ms.max(1)),
            move_stuck_warn: Duration::from_millis(args.meta_move_stuck_warn_ms.max(1)),
            lag_warn_ops: args.meta_lag_warn_ops.max(1),
            move_timeout: Duration::from_millis(args.meta_move_timeout_ms),
            max_client_inflight: args.meta_max_client_inflight.max(1),
        },
    );

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

    shutdown.await?;
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
    use crate::cluster::{ReplicaMove, ReplicaMovePhase};

    fn sample_state() -> ClusterState {
        ClusterState {
            epoch: 1,
            frozen: false,
            replication_factor: 3,
            members: BTreeMap::new(),
            shards: vec![],
            shard_replica_roles: BTreeMap::new(),
            shard_rebalances: BTreeMap::new(),
            shard_merges: BTreeMap::new(),
            shard_fences: BTreeMap::new(),
            retired_ranges: BTreeMap::new(),
            meta_ranges: Vec::new(),
            meta_replica_roles: BTreeMap::new(),
            meta_rebalances: BTreeMap::new(),
            controller_leases: BTreeMap::new(),
            meta_controller_lease: None,
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

    #[test]
    fn rebalance_controller_eligibility_rejects_learner_during_move() {
        let mut state = sample_state();
        state.members = BTreeMap::from([
            (
                1,
                MemberInfo {
                    node_id: 1,
                    grpc_addr: "127.0.0.1:15051".to_string(),
                    redis_addr: "127.0.0.1:16379".to_string(),
                    state: MemberState::Active,
                },
            ),
            (
                2,
                MemberInfo {
                    node_id: 2,
                    grpc_addr: "127.0.0.1:15052".to_string(),
                    redis_addr: "127.0.0.1:16380".to_string(),
                    state: MemberState::Active,
                },
            ),
            (
                3,
                MemberInfo {
                    node_id: 3,
                    grpc_addr: "127.0.0.1:15053".to_string(),
                    redis_addr: "127.0.0.1:16381".to_string(),
                    state: MemberState::Active,
                },
            ),
            (
                4,
                MemberInfo {
                    node_id: 4,
                    grpc_addr: "127.0.0.1:15054".to_string(),
                    redis_addr: "127.0.0.1:16382".to_string(),
                    state: MemberState::Active,
                },
            ),
        ]);
        let mut shard = sample_shard();
        shard.replicas = vec![1, 2, 3, 4];
        state.shards.push(shard.clone());
        state.shard_replica_roles.insert(
            shard.shard_id,
            BTreeMap::from([
                (1, ReplicaRole::Voter),
                (2, ReplicaRole::Voter),
                (3, ReplicaRole::Voter),
                (4, ReplicaRole::Learner),
            ]),
        );
        state.shard_rebalances.insert(
            shard.shard_id,
            ReplicaMove {
                from_node: 3,
                to_node: 4,
                phase: ReplicaMovePhase::LearnerSync,
                backfill_done: false,
                target_leaseholder: None,
                started_unix_ms: 1,
                last_progress_unix_ms: 1,
            },
        );

        assert!(NodeState::controller_lease_eligible_for_domain(
            &state,
            ControllerDomain::Rebalance,
            1
        ));
        assert!(!NodeState::controller_lease_eligible_for_domain(
            &state,
            ControllerDomain::Rebalance,
            4
        ));
    }

    #[test]
    fn rebalance_controller_eligibility_allows_any_node_without_inflight_move() {
        let state = sample_state();
        assert!(NodeState::controller_lease_eligible_for_domain(
            &state,
            ControllerDomain::Rebalance,
            42
        ));
        assert!(NodeState::controller_lease_eligible_for_domain(
            &state,
            ControllerDomain::Range,
            42
        ));
    }

    #[test]
    fn chunk_batch_set_items_respects_op_limit() {
        let items = (0..5)
            .map(|idx| (format!("k{idx}").into_bytes(), vec![idx as u8]))
            .collect::<Vec<_>>();
        let chunks = chunk_batch_set_items_by_limits(items, 2, usize::MAX);
        let sizes = chunks.iter().map(Vec::len).collect::<Vec<_>>();
        assert_eq!(sizes, vec![2, 2, 1]);
    }

    #[test]
    fn chunk_batch_set_items_respects_byte_limit() {
        let items = vec![
            (b"k1".to_vec(), vec![0; 7]), // 9 bytes
            (b"k2".to_vec(), vec![1; 7]), // 9 bytes (would exceed 16 if grouped)
            (b"k3".to_vec(), vec![2; 2]), // 4 bytes (fits with k2)
        ];
        let chunks = chunk_batch_set_items_by_limits(items, 100, 16);
        let sizes = chunks.iter().map(Vec::len).collect::<Vec<_>>();
        assert_eq!(sizes, vec![1, 2]);
    }

    #[test]
    fn chunk_batch_set_items_keeps_oversize_item_in_single_chunk() {
        let items = vec![
            (b"k1".to_vec(), vec![0; 20]), // 22 bytes, larger than cap
            (b"k2".to_vec(), vec![1; 2]),
        ];
        let chunks = chunk_batch_set_items_by_limits(items, 100, 16);
        let sizes = chunks.iter().map(Vec::len).collect::<Vec<_>>();
        assert_eq!(sizes, vec![1, 1]);
    }
}

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL_ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
