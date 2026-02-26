//! Minimal admin client for cluster control-plane RPCs.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;

use anyhow::Context;
use clap::{Parser, Subcommand};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use serde::Deserialize;

include!(concat!(env!("OUT_DIR"), "/volo_gen.rs"));

use volo_gen::holo_store::rpc;

const DEFAULT_RANGE_BOUND_MAX_CHARS: usize = 24;

#[derive(Parser)]
#[command(name = "holoctl")]
#[command(about = "Control-plane admin client for HoloStore", long_about = None)]
struct Args {
    /// Target gRPC address for a node (host:port).
    #[arg(long, default_value = "127.0.0.1:15051")]
    target: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Fetch and print the cluster state JSON.
    State,
    /// Show in-flight merge progress/status.
    MergeStatus,
    /// Show per-node range responsibilities and local record counts.
    Topology,
    /// Show meta-range load/lag/proposal and in-flight move status.
    MetaStatus,
    /// Show controller lease holders for each controller domain.
    ControllerStatus,
    /// Add or update a node in the cluster membership.
    AddNode {
        #[arg(long)]
        node_id: u64,
        #[arg(long)]
        grpc_addr: String,
        #[arg(long)]
        redis_addr: String,
    },
    /// Start decommissioning a node (drain replicas, then finalize removal).
    RemoveNode {
        #[arg(long)]
        node_id: u64,
    },
    /// Split a metadata range at a hash boundary.
    ///
    /// By default, `--target-meta-index 0` selects the first free meta-group slot.
    SplitMeta {
        #[arg(long)]
        split_hash: u64,
        #[arg(long, default_value_t = 0)]
        target_meta_index: u64,
    },
    /// Request staged metadata-range rebalance (single replica replacement) and/or lease transfer.
    MetaRebalance {
        #[arg(long)]
        meta_range_id: u64,
        #[arg(long = "replica")]
        replicas: Vec<u64>,
        #[arg(long, default_value_t = 0)]
        leaseholder: u64,
    },
    /// Split the range that owns the provided key.
    Split {
        /// Split key as UTF-8 text.
        #[arg(long)]
        split_key: String,
        /// Interpret the split key as hex bytes (no 0x prefix).
        #[arg(long, default_value_t = false)]
        hex: bool,
    },
    /// Merge the range with the right-hand neighbor.
    Merge {
        #[arg(long)]
        left_shard_id: u64,
        /// Pause an in-flight merge.
        #[arg(long, default_value_t = false)]
        pause: bool,
        /// Resume a paused merge.
        #[arg(long, default_value_t = false)]
        resume: bool,
        /// Cancel an in-flight merge (only valid pre-cutover).
        #[arg(long, default_value_t = false)]
        cancel: bool,
    },
    /// Request staged range rebalance (single replica replacement) and/or lease transfer.
    Rebalance {
        #[arg(long)]
        shard_id: u64,
        #[arg(long = "replica")]
        replicas: Vec<u64>,
        #[arg(long, default_value_t = 0)]
        leaseholder: u64,
    },
    /// Freeze or unfreeze client traffic (used for safe range operations).
    Freeze {
        /// Set to true to freeze traffic, false to unfreeze.
        #[arg(long, action = clap::ArgAction::Set, value_parser = clap::value_parser!(bool))]
        frozen: bool,
    },
    /// Control durability checkpoint runtime (pause/resume/trigger/status).
    Checkpoint {
        #[command(subcommand)]
        action: CheckpointAction,
    },
}

#[derive(Subcommand)]
enum CheckpointAction {
    Status,
    Pause,
    Resume,
    Trigger,
}

#[derive(Debug, Clone, Deserialize)]
struct ClusterStateView {
    members: BTreeMap<String, MemberView>,
    shards: Vec<ShardView>,
    #[serde(default)]
    meta_ranges: Vec<MetaRangeView>,
    #[serde(default)]
    meta_rebalances: BTreeMap<String, ReplicaMoveView>,
    #[serde(default)]
    controller_leases: BTreeMap<String, ControllerLeaseView>,
    #[serde(default)]
    shard_rebalances: BTreeMap<String, ReplicaMoveView>,
    #[serde(default)]
    shard_merges: BTreeMap<String, RangeMergeView>,
    #[serde(default)]
    meta_health: MetaHealthView,
    #[serde(default)]
    recovery_health: RecoveryHealthView,
    #[serde(default)]
    split_health: SplitHealthView,
}

#[derive(Debug, Clone, Deserialize)]
struct MemberView {
    node_id: u64,
    grpc_addr: String,
    #[allow(dead_code)]
    redis_addr: String,
    state: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ShardView {
    shard_id: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    replicas: Vec<u64>,
    leaseholder: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct MetaRangeView {
    meta_range_id: u64,
    meta_index: usize,
    start_hash: u64,
    end_hash: u64,
    replicas: Vec<u64>,
    leaseholder: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct ReplicaMoveView {
    from_node: u64,
    to_node: u64,
    phase: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ControllerLeaseView {
    holder: u64,
    term: u64,
    lease_until_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct MetaHealthView {
    #[serde(default)]
    ops_by_index: BTreeMap<String, u64>,
    #[serde(default)]
    lag_by_index: BTreeMap<String, u64>,
    #[serde(default)]
    proposal_total: MetaProposalTotalView,
    #[serde(default)]
    proposal_by_index: BTreeMap<String, MetaProposalIndexView>,
    #[serde(default)]
    rebalances_inflight: u64,
    #[serde(default)]
    rebalances_stuck: u64,
    #[serde(default)]
    stuck_threshold_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct MetaProposalTotalView {
    #[serde(default)]
    count: u64,
    #[serde(default)]
    errors: u64,
    #[serde(default)]
    avg_us: f64,
    #[serde(default)]
    max_us: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct MetaProposalIndexView {
    #[serde(default)]
    count: u64,
    #[serde(default)]
    errors: u64,
    #[serde(default)]
    avg_us: f64,
    #[serde(default)]
    max_us: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct SplitHealthView {
    #[serde(default)]
    attempts: u64,
    #[serde(default)]
    successes: u64,
    #[serde(default)]
    failures: u64,
    #[serde(default)]
    transient_aborts: u64,
    #[serde(default)]
    backoff_active: u64,
    #[serde(default)]
    last_attempt_ms: u64,
    #[serde(default)]
    last_success_ms: u64,
    #[serde(default)]
    last_failure_ms: u64,
    #[serde(default)]
    last_failure_reason: String,
    #[serde(default)]
    attempt_reasons: BTreeMap<String, u64>,
    #[serde(default)]
    failure_reasons: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RecoveryHealthView {
    #[serde(default)]
    checkpoint_successes: u64,
    #[serde(default)]
    checkpoint_failures: u64,
    #[serde(default)]
    checkpoint_manual_triggers: u64,
    #[serde(default)]
    checkpoint_manual_trigger_failures: u64,
    #[serde(default)]
    checkpoint_pressure_skips: u64,
    #[serde(default)]
    checkpoint_manifest_parse_errors: u64,
    #[serde(default)]
    last_attempt_ms: u64,
    #[serde(default)]
    last_success_ms: u64,
    #[serde(default)]
    max_lag_entries: u64,
    #[serde(default)]
    blocked_groups: u64,
    #[serde(default)]
    paused: bool,
    #[serde(default)]
    last_run_reason: String,
    #[serde(default)]
    last_free_bytes: u64,
    #[serde(default)]
    last_free_pct: f64,
    #[serde(default)]
    last_error: String,
}

#[derive(Debug, Clone, Deserialize)]
struct RangeMergeView {
    left_shard_id: u64,
    right_shard_id: u64,
    phase: String,
    #[serde(default)]
    copied_rows: u64,
    #[serde(default)]
    copied_bytes: u64,
    #[serde(default)]
    lag_ops: u64,
    #[serde(default)]
    retry_count: u32,
    #[serde(default)]
    eta_seconds: u64,
    #[serde(default)]
    last_error: String,
    #[serde(default)]
    paused: bool,
    #[serde(default)]
    started_unix_ms: u64,
    #[serde(default)]
    last_progress_unix_ms: u64,
    #[serde(default)]
    cutover_epoch: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let client = rpc::HoloRpcClientBuilder::new("holo_store.rpc.HoloRpc")
        .address(volo::net::Address::from(
            args.target.parse::<std::net::SocketAddr>()?,
        ))
        .build();

    match args.command {
        Command::State => {
            let resp = client
                .cluster_state(rpc::ClusterStateRequest {})
                .await?
                .into_inner();
            println!("{}", resp.json);
        }
        Command::MergeStatus => {
            let resp = client
                .cluster_state(rpc::ClusterStateRequest {})
                .await?
                .into_inner();
            let state: ClusterStateView =
                serde_json::from_str(&resp.json).context("parse cluster state json")?;
            let mut rows = Vec::new();
            let mut merges = state.shard_merges.values().cloned().collect::<Vec<_>>();
            merges.sort_by_key(|m| (m.left_shard_id, m.right_shard_id));
            for merge in merges {
                rows.push(vec![
                    merge.left_shard_id.to_string(),
                    merge.right_shard_id.to_string(),
                    merge.phase,
                    if merge.paused { "yes" } else { "no" }.to_string(),
                    merge.copied_rows.to_string(),
                    merge.copied_bytes.to_string(),
                    merge.lag_ops.to_string(),
                    merge.retry_count.to_string(),
                    if merge.eta_seconds == 0 {
                        "-".to_string()
                    } else {
                        format!("{}s", merge.eta_seconds)
                    },
                    if merge.last_error.is_empty() {
                        "-".to_string()
                    } else {
                        merge.last_error
                    },
                    if merge.started_unix_ms == 0 {
                        "-".to_string()
                    } else {
                        merge.started_unix_ms.to_string()
                    },
                    if merge.last_progress_unix_ms == 0 {
                        "-".to_string()
                    } else {
                        merge.last_progress_unix_ms.to_string()
                    },
                    merge
                        .cutover_epoch
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                ]);
            }
            if rows.is_empty() {
                println!("no in-flight merges");
            } else {
                print_ascii_table(
                    &[
                        "LEFT",
                        "RIGHT",
                        "PHASE",
                        "PAUSED",
                        "ROWS",
                        "BYTES",
                        "LAG_OPS",
                        "RETRIES",
                        "ETA",
                        "LAST_ERROR",
                        "STARTED_MS",
                        "LAST_PROGRESS_MS",
                        "CUTOVER_EPOCH",
                    ],
                    &rows,
                );
            }
        }
        Command::Topology => {
            let resp = client
                .cluster_state(rpc::ClusterStateRequest {})
                .await?
                .into_inner();
            let state: ClusterStateView =
                serde_json::from_str(&resp.json).context("parse cluster state json")?;
            let split_health = match fetch_split_health_for_range_controller(&state).await {
                Ok(view) => view,
                Err(err) => {
                    eprintln!(
                        "warning: failed to read split health from range-controller holder: {err}"
                    );
                    state.split_health.clone()
                }
            };
            let mut members = state.members.values().cloned().collect::<Vec<_>>();
            members.sort_by_key(|m| m.node_id);
            let mut member_state_counts = BTreeMap::<String, usize>::new();
            for member in &members {
                *member_state_counts.entry(member.state.clone()).or_default() += 1;
            }
            let mut moves_by_shard = HashMap::new();
            for (shard_id, mv) in &state.shard_rebalances {
                if let Ok(id) = shard_id.parse::<u64>() {
                    moves_by_shard.insert(id, mv.clone());
                }
            }
            let mut merges_by_shard = HashMap::<u64, String>::new();
            for merge in state.shard_merges.values() {
                let left_status = format_merge_status(merge, true);
                let right_status = format_merge_status(merge, false);
                merges_by_shard.insert(merge.left_shard_id, left_status);
                merges_by_shard.insert(merge.right_shard_id, right_status);
            }

            // Query local record counts from each non-removed member.
            let mut counts_by_node: HashMap<u64, HashMap<u64, u64>> = HashMap::new();
            let mut fetch_errors: HashMap<u64, String> = HashMap::new();
            let mut range_stats_tasks = FuturesUnordered::new();
            for member in &members {
                if member.state == "Removed" {
                    continue;
                }
                let node_id = member.node_id;
                let grpc_addr = member.grpc_addr.clone();
                range_stats_tasks
                    .push(async move { (node_id, fetch_range_stats(&grpc_addr).await) });
            }
            while let Some((node_id, result)) = range_stats_tasks.next().await {
                match result {
                    Ok(map) => {
                        counts_by_node.insert(node_id, map);
                    }
                    Err(err) => {
                        fetch_errors.insert(node_id, err.to_string());
                    }
                }
            }

            let mut total_unique_keys = 0u64;
            let mut unique_keys_complete = true;
            let mut total_replicated_keys = 0u64;
            let mut replicated_keys_complete = true;
            for shard in &state.shards {
                match shard_record_count(
                    shard.leaseholder,
                    shard.shard_id,
                    &counts_by_node,
                    &fetch_errors,
                ) {
                    Some(count) => total_unique_keys = total_unique_keys.saturating_add(count),
                    None => unique_keys_complete = false,
                }
                for replica in &shard.replicas {
                    match shard_record_count(
                        *replica,
                        shard.shard_id,
                        &counts_by_node,
                        &fetch_errors,
                    ) {
                        Some(count) => {
                            total_replicated_keys = total_replicated_keys.saturating_add(count);
                        }
                        None => replicated_keys_complete = false,
                    }
                }
            }

            let mut rows = Vec::new();
            for member in &members {
                let mut node_rows = 0usize;
                for shard in &state.shards {
                    if !shard.replicas.contains(&member.node_id) {
                        continue;
                    }
                    node_rows += 1;
                    let role = if shard.leaseholder == member.node_id {
                        "leaseholder"
                    } else {
                        "replica"
                    };
                    let records = if let Some(err) = fetch_errors.get(&member.node_id) {
                        format!("ERR ({err})")
                    } else {
                        counts_by_node
                            .get(&member.node_id)
                            .and_then(|m| m.get(&shard.shard_id))
                            .map(|v| format_count_u64(*v))
                            .unwrap_or_else(|| "n/a".to_string())
                    };
                    let move_status =
                        format_move_status(member.node_id, shard.shard_id, &moves_by_shard);
                    let merge_status = merges_by_shard
                        .get(&shard.shard_id)
                        .cloned()
                        .unwrap_or_default();
                    let state_status = format_row_state(&merge_status, &move_status);
                    rows.push(vec![
                        member.node_id.to_string(),
                        member.state.clone(),
                        role.to_string(),
                        shard.shard_id.to_string(),
                        format_range(&shard.start_key, &shard.end_key),
                        records,
                        state_status,
                    ]);
                }
                if node_rows == 0 {
                    rows.push(vec![
                        member.node_id.to_string(),
                        member.state.clone(),
                        "-".to_string(),
                        "-".to_string(),
                        "-".to_string(),
                        "-".to_string(),
                        String::new(),
                    ]);
                }
            }
            rows.sort_by(|a, b| {
                let an = a[0].parse::<u64>().unwrap_or(0);
                let bn = b[0].parse::<u64>().unwrap_or(0);
                let ashard = a[3].parse::<u64>().unwrap_or(0);
                let bshard = b[3].parse::<u64>().unwrap_or(0);
                (an, ashard).cmp(&(bn, bshard))
            });

            let headers = [
                "NODE",
                "NODE_STATE",
                "ROLE",
                "SHARD",
                "RANGE",
                "RECORDS",
                "STATE",
            ];
            let mut footer_lines = vec![vec![
                "TOTALS".to_string(),
                format!("nodes: {}", format_count_usize(members.len())),
                format!("states: {}", format_state_counts(&member_state_counts)),
                format!("shards: {}", format_count_usize(state.shards.len())),
                format!(
                    "unique: {}",
                    format_total_count(total_unique_keys, unique_keys_complete)
                ),
                format!(
                    "replicated: {}",
                    format_total_count(total_replicated_keys, replicated_keys_complete)
                ),
                String::new(),
            ]];
            footer_lines.push(vec![
                "SPLIT".to_string(),
                format!("attempts: {}", format_count_u64(split_health.attempts)),
                format!("success: {}", format_count_u64(split_health.successes)),
                format!(
                    "failures: {} (transient: {})",
                    format_count_u64(split_health.failures),
                    format_count_u64(split_health.transient_aborts)
                ),
                format!("backoff: {}", format_count_u64(split_health.backoff_active)),
                format!(
                    "attempts_by: {}",
                    format_top_reasons(&split_health.attempt_reasons),
                ),
                String::new(),
            ]);
            if rows.is_empty() {
                println!("no shard responsibilities found");
                print_ascii_table(&headers, &footer_lines);
            } else {
                print_ascii_table_with_footer(&headers, &rows, &footer_lines);
            }
        }
        Command::MetaStatus => {
            let resp = client
                .cluster_state(rpc::ClusterStateRequest {})
                .await?
                .into_inner();
            let state: ClusterStateView =
                serde_json::from_str(&resp.json).context("parse cluster state json")?;

            let mut rows = Vec::new();
            let mut ranges = state.meta_ranges.clone();
            ranges.sort_by_key(|r| (r.meta_index, r.meta_range_id));
            for range in ranges {
                let idx_key = range.meta_index.to_string();
                let ops = state
                    .meta_health
                    .ops_by_index
                    .get(&idx_key)
                    .copied()
                    .unwrap_or(0);
                let lag = state
                    .meta_health
                    .lag_by_index
                    .get(&idx_key)
                    .copied()
                    .unwrap_or(0);
                let prop = state
                    .meta_health
                    .proposal_by_index
                    .get(&idx_key)
                    .cloned()
                    .unwrap_or_default();
                rows.push(vec![
                    range.meta_range_id.to_string(),
                    range.meta_index.to_string(),
                    format_hash_range(range.start_hash, range.end_hash),
                    range.leaseholder.to_string(),
                    join_ids(&range.replicas),
                    ops.to_string(),
                    lag.to_string(),
                    prop.count.to_string(),
                    prop.errors.to_string(),
                    format!("{:.1}", prop.avg_us),
                    prop.max_us.to_string(),
                    format_meta_move_status(range.meta_range_id, &state.meta_rebalances),
                ]);
            }
            if rows.is_empty() {
                println!("no meta ranges");
            } else {
                print_ascii_table(
                    &[
                        "META_RANGE",
                        "META_INDEX",
                        "HASH_RANGE",
                        "LEASEHOLDER",
                        "REPLICAS",
                        "OPS",
                        "LAG",
                        "PROP_COUNT",
                        "PROP_ERR",
                        "PROP_AVG_US",
                        "PROP_MAX_US",
                        "MOVE",
                    ],
                    &rows,
                );
                println!(
                    "meta totals: inflight_moves={} stuck_moves={} stuck_threshold_ms={} proposal_count={} proposal_errors={} proposal_avg_us={:.1} proposal_max_us={}",
                    state.meta_health.rebalances_inflight,
                    state.meta_health.rebalances_stuck,
                    state.meta_health.stuck_threshold_ms,
                    state.meta_health.proposal_total.count,
                    state.meta_health.proposal_total.errors,
                    state.meta_health.proposal_total.avg_us,
                    state.meta_health.proposal_total.max_us
                );
                println!(
                    "recovery checkpoints: success={} failures={} manual_triggers={} manual_failures={} pressure_skips={} manifest_parse_errors={} paused={} blocked_groups={} max_lag_entries={} last_attempt_ms={} last_success_ms={} last_run_reason={} last_free_bytes={} last_free_pct={:.2} last_error={}",
                    state.recovery_health.checkpoint_successes,
                    state.recovery_health.checkpoint_failures,
                    state.recovery_health.checkpoint_manual_triggers,
                    state.recovery_health.checkpoint_manual_trigger_failures,
                    state.recovery_health.checkpoint_pressure_skips,
                    state.recovery_health.checkpoint_manifest_parse_errors,
                    state.recovery_health.paused,
                    state.recovery_health.blocked_groups,
                    state.recovery_health.max_lag_entries,
                    state.recovery_health.last_attempt_ms,
                    state.recovery_health.last_success_ms,
                    if state.recovery_health.last_run_reason.is_empty() {
                        "-"
                    } else {
                        &state.recovery_health.last_run_reason
                    },
                    state.recovery_health.last_free_bytes,
                    state.recovery_health.last_free_pct,
                    if state.recovery_health.last_error.is_empty() {
                        "-"
                    } else {
                        &state.recovery_health.last_error
                    }
                );
            }
        }
        Command::ControllerStatus => {
            let resp = client
                .cluster_state(rpc::ClusterStateRequest {})
                .await?
                .into_inner();
            let state: ClusterStateView =
                serde_json::from_str(&resp.json).context("parse cluster state json")?;
            let now_ms = unix_time_ms();
            let mut rows = Vec::new();
            let mut domains = state.controller_leases.into_iter().collect::<Vec<_>>();
            domains.sort_by(|a, b| a.0.cmp(&b.0));
            for (domain, lease) in domains {
                let remaining = lease.lease_until_ms.saturating_sub(now_ms);
                let active = if lease.lease_until_ms > now_ms {
                    "yes"
                } else {
                    "no"
                };
                rows.push(vec![
                    domain,
                    lease.holder.to_string(),
                    lease.term.to_string(),
                    lease.lease_until_ms.to_string(),
                    remaining.to_string(),
                    active.to_string(),
                ]);
            }
            if rows.is_empty() {
                println!("no controller leases");
            } else {
                print_ascii_table(
                    &[
                        "DOMAIN",
                        "HOLDER",
                        "TERM",
                        "LEASE_UNTIL_MS",
                        "REMAINING_MS",
                        "ACTIVE",
                    ],
                    &rows,
                );
            }
        }
        Command::AddNode {
            node_id,
            grpc_addr,
            redis_addr,
        } => {
            client
                .cluster_add_node(rpc::ClusterAddNodeRequest {
                    node_id,
                    grpc_addr: grpc_addr.into(),
                    redis_addr: redis_addr.into(),
                })
                .await?;
            println!("ok");
        }
        Command::RemoveNode { node_id } => {
            client
                .cluster_remove_node(rpc::ClusterRemoveNodeRequest { node_id })
                .await?;
            println!("ok (node marked decommissioning)");
        }
        Command::SplitMeta {
            split_hash,
            target_meta_index,
        } => {
            let resp = client
                .cluster_split_meta_range(rpc::ClusterSplitMetaRangeRequest {
                    split_hash,
                    target_meta_index,
                })
                .await?
                .into_inner();
            println!(
                "ok left_meta_index={} right_meta_index={}",
                resp.left_meta_index, resp.right_meta_index
            );
        }
        Command::MetaRebalance {
            meta_range_id,
            replicas,
            leaseholder,
        } => {
            if replicas.is_empty() {
                anyhow::bail!("at least one --replica is required");
            }
            client
                .cluster_meta_rebalance(rpc::ClusterMetaRebalanceRequest {
                    meta_range_id,
                    replicas,
                    leaseholder,
                })
                .await?;
            println!("ok (meta rebalance step accepted)");
        }
        Command::Split { split_key, hex } => {
            let key_bytes = if hex {
                parse_hex(&split_key)?
            } else {
                split_key.into_bytes()
            };
            let resp = client
                .range_split(rpc::RangeSplitRequest {
                    split_key: key_bytes.into(),
                })
                .await?
                .into_inner();
            println!(
                "ok left_shard_id={} right_shard_id={}",
                resp.left_shard_id, resp.right_shard_id
            );
        }
        Command::Merge {
            left_shard_id,
            pause,
            resume,
            cancel,
        } => {
            let mut mode_count = 0u8;
            if pause {
                mode_count += 1;
            }
            if resume {
                mode_count += 1;
            }
            if cancel {
                mode_count += 1;
            }
            if mode_count > 1 {
                anyhow::bail!("use at most one of --pause, --resume, --cancel");
            }
            let action = if pause {
                rpc::RangeMergeAction::RANGE_MERGE_ACTION_PAUSE
            } else if resume {
                rpc::RangeMergeAction::RANGE_MERGE_ACTION_RESUME
            } else if cancel {
                rpc::RangeMergeAction::RANGE_MERGE_ACTION_CANCEL
            } else {
                rpc::RangeMergeAction::RANGE_MERGE_ACTION_START
            };
            let resp = client
                .range_merge(rpc::RangeMergeRequest {
                    left_shard_id,
                    action,
                })
                .await?
                .into_inner();
            if resp.message.is_empty() {
                println!("ok");
            } else {
                println!("ok ({})", resp.message);
            }
        }
        Command::Rebalance {
            shard_id,
            replicas,
            leaseholder,
        } => {
            if replicas.is_empty() {
                anyhow::bail!("at least one --replica is required");
            }
            client
                .range_rebalance(rpc::RangeRebalanceRequest {
                    shard_id,
                    replicas,
                    leaseholder,
                })
                .await?;
            println!("ok (rebalance step accepted)");
        }
        Command::Freeze { frozen } => {
            client
                .cluster_freeze(rpc::ClusterFreezeRequest { frozen })
                .await?;
            println!("ok");
        }
        Command::Checkpoint { action } => {
            let rpc_action = match action {
                CheckpointAction::Status => {
                    rpc::ClusterCheckpointAction::CLUSTER_CHECKPOINT_ACTION_STATUS
                }
                CheckpointAction::Pause => {
                    rpc::ClusterCheckpointAction::CLUSTER_CHECKPOINT_ACTION_PAUSE
                }
                CheckpointAction::Resume => {
                    rpc::ClusterCheckpointAction::CLUSTER_CHECKPOINT_ACTION_RESUME
                }
                CheckpointAction::Trigger => {
                    rpc::ClusterCheckpointAction::CLUSTER_CHECKPOINT_ACTION_TRIGGER
                }
            };
            let resp = client
                .cluster_checkpoint_control(rpc::ClusterCheckpointControlRequest {
                    action: rpc_action,
                })
                .await?
                .into_inner();
            println!(
                "ok ({}) paused={} success={} failures={} manual_triggers={} manual_failures={} pressure_skips={} manifest_parse_errors={} blocked_groups={} max_lag_entries={} last_attempt_ms={} last_success_ms={} last_run_reason={} last_free_bytes={} last_free_pct={:.2} last_error={}",
                resp.message,
                resp.paused,
                resp.checkpoint_successes,
                resp.checkpoint_failures,
                resp.checkpoint_manual_triggers,
                resp.checkpoint_manual_trigger_failures,
                resp.checkpoint_pressure_skips,
                resp.checkpoint_manifest_parse_errors,
                resp.blocked_groups,
                resp.max_lag_entries,
                resp.last_attempt_ms,
                resp.last_success_ms,
                if resp.last_run_reason.is_empty() {
                    "-"
                } else {
                    &resp.last_run_reason
                },
                resp.last_free_bytes,
                resp.last_free_pct,
                if resp.last_error.is_empty() {
                    "-"
                } else {
                    &resp.last_error
                }
            );
        }
    }

    Ok(())
}

fn parse_hex(input: &str) -> anyhow::Result<Vec<u8>> {
    if input.len() % 2 != 0 {
        anyhow::bail!("hex string must have even length");
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = hex_value(bytes[i])?;
        let lo = hex_value(bytes[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_value(byte: u8) -> anyhow::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => anyhow::bail!("invalid hex character"),
    }
}

async fn fetch_range_stats(target: &str) -> anyhow::Result<HashMap<u64, u64>> {
    let addr: SocketAddr = target
        .parse()
        .with_context(|| format!("invalid grpc address: {target}"))?;
    let client = rpc::HoloRpcClientBuilder::new("holo_store.rpc.HoloRpc")
        .address(volo::net::Address::from(addr))
        .build();
    let resp = client
        .range_stats(rpc::RangeStatsRequest {})
        .await?
        .into_inner();
    let mut out = HashMap::new();
    for range in resp.ranges {
        out.insert(range.shard_id, range.record_count);
    }
    Ok(out)
}

async fn fetch_cluster_state(target: &str) -> anyhow::Result<ClusterStateView> {
    let addr: SocketAddr = target
        .parse()
        .with_context(|| format!("invalid grpc address: {target}"))?;
    let client = rpc::HoloRpcClientBuilder::new("holo_store.rpc.HoloRpc")
        .address(volo::net::Address::from(addr))
        .build();
    let resp = client
        .cluster_state(rpc::ClusterStateRequest {})
        .await?
        .into_inner();
    serde_json::from_str(&resp.json).context("parse cluster state json")
}

async fn fetch_split_health_for_range_controller(
    state: &ClusterStateView,
) -> anyhow::Result<SplitHealthView> {
    let Some(lease) = state.controller_leases.get("range") else {
        return Ok(state.split_health.clone());
    };
    let Some(holder) = state
        .members
        .values()
        .find(|member| member.node_id == lease.holder && member.state != "Removed")
    else {
        return Ok(state.split_health.clone());
    };
    let holder_state = fetch_cluster_state(holder.grpc_addr.as_str()).await?;
    Ok(holder_state.split_health)
}

fn format_range(start: &[u8], end: &[u8]) -> String {
    if start.is_empty() && end.is_empty() {
        return "all_keys".to_string();
    }
    format!(
        "[{}, {})",
        format_key_bound(start, true),
        format_key_bound(end, false)
    )
}

fn format_move_status(
    node_id: u64,
    shard_id: u64,
    moves_by_shard: &HashMap<u64, ReplicaMoveView>,
) -> String {
    let Some(mv) = moves_by_shard.get(&shard_id) else {
        return String::new();
    };
    if node_id == mv.to_node {
        format!("in:{}", mv.phase)
    } else if node_id == mv.from_node {
        format!("out:{}", mv.phase)
    } else {
        String::new()
    }
}

fn format_meta_move_status(
    meta_range_id: u64,
    moves_by_range: &BTreeMap<String, ReplicaMoveView>,
) -> String {
    let Some(mv) = moves_by_range.get(&meta_range_id.to_string()) else {
        return String::new();
    };
    format!("{}->{}:{}", mv.from_node, mv.to_node, mv.phase)
}

fn format_merge_status(merge: &RangeMergeView, is_left: bool) -> String {
    let side = if is_left { "left" } else { "right" };
    let mut parts = vec![format!("{side}:{}", merge.phase)];
    if merge.paused {
        parts.push("paused".to_string());
    }
    if merge.copied_rows > 0 {
        parts.push(format!("rows={}", merge.copied_rows));
    }
    if merge.copied_bytes > 0 {
        parts.push(format!("bytes={}", merge.copied_bytes));
    }
    if merge.lag_ops > 0 {
        parts.push(format!("lag={}", merge.lag_ops));
    }
    if merge.retry_count > 0 {
        parts.push(format!("retries={}", merge.retry_count));
    }
    if merge.eta_seconds > 0 {
        parts.push(format!("eta={}s", merge.eta_seconds));
    }
    if !merge.last_error.is_empty() {
        parts.push(format!("err={}", merge.last_error));
    }
    parts.join(" ")
}

fn format_row_state(merge_status: &str, move_status: &str) -> String {
    let merge_active = !merge_status.is_empty();
    let move_active = !move_status.is_empty();
    match (merge_active, move_active) {
        (false, false) => String::new(),
        (true, false) => merge_status.to_string(),
        (false, true) => move_status.to_string(),
        // Merge/move are intended to be mutually exclusive for a shard.
        // Keep both values visible if this invariant is ever violated.
        (true, true) => format!("BUG merge={merge_status} move={move_status}"),
    }
}

fn format_key_bound(key: &[u8], is_start: bool) -> String {
    if key.is_empty() {
        return if is_start {
            "start".to_string()
        } else {
            "end".to_string()
        };
    }
    if let Ok(s) = std::str::from_utf8(key) {
        if s.chars().all(|c| !c.is_control()) {
            return truncate_middle(s, DEFAULT_RANGE_BOUND_MAX_CHARS);
        }
    }
    let hex = format!("0x{}", hex_encode(key));
    truncate_middle(&hex, DEFAULT_RANGE_BOUND_MAX_CHARS)
}

fn format_hash_range(start: u64, end: u64) -> String {
    if start == 0 && end == u64::MAX {
        return "all_hashes".to_string();
    }
    format!("[{}, {}]", start, end)
}

fn join_ids(ids: &[u64]) -> String {
    ids.iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from(b"0123456789abcdef"[(b >> 4) as usize]));
        out.push(char::from(b"0123456789abcdef"[(b & 0x0f) as usize]));
    }
    out
}

fn shard_record_count(
    node_id: u64,
    shard_id: u64,
    counts_by_node: &HashMap<u64, HashMap<u64, u64>>,
    fetch_errors: &HashMap<u64, String>,
) -> Option<u64> {
    if fetch_errors.contains_key(&node_id) {
        return None;
    }
    counts_by_node
        .get(&node_id)
        .and_then(|counts| counts.get(&shard_id))
        .copied()
}

fn format_state_counts(counts: &BTreeMap<String, usize>) -> String {
    if counts.is_empty() {
        return "-".to_string();
    }
    counts
        .iter()
        .map(|(state, count)| format!("{state}: {}", format_count_usize(*count)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_total_count(total: u64, complete: bool) -> String {
    if complete {
        format_count_u64(total)
    } else {
        format!("{} (partial)", format_count_u64(total))
    }
}

fn format_top_reasons(reasons: &BTreeMap<String, u64>) -> String {
    if reasons.is_empty() {
        return "-".to_string();
    }
    let mut entries = reasons
        .iter()
        .map(|(reason, count)| (reason.clone(), *count))
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    entries
        .into_iter()
        .take(2)
        .map(|(reason, count)| format!("{reason}={}", format_count_u64(count)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn truncate_middle(input: &str, max_chars: usize) -> String {
    let char_count = input.chars().count();
    if char_count <= max_chars {
        return input.to_string();
    }
    if max_chars <= 3 {
        return ".".repeat(max_chars);
    }
    let keep = max_chars - 3;
    let head = (keep + 1) / 2;
    let tail = keep / 2;
    let head_str = input.chars().take(head).collect::<String>();
    let tail_str = input
        .chars()
        .rev()
        .take(tail)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("{head_str}...{tail_str}")
}

fn format_count_usize(value: usize) -> String {
    format_count_u64(value as u64)
}

fn format_count_u64(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    let head = if digits.len() % 3 == 0 {
        3
    } else {
        digits.len() % 3
    };
    out.push_str(&digits[..head]);
    let mut idx = head;
    while idx < digits.len() {
        out.push(',');
        out.push_str(&digits[idx..idx + 3]);
        idx += 3;
    }
    out
}

fn print_ascii_table_with_footer(
    headers: &[&str],
    rows: &[Vec<String>],
    footer_rows: &[Vec<String>],
) {
    if footer_rows.is_empty() {
        print_ascii_table(headers, rows);
        return;
    }
    let mut widths = headers.iter().map(|h| h.len()).collect::<Vec<_>>();
    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx >= widths.len() {
                widths.push(cell.len());
            } else {
                widths[idx] = widths[idx].max(cell.len());
            }
        }
    }
    for row in footer_rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx >= widths.len() {
                widths.push(cell.len());
            } else {
                widths[idx] = widths[idx].max(cell.len());
            }
        }
    }
    let separator = {
        let mut s = String::from("+");
        for w in &widths {
            s.push_str(&"-".repeat(*w + 2));
            s.push('+');
        }
        s
    };
    println!("{separator}");
    print!("|");
    for (idx, header) in headers.iter().enumerate() {
        print!(" {:width$} |", header, width = widths[idx]);
    }
    println!();
    println!("{separator}");
    for row in rows {
        print!("|");
        for (idx, cell) in row.iter().enumerate() {
            print!(" {:width$} |", cell, width = widths[idx]);
        }
        println!();
    }
    println!("{separator}");
    for (row_idx, row) in footer_rows.iter().enumerate() {
        print!("|");
        for (idx, width) in widths.iter().enumerate() {
            let cell = row.get(idx).map(String::as_str).unwrap_or("");
            print!(" {:width$} |", cell, width = width);
        }
        println!();
        if row_idx + 1 < footer_rows.len() {
            println!("{separator}");
        }
    }
    println!("{separator}");
}

fn print_ascii_table(headers: &[&str], rows: &[Vec<String>]) {
    let mut widths = headers.iter().map(|h| h.len()).collect::<Vec<_>>();
    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx >= widths.len() {
                widths.push(cell.len());
            } else {
                widths[idx] = widths[idx].max(cell.len());
            }
        }
    }

    let separator = {
        let mut s = String::from("+");
        for w in &widths {
            s.push_str(&"-".repeat(*w + 2));
            s.push('+');
        }
        s
    };

    println!("{separator}");
    print!("|");
    for (idx, header) in headers.iter().enumerate() {
        print!(" {:width$} |", header, width = widths[idx]);
    }
    println!();
    println!("{separator}");
    for row in rows {
        print!("|");
        for (idx, cell) in row.iter().enumerate() {
            print!(" {:width$} |", cell, width = widths[idx]);
        }
        println!();
    }
    println!("{separator}");
}
