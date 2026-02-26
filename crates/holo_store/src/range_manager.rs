//! Background range management: split/merge planning plus split execution.
//!
//! Design overview for pluggable split strategies:
//! - `SplitStrategyEngine` is the strategy interface boundary used by the loop.
//! - `Loadops` preserves the original load-first/size-second behavior for baseline comparisons.
//! - `Adaptive` consumes richer cluster telemetry (size/load/latency/hot-key signals),
//!   selects weighted split points, and uses staged backfill-aware execution.
//!
//! Runtime safety model:
//! - All split cutovers are coordinated with shard fences in the range controller domain.
//! - Only affected shards are paused while metadata + migration converge.
//! - Failure handling uses per-shard backoff and distinguishes transient lease-loss aborts.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fjall::{Keyspace, PartitionCreateOptions};
use holo_accord::accord::NodeId;
use serde::Deserialize;
use volo::net::Address;

use crate::cluster::{ClusterCommand, ClusterState, ControllerDomain, MemberState, ShardDesc};
use crate::load::ShardLoadSnapshot;
use crate::volo_gen::holo_store::rpc;
use crate::NodeState;

const SPLIT_FENCE_PREFIX: &str = "split-op";
const SPLIT_FENCE_STALE_TIMEOUT: Duration = Duration::from_secs(20);
const SPLIT_SHARD_DRAIN_LOW_WATERMARK: u64 = 8;
const SPLIT_SHARD_DRAIN_STABLE_FOR: Duration = Duration::from_millis(100);
const SPLIT_SHARD_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const SPLIT_FENCE_SYNC_TIMEOUT: Duration = Duration::from_secs(15);
const SPLIT_CLUSTER_SYNC_TIMEOUT: Duration = Duration::from_secs(15);
const SPLIT_FAILURE_BACKOFF_BASE: Duration = Duration::from_millis(250);
const SPLIT_FAILURE_BACKOFF_MAX: Duration = Duration::from_secs(15);
const SPLIT_FAILURE_BACKOFF_MAX_SHIFT: u32 = 6;
const SPLIT_KEY_SAMPLE_SIZE: usize = 256;
static SPLIT_TOKEN_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy)]
/// Per-shard retry state for failed split attempts.
///
/// Inputs:
/// - `failures`: number of consecutive failures recorded for a shard.
/// - `retry_after`: absolute wall-clock deadline when the shard is eligible again.
///
/// Output:
/// - Used by strategy evaluation to suppress candidates until backoff expires.
struct SplitFailureBackoff {
    failures: u32,
    retry_after: Instant,
}

#[derive(Debug, Clone)]
/// Metadata describing one fence epoch for a split workflow.
///
/// Inputs:
/// - `token`: unique operation identity.
/// - `started_ms`: operation start time in unix milliseconds.
/// - `reason`: encoded controller-visible reason written into fence metadata.
/// - `source_shard_id`: shard that is fenced for the split.
///
/// Output:
/// - Provides deterministic fence matching/cleanup and stale-fence recovery.
struct SplitFenceOp {
    token: String,
    started_ms: u64,
    reason: String,
    source_shard_id: u64,
}

impl SplitFenceOp {
    /// Build a unique split fence descriptor for one split attempt.
    ///
    /// Inputs:
    /// - `node_id`: node proposing the split.
    /// - `source_shard_id`: shard being split.
    ///
    /// Output:
    /// - `SplitFenceOp` with a stable `reason` string persisted in cluster fences.
    fn new(node_id: u64, source_shard_id: u64) -> Self {
        let started_ms = now_unix_ms();
        let seq = SPLIT_TOKEN_SEQ.fetch_add(1, Ordering::Relaxed);
        let token = format!("n{node_id}-{started_ms}-{seq}");
        let reason = encode_split_fence_reason(&token, started_ms);
        Self {
            token,
            started_ms,
            reason,
            source_shard_id,
        }
    }
}

#[derive(Debug, Clone)]
/// Fully resolved split plan returned by a strategy implementation.
///
/// Inputs:
/// - Candidate shard and selected split key.
/// - Placement/migration policy knobs chosen by strategy.
///
/// Output:
/// - Consumed by the execution pipeline (`maybe_split_once`) to run the split.
struct SplitDecision {
    reason: String,
    shard: ShardDesc,
    split_key: Vec<u8>,
    target_shard_index: usize,
    target_replicas: Option<Vec<NodeId>>,
    target_leaseholder: Option<NodeId>,
    skip_migration: bool,
    staged_backfill: bool,
}

#[derive(Debug, Default)]
/// Result envelope returned by strategy evaluation.
///
/// Inputs:
/// - Strategy-internal candidate search and telemetry snapshots.
///
/// Output:
/// - Optional split decision plus per-shard write QPS used by merge logic.
struct SplitStrategyEvaluation {
    decision: Option<SplitDecision>,
    qps_by_idx: Vec<u64>,
}

#[derive(Debug)]
/// Original split strategy state ("loadops"): load-first, then ops-since-baseline.
///
/// Inputs:
/// - Node-local `ShardLoadSnapshot` deltas.
/// - Baseline set-op counters per shard index.
///
/// Output:
/// - Maintains sustained/cooldown windows and emits at most one split candidate.
struct LoadOpsSplitStrategy {
    last: ShardLoadSnapshot,
    baseline_set_ops: Vec<u64>,
    sustained: Vec<u8>,
    cooldown_until: Vec<Instant>,
    qps_by_idx: Vec<u64>,
}

#[derive(Debug, Default, Clone, Copy)]
/// Last-seen cumulative telemetry totals for one `(node, shard)` stream.
///
/// Inputs:
/// - Monotonic counters from `RangeTelemetryStat`.
///
/// Output:
/// - Enables delta-to-rate conversion (QPS/BPS) for adaptive scoring.
struct AdaptiveNodeTotals {
    write_ops_total: u64,
    read_ops_total: u64,
    write_bytes_total: u64,
    observed_unix_ms: u64,
}

#[derive(Debug)]
/// Adaptive split strategy state.
///
/// Inputs:
/// - Cluster telemetry cache and per-stream cumulative baselines.
/// - Per-shard sustained-score streaks and cooldown deadlines.
///
/// Output:
/// - Produces score-ranked split decisions with staged backfill enabled.
struct AdaptiveSplitStrategy {
    last_refresh: Instant,
    last_totals_by_node_shard: std::collections::BTreeMap<(NodeId, u64), AdaptiveNodeTotals>,
    telemetry_cache: std::collections::BTreeMap<NodeId, Vec<crate::transport::RangeTelemetryStat>>,
    sustained: std::collections::BTreeMap<u64, u8>,
    cooldown_until: std::collections::BTreeMap<u64, Instant>,
    qps_by_idx: Vec<u64>,
}

#[derive(Debug)]
/// Pluggable split strategy interface used by the range manager loop.
///
/// Design:
/// - Keeps strategy-specific mutable state behind one enum.
/// - Exposes a uniform `evaluate` + `on_split_success` surface to the executor.
///
/// Output:
/// - Encapsulates strategy selection without adding dynamic dispatch overhead.
enum SplitStrategyEngine {
    Loadops(LoadOpsSplitStrategy),
    Adaptive(AdaptiveSplitStrategy),
}

impl SplitStrategyEngine {
    /// Construct the selected split strategy engine from runtime config.
    ///
    /// Inputs:
    /// - `state`: node runtime used to seed strategy state.
    /// - `cfg`: split strategy selection and thresholds.
    ///
    /// Output:
    /// - Initialized `SplitStrategyEngine` variant with per-strategy state.
    fn new(state: Arc<NodeState>, cfg: RangeManagerConfig) -> Self {
        match cfg.split_strategy {
            crate::RangeSplitStrategy::Loadops => {
                let last = state.shard_load.snapshot();
                let shards = state.shard_load.shards();
                Self::Loadops(LoadOpsSplitStrategy {
                    baseline_set_ops: last.set_ops.clone(),
                    qps_by_idx: vec![0; shards],
                    last,
                    sustained: vec![0; shards],
                    cooldown_until: vec![Instant::now(); shards],
                })
            }
            crate::RangeSplitStrategy::Adaptive => Self::Adaptive(AdaptiveSplitStrategy {
                last_refresh: Instant::now()
                    .checked_sub(cfg.adaptive_stats_refresh)
                    .unwrap_or_else(Instant::now),
                last_totals_by_node_shard: std::collections::BTreeMap::new(),
                telemetry_cache: std::collections::BTreeMap::new(),
                sustained: std::collections::BTreeMap::new(),
                cooldown_until: std::collections::BTreeMap::new(),
                qps_by_idx: vec![0; state.data_shards.max(1)],
            }),
        }
    }

    /// Evaluate split candidates with the configured strategy implementation.
    ///
    /// Inputs:
    /// - Runtime state, keyspace, cluster snapshot, config, and retry constraints.
    /// - `target_idx`: free shard index reserved for this evaluation cycle.
    ///
    /// Output:
    /// - `SplitStrategyEvaluation` containing optional decision + per-shard QPS.
    async fn evaluate(
        &mut self,
        state: Arc<NodeState>,
        keyspace: &Keyspace,
        cluster_state: &ClusterState,
        cfg: RangeManagerConfig,
        split_failure_backoff: &std::collections::BTreeMap<u64, SplitFailureBackoff>,
        target_idx: usize,
        now: Instant,
    ) -> anyhow::Result<SplitStrategyEvaluation> {
        // Delegate to the configured strategy while keeping a stable result shape
        // for the split/merge execution loop.
        match self {
            SplitStrategyEngine::Loadops(loadops) => loadops.evaluate(
                state,
                keyspace,
                &cluster_state.shards,
                cfg,
                split_failure_backoff,
                target_idx,
                now,
            ),
            SplitStrategyEngine::Adaptive(adaptive) => {
                adaptive
                    .evaluate(
                        state,
                        keyspace,
                        cluster_state,
                        cfg,
                        split_failure_backoff,
                        target_idx,
                        now,
                    )
                    .await
            }
        }
    }

    /// Notify strategy state after a successful split commit.
    ///
    /// Inputs:
    /// - `source_shard`: shard that was split.
    /// - `target_idx`: newly allocated shard index.
    /// - `cfg`/`now`: used to apply cooldown and reset sustained counters.
    ///
    /// Output:
    /// - Updates strategy-local baselines/cooldowns for future evaluations.
    fn on_split_success(
        &mut self,
        source_shard: &ShardDesc,
        target_idx: usize,
        cfg: RangeManagerConfig,
        now: Instant,
    ) {
        // Allow each strategy to update post-split baselines/cooldowns according
        // to its own state model.
        match self {
            SplitStrategyEngine::Loadops(loadops) => {
                if let Some(slot) = loadops.cooldown_until.get_mut(source_shard.shard_index) {
                    *slot = now + cfg.split_cooldown;
                }
                if source_shard.shard_index < loadops.baseline_set_ops.len() {
                    loadops.baseline_set_ops[source_shard.shard_index] = loadops
                        .last
                        .set_ops
                        .get(source_shard.shard_index)
                        .copied()
                        .unwrap_or(loadops.baseline_set_ops[source_shard.shard_index]);
                }
                if target_idx < loadops.baseline_set_ops.len() {
                    loadops.baseline_set_ops[target_idx] =
                        loadops.last.set_ops.get(target_idx).copied().unwrap_or(0);
                }
                if source_shard.shard_index < loadops.sustained.len() {
                    loadops.sustained[source_shard.shard_index] = 0;
                }
            }
            SplitStrategyEngine::Adaptive(adaptive) => {
                let deadline = now + cfg.split_cooldown;
                adaptive
                    .cooldown_until
                    .insert(source_shard.shard_id, deadline);
                adaptive.sustained.remove(&source_shard.shard_id);
            }
        }
    }
}

impl LoadOpsSplitStrategy {
    /// Evaluate one split tick using the legacy loadops heuristic.
    ///
    /// Inputs:
    /// - `state`: local load counters.
    /// - `keyspace`: used to derive a concrete split key when a shard is selected.
    /// - `shards`: current ordered range descriptors.
    /// - `cfg`: loadops thresholds/cooldowns.
    /// - `split_failure_backoff`: shards temporarily blocked from retry.
    /// - `target_idx`: free shard index reserved for a possible split.
    /// - `now`: evaluation timestamp for cooldown checks.
    ///
    /// Output:
    /// - `SplitStrategyEvaluation` with optional decision and per-shard write QPS.
    fn evaluate(
        &mut self,
        state: Arc<NodeState>,
        keyspace: &Keyspace,
        shards: &[ShardDesc],
        cfg: RangeManagerConfig,
        split_failure_backoff: &std::collections::BTreeMap<u64, SplitFailureBackoff>,
        target_idx: usize,
        now: Instant,
    ) -> anyhow::Result<SplitStrategyEvaluation> {
        let mut qps_by_idx = Vec::new();
        let decision = if let Some((reason, shard)) = pick_split_candidate(
            state,
            shards,
            cfg,
            &mut self.last,
            &mut self.baseline_set_ops,
            &mut self.sustained,
            &mut self.cooldown_until,
            split_failure_backoff,
            &mut qps_by_idx,
            now,
        )? {
            if let Some(split_key) = pick_split_key_from_range(keyspace, &shard)? {
                Some(SplitDecision {
                    reason: reason.to_string(),
                    shard,
                    split_key,
                    target_shard_index: target_idx,
                    target_replicas: None,
                    target_leaseholder: None,
                    skip_migration: false,
                    staged_backfill: false,
                })
            } else {
                if shard.shard_index < self.sustained.len() {
                    self.sustained[shard.shard_index] = 0;
                }
                None
            }
        } else {
            None
        };
        self.qps_by_idx = qps_by_idx.clone();
        Ok(SplitStrategyEvaluation {
            decision,
            qps_by_idx,
        })
    }
}

#[derive(Debug, Default, Clone)]
/// Per-shard aggregated telemetry used by adaptive stress scoring.
///
/// Inputs:
/// - Summed/merged signals across all reporting replicas of a shard.
///
/// Output:
/// - Single normalized signal record per shard for candidate ranking.
struct AdaptiveShardSignals {
    record_count: u64,
    write_qps: u64,
    read_qps: u64,
    write_bps: u64,
    queue_depth: u64,
    write_tail_latency_ms: f64,
    hot_key_concentration_bps: u32,
    hot_bucket_weights: Vec<u64>,
}

impl AdaptiveSplitStrategy {
    /// Evaluate one split tick using the adaptive hybrid score.
    ///
    /// Inputs:
    /// - Cluster state + telemetry cache + adaptive thresholds.
    /// - Split failure backoff/cooldown state.
    ///
    /// Output:
    /// - Highest-scoring eligible split decision, or `None` when no shard qualifies.
    async fn evaluate(
        &mut self,
        state: Arc<NodeState>,
        keyspace: &Keyspace,
        cluster_state: &ClusterState,
        cfg: RangeManagerConfig,
        split_failure_backoff: &std::collections::BTreeMap<u64, SplitFailureBackoff>,
        target_idx: usize,
        now: Instant,
    ) -> anyhow::Result<SplitStrategyEvaluation> {
        if now.duration_since(self.last_refresh) >= cfg.adaptive_stats_refresh {
            self.telemetry_cache = state.cluster_range_telemetry().await;
            self.last_refresh = now;
        }

        let mut signals_by_shard = self.aggregate_signals(cluster_state, now_unix_ms(), cfg);
        let mut qps_by_idx = vec![0u64; state.data_shards.max(1)];
        for shard in &cluster_state.shards {
            if let Some(signals) = signals_by_shard.get(&shard.shard_id) {
                if shard.shard_index < qps_by_idx.len() {
                    qps_by_idx[shard.shard_index] = signals.write_qps;
                }
            }
        }
        self.qps_by_idx = qps_by_idx.clone();

        let mut best: Option<(f64, SplitDecision)> = None;
        let clear_threshold =
            cfg.adaptive_score_threshold * (1.0 - (cfg.adaptive_hysteresis_pct as f64 / 100.0));
        self.cooldown_until.retain(|_, until| *until > now);

        for shard in &cluster_state.shards {
            if split_failure_backoff
                .get(&shard.shard_id)
                .map(|entry| entry.retry_after > now)
                .unwrap_or(false)
            {
                continue;
            }
            if self
                .cooldown_until
                .get(&shard.shard_id)
                .map(|until| *until > now)
                .unwrap_or(false)
            {
                continue;
            }
            let Some(signals) = signals_by_shard.get_mut(&shard.shard_id) else {
                continue;
            };
            let min_keys_for_split = cfg.split_min_keys.max(1) as u64;
            if signals.record_count < min_keys_for_split {
                self.sustained.remove(&shard.shard_id);
                continue;
            }
            let approx_bytes = estimate_shard_logical_bytes(signals, cfg);
            let score = adaptive_stress_score(signals, approx_bytes, cfg);

            if signals.hot_key_concentration_bps >= cfg.adaptive_hot_key_bps {
                self.sustained.remove(&shard.shard_id);
                continue;
            }
            if score >= cfg.adaptive_score_threshold {
                let streak = self.sustained.entry(shard.shard_id).or_insert(0);
                *streak = streak.saturating_add(1);
            } else if score < clear_threshold {
                self.sustained.remove(&shard.shard_id);
                continue;
            }
            if self.sustained.get(&shard.shard_id).copied().unwrap_or(0)
                < cfg.adaptive_sustain.max(1)
            {
                continue;
            }

            let min_side_keys = adaptive_min_side_keys(cfg);
            let Some(split_key) = pick_split_key_load_weighted(
                keyspace,
                shard,
                signals.hot_bucket_weights.as_slice(),
                min_side_keys,
            )?
            else {
                self.sustained.remove(&shard.shard_id);
                continue;
            };
            let (target_replicas, target_leaseholder) =
                plan_split_target_placement(cluster_state, shard);
            let decision = SplitDecision {
                reason: "adaptive".to_string(),
                shard: shard.clone(),
                split_key,
                target_shard_index: target_idx,
                target_replicas: Some(target_replicas),
                target_leaseholder: Some(target_leaseholder),
                // Keep authoritative per-node migration on cutover so source
                // shards are cleaned up and replica-local key counts converge.
                skip_migration: false,
                staged_backfill: true,
            };
            match &best {
                Some((best_score, _)) if *best_score >= score => {}
                _ => best = Some((score, decision)),
            }
        }

        Ok(SplitStrategyEvaluation {
            decision: best.map(|(_, d)| d),
            qps_by_idx,
        })
    }

    /// Merge per-node shard telemetry into shard-level adaptive signals.
    ///
    /// Inputs:
    /// - `cluster_state`: authoritative live shard set.
    /// - `now_ms`: timestamp used for delta-rate calculations.
    /// - `cfg`: fallback timing interval when no prior sample exists.
    ///
    /// Output:
    /// - Map `shard_id -> AdaptiveShardSignals` ready for stress scoring.
    fn aggregate_signals(
        &mut self,
        cluster_state: &ClusterState,
        now_ms: u64,
        cfg: RangeManagerConfig,
    ) -> std::collections::BTreeMap<u64, AdaptiveShardSignals> {
        let mut out = std::collections::BTreeMap::<u64, AdaptiveShardSignals>::new();
        let mut seen_keys = std::collections::BTreeSet::<(NodeId, u64)>::new();

        for (node_id, stats) in &self.telemetry_cache {
            for stat in stats {
                if !cluster_state
                    .shards
                    .iter()
                    .any(|s| s.shard_id == stat.shard_id)
                {
                    continue;
                }
                let key = (*node_id, stat.shard_id);
                seen_keys.insert(key);
                let prev = self
                    .last_totals_by_node_shard
                    .get(&key)
                    .copied()
                    .unwrap_or_default();
                let dt_s = if prev.observed_unix_ms == 0 {
                    cfg.interval.as_secs_f64().max(0.001)
                } else {
                    ((now_ms.saturating_sub(prev.observed_unix_ms)) as f64 / 1000.0).max(0.001)
                };
                let write_qps = (stat.write_ops_total.saturating_sub(prev.write_ops_total) as f64
                    / dt_s) as u64;
                let read_qps =
                    (stat.read_ops_total.saturating_sub(prev.read_ops_total) as f64 / dt_s) as u64;
                let write_bps = (stat
                    .write_bytes_total
                    .saturating_sub(prev.write_bytes_total) as f64
                    / dt_s) as u64;

                self.last_totals_by_node_shard.insert(
                    key,
                    AdaptiveNodeTotals {
                        write_ops_total: stat.write_ops_total,
                        read_ops_total: stat.read_ops_total,
                        write_bytes_total: stat.write_bytes_total,
                        observed_unix_ms: now_ms,
                    },
                );

                let entry = out.entry(stat.shard_id).or_default();
                entry.record_count = entry.record_count.max(stat.record_count);
                entry.write_qps = entry.write_qps.saturating_add(write_qps);
                entry.read_qps = entry.read_qps.saturating_add(read_qps);
                entry.write_bps = entry.write_bps.saturating_add(write_bps);
                entry.queue_depth = entry.queue_depth.max(stat.queue_depth);
                entry.write_tail_latency_ms =
                    entry.write_tail_latency_ms.max(stat.write_tail_latency_ms);
                entry.hot_key_concentration_bps = entry
                    .hot_key_concentration_bps
                    .max(stat.hot_key_concentration_bps);

                let max_len = stat
                    .write_hot_buckets
                    .len()
                    .max(stat.read_hot_buckets.len())
                    .max(crate::load::HOT_KEY_BUCKETS);
                if entry.hot_bucket_weights.len() < max_len {
                    entry.hot_bucket_weights.resize(max_len, 0);
                }
                for idx in 0..max_len {
                    let w = stat.write_hot_buckets.get(idx).copied().unwrap_or(0);
                    let r = stat.read_hot_buckets.get(idx).copied().unwrap_or(0);
                    entry.hot_bucket_weights[idx] =
                        entry.hot_bucket_weights[idx].saturating_add(w.saturating_add(r));
                }
            }
        }

        self.last_totals_by_node_shard
            .retain(|key, _| seen_keys.contains(key));
        out
    }
}

/// Compute adaptive split stress score for one shard.
///
/// Inputs:
/// - `signals`: current shard load/latency/hot-key telemetry.
/// - `approx_bytes`: estimated logical size for the shard.
/// - `cfg`: target values used to normalize each signal dimension.
///
/// Output:
/// - Unitless score where values >= configured threshold become split-eligible.
fn adaptive_stress_score(
    signals: &AdaptiveShardSignals,
    approx_bytes: u64,
    cfg: RangeManagerConfig,
) -> f64 {
    let size = (approx_bytes as f64 / cfg.adaptive_target_bytes as f64).max(0.0);
    let write_qps = (signals.write_qps as f64 / cfg.adaptive_target_write_qps as f64).max(0.0);
    let read_qps = (signals.read_qps as f64 / cfg.adaptive_target_read_qps as f64).max(0.0);
    let write_bps = (signals.write_bps as f64 / cfg.adaptive_target_write_bps as f64).max(0.0);
    let queue = (signals.queue_depth as f64 / cfg.adaptive_target_queue_depth as f64).max(0.0);
    let latency = (signals.write_tail_latency_ms / cfg.adaptive_target_p99_ms).max(0.0);
    let hot = if cfg.adaptive_hot_key_bps == 0 {
        0.0
    } else {
        (signals.hot_key_concentration_bps as f64 / cfg.adaptive_hot_key_bps as f64).max(0.0)
    };
    // Weighted hybrid score tuned for write-heavy key/value workloads.
    (size * 0.22)
        + (write_qps * 0.23)
        + (read_qps * 0.08)
        + (write_bps * 0.20)
        + (queue * 0.17)
        + (latency * 0.08)
        + (hot * 0.02)
}

/// Compute the minimum allowed key count on both sides of a split.
///
/// Inputs:
/// - `cfg`: split sizing thresholds.
///
/// Output:
/// - Guardrail used by weighted split-key selection to prevent tiny tail shards.
fn adaptive_min_side_keys(cfg: RangeManagerConfig) -> u64 {
    // Keep both resulting ranges materially sized; this prevents split storms
    // that repeatedly carve tiny tail ranges during bursty workloads.
    let split_floor = cfg.split_min_keys.max(1) as u64;
    (split_floor / 2).max(8_000)
}

/// Estimate logical bytes for a shard from row count and write throughput.
///
/// Inputs:
/// - `signals`: shard-level telemetry.
/// - `cfg`: adaptive byte/key targets used when direct per-write size is unknown.
///
/// Output:
/// - Approximate logical bytes used as one component of adaptive stress score.
fn estimate_shard_logical_bytes(signals: &AdaptiveShardSignals, cfg: RangeManagerConfig) -> u64 {
    let avg_bytes_per_write = if signals.write_qps > 0 {
        (signals.write_bps / signals.write_qps).max(32)
    } else {
        (cfg.adaptive_target_bytes / cfg.split_min_keys.max(1) as u64).max(32)
    };
    signals.record_count.saturating_mul(avg_bytes_per_write)
}

/// Choose target replicas/leaseholder for the split RHS at split planning time.
///
/// Inputs:
/// - `state`: current cluster membership and shard placement.
/// - `source`: source shard descriptor being split.
///
/// Output:
/// - `(replicas, leaseholder)` for the target shard, biased toward lower load.
fn plan_split_target_placement(state: &ClusterState, source: &ShardDesc) -> (Vec<NodeId>, NodeId) {
    let mut active = state
        .members
        .iter()
        .filter_map(|(id, member)| (member.state == MemberState::Active).then_some(*id))
        .collect::<Vec<_>>();
    active.sort_unstable();
    if active.is_empty() {
        return (source.replicas.clone(), source.leaseholder);
    }

    let desired = state.replication_factor.min(active.len()).max(1);
    let mut replica_counts = std::collections::BTreeMap::<NodeId, usize>::new();
    let mut lease_counts = std::collections::BTreeMap::<NodeId, usize>::new();
    for node in &active {
        replica_counts.insert(*node, 0);
        lease_counts.insert(*node, 0);
    }
    for shard in &state.shards {
        for replica in &shard.replicas {
            if let Some(v) = replica_counts.get_mut(replica) {
                *v += 1;
            }
        }
        if let Some(v) = lease_counts.get_mut(&shard.leaseholder) {
            *v += 1;
        }
    }

    let mut ordered = active.clone();
    ordered.sort_by_key(|node| {
        (
            replica_counts.get(node).copied().unwrap_or(usize::MAX),
            lease_counts.get(node).copied().unwrap_or(usize::MAX),
            *node,
        )
    });
    let mut replicas = ordered.into_iter().take(desired).collect::<Vec<_>>();
    if replicas.is_empty() {
        replicas = source
            .replicas
            .iter()
            .copied()
            .take(desired)
            .collect::<Vec<_>>();
    }
    if replicas.is_empty() {
        replicas.push(source.leaseholder);
    }
    replicas.sort_unstable();
    replicas.dedup();
    let leaseholder = replicas
        .iter()
        .copied()
        .min_by_key(|node| lease_counts.get(node).copied().unwrap_or(usize::MAX))
        .unwrap_or(source.leaseholder);
    (replicas, leaseholder)
}

/// Configuration for the range manager.
///
/// Inputs:
/// - Operator-provided split/merge thresholds and strategy knobs.
///
/// Output:
/// - Immutable config snapshot used by the manager loop and strategy engines.
#[derive(Clone, Copy, Debug)]
pub struct RangeManagerConfig {
    /// Split strategy implementation.
    pub split_strategy: crate::RangeSplitStrategy,
    /// Evaluate split candidates at this interval.
    pub interval: Duration,
    /// Split when a range has at least this many "new" keys since its last split.
    ///
    /// Today this is implemented as an approximation: the range manager tracks
    /// SET ops per shard index and uses "SET ops since baseline" as a proxy for
    /// unique key growth. This avoids expensive keyspace scans on the write path.
    ///
    /// Set to 0 to disable size/growth-based splitting.
    pub split_min_keys: usize,
    /// Split when a range's sustained SET QPS exceeds this threshold.
    ///
    /// Set to 0 to disable load-based splitting (size-based splitting may still apply).
    pub split_min_qps: u64,
    /// Require QPS threshold for this many consecutive evaluations.
    pub split_qps_sustain: u8,
    /// Cooldown between splits for the same shard index.
    pub split_cooldown: Duration,
    /// Adaptive strategy: sustained score threshold intervals.
    pub adaptive_sustain: u8,
    /// Adaptive strategy: stress score threshold.
    pub adaptive_score_threshold: f64,
    /// Adaptive strategy: hysteresis percentage.
    pub adaptive_hysteresis_pct: u8,
    /// Adaptive strategy: hot-key concentration guard (basis points).
    pub adaptive_hot_key_bps: u32,
    /// Adaptive strategy: bytes target for stress scoring.
    pub adaptive_target_bytes: u64,
    /// Adaptive strategy: write-qps target for stress scoring.
    pub adaptive_target_write_qps: u64,
    /// Adaptive strategy: read-qps target for stress scoring.
    pub adaptive_target_read_qps: u64,
    /// Adaptive strategy: write-bytes-per-second target for stress scoring.
    pub adaptive_target_write_bps: u64,
    /// Adaptive strategy: queue depth target for stress scoring.
    pub adaptive_target_queue_depth: u64,
    /// Adaptive strategy: p99 latency target for stress scoring.
    pub adaptive_target_p99_ms: f64,
    /// Adaptive strategy: staged backfill throughput budget (bytes/s).
    pub adaptive_backfill_max_bps: u64,
    /// Adaptive strategy: staged backfill page limit.
    pub adaptive_backfill_page_limit: usize,
    /// Adaptive strategy: max pages in one staged pass.
    pub adaptive_backfill_max_pages: usize,
    /// Adaptive strategy: minimum telemetry refresh interval.
    pub adaptive_stats_refresh: Duration,
    /// Adaptive strategy split budget.
    pub adaptive_max_concurrent_splits: usize,
    /// Merge adjacent ranges when their combined key count is below this threshold.
    ///
    /// Set to 0 to disable automatic merge proposals.
    pub merge_max_keys: usize,
    /// Cooldown after merge proposals to avoid split/merge oscillation.
    pub merge_cooldown: Duration,
    /// Maximum combined sustained SET QPS for an adjacent pair to be eligible
    /// for automatic merge. Set to 0 to disable QPS-based cold gating.
    pub merge_max_qps: u64,
    /// Require low combined QPS for this many consecutive evaluations before
    /// proposing a merge.
    pub merge_qps_sustain: u8,
    /// Hysteresis for merge key threshold as a percentage of `merge_max_keys`
    /// (1-100). Lower values reduce split/merge oscillation.
    pub merge_key_hysteresis_pct: u8,
}

/// Spawn the range manager background loop.
///
/// Inputs:
/// - `state`: node runtime with cluster metadata, telemetry, and proposal APIs.
/// - `keyspace`: storage handle used by split-key selection and key counting.
/// - `cfg`: split/merge strategy configuration.
///
/// Output:
/// - Detached Tokio task that continuously evaluates split/merge opportunities.
pub fn spawn(state: Arc<NodeState>, keyspace: Arc<Keyspace>, cfg: RangeManagerConfig) {
    if state.data_shards <= 1 {
        return;
    }
    if !matches!(state.routing_mode, crate::RoutingMode::Range) {
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(cfg.interval);
        let mut split_strategy = SplitStrategyEngine::new(state.clone(), cfg);
        let mut split_failure_backoff =
            std::collections::BTreeMap::<u64, SplitFailureBackoff>::new();
        let mut merge_cooldown_until = std::collections::BTreeMap::<u64, std::time::Instant>::new();
        let mut merge_sustained = std::collections::BTreeMap::<u64, u8>::new();
        loop {
            ticker.tick().await;
            if !state
                .ensure_controller_leader(ControllerDomain::Range)
                .await
            {
                continue;
            }
            if let Err(err) = recover_stale_split_fences(state.clone()).await {
                tracing::warn!(error = ?err, "range manager stale split-fence recovery failed");
            }
            let now = std::time::Instant::now();
            if let Err(err) = maybe_split_once(
                state.clone(),
                keyspace.clone(),
                cfg,
                &mut split_strategy,
                &mut split_failure_backoff,
                &mut merge_cooldown_until,
                &mut merge_sustained,
                now,
            )
            .await
            {
                tracing::warn!(error = ?err, "range manager split attempt failed");
            }
        }
    });
}

/// Run one manager iteration: at most one split or one merge proposal.
///
/// Inputs:
/// - Current cluster snapshot, strategy state, and backoff/cooldown maps.
///
/// Output:
/// - `Ok(())` for no-op/success, or error when a non-transient split stage fails.
async fn maybe_split_once(
    state: Arc<NodeState>,
    keyspace: Arc<Keyspace>,
    cfg: RangeManagerConfig,
    split_strategy: &mut SplitStrategyEngine,
    split_failure_backoff: &mut std::collections::BTreeMap<u64, SplitFailureBackoff>,
    merge_cooldown_until: &mut std::collections::BTreeMap<u64, std::time::Instant>,
    merge_sustained: &mut std::collections::BTreeMap<u64, u8>,
    now: std::time::Instant,
) -> anyhow::Result<()> {
    if state.cluster_store.frozen() {
        return Ok(());
    }

    let cluster_state = state.cluster_store.state();
    // Keep split operations out of the way while replica moves are in
    // progress; rebalancing has higher correctness priority.
    if !cluster_state.shard_rebalances.is_empty() || !cluster_state.shard_merges.is_empty() {
        return Ok(());
    }
    let live_shards = cluster_state
        .shards
        .iter()
        .map(|s| s.shard_id)
        .collect::<std::collections::BTreeSet<_>>();
    split_failure_backoff
        .retain(|shard_id, entry| live_shards.contains(shard_id) && entry.retry_after > now);
    state.set_split_backoff_active(split_failure_backoff.len() as u64);

    let shard_limit = state.data_shards.max(1);
    let Some(target_idx) = state.cluster_store.first_free_shard_index(shard_limit) else {
        return Ok(());
    };

    let eval = split_strategy
        .evaluate(
            state.clone(),
            keyspace.as_ref(),
            &cluster_state,
            cfg,
            split_failure_backoff,
            target_idx,
            now,
        )
        .await?;
    let qps_by_idx = eval.qps_by_idx;
    if let Some(split_decision) = eval.decision {
        let inflight_split_ops = cluster_state
            .shard_fences
            .values()
            .filter(|reason| reason.starts_with(SPLIT_FENCE_PREFIX))
            .count();
        if split_decision.staged_backfill
            && inflight_split_ops >= cfg.adaptive_max_concurrent_splits
        {
            return Ok(());
        }
        let backfill_targets = split_decision
            .target_replicas
            .clone()
            .unwrap_or_else(|| split_decision.shard.replicas.clone());
        if split_decision.staged_backfill {
            staged_split_backfill(
                state.clone(),
                split_decision.shard.shard_index,
                split_decision.target_shard_index,
                split_decision.shard.leaseholder,
                backfill_targets.as_slice(),
                split_decision.split_key.as_slice(),
                split_decision.shard.end_key.as_slice(),
                cfg.adaptive_backfill_page_limit,
                cfg.adaptive_backfill_max_pages,
                cfg.adaptive_backfill_max_bps,
            )
            .await?;
        }

        let split_fence_op = SplitFenceOp::new(state.node_id, split_decision.shard.shard_id);
        let fenced_shards = vec![split_fence_op.source_shard_id];

        tracing::info!(
            shard_id = split_decision.shard.shard_id,
            shard_index = split_decision.shard.shard_index,
            reason = split_decision.reason,
            target_shard_index = split_decision.target_shard_index,
            split_token = %split_fence_op.token,
            split_started_ms = split_fence_op.started_ms,
            split_key = %String::from_utf8_lossy(&split_decision.split_key),
            "range manager proposing split"
        );
        state.record_split_attempt(split_decision.reason.as_str());

        let split_res = run_split_with_controller_lease_heartbeat(state.clone(), async {
            ensure_range_controller_leader(state.clone()).await?;
            let fence_before_epoch = state.cluster_store.epoch();
            for shard_id in &fenced_shards {
                propose_meta(
                    state.clone(),
                    ClusterCommand::SetShardFence {
                        shard_id: *shard_id,
                        fenced: true,
                        reason: split_fence_op.reason.clone(),
                    },
                )
                .await?;
            }
            let required_fences =
                fenced_shards_to_expectations(&fenced_shards, &split_fence_op.reason);
            let fence_epoch = wait_for_local_fences(
                state.clone(),
                fence_before_epoch,
                &required_fences,
                &[],
                SPLIT_FENCE_SYNC_TIMEOUT,
            )
            .await?;
            wait_for_cluster_converged(
                state.clone(),
                fence_epoch,
                None,
                &required_fences,
                &[],
                SPLIT_CLUSTER_SYNC_TIMEOUT,
            )
            .await?;

            // Drain only the source shard to a low watermark; global client
            // drains under sustained ingest can starve split progress.
            wait_for_shard_low_watermark(
                state.clone(),
                split_decision.shard.shard_index,
                SPLIT_SHARD_DRAIN_LOW_WATERMARK,
                SPLIT_SHARD_DRAIN_STABLE_FOR,
                SPLIT_SHARD_DRAIN_TIMEOUT,
            )
            .await?;

            if split_decision.staged_backfill {
                staged_split_backfill(
                    state.clone(),
                    split_decision.shard.shard_index,
                    split_decision.target_shard_index,
                    split_decision.shard.leaseholder,
                    backfill_targets.as_slice(),
                    split_decision.split_key.as_slice(),
                    split_decision.shard.end_key.as_slice(),
                    cfg.adaptive_backfill_page_limit,
                    cfg.adaptive_backfill_max_pages,
                    cfg.adaptive_backfill_max_bps,
                )
                .await?;
            }

            ensure_range_controller_leader(state.clone()).await?;
            let split_before_epoch = state.cluster_store.epoch();
            propose_meta(
                state.clone(),
                ClusterCommand::SplitRange {
                    split_key: split_decision.split_key.clone(),
                    target_shard_index: split_decision.target_shard_index,
                    target_replicas: split_decision.target_replicas.clone(),
                    target_leaseholder: split_decision.target_leaseholder,
                    skip_migration: split_decision.skip_migration,
                },
            )
            .await?;
            let split_epoch =
                wait_for_local_epoch(state.clone(), split_before_epoch, SPLIT_FENCE_SYNC_TIMEOUT)
                    .await?;
            state
                .cluster_store
                .state()
                .shards
                .iter()
                .find(|s| s.shard_index == split_decision.target_shard_index)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "split finished without target shard index {}",
                        split_decision.target_shard_index
                    )
                })?;
            let shard_count = state.cluster_store.state().shards.len();
            wait_for_cluster_converged(
                state.clone(),
                split_epoch,
                Some(shard_count),
                &required_fences,
                &[],
                SPLIT_CLUSTER_SYNC_TIMEOUT,
            )
            .await?;
            Ok::<(), anyhow::Error>(())
        })
        .await;

        let clear_res = clear_split_fences(
            state.clone(),
            &fenced_shards,
            &split_fence_op.reason,
            SPLIT_FENCE_SYNC_TIMEOUT,
        )
        .await;
        if let Err(err) = split_res {
            let reason = classify_split_failure(&err);
            let (delay, failures) = schedule_split_failure_backoff(
                split_failure_backoff,
                split_decision.shard.shard_id,
                now,
            );
            let transient = is_transient_split_failure(reason);
            if transient {
                state.record_split_transient_abort(reason, &err.to_string());
            } else {
                state.record_split_failure(reason, &err.to_string());
            }
            state.set_split_backoff_active(split_failure_backoff.len() as u64);
            if transient {
                tracing::info!(
                    shard_id = split_decision.shard.shard_id,
                    reason = reason,
                    failures,
                    retry_after_ms = delay.as_millis(),
                    "range split aborted by transient lease handoff; backoff scheduled"
                );
            } else {
                tracing::warn!(
                    shard_id = split_decision.shard.shard_id,
                    reason = reason,
                    failures,
                    retry_after_ms = delay.as_millis(),
                    "range split failed; backoff scheduled"
                );
            }
            if let Err(clear_err) = clear_res {
                if transient {
                    tracing::info!(error = ?clear_err, "split abort fence cleanup deferred to recovery");
                } else {
                    tracing::warn!(error = ?clear_err, "failed to clear split fences after split failure");
                }
            }
            if transient {
                return Ok(());
            }
            return Err(err);
        }
        if let Err(err) = clear_res {
            let reason = classify_split_failure(&err);
            let (delay, failures) = schedule_split_failure_backoff(
                split_failure_backoff,
                split_decision.shard.shard_id,
                now,
            );
            let transient = is_transient_split_failure(reason);
            if transient {
                state.record_split_transient_abort(reason, &err.to_string());
            } else {
                state.record_split_failure(reason, &err.to_string());
            }
            state.set_split_backoff_active(split_failure_backoff.len() as u64);
            if transient {
                tracing::info!(
                    shard_id = split_decision.shard.shard_id,
                    reason = reason,
                    failures,
                    retry_after_ms = delay.as_millis(),
                    "range split cleanup aborted by transient lease handoff; backoff scheduled"
                );
            } else {
                tracing::warn!(
                    shard_id = split_decision.shard.shard_id,
                    reason = reason,
                    failures,
                    retry_after_ms = delay.as_millis(),
                    "range split fence cleanup failed; backoff scheduled"
                );
            }
            if transient {
                return Ok(());
            }
            return Err(err);
        }
        split_failure_backoff.remove(&split_decision.shard.shard_id);
        state.set_split_backoff_active(split_failure_backoff.len() as u64);
        state.record_split_success();
        split_strategy.on_split_success(
            &split_decision.shard,
            split_decision.target_shard_index,
            cfg,
            now,
        );
        return Ok(());
    }

    if let Some((left, right, key_total)) = pick_merge_candidate(
        &cluster_state,
        keyspace.as_ref(),
        cfg,
        &qps_by_idx,
        merge_cooldown_until,
        merge_sustained,
        now,
    )? {
        tracing::info!(
            left_shard_id = left.shard_id,
            right_shard_id = right.shard_id,
            left_index = left.shard_index,
            right_index = right.shard_index,
            key_total,
            "range manager proposing merge"
        );
        propose_meta(
            state.clone(),
            ClusterCommand::BeginRangeMerge {
                left_shard_id: left.shard_id,
                right_shard_id: right.shard_id,
            },
        )
        .await?;
        let cooldown_deadline = now + cfg.merge_cooldown;
        merge_cooldown_until.insert(left.shard_id, cooldown_deadline);
        merge_cooldown_until.insert(right.shard_id, cooldown_deadline);
        merge_sustained.remove(&left.shard_id);
        merge_sustained.remove(&right.shard_id);
    }
    Ok(())
}

/// Wait until a shard group's in-flight queue depth stays under a watermark.
///
/// Inputs:
/// - `shard_index`: data group to observe.
/// - `max_pending`: queue depth threshold.
/// - `stable_for`: minimum continuous period below threshold.
/// - `timeout`: maximum wait budget.
///
/// Output:
/// - `Ok(())` when the shard is stably drained enough for cutover.
async fn wait_for_shard_low_watermark(
    state: Arc<NodeState>,
    shard_index: usize,
    max_pending: u64,
    stable_for: Duration,
    timeout: Duration,
) -> anyhow::Result<()> {
    let group_id = crate::GROUP_DATA_BASE + shard_index as u64;
    let group = state
        .group(group_id)
        .ok_or_else(|| anyhow::anyhow!("missing group for shard index {}", shard_index))?;
    let deadline = Instant::now() + timeout;

    let mut stable_since: Option<Instant> = None;
    loop {
        ensure_range_controller_leader(state.clone()).await?;
        let stats = group.debug_stats().await;
        let pending = (stats.records_status_preaccepted_len
            + stats.records_status_accepted_len
            + stats.records_status_committed_len
            + stats.records_status_executing_len
            + stats.committed_queue_len
            + stats.read_waiters_len) as u64;
        if pending <= max_pending {
            let now = Instant::now();
            if let Some(since) = stable_since {
                if now.saturating_duration_since(since) >= stable_for {
                    return Ok(());
                }
            } else {
                stable_since = Some(now);
            }
        } else {
            stable_since = None;
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for shard {} low-watermark drain (pending={}, watermark={}, stable_for_ms={}, preaccepted={}, accepted={}, committed={}, executing={}, committed_queue={}, read_waiters={})",
                shard_index,
                pending,
                max_pending,
                stable_for.as_millis(),
                stats.records_status_preaccepted_len,
                stats.records_status_accepted_len,
                stats.records_status_committed_len,
                stats.records_status_executing_len,
                stats.committed_queue_len,
                stats.read_waiters_len
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[derive(Debug, Deserialize)]
/// Minimal cluster-state probe used for convergence checks against remote nodes.
struct ClusterProbe {
    epoch: u64,
    shards: Vec<serde_json::Value>,
    #[serde(default)]
    shard_fences: std::collections::BTreeMap<String, String>,
}

/// Return current unix timestamp in milliseconds.
///
/// Output:
/// - Best-effort monotonic wall-clock sample clamped to `u64`.
fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Encode split fence metadata for storage in cluster shard fences.
///
/// Inputs:
/// - `token`: unique split operation token.
/// - `started_ms`: split start timestamp.
///
/// Output:
/// - Stable reason string parseable by stale-fence recovery.
fn encode_split_fence_reason(token: &str, started_ms: u64) -> String {
    format!("{SPLIT_FENCE_PREFIX}:{token}:{started_ms}")
}

/// Parse split fence metadata previously encoded by `encode_split_fence_reason`.
///
/// Input:
/// - Raw fence reason string from cluster state.
///
/// Output:
/// - `(token, started_ms)` if format matches split-fence encoding.
fn parse_split_fence_reason(reason: &str) -> Option<(String, u64)> {
    let mut parts = reason.split(':');
    let prefix = parts.next()?;
    if prefix != SPLIT_FENCE_PREFIX {
        return None;
    }
    let token = parts.next()?.to_string();
    let started_ms = parts.next()?.parse::<u64>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((token, started_ms))
}

/// Build expected `(shard_id, reason)` tuples for fence convergence checks.
fn fenced_shards_to_expectations(shard_ids: &[u64], reason: &str) -> Vec<(u64, String)> {
    shard_ids
        .iter()
        .copied()
        .map(|shard_id| (shard_id, reason.to_string()))
        .collect()
}

/// Check that local cluster snapshot contains all expected fenced shards.
fn local_has_required_fences(
    snapshot: &crate::cluster::ClusterState,
    required_fences: &[(u64, String)],
) -> bool {
    required_fences.iter().all(|(shard_id, reason)| {
        snapshot
            .shard_fences
            .get(shard_id)
            .map(|current| current == reason)
            .unwrap_or(false)
    })
}

/// Check that local cluster snapshot has cleared all listed fenced shards.
fn local_has_cleared_fences(
    snapshot: &crate::cluster::ClusterState,
    cleared_fences: &[u64],
) -> bool {
    cleared_fences
        .iter()
        .all(|shard_id| !snapshot.shard_fences.contains_key(shard_id))
}

/// Check remote probe contains all required fence reasons.
fn probe_has_required_fences(probe: &ClusterProbe, required_fences: &[(u64, String)]) -> bool {
    required_fences.iter().all(|(shard_id, reason)| {
        probe
            .shard_fences
            .get(&shard_id.to_string())
            .map(|current| current == reason)
            .unwrap_or(false)
    })
}

/// Check remote probe has cleared all listed shard fences.
fn probe_has_cleared_fences(probe: &ClusterProbe, cleared_fences: &[u64]) -> bool {
    cleared_fences
        .iter()
        .all(|shard_id| !probe.shard_fences.contains_key(&shard_id.to_string()))
}

/// Verify this node still holds/renews the range-controller lease.
///
/// Output:
/// - `Ok(())` while leadership is valid; error when lease is lost.
async fn ensure_range_controller_leader(state: Arc<NodeState>) -> anyhow::Result<()> {
    if state
        .ensure_controller_leader(ControllerDomain::Range)
        .await
    {
        Ok(())
    } else {
        anyhow::bail!("lost range controller lease during split operation")
    }
}

/// Clear stale split fences left behind by interrupted split workflows.
///
/// Inputs:
/// - Current cluster fence map and stale-timeout constants.
///
/// Output:
/// - Removes stale split fences after local+cluster convergence checks.
async fn recover_stale_split_fences(state: Arc<NodeState>) -> anyhow::Result<()> {
    ensure_range_controller_leader(state.clone()).await?;
    let snapshot = state.cluster_store.state();
    if snapshot.shard_fences.is_empty() {
        return Ok(());
    }

    let now_ms = now_unix_ms();
    let stale_after_ms = SPLIT_FENCE_STALE_TIMEOUT
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let mut stale_by_reason = std::collections::BTreeMap::<String, Vec<u64>>::new();
    for (shard_id, reason) in &snapshot.shard_fences {
        let Some((_token, started_ms)) = parse_split_fence_reason(reason) else {
            continue;
        };
        if now_ms.saturating_sub(started_ms) >= stale_after_ms {
            stale_by_reason
                .entry(reason.clone())
                .or_default()
                .push(*shard_id);
        }
    }

    for (reason, shard_ids) in stale_by_reason {
        let (token, started_ms) = parse_split_fence_reason(&reason)
            .ok_or_else(|| anyhow::anyhow!("invalid split fence reason format: {reason}"))?;
        tracing::warn!(
            split_token = %token,
            started_ms,
            stale_for_ms = now_ms.saturating_sub(started_ms),
            shard_ids = ?shard_ids,
            "clearing stale split fences"
        );

        ensure_range_controller_leader(state.clone()).await?;
        let clear_before_epoch = state.cluster_store.epoch();
        for shard_id in &shard_ids {
            propose_meta(
                state.clone(),
                ClusterCommand::SetShardFence {
                    shard_id: *shard_id,
                    fenced: false,
                    reason: reason.clone(),
                },
            )
            .await?;
        }
        let local_epoch = wait_for_local_fences(
            state.clone(),
            clear_before_epoch,
            &[],
            &shard_ids,
            Duration::from_secs(5),
        )
        .await?;
        wait_for_cluster_converged(
            state.clone(),
            local_epoch,
            None,
            &[],
            &shard_ids,
            Duration::from_secs(5),
        )
        .await?;
    }
    Ok(())
}

/// Clear split fences for one completed/aborted split workflow.
///
/// Inputs:
/// - `fenced_shards`: shard ids that were fenced for the split.
/// - `reason`: fence reason token to match/clear.
/// - `timeout`: convergence deadline.
///
/// Output:
/// - `Ok(())` when local and cluster fence state confirms cleanup.
async fn clear_split_fences(
    state: Arc<NodeState>,
    fenced_shards: &[u64],
    reason: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    if fenced_shards.is_empty() {
        return Ok(());
    }
    ensure_range_controller_leader(state.clone()).await?;
    let clear_before_epoch = state.cluster_store.epoch();
    for shard_id in fenced_shards {
        propose_meta(
            state.clone(),
            ClusterCommand::SetShardFence {
                shard_id: *shard_id,
                fenced: false,
                reason: reason.to_string(),
            },
        )
        .await?;
    }
    let local_epoch = wait_for_local_fences(
        state.clone(),
        clear_before_epoch,
        &[],
        fenced_shards,
        timeout,
    )
    .await?;
    wait_for_cluster_converged(
        state.clone(),
        local_epoch,
        None,
        &[],
        fenced_shards,
        timeout,
    )
    .await
}

/// Wait for local cluster epoch to advance past a minimum epoch.
///
/// Output:
/// - New local epoch once observed, otherwise timeout error.
async fn wait_for_local_epoch(
    state: Arc<NodeState>,
    min_epoch: u64,
    timeout: Duration,
) -> anyhow::Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        ensure_range_controller_leader(state.clone()).await?;
        let local_epoch = state.cluster_store.epoch();
        if local_epoch > min_epoch {
            return Ok(local_epoch);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for local cluster epoch to advance beyond {min_epoch} (now={local_epoch})"
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until local fence state matches required+cleared expectations.
///
/// Inputs:
/// - `min_epoch`: lower bound for observation.
/// - `required_fences`: fences that must exist with exact reasons.
/// - `cleared_fences`: fences that must be absent.
///
/// Output:
/// - Local epoch where expectations are satisfied.
async fn wait_for_local_fences(
    state: Arc<NodeState>,
    min_epoch: u64,
    required_fences: &[(u64, String)],
    cleared_fences: &[u64],
    timeout: Duration,
) -> anyhow::Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        ensure_range_controller_leader(state.clone()).await?;
        let snapshot = state.cluster_store.state();
        if snapshot.epoch >= min_epoch
            && local_has_required_fences(&snapshot, required_fences)
            && local_has_cleared_fences(&snapshot, cleared_fences)
        {
            return Ok(snapshot.epoch);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for local fence state (epoch>={min_epoch}, required_fences={:?}, cleared_fences={:?}) (now_epoch={}, now_fences={:?})",
                required_fences,
                cleared_fences,
                snapshot.epoch,
                snapshot.shard_fences
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Wait until all active nodes converge to expected epoch/shard/fence state.
///
/// Inputs:
/// - Convergence thresholds and fence expectations.
///
/// Output:
/// - `Ok(())` when local and all probed remote nodes satisfy expectations.
async fn wait_for_cluster_converged(
    state: Arc<NodeState>,
    min_epoch: u64,
    min_shards: Option<usize>,
    required_fences: &[(u64, String)],
    cleared_fences: &[u64],
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        ensure_range_controller_leader(state.clone()).await?;
        let members = state
            .cluster_store
            .state()
            .members
            .values()
            .filter(|m| m.state != MemberState::Removed)
            .map(|m| (m.node_id, m.grpc_addr.clone()))
            .collect::<Vec<_>>();

        let local = state.cluster_store.state();
        let mut all_ok = true;
        let mut last_err = String::new();
        for (node_id, grpc_addr) in members {
            if node_id == state.node_id {
                let shard_ok = min_shards.map(|n| local.shards.len() >= n).unwrap_or(true);
                let fences_ok = local_has_required_fences(&local, required_fences)
                    && local_has_cleared_fences(&local, cleared_fences);
                if !(local.epoch >= min_epoch && shard_ok && fences_ok) {
                    all_ok = false;
                    last_err = format!(
                        "local node {} not converged (epoch={}, shards={}, fences={:?})",
                        node_id,
                        local.epoch,
                        local.shards.len(),
                        local.shard_fences
                    );
                    break;
                }
                continue;
            }

            match fetch_remote_probe(&grpc_addr, Duration::from_secs(1)).await {
                Ok(remote) => {
                    let shard_ok = min_shards.map(|n| remote.shards.len() >= n).unwrap_or(true);
                    let fences_ok = probe_has_required_fences(&remote, required_fences)
                        && probe_has_cleared_fences(&remote, cleared_fences);
                    if !(remote.epoch >= min_epoch && shard_ok && fences_ok) {
                        all_ok = false;
                        last_err = format!(
                            "node {} not converged (epoch={}, shards={}, fences={:?})",
                            node_id,
                            remote.epoch,
                            remote.shards.len(),
                            remote.shard_fences
                        );
                        break;
                    }
                }
                Err(err) => {
                    all_ok = false;
                    last_err = format!("node {} probe failed: {err}", node_id);
                    break;
                }
            }
        }

        if all_ok {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for cluster convergence (min_epoch={}, min_shards={:?}, required_fences={:?}, cleared_fences={:?}): {}",
                min_epoch,
                min_shards,
                required_fences,
                cleared_fences,
                last_err
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Fetch minimal cluster-state probe from a remote node over RPC.
///
/// Inputs:
/// - `addr`: remote grpc endpoint.
/// - `timeout`: RPC timeout budget.
///
/// Output:
/// - Decoded `ClusterProbe` used by convergence checks.
async fn fetch_remote_probe(addr: &str, timeout: Duration) -> anyhow::Result<ClusterProbe> {
    let socket_addr: std::net::SocketAddr = addr.parse()?;
    let client = rpc::HoloRpcClientBuilder::new("holo_store.rpc.HoloRpc")
        .address(Address::from(socket_addr))
        .build();
    let resp = tokio::time::timeout(timeout, client.cluster_state(rpc::ClusterStateRequest {}))
        .await
        .map_err(|_| anyhow::anyhow!("timeout calling cluster_state"))??;
    let json = resp.into_inner().json.to_string();
    Ok(serde_json::from_str::<ClusterProbe>(&json)?)
}

/// Propose one range-controller meta command with lease guard.
///
/// Inputs:
/// - `cmd`: controller-domain command (fence, split, merge, ...).
///
/// Output:
/// - `Ok(())` when command is proposed successfully.
async fn propose_meta(state: Arc<NodeState>, cmd: ClusterCommand) -> anyhow::Result<()> {
    ensure_range_controller_leader(state.clone()).await?;
    state
        .propose_meta_command_guarded(ControllerDomain::Range, cmd)
        .await
}

/// Copy `[start_key, end_key)` latest rows into target shard replicas before cutover.
///
/// Design:
/// - Performs paged snapshot reads from source shard.
/// - Applies pages to each target replica.
/// - Enforces optional throughput budget to protect foreground latency.
///
/// Inputs:
/// - Source/target shard indexes and participating nodes.
/// - Key range, page sizing, max pages, and bytes/sec cap.
///
/// Output:
/// - `Ok(())` when staged copy reaches `done`; error on stall or budget breach.
async fn staged_split_backfill(
    state: Arc<NodeState>,
    from_shard_index: usize,
    to_shard_index: usize,
    source_node: NodeId,
    target_nodes: &[NodeId],
    start_key: &[u8],
    end_key: &[u8],
    page_limit: usize,
    max_pages: usize,
    max_bps: u64,
) -> anyhow::Result<()> {
    let mut targets = target_nodes.to_vec();
    targets.sort_unstable();
    targets.dedup();
    if targets.is_empty() {
        return Ok(());
    }

    let limit = page_limit.clamp(64, 10_000);
    let mut cursor = Vec::new();
    let mut pages = 0usize;
    let mut window_start = Instant::now();
    let mut window_bytes = 0u64;

    loop {
        // Keep the range-controller lease alive during potentially long
        // snapshot/backfill scans so cutover fencing does not fail on lease loss.
        ensure_range_controller_leader(state.clone()).await?;
        if pages >= max_pages {
            anyhow::bail!(
                "staged split backfill exceeded max pages (from={}, to={}, max_pages={})",
                from_shard_index,
                to_shard_index,
                max_pages
            );
        }
        pages = pages.saturating_add(1);
        let (entries, next_cursor, done) = if source_node == state.node_id {
            state.snapshot_range_latest_page(
                from_shard_index,
                start_key,
                end_key,
                cursor.as_slice(),
                limit,
                false,
            )?
        } else {
            let (remote_entries, next_cursor, done) = state
                .transport
                .range_snapshot_latest(
                    source_node,
                    from_shard_index,
                    start_key,
                    end_key,
                    cursor.as_slice(),
                    limit,
                    false,
                )
                .await?;
            let entries = remote_entries
                .into_iter()
                .map(|entry| crate::RangeSnapshotLatestEntry {
                    key: entry.key,
                    value: entry.value,
                    version: entry.version,
                })
                .collect::<Vec<_>>();
            (entries, next_cursor, done)
        };
        if !entries.is_empty() {
            let page_bytes = entries.iter().fold(0u64, |acc, entry| {
                acc.saturating_add((entry.key.len() + entry.value.len()) as u64)
            });
            for node_id in &targets {
                if *node_id == state.node_id {
                    let _ = state.apply_range_latest_page(
                        to_shard_index,
                        start_key,
                        end_key,
                        &entries,
                    )?;
                } else {
                    let rpc_entries = entries
                        .iter()
                        .map(|entry| crate::transport::RangeLatestEntry {
                            key: entry.key.clone(),
                            value: entry.value.clone(),
                            version: entry.version,
                        })
                        .collect::<Vec<_>>();
                    let _ = state
                        .transport
                        .range_apply_latest(
                            *node_id,
                            to_shard_index,
                            start_key,
                            end_key,
                            rpc_entries,
                        )
                        .await?;
                }
            }
            window_bytes = window_bytes.saturating_add(page_bytes);
            if max_bps > 0 {
                let elapsed = window_start.elapsed();
                let expected = Duration::from_secs_f64(window_bytes as f64 / max_bps as f64);
                if expected > elapsed {
                    sleep_with_controller_lease_heartbeat(state.clone(), expected - elapsed)
                        .await?;
                }
                if elapsed >= Duration::from_secs(1) {
                    window_start = Instant::now();
                    window_bytes = 0;
                }
            }
        }
        if done {
            return Ok(());
        }
        if next_cursor == cursor {
            anyhow::bail!(
                "staged split backfill cursor stalled (from={}, to={})",
                from_shard_index,
                to_shard_index
            );
        }
        cursor = next_cursor;
    }
}

/// Run a split phase while renewing range-controller lease in the background.
///
/// Inputs:
/// - `fut`: split phase future to execute.
///
/// Output:
/// - Propagates `fut` result after stopping the lease heartbeat task.
async fn run_split_with_controller_lease_heartbeat<F>(
    state: Arc<NodeState>,
    fut: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    let stop = Arc::new(AtomicBool::new(false));
    let hb_state = state.clone();
    let hb_stop = stop.clone();
    let heartbeat = tokio::spawn(async move {
        // Keep renewing range-controller leadership during long split phases.
        while !hb_stop.load(Ordering::Relaxed) {
            let _ = hb_state
                .ensure_controller_leader(ControllerDomain::Range)
                .await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    let result = fut.await;
    stop.store(true, Ordering::Relaxed);
    let _ = heartbeat.await;
    result
}

/// Sleep with periodic range-controller lease checks.
///
/// Inputs:
/// - `duration`: total sleep time.
///
/// Output:
/// - `Ok(())` after sleeping if lease remains valid; error if lease is lost.
async fn sleep_with_controller_lease_heartbeat(
    state: Arc<NodeState>,
    duration: Duration,
) -> anyhow::Result<()> {
    let mut remaining = duration;
    let slice = Duration::from_millis(200);
    while remaining > Duration::ZERO {
        ensure_range_controller_leader(state.clone()).await?;
        let step = if remaining > slice { slice } else { remaining };
        tokio::time::sleep(step).await;
        remaining = remaining.saturating_sub(step);
    }
    Ok(())
}

/// Choose a split key using load-weighted sampling when hot-bucket data exists.
///
/// Design:
/// - Falls back to median-style sampling if no bucket weights are available.
/// - Applies `min_side_keys` guard to avoid repeatedly carving tiny ranges.
/// - Optimizes for weight balance with edge penalties.
///
/// Inputs:
/// - `keyspace`, source `shard`, per-bucket weights, and side-size guard.
///
/// Output:
/// - Candidate split key inside shard bounds, or `None` if no safe candidate.
fn pick_split_key_load_weighted(
    keyspace: &Keyspace,
    shard: &ShardDesc,
    bucket_weights: &[u64],
    min_side_keys: u64,
) -> anyhow::Result<Option<Vec<u8>>> {
    let use_weighted = bucket_weights.iter().any(|w| *w > 0);
    if !use_weighted {
        return pick_split_key_from_range(keyspace, shard);
    }

    let latest_name = format!("kv_latest_{}", shard.shard_index);
    let latest = keyspace.open_partition(&latest_name, PartitionCreateOptions::default())?;

    let sample_cap = SPLIT_KEY_SAMPLE_SIZE.saturating_mul(2).max(64);
    let mut seen = 0usize;
    let mut sample = Vec::<(Vec<u8>, u64)>::with_capacity(sample_cap);
    for item in latest_range(&latest, &shard.start_key, &shard.end_key) {
        let (key, _) = item?;
        let key_vec = key.to_vec();
        seen = seen.saturating_add(1);
        let bucket = weighted_bucket_for_key(&key_vec, bucket_weights.len().max(1));
        let weight = bucket_weights
            .get(bucket)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        if sample.len() < sample_cap {
            sample.push((key_vec, weight));
            continue;
        }
        let replace = reservoir_replace_index(shard.shard_id, seen, &key_vec);
        if replace < sample_cap {
            sample[replace] = (key_vec, weight);
        }
    }
    if seen < 2 || sample.len() < 2 {
        return Ok(None);
    }
    if min_side_keys > 0 && seen < min_side_keys.saturating_mul(2) as usize {
        return Ok(None);
    }

    sample.sort_by(|a, b| a.0.cmp(&b.0));
    let mut min_side = (sample.len() / 20).max(8);
    if min_side_keys > 0 {
        let frac = (min_side_keys as f64 / seen as f64).clamp(0.0, 0.49);
        let required = (frac * sample.len() as f64).ceil() as usize;
        min_side = min_side.max(required.max(1));
    }
    if sample.len() <= min_side.saturating_mul(2) {
        return Ok(None);
    }
    let total_weight = sample.iter().map(|(_, w)| *w).sum::<u64>().max(1);
    let mut prefix_weight = 0u64;
    let mut best_idx = None;
    let mut best_cost = f64::MAX;

    for idx in min_side..(sample.len() - min_side) {
        prefix_weight = prefix_weight.saturating_add(sample[idx - 1].1);
        let left_w = prefix_weight;
        let right_w = total_weight.saturating_sub(prefix_weight);
        let left_n = idx as u64;
        let right_n = (sample.len() - idx) as u64;
        let imbalance_w = left_w.abs_diff(right_w) as f64;
        let imbalance_n = left_n.abs_diff(right_n) as f64;
        let edge_penalty = if idx < sample.len() / 10 || idx > (sample.len() * 9 / 10) {
            10.0
        } else {
            0.0
        };
        let cost = imbalance_w + (0.35 * imbalance_n) + edge_penalty;
        if cost < best_cost {
            best_cost = cost;
            best_idx = Some(idx);
        }
    }

    let in_bounds = |key: &[u8]| {
        (shard.start_key.is_empty() || key > shard.start_key.as_slice())
            && (shard.end_key.is_empty() || key < shard.end_key.as_slice())
    };
    if let Some(idx) = best_idx {
        if let Some((candidate, _)) = sample.get(idx) {
            if in_bounds(candidate.as_slice()) {
                return Ok(Some(candidate.clone()));
            }
        }
    }
    pick_split_key_from_range(keyspace, shard)
}

/// Map a key into a hot-bucket index used by weighted split scoring.
///
/// Inputs:
/// - raw key bytes and bucket count.
///
/// Output:
/// - deterministic bucket index in `[0, buckets)`.
fn weighted_bucket_for_key(key: &[u8], buckets: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % buckets.max(1)
}

/// Legacy loadops candidate selection.
///
/// Design:
/// - Phase 1: choose hottest shard above sustained QPS threshold.
/// - Phase 2: otherwise choose shard with most set-ops since baseline.
///
/// Inputs:
/// - Node-local load snapshots, shard descriptors, thresholds, and backoff state.
///
/// Output:
/// - Optional `(reason, shard)` split candidate for downstream key selection.
fn pick_split_candidate(
    state: Arc<NodeState>,
    shards: &[ShardDesc],
    cfg: RangeManagerConfig,
    last: &mut ShardLoadSnapshot,
    baseline_set_ops: &mut [u64],
    sustained: &mut [u8],
    cooldown_until: &mut [std::time::Instant],
    split_failure_backoff: &std::collections::BTreeMap<u64, SplitFailureBackoff>,
    qps_by_idx_out: &mut Vec<u64>,
    now: std::time::Instant,
) -> anyhow::Result<Option<(&'static str, ShardDesc)>> {
    let cur = state.shard_load.snapshot();
    let secs = cfg.interval.as_secs_f64().max(0.001);

    // Compute SET QPS deltas per shard index.
    let mut qps_by_idx = vec![0u64; cur.set_ops.len()];
    for (idx, (cur_v, last_v)) in cur
        .set_ops
        .iter()
        .copied()
        .zip(last.set_ops.iter().copied())
        .enumerate()
    {
        let delta = cur_v.saturating_sub(last_v);
        let qps = ((delta as f64) / secs) as u64;
        qps_by_idx[idx] = qps;
    }

    // Update last snapshot.
    *last = cur;
    *qps_by_idx_out = qps_by_idx.clone();

    // Track sustained-above-threshold streaks.
    for (idx, qps) in qps_by_idx.iter().copied().enumerate() {
        let above = cfg.split_min_qps > 0 && qps >= cfg.split_min_qps;
        let cooldown_active = cooldown_until
            .get(idx)
            .map(|until| *until > now)
            .unwrap_or(false);

        if above && !cooldown_active {
            if let Some(s) = sustained.get_mut(idx) {
                *s = s.saturating_add(1);
            }
        } else if let Some(s) = sustained.get_mut(idx) {
            *s = 0;
        }
    }

    // 1) Prefer load-based splitting (hottest shard that is sustained above threshold).
    let mut best_load: Option<(u64, ShardDesc)> = None;
    for shard in shards {
        if split_failure_backoff
            .get(&shard.shard_id)
            .map(|entry| entry.retry_after > now)
            .unwrap_or(false)
        {
            continue;
        }
        let idx = shard.shard_index;
        if idx >= qps_by_idx.len() || idx >= sustained.len() {
            continue;
        }
        if sustained[idx] < cfg.split_qps_sustain {
            continue;
        }
        let qps = qps_by_idx[idx];
        match &best_load {
            Some((best_qps, _)) if *best_qps >= qps => {}
            _ => best_load = Some((qps, shard.clone())),
        }
    }
    if let Some((_qps, shard)) = best_load {
        return Ok(Some(("load", shard)));
    }

    // 2) Size-based splitting (approximate by SET ops since baseline).
    if cfg.split_min_keys == 0 {
        return Ok(None);
    }
    let mut best_size: Option<(u64, ShardDesc)> = None;
    for shard in shards {
        if split_failure_backoff
            .get(&shard.shard_id)
            .map(|entry| entry.retry_after > now)
            .unwrap_or(false)
        {
            continue;
        }
        let idx = shard.shard_index;
        if idx >= last.set_ops.len() || idx >= baseline_set_ops.len() {
            continue;
        }
        let cooldown_active = cooldown_until
            .get(idx)
            .map(|until| *until > now)
            .unwrap_or(false);
        if cooldown_active {
            continue;
        }
        let since = last.set_ops[idx].saturating_sub(baseline_set_ops[idx]);
        if since < cfg.split_min_keys as u64 {
            continue;
        }
        match &best_size {
            Some((best_since, _)) if *best_since >= since => {}
            _ => best_size = Some((since, shard.clone())),
        }
    }
    if let Some((_since, shard)) = best_size {
        return Ok(Some(("size", shard)));
    }

    Ok(None)
}

/// Pick one adjacent shard pair that is safe and worthwhile to merge.
///
/// Inputs:
/// - Ordered shard descriptors, merge thresholds, current per-shard QPS.
/// - Merge cooldown and sustain tracking maps.
///
/// Output:
/// - `(left, right, combined_keys)` for best cold/small adjacent pair, or `None`.
fn pick_merge_candidate(
    cluster_state: &crate::cluster::ClusterState,
    keyspace: &Keyspace,
    cfg: RangeManagerConfig,
    qps_by_idx: &[u64],
    merge_cooldown_until: &mut std::collections::BTreeMap<u64, std::time::Instant>,
    merge_sustained: &mut std::collections::BTreeMap<u64, u8>,
    now: std::time::Instant,
) -> anyhow::Result<Option<(ShardDesc, ShardDesc, usize)>> {
    if cfg.merge_max_keys == 0 || cluster_state.shards.len() < 2 {
        return Ok(None);
    }
    if !cluster_state.shard_rebalances.is_empty() || !cluster_state.shard_merges.is_empty() {
        return Ok(None);
    }

    merge_cooldown_until.retain(|_, until| *until > now);
    let adjacent_left_ids = cluster_state
        .shards
        .windows(2)
        .map(|pair| pair[0].shard_id)
        .collect::<std::collections::BTreeSet<_>>();
    merge_sustained.retain(|left_id, _| adjacent_left_ids.contains(left_id));

    let key_threshold = if cfg.merge_key_hysteresis_pct == 0 {
        cfg.merge_max_keys
    } else {
        ((cfg.merge_max_keys as u128).saturating_mul(cfg.merge_key_hysteresis_pct as u128) / 100)
            .max(1) as usize
    };
    let merge_qps_sustain = cfg.merge_qps_sustain.max(1);

    let mut best: Option<(usize, ShardDesc, ShardDesc)> = None;
    for pair in cluster_state.shards.windows(2) {
        let left = &pair[0];
        let right = &pair[1];
        if !left.end_key.is_empty() && left.end_key != right.start_key {
            continue;
        }
        if left.replicas != right.replicas || left.leaseholder != right.leaseholder {
            continue;
        }
        if merge_cooldown_until
            .get(&left.shard_id)
            .map(|until| *until > now)
            .unwrap_or(false)
            || merge_cooldown_until
                .get(&right.shard_id)
                .map(|until| *until > now)
                .unwrap_or(false)
        {
            continue;
        }

        let left_count =
            count_keys_in_range(keyspace, left.shard_index, &left.start_key, &left.end_key)?;
        let right_count = count_keys_in_range(
            keyspace,
            right.shard_index,
            &right.start_key,
            &right.end_key,
        )?;
        let total = left_count.saturating_add(right_count);
        if total > key_threshold {
            merge_sustained.remove(&left.shard_id);
            continue;
        }
        let combined_qps = qps_by_idx.get(left.shard_index).copied().unwrap_or(0)
            + qps_by_idx.get(right.shard_index).copied().unwrap_or(0);
        if cfg.merge_max_qps > 0 && combined_qps > cfg.merge_max_qps {
            merge_sustained.remove(&left.shard_id);
            continue;
        }
        let streak = merge_sustained.entry(left.shard_id).or_insert(0);
        *streak = streak.saturating_add(1);
        if *streak < merge_qps_sustain {
            continue;
        }
        match &best {
            Some((best_total, _, _)) if *best_total <= total => {}
            _ => best = Some((total, left.clone(), right.clone())),
        }
    }

    Ok(best.map(|(total, left, right)| (left, right, total)))
}

/// Count latest-row keys inside `[start_key, end_key)` for one shard index.
///
/// Output:
/// - Approximate key count used by merge candidate evaluation.
fn count_keys_in_range(
    keyspace: &Keyspace,
    shard_index: usize,
    start_key: &[u8],
    end_key: &[u8],
) -> anyhow::Result<usize> {
    let latest_name = format!("kv_latest_{shard_index}");
    let latest = keyspace.open_partition(&latest_name, PartitionCreateOptions::default())?;
    let mut count = 0usize;
    for item in latest_range(&latest, start_key, end_key) {
        let _ = item?;
        count = count.saturating_add(1);
    }
    Ok(count)
}

/// Select a split key from a shard using reservoir-sampled approximate median.
///
/// Inputs:
/// - `keyspace`: source of keys for range scan.
/// - `shard`: key bounds and shard index.
///
/// Output:
/// - In-range split key or `None` when shard has insufficient keys.
fn pick_split_key_from_range(
    keyspace: &Keyspace,
    shard: &ShardDesc,
) -> anyhow::Result<Option<Vec<u8>>> {
    let latest_name = format!("kv_latest_{}", shard.shard_index);
    let latest = keyspace.open_partition(&latest_name, PartitionCreateOptions::default())?;

    // Approximate median with one-pass reservoir sampling. This avoids a full
    // two-pass scan of the range while still spreading split points.
    let mut seen = 0usize;
    let mut sample = Vec::<Vec<u8>>::with_capacity(SPLIT_KEY_SAMPLE_SIZE);
    for item in latest_range(&latest, &shard.start_key, &shard.end_key) {
        let (key, _) = item?;
        let key_vec = key.to_vec();
        seen = seen.saturating_add(1);
        if sample.len() < SPLIT_KEY_SAMPLE_SIZE {
            sample.push(key_vec);
            continue;
        }
        let replace = reservoir_replace_index(shard.shard_id, seen, &key_vec);
        if replace < SPLIT_KEY_SAMPLE_SIZE {
            sample[replace] = key_vec;
        }
    }
    if seen < 2 || sample.len() < 2 {
        return Ok(None);
    }
    sample.sort();
    let mid = sample.len() / 2;
    let in_bounds = |key: &Vec<u8>| {
        (shard.start_key.is_empty() || key.as_slice() > shard.start_key.as_slice())
            && (shard.end_key.is_empty() || key.as_slice() < shard.end_key.as_slice())
    };
    if let Some(candidate) = sample[mid..].iter().find(|k| in_bounds(k)) {
        return Ok(Some(candidate.clone()));
    }
    if let Some(candidate) = sample[..mid].iter().rev().find(|k| in_bounds(k)) {
        return Ok(Some(candidate.clone()));
    }
    Ok(None)
}

/// Compute deterministic reservoir replacement index for sampled split keys.
///
/// Inputs:
/// - `shard_id`: keeps samples shard-specific.
/// - `seen`: number of keys seen so far.
/// - `key`: current key bytes.
///
/// Output:
/// - Index in `[0, seen)` used by reservoir sampling.
fn reservoir_replace_index(shard_id: u64, seen: usize, key: &[u8]) -> usize {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    shard_id.hash(&mut hasher);
    seen.hash(&mut hasher);
    key.hash(&mut hasher);
    (hasher.finish() as usize) % seen.max(1)
}

/// Map a split error into a coarse failure reason category.
///
/// Output:
/// - Stable short reason used by telemetry and split-health reporting.
fn classify_split_failure(err: &anyhow::Error) -> &'static str {
    let text = err.to_string().to_lowercase();
    if text.contains("controller lease") {
        "lease_lost"
    } else if text.contains("cluster convergence") {
        "cluster_convergence_timeout"
    } else if text.contains("fence") {
        "fence_sync_failed"
    } else if text.contains("low-watermark drain")
        || text.contains("timed out waiting for shard")
        || text.contains("quiesce")
    {
        "shard_drain_timeout"
    } else if text.contains("split finished without target shard") || text.contains("split key") {
        "split_apply_failed"
    } else {
        "other"
    }
}

/// Whether a classified failure should be tracked as transient abort.
fn is_transient_split_failure(reason: &str) -> bool {
    matches!(reason, "lease_lost")
}

/// Register a failure and compute its next retry delay for one shard.
///
/// Inputs:
/// - Backoff map, shard id, and current timestamp.
///
/// Output:
/// - `(delay, failures)` for logging and retry suppression.
fn schedule_split_failure_backoff(
    split_failure_backoff: &mut std::collections::BTreeMap<u64, SplitFailureBackoff>,
    shard_id: u64,
    now: Instant,
) -> (Duration, u32) {
    let entry = split_failure_backoff
        .entry(shard_id)
        .or_insert(SplitFailureBackoff {
            failures: 0,
            retry_after: now,
        });
    entry.failures = entry.failures.saturating_add(1);
    let delay = split_failure_backoff_delay(shard_id, entry.failures);
    entry.retry_after = now + delay;
    (delay, entry.failures)
}

/// Compute exponential backoff with deterministic jitter for split retries.
///
/// Inputs:
/// - `shard_id`: salt to de-synchronize retries across shards.
/// - `failures`: consecutive failure count.
///
/// Output:
/// - Retry delay clamped to configured min/max bounds.
fn split_failure_backoff_delay(shard_id: u64, failures: u32) -> Duration {
    use std::hash::{Hash, Hasher};

    let shift = failures
        .saturating_sub(1)
        .min(SPLIT_FAILURE_BACKOFF_MAX_SHIFT);
    let multiplier = 1u64 << shift;
    let base_ms = SPLIT_FAILURE_BACKOFF_BASE
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let raw_ms = base_ms.saturating_mul(multiplier);
    let capped_ms = raw_ms.min(
        SPLIT_FAILURE_BACKOFF_MAX
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
    );

    // Deterministic jitter in [80%, 120%] to avoid synchronized retries.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    shard_id.hash(&mut hasher);
    failures.hash(&mut hasher);
    now_unix_ms().hash(&mut hasher);
    let jitter_percent = 80u64 + (hasher.finish() % 41);
    let jittered_ms = capped_ms.saturating_mul(jitter_percent) / 100;
    Duration::from_millis(jittered_ms.max(1))
}

/// Return an iterator over latest rows in `[start, end)`.
///
/// Inputs:
/// - `latest`: shard latest-value partition.
/// - `start`, `end`: key bounds (`end=[]` means unbounded high end).
///
/// Output:
/// - Double-ended iterator used by split key selection and key counting.
fn latest_range(
    latest: &fjall::PartitionHandle,
    start: &[u8],
    end: &[u8],
) -> Box<dyn DoubleEndedIterator<Item = fjall::Result<fjall::KvPair>>> {
    let start = start.to_vec();
    if end.is_empty() {
        Box::new(latest.range(start..))
    } else {
        Box::new(latest.range(start..end.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "holo_store_{name}_{}_{}",
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn weighted_split_respects_min_side_key_guard() {
        let dir = temp_dir("weighted_split_min_side");
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let keyspace = fjall::Config::new(&dir)
            .open()
            .expect("open temporary keyspace");
        let latest = keyspace
            .open_partition("kv_latest_0", PartitionCreateOptions::default())
            .expect("open partition");
        for i in 0..1000u64 {
            let key = format!("k{i:06}");
            latest
                .insert(key.as_bytes(), b"v".as_slice())
                .expect("insert key");
        }

        let shard = ShardDesc {
            shard_id: 1,
            shard_index: 0,
            start_hash: 0,
            end_hash: 0,
            start_key: Vec::new(),
            end_key: Vec::new(),
            replicas: vec![1],
            leaseholder: 1,
        };
        let buckets = vec![1u64; 64];

        let blocked = pick_split_key_load_weighted(&keyspace, &shard, &buckets, 600)
            .expect("split-key selection should not error");
        assert!(
            blocked.is_none(),
            "min-side guard should block split when both sides cannot keep enough keys"
        );

        let selected = pick_split_key_load_weighted(&keyspace, &shard, &buckets, 200)
            .expect("split-key selection should not error")
            .expect("split should be possible with relaxed min-side guard");
        let mut left_count = 0usize;
        for item in latest_range(&latest, &shard.start_key, &shard.end_key) {
            let (k, _) = item.expect("read key");
            if k.as_ref() < selected.as_slice() {
                left_count = left_count.saturating_add(1);
            }
        }
        assert!(
            (200..=800).contains(&left_count),
            "split point should keep both sides above min-side guard, left_count={left_count}"
        );

        drop(latest);
        drop(keyspace);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
