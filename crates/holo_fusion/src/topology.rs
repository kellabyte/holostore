//! Cluster-topology helpers for shard-aware routing and scanning.
//!
//! HoloFusion uses HoloStore cluster state to route point writes and range
//! scans to appropriate shard leaseholders while still providing local fallbacks
//! when topology information is incomplete.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

use anyhow::{anyhow, Context, Result};
use holo_store::HoloStoreClient;
use serde::Deserialize;

/// Materialized view of cluster members and shard ownership.
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    /// All members keyed by node id.
    pub members: BTreeMap<u64, TopologyMember>,
    /// Shard descriptors sorted by shard index.
    pub shards: Vec<TopologyShard>,
}

/// Member attributes relevant for routing decisions.
#[derive(Debug, Clone)]
pub struct TopologyMember {
    /// Node identifier in the cluster.
    pub node_id: u64,
    /// gRPC endpoint used for direct shard access.
    pub grpc_addr: SocketAddr,
    /// Membership state string from cluster state view.
    pub state: String,
}

/// Shard descriptor used for range and hash routing.
#[derive(Debug, Clone)]
pub struct TopologyShard {
    /// Stable shard id.
    pub shard_id: u64,
    /// Dense shard index used by RPC APIs.
    pub shard_index: usize,
    /// Node id that currently owns the lease.
    pub leaseholder: u64,
    /// Inclusive start hash bound.
    pub start_hash: u64,
    /// Inclusive end hash bound.
    pub end_hash: u64,
    /// Inclusive logical start key for range routing.
    pub start_key: Vec<u8>,
    /// Exclusive logical end key for range routing.
    pub end_key: Vec<u8>,
}

/// One scan segment routed to a specific shard/target.
#[derive(Debug, Clone)]
pub struct ScanTarget {
    /// Destination shard index.
    pub shard_index: usize,
    /// Destination gRPC endpoint.
    pub grpc_addr: SocketAddr,
    /// Segment start key (inclusive).
    pub start_key: Vec<u8>,
    /// Segment end key (exclusive, empty means unbounded).
    pub end_key: Vec<u8>,
}

/// Result of routing a single key to a shard/endpoint.
#[derive(Debug, Clone)]
pub struct KeyRoute {
    /// Destination shard index.
    pub shard_index: usize,
    /// Destination gRPC endpoint.
    pub grpc_addr: SocketAddr,
    /// Shard start bound at route time.
    pub start_key: Vec<u8>,
    /// Shard end bound at route time.
    pub end_key: Vec<u8>,
}

/// Fetches and parses topology from HoloStore cluster-state JSON.
pub async fn fetch_topology(client: &HoloStoreClient) -> Result<ClusterTopology> {
    let raw = client
        .cluster_state_json()
        .await
        .context("fetch cluster state json")?;
    let parsed: ClusterStateView =
        serde_json::from_str(&raw).context("parse cluster state json")?;

    let mut members = BTreeMap::new();
    for (_, member) in parsed.members {
        // Decision: skip members without gRPC addresses because they cannot be
        // used as routing targets.
        // Decision: evaluate `if member.grpc_addr.is_empty() {` to choose the correct SQL/storage control path.
        if member.grpc_addr.is_empty() {
            continue;
        }
        let grpc_addr: SocketAddr = member
            .grpc_addr
            .parse()
            .with_context(|| format!("invalid grpc addr in cluster state: {}", member.grpc_addr))?;
        members.insert(
            member.node_id,
            TopologyMember {
                node_id: member.node_id,
                grpc_addr,
                state: member.state,
            },
        );
    }

    let mut shards = parsed
        .shards
        .into_iter()
        .map(|shard| TopologyShard {
            shard_id: shard.shard_id,
            shard_index: shard.shard_index,
            leaseholder: shard.leaseholder,
            start_hash: shard.start_hash,
            end_hash: shard.end_hash,
            start_key: shard.start_key,
            end_key: shard.end_key,
        })
        .collect::<Vec<_>>();
    shards.sort_by_key(|shard| shard.shard_index);

    Ok(ClusterTopology { members, shards })
}

/// Produces shard-aware scan segments for a requested key range.
pub fn scan_targets(
    topology: &ClusterTopology,
    local_target: SocketAddr,
    scan_start: &[u8],
    scan_end: &[u8],
    preferred_shards: &[usize],
) -> Vec<ScanTarget> {
    let use_range_mode = topology
        .shards
        .iter()
        .any(|shard| !shard.start_key.is_empty() || !shard.end_key.is_empty());

    let mut targets = Vec::new();
    for shard in &topology.shards {
        // Decision: honor shard pinning when preferred shard ids are supplied.
        if !preferred_shards.is_empty() && !preferred_shards.contains(&shard.shard_index) {
            continue;
        }

        let grpc_addr = topology
            .members
            .get(&shard.leaseholder)
            .filter(|member| member_is_usable(member))
            .map(|member| member.grpc_addr)
            .unwrap_or(local_target);

        // Decision: if topology provides key-range metadata, intersect the scan
        // request with each shard range; otherwise forward the full scan to each
        // selected shard and rely on hash partitioning logic upstream.
        let (effective_start, effective_end) = if use_range_mode {
            let start = max_bytes(scan_start, shard.start_key.as_slice());
            let end = min_end_bound(scan_end, shard.end_key.as_slice());
            (start, end)
        } else {
            (scan_start.to_vec(), scan_end.to_vec())
        };

        // Decision: skip empty intersections to avoid no-op RPC scans.
        if !effective_end.is_empty() && effective_start >= effective_end {
            continue;
        }

        targets.push(ScanTarget {
            shard_index: shard.shard_index,
            grpc_addr,
            start_key: effective_start,
            end_key: effective_end,
        });
    }

    // Decision: always return at least one target so callers can still execute
    // in degraded mode even if topology is temporarily empty.
    // Decision: evaluate `if targets.is_empty() {` to choose the correct SQL/storage control path.
    if targets.is_empty() {
        let shard_index = preferred_shards.first().copied().unwrap_or(0);
        targets.push(ScanTarget {
            shard_index,
            grpc_addr: local_target,
            start_key: scan_start.to_vec(),
            end_key: scan_end.to_vec(),
        });
    }

    targets
}

/// Routes one key to a shard leaseholder using range or hash metadata.
pub fn route_key(
    topology: &ClusterTopology,
    local_target: SocketAddr,
    key: &[u8],
    preferred_shards: &[usize],
) -> Option<KeyRoute> {
    let allowed = |shard: &&TopologyShard| {
        preferred_shards.is_empty() || preferred_shards.contains(&shard.shard_index)
    };
    let use_range_mode = topology
        .shards
        .iter()
        .any(|shard| !shard.start_key.is_empty() || !shard.end_key.is_empty());

    // Decision: prefer explicit key ranges when present because they provide
    // deterministic routing independent of hash scheme changes.
    let shard = if use_range_mode {
        topology
            .shards
            .iter()
            .filter(allowed)
            .find(|shard| key_in_range(key, shard.start_key.as_slice(), shard.end_key.as_slice()))
            .or_else(|| topology.shards.iter().filter(allowed).next())
    } else {
        let hash = hash_key(key);
        let mut candidates = topology.shards.iter().filter(allowed).collect::<Vec<_>>();
        // Decision: if no allowed shards exist, routing fails fast.
        if candidates.is_empty() {
            None
        // Decision: evaluate `} else if candidates` to choose the correct SQL/storage control path.
        } else if candidates
            .iter()
            .all(|shard| shard.start_hash == 0 && shard.end_hash == 0)
        {
            // Decision: when hash ranges are absent, distribute deterministically
            // by modulo over sorted shard indices.
            candidates.sort_by_key(|shard| shard.shard_index);
            let idx = (hash as usize) % candidates.len();
            Some(candidates[idx])
        } else {
            // Decision: when ranges exist, choose containing range and fall back
            // to first candidate to keep routing available under partial metadata.
            candidates.sort_by_key(|shard| shard.shard_index);
            candidates
                .iter()
                .copied()
                .find(|shard| hash >= shard.start_hash && hash <= shard.end_hash)
                .or_else(|| candidates.first().copied())
        }
    };

    shard.map(|shard| KeyRoute {
        shard_index: shard.shard_index,
        grpc_addr: topology
            .members
            .get(&shard.leaseholder)
            .filter(|member| member_is_usable(member))
            .map(|member| member.grpc_addr)
            .unwrap_or(local_target),
        start_key: shard.start_key.clone(),
        end_key: shard.end_key.clone(),
    })
}

/// Returns `true` when a member should receive traffic.
fn member_is_usable(member: &TopologyMember) -> bool {
    !member.state.eq_ignore_ascii_case("removed")
}

/// Returns the lexicographically larger of two byte keys.
pub fn max_bytes(left: &[u8], right: &[u8]) -> Vec<u8> {
    // Decision: treat empty `right` as unbounded and preserve `left`.
    if right.is_empty() || left >= right {
        left.to_vec()
    } else {
        right.to_vec()
    }
}

/// Returns the tighter exclusive end bound across two optional bounds.
pub fn min_end_bound(left: &[u8], right: &[u8]) -> Vec<u8> {
    // Decision: empty bounds represent "no upper bound", so choose the
    // concrete bound when only one side is bounded.
    // Decision: evaluate `match (left.is_empty(), right.is_empty()) {` to choose the correct SQL/storage control path.
    match (left.is_empty(), right.is_empty()) {
        (true, true) => Vec::new(),
        (true, false) => right.to_vec(),
        (false, true) => left.to_vec(),
        (false, false) => {
            // Decision: with two bounded ends, keep the smaller one to preserve
            // intersection semantics.
            // Decision: evaluate `if left <= right {` to choose the correct SQL/storage control path.
            if left <= right {
                left.to_vec()
            } else {
                right.to_vec()
            }
        }
    }
}

/// Checks whether `key` is within `[start, end)` where empty bounds are open.
pub fn key_in_range(key: &[u8], start: &[u8], end: &[u8]) -> bool {
    let in_start = start.is_empty() || key >= start;
    let in_end = end.is_empty() || key < end;
    in_start && in_end
}

/// Computes a stable u64 hash for key-based shard routing.
pub fn hash_key(key: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Wire model for cluster state JSON payload.
#[derive(Debug, Deserialize)]
struct ClusterStateView {
    #[serde(default)]
    members: BTreeMap<String, MemberView>,
    #[serde(default)]
    shards: Vec<ShardView>,
}

/// Wire model for member entries in cluster state JSON.
#[derive(Debug, Deserialize)]
struct MemberView {
    node_id: u64,
    grpc_addr: String,
    #[serde(default)]
    state: String,
}

/// Wire model for shard entries in cluster state JSON.
#[derive(Debug, Deserialize)]
struct ShardView {
    shard_id: u64,
    shard_index: usize,
    leaseholder: u64,
    start_hash: u64,
    end_hash: u64,
    #[serde(default)]
    start_key: Vec<u8>,
    #[serde(default)]
    end_key: Vec<u8>,
}

/// Validates that topology contains at least one shard descriptor.
pub fn require_non_empty_topology(topology: &ClusterTopology) -> Result<()> {
    // Decision: fail early when topology has no shards because routing/scans
    // cannot execute safely without at least one destination.
    // Decision: evaluate `if topology.shards.is_empty() {` to choose the correct SQL/storage control path.
    if topology.shards.is_empty() {
        return Err(anyhow!("cluster topology has no shards"));
    }
    Ok(())
}
