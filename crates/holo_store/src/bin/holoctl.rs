//! Minimal admin client for cluster control-plane RPCs.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;

use anyhow::Context;
use clap::{Parser, Subcommand};
use serde::Deserialize;

include!(concat!(env!("OUT_DIR"), "/volo_gen.rs"));

use volo_gen::holo_store::rpc;

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
    /// Show per-node range responsibilities and local record counts.
    Topology,
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
}

#[derive(Debug, Clone, Deserialize)]
struct ClusterStateView {
    members: BTreeMap<String, MemberView>,
    shards: Vec<ShardView>,
    #[serde(default)]
    shard_rebalances: BTreeMap<String, ReplicaMoveView>,
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
struct ReplicaMoveView {
    from_node: u64,
    to_node: u64,
    phase: String,
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
        Command::Topology => {
            let resp = client
                .cluster_state(rpc::ClusterStateRequest {})
                .await?
                .into_inner();
            let state: ClusterStateView =
                serde_json::from_str(&resp.json).context("parse cluster state json")?;
            let mut members = state.members.values().cloned().collect::<Vec<_>>();
            members.sort_by_key(|m| m.node_id);
            let mut moves_by_shard = HashMap::new();
            for (shard_id, mv) in &state.shard_rebalances {
                if let Ok(id) = shard_id.parse::<u64>() {
                    moves_by_shard.insert(id, mv.clone());
                }
            }

            // Query local record counts from each non-removed member.
            let mut counts_by_node: HashMap<u64, HashMap<u64, u64>> = HashMap::new();
            let mut fetch_errors: HashMap<u64, String> = HashMap::new();
            for member in &members {
                if member.state == "Removed" {
                    continue;
                }
                match fetch_range_stats(&member.grpc_addr).await {
                    Ok(map) => {
                        counts_by_node.insert(member.node_id, map);
                    }
                    Err(err) => {
                        fetch_errors.insert(member.node_id, err.to_string());
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
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "n/a".to_string())
                    };
                    let move_status =
                        format_move_status(member.node_id, shard.shard_id, &moves_by_shard);
                    rows.push(vec![
                        member.node_id.to_string(),
                        member.state.clone(),
                        role.to_string(),
                        shard.shard_id.to_string(),
                        format_range(&shard.start_key, &shard.end_key),
                        records,
                        move_status,
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
                        "-".to_string(),
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

            if rows.is_empty() {
                println!("no shard responsibilities found");
            } else {
                print_ascii_table(
                    &["NODE", "STATE", "ROLE", "SHARD", "RANGE", "RECORDS", "MOVE"],
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
        Command::Merge { left_shard_id } => {
            client
                .range_merge(rpc::RangeMergeRequest { left_shard_id })
                .await?;
            println!("ok");
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
        return "-".to_string();
    };
    if node_id == mv.to_node {
        format!("incoming:{}", mv.phase)
    } else if node_id == mv.from_node {
        format!("outgoing:{}", mv.phase)
    } else {
        "-".to_string()
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
            return s.to_string();
        }
    }
    format!("0x{}", hex_encode(key))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from(b"0123456789abcdef"[(b >> 4) as usize]));
        out.push(char::from(b"0123456789abcdef"[(b & 0x0f) as usize]));
    }
    out
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
