//! Minimal admin client for cluster control-plane RPCs.

use clap::{Parser, Subcommand};

include!(concat!(env!("OUT_DIR"), "/volo_gen.rs"));

use volo_gen::holo_store::rpc;

#[derive(Parser)]
#[command(name = "cluster_admin")]
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
    /// Add or update a node in the cluster membership.
    AddNode {
        #[arg(long)]
        node_id: u64,
        #[arg(long)]
        grpc_addr: String,
        #[arg(long)]
        redis_addr: String,
    },
    /// Remove a node from the cluster membership.
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
    /// Update replica set + leaseholder for a range.
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let client = rpc::HoloRpcClientBuilder::new("holo_store.rpc.HoloRpc")
        .address(volo::net::Address::from(args.target.parse::<std::net::SocketAddr>()?))
        .build();

    match args.command {
        Command::State => {
            let resp = client
                .cluster_state(rpc::ClusterStateRequest {})
                .await?
                .into_inner();
            println!("{}", resp.json);
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
            println!("ok");
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
            println!("ok");
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
