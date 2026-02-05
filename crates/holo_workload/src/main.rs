//! Workload generator for exercising HoloStore via the Redis protocol.
//!
//! This binary issues GET/SET operations across one or more nodes, records a
//! Porcupine-compatible history, and can be used for linearizability checks.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame;
use tokio::net::TcpStream;
use tokio::time;
use tokio_util::codec::Framed;

/// CLI entry point wrapper.
#[derive(Parser, Debug)]
#[command(name = "holo-workload")]
struct Args {
    #[command(subcommand)]
    cmd: Command,
}

/// Top-level CLI subcommands.
#[derive(Subcommand, Debug)]
enum Command {
    Run(RunArgs),
}

/// CLI options for running the workload.
#[derive(Parser, Debug, Clone)]
struct RunArgs {
    /// Comma-separated Redis endpoints (RESP), e.g. `127.0.0.1:16379,127.0.0.1:16380`
    #[arg(long)]
    nodes: String,

    /// Optional read nodes (GETs only). Defaults to --nodes when empty.
    #[arg(long)]
    read_nodes: Option<String>,

    /// Optional write nodes (SETs only). Defaults to --nodes when empty.
    #[arg(long)]
    write_nodes: Option<String>,

    /// Number of concurrent clients (each client uses one TCP connection).
    #[arg(long, default_value_t = 10)]
    clients: usize,

    /// Number of hot keys used by the workload.
    #[arg(long, default_value_t = 5)]
    keys: usize,

    /// Key prefix/namespace. Keys are generated as `{key_prefix}{seed}_k{idx}`.
    #[arg(long, default_value = "holo_")]
    key_prefix: String,

    /// Percent of operations that are SET (rest are GET).
    #[arg(long, default_value_t = 50)]
    set_pct: u8,

    /// Total runtime for the workload.
    #[arg(long, default_value = "30s")]
    duration: humantime::Duration,

    /// Random seed (0 picks a random seed).
    #[arg(long, default_value_t = 0)]
    seed: u64,

    /// Per-operation timeout (network + server response).
    #[arg(long, default_value = "10s")]
    op_timeout: humantime::Duration,

    /// When true, any operation error aborts the run.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    fail_fast: bool,

    /// Percentage of operations that force a client reconnect before issuing the op.
    #[arg(long, default_value_t = 0)]
    fault_disconnect_pct: u8,

    /// Write a JSON history to this path (Porcupine input).
    #[arg(long, default_value = ".tmp/porcupine/history.json")]
    out: PathBuf,
}

/// Metadata embedded in the history file for reproducibility.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct HistoryMeta {
    nodes: Vec<String>,
    read_nodes: Vec<String>,
    write_nodes: Vec<String>,
    clients: usize,
    keys: usize,
    key_prefix: String,
    set_pct: u8,
    duration_ms: u64,
    seed: u64,
    fault_disconnect_pct: u8,
}

/// Full workload history serialized for Porcupine.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct History {
    meta: HistoryMeta,
    ops: Vec<OpRecord>,
}

/// Single operation record captured during the workload run.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct OpRecord {
    client: usize,
    node: String,
    op: OpKind,
    key: String,
    value: Option<String>,
    call_us: u64,
    return_us: u64,
    result: OpResult,
}

/// Operation kind (GET or SET).
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum OpKind {
    Get,
    Set,
}

/// Result of an operation with structured error/value encoding.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OpResult {
    Ok,
    Nil,
    Value { value: String },
    Err { error: String },
}

#[tokio::main]
/// Parse CLI args and dispatch to the selected subcommand.
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.cmd {
        // Run the workload generator.
        Command::Run(args) => run(args).await,
    }
}

/// Run the workload and write a Porcupine history file.
async fn run(args: RunArgs) -> anyhow::Result<()> {
    anyhow::ensure!(args.clients > 0, "--clients must be > 0");
    anyhow::ensure!(args.keys > 0, "--keys must be > 0");
    anyhow::ensure!(args.set_pct <= 100, "--set-pct must be <= 100");
    anyhow::ensure!(
        args.fault_disconnect_pct <= 100,
        "--fault-disconnect-pct must be <= 100"
    );

    let nodes = parse_nodes(&args.nodes)?;
    anyhow::ensure!(!nodes.is_empty(), "--nodes must not be empty");
    let read_nodes = parse_nodes(args.read_nodes.as_deref().unwrap_or(&args.nodes))?;
    anyhow::ensure!(!read_nodes.is_empty(), "--read-nodes must not be empty");
    let write_nodes = parse_nodes(args.write_nodes.as_deref().unwrap_or(&args.nodes))?;
    anyhow::ensure!(!write_nodes.is_empty(), "--write-nodes must not be empty");

    let duration: Duration = args.duration.into();
    // Use a random seed when the user provides zero.
    let seed = if args.seed == 0 {
        rand::thread_rng().gen()
    } else {
        args.seed
    };

    let keyspace = (0..args.keys)
        .map(|i| format!("{}{}_k{i}", args.key_prefix, seed))
        .collect::<Vec<_>>();

    let start = time::Instant::now();
    let deadline = start + duration;

    let mut tasks = Vec::with_capacity(args.clients);
    for client_id in 0..args.clients {
        let read_node = read_nodes[client_id % read_nodes.len()];
        let read_node_str = read_node.to_string();
        let write_node = write_nodes[client_id % write_nodes.len()];
        let write_node_str = write_node.to_string();
        let keyspace = keyspace.clone();
        let op_timeout: Duration = args.op_timeout.into();
        let fail_fast = args.fail_fast;
        let set_pct = args.set_pct;
        let fault_disconnect_pct = args.fault_disconnect_pct;
        // Mix the base seed with the client id for deterministic per-client RNG.
        let seed = seed ^ (client_id as u64).wrapping_mul(0x9e3779b97f4a7c15);
        tasks.push(tokio::spawn(async move {
            run_client(
                client_id,
                read_node,
                read_node_str,
                write_node,
                write_node_str,
                keyspace,
                set_pct,
                seed,
                start,
                deadline,
                op_timeout,
                fail_fast,
                fault_disconnect_pct,
            )
            .await
        }));
    }

    let mut all_ops = Vec::new();
    for task in tasks {
        let mut ops = task.await.context("client task panicked")??;
        all_ops.append(&mut ops);
    }

    // Sort ops for deterministic history ordering.
    all_ops.sort_by_key(|op| (op.call_us, op.client));

    let meta = HistoryMeta {
        nodes: nodes.iter().map(|n| n.to_string()).collect(),
        read_nodes: read_nodes.iter().map(|n| n.to_string()).collect(),
        write_nodes: write_nodes.iter().map(|n| n.to_string()).collect(),
        clients: args.clients,
        keys: args.keys,
        key_prefix: args.key_prefix.clone(),
        set_pct: args.set_pct,
        duration_ms: duration.as_millis() as u64,
        seed,
        fault_disconnect_pct: args.fault_disconnect_pct,
    };

    let history = History { meta, ops: all_ops };
    write_history(&args.out, &history).context("write history")?;
    eprintln!("wrote history: {}", args.out.display());
    Ok(())
}

/// Run a single client connection until the deadline, returning its op history.
async fn run_client(
    client_id: usize,
    read_node: SocketAddr,
    read_node_str: String,
    write_node: SocketAddr,
    write_node_str: String,
    keyspace: Vec<String>,
    set_pct: u8,
    seed: u64,
    start: time::Instant,
    deadline: time::Instant,
    op_timeout: Duration,
    fail_fast: bool,
    fault_disconnect_pct: u8,
) -> anyhow::Result<Vec<OpRecord>> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut ops = Vec::new();

    let mut read_conn = connect(read_node).await?;
    let mut write_conn = connect(write_node).await?;

    let mut seq = 0u64;
    while time::Instant::now() < deadline {
        seq += 1;
        let key = keyspace[rng.gen_range(0..keyspace.len())].clone();
        let do_set = rng.gen_range(0..100) < (set_pct as u32);

        // Decide whether this operation is a SET or GET.
        let (kind, value, req) = if do_set {
            let value = format!("c{client_id}:{seq}");
            (OpKind::Set, Some(value.clone()), make_set(&key, &value))
        } else {
            (OpKind::Get, None, make_get(&key))
        };

        let call_us = start.elapsed().as_micros() as u64;
        let (conn, node_str) = if kind == OpKind::Set {
            if should_inject_fault(&mut rng, fault_disconnect_pct) {
                write_conn = connect(write_node).await?;
            }
            (&mut write_conn, write_node_str.clone())
        } else {
            if should_inject_fault(&mut rng, fault_disconnect_pct) {
                read_conn = connect(read_node).await?;
            }
            (&mut read_conn, read_node_str.clone())
        };

        // Enforce per-op timeouts for send.
        let send_result = time::timeout(op_timeout, conn.send(req)).await;
        let send_err = match send_result {
            Ok(Ok(())) => None,
            Ok(Err(err)) => Some(format!("{err}")),
            Err(_) => Some("send timed out".to_string()),
        };

        if let Some(error) = send_err {
            let return_us = start.elapsed().as_micros() as u64;
            ops.push(OpRecord {
                client: client_id,
                node: node_str.clone(),
                op: kind,
                key,
                value,
                call_us,
                return_us,
                result: OpResult::Err {
                    error: error.clone(),
                },
            });
            if fail_fast {
                // Abort immediately on send failure when fail-fast is enabled.
                anyhow::bail!("client {client_id} send failed: {error}");
            }
            break;
        }

        // Enforce per-op timeouts for receive.
        let recv = time::timeout(op_timeout, conn.next()).await;
        let resp = match recv {
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(err))) => {
                let return_us = start.elapsed().as_micros() as u64;
                ops.push(OpRecord {
                    client: client_id,
                    node: node_str.clone(),
                    op: kind,
                    key,
                    value,
                    call_us,
                    return_us,
                    result: OpResult::Err {
                        error: format!("recv failed: {err}"),
                    },
                });
                if fail_fast {
                    // Abort immediately on receive failure when fail-fast is enabled.
                    anyhow::bail!("client {client_id} recv failed: {err}");
                }
                break;
            }
            Ok(None) => {
                let return_us = start.elapsed().as_micros() as u64;
                ops.push(OpRecord {
                    client: client_id,
                    node: node_str.clone(),
                    op: kind,
                    key,
                    value,
                    call_us,
                    return_us,
                    result: OpResult::Err {
                        error: "connection closed".to_string(),
                    },
                });
                if fail_fast {
                    // Abort when the connection is closed if fail-fast is enabled.
                    anyhow::bail!("client {client_id} connection closed");
                }
                break;
            }
            Err(_) => {
                let return_us = start.elapsed().as_micros() as u64;
                ops.push(OpRecord {
                    client: client_id,
                    node: node_str.clone(),
                    op: kind,
                    key,
                    value,
                    call_us,
                    return_us,
                    result: OpResult::Err {
                        error: "recv timed out".to_string(),
                    },
                });
                if fail_fast {
                    // Abort on timeout if fail-fast is enabled.
                    anyhow::bail!("client {client_id} recv timed out");
                }
                break;
            }
        };

        let return_us = start.elapsed().as_micros() as u64;
        // Parse the server response based on operation type.
        let result = match kind {
            OpKind::Set => parse_set_response(resp),
            OpKind::Get => parse_get_response(resp),
        };

        ops.push(OpRecord {
            client: client_id,
            node: node_str,
            op: kind,
            key,
            value,
            call_us,
            return_us,
            result,
        });
    }

    Ok(ops)
}

async fn connect(node: SocketAddr) -> anyhow::Result<Framed<TcpStream, Resp2>> {
    let socket = TcpStream::connect(node)
        .await
        .with_context(|| format!("connect to {node}"))?;
    socket.set_nodelay(true).ok();
    Ok(Framed::new(socket, Resp2::default()))
}

fn should_inject_fault(rng: &mut SmallRng, pct: u8) -> bool {
    if pct == 0 {
        return false;
    }
    rng.gen_range(0..100) < pct as u32
}

/// Parse a comma-separated list of `host:port` addresses.
fn parse_nodes(input: &str) -> anyhow::Result<Vec<SocketAddr>> {
    let mut out = Vec::new();
    for part in input.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        out.push(
            part.parse::<SocketAddr>()
                .with_context(|| format!("invalid node address {part:?} (expected host:port)"))?,
        );
    }
    Ok(out)
}

/// Build a RESP GET request frame.
fn make_get(key: &str) -> BytesFrame {
    BytesFrame::Array(vec![
        BytesFrame::BulkString(Bytes::from_static(b"GET")),
        BytesFrame::BulkString(Bytes::from(key.as_bytes().to_vec())),
    ])
}

/// Build a RESP SET request frame.
fn make_set(key: &str, value: &str) -> BytesFrame {
    BytesFrame::Array(vec![
        BytesFrame::BulkString(Bytes::from_static(b"SET")),
        BytesFrame::BulkString(Bytes::from(key.as_bytes().to_vec())),
        BytesFrame::BulkString(Bytes::from(value.as_bytes().to_vec())),
    ])
}

/// Interpret a RESP SET response as an `OpResult`.
fn parse_set_response(resp: BytesFrame) -> OpResult {
    match resp {
        BytesFrame::SimpleString(s) if s.as_ref() == b"OK" => OpResult::Ok,
        BytesFrame::Error(err) => OpResult::Err {
            error: err.to_string(),
        },
        other => OpResult::Err {
            error: format!("unexpected SET response: {other:?}"),
        },
    }
}

/// Interpret a RESP GET response as an `OpResult`.
fn parse_get_response(resp: BytesFrame) -> OpResult {
    match resp {
        BytesFrame::Null => OpResult::Nil,
        BytesFrame::BulkString(bytes) | BytesFrame::SimpleString(bytes) => OpResult::Value {
            value: String::from_utf8_lossy(&bytes).to_string(),
        },
        BytesFrame::Error(err) => OpResult::Err {
            error: err.to_string(),
        },
        other => OpResult::Err {
            error: format!("unexpected GET response: {other:?}"),
        },
    }
}

/// Serialize and write the workload history JSON.
fn write_history(path: &PathBuf, history: &History) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        // Ensure the output directory exists before writing.
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dir {}", parent.display()))?;
    }
    let data = serde_json::to_vec_pretty(history).context("serialize history")?;
    std::fs::write(path, data).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}
