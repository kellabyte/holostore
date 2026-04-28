//! Workload generator for exercising HoloStore via the Redis protocol.
//!
//! This binary issues GET/SET operations across one or more nodes, records a
//! Porcupine-compatible history, and can be used for linearizability checks.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame;
use sha2::{Digest, Sha256};
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

    /// Key prefix/namespace. Keys are generated as `{key_prefix}k{idx}`.
    #[arg(long, default_value = "holo_")]
    key_prefix: String,

    /// Value prefix/namespace. Values are generated as `{value_prefix}c{client}:{seq}`.
    #[arg(long, default_value = "")]
    value_prefix: String,

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

    /// Optional sibling path for machine-readable workload summary JSON.
    #[arg(long)]
    summary_out: Option<PathBuf>,

    /// Encode SET values with a key-bound checksum payload.
    #[arg(long, default_value_t = false)]
    checksum_values: bool,
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
    value_prefix: String,
    set_pct: u8,
    duration_ms: u64,
    seed: u64,
    start_unix_us: u64,
    fault_disconnect_pct: u8,
    checksum_values: bool,
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

/// Machine-readable operation summary written next to the history file.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default, PartialEq, Eq)]
struct WorkloadSummary {
    ops: usize,
    ok_sets: usize,
    value_gets: usize,
    nil_gets: usize,
    errors: usize,
    seed: u64,
    history_path: String,
    checksum_values: bool,
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

    let keyspace = build_keyspace(&args.key_prefix, args.keys);

    let start = time::Instant::now();
    let start_unix_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0);
    let deadline = start + duration;

    let mut tasks = Vec::with_capacity(args.clients);
    for client_id in 0..args.clients {
        let read_node = read_nodes[client_id % read_nodes.len()];
        let read_node_str = read_node.to_string();
        let write_node = write_nodes[client_id % write_nodes.len()];
        let write_node_str = write_node.to_string();
        let keyspace = keyspace.clone();
        let value_prefix = args.value_prefix.clone();
        let op_timeout: Duration = args.op_timeout.into();
        let fail_fast = args.fail_fast;
        let set_pct = args.set_pct;
        let fault_disconnect_pct = args.fault_disconnect_pct;
        let checksum_values = args.checksum_values;
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
                value_prefix,
                set_pct,
                seed,
                start,
                deadline,
                op_timeout,
                fail_fast,
                fault_disconnect_pct,
                checksum_values,
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
        value_prefix: args.value_prefix.clone(),
        set_pct: args.set_pct,
        duration_ms: duration.as_millis() as u64,
        seed,
        start_unix_us,
        fault_disconnect_pct: args.fault_disconnect_pct,
        checksum_values: args.checksum_values,
    };

    let history = History { meta, ops: all_ops };
    write_history(&args.out, &history).context("write history")?;
    let summary_path = resolve_summary_path(&args.out, args.summary_out.as_ref());
    let summary = build_summary(&history.ops, seed, &args.out, args.checksum_values);
    write_summary(&summary_path, &summary).context("write workload summary")?;
    eprintln!("wrote history: {}", args.out.display());
    eprintln!("wrote summary: {}", summary_path.display());
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
    value_prefix: String,
    set_pct: u8,
    seed: u64,
    start: time::Instant,
    deadline: time::Instant,
    op_timeout: Duration,
    fail_fast: bool,
    fault_disconnect_pct: u8,
    checksum_values: bool,
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
            let value = build_value(&value_prefix, checksum_values, &key, client_id, seq);
            (OpKind::Set, Some(value.clone()), make_set(&key, &value))
        } else {
            (OpKind::Get, None, make_get(&key))
        };

        let call_us = start.elapsed().as_micros() as u64;
        let node_str = if kind == OpKind::Set {
            if should_inject_fault(&mut rng, fault_disconnect_pct) {
                write_conn = connect(write_node).await?;
            }
            write_node_str.clone()
        } else {
            if should_inject_fault(&mut rng, fault_disconnect_pct) {
                read_conn = connect(read_node).await?;
            }
            read_node_str.clone()
        };

        // Execute each request with per-op send/receive timeouts.
        let response = if kind == OpKind::Set {
            execute_request(&mut write_conn, req, op_timeout).await
        } else {
            execute_request(&mut read_conn, req, op_timeout).await
        };
        let resp = match response {
            Ok(frame) => frame,
            Err(error) => {
                let return_us = start.elapsed().as_micros() as u64;
                ops.push(OpRecord {
                    client: client_id,
                    node: node_str,
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
                    anyhow::bail!("client {client_id} operation failed: {error}");
                }
                if kind == OpKind::Set {
                    write_conn = connect(write_node).await?;
                } else {
                    read_conn = connect(read_node).await?;
                }
                continue;
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

/// Send one request and await the matching response with bounded timeouts.
async fn execute_request(
    conn: &mut Framed<TcpStream, Resp2>,
    req: BytesFrame,
    op_timeout: Duration,
) -> Result<BytesFrame, String> {
    let send_result = time::timeout(op_timeout, conn.send(req)).await;
    match send_result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => return Err(format!("send failed: {err}")),
        Err(_) => return Err("send timed out".to_string()),
    }

    let recv = time::timeout(op_timeout, conn.next()).await;
    match recv {
        Ok(Some(Ok(frame))) => Ok(frame),
        Ok(Some(Err(err))) => Err(format!("recv failed: {err}")),
        Ok(None) => Err("connection closed".to_string()),
        Err(_) => Err("recv timed out".to_string()),
    }
}

fn should_inject_fault(rng: &mut SmallRng, pct: u8) -> bool {
    if pct == 0 {
        return false;
    }
    rng.gen_range(0..100) < pct as u32
}

/// Build the deterministic keyspace for a workload run.
fn build_keyspace(key_prefix: &str, keys: usize) -> Vec<String> {
    (0..keys).map(|i| format!("{key_prefix}k{i}")).collect()
}

/// Build the logical scenario label embedded in checksum-mode values.
fn scenario_label(value_prefix: &str) -> String {
    let trimmed = value_prefix.trim_matches('_');
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Construct the SET payload for one logical client operation.
fn build_value(
    value_prefix: &str,
    checksum_values: bool,
    key: &str,
    client_id: usize,
    seq: u64,
) -> String {
    if !checksum_values {
        return format!("{value_prefix}c{client_id}:{seq}");
    }

    let prefix = format!(
        "scenario={};key={key};client={client_id};seq={seq}",
        scenario_label(value_prefix)
    );
    format!("{prefix};checksum={}", sha256_hex(prefix.as_bytes()))
}

/// Render a digest as lowercase hexadecimal without adding another dependency.
fn sha256_hex(input: &[u8]) -> String {
    let digest = Sha256::digest(input);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
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

/// Resolve the workload summary output path, defaulting to a sibling JSON file.
fn resolve_summary_path(history_path: &PathBuf, explicit: Option<&PathBuf>) -> PathBuf {
    explicit
        .cloned()
        .unwrap_or_else(|| history_path.with_extension("summary.json"))
}

/// Build a compact summary that Antithesis drivers can assert on directly.
fn build_summary(
    ops: &[OpRecord],
    seed: u64,
    history_path: &PathBuf,
    checksum_values: bool,
) -> WorkloadSummary {
    let mut summary = WorkloadSummary {
        ops: ops.len(),
        seed,
        history_path: history_path.display().to_string(),
        checksum_values,
        ..WorkloadSummary::default()
    };

    for op in ops {
        match (&op.op, &op.result) {
            (OpKind::Set, OpResult::Ok) => summary.ok_sets += 1,
            (OpKind::Get, OpResult::Value { .. }) => summary.value_gets += 1,
            (OpKind::Get, OpResult::Nil) => summary.nil_gets += 1,
            (_, OpResult::Err { .. }) => summary.errors += 1,
            _ => {}
        }
    }

    summary
}

/// Serialize and write the workload summary JSON.
fn write_summary(path: &PathBuf, summary: &WorkloadSummary) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dir {}", parent.display()))?;
    }
    let data = serde_json::to_vec_pretty(summary).context("serialize summary")?;
    std::fs::write(path, data).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        build_keyspace, build_summary, build_value, resolve_summary_path, sha256_hex, OpKind,
        OpRecord, OpResult,
    };
    use std::path::PathBuf;

    /// Checksum-mode values must be stable and key-bound.
    #[test]
    fn checksum_value_contains_expected_fields() {
        let value = build_value("range_churn_", true, "antithesis_shared_k0", 2, 7);
        let expected_prefix =
            "scenario=range_churn;key=antithesis_shared_k0;client=2;seq=7".to_string();
        let expected = format!(
            "{expected_prefix};checksum={}",
            sha256_hex(expected_prefix.as_bytes())
        );
        assert_eq!(value, expected);
    }

    /// Shared-key workloads should be able to opt out of seed-derived key names.
    #[test]
    fn keyspace_uses_literal_prefix() {
        assert_eq!(
            build_keyspace("antithesis_shared_", 3),
            vec![
                "antithesis_shared_k0".to_string(),
                "antithesis_shared_k1".to_string(),
                "antithesis_shared_k2".to_string(),
            ]
        );
    }

    /// Workload summaries should default to a sibling path next to the history.
    #[test]
    fn summary_path_defaults_to_sibling_file() {
        let history = PathBuf::from("/tmp/history-singleton.json");
        assert_eq!(
            resolve_summary_path(&history, None),
            PathBuf::from("/tmp/history-singleton.summary.json")
        );
    }

    /// Summary counters should distinguish successful reads, writes, and errors.
    #[test]
    fn summary_counts_operations() {
        let ops = vec![
            OpRecord {
                client: 0,
                node: "n1".to_string(),
                op: OpKind::Set,
                key: "k0".to_string(),
                value: Some("v0".to_string()),
                call_us: 1,
                return_us: 2,
                result: OpResult::Ok,
            },
            OpRecord {
                client: 0,
                node: "n1".to_string(),
                op: OpKind::Get,
                key: "k0".to_string(),
                value: None,
                call_us: 3,
                return_us: 4,
                result: OpResult::Value {
                    value: "v0".to_string(),
                },
            },
            OpRecord {
                client: 1,
                node: "n2".to_string(),
                op: OpKind::Get,
                key: "k1".to_string(),
                value: None,
                call_us: 5,
                return_us: 6,
                result: OpResult::Err {
                    error: "boom".to_string(),
                },
            },
        ];

        let summary = build_summary(&ops, 42, &PathBuf::from("/tmp/history.json"), true);
        assert_eq!(summary.ops, 3);
        assert_eq!(summary.ok_sets, 1);
        assert_eq!(summary.value_gets, 1);
        assert_eq!(summary.nil_gets, 0);
        assert_eq!(summary.errors, 1);
        assert!(summary.checksum_values);
    }
}
