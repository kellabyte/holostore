//! Meta-plane failover/recovery integration test.
//!
//! This test validates that control-plane operations continue to make progress
//! when the bootstrap node crashes, and that a restarted node converges to the
//! latest metadata state.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use common::{
    cleanup_dir, holoctl_bin, pick_free_port, spawn_node_custom_env, test_dir, wait_for_port,
    wait_for_redis_ready, IO_TIMEOUT,
};
use serde_json::Value;

const TEST_TIMEOUT: Duration = Duration::from_secs(120);

fn run_holoctl(target: SocketAddr, args: &[&str]) -> String {
    let mut cmd = Command::new(holoctl_bin());
    cmd.arg("--target").arg(target.to_string());
    for arg in args {
        cmd.arg(arg);
    }
    let out = cmd.output().expect("run holoctl");
    if !out.status.success() {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        panic!(
            "holoctl failed for args {:?}\nstdout:\n{}\nstderr:\n{}",
            args, stdout, stderr
        );
    }
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn read_state(target: SocketAddr) -> Value {
    let text = run_holoctl(target, &["state"]);
    serde_json::from_str(&text).expect("parse cluster state json")
}

fn spawn_node(
    node_id: u64,
    data_dir: &Path,
    redis_addr: SocketAddr,
    grpc_addr: SocketAddr,
    bootstrap: bool,
    join: Option<SocketAddr>,
    members: &str,
) -> common::NodeProcess {
    let envs = [
        ("HOLO_META_GROUPS", "2"),
        ("HOLO_REBALANCE_ENABLED", "false"),
        ("HOLO_RANGE_SPLIT_MIN_KEYS", "0"),
        ("HOLO_RANGE_SPLIT_MIN_QPS", "0"),
        ("HOLO_RANGE_MERGE_MAX_KEYS", "0"),
        ("HOLO_META_SPLIT_ENABLED", "false"),
    ];
    spawn_node_custom_env(
        node_id,
        &data_dir.to_path_buf(),
        redis_addr,
        grpc_addr,
        bootstrap,
        join,
        members.to_string(),
        "local",
        4,
        &envs,
    )
}

#[test]
fn meta_plane_ops_survive_bootstrap_crash_and_recovery() {
    let base_dir = test_dir("meta-plane-failover");
    cleanup_dir(&base_dir);

    let redis_ports = [
        pick_free_port().expect("redis port 1"),
        pick_free_port().expect("redis port 2"),
        pick_free_port().expect("redis port 3"),
    ];
    let grpc_ports = [
        pick_free_port().expect("grpc port 1"),
        pick_free_port().expect("grpc port 2"),
        pick_free_port().expect("grpc port 3"),
    ];

    let redis_addrs: Vec<SocketAddr> = redis_ports
        .iter()
        .map(|p| format!("127.0.0.1:{p}").parse().unwrap())
        .collect();
    let grpc_addrs: Vec<SocketAddr> = grpc_ports
        .iter()
        .map(|p| format!("127.0.0.1:{p}").parse().unwrap())
        .collect();
    let members = format!(
        "1@{},2@{},3@{}",
        grpc_addrs[0], grpc_addrs[1], grpc_addrs[2]
    );

    let mut node1 = spawn_node(
        1,
        &base_dir.join("node1"),
        redis_addrs[0],
        grpc_addrs[0],
        true,
        None,
        &members,
    );
    wait_for_port(grpc_addrs[0], IO_TIMEOUT);
    wait_for_port(redis_addrs[0], IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addrs[0], IO_TIMEOUT),
        "node1 redis not ready"
    );

    let mut node2 = spawn_node(
        2,
        &base_dir.join("node2"),
        redis_addrs[1],
        grpc_addrs[1],
        false,
        Some(grpc_addrs[0]),
        &members,
    );
    wait_for_port(grpc_addrs[1], IO_TIMEOUT);
    wait_for_port(redis_addrs[1], IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addrs[1], IO_TIMEOUT),
        "node2 redis not ready"
    );

    let mut node3 = spawn_node(
        3,
        &base_dir.join("node3"),
        redis_addrs[2],
        grpc_addrs[2],
        false,
        Some(grpc_addrs[0]),
        &members,
    );
    wait_for_port(grpc_addrs[2], IO_TIMEOUT);
    wait_for_port(redis_addrs[2], IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addrs[2], IO_TIMEOUT),
        "node3 redis not ready"
    );

    // Simulate bootstrap/controller crash.
    node1.child.kill().ok();
    let _ = node1.child.wait();

    // Execute control-plane operations on node2 while node1 is down.
    run_holoctl(
        grpc_addrs[1],
        &[
            "add-node",
            "--node-id",
            "4",
            "--grpc-addr",
            "127.0.0.1:15054",
            "--redis-addr",
            "127.0.0.1:16382",
        ],
    );
    run_holoctl(
        grpc_addrs[1],
        &[
            "split-meta",
            "--split-hash",
            "9223372036854775808",
            "--target-meta-index",
            "0",
        ],
    );
    run_holoctl(grpc_addrs[1], &["split", "--split-key", "key:50000"]);
    run_holoctl(
        grpc_addrs[1],
        &[
            "rebalance",
            "--shard-id",
            "1",
            "--replica",
            "1",
            "--replica",
            "2",
            "--replica",
            "3",
            "--leaseholder",
            "2",
        ],
    );

    let state = read_state(grpc_addrs[1]);
    assert_eq!(
        state["members"]["4"]["state"].as_str(),
        Some("Active"),
        "member 4 should be active after add-node"
    );
    assert!(
        state["meta_ranges"].as_array().map(|v| v.len()).unwrap_or(0) >= 2,
        "meta ranges should split under split-meta command"
    );
    assert!(
        state["meta_health"]["ops_by_index"].is_object(),
        "cluster state should include meta health ops_by_index"
    );
    assert!(
        state["meta_health"]["proposal_total"]["count"].is_u64(),
        "cluster state should include meta proposal totals"
    );
    assert!(
        state["shards"].as_array().map(|v| v.len()).unwrap_or(0) >= 2,
        "data ranges should split under range split command"
    );
    let shard1_leaseholder = state["shards"]
        .as_array()
        .and_then(|arr| {
            arr.iter()
                .find(|s| s["shard_id"].as_u64() == Some(1))
                .and_then(|s| s["leaseholder"].as_u64())
        })
        .unwrap_or(0);
    assert_eq!(
        shard1_leaseholder, 2,
        "shard 1 leaseholder should transfer to node2"
    );

    // Restart node1 and ensure it converges to latest meta state.
    let mut node1 = spawn_node(
        1,
        &base_dir.join("node1"),
        redis_addrs[0],
        grpc_addrs[0],
        false,
        Some(grpc_addrs[1]),
        &members,
    );
    wait_for_port(grpc_addrs[0], TEST_TIMEOUT);
    wait_for_port(redis_addrs[0], TEST_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addrs[0], TEST_TIMEOUT),
        "node1 redis not ready after restart"
    );

    let restarted_state = read_state(grpc_addrs[0]);
    assert_eq!(
        restarted_state["members"]["4"]["state"].as_str(),
        Some("Active"),
        "restarted node should observe active member 4"
    );
    assert!(
        restarted_state["meta_ranges"]
            .as_array()
            .map(|v| v.len())
            .unwrap_or(0)
            >= 2,
        "restarted node should observe meta split"
    );

    // Keep processes alive until the end of scope.
    let _ = (&mut node1, &mut node2, &mut node3);
}
