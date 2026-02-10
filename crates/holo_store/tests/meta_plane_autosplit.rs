//! Meta-range autosplit integration test.
//!
//! This verifies that sustained control-plane write load triggers a metadata
//! range split when spare meta-group slots are available.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use common::{
    cleanup_dir, holoctl_bin, pick_free_port, spawn_node_custom_env, test_dir, wait_for_port,
    wait_for_redis_ready, IO_TIMEOUT,
};
use serde_json::Value;

const TEST_TIMEOUT: Duration = Duration::from_secs(90);

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
        ("HOLO_META_GROUPS", "3"),
        ("HOLO_REBALANCE_ENABLED", "false"),
        ("HOLO_META_BALANCE_ENABLED", "false"),
        ("HOLO_RANGE_SPLIT_MIN_KEYS", "0"),
        ("HOLO_RANGE_SPLIT_MIN_QPS", "0"),
        ("HOLO_RANGE_MERGE_MAX_KEYS", "0"),
        ("HOLO_META_SPLIT_ENABLED", "true"),
        ("HOLO_META_MGR_INTERVAL_MS", "100"),
        // `holoctl add-node` is issued serially in this test, so QPS is modest.
        // Keep thresholds low so autosplit intent is still exercised reliably.
        ("HOLO_META_SPLIT_MIN_QPS", "1"),
        ("HOLO_META_SPLIT_MIN_OPS", "10"),
        ("HOLO_META_SPLIT_QPS_SUSTAIN", "1"),
        ("HOLO_META_SPLIT_COOLDOWN_MS", "1000"),
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
        2,
        &envs,
    )
}

#[test]
fn meta_autosplit_triggers_on_sustained_control_plane_load() {
    let base_dir = test_dir("meta-autosplit");
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

    let state_before = read_state(grpc_addrs[0]);
    let initial_ranges = state_before["meta_ranges"]
        .as_array()
        .map(|v| v.len())
        .unwrap_or(0);
    assert!(
        initial_ranges >= 1,
        "cluster should start with at least one meta range"
    );

    // Drive sustained control-plane proposals.
    for i in 0..160u32 {
        let node_id = (1000 + i).to_string();
        let grpc_addr = format!("127.0.0.1:{}", 18000 + i);
        let redis_addr = format!("127.0.0.1:{}", 19000 + i);
        run_holoctl(
            grpc_addrs[0],
            &[
                "add-node",
                "--node-id",
                &node_id,
                "--grpc-addr",
                &grpc_addr,
                "--redis-addr",
                &redis_addr,
            ],
        );
    }

    let deadline = Instant::now() + TEST_TIMEOUT;
    loop {
        let state = read_state(grpc_addrs[0]);
        let ranges = state["meta_ranges"]
            .as_array()
            .map(|v| v.len())
            .unwrap_or(0);
        if ranges > initial_ranges {
            break;
        }
        if Instant::now() >= deadline {
            panic!("meta autosplit did not trigger in time");
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let _ = (&mut node1, &mut node2, &mut node3);
}
