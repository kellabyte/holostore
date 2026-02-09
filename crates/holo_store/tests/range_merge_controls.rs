//! Integration tests for merge control-plane safety controls.

mod common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use common::{cleanup_dir, pick_free_port, spawn_node_custom_env, test_dir, wait_for_redis_ready};

#[test]
fn merge_pause_resume_cancel_controls_work() {
    let dir = test_dir("range-merge-controls");
    let node1_dir = dir.join("node1");
    let node2_dir = dir.join("node2");
    let node3_dir = dir.join("node3");
    let _ = std::fs::create_dir_all(&node1_dir);
    let _ = std::fs::create_dir_all(&node2_dir);
    let _ = std::fs::create_dir_all(&node3_dir);

    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let redis1 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let redis2 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let redis3 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let grpc1 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let grpc2 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let grpc3 = SocketAddr::new(ip, pick_free_port().expect("port"));

    let initial_members = format!("1@{grpc1},2@{grpc2},3@{grpc3}");
    let envs = [
        ("HOLO_ROUTING_MODE", "range"),
        ("HOLO_INITIAL_RANGES", "2"),
        ("HOLO_RANGE_SPLIT_MIN_KEYS", "1000000"),
        ("HOLO_RANGE_SPLIT_MIN_QPS", "0"),
        ("HOLO_RANGE_MERGE_MAX_KEYS", "0"),
        ("HOLO_REBALANCE_ENABLED", "false"),
    ];

    let mut n1 = spawn_node_custom_env(
        1,
        &node1_dir,
        redis1,
        grpc1,
        true,
        None,
        initial_members.clone(),
        "accord",
        4,
        &envs,
    );
    let mut n2 = spawn_node_custom_env(
        2,
        &node2_dir,
        redis2,
        grpc2,
        false,
        Some(grpc1),
        initial_members.clone(),
        "accord",
        4,
        &envs,
    );
    let mut n3 = spawn_node_custom_env(
        3,
        &node3_dir,
        redis3,
        grpc3,
        false,
        Some(grpc1),
        initial_members.clone(),
        "accord",
        4,
        &envs,
    );

    assert!(wait_for_redis_ready(redis1, Duration::from_secs(20)));
    assert!(wait_for_redis_ready(redis2, Duration::from_secs(20)));
    assert!(wait_for_redis_ready(redis3, Duration::from_secs(20)));

    let mut conn = common::RespConn::connect(redis1);
    let quorum_deadline = Instant::now() + Duration::from_secs(20);
    loop {
        n1.assert_running("waiting for write quorum");
        n2.assert_running("waiting for write quorum");
        n3.assert_running("waiting for write quorum");
        if let Ok(resp) = conn.send_command(&["SET", "__merge_ctrl_ready__", "1"]) {
            if resp == b"+OK\r\n".to_vec() {
                break;
            }
        }
        assert!(Instant::now() < quorum_deadline);
        std::thread::sleep(Duration::from_millis(50));
    }

    for i in 0..200u32 {
        let k0 = format!("0seed_{i:03}");
        let kz = format!("kseed_{i:03}");
        let _ = conn.send_command(&["SET", &k0, "a"]).expect("seed left");
        let _ = conn.send_command(&["SET", &kz, "b"]).expect("seed right");
    }

    run_holoctl_ok(grpc1, &["merge", "--left-shard-id", "1"]);
    let state_path = node1_dir.join("meta").join("cluster_state.json");
    assert!(
        wait_for_merge_exists(&state_path, 1, Duration::from_secs(10)),
        "merge did not appear in state"
    );

    run_holoctl_ok(grpc1, &["merge", "--left-shard-id", "1", "--pause"]);
    assert!(
        wait_for_merge_paused(&state_path, 1, true, Duration::from_secs(10)),
        "merge was not paused"
    );

    run_holoctl_ok(grpc1, &["merge", "--left-shard-id", "1", "--resume"]);
    assert!(
        wait_for_merge_paused(&state_path, 1, false, Duration::from_secs(10)),
        "merge was not resumed"
    );

    run_holoctl_ok(grpc1, &["merge", "--left-shard-id", "1", "--cancel"]);
    assert!(
        wait_for_no_merge(&state_path, 1, Duration::from_secs(10)),
        "merge was not canceled"
    );

    cleanup_dir(&dir);
}

#[test]
fn rebalance_rejected_while_merge_inflight() {
    let dir = test_dir("range-merge-rebalance-guard");
    let node1_dir = dir.join("node1");
    let node2_dir = dir.join("node2");
    let node3_dir = dir.join("node3");
    let _ = std::fs::create_dir_all(&node1_dir);
    let _ = std::fs::create_dir_all(&node2_dir);
    let _ = std::fs::create_dir_all(&node3_dir);

    let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let redis1 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let redis2 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let redis3 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let redis4 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let grpc1 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let grpc2 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let grpc3 = SocketAddr::new(ip, pick_free_port().expect("port"));
    let grpc4 = SocketAddr::new(ip, pick_free_port().expect("port"));

    let initial_members = format!("1@{grpc1},2@{grpc2},3@{grpc3}");
    let envs = [
        ("HOLO_ROUTING_MODE", "range"),
        ("HOLO_INITIAL_RANGES", "2"),
        ("HOLO_RANGE_SPLIT_MIN_KEYS", "1000000"),
        ("HOLO_RANGE_SPLIT_MIN_QPS", "0"),
        ("HOLO_RANGE_MERGE_MAX_KEYS", "0"),
        ("HOLO_REBALANCE_ENABLED", "false"),
    ];

    let _n1 = spawn_node_custom_env(
        1,
        &node1_dir,
        redis1,
        grpc1,
        true,
        None,
        initial_members.clone(),
        "accord",
        4,
        &envs,
    );
    let _n2 = spawn_node_custom_env(
        2,
        &node2_dir,
        redis2,
        grpc2,
        false,
        Some(grpc1),
        initial_members.clone(),
        "accord",
        4,
        &envs,
    );
    let _n3 = spawn_node_custom_env(
        3,
        &node3_dir,
        redis3,
        grpc3,
        false,
        Some(grpc1),
        initial_members.clone(),
        "accord",
        4,
        &envs,
    );
    assert!(wait_for_redis_ready(redis1, Duration::from_secs(20)));
    assert!(wait_for_redis_ready(redis2, Duration::from_secs(20)));
    assert!(wait_for_redis_ready(redis3, Duration::from_secs(20)));
    run_holoctl_ok(
        grpc1,
        &[
            "add-node",
            "--node-id",
            "4",
            "--grpc-addr",
            &grpc4.to_string(),
            "--redis-addr",
            &redis4.to_string(),
        ],
    );

    run_holoctl_ok(grpc1, &["merge", "--left-shard-id", "1"]);

    let out = run_holoctl(
        grpc1,
        &[
            "rebalance",
            "--shard-id",
            "1",
            "--replica",
            "2",
            "--replica",
            "3",
            "--replica",
            "4",
            "--leaseholder",
            "2",
        ],
    );
    assert!(
        !out.status.success(),
        "rebalance unexpectedly succeeded during in-flight merge\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("in-flight merge"),
        "unexpected rebalance failure output: {stderr}"
    );

    cleanup_dir(&dir);
}

fn run_holoctl(target: SocketAddr, args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(common::holoctl_bin());
    cmd.arg("--target").arg(target.to_string()).args(args);
    cmd.output().expect("run holoctl")
}

fn run_holoctl_ok(target: SocketAddr, args: &[&str]) {
    let out = run_holoctl(target, args);
    assert!(
        out.status.success(),
        "holoctl failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn wait_for_merge_exists(state_path: &Path, left_shard_id: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(json) = read_state_json(state_path) {
            if json
                .get("shard_merges")
                .and_then(|v| v.as_object())
                .and_then(|m| m.get(&left_shard_id.to_string()))
                .is_some()
            {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn wait_for_no_merge(state_path: &Path, left_shard_id: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(json) = read_state_json(state_path) {
            let exists = json
                .get("shard_merges")
                .and_then(|v| v.as_object())
                .and_then(|m| m.get(&left_shard_id.to_string()))
                .is_some();
            if !exists {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn wait_for_merge_paused(
    state_path: &Path,
    left_shard_id: u64,
    paused: bool,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(json) = read_state_json(state_path) {
            let actual = json
                .get("shard_merges")
                .and_then(|v| v.as_object())
                .and_then(|m| m.get(&left_shard_id.to_string()))
                .and_then(|v| v.get("paused"))
                .and_then(|v| v.as_bool());
            if actual == Some(paused) {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn read_state_json(path: &Path) -> Option<serde_json::Value> {
    let data = std::fs::read(path).ok()?;
    serde_json::from_slice(&data).ok()
}
