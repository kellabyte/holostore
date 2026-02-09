//! Integration tests for merge recovery and merge/rebalance interaction.
//!
//! These tests focus on production-safety behaviors:
//! - merge workflow survives controller restarts while progressing through phases,
//! - disjoint-shard rebalance requests can proceed while another shard merge is in flight.

mod common;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use common::{cleanup_dir, pick_free_port, spawn_node_custom_env, test_dir, wait_for_redis_ready};

#[test]
fn merge_recovers_through_phase_restarts() {
    let dir = test_dir("range-merge-recovery");
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
        // Keep placement deterministic; merge phase progression still runs
        // even when balancing proposals are disabled.
        ("HOLO_REBALANCE_ENABLED", "false"),
        // Slow down phase advancement enough for deterministic restart windows.
        ("HOLO_REBALANCE_INTERVAL_MS", "200"),
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
        6,
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
        6,
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
        6,
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
        if let Ok(resp) = conn.send_command(&["SET", "__merge_recovery_ready__", "1"]) {
            if resp == b"+OK\r\n".to_vec() {
                break;
            }
        }
        assert!(
            Instant::now() < quorum_deadline,
            "write quorum not ready in time"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Seed both halves of the split boundary and record expected committed values.
    let mut expected = HashMap::<String, String>::new();
    for i in 0..300u32 {
        let k0 = format!("0seed_{i:03}");
        let kz = format!("kseed_{i:03}");
        let v0 = format!("left_{i:03}");
        let vz = format!("right_{i:03}");
        let r0 = conn.send_command(&["SET", &k0, &v0]).expect("set left");
        let rz = conn.send_command(&["SET", &kz, &vz]).expect("set right");
        assert_eq!(r0, b"+OK\r\n".to_vec());
        assert_eq!(rz, b"+OK\r\n".to_vec());
        expected.insert(k0, v0);
        expected.insert(kz, vz);
    }

    run_holoctl_ok(grpc1, &["merge", "--left-shard-id", "1"]);
    let state_path = node1_dir.join("meta").join("cluster_state.json");
    assert!(
        wait_for_merge_exists(&state_path, 1, Duration::from_secs(15)),
        "merge did not appear in control-plane state"
    );

    // Restart the controller while merge is in several in-flight phases.
    for phase in ["Preparing", "Catchup", "Copying", "Cutover"] {
        assert!(
            wait_for_merge_phase(&state_path, 1, phase, Duration::from_secs(20)),
            "merge did not reach phase {phase}"
        );
        let _ = n1.child.kill();
        let _ = n1.child.wait();
        n1 = spawn_node_custom_env(
            1,
            &node1_dir,
            redis1,
            grpc1,
            false,
            Some(grpc2),
            initial_members.clone(),
            "accord",
            6,
            &envs,
        );
        assert!(
            wait_for_redis_ready(redis1, Duration::from_secs(20)),
            "node1 did not come back after restart in phase {phase}"
        );
        n2.assert_running("node2 should stay up during merge recovery");
        n3.assert_running("node3 should stay up during merge recovery");
    }

    assert!(
        wait_for_no_merge(&state_path, 1, Duration::from_secs(60)),
        "merge did not finalize after restarts"
    );
    assert!(
        wait_for_shard_count(&state_path, 1, Duration::from_secs(20)),
        "expected merged single range"
    );
    assert!(
        wait_for_frozen_state(&state_path, false, Duration::from_secs(20)),
        "cluster unexpectedly remained frozen after merge recovery"
    );

    let mut keys = expected.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    // Validate on a non-restarted node to avoid transient local socket churn.
    let sample = keys.into_iter().take(40).collect::<Vec<_>>();
    let values = common::read_keys(redis2, &sample);
    for (idx, key) in sample.iter().enumerate() {
        let expected_value = expected.get(key).expect("expected map");
        assert_eq!(
            values[idx].as_deref(),
            Some(expected_value.as_str()),
            "stale value after merge recovery for key {key}"
        );
    }

    cleanup_dir(&dir);
}

#[test]
fn rebalance_on_disjoint_shard_allowed_during_merge() {
    let dir = test_dir("range-merge-rebalance-disjoint");
    let node1_dir = dir.join("node1");
    let node2_dir = dir.join("node2");
    let node3_dir = dir.join("node3");
    let node4_dir = dir.join("node4");
    let _ = std::fs::create_dir_all(&node1_dir);
    let _ = std::fs::create_dir_all(&node2_dir);
    let _ = std::fs::create_dir_all(&node3_dir);
    let _ = std::fs::create_dir_all(&node4_dir);

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
        ("HOLO_INITIAL_RANGES", "3"),
        ("HOLO_RANGE_SPLIT_MIN_KEYS", "1000000"),
        ("HOLO_RANGE_SPLIT_MIN_QPS", "0"),
        ("HOLO_RANGE_MERGE_MAX_KEYS", "0"),
        // Keep this test deterministic: explicitly drive merge/rebalance commands.
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
        6,
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
        6,
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
        6,
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
    let _n4 = spawn_node_custom_env(
        4,
        &node4_dir,
        redis4,
        grpc4,
        false,
        Some(grpc1),
        format!("1@{grpc1},2@{grpc2},3@{grpc3},4@{grpc4}"),
        "accord",
        6,
        &envs,
    );
    assert!(wait_for_redis_ready(redis4, Duration::from_secs(20)));

    run_holoctl_ok(grpc1, &["merge", "--left-shard-id", "1"]);

    let out = run_holoctl(
        grpc1,
        &[
            "rebalance",
            "--shard-id",
            "3",
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
        out.status.success(),
        "rebalance for disjoint shard should succeed while merge is in-flight\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let state_path = node1_dir.join("meta").join("cluster_state.json");
    assert!(
        wait_for_rebalance_exists(&state_path, 3, Duration::from_secs(10)),
        "expected rebalance metadata for shard 3"
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

fn read_state_json(path: &Path) -> Option<serde_json::Value> {
    let data = std::fs::read(path).ok()?;
    serde_json::from_slice::<serde_json::Value>(&data).ok()
}

fn wait_for_merge_exists(path: &Path, left_shard_id: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(json) = read_state_json(path) {
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

fn wait_for_no_merge(path: &Path, left_shard_id: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(json) = read_state_json(path) {
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

fn wait_for_merge_phase(path: &Path, left_shard_id: u64, phase: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(json) = read_state_json(path) {
            let observed = json
                .get("shard_merges")
                .and_then(|v| v.as_object())
                .and_then(|m| m.get(&left_shard_id.to_string()))
                .and_then(|v| v.get("phase"))
                .and_then(|v| v.as_str());
            if observed == Some(phase) {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    false
}

fn wait_for_rebalance_exists(path: &Path, shard_id: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(json) = read_state_json(path) {
            if json
                .get("shard_rebalances")
                .and_then(|v| v.as_object())
                .and_then(|m| m.get(&shard_id.to_string()))
                .is_some()
            {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn wait_for_shard_count(path: &Path, expected: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(json) = read_state_json(path) {
            let count = json
                .get("shards")
                .and_then(|v| v.as_array())
                .map(|v| v.len())
                .unwrap_or(0);
            if count == expected {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn wait_for_frozen_state(path: &Path, expected: bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(json) = read_state_json(path) {
            if json.get("frozen").and_then(|v| v.as_bool()) == Some(expected) {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}
