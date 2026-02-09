//! Integration test for range merge safety under concurrent writes.
//!
//! This test validates the merge orchestration path:
//! - freeze + drain + quiesce before descriptor cutover,
//! - merge while clients are concurrently issuing writes,
//! - unfreeze and continue serving reads/writes with no lost acknowledged data.

mod common;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{cleanup_dir, pick_free_port, spawn_node_custom_env, test_dir, wait_for_redis_ready};

#[test]
fn range_merge_under_load_preserves_acknowledged_writes() {
    let dir = test_dir("range-merge-under-load");
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
        // Keep this test deterministic: it explicitly drives merge itself.
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

    assert!(
        wait_for_redis_ready(redis1, Duration::from_secs(20)),
        "redis not ready on node1"
    );
    assert!(
        wait_for_redis_ready(redis2, Duration::from_secs(20)),
        "redis not ready on node2"
    );
    assert!(
        wait_for_redis_ready(redis3, Duration::from_secs(20)),
        "redis not ready on node3"
    );

    let mut conn = common::RespConn::connect(redis1);
    // Wait for write quorum.
    let quorum_deadline = Instant::now() + Duration::from_secs(20);
    loop {
        n1.assert_running("waiting for write quorum");
        n2.assert_running("waiting for write quorum");
        n3.assert_running("waiting for write quorum");
        if let Ok(resp) = conn.send_command(&["SET", "__merge_ready__", "1"]) {
            if resp == b"+OK\r\n".to_vec() {
                break;
            }
        }
        assert!(
            Instant::now() < quorum_deadline,
            "cluster write quorum not ready in time"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Seed keys on both sides of the initial split boundary.
    for i in 0..200u32 {
        let k0 = format!("0seed_{i:03}");
        let kz = format!("kseed_{i:03}");
        let v0 = format!("a{i:03}");
        let vz = format!("b{i:03}");
        let r0 = conn.send_command(&["SET", &k0, &v0]).expect("set 0seed");
        let rz = conn.send_command(&["SET", &kz, &vz]).expect("set kseed");
        assert_eq!(r0, b"+OK\r\n".to_vec());
        assert_eq!(rz, b"+OK\r\n".to_vec());
    }

    let state_path = node1_dir.join("meta").join("cluster_state.json");
    assert!(
        wait_for_shard_count(&state_path, 2, Duration::from_secs(10)),
        "expected two initial ranges before merge"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let latest_ok = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    let stop_w = stop.clone();
    let latest_w = latest_ok.clone();
    let writer = std::thread::spawn(move || {
        let mut c = common::RespConn::connect(redis1);
        let mut i = 0u64;
        while !stop_w.load(Ordering::Relaxed) {
            let key = if i % 2 == 0 {
                format!("0hot_{:02}", i % 32)
            } else {
                format!("khot_{:02}", i % 32)
            };
            let value = format!("v{i}");
            if let Ok(resp) = c.send_command(&["SET", &key, &value]) {
                if resp == b"+OK\r\n".to_vec() {
                    latest_w.lock().expect("latest lock").insert(key, value);
                }
            }
            i = i.saturating_add(1);
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    std::thread::sleep(Duration::from_millis(300));

    let merge = Command::new(common::holoctl_bin())
        .arg("--target")
        .arg(grpc1.to_string())
        .arg("merge")
        .arg("--left-shard-id")
        .arg("1")
        .output()
        .expect("run holoctl merge");
    assert!(
        merge.status.success(),
        "merge command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&merge.stdout),
        String::from_utf8_lossy(&merge.stderr)
    );

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer join");

    assert!(
        wait_for_shard_count(&state_path, 1, Duration::from_secs(15)),
        "expected merged single range"
    );
    assert!(
        wait_for_frozen_state(&state_path, false, Duration::from_secs(10)),
        "expected cluster to unfreeze after merge"
    );

    // Every acknowledged write from the concurrent writer must be readable.
    let latest = latest_ok.lock().expect("latest lock").clone();
    assert!(
        latest.len() >= 16,
        "too few successful writes recorded during merge: {}",
        latest.len()
    );
    for (key, value) in latest {
        let resp = conn.send_command(&["GET", &key]).expect("get merged key");
        let expected = format!("${}\r\n{}\r\n", value.len(), value);
        assert_eq!(
            String::from_utf8_lossy(&resp),
            expected,
            "stale/missing value for key {key}"
        );
    }

    // Post-merge writes/read should continue to work.
    let post = conn
        .send_command(&["SET", "kpost_merge_key", "ok"])
        .expect("post-merge set");
    assert_eq!(post, b"+OK\r\n".to_vec());
    let post_get = conn
        .send_command(&["GET", "kpost_merge_key"])
        .expect("post-merge get");
    assert_eq!(String::from_utf8_lossy(&post_get), "$2\r\nok\r\n");

    cleanup_dir(&dir);
}

fn wait_for_shard_count(state_path: &std::path::Path, expected: usize, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let Ok(data) = std::fs::read(state_path) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let Ok(json) = serde_json::from_slice::<serde_json::Value>(&data) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let count = json
            .get("shards")
            .and_then(|v| v.as_array())
            .map(|v| v.len())
            .unwrap_or(0);
        if count == expected {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn wait_for_frozen_state(
    state_path: &std::path::Path,
    expected_frozen: bool,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let Ok(data) = std::fs::read(state_path) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let Ok(json) = serde_json::from_slice::<serde_json::Value>(&data) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let Some(frozen) = json.get("frozen").and_then(|v| v.as_bool()) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        if frozen == expected_frozen {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}
