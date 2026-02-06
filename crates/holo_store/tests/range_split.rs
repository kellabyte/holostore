//! Integration test for Cockroach-style range splits.
//!
//! This test runs a small 3-node cluster with range routing enabled and starts
//! with a single full-keyspace range. A background range manager should split
//! the hot range once it accumulates enough keys, migrating the right-hand side
//! keys to a different shard partition.

mod common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use common::{cleanup_dir, pick_free_port, spawn_node_custom_env, test_dir, wait_for_redis_ready};

#[test]
fn range_autosplit_keeps_reads_writes_working() {
    let dir = test_dir("range-autosplit");
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
        ("HOLO_INITIAL_RANGES", "1"),
        // Split quickly so the test doesn't take long.
        ("HOLO_RANGE_SPLIT_MIN_KEYS", "2"),
        ("HOLO_RANGE_MGR_INTERVAL_MS", "50"),
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

    let mut conn = common::RespConn::connect(redis1);
    let keys = (0..20)
        .map(|i| format!("holo_test_k{i:03}"))
        .collect::<Vec<_>>();
    for (i, key) in keys.iter().enumerate() {
        n1.assert_running("before set");
        n2.assert_running("before set");
        n3.assert_running("before set");
        let val = format!("v{i:03}");
        let resp = conn
            .send_command(&["SET", key, &val])
            .expect("set command");
        assert_eq!(resp, b"+OK\r\n".to_vec(), "SET failed");
    }

    // Wait for the range manager to apply a split (meta state persists to disk).
    let state_path = node1_dir.join("meta").join("cluster_state.json");
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut saw_split = false;
    while Instant::now() < deadline {
        let Ok(data) = std::fs::read(&state_path) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let Ok(json) = serde_json::from_slice::<serde_json::Value>(&data) else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        let shards = json.get("shards").and_then(|v| v.as_array()).cloned().unwrap_or_default();
        if shards.len() > 1 {
            saw_split = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(saw_split, "expected an automatic range split");

    // Reads should continue to work after the split + migration.
    for (i, key) in keys.iter().enumerate() {
        let resp = conn.send_command(&["GET", key]).expect("get command");
        let expected = format!("${}\r\nv{i:03}\r\n", 4);
        assert_eq!(String::from_utf8_lossy(&resp), expected);
    }

    cleanup_dir(&dir);
}

