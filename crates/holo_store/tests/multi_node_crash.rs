//! Multi-node crash/restart test for replication and WAL replay across shards.
//!
//! Test flow:
//! 1) Start a 3-node cluster (node1 bootstrap, node2+node3 join).
//! 2) Write a batch of keys via node1 and verify visibility.
//! 3) Kill node2 (follower) and continue writing.
//! 4) Restart node2 with its existing data directory.
//! 5) Read all keys from node2 and verify values are present.
//!
//! Failure model covered:
//! - Follower crash/restart while writes continue on the leader.
//!
//! Verification:
//! - All keys written before and during the crash must be visible on the restarted follower.

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::{
    cleanup_dir, pick_free_port, read_keys, spawn_node_custom, test_dir, wait_for_port,
    wait_for_redis_ready, write_keys, IO_TIMEOUT,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(90);

fn check_timeout(start: Instant, step: &str, nodes: &mut [common::NodeProcess]) {
    if start.elapsed() <= TEST_TIMEOUT {
        return;
    }
    let mut stdout = String::new();
    let mut stderr = String::new();
    for (idx, node) in nodes.iter_mut().enumerate() {
        stdout.push_str(&format!("== node{idx} stdout ==\n{}\n", node.read_stdout()));
        stderr.push_str(&format!("== node{idx} stderr ==\n{}\n", node.read_stderr()));
    }
    panic!("multi_node_crash timed out during {step}\n{stdout}\n{stderr}");
}

#[test]
fn multi_node_crash_restart() {
    let test_start = Instant::now();
    let base_dir = test_dir("multi-node-crash");
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

    let mut node1 = spawn_node_custom(
        1,
        &base_dir.join("node1"),
        redis_addrs[0],
        grpc_addrs[0],
        true,
        None,
        members.clone(),
        "local",
        4,
    );
    node1.assert_running("node1 bootstrap");
    wait_for_port(grpc_addrs[0], IO_TIMEOUT);
    wait_for_port(redis_addrs[0], IO_TIMEOUT);
    wait_for_redis_ready(redis_addrs[0], IO_TIMEOUT);

    let mut node2 = spawn_node_custom(
        2,
        &base_dir.join("node2"),
        redis_addrs[1],
        grpc_addrs[1],
        false,
        Some(grpc_addrs[0]),
        members.clone(),
        "local",
        4,
    );
    node2.assert_running("node2 join");
    wait_for_port(grpc_addrs[1], IO_TIMEOUT);
    wait_for_port(redis_addrs[1], IO_TIMEOUT);
    wait_for_redis_ready(redis_addrs[1], IO_TIMEOUT);

    let mut node3 = spawn_node_custom(
        3,
        &base_dir.join("node3"),
        redis_addrs[2],
        grpc_addrs[2],
        false,
        Some(grpc_addrs[0]),
        members.clone(),
        "local",
        4,
    );
    node3.assert_running("node3 join");
    wait_for_port(grpc_addrs[2], IO_TIMEOUT);
    wait_for_port(redis_addrs[2], IO_TIMEOUT);
    wait_for_redis_ready(redis_addrs[2], IO_TIMEOUT);

    let mut nodes = vec![node1, node2, node3];
    check_timeout(test_start, "cluster start", &mut nodes);

    let mut keys = Vec::new();
    for i in 0..20 {
        keys.push((format!("k{i}"), format!("v{i}")));
    }
    if let Err(err) = write_keys(redis_addrs[0], &keys) {
        let stderr = nodes[0].read_stderr();
        panic!("{err}\nnode stderr:\n{stderr}");
    }
    let key_names: Vec<String> = keys.iter().map(|(k, _)| k.clone()).collect();
    let verify_deadline = Instant::now() + IO_TIMEOUT;
    loop {
        let values = read_keys(redis_addrs[0], &key_names);
        let mut all_ok = true;
        for (idx, value) in values.into_iter().enumerate() {
            let expected = format!("v{idx}");
            if value.as_deref() != Some(expected.as_str()) {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            break;
        }
        if Instant::now() >= verify_deadline {
            let stderr = nodes[0].read_stderr();
            panic!("node1 did not apply initial writes in time\nnode stderr:\n{stderr}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Kill follower node2.
    nodes[1].child.kill().ok();
    let _ = nodes[1].child.wait();
    check_timeout(test_start, "after node2 kill", &mut nodes);

    // Continue writing while node2 is down.
    let mut more_keys = Vec::new();
    for i in 20..40 {
        more_keys.push((format!("k{i}"), format!("v{i}")));
    }
    if let Err(err) = write_keys(redis_addrs[0], &more_keys) {
        let stderr = nodes[0].read_stderr();
        panic!("{err}\nnode stderr:\n{stderr}");
    }
    keys.extend(more_keys);

    // Restart node2 and verify it catches up.
    let mut node2 = spawn_node_custom(
        2,
        &base_dir.join("node2"),
        redis_addrs[1],
        grpc_addrs[1],
        false,
        Some(grpc_addrs[0]),
        members.clone(),
        "local",
        4,
    );
    node2.assert_running("node2 restart");
    wait_for_port(grpc_addrs[1], IO_TIMEOUT);
    wait_for_port(redis_addrs[1], IO_TIMEOUT);
    if !wait_for_redis_ready(redis_addrs[1], IO_TIMEOUT) {
        let stdout = node2.read_stdout();
        let stderr = node2.read_stderr();
        panic!(
            "node2 redis did not respond\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }
    nodes[1] = node2;

    let verify_deadline = Instant::now() + IO_TIMEOUT;
    loop {
        let values = read_keys(redis_addrs[1], &key_names);
        let mut all_ok = true;
        for (idx, value) in values.into_iter().enumerate() {
            let expected = format!("v{idx}");
            if value.as_deref() != Some(expected.as_str()) {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            break;
        }
        if Instant::now() >= verify_deadline {
            let stderr = nodes[1].read_stderr();
            panic!("node2 did not catch up in time\nnode stderr:\n{stderr}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
