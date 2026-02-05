//! Smoke test for accord read-mode barriers on a single-node cluster.
//!
//! Test flow:
//! 1) Start a single-node holo-store process in `accord` read mode.
//! 2) Write a key/value pair.
//! 3) Perform a GET for the key via the Redis protocol.
//!
//! Failure model covered:
//! - Read barrier logic hangs or times out waiting for commits to execute.
//! - Read barrier incorrectly blocks on single-node execution.
//!
//! Verification:
//! - The GET must return the value that was just written.
//! - Timeouts indicate a stalled read barrier or executor sequencing issue.

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::{
    cleanup_dir, pick_free_port, read_keys, spawn_node_with_read_mode, test_dir, wait_for_port,
    wait_for_redis_ready, write_keys, IO_TIMEOUT,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(60);

fn check_timeout(start: Instant, step: &str, node: &mut common::NodeProcess) {
    if start.elapsed() <= TEST_TIMEOUT {
        return;
    }
    let stdout = node.read_stdout();
    let stderr = node.read_stderr();
    panic!(
        "accord_read_barrier timed out during {step}\nnode stdout:\n{stdout}\nnode stderr:\n{stderr}"
    );
}

#[test]
fn accord_read_barrier_single_node() {
    let test_start = Instant::now();
    let data_dir = test_dir("accord-read-barrier");
    cleanup_dir(&data_dir);

    let redis_port = match pick_free_port() {
        Ok(port) => port,
        Err(err) => panic!("bind ephemeral port failed: {err}"),
    };
    let grpc_port = match pick_free_port() {
        Ok(port) => port,
        Err(err) => panic!("bind ephemeral port failed: {err}"),
    };
    let redis_addr: SocketAddr = format!("127.0.0.1:{redis_port}").parse().unwrap();
    let grpc_addr: SocketAddr = format!("127.0.0.1:{grpc_port}").parse().unwrap();

    let mut node = spawn_node_with_read_mode(&data_dir, redis_addr, grpc_addr, "accord");
    check_timeout(test_start, "spawn", &mut node);
    node.assert_running("after spawn");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    if !wait_for_redis_ready(redis_addr, IO_TIMEOUT) {
        let stdout = node.read_stdout();
        let stderr = node.read_stderr();
        panic!(
            "redis port {redis_addr} did not respond to PING in time\nnode stdout:\n{stdout}\nnode stderr:\n{stderr}"
        );
    }

    let keys = vec![("k1".to_string(), "v1".to_string())];
    check_timeout(test_start, "before write", &mut node);
    if let Err(err) = write_keys(redis_addr, &keys) {
        let stderr = node.read_stderr();
        panic!("{err}\nnode stderr:\n{stderr}");
    }

    check_timeout(test_start, "before read", &mut node);
    let values = read_keys(redis_addr, &["k1".to_string()]);
    assert_eq!(values.get(0).and_then(|v| v.as_deref()), Some("v1"));

    cleanup_dir(&data_dir);
}
