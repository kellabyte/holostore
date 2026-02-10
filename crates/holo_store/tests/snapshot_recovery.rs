//! Recovery test around WAL compaction checkpoints.
//!
//! This test writes enough operations to trigger regular WAL compaction, then
//! crash/restarts the node twice and verifies all committed keys are still
//! readable. It guards against WAL GC dropping state required for recovery.

mod common;

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::{
    cleanup_dir, holoctl_bin, pick_free_port, read_keys, spawn_node_custom_env, test_dir,
    wait_for_port, wait_for_redis_ready, write_keys, IO_TIMEOUT,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(120);

fn check_timeout(start: Instant, step: &str, node: &mut common::NodeProcess) {
    if start.elapsed() <= TEST_TIMEOUT {
        return;
    }
    let stdout = node.read_stdout();
    let stderr = node.read_stderr();
    panic!(
        "snapshot_recovery timed out during {step}\nnode stdout:\n{stdout}\nnode stderr:\n{stderr}"
    );
}

fn checkpoint_envs() -> [(&'static str, &'static str); 7] {
    [
        ("HOLO_WAL_ENGINE", "raft-engine"),
        ("HOLO_WAL_PERSIST_EVERY", "1"),
        ("HOLO_WAL_PERSIST_INTERVAL_US", "500"),
        ("HOLO_RECOVERY_CHECKPOINT_ENABLED", "true"),
        ("HOLO_RECOVERY_CHECKPOINT_INTERVAL_MS", "200"),
        ("HOLO_RECOVERY_CHECKPOINT_WARN_LAG_ENTRIES", "100000"),
        ("HOLO_RECOVERY_CHECKPOINT_PERSIST_MODE", "sync-data"),
    ]
}

fn wait_for_manifest(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "recovery checkpoint manifest did not appear: {}",
        path.display()
    );
}

fn wait_for_manifest_success_count(path: &Path, min_success_count: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(bytes) = fs::read(path) {
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                let success = json
                    .get("payload")
                    .and_then(|v| v.get("success_count"))
                    .or_else(|| json.get("success_count"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                if success >= min_success_count {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "recovery checkpoint success_count did not reach {} in time: {}",
        min_success_count,
        path.display()
    );
}

fn wait_for_expected_values(
    redis_addr: SocketAddr,
    expected: &[(String, String)],
    timeout: Duration,
    context: &str,
    node: &mut common::NodeProcess,
) {
    let key_names = expected
        .iter()
        .map(|(k, _)| k.clone())
        .collect::<Vec<String>>();
    let deadline = Instant::now() + timeout;
    let mut last_mismatch: Option<(String, String, Option<String>)> = None;
    loop {
        let values = read_keys(redis_addr, &key_names);
        let mut all_ok = true;
        let mut local_ok = 0usize;
        for (idx, value) in values.into_iter().enumerate() {
            if value.as_deref() != Some(expected[idx].1.as_str()) {
                all_ok = false;
                last_mismatch = Some((
                    expected[idx].0.clone(),
                    expected[idx].1.clone(),
                    value.clone(),
                ));
                break;
            }
            local_ok += 1;
        }
        if all_ok {
            return;
        }
        if Instant::now() >= deadline {
            let mismatch_text = if let Some((key, want, got)) = last_mismatch {
                format!("first_mismatch key={key} expected={want:?} got={got:?}")
            } else {
                "first_mismatch=unknown".to_string()
            };
            panic!(
                "{context} matched={}/{} {mismatch_text}\nnode stdout:\n{}\nnode stderr:\n{}",
                local_ok,
                expected.len(),
                node.read_stdout(),
                node.read_stderr()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn run_holoctl(target: SocketAddr, args: &[&str]) -> String {
    let mut cmd = Command::new(holoctl_bin());
    cmd.arg("--target").arg(target.to_string());
    cmd.args(args);
    let out = cmd.output().expect("run holoctl");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    if !out.status.success() {
        panic!(
            "holoctl failed for args {:?}\nstdout:\n{}\nstderr:\n{}",
            args, stdout, stderr
        );
    }
    stdout
}

#[test]
fn snapshot_recovery_boundary() {
    let test_start = Instant::now();
    let data_dir = test_dir("snapshot-recovery");
    cleanup_dir(&data_dir);

    let redis_port = pick_free_port().expect("redis port");
    let grpc_port = pick_free_port().expect("grpc port");
    let redis_addr: SocketAddr = format!("127.0.0.1:{redis_port}").parse().unwrap();
    let grpc_addr: SocketAddr = format!("127.0.0.1:{grpc_port}").parse().unwrap();

    let envs = checkpoint_envs();
    let mut node = spawn_node_custom_env(
        1,
        &data_dir,
        redis_addr,
        grpc_addr,
        true,
        None,
        format!("1@{grpc_addr}"),
        "local",
        1,
        &envs,
    );
    check_timeout(test_start, "spawn", &mut node);
    node.assert_running("after spawn");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addr, IO_TIMEOUT),
        "redis did not become ready"
    );

    // >1024 writes so Accord's periodic WAL compaction path runs at least once.
    let mut keys = Vec::new();
    for i in 0..1100 {
        keys.push((format!("k{i}"), format!("v{i}")));
    }
    if let Err(err) = write_keys(redis_addr, &keys) {
        panic!("{err}\nnode stderr:\n{}", node.read_stderr());
    }

    wait_for_expected_values(
        redis_addr,
        &keys,
        Duration::from_secs(60),
        "phase1 writes not visible in time",
        &mut node,
    );

    // Crash/restart #1.
    node.child.kill().ok();
    let _ = node.child.wait();
    let mut node = spawn_node_custom_env(
        1,
        &data_dir,
        redis_addr,
        grpc_addr,
        true,
        None,
        format!("1@{grpc_addr}"),
        "local",
        1,
        &envs,
    );
    check_timeout(test_start, "restart-1", &mut node);
    node.assert_running("after restart-1");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addr, IO_TIMEOUT),
        "redis did not become ready after restart-1"
    );

    wait_for_expected_values(
        redis_addr,
        &keys,
        Duration::from_secs(60),
        "phase1 data not visible after restart-1",
        &mut node,
    );

    // Crash/restart #2 and full verification after compaction boundary.
    node.child.kill().ok();
    let _ = node.child.wait();
    let mut node = spawn_node_custom_env(
        1,
        &data_dir,
        redis_addr,
        grpc_addr,
        true,
        None,
        format!("1@{grpc_addr}"),
        "local",
        1,
        &envs,
    );
    check_timeout(test_start, "restart-2", &mut node);
    node.assert_running("after restart-2");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addr, IO_TIMEOUT),
        "redis did not become ready after restart-2"
    );

    let key_names = keys.iter().map(|(k, _)| k.clone()).collect::<Vec<String>>();
    let values = read_keys(redis_addr, &key_names);
    for (idx, value) in values.into_iter().enumerate() {
        let expected = format!("v{idx}");
        assert_eq!(value.as_deref(), Some(expected.as_str()));
    }

    node.child.kill().ok();
    let _ = node.child.wait();
    cleanup_dir(&data_dir);
}

#[test]
fn snapshot_recovery_manifest_corruption_tolerated() {
    let test_start = Instant::now();
    let data_dir = test_dir("snapshot-recovery-corrupt-manifest");
    cleanup_dir(&data_dir);

    let redis_port = pick_free_port().expect("redis port");
    let grpc_port = pick_free_port().expect("grpc port");
    let redis_addr: SocketAddr = format!("127.0.0.1:{redis_port}").parse().unwrap();
    let grpc_addr: SocketAddr = format!("127.0.0.1:{grpc_port}").parse().unwrap();
    let envs = checkpoint_envs();

    let mut node = spawn_node_custom_env(
        1,
        &data_dir,
        redis_addr,
        grpc_addr,
        true,
        None,
        format!("1@{grpc_addr}"),
        "local",
        1,
        &envs,
    );
    check_timeout(test_start, "spawn", &mut node);
    node.assert_running("after spawn");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addr, IO_TIMEOUT),
        "redis did not become ready"
    );

    let mut keys = Vec::new();
    for i in 0..300 {
        keys.push((format!("m{i}"), format!("v{i}")));
    }
    if let Err(err) = write_keys(redis_addr, &keys) {
        panic!("{err}\nnode stderr:\n{}", node.read_stderr());
    }
    wait_for_expected_values(
        redis_addr,
        &keys,
        Duration::from_secs(60),
        "pre-restart values not visible before manifest corruption",
        &mut node,
    );
    let _ = run_holoctl(grpc_addr, &["checkpoint", "trigger"]);

    let manifest_path: PathBuf = data_dir.join("meta").join("recovery_checkpoints.json");
    wait_for_manifest(&manifest_path, Duration::from_secs(10));
    wait_for_manifest_success_count(&manifest_path, 1, Duration::from_secs(15));

    node.child.kill().ok();
    let _ = node.child.wait();

    fs::write(&manifest_path, b"{not-valid-json")
        .unwrap_or_else(|err| panic!("corrupt manifest write failed: {err}"));

    let mut node = spawn_node_custom_env(
        1,
        &data_dir,
        redis_addr,
        grpc_addr,
        true,
        None,
        format!("1@{grpc_addr}"),
        "local",
        1,
        &envs,
    );
    check_timeout(test_start, "restart-with-corrupt-manifest", &mut node);
    node.assert_running("after restart with corrupt manifest");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addr, IO_TIMEOUT),
        "redis did not become ready after corrupt manifest restart"
    );

    let key_names = keys.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
    let verify_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let values = read_keys(redis_addr, &key_names);
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
            panic!(
                "post-restart values not visible after corrupt manifest recovery\nnode stdout:\n{}\nnode stderr:\n{}",
                node.read_stdout(),
                node.read_stderr()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let stdout = node.read_stdout();
    let stderr = node.read_stderr();
    assert!(
        stdout.contains("recovery manifest parse failed")
            || stderr.contains("recovery manifest parse failed"),
        "expected corruption warning in logs, got stdout:\n{stdout}\n\nstderr:\n{stderr}"
    );

    node.child.kill().ok();
    let _ = node.child.wait();
    cleanup_dir(&data_dir);
}

#[test]
fn snapshot_recovery_checkpoint_controls_and_crash_loop() {
    let test_start = Instant::now();
    let data_dir = test_dir("snapshot-recovery-controls");
    cleanup_dir(&data_dir);

    let redis_port = pick_free_port().expect("redis port");
    let grpc_port = pick_free_port().expect("grpc port");
    let redis_addr: SocketAddr = format!("127.0.0.1:{redis_port}").parse().unwrap();
    let grpc_addr: SocketAddr = format!("127.0.0.1:{grpc_port}").parse().unwrap();
    let mut envs = checkpoint_envs().to_vec();
    envs.push(("HOLO_RECOVERY_CHECKPOINT_INTERVAL_MS", "5000"));

    let mut node = spawn_node_custom_env(
        1,
        &data_dir,
        redis_addr,
        grpc_addr,
        true,
        None,
        format!("1@{grpc_addr}"),
        "local",
        1,
        &envs,
    );
    check_timeout(test_start, "spawn", &mut node);
    node.assert_running("after spawn");
    wait_for_port(grpc_addr, IO_TIMEOUT);
    wait_for_port(redis_addr, IO_TIMEOUT);
    assert!(
        wait_for_redis_ready(redis_addr, IO_TIMEOUT),
        "redis did not become ready"
    );

    let out = run_holoctl(grpc_addr, &["checkpoint", "status"]);
    assert!(
        out.contains("paused=false"),
        "unexpected checkpoint status: {out}"
    );
    let _ = run_holoctl(grpc_addr, &["checkpoint", "pause"]);
    let out = run_holoctl(grpc_addr, &["checkpoint", "status"]);
    assert!(out.contains("paused=true"), "pause not reflected: {out}");

    let manifest_path: PathBuf = data_dir.join("meta").join("recovery_checkpoints.json");
    let mut all_keys = Vec::new();
    for round in 0..3 {
        let mut keys = Vec::new();
        for i in 0..150 {
            let key = format!("loop{round}_k{i}");
            let value = format!("loop{round}_v{i}");
            keys.push((key.clone(), value.clone()));
            all_keys.push((key, value));
        }
        if let Err(err) = write_keys(redis_addr, &keys) {
            panic!(
                "{err}\nnode stdout:\n{}\nnode stderr:\n{}",
                node.read_stdout(),
                node.read_stderr()
            );
        }
        let _ = run_holoctl(grpc_addr, &["checkpoint", "trigger"]);
        let status = run_holoctl(grpc_addr, &["checkpoint", "status"]);
        assert!(
            status.contains("manual_triggers=1"),
            "expected one manual trigger before restart, got: {status}"
        );
        wait_for_manifest(&manifest_path, Duration::from_secs(10));
        wait_for_manifest_success_count(&manifest_path, 1, Duration::from_secs(15));

        node.child.kill().ok();
        let _ = node.child.wait();
        node = spawn_node_custom_env(
            1,
            &data_dir,
            redis_addr,
            grpc_addr,
            true,
            None,
            format!("1@{grpc_addr}"),
            "local",
            1,
            &envs,
        );
        check_timeout(test_start, "restart-loop", &mut node);
        node.assert_running("after restart loop");
        wait_for_port(grpc_addr, IO_TIMEOUT);
        wait_for_port(redis_addr, IO_TIMEOUT);
        assert!(
            wait_for_redis_ready(redis_addr, IO_TIMEOUT),
            "redis did not become ready after restart loop"
        );
        wait_for_expected_values(
            redis_addr,
            &all_keys,
            Duration::from_secs(60),
            "post-restart values not visible in crash loop",
            &mut node,
        );
    }

    let _ = run_holoctl(grpc_addr, &["checkpoint", "resume"]);
    let out = run_holoctl(grpc_addr, &["checkpoint", "status"]);
    assert!(
        out.contains("paused=false"),
        "resume not reflected in status: {out}"
    );

    wait_for_expected_values(
        redis_addr,
        &all_keys,
        Duration::from_secs(60),
        "post-loop values not visible after crash/restart sequence",
        &mut node,
    );

    node.child.kill().ok();
    let _ = node.child.wait();
    cleanup_dir(&data_dir);
}
