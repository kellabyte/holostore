//! Shared helpers for integration tests.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Timeout for node startup and command round-trips.
pub const IO_TIMEOUT: Duration = Duration::from_secs(20);
/// Timeout for individual RESP read/write operations.
pub const RESP_TIMEOUT: Duration = Duration::from_secs(5);

/// Simple wrapper around a spawned node process and its stderr log path.
pub struct NodeProcess {
    pub child: Child,
    stderr_path: PathBuf,
    stdout_path: PathBuf,
}

impl NodeProcess {
    /// Best-effort read of the captured stderr log.
    pub fn read_stderr(&self) -> String {
        std::fs::read_to_string(&self.stderr_path).unwrap_or_default()
    }

    /// Best-effort read of the captured stdout log.
    pub fn read_stdout(&self) -> String {
        std::fs::read_to_string(&self.stdout_path).unwrap_or_default()
    }

    /// Panic if the process has already exited.
    pub fn assert_running(&mut self, context: &str) {
        if let Ok(Some(status)) = self.child.try_wait() {
            let stdout = self.read_stdout();
            let stderr = self.read_stderr();
            panic!(
                "holo-store exited early ({context}) with status {status}\nstdout:\n{stdout}\nstderr:\n{stderr}"
            );
        }
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

/// Spawn a holo-store node process for testing and capture stderr to a log file.
pub fn spawn_node(
    data_dir: &PathBuf,
    redis_addr: SocketAddr,
    grpc_addr: SocketAddr,
) -> NodeProcess {
    spawn_node_with_read_mode(data_dir, redis_addr, grpc_addr, "local")
}

pub fn spawn_node_with_read_mode(
    data_dir: &PathBuf,
    redis_addr: SocketAddr,
    grpc_addr: SocketAddr,
    read_mode: &str,
) -> NodeProcess {
    spawn_node_custom(
        1,
        data_dir,
        redis_addr,
        grpc_addr,
        true,
        None,
        format!("1@{}", grpc_addr),
        read_mode,
        2,
    )
}

pub fn spawn_node_custom(
    node_id: u64,
    data_dir: &PathBuf,
    redis_addr: SocketAddr,
    grpc_addr: SocketAddr,
    bootstrap: bool,
    join: Option<SocketAddr>,
    initial_members: String,
    read_mode: &str,
    data_shards: usize,
) -> NodeProcess {
    spawn_node_custom_env(
        node_id,
        data_dir,
        redis_addr,
        grpc_addr,
        bootstrap,
        join,
        initial_members,
        read_mode,
        data_shards,
        &[],
    )
}

/// Spawn a holo-store node with additional environment variables.
pub fn spawn_node_custom_env(
    node_id: u64,
    data_dir: &PathBuf,
    redis_addr: SocketAddr,
    grpc_addr: SocketAddr,
    bootstrap: bool,
    join: Option<SocketAddr>,
    initial_members: String,
    read_mode: &str,
    data_shards: usize,
    envs: &[(&str, &str)],
) -> NodeProcess {
    let bin = holo_store_bin();
    let log_dir = data_dir.join("logs");
    let _ = std::fs::create_dir_all(&log_dir);
    let stdout_path = log_dir.join("holo-store.out.log");
    let stderr_path = log_dir.join("holo-store.err.log");
    let stdout_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stdout_path)
        .expect("open stdout log");
    let stderr_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&stderr_path)
        .expect("open stderr log");

    let mut cmd = Command::new(bin);
    cmd.arg("node")
        .arg("--node-id")
        .arg(node_id.to_string())
        .arg("--listen-redis")
        .arg(redis_addr.to_string())
        .arg("--listen-grpc")
        .arg(grpc_addr.to_string())
        .arg("--initial-members")
        .arg(initial_members)
        .arg("--read-mode")
        .arg(read_mode)
        .arg("--data-dir")
        .arg(data_dir.to_string_lossy().to_string())
        .arg("--max-shards")
        .arg(data_shards.to_string())
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));

    for (k, v) in envs {
        cmd.env(k, v);
    }

    if bootstrap {
        cmd.arg("--bootstrap");
    } else if let Some(join_addr) = join {
        cmd.arg("--join").arg(join_addr.to_string());
    }

    let child = cmd.spawn().expect("failed to spawn holo-store");

    NodeProcess {
        child,
        stderr_path,
        stdout_path,
    }
}

/// Locate the holo-store binary built by cargo.
pub fn holo_store_bin() -> PathBuf {
    if let Some(bin) = std::env::var_os("CARGO_BIN_EXE_holo-store") {
        return PathBuf::from(bin);
    }
    let root = find_repo_root().unwrap_or_else(|| std::env::current_dir().unwrap());
    let exe = if cfg!(windows) {
        "holo-store.exe"
    } else {
        "holo-store"
    };
    let candidate = root.join("target").join("debug").join(exe);
    if candidate.exists() {
        return candidate;
    }
    panic!("holo-store binary not found; run `cargo build -p holo_store --bin holo-store` first");
}

/// Locate the holoctl binary built by cargo.
pub fn holoctl_bin() -> PathBuf {
    if let Some(bin) = std::env::var_os("CARGO_BIN_EXE_holoctl") {
        return PathBuf::from(bin);
    }
    let root = find_repo_root().unwrap_or_else(|| std::env::current_dir().unwrap());
    let exe = if cfg!(windows) {
        "holoctl.exe"
    } else {
        "holoctl"
    };
    let candidate = root.join("target").join("debug").join(exe);
    if candidate.exists() {
        return candidate;
    }
    panic!("holoctl binary not found; run `cargo build -p holo_store --bin holoctl` first");
}

/// Wait for a TCP port to accept connections.
pub fn wait_for_port(addr: SocketAddr, timeout: Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("port {addr} did not open in time");
}

/// Wait for the Redis port to answer a PING.
pub fn wait_for_redis_ready(addr: SocketAddr, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        match try_ping(addr, Duration::from_millis(250)) {
            Ok(true) => return true,
            Ok(false) => {}
            Err(_) => {}
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

/// Pick an available local port by binding to port 0.
pub fn pick_free_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

/// Find the repo root by walking up until we see `.git`.
pub fn find_repo_root() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        if dir.join(".git").exists() {
            return Some(dir);
        }
        let parent = dir.parent()?.to_path_buf();
        if parent == dir {
            return None;
        }
        dir = parent;
    }
}

/// Build a per-test data directory under the repo's `.tmp/tests` folder.
pub fn test_dir(name: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let pid = std::process::id();
    let root = find_repo_root().unwrap_or_else(|| std::env::temp_dir());
    root.join(".tmp")
        .join("tests")
        .join(format!("{name}-{pid}-{ts}"))
}

/// Best-effort cleanup of a test directory.
pub fn cleanup_dir(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}

/// RESP connection wrapper that preserves buffered reads between commands.
pub struct RespConn {
    reader: BufReader<TcpStream>,
}

impl RespConn {
    /// Connect to the Redis port with read/write timeouts.
    pub fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).expect("connect redis");
        stream.set_read_timeout(Some(RESP_TIMEOUT)).ok();
        stream.set_write_timeout(Some(RESP_TIMEOUT)).ok();
        Self {
            reader: BufReader::new(stream),
        }
    }

    /// Send a RESP command with bulk string arguments and return the raw response.
    pub fn send_command(&mut self, parts: &[&str]) -> std::io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for part in parts {
            buf.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            buf.extend_from_slice(part.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        let stream = self.reader.get_mut();
        stream.write_all(&buf).expect("write resp command");
        stream.flush().ok();

        read_resp(&mut self.reader)
    }
}

/// Parse a minimal RESP response into raw bytes.
fn read_resp(reader: &mut BufReader<TcpStream>) -> std::io::Result<Vec<u8>> {
    let mut first = [0u8; 1];
    read_exact_with_timeout(reader, &mut first)?;
    match first[0] {
        b'+' | b'-' | b':' => {
            // Simple string, error, or integer: read line.
            let mut line = Vec::new();
            read_until_with_timeout(reader, b'\n', &mut line)?;
            let mut out = vec![first[0]];
            out.extend_from_slice(&line);
            Ok(out)
        }
        b'$' => {
            let mut line = Vec::new();
            read_until_with_timeout(reader, b'\n', &mut line)?;
            let len = parse_bulk_len(&line);
            let mut out = vec![first[0]];
            out.extend_from_slice(&line);
            if len >= 0 {
                let mut data = vec![0u8; len as usize + 2];
                read_exact_with_timeout(reader, &mut data)?;
                out.extend_from_slice(&data);
            }
            Ok(out)
        }
        other => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unexpected resp prefix {other}"),
        )),
    }
}

fn try_ping(addr: SocketAddr, timeout: Duration) -> std::io::Result<bool> {
    let stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();
    let mut reader = BufReader::new(stream);
    let cmd = b"*1\r\n$4\r\nPING\r\n";
    reader.get_mut().write_all(cmd)?;
    reader.get_mut().flush().ok();

    let mut first = [0u8; 1];
    match reader.read_exact(&mut first) {
        Ok(()) => {}
        Err(err)
            if err.kind() == std::io::ErrorKind::WouldBlock
                || err.kind() == std::io::ErrorKind::TimedOut =>
        {
            return Ok(false);
        }
        Err(err) => return Err(err),
    }

    let mut line = Vec::new();
    match reader.read_until(b'\n', &mut line) {
        Ok(0) => return Ok(false),
        Ok(_) => {}
        Err(err)
            if err.kind() == std::io::ErrorKind::WouldBlock
                || err.kind() == std::io::ErrorKind::TimedOut =>
        {
            return Ok(false);
        }
        Err(err) => return Err(err),
    }

    if first[0] != b'+' {
        return Ok(false);
    }
    let text = String::from_utf8_lossy(&line);
    Ok(text.trim_end() == "PONG")
}

/// Parse the bulk string length line ("<len>\r\n").
fn parse_bulk_len(line: &[u8]) -> i64 {
    let text = std::str::from_utf8(line).expect("bulk len utf8");
    let trimmed = text.trim();
    trimmed.parse::<i64>().expect("parse bulk len")
}

fn read_exact_with_timeout(
    reader: &mut BufReader<TcpStream>,
    buf: &mut [u8],
) -> std::io::Result<()> {
    let start = std::time::Instant::now();
    let mut offset = 0usize;
    while offset < buf.len() {
        match reader.read(&mut buf[offset..]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "stream closed",
                ));
            }
            Ok(n) => {
                offset += n;
            }
            Err(err)
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.kind() == std::io::ErrorKind::TimedOut =>
            {
                if start.elapsed() >= RESP_TIMEOUT {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "resp read timed out",
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn read_until_with_timeout(
    reader: &mut BufReader<TcpStream>,
    byte: u8,
    buf: &mut Vec<u8>,
) -> std::io::Result<usize> {
    let start = std::time::Instant::now();
    loop {
        match reader.read_until(byte, buf) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "stream closed",
                ));
            }
            Ok(n) => return Ok(n),
            Err(err)
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.kind() == std::io::ErrorKind::TimedOut =>
            {
                if start.elapsed() >= RESP_TIMEOUT {
                    return Err(err);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => return Err(err),
        }
    }
}

/// Decode a RESP bulk string response into an Option<String>.
pub fn parse_bulk_string(resp: &[u8]) -> Option<String> {
    if resp.is_empty() {
        return None;
    }
    if resp[0] != b'$' {
        return None;
    }
    let mut idx = 1usize;
    let mut len_line = Vec::new();
    while idx < resp.len() {
        let b = resp[idx];
        idx += 1;
        len_line.push(b);
        if b == b'\n' {
            break;
        }
    }
    let len = parse_bulk_len(&len_line);
    if len < 0 {
        return None;
    }
    let len = len as usize;
    if idx + len > resp.len() {
        return None;
    }
    let data = &resp[idx..idx + len];
    Some(String::from_utf8_lossy(data).to_string())
}

/// Write a set of key/value pairs to the Redis port.
pub fn write_keys(addr: SocketAddr, keys: &[(String, String)]) -> Result<(), String> {
    let max_attempts = 3usize;
    for attempt in 0..max_attempts {
        let mut conn = RespConn::connect(addr);
        let mut timed_out = false;
        for (k, v) in keys {
            let resp = match conn.send_command(&["SET", k, v]) {
                Ok(resp) => resp,
                Err(err)
                    if err.kind() == std::io::ErrorKind::TimedOut
                        || err.kind() == std::io::ErrorKind::WouldBlock =>
                {
                    timed_out = true;
                    break;
                }
                Err(err) => return Err(format!("SET failed: {err}")),
            };
            if !resp.starts_with(b"+OK") {
                let text = String::from_utf8_lossy(&resp).to_string();
                return Err(format!("SET failed: {text:?}"));
            }
        }
        if !timed_out {
            return Ok(());
        }
        if attempt + 1 < max_attempts {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    Err("SET failed: resp read timed out".to_string())
}

/// Read keys back from the Redis port and return their values.
pub fn read_keys(addr: SocketAddr, keys: &[String]) -> Vec<Option<String>> {
    let mut out = Vec::with_capacity(keys.len());
    let mut conn = RespConn::connect(addr);
    for k in keys {
        let mut attempt = 0usize;
        let max_attempts = 3usize;
        loop {
            match conn.send_command(&["GET", k]) {
                Ok(resp) => {
                    out.push(parse_bulk_string(&resp));
                    break;
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::TimedOut
                        || err.kind() == std::io::ErrorKind::WouldBlock =>
                {
                    attempt += 1;
                    if attempt >= max_attempts {
                        panic!("GET failed: resp read timed out");
                    }
                    // Reconnect to clear any half-open socket state before retrying.
                    conn = RespConn::connect(addr);
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::AddrNotAvailable
                        || err.kind() == std::io::ErrorKind::ConnectionRefused
                        || err.kind() == std::io::ErrorKind::ConnectionReset
                        || err.kind() == std::io::ErrorKind::BrokenPipe =>
                {
                    attempt += 1;
                    if attempt >= max_attempts {
                        panic!("GET failed: {err}");
                    }
                    std::thread::sleep(Duration::from_millis(50));
                    conn = RespConn::connect(addr);
                }
                Err(err) => panic!("GET failed: {err}"),
            }
        }
    }
    out
}
