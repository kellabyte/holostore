"""Cluster, RESP, and subprocess helpers for the HoloStore Antithesis harness."""

from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
from pathlib import Path
from typing import Any

from helper_process import tail_text, unique_suffix

REDIS_PORT = 6379
GRPC_PORT = 15051
REDIS_SERVICES = ("holo-node1", "holo-node2", "holo-node3")
HISTORY_ROOT = Path(os.getenv("HOLO_HISTORY_DIR", "/history"))


def resolve(host: str) -> str:
    """Resolve a service name to the IPv4 address used inside the Compose network."""
    return socket.gethostbyname(host)


def redis_nodes() -> list[str]:
    """Return Redis endpoints for all HoloStore nodes as ip:port strings."""
    return [f"{resolve(host)}:{REDIS_PORT}" for host in REDIS_SERVICES]


def nodes_csv() -> str:
    """Return the CSV node list expected by holo-workload --nodes."""
    return ",".join(redis_nodes())


def grpc_target(host: str = "holo-node1") -> str:
    """Return a gRPC endpoint for holoctl-style cluster checks."""
    return f"{resolve(host)}:{GRPC_PORT}"


def history_path(prefix: str) -> str:
    """Return a unique history path under the shared /history mount."""
    HISTORY_ROOT.mkdir(parents=True, exist_ok=True)
    return str(HISTORY_ROOT / f"{prefix}-{unique_suffix()}.json")


def artifact_stem(prefix: str) -> str:
    """Return a unique artifact stem under /history."""
    HISTORY_ROOT.mkdir(parents=True, exist_ok=True)
    return str(HISTORY_ROOT / f"{prefix}-{unique_suffix()}")


def summary_path_for(history_json: str) -> str:
    """Map a history path to the sibling workload summary JSON path."""
    return str(Path(history_json).with_suffix(".summary.json"))


def checker_summary_path_for(history_json: str) -> str:
    """Map a history path to the sibling Porcupine checker summary JSON path."""
    path = Path(history_json)
    return str(path.with_name(f"{path.stem}.checker-summary.json"))


def load_json(path: str, default: Any) -> Any:
    """Load JSON from disk, returning default when the file is absent."""
    file_path = Path(path)
    if not file_path.exists():
        return default
    with file_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def run_checked(cmd: list[str], timeout_s: int, log_name: str) -> subprocess.CompletedProcess[str]:
    """Run a subprocess, capture output, and write stdout/stderr artifacts to /history."""
    stem = artifact_stem(log_name)
    stdout_path = f"{stem}.stdout"
    stderr_path = f"{stem}.stderr"
    command_path = f"{stem}.command"

    Path(command_path).write_text(
        " ".join(shlex.quote(part) for part in cmd) + "\n", encoding="utf-8"
    )

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = tail_text(exc.stdout)
        stderr = tail_text(exc.stderr)
        proc = subprocess.CompletedProcess(cmd, 124, stdout=stdout, stderr=stderr)

    Path(stdout_path).write_text(proc.stdout or "", encoding="utf-8")
    Path(stderr_path).write_text(proc.stderr or "", encoding="utf-8")
    return proc


def parse_endpoint(endpoint: str) -> tuple[str, int]:
    """Split an ip:port endpoint string into host and port."""
    host, port = endpoint.rsplit(":", 1)
    return host, int(port)


def _encode_resp(parts: tuple[str, ...]) -> bytes:
    """Encode a small Redis command as RESP2."""
    encoded = [f"*{len(parts)}\r\n".encode("utf-8")]
    for part in parts:
        raw = part.encode("utf-8")
        encoded.append(f"${len(raw)}\r\n".encode("utf-8"))
        encoded.append(raw + b"\r\n")
    return b"".join(encoded)


def _read_resp(handle: Any) -> bytes | None:
    """Read a single RESP2 response from a buffered file object."""
    prefix = handle.read(1)
    if prefix == b"":
        raise RuntimeError("connection closed before response")
    if prefix == b"+":
        return handle.readline().rstrip(b"\r\n")
    if prefix == b"-":
        raise RuntimeError(handle.readline().decode("utf-8", errors="replace").rstrip())
    if prefix == b"$":
        length = int(handle.readline().strip())
        if length < 0:
            return None
        data = handle.read(length)
        handle.read(2)
        return data
    if prefix == b":":
        return handle.readline().rstrip(b"\r\n")
    raise RuntimeError(f"unsupported RESP prefix: {prefix!r}")


def redis_command(endpoint: str, *parts: str, timeout_s: float = 2.0) -> bytes | None:
    """Send one Redis command to an endpoint and return the decoded RESP payload."""
    host, port = parse_endpoint(endpoint)
    with socket.create_connection((host, port), timeout=timeout_s) as conn:
        conn.sendall(_encode_resp(tuple(parts)))
        conn.shutdown(socket.SHUT_WR)
        with conn.makefile("rb") as handle:
            return _read_resp(handle)


def redis_ping(endpoint: str, timeout_s: float = 2.0) -> bool:
    """Return true when Redis PING succeeds against an endpoint."""
    try:
        return redis_command(endpoint, "PING", timeout_s=timeout_s) == b"PONG"
    except OSError:
        return False
    except RuntimeError:
        return False


def fetch_holometrics(endpoint: str, timeout_s: float = 2.0) -> str:
    """Fetch HOLOMETRICS text from one node."""
    payload = redis_command(endpoint, "HOLOMETRICS", timeout_s=timeout_s)
    if payload is None:
        return ""
    return payload.decode("utf-8", errors="replace")


def redis_get(endpoint: str, key: str, timeout_s: float = 2.0) -> str | None:
    """Read one key via Redis GET and decode the UTF-8 value when present."""
    payload = redis_command(endpoint, "GET", key, timeout_s=timeout_s)
    if payload is None:
        return None
    return payload.decode("utf-8", errors="replace")


def reachable_nodes(timeout_s: float = 2.0) -> list[str]:
    """Return the subset of cluster Redis endpoints that currently answer PING."""
    return [endpoint for endpoint in redis_nodes() if redis_ping(endpoint, timeout_s=timeout_s)]

