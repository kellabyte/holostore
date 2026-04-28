#!/usr/bin/env python3
"""Wait for the HoloStore cluster to become usable, then emit setup_complete."""

from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path
from typing import Any

try:
    from antithesis.lifecycle import setup_complete
except Exception:
    def setup_complete(details: dict[str, Any] | None = None) -> None:
        """Fallback local no-op that preserves the details in Antithesis JSONL form."""
        output_dir = os.getenv("ANTITHESIS_OUTPUT_DIR")
        if not output_dir:
            return
        output_path = Path(output_dir) / "sdk.jsonl"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"antithesis_setup": {"status": "complete", "details": details}}))
            handle.write("\n")


NODES = [("holo-node1", 6379), ("holo-node2", 6379), ("holo-node3", 6379)]
HISTORY_DIR = Path(os.getenv("HOLO_HISTORY_DIR", "/history"))
SETUP_SENTINEL = HISTORY_DIR / "setup-complete.json"


def wait_port(host: str, port: int, deadline_s: int = 180) -> None:
    """Wait until the target port is reachable or raise after the deadline."""
    deadline = time.time() + deadline_s
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            time.sleep(0.5)
    raise RuntimeError(f"timed out waiting for {host}:{port}")


def send_resp_command(host: str, port: int, command: str) -> bytes:
    """Send a minimal RESP command and return the raw server response."""
    payload = f"*1\r\n${len(command)}\r\n{command}\r\n".encode("utf-8")
    with socket.create_connection((host, port), timeout=2) as conn:
        conn.sendall(payload)
        return conn.recv(65535)


def wait_redis_ready(host: str, port: int, deadline_s: int = 180) -> dict[str, bool]:
    """Wait until PING succeeds and opportunistically probe HOLOMETRICS."""
    deadline = time.time() + deadline_s
    metrics_ok = False
    while time.time() < deadline:
        try:
            ping = send_resp_command(host, port, "PING")
            if not ping.startswith(b"+PONG"):
                raise RuntimeError(f"unexpected PING response from {host}:{port}: {ping!r}")
            try:
                metrics = send_resp_command(host, port, "HOLOMETRICS")
                metrics_ok = metrics.startswith(b"$") or metrics.startswith(b"+")
            except OSError:
                metrics_ok = False
            return {"ping": True, "holometrics": metrics_ok}
        except OSError:
            time.sleep(0.5)
    raise RuntimeError(f"timed out waiting for Redis readiness on {host}:{port}")


def main() -> None:
    """Wait for all nodes, record readiness details, and keep the container alive."""
    readiness: dict[str, dict[str, bool]] = {}
    for host, port in NODES:
        wait_port(host, port)
        readiness[f"{host}:{port}"] = wait_redis_ready(host, port)

    details = {
        "message": "HoloStore cluster ready",
        "nodes": [f"{host}:{port}" for host, port in NODES],
        "readiness": readiness,
    }
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    SETUP_SENTINEL.write_text(json.dumps(details, indent=2, sort_keys=True), encoding="utf-8")
    setup_complete(details)

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()

