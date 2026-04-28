"""History loading and merge helpers for final Antithesis correctness checks."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any


def load_history(path: str) -> dict[str, Any]:
    """Load a holo-workload history JSON file."""
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_history(path: str, history: dict[str, Any]) -> None:
    """Write a Porcupine-compatible history JSON file."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(history, indent=2, sort_keys=True), encoding="utf-8")


def collect_histories(root: str = "/history") -> list[str]:
    """Collect real workload history files while skipping summaries and merged outputs."""
    paths: list[str] = []
    for path in sorted(Path(root).glob("history-*.json")):
        name = path.name
        if name == "merged-history.json":
            continue
        if name.endswith(".summary.json") or name.endswith(".checker-summary.json"):
            continue
        paths.append(str(path))
    return paths


def merge_histories(paths: list[str]) -> dict[str, Any]:
    """Merge workload histories, offset client IDs, and align times by absolute start."""
    loaded = [(path, load_history(path)) for path in paths]
    start_us_values = [
        int(history.get("meta", {}).get("start_unix_us", 0))
        for _, history in loaded
        if int(history.get("meta", {}).get("start_unix_us", 0)) > 0
    ]
    base_start_us = min(start_us_values) if start_us_values else 0

    merged_ops: list[dict[str, Any]] = []
    metadata_sources: list[dict[str, Any]] = []
    client_offset = 0

    for path, history in loaded:
        meta = history.get("meta", {})
        metadata_sources.append({"path": path, "meta": meta})
        start_us = int(meta.get("start_unix_us", 0))
        time_offset_us = max(0, start_us - base_start_us) if base_start_us and start_us else 0

        max_client = -1
        for op in history.get("ops", []):
            merged_op = copy.deepcopy(op)
            merged_op["client"] = int(merged_op.get("client", 0)) + client_offset
            merged_op["call_us"] = int(merged_op.get("call_us", 0)) + time_offset_us
            merged_op["return_us"] = int(merged_op.get("return_us", 0)) + time_offset_us
            merged_ops.append(merged_op)
            max_client = max(max_client, int(op.get("client", 0)))

        client_offset += max_client + 1 if max_client >= 0 else int(meta.get("clients", 0))

    merged_ops.sort(
        key=lambda op: (
            int(op.get("call_us", 0)),
            int(op.get("client", 0)),
            int(op.get("return_us", 0)),
        )
    )

    nodes: list[str] = []
    for _, history in loaded:
        for node in history.get("meta", {}).get("nodes", []):
            if node not in nodes:
                nodes.append(node)

    return {
        "meta": {
            "nodes": nodes,
            "clients": client_offset,
            "keys": 0,
            "set_pct": 0,
            "duration_ms": 0,
            "seed": 0,
            "start_unix_us": base_start_us,
            "sources": metadata_sources,
        },
        "ops": merged_ops,
    }

