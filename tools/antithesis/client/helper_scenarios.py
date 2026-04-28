"""Scenario definitions and runners for HoloStore Antithesis drivers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from helper_holostore import (
    checker_summary_path_for,
    history_path,
    load_json,
    nodes_csv,
    run_checked,
    summary_path_for,
)
from helper_process import parse_duration_seconds, tail_text, unique_suffix


@dataclass(slots=True)
class WorkloadScenario:
    """Runtime configuration for one holo-workload invocation."""

    name: str
    clients: int
    keys: int
    set_pct: int
    duration: str
    op_timeout: str = "5s"
    fail_fast: bool = False
    allow_errors: bool = False
    fault_disconnect_pct: int = 0
    key_prefix: str = "antithesis_shared_"
    value_prefix: str = ""
    checksum_values: bool = True
    seed: int = 0
    read_nodes: str | None = None
    write_nodes: str | None = None


@dataclass(slots=True)
class WorkloadRun:
    """Captured result of one holo-workload execution."""

    scenario: WorkloadScenario
    history_path: str
    summary_path: str
    process: Any
    summary: dict[str, Any]


@dataclass(slots=True)
class CheckerRun:
    """Captured result of one porcupine-check execution."""

    history_path: str
    summary_path: str
    process: Any
    summary: dict[str, Any]


def env_int(name: str, default: int) -> int:
    """Read an integer environment override."""
    return int(os.getenv(name, str(default)))


def env_str(name: str, default: str) -> str:
    """Read a string environment override."""
    return os.getenv(name, default)


def env_bool(name: str, default: bool) -> bool:
    """Read a boolean environment override using common truthy spellings."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def scenario_value_prefix(name: str) -> str:
    """Return a unique value prefix for one scenario run."""
    return f"{name}_{unique_suffix()}_"


def run_workload(scenario: WorkloadScenario) -> WorkloadRun:
    """Run holo-workload with a scenario config and return captured artifacts."""
    history_json = history_path(f"history-{scenario.name}")
    workload_summary_json = summary_path_for(history_json)
    cmd = [
        "/usr/local/bin/holo-workload",
        "run",
        "--nodes",
        nodes_csv(),
        "--clients",
        str(scenario.clients),
        "--keys",
        str(scenario.keys),
        "--key-prefix",
        scenario.key_prefix,
        "--value-prefix",
        scenario.value_prefix,
        "--set-pct",
        str(scenario.set_pct),
        "--duration",
        scenario.duration,
        "--op-timeout",
        scenario.op_timeout,
        f"--fail-fast={'true' if scenario.fail_fast else 'false'}",
        "--fault-disconnect-pct",
        str(scenario.fault_disconnect_pct),
        "--out",
        history_json,
        "--summary-out",
        workload_summary_json,
        "--seed",
        str(scenario.seed),
    ]
    if scenario.checksum_values:
        cmd.append("--checksum-values")
    if scenario.read_nodes:
        cmd.extend(["--read-nodes", scenario.read_nodes])
    if scenario.write_nodes:
        cmd.extend(["--write-nodes", scenario.write_nodes])

    timeout_s = max(60, parse_duration_seconds(scenario.duration) + 30)
    process = run_checked(cmd, timeout_s=timeout_s, log_name=f"workload-{scenario.name}")
    summary = load_json(workload_summary_json, {})
    return WorkloadRun(
        scenario=scenario,
        history_path=history_json,
        summary_path=workload_summary_json,
        process=process,
        summary=summary,
    )


def run_checker(history_json: str, allow_errors: bool, log_name: str) -> CheckerRun:
    """Run Porcupine on one history and capture the checker summary JSON."""
    checker_summary_json = checker_summary_path_for(history_json)
    cmd = [
        "/usr/local/bin/porcupine-check",
        "--history",
        history_json,
        "--json-summary",
        checker_summary_json,
    ]
    if allow_errors:
        cmd.append("--allow-errors")

    process = run_checked(cmd, timeout_s=120, log_name=log_name)
    summary = load_json(checker_summary_json, {})
    return CheckerRun(
        history_path=history_json,
        summary_path=checker_summary_json,
        process=process,
        summary=summary,
    )


def process_details(process: Any) -> dict[str, Any]:
    """Render a CompletedProcess into compact assertion details."""
    return {
        "returncode": process.returncode,
        "stdout": tail_text(getattr(process, "stdout", "")),
        "stderr": tail_text(getattr(process, "stderr", "")),
    }
