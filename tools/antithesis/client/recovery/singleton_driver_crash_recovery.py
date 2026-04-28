#!/usr/bin/env python3
"""Run a set-heavy workload for Antithesis crash-and-recovery timelines."""

from __future__ import annotations

import json
from pathlib import Path

from helper_assertions import always, reachable, sometimes
from helper_history import load_history
from helper_holostore import HISTORY_ROOT
from helper_process import unique_suffix
from helper_scenarios import (
    WorkloadScenario,
    env_bool,
    env_int,
    env_str,
    process_details,
    run_workload,
    scenario_value_prefix,
)


def write_acknowledged_values(history_path: str) -> str:
    """Persist successful SET values for optional post-recovery spot checks."""
    history = load_history(history_path)
    acknowledged: dict[str, list[str]] = {}
    for op in history.get("ops", []):
        if op.get("op") != "set":
            continue
        if op.get("result", {}).get("type") != "ok":
            continue
        value = op.get("value")
        key = op.get("key")
        if not value or not key:
            continue
        acknowledged.setdefault(key, []).append(value)

    output_path = Path(HISTORY_ROOT) / f"recovery-acked-{unique_suffix()}.json"
    output_path.write_text(
        json.dumps({"history": history_path, "acknowledged": acknowledged}, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return str(output_path)


def main() -> None:
    """Generate acknowledged writes that later recovery checks can probe."""
    scenario = WorkloadScenario(
        name="crash-recovery",
        clients=env_int("CLIENTS", 4),
        keys=env_int("KEYS", 8),
        set_pct=env_int("SET_PCT", 100),
        duration=env_str("DURATION", "30s"),
        fail_fast=env_bool("FAIL_FAST", False),
        allow_errors=env_bool("ALLOW_ERRORS", True),
        fault_disconnect_pct=env_int("FAULT_DISCONNECT_PCT", 0),
        key_prefix=f"antithesis_recovery_{scenario_value_prefix('keys')}",
        value_prefix=env_str("VALUE_PREFIX", scenario_value_prefix("recovery")),
        checksum_values=env_bool("CHECKSUM_VALUES", True),
        seed=env_int("SEED", 0),
    )
    reachable("Starting crash-recovery workload", {"scenario": scenario.name})
    run = run_workload(scenario)
    always(run.process.returncode == 0, "Crash-recovery workload exits successfully", {**process_details(run.process), "summary": run.summary, "history": run.history_path})
    ack_path = write_acknowledged_values(run.history_path)
    reachable("Persisted acknowledged recovery writes", {"acknowledged_path": ack_path, "history": run.history_path})
    sometimes(int(run.summary.get("ok_sets", 0)) > 0, "Crash-recovery workload observed successful writes", {"summary": run.summary, "acknowledged_path": ack_path})


if __name__ == "__main__":
    main()

