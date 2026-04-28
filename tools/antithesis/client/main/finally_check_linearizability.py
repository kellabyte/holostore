#!/usr/bin/env python3
"""Merge all workload histories and run a final Porcupine safety check."""

from __future__ import annotations

from pathlib import Path

from helper_assertions import always, reachable
from helper_history import collect_histories, merge_histories, write_history
from helper_holostore import HISTORY_ROOT
from helper_scenarios import process_details, run_checker


def main() -> None:
    """Collect scenario histories, merge them, and re-check successful operations."""
    histories = collect_histories(str(HISTORY_ROOT))
    always(len(histories) > 0, "Final linearizability check found workload histories", {"history_root": str(HISTORY_ROOT)})

    merged_path = str(Path(HISTORY_ROOT) / "merged-history.json")
    merged = merge_histories(histories)
    write_history(merged_path, merged)
    reachable("Merged Antithesis histories for final safety check", {"histories": histories, "merged_history": merged_path})

    checker = run_checker(merged_path, allow_errors=True, log_name="checker-final-merged")
    always(
        checker.process.returncode == 0,
        "Merged successful GET/SET operations are linearizable",
        {
            **process_details(checker.process),
            "merged_history": merged_path,
            "histories": histories,
            "checker_summary": checker.summary,
        },
    )


if __name__ == "__main__":
    main()

