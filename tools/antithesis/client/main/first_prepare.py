#!/usr/bin/env python3
"""Confirm the Antithesis harness is ready to start issuing test commands."""

from __future__ import annotations

from pathlib import Path

from helper_assertions import always, reachable
from helper_holostore import HISTORY_ROOT, nodes_csv
from helper_process import wait_until


def main() -> None:
    """Wait for the health-checker sentinel and prove the shared history mount is writable."""
    sentinel = wait_until(
        lambda: Path(HISTORY_ROOT / "setup-complete.json")
        if (HISTORY_ROOT / "setup-complete.json").exists()
        else None,
        timeout_s=180,
        interval_s=1.0,
        description="health-checker setup sentinel",
    )
    reachable(
        "Antithesis first_prepare completed",
        {
            "sentinel": str(sentinel),
            "nodes": nodes_csv(),
        },
    )
    always(HISTORY_ROOT.exists(), "Shared history directory exists", {"history_root": str(HISTORY_ROOT)})


if __name__ == "__main__":
    main()

