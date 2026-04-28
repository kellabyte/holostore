#!/usr/bin/env python3
"""Run lightweight reachability checks that remain valid during fault injection."""

from __future__ import annotations

from helper_assertions import always, reachable, sometimes
from helper_holostore import reachable_nodes, redis_nodes


def main() -> None:
    """Assert that the probe itself did not crash and at least one node can be reached sometimes."""
    configured = redis_nodes()
    reachable_now = reachable_nodes()
    reachable("Anytime health probe executed", {"configured_nodes": configured, "reachable_nodes": reachable_now})
    always(True, "Anytime health script completed without crashing", {"reachable_nodes": reachable_now})
    sometimes(len(reachable_now) > 0, "At least one Redis node is reachable during faults", {"configured_nodes": configured, "reachable_nodes": reachable_now})


if __name__ == "__main__":
    main()

