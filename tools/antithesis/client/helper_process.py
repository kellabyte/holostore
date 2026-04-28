"""Process, timing, and formatting helpers shared by Antithesis drivers."""

from __future__ import annotations

import datetime as dt
import os
import time
from typing import Callable, TypeVar

T = TypeVar("T")


def utc_timestamp_fragment() -> str:
    """Return a sortable UTC timestamp with microsecond precision."""
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def unique_suffix() -> str:
    """Return a stable-enough suffix for history and artifact filenames."""
    return f"{os.getpid()}-{utc_timestamp_fragment()}"


def tail_text(text: str | bytes | None, limit: int = 4000) -> str:
    """Return the last limit characters of text for assertion details."""
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    return text[-limit:]


def parse_duration_seconds(duration: str) -> int:
    """Parse small human-readable durations like 20s, 2m, or 1h."""
    duration = duration.strip().lower()
    if duration.endswith("ms"):
        return max(1, int(duration[:-2]) // 1000)
    if duration.endswith("s"):
        return int(duration[:-1])
    if duration.endswith("m"):
        return int(duration[:-1]) * 60
    if duration.endswith("h"):
        return int(duration[:-1]) * 3600
    return int(duration)


def wait_until(
    predicate: Callable[[], T],
    timeout_s: int,
    interval_s: float,
    description: str,
) -> T:
    """Poll a predicate until it returns a truthy value or raise on timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval_s)
    raise TimeoutError(f"timed out waiting for {description}")


def sleep_forever() -> None:
    """Keep a long-running helper container alive after setup work completes."""
    while True:
        time.sleep(3600)

