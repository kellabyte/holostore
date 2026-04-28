"""Antithesis assertion wrappers with a local fallback mode.

The client image includes the Antithesis SDK, but local Docker Compose runs
must still fail their process when an always assertion is false. Set
``ANTITHESIS_LOCAL_ASSERTS=1`` for local suite execution to force that behavior.
"""

from __future__ import annotations

import os
from typing import Any, Mapping


def _truthy(value: str | None) -> bool:
    """Return true when an environment flag requests enabled behavior."""

    return value in {"1", "true", "TRUE", "yes", "YES", "y", "Y"}


def _local_always(condition: bool, message: str, details: Mapping[str, Any] | None = None) -> None:
    """Fail locally when an always-asserted condition does not hold."""

    if not condition:
        raise AssertionError(f"{message}: {details}")


def _local_always_or_unreachable(
    condition: bool, message: str, details: Mapping[str, Any] | None = None
) -> None:
    """Local fallback for optional-path always assertions."""

    if not condition:
        raise AssertionError(f"{message}: {details}")


def _local_reachable(message: str, details: Mapping[str, Any] | None = None) -> None:
    """Local no-op reachability marker."""

    return None


def _local_sometimes(
    condition: bool, message: str, details: Mapping[str, Any] | None = None
) -> None:
    """Local no-op for coverage-style assertions."""

    return None


def _local_unreachable(message: str, details: Mapping[str, Any] | None = None) -> None:
    """Fail locally if an unreachable path is hit."""

    raise AssertionError(f"unreachable reached: {message}: {details}")


if _truthy(os.getenv("ANTITHESIS_LOCAL_ASSERTS")):
    always = _local_always
    always_or_unreachable = _local_always_or_unreachable
    reachable = _local_reachable
    sometimes = _local_sometimes
    unreachable = _local_unreachable
else:
    try:
        from antithesis.assertions import always, reachable, sometimes, unreachable

        try:
            from antithesis.assertions import always_or_unreachable
        except Exception:
            def always_or_unreachable(
                condition: bool, message: str, details: Mapping[str, Any] | None = None
            ) -> None:
                """Fallback to always() if the SDK lacks always_or_unreachable."""
                always(condition, message, details)
    except Exception:
        always = _local_always
        always_or_unreachable = _local_always_or_unreachable
        reachable = _local_reachable
        sometimes = _local_sometimes
        unreachable = _local_unreachable
