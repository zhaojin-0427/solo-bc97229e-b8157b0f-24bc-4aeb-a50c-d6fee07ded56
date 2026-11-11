"""Runtime configuration for batch computation capacity and time limits."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise RuntimeError(f"{name} must be >= {minimum}, got {value}")
    return value


@dataclass(frozen=True)
class Settings:
    """Process-level limits, all overridable through environment variables.

    max_concurrent_batches: how many batches a single API process may compute
        at the same time (default 2). Requests that pass validation while every
        slot is busy get HTTP 429 immediately - they are never queued.
    batch_timeout_seconds: wall-clock deadline for one batch (default 10 s).
        A batch still running when the deadline expires is killed and answered
        with HTTP 504.
    slow_batch_id / crash_batch_id: demo seams. A record with this id makes the
        computation worker sleep (to exercise overload/timeout) or exit hard
        with an error code (to exercise 503). Empty string disables the seam.
    """

    max_concurrent_batches: int = 2
    batch_timeout_seconds: float = 10.0
    slow_batch_id: str = ""
    crash_batch_id: str = ""

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            max_concurrent_batches=_env_int("MAX_CONCURRENT_BATCHES", 2),
            batch_timeout_seconds=float(
                _env_int("BATCH_TIMEOUT_SECONDS", 10)
            ),
            slow_batch_id=os.environ.get("SLOW_BATCH_ID", ""),
            crash_batch_id=os.environ.get("CRASH_BATCH_ID", ""),
        )
