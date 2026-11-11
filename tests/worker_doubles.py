"""Picklable test doubles for the spawned computation worker.

``multiprocessing`` with the spawn context only accepts importable top-level
callables, so the test seams live here rather than inside test functions. The
doubles read their behavior from environment variables that the tests set
*before* spawning; child processes inherit the parent environment.
"""

from __future__ import annotations

import os
import time

from app import solver
from app.settings import Settings


def _settings_from_env() -> Settings:
    return Settings.from_env()


def normal_worker(records, links, _settings):
    """Run the real solver unchanged."""
    return solver.normalize(records, links)


def slow_worker(records, links, _settings):
    """Busy-wait beyond any test deadline, then compute (late result)."""
    delay = float(os.environ.get("TEST_WORKER_DELAY", "3"))
    deadline = time.monotonic() + delay
    x = 0.0
    while time.monotonic() < deadline:
        for i in range(200_000):
            x += (i % 5) ** 0.5
    if x < -1:  # pragma: no cover
        raise RuntimeError("unreachable")
    return solver.normalize(records, links)


def crashing_worker(records, links, _settings):
    """Hard-exit the process like an OOM-killed / segfaulting worker."""
    os._exit(2)


def raising_worker(records, links, _settings):
    """Fail with an uncaught exception inside the worker."""
    raise RuntimeError("boom from worker")


def env_gated_worker(records, links, settings):
    """Dispatch on TEST_WORKER_MODE so one runner can cover every scenario."""
    mode = os.environ.get("TEST_WORKER_MODE", "normal")
    if mode == "slow":
        return slow_worker(records, links, settings)
    if mode == "crash":
        return crashing_worker(records, links, settings)
    if mode == "raise":
        return raising_worker(records, links, settings)
    return normal_worker(records, links, settings)


def id_dispatch_worker(records, links, settings):
    """Deterministic per-batch dispatch based on record id (no shared env).

    A batch containing an id starting with ``CRASH`` hard-exits; every other
    batch is computed normally. Lets two threads exercise crash and success
    concurrently without racing over environment variables.
    """
    if any(r.id.startswith("CRASH") for r in records):
        return crashing_worker(records, links, settings)
    return normal_worker(records, links, settings)
