"""Process-isolated batch execution with hard time limits.

Every accepted batch is computed in a freshly spawned, short-lived OS process.
The API process only supervises: it enforces the per-batch wall-clock deadline,
reaps the worker deterministically and hands back one of three outcomes:

* :class:`BatchSucceeded` - the worker finished in time with a complete result;
* :class:`BatchTimedOut` - the deadline expired, the worker was terminated and
  reaped, and any result it produced *after* the deadline is discarded;
* :class:`BatchFailed` - the worker exited abnormally (non-zero status or an
  uncaught exception reported over the result channel).

A capacity slot is considered occupied until the worker process has actually
ended and been reaped, so a crashed or timed-out batch can never leak a slot.
One batch dying never touches another batch: each worker is an independent
process and all supervisor state is local to one :meth:`BatchRunner.run` call.
"""

from __future__ import annotations

import multiprocessing
import os
import queue
import time
from dataclasses import dataclass
from typing import Any, Callable

from . import solver
from .settings import Settings


@dataclass
class BatchSucceeded:
    """The worker completed normally within the deadline."""

    outcome: Any  # solver.Outcome (Ok | NoLegalCandidate | NoGlobalSolution)


@dataclass
class BatchTimedOut:
    """The deadline expired; the worker was killed and reaped."""

    timeout_seconds: float


@dataclass
class BatchFailed:
    """The worker process exited abnormally or raised before producing output."""

    reason: str
    exit_code: int | None = None


# Internal message envelope exchanged through the multiprocessing queue.
@dataclass
class _WorkerResult:
    kind: str  # "ok" | "error"
    payload: Any = None
    error: str = ""


def compute_batch(
    records: list[solver.ScanInput],
    links: list[solver.LinkInput],
    settings: Settings,
) -> solver.Outcome:
    """Entry point executed inside the worker process.

    The demo seams (slow / crashing record id) are read from the settings the
    parent hands over, so behavior is fully controlled by environment variables.
    """
    ids = {r.id for r in records}
    if settings.crash_batch_id and settings.crash_batch_id in ids:
        # Simulate a hard failure of the computation process (e.g. segfault /
        # killed by the OOM killer): exit without putting anything on the queue.
        os._exit(2)
    if settings.slow_batch_id and settings.slow_batch_id in ids:
        # Keep the pure-Python computation busy long enough to cross the
        # deadline or hold a capacity slot for an overload demonstration.
        deadline = time.monotonic() + settings.batch_timeout_seconds + 5.0
        x = 0.0
        while time.monotonic() < deadline:
            for i in range(1_000_000):
                x += (i % 7) ** 0.5
        if x < -1:  # pragma: no cover - keeps the optimizer honest
            raise RuntimeError("unreachable")
    return solver.normalize(records, links)


def _worker_main(
    conn: "multiprocessing.Queue[_WorkerResult]",
    target: Callable[..., Any],
    records: list[solver.ScanInput],
    links: list[solver.LinkInput],
    settings: Settings,
) -> None:
    """Top-level (picklable) function run in the spawned process."""
    try:
        result = target(records, links, settings)
        conn.put(_WorkerResult(kind="ok", payload=result))
    except Exception as exc:  # the worker must report, not die silently
        conn.put(_WorkerResult(kind="error", error=f"{type(exc).__name__}: {exc}"))


class BatchRunner:
    """Supervises one batch computation in an isolated spawned process."""

    def __init__(
        self,
        settings: Settings,
        target: Callable[..., Any] = compute_batch,
        ctx: multiprocessing.context.BaseContext | None = None,
    ) -> None:
        self._settings = settings
        self._target = target
        # "spawn" gives a truly fresh interpreter: no inherited locks, threads
        # or partially-initialized module state from the API process.
        self._ctx = ctx or multiprocessing.get_context("spawn")

    def run(
        self,
        records: list[solver.ScanInput],
        links: list[solver.LinkInput],
    ) -> BatchSucceeded | BatchTimedOut | BatchFailed:
        result_q: "multiprocessing.Queue[_WorkerResult]" = self._ctx.Queue()
        proc = self._ctx.Process(
            target=_worker_main,
            args=(result_q, self._target, records, links, self._settings),
            daemon=True,
        )
        proc.start()
        timeout = self._settings.batch_timeout_seconds
        # Poll the result channel in short slices so an abnormal worker exit
        # (which sends no message) is detected immediately instead of after
        # the full deadline; the deadline itself is measured on the wall clock.
        poll_interval = 0.05

        try:
            deadline = time.monotonic() + timeout
            message: _WorkerResult | None = None
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # Deadline elapsed while the worker was still computing.
                    # Kill the *actual* computation, then wait until the kernel
                    # reaps it before declaring the slot free. SIGTERM -> SIGKILL.
                    self._terminate_and_reap(proc)
                    self._drain_late_results(result_q)
                    return BatchTimedOut(timeout_seconds=timeout)
                try:
                    message = result_q.get(timeout=min(remaining, poll_interval))
                    break
                except queue.Empty:
                    if proc.exitcode is not None:
                        # The process ended without putting a result on the
                        # channel (hard exit / killed). Reap and fail at once.
                        proc.join()
                        self._drain_late_results(result_q)
                        return BatchFailed(
                            reason="computation process exited without a result",
                            exit_code=proc.exitcode,
                        )

            # The worker sent a message; make sure the process itself has
            # exited and is reaped before the slot is released.
            proc.join()
            if proc.exitcode != 0:
                # Process died (or was killed) around the same time: a late
                # message must never be treated as a successful conversion.
                self._drain_late_results(result_q)
                return BatchFailed(
                    reason="computation process exited abnormally",
                    exit_code=proc.exitcode,
                )
            if message.kind == "error":
                return BatchFailed(reason=message.error, exit_code=proc.exitcode)
            return BatchSucceeded(outcome=message.payload)
        finally:
            # Belt-and-braces: never leave a live child or a queue feeder thread.
            if proc.is_alive():
                self._terminate_and_reap(proc)
            result_q.close()
            result_q.join_thread()

    @staticmethod
    def _terminate_and_reap(proc: multiprocessing.Process) -> None:
        proc.terminate()
        proc.join(timeout=5)
        if proc.is_alive():  # pragma: no cover - hard kill if SIGTERM ignored
            proc.kill()
            proc.join()

    @staticmethod
    def _drain_late_results(result_q: "multiprocessing.Queue") -> None:
        """Discard anything the worker produced after the deadline/exit."""
        while True:
            try:
                result_q.get_nowait()
            except queue.Empty:
                return
