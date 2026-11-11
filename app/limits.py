"""Capacity control for concurrent batch computations.

A single API process may run at most ``max_concurrent_batches`` batches at
once. The limiter is deliberately *non-queueing*: :meth:`try_acquire` returns
``None`` when every slot is busy so the caller can answer HTTP 429 immediately
instead of parking the request.

A slot is released only by the endpoint's ``finally`` block, which runs after
the supervisor has reaped the worker process - capacity therefore tracks real
resource usage, including crashed and timed-out batches.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class Slot:
    """Opaque handle returned while a capacity slot is held."""

    index: int


class CapacityLimiter:
    def __init__(self, max_concurrent_batches: int) -> None:
        if max_concurrent_batches < 1:
            raise ValueError("max_concurrent_batches must be >= 1")
        self._capacity = max_concurrent_batches
        self._free: list[int] = list(range(max_concurrent_batches))
        self._lock = threading.Lock()
        self._in_use = 0

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def in_use(self) -> int:
        with self._lock:
            return self._in_use

    def try_acquire(self) -> Slot | None:
        """Take a free slot, or return ``None`` immediately when saturated."""
        with self._lock:
            if not self._free:
                return None
            index = self._free.pop()
            self._in_use += 1
            return Slot(index=index)

    def release(self, slot: Slot) -> None:
        with self._lock:
            if slot.index in self._free or not (0 <= slot.index < self._capacity):
                raise RuntimeError(f"invalid slot release: {slot!r}")
            self._free.append(slot.index)
            self._in_use -= 1
