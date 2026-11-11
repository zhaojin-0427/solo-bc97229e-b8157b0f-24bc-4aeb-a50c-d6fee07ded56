"""Concurrency, isolation and capacity tests for the process-based /normalize.

These tests spawn real OS processes via the spawn context (no mocking of the
process boundary), so they actually verify:

* saturation -> immediate 429 with no queueing;
* per-batch deadline -> 504, worker killed and reaped, slot released;
* abnormal worker exit / in-worker exception -> 503, slot released;
* one batch dying does not abort another;
* /healthz answers while batches are in progress;
* 422 validation happens before a capacity slot is taken;
* late worker results are never returned as success;
* every batch is all-or-nothing (no partial conversion).
"""

from __future__ import annotations

import threading
import time
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from app import main, solver
from app.limits import CapacityLimiter
from app.runner import BatchRunner, BatchFailed, BatchSucceeded, BatchTimedOut
from app.settings import Settings
from tests import worker_doubles

SIMPLE_BATCH = {
    "records": [
        {"id": "A", "local_time": "2024-06-01T10:00:00", "timezone": "UTC"},
        {"id": "B", "local_time": "2024-06-01T11:00:00", "timezone": "UTC"},
    ],
    "links": [{"from_id": "A", "to_id": "B", "min_seconds": 3600, "max_seconds": 3600}],
}

INVALID_BATCH = {
    "records": [
        {"id": "A", "local_time": "2024-06-01T10:00:00", "timezone": "Mars/Olympus"},
    ],
    "links": [],
}


@pytest.fixture
def client(monkeypatch):
    """Fresh app wiring per test: 2 slots, 1 s deadline, env-gated test worker."""
    monkeypatch.setenv("MAX_CONCURRENT_BATCHES", "2")
    monkeypatch.setenv("BATCH_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("TEST_WORKER_MODE", "normal")
    monkeypatch.delenv("TEST_WORKER_DELAY", raising=False)

    settings = Settings.from_env()
    monkeypatch.setattr(main, "settings", settings)
    monkeypatch.setattr(main, "limiter", CapacityLimiter(settings.max_concurrent_batches))
    monkeypatch.setattr(
        main,
        "runner",
        BatchRunner(settings, target=worker_doubles.env_gated_worker),
    )
    with TestClient(main.app) as c:
        yield c


@pytest.fixture
def id_dispatch_client(monkeypatch):
    """App wiring whose worker dispatches on record id (thread-safe seams)."""
    monkeypatch.setenv("MAX_CONCURRENT_BATCHES", "2")
    monkeypatch.setenv("BATCH_TIMEOUT_SECONDS", "5")
    settings = Settings.from_env()
    monkeypatch.setattr(main, "settings", settings)
    monkeypatch.setattr(main, "limiter", CapacityLimiter(settings.max_concurrent_batches))
    monkeypatch.setattr(
        main,
        "runner",
        BatchRunner(settings, target=worker_doubles.id_dispatch_worker),
    )
    with TestClient(main.app) as c:
        yield c


def _post_async(client, payload, box, name):
    resp = client.post("/normalize", json=payload)
    box[name] = (resp.status_code, resp.json())


def test_capacity_two_slots_third_valid_request_gets_429_without_queueing(client, monkeypatch):
    monkeypatch.setenv("TEST_WORKER_MODE", "slow")
    monkeypatch.setenv("TEST_WORKER_DELAY", "3")

    box: dict = {}
    threads = [
        threading.Thread(target=_post_async, args=(client, SIMPLE_BATCH, box, f"t{i}"),
                         daemon=True)
        for i in range(2)
    ]
    for t in threads:
        t.start()

    # Wait until both slots are genuinely occupied by live worker processes.
    deadline = time.monotonic() + 5
    while main.limiter.in_use < 2 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert main.limiter.in_use == 2

    # Saturated: the third validated request is rejected immediately.
    started = time.monotonic()
    resp = client.post("/normalize", json=SIMPLE_BATCH)
    rejected_in = time.monotonic() - started
    assert resp.status_code == 429
    assert rejected_in < 0.5  # no queueing, no waiting for a slot
    body = resp.json()
    assert body["error"] == "capacity_exhausted"
    assert body["max_concurrent_batches"] == 2

    # The two in-flight batches hit their deadline and free their slots.
    for t in threads:
        t.join(timeout=15)
    assert set(box) == {"t0", "t1"}
    assert all(code == 504 for code, _ in box.values())
    assert main.limiter.in_use == 0  # slots reclaimed after reap


def test_slots_are_reusable_after_timeout(client, monkeypatch):
    monkeypatch.setenv("TEST_WORKER_MODE", "slow")
    monkeypatch.setenv("TEST_WORKER_DELAY", "3")
    resp = client.post("/normalize", json=SIMPLE_BATCH)
    assert resp.status_code == 504
    assert main.limiter.in_use == 0

    # A following request must be able to take the released slot and succeed.
    monkeypatch.setenv("TEST_WORKER_MODE", "normal")
    resp = client.post("/normalize", json=SIMPLE_BATCH)
    assert resp.status_code == 200
    assert resp.json()["summary"]["total"] == 2


def test_timeout_returns_504_and_never_a_late_success(client, monkeypatch):
    monkeypatch.setenv("TEST_WORKER_MODE", "slow")
    monkeypatch.setenv("TEST_WORKER_DELAY", "3")
    resp = client.post("/normalize", json=SIMPLE_BATCH)
    assert resp.status_code == 504
    body = resp.json()
    assert body["error"] == "batch_timeout"
    assert body["timeout_seconds"] == 1
    assert "records" not in body  # late result discarded, no partial conversion
    assert main.limiter.in_use == 0


def test_worker_hard_exit_returns_503_and_reclaims_slot(client, monkeypatch):
    # A generous deadline distinguishes an abnormal exit (503) from a timeout
    # (504): the worker crashes almost immediately, well within 5 seconds.
    monkeypatch.setenv("BATCH_TIMEOUT_SECONDS", "5")
    monkeypatch.setattr(main, "settings", Settings.from_env())
    monkeypatch.setattr(
        main,
        "runner",
        BatchRunner(main.settings, target=worker_doubles.env_gated_worker),
    )
    monkeypatch.setenv("TEST_WORKER_MODE", "crash")
    resp = client.post("/normalize", json=SIMPLE_BATCH)
    assert resp.status_code == 503
    body = resp.json()
    assert body["error"] == "computation_unavailable"
    assert body["exit_code"] == 2
    assert main.limiter.in_use == 0

    # Service keeps serving: a later healthy batch succeeds on the freed slot.
    monkeypatch.setenv("TEST_WORKER_MODE", "normal")
    assert client.post("/normalize", json=SIMPLE_BATCH).status_code == 200


def test_worker_exception_returns_503(client, monkeypatch):
    monkeypatch.setenv("BATCH_TIMEOUT_SECONDS", "5")
    monkeypatch.setattr(main, "settings", Settings.from_env())
    monkeypatch.setattr(
        main,
        "runner",
        BatchRunner(main.settings, target=worker_doubles.env_gated_worker),
    )
    monkeypatch.setenv("TEST_WORKER_MODE", "raise")
    resp = client.post("/normalize", json=SIMPLE_BATCH)
    assert resp.status_code == 503
    assert resp.json()["error"] == "computation_unavailable"
    assert "RuntimeError" in resp.json()["reason"]
    assert main.limiter.in_use == 0


def test_one_batch_dying_does_not_abort_another(id_dispatch_client):
    # One slot gets a crashing batch, the other a healthy one; both finish
    # independently with their own status. Dispatch is driven by record id, so
    # the two threads never race over shared configuration.
    crash_payload = {
        "records": [
            {"id": "CRASH_1", "local_time": "2024-06-01T10:00:00", "timezone": "UTC"},
        ],
        "links": [],
    }

    box: dict = {}
    t_crash = threading.Thread(
        target=lambda: box.setdefault(
            "crash", id_dispatch_client.post("/normalize", json=crash_payload)),
        daemon=True,
    )
    t_ok = threading.Thread(
        target=lambda: box.setdefault(
            "ok", id_dispatch_client.post("/normalize", json=SIMPLE_BATCH)),
        daemon=True,
    )
    t_crash.start()
    t_ok.start()
    t_crash.join(timeout=15)
    t_ok.join(timeout=15)

    assert box["crash"].status_code == 503
    assert box["ok"].status_code == 200
    assert box["ok"].json()["summary"]["total"] == 2
    assert main.limiter.in_use == 0


def test_healthz_responds_while_batches_are_computing(client, monkeypatch):
    monkeypatch.setenv("TEST_WORKER_MODE", "slow")
    monkeypatch.setenv("TEST_WORKER_DELAY", "3")

    box: dict = {}
    threads = [
        threading.Thread(target=_post_async, args=(client, SIMPLE_BATCH, box, f"t{i}"),
                         daemon=True)
        for i in range(2)
    ]
    for t in threads:
        t.start()
    deadline = time.monotonic() + 5
    while main.limiter.in_use < 2 and time.monotonic() < deadline:
        time.sleep(0.02)
    assert main.limiter.in_use == 2

    # Event loop is free: health checks keep answering and report progress.
    for _ in range(3):
        h = client.get("/healthz")
        assert h.status_code == 200
        assert h.json() == {"status": "up", "batches_in_progress": 2}
        time.sleep(0.1)

    for t in threads:
        t.join(timeout=15)
    assert client.get("/healthz").json()["batches_in_progress"] == 0


def test_invalid_request_422_does_not_consume_a_slot(client):
    assert main.limiter.in_use == 0
    resp = client.post("/normalize", json=INVALID_BATCH)
    assert resp.status_code == 422
    assert main.limiter.in_use == 0


def test_422_still_returned_while_capacity_is_saturated(client, monkeypatch):
    monkeypatch.setenv("TEST_WORKER_MODE", "slow")
    monkeypatch.setenv("TEST_WORKER_DELAY", "3")
    box: dict = {}
    threads = [
        threading.Thread(target=_post_async, args=(client, SIMPLE_BATCH, box, f"t{i}"),
                         daemon=True)
        for i in range(2)
    ]
    for t in threads:
        t.start()
    deadline = time.monotonic() + 5
    while main.limiter.in_use < 2 and time.monotonic() < deadline:
        time.sleep(0.02)

    # Valid request -> 429; malformed request still gets 422 (validation first).
    assert client.post("/normalize", json=SIMPLE_BATCH).status_code == 429
    assert client.post("/normalize", json=INVALID_BATCH).status_code == 422

    for t in threads:
        t.join(timeout=15)


def test_domain_conflict_in_worker_still_returns_409_without_partials(client):
    payload = {
        "records": [
            {"id": "A", "local_time": "2024-06-01T10:00:00", "timezone": "UTC"},
            {"id": "B", "local_time": "2024-06-01T12:00:00", "timezone": "UTC"},
        ],
        "links": [{"from_id": "A", "to_id": "B", "min_seconds": 0, "max_seconds": 3600}],
    }
    resp = client.post("/normalize", json=payload)
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"] == "no_global_solution"
    assert "records" not in body
    assert main.limiter.in_use == 0


def test_capacity_limiter_does_not_queue():
    limiter = CapacityLimiter(2)
    s0 = limiter.try_acquire()
    s1 = limiter.try_acquire()
    assert s0 is not None and s1 is not None
    assert limiter.try_acquire() is None  # immediate rejection, no blocking
    limiter.release(s0)
    s2 = limiter.try_acquire()
    assert s2 is not None
    limiter.release(s1)
    limiter.release(s2)
    assert limiter.in_use == 0


def test_runner_directly_reports_timeout_failure_and_success(monkeypatch):
    records = [solver.ScanInput("A", datetime(2024, 6, 1, 10), "UTC")]
    links: list = []

    ok_runner = BatchRunner(Settings(batch_timeout_seconds=5), target=worker_doubles.normal_worker)
    outcome = ok_runner.run(records, links)
    assert isinstance(outcome, BatchSucceeded)
    assert isinstance(outcome.outcome, solver.Ok)

    monkeypatch.setenv("TEST_WORKER_DELAY", "3")
    slow_runner = BatchRunner(Settings(batch_timeout_seconds=1), target=worker_doubles.slow_worker)
    outcome = slow_runner.run(records, links)
    assert isinstance(outcome, BatchTimedOut)
    monkeypatch.delenv("TEST_WORKER_DELAY")

    crash_runner = BatchRunner(Settings(batch_timeout_seconds=5), target=worker_doubles.crashing_worker)
    outcome = crash_runner.run(records, links)
    assert isinstance(outcome, BatchFailed)
    assert outcome.exit_code == 2
