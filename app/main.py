"""FastAPI entrypoint for the scan-time normalization service."""

from __future__ import annotations

import asyncio
from typing import Annotated

from fastapi import Body, FastAPI
from fastapi.responses import JSONResponse

from . import solver
from .limits import CapacityLimiter
from .runner import BatchFailed, BatchRunner, BatchSucceeded, BatchTimedOut
from .schemas import (
    CapacityErrorResponse,
    ErrorResponse,
    InvalidRecordInfo,
    LinkInfo,
    NormalizeRequest,
    NormalizeResponse,
    RecordOut,
    Summary,
    TimeoutErrorResponse,
    UnavailableErrorResponse,
)
from .settings import Settings

DESCRIPTION = """
Normalize carrier-exported local scan times to UTC under IANA timezone rules.

* Each record's local time yields **0, 1 or 2 legal UTC candidates**
  (DST fall-back duplicates a local time; spring-forward makes one nonexistent).
* Transfer links constrain the UTC duration between two records to a closed
  interval; **all links must hold simultaneously** and links may share records.
* The service computes, per record, **every candidate that appears in at least
  one globally feasible interpretation**:
  * exactly one → `determined` (确定), returned as `utc`;
  * several → `needs_review` (待核对), all candidates returned, none picked.
* If any record has no legal candidate, or the links are mutually inconsistent,
  the request fails with **HTTP 409** and a precise reason — no partial
  conversion is ever returned.

## Concurrency, isolation and capacity

* Every validated batch is computed in its own **short-lived OS process**; the
  API process only supervises it, so one batch crashing or timing out cannot
  abort another batch.
* A single API process computes at most **2 batches concurrently**
  (`MAX_CONCURRENT_BATCHES`). When every slot is busy, a validated request gets
  **HTTP 429 immediately — it is never queued**.
* Each batch has a configurable wall-clock deadline (default **10 s**,
  `BATCH_TIMEOUT_SECONDS`). On expiry the worker is terminated and reaped and
  the request fails with **HTTP 504**; late results are discarded, never
  returned as success.
* If the computation process exits abnormally, the request fails with
  **HTTP 503**. A slot is released only after the worker process has actually
  ended and been reaped.
* `/healthz` is served by the event loop and keeps responding while batches
  are being computed.
"""

DEMO_DST_FALLBACK = {
    "records": [
        {"id": "ARR_JFK", "local_time": "2024-11-03T01:30:00", "timezone": "America/New_York"},
        {"id": "DEP_JFK", "local_time": "2024-11-03T02:45:00", "timezone": "America/New_York"},
        {"id": "ARR_ORD", "local_time": "2024-11-03T04:10:00", "timezone": "America/Chicago"},
        {"id": "DEP_LAX", "local_time": "2024-11-03T03:20:00", "timezone": "America/Los_Angeles"},
        {"id": "XFR_ORD", "local_time": "2024-11-03T01:30:00", "timezone": "America/Chicago"},
    ],
    "links": [
        {"from_id": "ARR_JFK", "to_id": "DEP_JFK", "min_seconds": 3600, "max_seconds": 7200},
        {"from_id": "DEP_JFK", "to_id": "ARR_ORD", "min_seconds": 7200, "max_seconds": 10800},
        {"from_id": "ARR_ORD", "to_id": "DEP_LAX", "min_seconds": 1800, "max_seconds": 7200},
        {"from_id": "ARR_JFK", "to_id": "XFR_ORD", "min_seconds": 0, "max_seconds": 86400},
    ],
}

DEMO_IMAGINARY_TIME = {
    "records": [
        {"id": "GAP_1", "local_time": "2024-03-10T02:30:00", "timezone": "America/New_York"},
    ],
    "links": [],
}

DEMO_CONFLICTING_LINKS = {
    "records": [
        {"id": "A", "local_time": "2024-06-01T10:00:00", "timezone": "UTC"},
        {"id": "B", "local_time": "2024-06-01T12:00:00", "timezone": "UTC"},
    ],
    "links": [
        {"from_id": "A", "to_id": "B", "min_seconds": 0, "max_seconds": 3600},
    ],
}


def _demo_slow_payload() -> dict:
    """Batch containing the configured slow-record id (drives 429/504 demos)."""
    slow_id = settings.slow_batch_id or "SLOW"
    payload = {
        "records": [
            {"id": slow_id, "local_time": "2024-06-01T10:00:00", "timezone": "UTC"},
        ],
        "links": [],
    }
    return payload


def _demo_crash_payload() -> dict:
    """Batch containing the configured crash-record id (drives the 503 demo)."""
    crash_id = settings.crash_batch_id or "CRASH"
    return {
        "records": [
            {"id": crash_id, "local_time": "2024-06-01T10:00:00", "timezone": "UTC"},
        ],
        "links": [],
    }


settings = Settings.from_env()
limiter = CapacityLimiter(settings.max_concurrent_batches)
runner = BatchRunner(settings)

app = FastAPI(
    title="Scan Time Normalizer",
    description=DESCRIPTION,
    version="2.0.0",
)


@app.get("/", include_in_schema=False)
def root() -> dict:
    return {
        "service": "scan-time-normalizer",
        "docs": "/docs",
        "normalize": "POST /normalize",
        "health": "/healthz",
        "max_concurrent_batches": settings.max_concurrent_batches,
        "batch_timeout_seconds": settings.batch_timeout_seconds,
    }


@app.get("/healthz", tags=["meta"])
def healthz() -> dict:
    # Served straight from the event loop; worker processes and the thread
    # running the supervisor never block it.
    return {"status": "up", "batches_in_progress": limiter.in_use}


@app.post(
    "/normalize",
    response_model=NormalizeResponse,
    responses={
        409: {"model": ErrorResponse, "description": "No legal candidate or no global solution"},
        429: {
            "model": CapacityErrorResponse,
            "description": "All computation slots busy; request rejected without queueing",
        },
        504: {
            "model": TimeoutErrorResponse,
            "description": "Batch exceeded its deadline; the worker was terminated and reaped",
        },
        503: {
            "model": UnavailableErrorResponse,
            "description": "Computation process exited abnormally",
        },
    },
    tags=["normalize"],
)
async def normalize_endpoint(
    req: Annotated[
        NormalizeRequest,
        Body(
            openapi_examples={
                "dst_fallback_resolved": {
                    "summary": "DST fall-back: constraints disambiguate, one record stays 待核对",
                    "description": "2024-11-03 US fall-back. ARR_JFK 01:30 New York is ambiguous "
                                   "(05:30Z/06:30Z); the 1-2h transfer window to DEP_JFK forces "
                                   "06:30Z. XFR_ORD 01:30 Chicago keeps both candidates.",
                    "value": DEMO_DST_FALLBACK,
                },
                "imaginary_local_time": {
                    "summary": "Spring-forward gap: local time does not exist → 409",
                    "description": "2024-03-10 02:30 never happened in America/New_York, so the "
                                   "record has no legal UTC candidate.",
                    "value": DEMO_IMAGINARY_TIME,
                },
                "conflicting_links": {
                    "summary": "Mutually inconsistent links → 409 with irreducible conflict",
                    "description": "A→B is fixed at 7200s but the link allows at most 3600s.",
                    "value": DEMO_CONFLICTING_LINKS,
                },
                "slow_batch_timeout_or_overload": {
                    "summary": "Slow batch → 504 when alone; two of these make a third call return 429",
                    "description": "Contains the SLOW_BATCH_ID record so the isolated worker keeps "
                                   "computing past BATCH_TIMEOUT_SECONDS. Fire two of these, then a "
                                   "third request: the third is rejected with 429 immediately; the "
                                   "slow ones finish with 504 and their slots are reclaimed.",
                    "value": _demo_slow_payload(),
                },
                "crashing_batch_503": {
                    "summary": "Worker process exits hard → 503, slot is still reclaimed",
                    "description": "Contains the CRASH_BATCH_ID record; the computation process "
                                   "terminates with a non-zero exit code and the API answers 503.",
                    "value": _demo_crash_payload(),
                },
            }
        ),
    ],
) -> NormalizeResponse | JSONResponse:
    # Validation (including 422) has already succeeded by the time we run.
    slot = limiter.try_acquire()
    if slot is None:
        body = CapacityErrorResponse(
            message=(
                f"At most {settings.max_concurrent_batches} batch(es) may be computed "
                "concurrently per API process; all slots are busy. The request was not "
                "queued — retry once a batch has finished."
            ),
            max_concurrent_batches=settings.max_concurrent_batches,
        )
        return JSONResponse(status_code=429, content=body.model_dump(mode="json"))

    records = [solver.ScanInput(r.id, r.local_time, r.timezone) for r in req.records]
    links = [
        solver.LinkInput(l.from_id, l.to_id, l.min_seconds, l.max_seconds)
        for l in req.links
    ]

    try:
        # The blocking process supervision runs in a worker thread so the
        # event loop stays free for /healthz and other incoming requests.
        outcome = await asyncio.to_thread(runner.run, records, links)
    finally:
        # Reached only after runner.run() has reaped the worker process, so
        # the freed slot always corresponds to fully recycled OS resources.
        limiter.release(slot)

    if isinstance(outcome, BatchTimedOut):
        body = TimeoutErrorResponse(
            message=(
                f"The batch did not finish within the {outcome.timeout_seconds:g} s "
                "deadline. Its computation process was terminated and reaped; any "
                "late result was discarded and no partial conversion is returned."
            ),
            timeout_seconds=outcome.timeout_seconds,
        )
        return JSONResponse(status_code=504, content=body.model_dump(mode="json"))

    if isinstance(outcome, BatchFailed):
        body = UnavailableErrorResponse(
            message=(
                "The isolated computation process exited abnormally. No partial "
                "conversion is returned; the process was reaped and its capacity "
                "slot released."
            ),
            reason=outcome.reason,
            exit_code=outcome.exit_code,
        )
        return JSONResponse(status_code=503, content=body.model_dump(mode="json"))

    assert isinstance(outcome, BatchSucceeded)
    result = outcome.outcome

    if isinstance(result, solver.NoLegalCandidate):
        body = ErrorResponse(
            error="no_legal_candidate",
            message="Local time has no legal UTC reading under its timezone rules "
                    "(it falls into a DST spring-forward gap). No partial conversion returned.",
            invalid_records=[
                InvalidRecordInfo(
                    id=r.id,
                    local_time=r.local_time,
                    timezone=r.timezone,
                    reason="local time does not exist in this timezone",
                )
                for r in result.records
            ],
        )
        return JSONResponse(status_code=409, content=body.model_dump(mode="json"))

    if isinstance(result, solver.NoGlobalSolution):
        body = ErrorResponse(
            error="no_global_solution",
            message="Transfer links are mutually inconsistent: no assignment of UTC "
                    "candidates satisfies every interval simultaneously. The listed "
                    "links form an irreducible conflict. No partial conversion returned.",
            conflicting_links=[
                LinkInfo(
                    index=i,
                    from_id=req.links[i].from_id,
                    to_id=req.links[i].to_id,
                    min_seconds=req.links[i].min_seconds,
                    max_seconds=req.links[i].max_seconds,
                )
                for i in result.link_indices
            ],
            involved_records=result.record_ids,
        )
        return JSONResponse(status_code=409, content=body.model_dump(mode="json"))

    by_id = {r.id: r for r in req.records}
    records_out = [
        RecordOut(
            id=res.id,
            timezone=by_id[res.id].timezone,
            local_time=by_id[res.id].local_time,
            status=res.status,
            utc=res.candidates[0] if res.status == "determined" else None,
            candidates=res.candidates,
        )
        for res in result.results
    ]
    return NormalizeResponse(
        summary=Summary(
            total=len(records_out),
            determined=sum(1 for r in records_out if r.status == "determined"),
            needs_review=sum(1 for r in records_out if r.status == "needs_review"),
        ),
        records=records_out,
    )
