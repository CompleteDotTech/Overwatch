"""HTTP handlers for reading and incrementally refreshing cached job logs."""

from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any

from aiohttp import web
from loguru import logger

from overwatch.constants import ACTIVE_SKY_STATUSES
from overwatch.web_state import (
    COLLECTOR_OPTIONS_KEY,
    LOG_ATTEMPT_CACHE_KEY,
    LOG_ATTEMPT_TASKS_KEY,
    LOG_REFRESH_STARTED_KEY,
    LOG_REFRESH_TASKS_KEY,
    STATE_KEY,
)

LOG_REFRESH_MIN_INTERVAL_SECONDS = 10.0


def integer_query_parameter(
    request: web.Request, name: str, *, default: int | None, minimum: int
) -> int:
    """Parse one bounded integer query parameter or return HTTP 400."""
    text = request.query.get(name)
    if text is None:
        if default is None:
            raise web.HTTPBadRequest(text=f"{name} is required")
        return default
    try:
        value = int(text)
    except ValueError as error:
        raise web.HTTPBadRequest(text=f"{name} must be an integer") from error
    if value < minimum:
        raise web.HTTPBadRequest(text=f"{name} must be at least {minimum}")
    return value


def log_cursor_query_parameter(request: web.Request, name: str) -> str | None:
    """Validate a timestamp and event-ID log cursor or return HTTP 400."""
    value = request.query.get(name)
    if value is not None and re.fullmatch(r"\d+:[0-9a-f]{24}", value) is None:
        raise web.HTTPBadRequest(text=f"{name} is not a valid log cursor")
    return value


def resource_record_for_job(
    app: web.Application, job_id: int
) -> dict[str, Any] | None:
    """Find one managed-job report record without consulting a provider."""
    report = app[STATE_KEY].report
    if report is None:
        return None
    return next(
        (
            record
            for record in report["resources"]
            if record["skypilot"].get("job_id") == job_id
        ),
        None,
    )


def ensure_cached_log_history_task(
    app: web.Application, job_id: int
) -> asyncio.Task[dict[str, Any]]:
    """Run one collector-owned full-log backfill per job at a time."""
    from overwatch.collector import collect_raw_metrics

    cache_key = str(job_id)
    task = app[LOG_ATTEMPT_TASKS_KEY].get(cache_key)
    if task is None or task.done():
        task = asyncio.create_task(
            collect_raw_metrics(
                app[COLLECTOR_OPTIONS_KEY], job_id=job_id, full_logs=True
            )
        )
        app[LOG_ATTEMPT_TASKS_KEY][cache_key] = task
    return task


def ensure_cached_log_refresh_task(
    app: web.Application, job_id: int
) -> asyncio.Task[dict[str, Any]]:
    """Start one throttled, non-blocking collector cursor refresh per job."""
    from overwatch.collector import collect_raw_metrics

    cache_key = str(job_id)
    task = app[LOG_REFRESH_TASKS_KEY].get(cache_key)
    last_started = app[LOG_REFRESH_STARTED_KEY].get(cache_key, 0.0)
    if task is None or (
        task.done()
        and perf_counter() - last_started >= LOG_REFRESH_MIN_INTERVAL_SECONDS
    ):
        task = asyncio.create_task(
            collect_raw_metrics(app[COLLECTOR_OPTIONS_KEY], job_id=job_id)
        )
        app[LOG_REFRESH_TASKS_KEY][cache_key] = task
        app[LOG_REFRESH_STARTED_KEY][cache_key] = perf_counter()
    return task


def cached_log_context(
    app: web.Application, job_id: int
) -> tuple[dict[str, Any], int]:
    """Validate CloudWatch availability and return its report context."""
    record = resource_record_for_job(app, job_id)
    if record is None:
        raise web.HTTPNotFound(text="Cached logs are unavailable for this job")
    if str(record["skypilot"].get("cloud") or "").casefold() != "aws":
        raise web.HTTPNotFound(text="CloudWatch is unavailable for this job")
    expected_attempts = (record["retries"].get("total_recoveries") or 0) + 1
    return record, expected_attempts


def cached_log_attempts(
    app: web.Application, job_id: int, expected_attempts: int
) -> list[dict[str, Any]]:
    """Reuse process metadata across requests while a history backfill grows."""
    from overwatch.cached_metrics import cached_cloudwatch_attempts

    cache_key = str(job_id)
    attempts = app[LOG_ATTEMPT_CACHE_KEY].get(cache_key)
    if attempts is None:
        attempts = cached_cloudwatch_attempts(job_id, expected_attempts)
        app[LOG_ATTEMPT_CACHE_KEY][cache_key] = attempts
    return attempts


async def handle_cached_log_attempts(request: web.Request) -> web.Response:
    """List attempts derived from raw cache files while history fills in."""
    from overwatch.cached_metrics import cached_cloudwatch_attempts
    from overwatch.raw_cache import raw_cache_path, read_json

    job_id = int(request.match_info["job_id"])
    record, expected_attempts = cached_log_context(request.app, job_id)
    cursor = read_json(
        raw_cache_path("jobs", str(job_id), "cloudwatch", "cursor.json"), {}
    )
    task = request.app[LOG_ATTEMPT_TASKS_KEY].get(str(job_id))
    if not cursor.get("complete_from_head"):
        task = ensure_cached_log_history_task(request.app, job_id)

    # Recompute only when the raw history is stable enough to update the cache.
    attempt_cache = request.app[LOG_ATTEMPT_CACHE_KEY]
    if cursor.get("backfill_in_progress") and str(job_id) in attempt_cache:
        attempts = attempt_cache[str(job_id)]
    else:
        attempts = cached_cloudwatch_attempts(job_id, expected_attempts)
        attempt_cache[str(job_id)] = attempts
    active = record["status"].get("skypilot") in ACTIVE_SKY_STATUSES
    attempt_items = [
        {
            "attempt": attempt["attempt"],
            "pid": attempt["pid"],
            "started_at": datetime.fromtimestamp(
                attempt["started_at"] / 1000, UTC
            ).isoformat(),
            "current": attempt["scan_end_at"] is None and active,
        }
        for attempt in attempts
    ]
    error = cursor.get("reason")
    if task is not None and task.done() and not task.cancelled():
        try:
            task.result()
        except Exception as task_error:  # noqa: BLE001 - collector errors vary
            error = f"{type(task_error).__name__}: {task_error}"
    return web.json_response(
        {
            "attempts": attempt_items,
            "expected_attempts": expected_attempts,
            "updated_at": cursor.get("updated_at"),
            "indexing": not cursor.get("complete_from_head") and error is None,
            "backfill": {
                "scanned_events": cursor.get("backfill_scanned_events"),
                "cached_events": cursor.get("backfill_cached_events"),
                "appended_events": cursor.get("backfill_appended_events"),
                "total_bytes": cursor.get("total_bytes"),
            },
            "error": error,
        }
    )


async def handle_cached_log_stream(request: web.Request) -> web.StreamResponse:
    """Stream cached logs while the collector refreshes in the background."""
    from overwatch.cached_metrics import cached_cloudwatch_log_page

    job_id = int(request.match_info["job_id"])
    record, expected_attempts = cached_log_context(request.app, job_id)
    attempt_number = integer_query_parameter(
        request, "attempt", default=0, minimum=0
    )
    if attempt_number:
        refresh_task = ensure_cached_log_history_task(request.app, job_id)
    else:
        refresh_task = ensure_cached_log_refresh_task(request.app, job_id)

    attempts = cached_log_attempts(request.app, job_id, expected_attempts)
    selected_attempt = next(
        (
            attempt
            for attempt in attempts
            if attempt["attempt"] == attempt_number
        ),
        None,
    )
    if attempt_number and selected_attempt is None:
        raise web.HTTPNotFound(text=f"Cached attempt {attempt_number} was not found")
    attempt_is_live = bool(
        selected_attempt
        and selected_attempt["scan_end_at"] is None
        and record["status"].get("skypilot") in ACTIVE_SKY_STATUSES
    )

    response = web.StreamResponse(
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )
    await response.prepare(request)
    started_after = (
        selected_attempt["scan_start_at"]
        if selected_attempt
        else int((datetime.now(UTC) - timedelta(minutes=2)).timestamp() * 1000)
    )
    ended_before = selected_attempt["scan_end_at"] if selected_attempt else None
    sent_event_ids: set[str] = set()
    try:
        # Send a bounded tail; the paging endpoint owns older history retrieval.
        initial_page = cached_cloudwatch_log_page(
            job_id,
            started_after=started_after,
            ended_before=ended_before,
        )
        payload = json.dumps(initial_page, separators=(",", ":"))
        await response.write(f"event: tail\ndata: {payload}\n\n".encode())
        sent_event_ids.update(item["id"] for item in initial_page["events"])
        if selected_attempt and not attempt_is_live:
            await response.write(b"event: complete\ndata: {}\n\n")
            return response

        # Live views wait only on local tasks and never block directly on AWS.
        while True:
            await asyncio.sleep(2)
            if not refresh_task.done():
                await response.write(b": cache refresh in progress\n\n")
                continue
            try:
                refresh_task.result()
            except Exception as refresh_error:  # noqa: BLE001 - SDK errors vary
                logger.warning(
                    "Background log refresh failed for job {}: {}",
                    job_id,
                    refresh_error,
                )
            page = cached_cloudwatch_log_page(
                job_id,
                started_after=started_after,
                ended_before=ended_before,
            )
            items = [
                item for item in page["events"] if item["id"] not in sent_event_ids
            ]
            if items:
                sent_event_ids.update(item["id"] for item in items)
                if len(sent_event_ids) > 5_000:
                    sent_event_ids = {item["id"] for item in page["events"]}
                payload = json.dumps(
                    {**page, "events": items}, separators=(",", ":")
                )
                await response.write(f"data: {payload}\n\n".encode())
            else:
                await response.write(b": keepalive\n\n")
            refresh_task = ensure_cached_log_refresh_task(request.app, job_id)
    except (asyncio.CancelledError, ConnectionResetError):
        pass
    return response


async def handle_cached_log_page(request: web.Request) -> web.Response:
    """Return one bounded older page after scheduling history collection."""
    from overwatch.cached_metrics import cached_cloudwatch_log_page

    job_id = int(request.match_info["job_id"])
    _record, expected_attempts = cached_log_context(request.app, job_id)
    attempt_number = integer_query_parameter(
        request, "attempt", default=None, minimum=1
    )
    before = log_cursor_query_parameter(request, "before")
    after = log_cursor_query_parameter(request, "after")
    if before is not None and after is not None:
        raise web.HTTPBadRequest(text="before and after cannot be combined")
    edge = request.query.get("edge")
    if edge not in {None, "start"}:
        raise web.HTTPBadRequest(text="edge must be start")
    ensure_cached_log_history_task(request.app, job_id)
    attempts = cached_log_attempts(request.app, job_id, expected_attempts)
    selected_attempt = next(
        (
            attempt
            for attempt in attempts
            if attempt["attempt"] == attempt_number
        ),
        None,
    )
    if selected_attempt is None:
        raise web.HTTPNotFound(text=f"Cached attempt {attempt_number} was not found")
    return web.json_response(
        cached_cloudwatch_log_page(
            job_id,
            started_after=selected_attempt["scan_start_at"],
            ended_before=selected_attempt["scan_end_at"],
            before=before,
            after=after,
            from_start=edge == "start",
        )
    )
