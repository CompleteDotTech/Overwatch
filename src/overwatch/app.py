"""Run a local monitor for cloud resources and training workloads.

SkyPilot is the authoritative cloud inventory. W&B, Zymtrace, durable storage,
and CloudWatch add training-specific context when it is available.

Run from any directory after installing the tool::

    overwatch --limit 20

The report refreshes automatically, and the service restarts when its source changes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from pathlib import Path
from time import perf_counter
from typing import Any

from aiohttp import web
from dotenv import load_dotenv
from loguru import logger
from watchfiles import DefaultFilter, run_process

from overwatch.constants import ACTIVE_SKY_STATUSES, DEFAULT_ZYMTRACE_PROJECT_ID
from overwatch.report_service import QUERY_LABELS, collect_report_from_raw_cache
from overwatch.utils import isoformat

LOG_REFRESH_MIN_INTERVAL_SECONDS = 10.0


@dataclass
class RefreshState:
    """Inspectable lifecycle state for periodic report refreshes."""

    in_progress: bool = False
    last_attempt_at: str | None = None
    last_success_at: str | None = None
    last_duration_seconds: float | None = None
    last_error: str | None = None


@dataclass
class ServiceState:
    """Mutable application state shared by the HTTP handlers."""

    startup_id: str
    report: dict[str, Any] | None
    report_error: str | None
    refresh: RefreshState
    query_diagnostics: dict[str, dict[str, Any]]


STATE_KEY = web.AppKey("state", ServiceState)
COLLECTOR_OPTIONS_KEY = web.AppKey("collector_options", object)
LOG_ATTEMPT_TASKS_KEY = web.AppKey(
    "log_attempt_tasks", dict[str, asyncio.Task[dict[str, Any]]]
)
LOG_ATTEMPT_CACHE_KEY = web.AppKey("log_attempt_cache", dict[str, list[dict[str, Any]]])
LOG_REFRESH_TASKS_KEY = web.AppKey(
    "log_refresh_tasks", dict[str, asyncio.Task[dict[str, Any]]]
)
LOG_REFRESH_STARTED_KEY = web.AppKey("log_refresh_started", dict[str, float])


def build_frontend_assets_if_sources_are_newer() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    package_json = repository_root / "package.json"
    frontend_index = Path(files("overwatch").joinpath("static", "dist", "index.html"))
    if not package_json.exists():
        if not frontend_index.exists():
            raise RuntimeError("Packaged React frontend assets are missing")
        return

    frontend_source = Path(__file__).parent / "frontend"
    frontend_inputs = [
        path
        for path in (
            *frontend_source.rglob("*"),
            package_json,
            repository_root / "package-lock.json",
            repository_root / "vite.config.ts",
            repository_root / "tsconfig.app.json",
        )
        if path.is_file()
    ]
    newest_input_mtime = max(path.stat().st_mtime for path in frontend_inputs)
    if frontend_index.exists() and frontend_index.stat().st_mtime >= newest_input_mtime:
        return

    logger.info("Building the React frontend…")
    try:
        subprocess.run(["npm", "run", "build"], cwd=repository_root, check=True)
    except subprocess.CalledProcessError:
        if not frontend_index.exists():
            raise
        logger.exception(
            "Frontend build failed; keeping the last successful build online."
        )


def configure_colored_logging() -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        colorize=True,
        format=(
            "<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | "
            "<level>{message}</level>"
        ),
        backtrace=False,
        diagnose=False,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="recent W&B training runs to inspect (default: 20)",
    )
    parser.add_argument(
        "--entity", help="W&B entity (default: authenticated user's default entity)"
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="service host (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=8765, help="service port (default: 8765)"
    )
    parser.add_argument(
        "--refresh-interval",
        type=float,
        default=60,
        help="report refresh seconds (default: 60)",
    )
    parser.add_argument(
        "--no-log-enrichment",
        action="store_true",
        help="skip CloudWatch log enrichment",
    )
    parser.add_argument(
        "--gcp-billing-table",
        help=(
            "GCP standard billing export as project.dataset.table "
            "(default: GCP_BILLING_EXPORT_TABLE)"
        ),
    )
    parser.add_argument(
        "--no-reload", action="store_true", help="disable source-code auto reload"
    )
    parser.add_argument("--zymtrace-project-id", default=DEFAULT_ZYMTRACE_PROJECT_ID)
    parser.add_argument(
        "--no-open", action="store_true", help="do not open the monitor in Chrome"
    )
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be positive")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.refresh_interval <= 0:
        parser.error("--refresh-interval must be positive")
    return args


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



async def handle_index(request: web.Request) -> web.Response:
    frontend_index = Path(files("overwatch").joinpath("static", "dist", "index.html"))
    return web.FileResponse(frontend_index)


async def handle_report_json(request: web.Request) -> web.Response:
    state = request.app[STATE_KEY]
    if state.report is None:
        return web.json_response(
            {"ready": False, "error": state.report_error}, status=202
        )
    return web.json_response(state.report)


async def handle_health(request: web.Request) -> web.Response:
    state = request.app[STATE_KEY]
    return web.json_response(
        {
            "startup_id": state.startup_id,
            "report_version": (
                state.report["generated_at"] if state.report else None
            ),
        }
    )


async def handle_query_status(request: web.Request) -> web.Response:
    state = request.app[STATE_KEY]
    queries = [
        {key: value for key, value in diagnostic.items() if key != "raw_output"}
        for diagnostic in state.query_diagnostics.values()
    ]
    return web.json_response({"refresh": asdict(state.refresh), "queries": queries})


async def handle_raw_query_output(request: web.Request) -> web.Response:
    query_key = request.match_info["query_key"]
    diagnostic = request.app[STATE_KEY].query_diagnostics.get(query_key)
    if diagnostic is None:
        raise web.HTTPNotFound(text=f"Unknown query: {query_key}")
    raw_output = diagnostic["raw_output"]
    if diagnostic.get("raw_files"):
        raw_output = []
        for raw_file in diagnostic["raw_files"]:
            path = Path(raw_file)
            try:
                value = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError) as error:
                value = {"read_error": f"{type(error).__name__}: {error}"}
            raw_output.append({"path": raw_file, "value": value})
    return web.json_response(
        {
            "key": query_key,
            "label": diagnostic["label"],
            "status": diagnostic["status"],
            "error": diagnostic["error"],
            "updated_at": diagnostic["updated_at"],
            "duration_seconds": diagnostic["duration_seconds"],
            "raw_output_updated_at": diagnostic["raw_output_updated_at"],
            "output": raw_output,
        }
    )


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
    """Start one non-blocking collector cursor refresh per job."""
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


async def handle_cached_log_attempts(request: web.Request) -> web.Response:
    """List attempts derived from raw cache files while history fills in."""
    from overwatch.cached_metrics import cached_cloudwatch_attempts
    from overwatch.raw_cache import raw_cache_path, read_json

    job_id = int(request.match_info["job_id"])
    record = resource_record_for_job(request.app, job_id)
    if record is None:
        raise web.HTTPNotFound(text="Cached logs are unavailable for this job")
    if str(record["skypilot"].get("cloud") or "").casefold() != "aws":
        raise web.HTTPNotFound(text="CloudWatch is unavailable for this job")
    expected_attempts = (record["retries"].get("total_recoveries") or 0) + 1
    cursor = read_json(
        raw_cache_path("jobs", str(job_id), "cloudwatch", "cursor.json"), {}
    )
    task = request.app[LOG_ATTEMPT_TASKS_KEY].get(str(job_id))
    if not cursor.get("complete_from_head"):
        task = ensure_cached_log_history_task(request.app, job_id)

    # Reuse attempt metadata while a large raw history file is actively growing.
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
        except Exception as task_error:  # noqa: BLE001 - collector reports SDK errors
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
    """Stream cached logs immediately while the collector refreshes in the background."""
    from overwatch.cached_metrics import (
        cached_cloudwatch_attempts,
        cached_cloudwatch_log_page,
    )
    job_id = int(request.match_info["job_id"])
    record = resource_record_for_job(request.app, job_id)
    if record is None:
        raise web.HTTPNotFound(text="Cached logs are unavailable for this job")
    if str(record["skypilot"].get("cloud") or "").casefold() != "aws":
        raise web.HTTPNotFound(text="CloudWatch is unavailable for this job")
    attempt_number = integer_query_parameter(
        request, "attempt", default=0, minimum=0
    )
    if attempt_number:
        refresh_task = ensure_cached_log_history_task(request.app, job_id)
    else:
        refresh_task = ensure_cached_log_refresh_task(request.app, job_id)

    expected_attempts = (record["retries"].get("total_recoveries") or 0) + 1
    attempts = request.app[LOG_ATTEMPT_CACHE_KEY].get(str(job_id))
    if attempts is None:
        attempts = cached_cloudwatch_attempts(job_id, expected_attempts)
        request.app[LOG_ATTEMPT_CACHE_KEY][str(job_id)] = attempts
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
    pid = None
    started_after = (
        selected_attempt["scan_start_at"]
        if selected_attempt
        else int((datetime.now(UTC) - timedelta(minutes=2)).timestamp() * 1000)
    )
    ended_before = selected_attempt["scan_end_at"] if selected_attempt else None
    sent_event_ids: set[str] = set()
    try:
        # Send only a bounded tail page; older history is fetched explicitly by cursor.
        initial_page = cached_cloudwatch_log_page(
            job_id,
            pid=pid,
            started_after=started_after,
            ended_before=ended_before,
        )
        initial_items = initial_page["events"]
        payload = json.dumps(initial_page, separators=(",", ":"))
        await response.write(f"event: tail\ndata: {payload}\n\n".encode())
        sent_event_ids.update(item["id"] for item in initial_items)
        if selected_attempt and not attempt_is_live:
            await response.write(b"event: complete\ndata: {}\n\n")
            return response

        # Live views never wait on AWS; completed background refreshes expose new local lines.
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
                pid=pid,
                started_after=started_after,
                ended_before=ended_before,
            )
            items = [
                item
                for item in page["events"]
                if item["id"] not in sent_event_ids
            ]
            if items:
                sent_event_ids.update(item["id"] for item in items)
                if len(sent_event_ids) > 5_000:
                    sent_event_ids = {
                        item["id"] for item in page["events"]
                    }
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
    """Return one bounded older page from the raw cache after scheduling collection."""
    from overwatch.cached_metrics import (
        cached_cloudwatch_attempts,
        cached_cloudwatch_log_page,
    )

    job_id = int(request.match_info["job_id"])
    record = resource_record_for_job(request.app, job_id)
    if record is None:
        raise web.HTTPNotFound(text="Cached logs are unavailable for this job")
    if str(record["skypilot"].get("cloud") or "").casefold() != "aws":
        raise web.HTTPNotFound(text="CloudWatch is unavailable for this job")
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
    expected_attempts = (record["retries"].get("total_recoveries") or 0) + 1
    attempts = request.app[LOG_ATTEMPT_CACHE_KEY].get(str(job_id))
    if attempts is None:
        attempts = cached_cloudwatch_attempts(job_id, expected_attempts)
        request.app[LOG_ATTEMPT_CACHE_KEY][str(job_id)] = attempts
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


async def update_report_state(
    app: web.Application, args: argparse.Namespace, *, log_progress: bool = False
) -> dict[str, Any]:
    """Run one refresh while preserving inspectable success and failure state."""
    state = app[STATE_KEY]
    refresh = state.refresh
    refresh_started = perf_counter()
    refresh.in_progress = True
    refresh.last_attempt_at = isoformat(datetime.now(UTC))
    try:
        report = await collect_report_from_raw_cache(
            args,
            log_progress=log_progress,
            query_diagnostics=state.query_diagnostics,
        )
    except Exception as error:
        error_message = f"{type(error).__name__}: {error}"
        state.report_error = error_message
        refresh.last_error = error_message
        raise
    else:
        state.report = report
        state.report_error = None
        refresh.last_success_at = report["generated_at"]
        refresh.last_error = None
        return report
    finally:
        refresh.in_progress = False
        refresh.last_duration_seconds = round(perf_counter() - refresh_started, 2)


async def refresh_report_forever(
    app: web.Application, args: argparse.Namespace
) -> None:
    while True:
        await asyncio.sleep(args.refresh_interval)
        try:
            await update_report_state(app, args)
        except Exception as error:  # noqa: BLE001
            logger.error(
                "Report refresh failed; retrying in {:g}s: {}",
                args.refresh_interval,
                error,
            )


async def run_service(args: argparse.Namespace) -> int:
    from overwatch.collector import CollectorOptions

    service_started = perf_counter()
    url = f"http://{args.host}:{args.port}"
    if uv_env_file := os.environ.get("UV_ENV_FILE"):
        load_dotenv(uv_env_file)
    await asyncio.to_thread(build_frontend_assets_if_sources_are_newer)
    refresh_state = RefreshState()
    query_diagnostics = {
        key: {
            "key": key,
            "label": label,
            "status": "pending",
            "summary": "Waiting for first refresh",
            "updated_at": None,
            "duration_seconds": None,
            "error": None,
            "raw_output_updated_at": None,
            "raw_output": None,
        }
        for key, label in QUERY_LABELS.items()
    }

    app = web.Application()
    app[COLLECTOR_OPTIONS_KEY] = CollectorOptions(
        limit=args.limit,
        entity=args.entity,
        gcp_billing_table=(
            getattr(args, "gcp_billing_table", None)
            or os.environ.get("GCP_BILLING_EXPORT_TABLE")
        ),
    )
    app[STATE_KEY] = ServiceState(
        startup_id=uuid.uuid4().hex,
        report=None,
        report_error=None,
        refresh=refresh_state,
        query_diagnostics=query_diagnostics,
    )
    app[LOG_ATTEMPT_TASKS_KEY] = {}
    app[LOG_ATTEMPT_CACHE_KEY] = {}
    app[LOG_REFRESH_TASKS_KEY] = {}
    app[LOG_REFRESH_STARTED_KEY] = {}
    from overwatch.raw_cache import raw_cache_path

    # Build the first screen immediately from raw files before refreshing providers.
    if raw_cache_path("global", "sky", "jobs.json").exists():
        try:
            app[STATE_KEY].report = await collect_report_from_raw_cache(
                args,
                query_diagnostics=query_diagnostics,
                refresh_cache=False,
            )
        except (OSError, TypeError, ValueError) as error:
            logger.warning("Could not render the existing raw metric cache: {}", error)
    if app[STATE_KEY].report is not None:
        logger.info(
            "Restored {} cached resources; refreshing in the background.",
            len(app[STATE_KEY].report["resources"]),
        )
    app.add_routes(
        [
            web.get("/", handle_index),
            web.get("/runs/{job_id:\\d+}", handle_index),
            web.get("/billing", handle_index),
            web.get("/cost-waste", handle_index),
            web.get("/api/report", handle_report_json),
            web.get("/api/health", handle_health),
            web.get("/api/status", handle_query_status),
            web.get("/api/status/{query_key}/raw", handle_raw_query_output),
            web.get(
                "/api/logs/{job_id:\\d+}/attempts", handle_cached_log_attempts
            ),
            web.get("/api/logs/{job_id:\\d+}/page", handle_cached_log_page),
            web.get("/api/logs/{job_id:\\d+}", handle_cached_log_stream),
            web.static("/static", Path(files("overwatch").joinpath("static"))),
        ]
    )
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, args.host, args.port).start()
    logger.success(
        "Overwatch UI is ready at {} ({:.1f}s); loading cloud inventory…",
        url,
        perf_counter() - service_started,
    )
    logger.info(
        "Auto-reload is enabled." if not args.no_reload else "Auto-reload is disabled."
    )
    logger.info("Press Ctrl-C to stop it.")

    # Open Chrome once; a source reload keeps the existing browser tab alive.
    was_restarted = os.environ.get("WATCHFILES_CHANGES", "[]") != "[]"
    if not args.no_open and not was_restarted:
        try:
            await asyncio.to_thread(
                subprocess.run, ["open", "-a", "Google Chrome", url], check=True
            )
        except (OSError, subprocess.CalledProcessError) as error:
            logger.warning("Could not open Chrome ({}); open {} manually.", error, url)

    background_tasks: list[asyncio.Task[Any]] = []
    try:
        try:
            report = await update_report_state(app, args, log_progress=True)
            logger.success(
                "Cloud inventory is ready with {} resources ({:.1f}s).",
                len(report["resources"]),
                perf_counter() - service_started,
            )
        except Exception as error:  # noqa: BLE001
            logger.error(
                "Initial report failed; background refresh will retry: {}", error
            )
        background_tasks.append(asyncio.create_task(refresh_report_forever(app, args)))
        await asyncio.Event().wait()
    finally:
        service_tasks = {
            *background_tasks,
            *app[LOG_ATTEMPT_TASKS_KEY].values(),
            *app[LOG_REFRESH_TASKS_KEY].values(),
        }
        for task in service_tasks:
            task.cancel()
        await asyncio.gather(*service_tasks, return_exceptions=True)
        await runner.cleanup()
    return 0


def run_service_worker(args: argparse.Namespace) -> None:
    """Run one supervised service generation."""
    configure_colored_logging()
    try:
        asyncio.run(run_service(args))
    except KeyboardInterrupt:
        pass


def log_detected_source_changes(changes: set[tuple[Any, str]]) -> None:
    logger.info("Detected {} source change(s); relaunching Overwatch…", len(changes))


def main() -> int:
    configure_colored_logging()
    args = parse_args()
    if args.no_reload:
        try:
            return asyncio.run(run_service(args))
        except KeyboardInterrupt:
            return 0

    watch_root = Path(__file__).parent
    logger.info("Auto-reload supervisor is watching {}.", watch_root)
    try:
        run_process(
            watch_root,
            target=run_service_worker,
            args=(args,),
            target_type="function",
            callback=log_detected_source_changes,
            watch_filter=DefaultFilter(ignore_paths=(watch_root / "static",)),
        )
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
