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
import subprocess
import sys
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from time import perf_counter
from typing import Any

from aiohttp import web
from dotenv import load_dotenv
from loguru import logger
from watchfiles import DefaultFilter, run_process

from overwatch.constants import DEFAULT_ZYMTRACE_PROJECT_ID
from overwatch.log_api import (
    handle_cached_log_attempts,
    handle_cached_log_page,
    handle_cached_log_stream,
)
from overwatch.report_service import QUERY_LABELS, collect_report_from_raw_cache
from overwatch.utils import isoformat
from overwatch.web_state import (
    COLLECTOR_OPTIONS_KEY,
    LOG_ATTEMPT_CACHE_KEY,
    LOG_ATTEMPT_TASKS_KEY,
    LOG_REFRESH_STARTED_KEY,
    LOG_REFRESH_TASKS_KEY,
    STATE_KEY,
    RefreshState,
    ServiceState,
)

REPORT_ARGUMENTS_KEY = web.AppKey("report_arguments", argparse.Namespace)
REPORT_REFRESH_TASK_KEY = web.AppKey(
    "report_refresh_task", asyncio.Task[None] | None
)


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


async def handle_report_refresh(request: web.Request) -> web.Response:
    """Schedule one report collection pass unless a refresh is already active."""
    state = request.app[STATE_KEY]
    task = request.app[REPORT_REFRESH_TASK_KEY]
    started = not state.refresh.in_progress and (task is None or task.done())
    if started:

        async def refresh_in_background() -> None:
            try:
                await update_report_state(
                    request.app, request.app[REPORT_ARGUMENTS_KEY], log_progress=True
                )
            except Exception as error:  # noqa: BLE001 - status exposes provider errors
                logger.error("Manual report refresh failed: {}", error)

        request.app[REPORT_REFRESH_TASK_KEY] = asyncio.create_task(
            refresh_in_background()
        )
    return web.json_response(
        {"started": started, "in_progress": True},
        status=202,
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
    app[REPORT_ARGUMENTS_KEY] = args
    app[REPORT_REFRESH_TASK_KEY] = None
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
            web.post("/api/refresh", handle_report_refresh),
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
        if report_refresh_task := app[REPORT_REFRESH_TASK_KEY]:
            service_tasks.add(report_refresh_task)
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
