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
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from pathlib import Path
from time import perf_counter
from typing import Any

from aiohttp import web
from dotenv import load_dotenv
from loguru import logger
from watchfiles import DefaultFilter, run_process

from overwatch.constants import (
    ACTIVE_RESOURCE_STATUSES,
    ACTIVE_SKY_STATUSES,
    DEFAULT_ZYMTRACE_PROJECT_ID,
)
from overwatch.providers.wandb_flow import match_skypilot_jobs
from overwatch.report import (
    build_cluster_record,
    build_run_record,
    build_sky_only_record,
)
from overwatch.utils import enum_value, isoformat
from overwatch.view import config_differences

QUERY_LABELS = {
    "sky_jobs": "SkyPilot managed jobs",
    "sky_clusters": "SkyPilot clusters",
    "billing": "Cloud billing",
    "wandb": "W&B training runs",
    "cloudwatch": "CloudWatch progress",
}


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


async def collect_report_from_raw_cache(
    args: argparse.Namespace,
    *,
    log_progress: bool = False,
    query_diagnostics: dict[str, dict[str, Any]] | None = None,
    refresh_cache: bool = True,
) -> dict[str, Any]:
    """Refresh the standalone collector, then build a report only from cache files."""
    from overwatch.cached_metrics import (
        cached_billing_report,
        cached_cloudwatch_retry_breakdown,
        cached_cloudwatch_telemetry,
        cached_sky_clusters,
        cached_sky_jobs,
        cached_train_config,
        cached_wandb_runs,
        raw_cache_manifest,
    )
    from overwatch.collector import CollectorOptions, collect_raw_metrics
    from overwatch.raw_cache import raw_cache_path, read_json

    options = CollectorOptions(
        limit=args.limit,
        entity=args.entity,
        gcp_billing_table=(
            getattr(args, "gcp_billing_table", None)
            or os.environ.get("GCP_BILLING_EXPORT_TABLE")
        ),
    )
    if log_progress:
        logger.info("Refreshing the versioned raw metric cache…")
    if refresh_cache:
        await collect_raw_metrics(options)
    manifest = raw_cache_manifest()

    # Load every presentation input from files written by the collector.
    jobs = cached_sky_jobs()
    clusters = cached_sky_clusters()
    runs = cached_wandb_runs()
    billing = cached_billing_report(manifest)
    matched_jobs = match_skypilot_jobs(runs, jobs)
    matched_jobs_by_id = {job.job_id: job for job in matched_jobs if job is not None}
    jobs_for_progress = (
        {}
        if args.no_log_enrichment
        else {
            job.job_id: job
            for job in jobs
            if job.job_id in matched_jobs_by_id
            or enum_value(job.status) in ACTIVE_SKY_STATUSES
        }
    )
    log_results = {
        job_id: cached_cloudwatch_telemetry(job_id) for job_id in jobs_for_progress
    }
    retry_breakdowns = {
        job.job_id: cached_cloudwatch_retry_breakdown(
            job.job_id, job.recovery_count
        )
        for job in jobs
    }

    # Query diagnostics reference the raw files instead of duplicating SDK dumps in memory.
    if query_diagnostics is not None:
        source_mapping = {
            "sky_jobs": ("sky_jobs", [raw_cache_path("global", "sky", "jobs.json")]),
            "sky_clusters": (
                "sky_clusters",
                [raw_cache_path("global", "sky", "clusters.json")],
            ),
            "billing": (
                "aws_billing",
                [
                    raw_cache_path("global", "billing", "aws.json"),
                    raw_cache_path("global", "billing", "gcp.json"),
                ],
            ),
            "wandb": ("wandb", [raw_cache_path("global", "wandb", "runs.json")]),
            "cloudwatch": (
                "cloudwatch",
                [raw_cache_path("jobs", str(job_id), "cloudwatch", "cursor.json") for job_id in jobs_for_progress],
            ),
        }
        for query_key, (source_key, paths) in source_mapping.items():
            source = manifest.get("sources", {}).get(source_key, {})
            error = source.get("error")
            existing_paths = [str(path) for path in paths if path.exists()]
            query_diagnostics[query_key] = {
                "key": query_key,
                "label": QUERY_LABELS[query_key],
                "status": "error" if error else "ok",
                "summary": error or f"Cached in {len(existing_paths)} raw file(s)",
                "updated_at": manifest.get("updated_at"),
                "duration_seconds": source.get("duration_seconds"),
                "error": error,
                "raw_output_updated_at": manifest.get("updated_at"),
                "raw_output": None,
                "raw_files": existing_paths,
            }

    # Match cached scheduler and W&B records, then derive the UI report locally.
    matched_runs_by_job_id = {
        job.job_id: (run, dict(run.config))
        for run, job in zip(runs, matched_jobs)
        if job is not None
    }
    records = []
    matched_records = []
    matched_configs = []
    for job in jobs:
        job_log_progress, progress_error, training_references = log_results.get(
            job.job_id, (None, "CloudWatch cache was not requested", {})
        )
        if matched_run := matched_runs_by_job_id.get(job.job_id):
            run, config = matched_run
            record = build_run_record(
                run,
                job,
                config,
                job_log_progress,
                progress_error,
                args.zymtrace_project_id,
                retry_breakdowns[job.job_id],
            )
            matched_records.append(record)
            matched_configs.append(config)
        else:
            record = build_sky_only_record(
                job,
                job_log_progress,
                progress_error,
                args.zymtrace_project_id,
                retry_breakdowns[job.job_id],
                training_references,
                cached_train_config(job.job_id),
            )
        job_directory = raw_cache_path("jobs", str(job.job_id))
        expected_cache_files = ["sky.json", "wandb.json", "train_config.yaml"]
        if str(job.cloud).casefold() == "aws" and record["skypilot"]["cluster_name"]:
            expected_cache_files.extend(
                (
                    "cloudwatch/cursor.json",
                    "cloudwatch/events.jsonl.zst",
                    "cloudwatch/events.index.json",
                    "cloudwatch/events.log",
                )
            )
        train_config_error = read_json(
            job_directory / "train_config.error.json", {}
        )
        cache_errors = {}
        if train_config_error.get("error"):
            cache_errors["train_config.yaml"] = train_config_error["error"]
        record["cache"] = {
            "missing_files": [
                relative_path
                for relative_path in expected_cache_files
                if not (job_directory / relative_path).exists()
            ],
            "errors": cache_errors,
        }
        records.append(record)
    records.extend(
        build_cluster_record(cluster, args.zymtrace_project_id) for cluster in clusters
    )
    records.sort(
        key=lambda record: (
            record["status"]["skypilot"] in ACTIVE_RESOURCE_STATUSES,
            record["submitted_at"] or "",
        ),
        reverse=True,
    )
    return {
        "generated_at": manifest.get("updated_at") or isoformat(datetime.now(UTC)),
        "wandb_entity": runs[0].entity if runs else args.entity,
        "requested_limit": args.limit,
        "selection": "raw file cache",
        "log_enrichment": not args.no_log_enrichment,
        "billing": billing,
        "resources": records,
        "config_differences": config_differences(matched_records, matched_configs),
        "warnings": billing["warnings"],
        "raw_cache_root": manifest.get("cache_root"),
    }


async def handle_index(request: web.Request) -> web.Response:
    frontend_index = Path(files("overwatch").joinpath("static", "dist", "index.html"))
    return web.FileResponse(frontend_index)


async def handle_report_json(request: web.Request) -> web.Response:
    state = request.app["state"]
    if state["report"] is None:
        return web.json_response(
            {"ready": False, "error": state["report_error"]}, status=202
        )
    return web.json_response(state["report"])


async def handle_health(request: web.Request) -> web.Response:
    state = request.app["state"]
    return web.json_response(
        {
            "startup_id": state["startup_id"],
            "report_version": (
                state["report"]["generated_at"] if state["report"] else None
            ),
        }
    )


async def handle_query_status(request: web.Request) -> web.Response:
    state = request.app["state"]
    queries = [
        {key: value for key, value in diagnostic.items() if key != "raw_output"}
        for diagnostic in state["query_diagnostics"].values()
    ]
    return web.json_response({"refresh": state["refresh"], "queries": queries})


async def handle_raw_query_output(request: web.Request) -> web.Response:
    query_key = request.match_info["query_key"]
    diagnostic = request.app["state"]["query_diagnostics"].get(query_key)
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
    report = app["state"]["report"]
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
    task = app["log_attempt_tasks"].get(cache_key)
    if task is None or task.done():
        task = asyncio.create_task(
            collect_raw_metrics(
                app["collector_options"], job_id=job_id, full_logs=True
            )
        )
        app["log_attempt_tasks"][cache_key] = task
    return task


def ensure_cached_log_refresh_task(
    app: web.Application, job_id: int
) -> asyncio.Task[dict[str, Any]]:
    """Start one non-blocking collector cursor refresh per job."""
    from overwatch.collector import collect_raw_metrics

    cache_key = str(job_id)
    task = app["log_refresh_tasks"].get(cache_key)
    if task is None or task.done():
        task = asyncio.create_task(
            collect_raw_metrics(app["collector_options"], job_id=job_id)
        )
        app["log_refresh_tasks"][cache_key] = task
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
    task = request.app["log_attempt_tasks"].get(str(job_id))
    if not cursor.get("complete_from_head"):
        task = ensure_cached_log_history_task(request.app, job_id)

    # Reuse attempt metadata while a large raw history file is actively growing.
    attempt_cache = request.app["log_attempt_cache"]
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
    attempt_number = int(request.query.get("attempt", "0"))
    if attempt_number:
        refresh_task = ensure_cached_log_history_task(request.app, job_id)
    else:
        refresh_task = ensure_cached_log_refresh_task(request.app, job_id)

    expected_attempts = (record["retries"].get("total_recoveries") or 0) + 1
    attempts = request.app["log_attempt_cache"].get(str(job_id))
    if attempts is None:
        attempts = cached_cloudwatch_attempts(job_id, expected_attempts)
        request.app["log_attempt_cache"][str(job_id)] = attempts
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
    ensure_cached_log_history_task(request.app, job_id)

    attempt_number = int(request.query.get("attempt", "0"))
    expected_attempts = (record["retries"].get("total_recoveries") or 0) + 1
    attempts = request.app["log_attempt_cache"].get(str(job_id))
    if attempts is None:
        attempts = cached_cloudwatch_attempts(job_id, expected_attempts)
        request.app["log_attempt_cache"][str(job_id)] = attempts
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
            before=request.query.get("before"),
            after=request.query.get("after"),
            from_start=request.query.get("edge") == "start",
        )
    )


async def update_report_state(
    app: web.Application, args: argparse.Namespace, *, log_progress: bool = False
) -> dict[str, Any]:
    """Run one refresh while preserving inspectable success and failure state."""
    state = app["state"]
    refresh = state["refresh"]
    refresh_started = perf_counter()
    refresh["in_progress"] = True
    refresh["last_attempt_at"] = isoformat(datetime.now(UTC))
    try:
        report = await collect_report_from_raw_cache(
            args,
            log_progress=log_progress,
            query_diagnostics=state["query_diagnostics"],
        )
    except Exception as error:
        error_message = f"{type(error).__name__}: {error}"
        state["report_error"] = error_message
        refresh["last_error"] = error_message
        raise
    else:
        state["report"] = report
        state["report_error"] = None
        refresh["last_success_at"] = report["generated_at"]
        refresh["last_error"] = None
        return report
    finally:
        refresh["in_progress"] = False
        refresh["last_duration_seconds"] = round(perf_counter() - refresh_started, 2)


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
    refresh_state = {
        "in_progress": False,
        "last_attempt_at": None,
        "last_success_at": None,
        "last_duration_seconds": None,
        "last_error": None,
    }
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
    app["collector_options"] = CollectorOptions(
        limit=args.limit,
        entity=args.entity,
        gcp_billing_table=(
            getattr(args, "gcp_billing_table", None)
            or os.environ.get("GCP_BILLING_EXPORT_TABLE")
        ),
    )
    app["state"] = {
        "startup_id": uuid.uuid4().hex,
        "report": None,
        "report_error": None,
        "refresh": refresh_state,
        "query_diagnostics": query_diagnostics,
    }
    app["log_attempt_tasks"] = {}
    app["log_attempt_cache"] = {}
    app["log_refresh_tasks"] = {}
    from overwatch.raw_cache import raw_cache_path

    # Build the first screen immediately from raw files before refreshing providers.
    if raw_cache_path("global", "sky", "jobs.json").exists():
        try:
            app["state"]["report"] = await collect_report_from_raw_cache(
                args,
                query_diagnostics=query_diagnostics,
                refresh_cache=False,
            )
        except (OSError, TypeError, ValueError) as error:
            logger.warning("Could not render the existing raw metric cache: {}", error)
    if app["state"]["report"] is not None:
        logger.info(
            "Restored {} cached resources; refreshing in the background.",
            len(app["state"]["report"]["resources"]),
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

    background_tasks = []
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
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)
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
