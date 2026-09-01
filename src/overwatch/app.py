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
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from importlib.resources import files
from pathlib import Path
from time import perf_counter
from typing import Any
from urllib.parse import urlparse

import boto3
import wandb
from aiohttp import web
from dotenv import load_dotenv
from loguru import logger
from watchfiles import DefaultFilter, run_process

from overwatch.constants import (
    ACTIVE_RESOURCE_STATUSES,
    ACTIVE_SKY_STATUSES,
    CLOUDWATCH_LOG_GROUP,
    CLOUDWATCH_REGION,
    DEFAULT_ZYMTRACE_PROJECT_ID,
)
from overwatch.logs import (
    ansi_log_text_to_safe_html,
    cloudwatch_event_message,
    progress_from_job_log,
    resolve_cloudwatch_stream_name,
)
from overwatch.providers.billing import collect_daily_cloud_spend
from overwatch.providers.sky import collect_managed_jobs, collect_standalone_clusters
from overwatch.providers.wandb_flow import collect_recent_flow_runs, match_skypilot_jobs
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


def json_safe_query_output(value: Any) -> Any:
    """Convert provider objects into inspectable JSON-compatible values."""
    if hasattr(value, "model_dump"):
        return json_safe_query_output(value.model_dump(mode="json"))
    if isinstance(value, dict):
        return {str(key): json_safe_query_output(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe_query_output(child) for child in value]
    if isinstance(value, Enum):
        return json_safe_query_output(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


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


async def collect_report(
    args: argparse.Namespace,
    *,
    log_progress: bool = False,
    query_diagnostics: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    collection_started = perf_counter()

    def record_query(
        key: str,
        status: str,
        summary: str,
        *,
        duration_seconds: float | None = None,
        raw_output: Any = None,
        error: BaseException | None = None,
    ) -> None:
        if query_diagnostics is None:
            return
        query_diagnostics[key] = {
            "key": key,
            "label": QUERY_LABELS[key],
            "status": status,
            "summary": summary,
            "updated_at": isoformat(datetime.now(UTC)),
            "duration_seconds": (
                round(duration_seconds, 2) if duration_seconds is not None else None
            ),
            "error": f"{type(error).__name__}: {error}" if error else None,
            "raw_output": json_safe_query_output(raw_output),
        }

    async def run_timed_thread_query(
        function: Any, *arguments: Any
    ) -> tuple[Any, Exception | None, float]:
        query_started = perf_counter()
        try:
            return (
                await asyncio.to_thread(function, *arguments),
                None,
                perf_counter() - query_started,
            )
        except Exception as error:  # noqa: BLE001
            return None, error, perf_counter() - query_started

    # Mark every source pending so a stalled query is visible while refresh is active.
    for query_key in QUERY_LABELS:
        record_query(query_key, "pending", "Waiting for query")

    # Match `uv run` credential loading when the globally installed executable runs directly.
    if uv_env_file := os.environ.get("UV_ENV_FILE"):
        load_dotenv(uv_env_file)
    if log_progress:
        logger.info("Loading the complete SkyPilot job and cluster inventory…")
    jobs_query, clusters_query, billing_query = await asyncio.gather(
        run_timed_thread_query(collect_managed_jobs),
        run_timed_thread_query(collect_standalone_clusters),
        run_timed_thread_query(
            collect_daily_cloud_spend,
            getattr(args, "gcp_billing_table", None)
            or os.environ.get("GCP_BILLING_EXPORT_TABLE"),
        ),
    )
    jobs_result, jobs_error, jobs_duration = jobs_query
    clusters_result, clusters_error, clusters_duration = clusters_query
    billing_result, billing_error, billing_duration = billing_query

    # Preserve independent provider results even when an authoritative inventory query fails.
    if jobs_error is not None:
        record_query(
            "sky_jobs",
            "error",
            "Query failed",
            duration_seconds=jobs_duration,
            error=jobs_error,
        )
    else:
        record_query(
            "sky_jobs",
            "ok",
            f"{len(jobs_result)} jobs",
            duration_seconds=jobs_duration,
            raw_output=jobs_result,
        )
    if clusters_error is not None:
        record_query(
            "sky_clusters",
            "error",
            "Query failed",
            duration_seconds=clusters_duration,
            error=clusters_error,
        )
    else:
        record_query(
            "sky_clusters",
            "ok",
            f"{len(clusters_result)} clusters",
            duration_seconds=clusters_duration,
            raw_output=clusters_result,
        )
    if billing_error is not None:
        record_query(
            "billing",
            "error",
            "Query failed",
            duration_seconds=billing_duration,
            error=billing_error,
        )
    else:
        billing_status = "warning" if billing_result["warnings"] else "ok"
        record_query(
            "billing",
            billing_status,
            "; ".join(billing_result["warnings"]) or "Billing query completed",
            duration_seconds=billing_duration,
            raw_output=billing_result,
        )

    fatal_queries = [
        (QUERY_LABELS[key], error)
        for key, error in (
            ("sky_jobs", jobs_error),
            ("sky_clusters", clusters_error),
            ("billing", billing_error),
        )
        if error is not None
    ]
    if fatal_queries:
        raise RuntimeError(
            "; ".join(f"{label}: {error}" for label, error in fatal_queries)
        )
    jobs = jobs_result
    clusters = clusters_result
    billing = billing_result
    if log_progress:
        logger.info(
            "Found {} managed jobs and {} standalone clusters ({:.1f}s).",
            len(jobs),
            len(clusters),
            perf_counter() - collection_started,
        )

    # W&B is optional enrichment: a telemetry failure must never remove Sky resources.
    global_warnings = list(billing["warnings"])
    wandb_started = perf_counter()
    if log_progress:
        logger.info(
            "Loading up to {} recent W&B training runs for enrichment…", args.limit
        )
    try:
        api = await asyncio.to_thread(wandb.Api, timeout=30)
        entity = args.entity or api.default_entity
        runs = await collect_recent_flow_runs(api, entity, args.limit)

        # Hydrate scheduler-advertised runs even after they age out of the recent-run window.
        selected_wandb_ids = {run.id for run in runs}
        linked_wandb_paths = set()
        for job in jobs:
            for linked_url in (job.links or {}).values():
                parsed_url = urlparse(linked_url)
                path_parts = parsed_url.path.strip("/").split("/")
                if (
                    parsed_url.hostname in {"wandb.ai", "app.wandb.ai"}
                    and len(path_parts) >= 4
                    and path_parts[-2] == "runs"
                    and path_parts[-1] not in selected_wandb_ids
                ):
                    linked_wandb_paths.add(
                        f"{path_parts[-4]}/{path_parts[-3]}/{path_parts[-1]}"
                    )
        linked_wandb_results = await asyncio.gather(
            *(
                asyncio.to_thread(api.run, linked_wandb_path)
                for linked_wandb_path in linked_wandb_paths
            ),
            return_exceptions=True,
        )
        runs.extend(
            linked_run
            for linked_run in linked_wandb_results
            if not isinstance(linked_run, BaseException)
            and linked_run.config.get("_id_") == "TrainConfig"
        )
        record_query(
            "wandb",
            "ok",
            f"{len(runs)} runs",
            duration_seconds=perf_counter() - wandb_started,
            raw_output=[
                {
                    "id": run.id,
                    "name": run.name,
                    "path": list(run.path),
                    "state": run.state,
                    "created_at": run.created_at,
                    "url": run.url,
                    "config": dict(run.config),
                }
                for run in runs
            ],
        )
    # W&B is non-authoritative; any client failure must leave cloud inventory visible.
    except Exception as error:  # noqa: BLE001
        runs = []
        entity = args.entity
        global_warnings.append(f"Could not collect W&B enrichment: {error}")
        record_query(
            "wandb",
            "error",
            "Query failed",
            duration_seconds=perf_counter() - wandb_started,
            error=error,
        )
        if log_progress:
            logger.warning("W&B enrichment is unavailable: {}", error)
    if log_progress:
        logger.info(
            "W&B enrichment returned {} runs ({:.1f}s).",
            len(runs),
            perf_counter() - wandb_started,
        )

    matched_jobs = match_skypilot_jobs(runs, jobs)
    matched_jobs_by_id = {job.job_id: job for job in matched_jobs if job is not None}
    active_jobs_by_id = {
        job.job_id: job for job in jobs if enum_value(job.status) in ACTIVE_SKY_STATUSES
    }
    jobs_for_progress = (
        {} if args.no_log_enrichment else matched_jobs_by_id | active_jobs_by_id
    )
    log_started = perf_counter()
    if log_progress:
        message = (
            f"Enriching {len(jobs_for_progress)} jobs from CloudWatch logs"
            if jobs_for_progress
            else "Skipping log enrichment"
        )
        logger.info("{}…", message)
    cloudwatch_client = boto3.client("logs", region_name=CLOUDWATCH_REGION)
    log_semaphore = asyncio.Semaphore(8)
    log_values = await asyncio.gather(
        *(
            progress_from_job_log(job, cloudwatch_client, log_semaphore)
            for job in jobs_for_progress.values()
        )
    )
    log_results = dict(zip(jobs_for_progress, log_values))
    if args.no_log_enrichment:
        record_query(
            "cloudwatch",
            "skipped",
            "Disabled by --no-log-enrichment",
            duration_seconds=perf_counter() - log_started,
        )
    else:
        cloudwatch_errors = [error for _progress, error in log_values if error]
        record_query(
            "cloudwatch",
            "warning" if cloudwatch_errors else "ok",
            (
                f"{len(log_values) - len(cloudwatch_errors)}/{len(log_values)} jobs enriched"
                if log_values
                else "No jobs required enrichment"
            ),
            duration_seconds=perf_counter() - log_started,
            raw_output={
                str(job_id): {"progress": progress, "error": error}
                for job_id, (progress, error) in log_results.items()
            },
        )
    if log_progress:
        logger.info("Log enrichment finished ({:.1f}s).", perf_counter() - log_started)

    records = []
    matched_runs_by_job_id = {
        job.job_id: (run, dict(run.config))
        for run, job in zip(runs, matched_jobs)
        if job is not None
    }
    matched_records = []
    matched_configs = []
    for job in jobs:
        matched_run = matched_runs_by_job_id.get(job.job_id)
        job_log_progress, progress_error = (
            log_results.get(job.job_id, (None, None)) if job else (None, None)
        )
        if matched_run is not None:
            run, config = matched_run
            record = build_run_record(
                run,
                job,
                config,
                job_log_progress,
                progress_error,
                args.zymtrace_project_id,
            )
            matched_records.append(record)
            matched_configs.append(config)
        else:
            record = build_sky_only_record(
                job,
                job_log_progress,
                progress_error,
                args.zymtrace_project_id,
            )
        records.append(record)

    differences = config_differences(matched_records, matched_configs)
    records.extend(
        build_cluster_record(cluster, args.zymtrace_project_id) for cluster in clusters
    )

    def active_record_sort_key(record: dict[str, Any]) -> tuple[bool, str]:
        is_active = record["status"]["skypilot"] in ACTIVE_RESOURCE_STATUSES
        return is_active, record["submitted_at"] or ""

    records.sort(key=active_record_sort_key, reverse=True)
    return {
        "generated_at": isoformat(datetime.now(UTC)),
        "wandb_entity": entity,
        "requested_limit": args.limit,
        "selection": "active and recent SkyPilot jobs and standalone clusters, optionally enriched with training data",
        "log_enrichment": not args.no_log_enrichment,
        "billing": billing,
        "resources": records,
        "config_differences": differences,
        "warnings": global_warnings,
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
    return web.json_response(
        {
            "key": query_key,
            "label": diagnostic["label"],
            "status": diagnostic["status"],
            "error": diagnostic["error"],
            "updated_at": diagnostic["updated_at"],
            "duration_seconds": diagnostic["duration_seconds"],
            "output": diagnostic["raw_output"],
        }
    )


async def handle_log_stream(request: web.Request) -> web.StreamResponse:
    job_id = int(request.match_info["job_id"])
    report = request.app["state"]["report"]
    record = (
        next(
            (
                record
                for record in report["resources"]
                if record["skypilot"]["job_id"] == job_id
            ),
            None,
        )
        if report
        else None
    )
    if record is None or not record["skypilot"].get("cluster_name"):
        raise web.HTTPNotFound(text="CloudWatch logs are unavailable for this job")

    cloudwatch_client = request.app["cloudwatch_client"]
    stream_name = await asyncio.to_thread(
        resolve_cloudwatch_stream_name,
        cloudwatch_client,
        record["skypilot"]["cluster_name"],
    )
    if stream_name is None:
        raise web.HTTPNotFound(text="CloudWatch log stream was not found")

    response = web.StreamResponse(
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )
    await response.prepare(request)
    next_token = None
    try:
        while True:
            request_arguments = {
                "logGroupName": CLOUDWATCH_LOG_GROUP,
                "logStreamName": stream_name,
                "startFromHead": True,
                "limit": 200,
            }
            if next_token is not None:
                request_arguments["nextToken"] = next_token
            else:
                request_arguments["startTime"] = int(
                    (datetime.now(UTC) - timedelta(minutes=2)).timestamp() * 1000
                )
            events = await asyncio.to_thread(
                cloudwatch_client.get_log_events, **request_arguments
            )
            next_token = events["nextForwardToken"]
            for event in events["events"]:
                payload = json.dumps(
                    {
                        "timestamp": event["timestamp"],
                        "html": ansi_log_text_to_safe_html(
                            cloudwatch_event_message(event)
                        ),
                    },
                    separators=(",", ":"),
                )
                await response.write(f"data: {payload}\n\n".encode())
            await response.write(b": keepalive\n\n")
            await asyncio.sleep(2)
    except (asyncio.CancelledError, ConnectionResetError):
        pass
    return response


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
        report = await collect_report(
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
    service_started = perf_counter()
    url = f"http://{args.host}:{args.port}"
    await asyncio.to_thread(build_frontend_assets_if_sources_are_newer)
    app = web.Application()
    app["state"] = {
        "startup_id": uuid.uuid4().hex,
        "report": None,
        "report_error": None,
        "refresh": {
            "in_progress": False,
            "last_attempt_at": None,
            "last_success_at": None,
            "last_duration_seconds": None,
            "last_error": None,
        },
        "query_diagnostics": {
            key: {
                "key": key,
                "label": label,
                "status": "pending",
                "summary": "Waiting for first refresh",
                "updated_at": None,
                "duration_seconds": None,
                "error": None,
                "raw_output": None,
            }
            for key, label in QUERY_LABELS.items()
        },
    }
    app.add_routes(
        [
            web.get("/", handle_index),
            web.get("/billing", handle_index),
            web.get("/cost-waste", handle_index),
            web.get("/api/report", handle_report_json),
            web.get("/api/health", handle_health),
            web.get("/api/status", handle_query_status),
            web.get("/api/status/{query_key}/raw", handle_raw_query_output),
            web.get("/api/logs/{job_id:\\d+}", handle_log_stream),
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
        app["cloudwatch_client"] = boto3.client("logs", region_name=CLOUDWATCH_REGION)
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
