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
from urllib.parse import urlparse

import boto3
import wandb
from aiohttp import web
from botocore.exceptions import ClientError
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
    error_retry_count_from_controller_log,
    progress_and_retries_from_job_log,
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
        help="skip CloudWatch/SkyPilot log enrichment",
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
    args: argparse.Namespace, *, log_progress: bool = False
) -> dict[str, Any]:
    collection_started = perf_counter()

    # Match `uv run` credential loading when the globally installed executable runs directly.
    if uv_env_file := os.environ.get("UV_ENV_FILE"):
        load_dotenv(uv_env_file)
    if log_progress:
        logger.info("Loading the complete SkyPilot job and cluster inventory…")
    jobs, clusters, billing = await asyncio.gather(
        asyncio.to_thread(collect_managed_jobs),
        asyncio.to_thread(collect_standalone_clusters),
        asyncio.to_thread(
            collect_daily_cloud_spend,
            getattr(args, "gcp_billing_table", None)
            or os.environ.get("GCP_BILLING_EXPORT_TABLE"),
        ),
    )
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
    # W&B is non-authoritative; any client failure must leave cloud inventory visible.
    except Exception as error:  # noqa: BLE001
        runs = []
        entity = args.entity
        global_warnings.append(f"Could not collect W&B enrichment: {error}")
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
            f"Enriching {len(jobs_for_progress)} jobs from CloudWatch/SkyPilot logs"
            if jobs_for_progress
            else "Skipping log enrichment"
        )
        logger.info("{}…", message)
    cloudwatch_client = boto3.client("logs", region_name=CLOUDWATCH_REGION)
    log_semaphore = asyncio.Semaphore(8)
    log_values, retry_values = await asyncio.gather(
        asyncio.gather(
            *(
                progress_and_retries_from_job_log(job, cloudwatch_client, log_semaphore)
                for job in jobs_for_progress.values()
            )
        ),
        asyncio.gather(
            *(
                error_retry_count_from_controller_log(job, log_semaphore)
                for job in jobs_for_progress.values()
            )
        ),
    )
    log_results = dict(zip(jobs_for_progress, log_values))
    retry_results = dict(zip(jobs_for_progress, retry_values))
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
        error_retries, retry_error = (
            retry_results.get(job.job_id, (None, None)) if job else (None, None)
        )
        if matched_run is not None:
            run, config = matched_run
            record = build_run_record(
                run,
                job,
                config,
                job_log_progress,
                progress_error,
                error_retries,
                retry_error,
                args.zymtrace_project_id,
            )
            matched_records.append(record)
            matched_configs.append(config)
        else:
            record = build_sky_only_record(
                job,
                job_log_progress,
                progress_error,
                error_retries,
                retry_error,
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


async def refresh_report_forever(
    app: web.Application, args: argparse.Namespace
) -> None:
    while True:
        await asyncio.sleep(args.refresh_interval)
        try:
            app["state"]["report"] = await collect_report(args)
            app["state"]["report_error"] = None
        except (
            ClientError,
            OSError,
            RuntimeError,
            ValueError,
            wandb.errors.Error,
        ) as error:
            app["state"]["report_error"] = str(error)
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
    }
    app.add_routes(
        [
            web.get("/", handle_index),
            web.get("/billing", handle_index),
            web.get("/cost-waste", handle_index),
            web.get("/api/report", handle_report_json),
            web.get("/api/health", handle_health),
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
            report = await collect_report(args, log_progress=True)
            app["state"]["report"] = report
            logger.success(
                "Cloud inventory is ready with {} resources ({:.1f}s).",
                len(report["resources"]),
                perf_counter() - service_started,
            )
        except (
            ClientError,
            OSError,
            RuntimeError,
            ValueError,
            wandb.errors.Error,
        ) as error:
            app["state"]["report_error"] = str(error)
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
