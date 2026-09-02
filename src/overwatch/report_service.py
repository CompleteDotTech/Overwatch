"""Build presentation reports exclusively from the raw metric cache."""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from typing import Any

from loguru import logger

from overwatch.constants import (
    ACTIVE_RESOURCE_STATUSES,
    ACTIVE_SKY_STATUSES,
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
