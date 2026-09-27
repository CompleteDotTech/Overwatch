"""Standalone raw metric collector and versioned file cache.

This module owns every provider request made by Overwatch. It has no dependency
on the HTTP server or frontend and can be run directly for manual collection::

    uv run python -m overwatch.collector
    uv run python -m overwatch.collector --job-id 183 --full-logs
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

import wandb
from loguru import logger

from overwatch.cloudwatch_cache import (
    append_cloudwatch_events,
    ensure_cloudwatch_zstd_cache,
)
from overwatch.constants import (
    ACTIVE_SKY_STATUSES,
    RESOURCE_HISTORY_DAYS,
    TERMINAL_SKY_STATUSES,
)
from overwatch.providers.billing import (
    collect_aws_billing_responses,
    collect_gcp_billing_rows,
)
from overwatch.providers.sky import collect_managed_jobs, collect_standalone_clusters
from overwatch.providers.wandb_flow import collect_recent_flow_runs, match_skypilot_jobs
from overwatch.raw_cache import (
    RAW_CACHE_ROOT,
    json_safe,
    raw_cache_path,
    read_json,
    write_json_atomically,
)
from overwatch.utils import enum_value, isoformat

RAW_CACHE_SPEC_VERSION = 1


@dataclass(frozen=True)
class CollectorOptions:
    """Provider configuration for one raw collection pass."""

    limit: int = 20
    entity: str | None = None
    gcp_billing_table: str | None = None
    local_models_only: bool = False














def serialize_sky_object(value: Any) -> dict[str, Any]:
    """Dump a Sky SDK model plus scalar properties omitted by model serialization."""
    serialized = json_safe(value)
    if not isinstance(serialized, dict):
        serialized = {"sdk_value": serialized}
    for attribute in (
        "name",
        "status",
        "is_managed",
        "launched_at",
        "status_updated_at",
        "user_name",
        "resources_str",
        "job_id",
        "job_name",
        "submitted_at",
        "start_at",
        "end_at",
        "job_duration",
        "recovery_count",
        "last_recovered_at",
        "resources",
        "links",
        "cloud",
        "region",
        "nodes",
    ):
        try:
            serialized.setdefault(attribute, json_safe(getattr(value, attribute)))
        except (AttributeError, RuntimeError, TypeError, ValueError):
            continue
    return serialized


def serialize_wandb_run(run: Any) -> dict[str, Any]:
    """Dump the raw W&B attributes alongside lazily loaded SDK properties."""
    return {
        "attrs": json_safe(getattr(run, "_attrs", {})),
        "id": run.id,
        "name": run.name,
        "path": list(run.path),
        "state": run.state,
        "created_at": run.created_at,
        "url": run.url,
        "project": run.project,
        "entity": run.entity,
        "config": json_safe(dict(run.config)),
        "summary": json_safe(dict(run.summary)),
        "metadata": json_safe(getattr(run, "_metadata", None)),
    }




async def collect_raw_metrics(
    options: CollectorOptions,
    *,
    job_id: int | None = None,
    full_logs: bool = False,
) -> dict[str, Any]:
    """Refresh all raw snapshots or one job and return the collection manifest."""
    collection_started = perf_counter()
    manifest_path = raw_cache_path("manifest.json")
    previous_manifest = read_json(manifest_path, {})

    # A settled terminal job is immutable and returns from disk without provider requests.
    if job_id is not None:
        cached_job = read_json(raw_cache_path("jobs", str(job_id), "sky.json"))
        cached_cursor = read_json(
            raw_cache_path("jobs", str(job_id), "cloudwatch", "cursor.json"), {}
        )
        if (
            cached_job
            and enum_value(cached_job.get("status")) in TERMINAL_SKY_STATUSES
            and cached_cursor.get("settled")
            and (cached_cursor.get("complete_from_head") or not full_logs)
        ):
            events_path, events_index_path = ensure_cloudwatch_zstd_cache(
                raw_cache_path("jobs", str(job_id), "cloudwatch")
            )
            if events_path.exists():
                cached_cursor.update(
                    {
                        "events_file": events_path.name,
                        "events_index_file": events_index_path.name,
                        "total_bytes": events_path.stat().st_size,
                    }
                )
                write_json_atomically(
                    raw_cache_path(
                        "jobs", str(job_id), "cloudwatch", "cursor.json"
                    ),
                    cached_cursor,
                )
            return previous_manifest

    source_status: dict[str, dict[str, Any]] = {}

    async def collect_source(
        key: str,
        function: Any,
        *arguments: Any,
        **keyword_arguments: Any,
    ) -> Any:
        started = perf_counter()
        try:
            value = await asyncio.to_thread(
                function, *arguments, **keyword_arguments
            )
        except Exception as error:  # noqa: BLE001 - provider SDKs expose diverse errors
            source_status[key] = {
                "status": "error",
                "duration_seconds": round(perf_counter() - started, 3),
                "error": f"{type(error).__name__}: {error}",
            }
            return None
        source_status[key] = {
            "status": "ok",
            "duration_seconds": round(perf_counter() - started, 3),
            "error": None,
        }
        return value

    # Explicit offline/local mode: use the REAL collector and cache without cloud I/O.
    # Default behavior and existing Flow/resource records remain unchanged.
    if options.local_models_only:
        if job_id is not None:
            raise ValueError("local-models-only cannot refresh a SkyPilot job")
        result = await collect_source("kev_laya", collect_kev_laya_cache)
        if result is not None:
            source_status["kev_laya"]["raw"] = result
            if result["warnings"]:
                source_status["kev_laya"]["status"] = "warning"
        for key in ("sky_jobs", "sky_clusters", "aws_billing", "gcp_billing", "wandb", "cloudwatch"):
            source_status[key] = {"status": "skipped", "duration_seconds": 0.0,
                                  "error": None, "reason": "explicit local-models-only"}
        manifest = {
            "cache_spec_version": RAW_CACHE_SPEC_VERSION, "cache_root": str(RAW_CACHE_ROOT),
            "updated_at": isoformat(datetime.now(UTC)),
            "duration_seconds": round(perf_counter() - collection_started, 3),
            "scope": {"job_id": None, "full_logs": False, "local_models_only": True},
            "sources": source_status,
        }
        write_json_atomically(manifest_path, manifest)
        return manifest

    # A scoped job refresh reuses inventory and only advances that job's raw log cursor.
    if job_id is not None:
        job = read_json(raw_cache_path("jobs", str(job_id), "sky.json"))
        if job is None:
            raise KeyError(f"SkyPilot job {job_id} is not present in the raw cache")
        cursor = await collect_source(
            "cloudwatch", append_cloudwatch_events, job, full_logs=full_logs
        )
        if cursor is not None:
            source_status["cloudwatch"]["raw"] = cursor
    else:
        jobs_result, clusters_result, aws_billing_result = await asyncio.gather(
            collect_source("sky_jobs", collect_managed_jobs),
            collect_source("sky_clusters", collect_standalone_clusters),
            collect_source("aws_billing", collect_aws_billing_responses),
        )
        serialized_jobs = (
            [serialize_sky_object(job) for job in jobs_result]
            if jobs_result is not None
            else read_json(raw_cache_path("global", "sky", "jobs.json"), [])
        )
        serialized_clusters = (
            [serialize_sky_object(cluster) for cluster in clusters_result]
            if clusters_result is not None
            else read_json(raw_cache_path("global", "sky", "clusters.json"), [])
        )
        jobs = (
            jobs_result
            if jobs_result is not None
            else [SimpleNamespace(**job) for job in serialized_jobs]
        )
        if jobs_result is not None:
            write_json_atomically(
                raw_cache_path("global", "sky", "jobs.json"), serialized_jobs
            )
        if clusters_result is not None:
            write_json_atomically(
                raw_cache_path("global", "sky", "clusters.json"), serialized_clusters
            )
        if aws_billing_result is not None:
            write_json_atomically(
                raw_cache_path("global", "billing", "aws.json"), aws_billing_result
            )
        if options.gcp_billing_table:
            gcp_billing_result = await collect_source(
                "gcp_billing", collect_gcp_billing_rows, options.gcp_billing_table
            )
            if gcp_billing_result is not None:
                write_json_atomically(
                    raw_cache_path("global", "billing", "gcp.json"),
                    gcp_billing_result,
                )
        else:
            source_status["gcp_billing"] = {
                "status": "skipped",
                "duration_seconds": 0.0,
                "error": None,
            }

        kev_laya_result = await collect_source("kev_laya", collect_kev_laya_cache)
        if kev_laya_result is not None:
            source_status["kev_laya"]["raw"] = kev_laya_result
            if kev_laya_result["warnings"]:
                source_status["kev_laya"]["status"] = "warning"

        # W&B stays optional and is captured as raw hydrated run records.
        wandb_started = perf_counter()
        try:
            api = await asyncio.to_thread(wandb.Api, timeout=30)
            entity = options.entity or api.default_entity
            runs = await collect_recent_flow_runs(api, entity, options.limit)
            linked_paths = set()
            history_cutoff = (
                datetime.now(UTC).timestamp() - RESOURCE_HISTORY_DAYS * 86400
            )
            for job in jobs:
                if (
                    enum_value(job.status) in TERMINAL_SKY_STATUSES
                    and job.end_at is not None
                    and job.end_at < history_cutoff
                ):
                    continue
                for label, url in (job.links or {}).items():
                    path_parts = urlparse(url).path.strip("/").split("/")
                    if (
                        ("wandb" in label.casefold() or "w&b" in label.casefold())
                        and len(path_parts) >= 4
                        and path_parts[-2] == "runs"
                    ):
                        linked_paths.add(
                            f"{path_parts[-4]}/{path_parts[-3]}/{path_parts[-1]}"
                        )
            selected_ids = {run.id for run in runs}
            linked_runs = await asyncio.gather(
                *(
                    asyncio.to_thread(api.run, linked_path)
                    for linked_path in linked_paths
                    if linked_path.rsplit("/", 1)[-1] not in selected_ids
                ),
                return_exceptions=True,
            )
            runs.extend(
                run
                for run in linked_runs
                if not isinstance(run, BaseException)
                and run.config.get("_id_") == "TrainConfig"
            )
            serialized_runs = [serialize_wandb_run(run) for run in runs]
            write_json_atomically(
                raw_cache_path("global", "wandb", "runs.json"), serialized_runs
            )
            source_status["wandb"] = {
                "status": "ok",
                "duration_seconds": round(perf_counter() - wandb_started, 3),
                "error": None,
            }
        except Exception as error:  # noqa: BLE001 - W&B SDK errors are not stable
            runs = []
            source_status["wandb"] = {
                "status": "error",
                "duration_seconds": round(perf_counter() - wandb_started, 3),
                "error": f"{type(error).__name__}: {error}",
            }

        # Materialize raw per-job files for direct shell exploration.
        matched_jobs = match_skypilot_jobs(runs, jobs) if runs else []
        run_by_job_id = {
            matched_job.job_id: run
            for run, matched_job in zip(runs, matched_jobs)
            if matched_job is not None
        }
        for job, serialized_job in zip(jobs, serialized_jobs):
            job_directory = raw_cache_path("jobs", str(job.job_id))
            write_json_atomically(job_directory / "sky.json", serialized_job)
            if run := run_by_job_id.get(job.job_id):
                write_json_atomically(
                    job_directory / "wandb.json", serialize_wandb_run(run)
                )

        # Routine collection advances active cursors and enriches old local indexes once.
        active_jobs = [
            serialized_job
            for serialized_job in serialized_jobs
            if enum_value(serialized_job.get("status")) in ACTIVE_SKY_STATUSES
        ]
        inactive_aws_job_directories = [
            raw_cache_path("jobs", str(job["job_id"]), "cloudwatch")
            for job in serialized_jobs
            if enum_value(job.get("status")) not in ACTIVE_SKY_STATUSES
            and str(job.get("cloud", "")).casefold() == "aws"
            and raw_cache_path(
                "jobs", str(job["job_id"]), "cloudwatch", "events.jsonl.zst"
            ).exists()
        ]
        cloudwatch_started = perf_counter()
        cloudwatch_semaphore = asyncio.Semaphore(8)

        async def run_bounded_cloudwatch_operation(
            function: Any, *arguments: Any
        ) -> Any:
            async with cloudwatch_semaphore:
                return await asyncio.to_thread(function, *arguments)

        # Bound SDK threads when a large shared inventory has many active jobs.
        cloudwatch_results = await asyncio.gather(
            *(
                run_bounded_cloudwatch_operation(append_cloudwatch_events, job)
                for job in active_jobs
                if str(job.get("cloud", "")).casefold() == "aws"
            ),
            *(
                run_bounded_cloudwatch_operation(
                    ensure_cloudwatch_zstd_cache, job_directory
                )
                for job_directory in inactive_aws_job_directories
            ),
            return_exceptions=True,
        )
        cloudwatch_errors = [
            result for result in cloudwatch_results if isinstance(result, BaseException)
        ]
        source_status["cloudwatch"] = {
            "status": "warning" if cloudwatch_errors else "ok",
            "duration_seconds": round(perf_counter() - cloudwatch_started, 3),
            "error": "; ".join(str(error) for error in cloudwatch_errors) or None,
        }

    manifest = {
        "cache_spec_version": RAW_CACHE_SPEC_VERSION,
        "cache_root": str(RAW_CACHE_ROOT),
        "updated_at": isoformat(datetime.now(UTC)),
        "duration_seconds": round(perf_counter() - collection_started, 3),
        "scope": {"job_id": job_id, "full_logs": full_logs},
        "sources": source_status,
    }
    # Scoped refreshes retain global query status and write their own inspectable manifest.
    if job_id is None:
        write_json_atomically(manifest_path, manifest)
    else:
        write_json_atomically(
            raw_cache_path("jobs", str(job_id), "collection.json"), manifest
        )
    return manifest



def collect_kev_laya_cache() -> dict[str, Any]:
    """The collector alone owns Kev-Laya raw-cache writes."""
    from overwatch.constants import KEV_LAYA_RETENTION_DAYS
    from overwatch.kev_laya_adapter import merge_snapshots
    from overwatch.kev_laya_lock import collector_lock
    from overwatch.providers.kev_laya import collect_snapshots
    from overwatch.raw_cache import model_runs_cache_path

    path = model_runs_cache_path()
    collected = collect_snapshots()
    with collector_lock(path.with_suffix(".lock")):
        from overwatch.raw_cache import read_model_runs_cache
        envelope = merge_snapshots(read_model_runs_cache(), collected,
                                   retention_days=KEV_LAYA_RETENTION_DAYS)
        write_json_atomically(path, envelope)
    return {"runs": len(envelope["records"]), "warnings": len(envelope["warnings"]),
            "complete": collected.get("complete", False),
            "accounting": collected.get("accounting", {}),
            "source_status": collected.get("sources", []),
            "collection_warnings": collected.get("warnings", [])}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect raw Overwatch metrics")
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--full-logs", action="store_true")
    parser.add_argument("--local-models-only", action="store_true",
                        default=os.environ.get("OVERWATCH_LOCAL_MODELS_ONLY") == "1")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--entity")
    parser.add_argument(
        "--gcp-billing-table", default=os.environ.get("GCP_BILLING_EXPORT_TABLE")
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    options = CollectorOptions(
        limit=args.limit,
        entity=args.entity,
        gcp_billing_table=args.gcp_billing_table,
        local_models_only=args.local_models_only,
    )
    manifest = asyncio.run(
        collect_raw_metrics(options, job_id=args.job_id, full_logs=args.full_logs)
    )
    logger.success("Raw metrics cached at {}", RAW_CACHE_ROOT)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
