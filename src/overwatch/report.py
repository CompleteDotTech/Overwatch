"""Cloud resource records with optional training estimates and links."""

from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote, urlencode

import sky
import sky.catalog
from sky.server import common as sky_server_common

from overwatch.constants import ACTIVE_RESOURCE_STATUSES, GPU_RESOURCE_RE, NODE_COUNT_RE
from overwatch.logs import aws_cluster_name_from_job
from overwatch.providers.wandb_flow import (
    flow_config_uri_from_run,
    normalized_wandb_experiment_name,
)
from overwatch.utils import (
    enum_value,
    format_progress,
    isoformat,
    parse_utc,
    timestamp_from_epoch,
)


def estimated_hourly_cost(job: Any) -> tuple[float | None, str | None]:
    resources = job.resources or ""
    match = GPU_RESOURCE_RE.search(resources)
    if match is None:
        return None, None
    accelerator, count_text = match.groups()
    node_count_match = NODE_COUNT_RE.match(resources)
    node_count = int(node_count_match.group(1)) if node_count_match else 1
    cloud = str(job.cloud or "").lower()
    cloud = cloud if cloud in {"aws", "gcp"} else None
    region = job.region if job.region and job.region != "-" else None
    use_spot = "Spot" in resources
    estimates: list[tuple[float, str, str]] = []
    for candidate_cloud in [cloud] if cloud else ["aws", "gcp"]:
        try:
            instance_types, _ = sky.catalog.get_instance_type_for_accelerator(
                accelerator,
                float(count_text),
                use_spot=use_spot,
                region=region,
                clouds=candidate_cloud,
            )
            for instance_type in instance_types or []:
                hourly = sky.catalog.get_hourly_cost(
                    instance_type, use_spot, region, None, clouds=candidate_cloud
                )
                if hourly > 0:
                    estimates.append((hourly, candidate_cloud, instance_type))
        except (
            AssertionError,
            KeyError,
            ValueError,
            sky.exceptions.ResourcesMismatchError,
            sky.exceptions.ResourcesUnavailableError,
        ):
            continue
    if not estimates:
        return None, None
    hourly, selected_cloud, instance_type = min(estimates)
    market = "spot" if use_spot else "on-demand"
    basis = f"Sky catalog: {selected_cloud}/{region or 'cheapest-region'} {instance_type} {market}"
    return (
        hourly * node_count,
        f"{basis}; {node_count} node{'s' if node_count != 1 else ''}",
    )


def gcp_storage_info(
    config_uri: str | None,
) -> tuple[str | None, str | None, str | None]:
    if config_uri is None:
        return None, None, None
    run_uri = config_uri.removesuffix("/train_config.yaml")
    if not config_uri.startswith("gs://"):
        return run_uri, None, None
    bucket_and_key = run_uri.removeprefix("gs://")
    bucket, _, key = bucket_and_key.partition("/")
    console_url = f"https://console.cloud.google.com/storage/browser/{quote(bucket)}/{quote(key, safe='/')}"
    return run_uri, bucket, console_url


def zymtrace_url(project_id: str, job_name: str, started_at: datetime | None) -> str:
    start = (started_at - timedelta(hours=1)).isoformat() if started_at else "-30D"
    query = urlencode({"start_ts": start, "end_ts": "now", "job_tag": job_name})
    return f"https://zymtrace.training.stoptypesafe.ai/project/{project_id}/profiles/gpu/timeline?{query}"


def calculate_progress_and_timing(
    run: Any,
    job: Any | None,
    log_progress: dict[str, Any] | None,
    config: dict[str, Any],
) -> dict[str, Any]:
    summary = run.summary
    batches_per_epoch = summary.get("misc/batches_per_epoch")
    if batches_per_epoch is None and log_progress:
        batches_per_epoch = log_progress.get("batches_per_epoch")
    batch_in_epoch = summary.get("misc/batch_idx")
    if batch_in_epoch is None and log_progress:
        batch_in_epoch = log_progress.get("batch")
    epoch = summary.get("misc/epoch")
    if epoch is None and log_progress:
        epoch = log_progress.get("epoch")
    epoch = epoch or 1
    n_epochs = config.get("n_epochs") or 1
    completed_batches = summary.get("misc/completed_batches")
    total_batches = summary.get("misc/total_batches")
    if completed_batches is None and log_progress:
        completed_batches = log_progress.get("completed_batches")
    if total_batches is None and log_progress:
        total_batches = log_progress.get("total_batches")
    if (
        completed_batches is None
        and batch_in_epoch is not None
        and batches_per_epoch is not None
    ):
        completed_batches = int((epoch - 1) * batches_per_epoch + batch_in_epoch)
    if total_batches is None and batches_per_epoch is not None:
        total_batches = int(n_epochs * batches_per_epoch)
    if completed_batches is None:
        completed_batches = summary.get("_step")
    remaining_batches = (
        max(0, total_batches - completed_batches)
        if completed_batches is not None and total_batches is not None
        else None
    )

    started_at = timestamp_from_epoch(job.start_at) if job else None
    loaded_metadata = getattr(run, "_metadata", None) or {}
    started_at = (
        started_at
        or parse_utc(loaded_metadata.get("startedAt"))
        or parse_utc(run.created_at)
    )
    elapsed_seconds = (
        job.job_duration
        if job and job.job_duration is not None
        else summary.get("_runtime")
    )
    estimated_total_seconds = None
    estimated_finish_at = None
    remaining_seconds = summary.get("misc/estimated_remaining_seconds")
    if remaining_seconds is None and log_progress:
        remaining_seconds = log_progress.get("remaining_seconds")
    tokens_per_second = log_progress.get("tokens_per_second") if log_progress else None
    if tokens_per_second is None:
        tokens_per_second = summary.get("misc/tokens_per_sec")
    is_running = run.state == "running" or enum_value(job.status if job else None) in {
        "RUNNING",
        "RECOVERING",
    }
    if remaining_seconds is not None and elapsed_seconds is not None and is_running:
        estimated_total_seconds = elapsed_seconds + remaining_seconds
        estimated_finish_at = datetime.now(UTC) + timedelta(seconds=remaining_seconds)
    elif completed_batches and total_batches and completed_batches >= total_batches:
        estimated_total_seconds = elapsed_seconds
        estimated_finish_at = timestamp_from_epoch(job.end_at) if job else None

    return {
        "started_at": isoformat(started_at),
        "elapsed_seconds": round(elapsed_seconds, 1)
        if elapsed_seconds is not None
        else None,
        "estimated_total_seconds": round(estimated_total_seconds, 1)
        if estimated_total_seconds is not None
        else None,
        "estimated_finish_at": isoformat(estimated_finish_at),
        "batch": int(batch_in_epoch) if batch_in_epoch is not None else None,
        "batches_per_epoch": batches_per_epoch,
        "completed_batches": completed_batches,
        "remaining_batches": remaining_batches,
        "total_batches": total_batches,
        "progress_fraction": completed_batches / total_batches
        if completed_batches is not None and total_batches
        else None,
        "tokens_per_second": tokens_per_second,
        "tokens_per_second_ema_samples": (
            log_progress.get("tokens_per_second_ema_samples") if log_progress else None
        ),
        "log_elapsed_seconds": log_progress.get("elapsed_seconds")
        if log_progress
        else None,
        "estimated_remaining_seconds": remaining_seconds,
    }


def build_run_record(
    run: Any,
    job: Any | None,
    config: dict[str, Any],
    log_progress: dict[str, Any] | None,
    progress_error: str | None,
    error_retries: int | None,
    retry_error: str | None,
    zymtrace_project_id: str,
) -> dict[str, Any]:
    timing = calculate_progress_and_timing(run, job, log_progress, config)
    config_uri = flow_config_uri_from_run(run, config)
    run_uri, gcp_bucket, gcp_console_url = gcp_storage_info(config_uri)
    job_name = job.job_name if job else normalized_wandb_experiment_name(run)
    recovery_count = job.recovery_count if job else None
    preemption_retries = None
    if recovery_count is not None and error_retries is not None:
        preemption_retries = max(0, recovery_count - error_retries)

    hourly_cost, cost_basis = estimated_hourly_cost(job) if job else (None, None)
    elapsed_seconds = timing["elapsed_seconds"]
    estimated_total_seconds = timing["estimated_total_seconds"]
    return {
        "kind": "managed_job",
        "name": run.name,
        "project": run.project,
        "user": config.get("user") or (job.user_name if job else None),
        "wandb_id": run.id,
        "submitted_at": isoformat(parse_utc(run.created_at)),
        "status": {
            "wandb": run.state,
            "skypilot": enum_value(job.status) if job else None,
        },
        "progress": {
            key: timing[key]
            for key in (
                "batch",
                "batches_per_epoch",
                "completed_batches",
                "remaining_batches",
                "total_batches",
                "progress_fraction",
                "tokens_per_second",
                "tokens_per_second_ema_samples",
                "log_elapsed_seconds",
                "estimated_remaining_seconds",
            )
        }
        | {
            "display": format_progress(
                timing["completed_batches"], timing["total_batches"]
            ),
            "collection_error": progress_error,
        },
        "retries": {
            "preemption_or_infrastructure": preemption_retries,
            "application_error": error_retries,
            "total_recoveries": recovery_count,
            "collection_error": retry_error,
        },
        "timing": {
            key: timing[key]
            for key in (
                "started_at",
                "elapsed_seconds",
                "estimated_total_seconds",
                "estimated_finish_at",
            )
        },
        "cost": {
            "hourly_usd": round(hourly_cost, 4) if hourly_cost is not None else None,
            "estimated_spend_usd": round(hourly_cost * elapsed_seconds / 3600, 2)
            if hourly_cost is not None and elapsed_seconds is not None
            else None,
            "estimated_total_usd": round(
                hourly_cost * estimated_total_seconds / 3600, 2
            )
            if hourly_cost is not None and estimated_total_seconds is not None
            else None,
            "basis": cost_basis,
        },
        "skypilot": {
            "resource_kind": "job" if job else None,
            "job_id": job.job_id if job else None,
            "cluster_name": aws_cluster_name_from_job(job) if job else None,
            "job_name": job_name,
            "resources": job.resources if job else None,
            "cloud": job.cloud if job else None,
            "region": job.region if job else None,
        },
        "storage": {
            "config_uri": config_uri,
            "run_uri": run_uri,
            "gcp_bucket": gcp_bucket,
        },
        "links": {
            "wandb": run.url,
            "skypilot": (
                f"{sky_server_common.get_server_url()}/dashboard/jobs/{job.job_id}"
                if job
                else None
            ),
            "zymtrace": zymtrace_url(
                zymtrace_project_id, job_name, parse_utc(timing["started_at"])
            ),
            "gcp_bucket": gcp_console_url,
        },
    }


def build_sky_only_record(
    job: Any,
    log_progress: dict[str, Any] | None,
    progress_error: str | None,
    error_retries: int | None,
    retry_error: str | None,
    zymtrace_project_id: str,
) -> dict[str, Any]:
    completed_batches = log_progress.get("completed_batches") if log_progress else None
    total_batches = log_progress.get("total_batches") if log_progress else None
    elapsed_seconds = job.job_duration
    remaining_seconds = log_progress.get("remaining_seconds") if log_progress else None
    estimated_total_seconds = (
        elapsed_seconds + remaining_seconds
        if elapsed_seconds is not None and remaining_seconds is not None
        else None
    )
    estimated_finish_at = (
        datetime.now(UTC) + timedelta(seconds=remaining_seconds)
        if remaining_seconds is not None
        else None
    )
    hourly_cost, cost_basis = estimated_hourly_cost(job)
    recovery_count = job.recovery_count
    preemption_retries = (
        max(0, recovery_count - error_retries)
        if recovery_count is not None and error_retries is not None
        else None
    )
    started_at = timestamp_from_epoch(job.start_at)
    return {
        "kind": "managed_job",
        "name": job.job_name,
        "project": None,
        "user": job.user_name,
        "wandb_id": None,
        "submitted_at": isoformat(timestamp_from_epoch(job.submitted_at)),
        "status": {"wandb": None, "skypilot": enum_value(job.status)},
        "progress": {
            "batch": log_progress.get("batch") if log_progress else None,
            "batches_per_epoch": log_progress.get("batches_per_epoch")
            if log_progress
            else None,
            "completed_batches": completed_batches,
            "remaining_batches": (
                max(0, total_batches - completed_batches)
                if completed_batches is not None and total_batches is not None
                else None
            ),
            "total_batches": total_batches,
            "progress_fraction": (
                completed_batches / total_batches
                if completed_batches is not None and total_batches
                else None
            ),
            "tokens_per_second": (
                log_progress.get("tokens_per_second") if log_progress else None
            ),
            "tokens_per_second_ema_samples": (
                log_progress.get("tokens_per_second_ema_samples")
                if log_progress
                else None
            ),
            "log_elapsed_seconds": (
                log_progress.get("elapsed_seconds") if log_progress else None
            ),
            "estimated_remaining_seconds": remaining_seconds,
            "display": format_progress(completed_batches, total_batches),
            "collection_error": progress_error,
        },
        "retries": {
            "preemption_or_infrastructure": preemption_retries,
            "application_error": error_retries,
            "total_recoveries": recovery_count,
            "collection_error": retry_error,
        },
        "timing": {
            "started_at": isoformat(started_at),
            "elapsed_seconds": round(elapsed_seconds, 1)
            if elapsed_seconds is not None
            else None,
            "estimated_total_seconds": (
                round(estimated_total_seconds, 1)
                if estimated_total_seconds is not None
                else None
            ),
            "estimated_finish_at": isoformat(estimated_finish_at),
        },
        "cost": {
            "hourly_usd": round(hourly_cost, 4) if hourly_cost is not None else None,
            "estimated_spend_usd": (
                round(hourly_cost * elapsed_seconds / 3600, 2)
                if hourly_cost is not None and elapsed_seconds is not None
                else None
            ),
            "estimated_total_usd": (
                round(hourly_cost * estimated_total_seconds / 3600, 2)
                if hourly_cost is not None and estimated_total_seconds is not None
                else None
            ),
            "basis": cost_basis,
        },
        "skypilot": {
            "resource_kind": "job",
            "job_id": job.job_id,
            "cluster_name": aws_cluster_name_from_job(job),
            "job_name": job.job_name,
            "resources": job.resources,
            "cloud": job.cloud,
            "region": job.region,
        },
        "storage": {"config_uri": None, "run_uri": None, "gcp_bucket": None},
        "links": {
            "wandb": None,
            "skypilot": f"{sky_server_common.get_server_url()}/dashboard/jobs/{job.job_id}",
            "zymtrace": zymtrace_url(zymtrace_project_id, job.job_name, started_at),
            "gcp_bucket": None,
        },
    }


def build_cluster_record(cluster: Any, zymtrace_project_id: str) -> dict[str, Any]:
    """Build a report row for a standalone SkyPilot cluster."""
    status = enum_value(cluster.status)
    started_at = timestamp_from_epoch(cluster.launched_at)
    elapsed_seconds = (
        (datetime.now(UTC) - started_at).total_seconds()
        if started_at is not None and status in ACTIVE_RESOURCE_STATUSES
        else None
    )
    try:
        hourly_cost = cluster.handle.launched_resources.get_cost(3600) * cluster.nodes
        cost_basis = f"Sky launched resources: {cluster.handle.launched_resources}; {cluster.nodes} node(s)"
    except (AssertionError, AttributeError, KeyError, ValueError):
        hourly_cost = None
        cost_basis = None
    return {
        "kind": "cluster",
        "name": cluster.name,
        "project": None,
        "user": cluster.user_name,
        "wandb_id": None,
        "submitted_at": isoformat(started_at),
        "status": {"wandb": None, "skypilot": status},
        "progress": {
            "batch": None,
            "batches_per_epoch": None,
            "completed_batches": None,
            "remaining_batches": None,
            "total_batches": None,
            "progress_fraction": None,
            "tokens_per_second": None,
            "tokens_per_second_ema_samples": None,
            "log_elapsed_seconds": None,
            "estimated_remaining_seconds": None,
            "display": "—",
            "collection_error": None,
        },
        "retries": {
            "preemption_or_infrastructure": None,
            "application_error": None,
            "total_recoveries": None,
            "collection_error": None,
        },
        "timing": {
            "started_at": isoformat(started_at),
            "elapsed_seconds": round(elapsed_seconds, 1)
            if elapsed_seconds is not None
            else None,
            "estimated_total_seconds": None,
            "estimated_finish_at": None,
        },
        "cost": {
            "hourly_usd": round(hourly_cost, 4) if hourly_cost is not None else None,
            "estimated_spend_usd": (
                round(hourly_cost * elapsed_seconds / 3600, 2)
                if hourly_cost is not None and elapsed_seconds is not None
                else None
            ),
            "estimated_total_usd": None,
            "basis": cost_basis,
        },
        "skypilot": {
            "resource_kind": "cluster",
            "job_id": None,
            "cluster_name": cluster.name,
            "job_name": cluster.name,
            "resources": cluster.resources_str,
            "cloud": cluster.cloud,
            "region": cluster.region,
        },
        "storage": {"config_uri": None, "run_uri": None, "gcp_bucket": None},
        "links": {
            "wandb": None,
            "skypilot": f"{sky_server_common.get_server_url()}/dashboard/clusters",
            "zymtrace": zymtrace_url(zymtrace_project_id, cluster.name, started_at),
            "gcp_bucket": None,
        },
    }
