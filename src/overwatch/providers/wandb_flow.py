"""Optional W&B enrichment for Flow training workloads."""

import asyncio
from itertools import islice
from typing import Any

import wandb

from overwatch.constants import ACTIVE_SKY_STATUSES, CLOUD_CONFIG_RE, HAIKU_SUFFIX_RE
from overwatch.utils import enum_value, parse_utc, timestamp_from_epoch


def metadata_for_run(run: Any) -> dict[str, Any]:
    """Read W&B metadata once; its property otherwise retries silent five-second failures."""
    metadata = run.metadata or {}
    if run._metadata is None:
        run._metadata = {}
    return metadata


def flow_config_uri_from_run(
    run: Any, config: dict[str, Any] | None = None
) -> str | None:
    config = config or run.config
    required_keys = ("base_output_dir", "project", "stage", "exp_name")
    if all(config.get(key) for key in required_keys):
        return (
            "/".join(str(config[key]).strip("/") for key in required_keys).replace(
                "gcs://", "gs://", 1
            )
            + "/train_config.yaml"
        )
    metadata = metadata_for_run(run)
    for argument in reversed(metadata.get("args", [])):
        if isinstance(argument, str) and CLOUD_CONFIG_RE.match(argument):
            return argument.replace("gcs://", "gs://", 1)
    return None


async def collect_recent_flow_runs(
    api: wandb.Api, entity: str, limit: int
) -> list[Any]:
    """Collect the last N Flow runs plus every running Flow run."""
    projects = await asyncio.to_thread(lambda: list(api.projects(entity)))
    semaphore = asyncio.Semaphore(16)

    async def project_runs(project: Any, *, running_only: bool) -> list[Any]:
        async with semaphore:
            return await asyncio.to_thread(
                lambda: list(
                    api.runs(
                        f"{entity}/{project.name}",
                        filters={"state": "running"} if running_only else None,
                        order="-created_at",
                        per_page=max(50, limit),
                    )
                    if running_only
                    else islice(
                        api.runs(
                            f"{entity}/{project.name}",
                            order="-created_at",
                            per_page=limit,
                        ),
                        limit,
                    )
                )
            )

    recent_by_project, running_by_project = await asyncio.gather(
        asyncio.gather(
            *(project_runs(project, running_only=False) for project in projects)
        ),
        asyncio.gather(
            *(project_runs(project, running_only=True) for project in projects)
        ),
    )
    recent_candidates: list[Any] = []
    for runs in recent_by_project:
        # Flow names are `<stage>/<experiment>`. Avoid W&B's metadata property here: on a
        # missing metadata file it silently waits five seconds for every bulk-listed run.
        recent_candidates.extend(run for run in runs if "/" in (run.name or ""))
    running_candidates = [
        run for runs in running_by_project for run in runs if "/" in (run.name or "")
    ]
    recent_candidates.sort(key=lambda run: run.created_at or "", reverse=True)
    candidates_by_path = {
        "/".join(run.path): run
        for run in [*recent_candidates[: limit * 2], *running_candidates]
    }

    # Bulk run listings omit config. Hydrate a bounded surplus, then identify Flow runs by the
    # TrainConfig marker so unrelated W&B projects cannot displace the requested last N runs.
    async def hydrate_run(run: Any) -> Any:
        async with semaphore:
            return await asyncio.to_thread(api.run, "/".join(run.path))

    hydrated = await asyncio.gather(
        *(hydrate_run(run) for run in candidates_by_path.values())
    )
    flow_runs = [run for run in hydrated if run.config.get("_id_") == "TrainConfig"]
    flow_runs.sort(key=lambda run: run.created_at or "", reverse=True)
    selected = flow_runs[:limit]
    selected_paths = {tuple(run.path) for run in selected}
    selected.extend(
        run
        for run in flow_runs
        if run.state == "running" and tuple(run.path) not in selected_paths
    )
    selected.sort(key=lambda run: run.created_at or "", reverse=True)
    return selected


def normalized_wandb_experiment_name(run: Any) -> str:
    experiment_name = (run.name or "").split("/", 1)[-1]
    return HAIKU_SUFFIX_RE.sub("", experiment_name)


def match_skypilot_job(
    run: Any, jobs: list[Any], excluded_job_ids: set[int]
) -> Any | None:
    experiment_name = normalized_wandb_experiment_name(run)
    candidates = [
        job
        for job in jobs
        if job.job_name == experiment_name and job.job_id not in excluded_job_ids
    ]
    if not candidates:
        return None

    # A resumed logical run can span multiple Sky jobs. Prefer jobs that advertise this exact
    # W&B run, then select the newest active incarnation rather than the original submission.
    exact_wandb_url_candidates = [
        job for job in candidates if run.url in (job.links or {}).values()
    ]
    if exact_wandb_url_candidates:
        return max(
            exact_wandb_url_candidates,
            key=lambda job: (
                enum_value(job.status) in ACTIVE_SKY_STATUSES,
                job.submitted_at or 0,
            ),
        )

    # SkyPilot is authoritative for live execution. If the external W&B link has not been
    # captured yet, still associate a same-name active job before considering historical jobs.
    active_candidates = [
        job for job in candidates if enum_value(job.status) in ACTIVE_SKY_STATUSES
    ]
    if active_candidates:
        return max(active_candidates, key=lambda job: job.submitted_at or 0)

    created_at = parse_utc(run.created_at)
    if created_at is None:
        return max(candidates, key=lambda job: job.submitted_at or 0)
    return min(
        candidates,
        key=lambda job: (
            abs((timestamp_from_epoch(job.submitted_at) - created_at).total_seconds())
            if job.submitted_at
            else float("inf")
        ),
    )


def match_skypilot_jobs(runs: list[Any], jobs: list[Any]) -> list[Any | None]:
    """Match at most one W&B run to each SkyPilot job."""
    matched_jobs: list[Any | None] = []
    claimed_job_ids: set[int] = set()
    for run in runs:
        job = match_skypilot_job(run, jobs, claimed_job_ids)
        matched_jobs.append(job)
        if job is not None:
            claimed_job_ids.add(job.job_id)
    return matched_jobs
