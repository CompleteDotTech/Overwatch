"""Authoritative SkyPilot cloud inventory provider."""

import time
from typing import Any

import sky
import sky.jobs

from overwatch.constants import (
    ACTIVE_RESOURCE_STATUSES,
    RESOURCE_HISTORY_DAYS,
    TERMINAL_SKY_STATUSES,
)
from overwatch.utils import enum_value


def collect_managed_jobs() -> list[Any]:
    """Collect active and recently completed all-user managed jobs."""
    fields = (
        "job_id",
        "job_name",
        "status",
        "resources",
        "submitted_at",
        "start_at",
        "end_at",
        "job_duration",
        "recovery_count",
        "last_recovered_at",
        "cloud",
        "region",
        "zone",
        "infra",
        "details",
        "failure_reason",
        "user_name",
        "metadata",
        "links",
    )
    result = sky.get(
        sky.jobs.queue_v2(
            refresh=False,
            all_users=True,
            limit=None,
            fields=fields,
            sort_by="submitted_at",
            sort_order="desc",
        )
    )
    jobs = result[0] if isinstance(result, tuple) else result
    history_cutoff = time.time() - RESOURCE_HISTORY_DAYS * 24 * 60 * 60
    return [
        job
        for job in jobs
        if enum_value(job.status) not in TERMINAL_SKY_STATUSES
        or getattr(job, "end_at", None) is None
        or job.end_at >= history_cutoff
    ]


def collect_standalone_clusters() -> list[Any]:
    """Collect active and recently stopped standalone clusters."""
    history_cutoff = time.time() - RESOURCE_HISTORY_DAYS * 24 * 60 * 60
    return [
        cluster
        for cluster in sky.get(sky.status(all_users=True))
        if not cluster.is_managed
        and (
            enum_value(cluster.status) in ACTIVE_RESOURCE_STATUSES
            or getattr(cluster, "status_updated_at", None) is None
            or cluster.status_updated_at >= history_cutoff
        )
    ]
