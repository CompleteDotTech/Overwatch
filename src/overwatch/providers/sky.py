"""Raw SkyPilot cloud inventory provider."""

from typing import Any

import sky
import sky.jobs


def collect_managed_jobs() -> list[Any]:
    """Collect the unfiltered all-user managed-job response."""
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
    return result[0] if isinstance(result, tuple) else result


def collect_standalone_clusters() -> list[Any]:
    """Collect the unfiltered all-user cluster response."""
    return sky.get(sky.status(all_users=True))
