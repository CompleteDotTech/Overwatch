"""Read and normalize raw collector files without making provider requests."""

from __future__ import annotations

import gzip
import heapq
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from types import SimpleNamespace
from typing import Any

import pyzstd

from overwatch.collector import (
    RAW_CACHE_ROOT,
    cloudwatch_events_index_is_valid,
    raw_cache_path,
    read_json,
)
from overwatch.constants import (
    ACTIVE_RESOURCE_STATUSES,
    ACTIVE_SKY_STATUSES,
    BILLING_HISTORY_DAYS,
    RESOURCE_HISTORY_DAYS,
    TERMINAL_SKY_STATUSES,
)
from overwatch.logs import (
    ansi_log_text_to_safe_html,
    cloudwatch_event_id,
    cloudwatch_event_message,
    cloudwatch_event_pid,
    flow_progress_from_log_text,
)
from overwatch.utils import enum_value


class CachedRun(SimpleNamespace):
    """Attribute adapter for a raw cached W&B run."""


def iter_cached_cloudwatch_event_lines(
    job_id: int,
    *,
    minimum_timestamp: int | None = None,
    maximum_timestamp: int | None = None,
) -> Iterator[str]:
    """Yield raw event lines, skipping indexed frames outside a time range."""
    directory = raw_cache_path("jobs", str(job_id), "cloudwatch")
    events_path = directory / "events.jsonl.zst"
    index = read_json(directory / "events.index.json", {})
    if cloudwatch_events_index_is_valid(events_path, index):
        with events_path.open("rb") as events_file:
            for block in index["blocks"]:
                minimum_cursor = block.get("minimum_cursor")
                maximum_cursor = block.get("maximum_cursor")
                block_minimum_timestamp = (
                    int(minimum_cursor.partition(":")[0])
                    if minimum_cursor
                    else None
                )
                block_maximum_timestamp = (
                    int(maximum_cursor.partition(":")[0])
                    if maximum_cursor
                    else None
                )
                if (
                    minimum_timestamp is not None
                    and block_maximum_timestamp is not None
                    and block_maximum_timestamp < minimum_timestamp
                ):
                    continue
                if (
                    maximum_timestamp is not None
                    and block_minimum_timestamp is not None
                    and block_minimum_timestamp > maximum_timestamp
                ):
                    continue
                events_file.seek(int(block["compressed_offset"]))
                compressed_block = events_file.read(int(block["compressed_size"]))
                yield from pyzstd.decompress(compressed_block).decode().splitlines(
                    keepends=True
                )
        return
    if events_path.exists():
        with pyzstd.open(events_path, "rt") as events_file:
            yield from events_file
        return
    legacy_path = directory / "events.jsonl"
    if legacy_path.exists():
        with legacy_path.open() as events_file:
            yield from events_file
        return
    compressed_legacy_path = directory / "events.jsonl.gz"
    if compressed_legacy_path.exists():
        with gzip.open(compressed_legacy_path, "rt") as events_file:
            yield from events_file


def cached_sky_jobs() -> list[SimpleNamespace]:
    """Load raw Sky job dictionaries as attribute-addressable records."""
    history_cutoff = datetime.now(UTC).timestamp() - RESOURCE_HISTORY_DAYS * 86400
    jobs = [
        SimpleNamespace(**job)
        for job in read_json(raw_cache_path("global", "sky", "jobs.json"), [])
        if enum_value(job.get("status")) not in TERMINAL_SKY_STATUSES
        or job.get("end_at") is None
        or float(job["end_at"]) >= history_cutoff
    ]
    return sorted(
        jobs,
        key=lambda job: (
            enum_value(job.status) != "RUNNING",
            enum_value(job.status) not in ACTIVE_SKY_STATUSES,
            -(getattr(job, "submitted_at", None) or 0),
        ),
    )


def cached_sky_clusters() -> list[SimpleNamespace]:
    """Load raw Sky cluster dictionaries as attribute-addressable records."""
    history_cutoff = datetime.now(UTC).timestamp() - RESOURCE_HISTORY_DAYS * 86400
    return [
        SimpleNamespace(**cluster)
        for cluster in read_json(raw_cache_path("global", "sky", "clusters.json"), [])
        if not cluster.get("is_managed", False)
        and (
            enum_value(cluster.get("status")) in ACTIVE_RESOURCE_STATUSES
            or cluster.get("status_updated_at") is None
            or float(cluster["status_updated_at"]) >= history_cutoff
        )
    ]


def cached_wandb_runs() -> list[CachedRun]:
    """Load raw W&B files through the interface used by report construction."""
    runs = []
    for value in read_json(raw_cache_path("global", "wandb", "runs.json"), []):
        runs.append(
            CachedRun(
                **value,
                _metadata=value.get("metadata") or {},
            )
        )
    return runs


def billing_category(service: str, sku: str = "") -> str:
    """Classify cached provider billing rows for the UI's major-category chart."""
    normalized = f"{service} {sku}".casefold()
    if any(
        keyword in normalized
        for keyword in ("storage", "disk", "snapshot", "filestore", "backup", "archive", "s3", "fsx")
    ):
        return "storage"
    if any(
        keyword in normalized
        for keyword in (
            "compute engine",
            "elastic compute cloud",
            "ec2",
            "vertex ai",
            "kubernetes",
            "cloud run",
            "cloud function",
            "lambda",
            "sagemaker",
            "elastic container",
            "batch",
            "cpu",
            "gpu",
            "tpu",
            "instance",
            "core running",
            "ram running",
        )
    ):
        return "compute"
    return "everything_else"


def cached_billing_report(manifest: dict[str, Any]) -> dict[str, Any]:
    """Normalize raw Cost Explorer and BigQuery response files for charting."""
    today = datetime.now(UTC).date()
    start_date = today - timedelta(days=BILLING_HISTORY_DAYS - 1)
    aws_raw = read_json(raw_cache_path("global", "billing", "aws.json"))
    gcp_raw = read_json(raw_cache_path("global", "billing", "gcp.json"))
    warnings = []
    sources = manifest.get("sources", {})
    for key, label in (("aws_billing", "AWS"), ("gcp_billing", "GCP")):
        if sources.get(key, {}).get("error"):
            warnings.append(f"Could not collect {label} billing history: {sources[key]['error']}")

    # Reduce raw AWS service groups only while constructing the presentation report.
    aws_daily: dict[str, float] | None = {} if aws_raw else None
    aws_categories: dict[str, dict[str, float]] = {}
    for response in (aws_raw or {}).get("responses", []):
        for period in response.get("ResultsByTime", []):
            usage_date = period["TimePeriod"]["Start"]
            aws_daily.setdefault(usage_date, 0.0)
            aws_categories.setdefault(
                usage_date,
                {"compute": 0.0, "storage": 0.0, "everything_else": 0.0},
            )
            for group in period.get("Groups", []):
                amount = float(group["Metrics"]["NetUnblendedCost"]["Amount"])
                aws_daily[usage_date] += amount
                aws_categories[usage_date][billing_category(group["Keys"][0])] += amount

    # Preserve a separate line per GCP project from the raw service/SKU rows.
    gcp_projects: dict[str, dict[str, float]] | None = {} if gcp_raw else None
    gcp_categories: dict[str, dict[str, float]] = {}
    for row in (gcp_raw or {}).get("rows", []):
        usage_date = str(row["usage_date"])
        project_id = row.get("project_id") or "Unattributed GCP"
        conversion_rate = float(row.get("currency_conversion_rate") or 1)
        amount = (float(row.get("cost") or 0) + float(row.get("credits") or 0)) / conversion_rate
        gcp_projects.setdefault(project_id, {}).setdefault(usage_date, 0.0)
        gcp_projects[project_id][usage_date] += amount
        gcp_categories.setdefault(
            usage_date,
            {"compute": 0.0, "storage": 0.0, "everything_else": 0.0},
        )
        category = billing_category(
            row.get("service_description") or "", row.get("sku_description") or ""
        )
        gcp_categories[usage_date][category] += amount

    aws_total = sum((aws_daily or {}).values()) if aws_daily is not None else None
    gcp_total = (
        sum(sum(costs.values()) for costs in (gcp_projects or {}).values())
        if gcp_projects is not None
        else None
    )
    gcp_series = [
        {
            "key": f"gcp_{index}",
            "label": project,
            "provider": "gcp",
            "total": round(sum(costs.values()), 2),
            "costs": costs,
        }
        for index, (project, costs) in enumerate(sorted((gcp_projects or {}).items()))
    ]
    combined_total = (
        sum(total for total in (aws_total, gcp_total) if total is not None)
        if aws_total is not None or gcp_total is not None
        else None
    )
    series = [
        {"key": "aws", "label": "AWS", "provider": "aws", "total": round(aws_total, 2) if aws_total is not None else None},
        *({key: value for key, value in item.items() if key != "costs"} for item in gcp_series),
        {"key": "combined", "label": "Combined", "provider": "combined", "total": round(combined_total, 2) if combined_total is not None else None},
    ]

    daily = []
    category_daily = []
    for offset in range(BILLING_HISTORY_DAYS):
        usage_date = (start_date + timedelta(days=offset)).isoformat()
        aws_spend = aws_daily.get(usage_date, 0.0) if aws_daily is not None else None
        row: dict[str, Any] = {"date": usage_date, "aws": round(aws_spend, 2) if aws_spend is not None else None}
        combined_values = [aws_spend] if aws_spend is not None else []
        for item in gcp_series:
            project_spend = item["costs"].get(usage_date, 0.0)
            row[item["key"]] = round(project_spend, 2)
            combined_values.append(project_spend)
        row["combined"] = round(sum(combined_values), 2) if combined_values else None
        daily.append(row)
        category_row: dict[str, Any] = {"date": usage_date}
        for provider, categories in (("aws", aws_categories), ("gcp", gcp_categories)):
            for category in ("compute", "storage", "everything_else"):
                category_row[f"{provider}_{category}"] = (
                    round(categories.get(usage_date, {}).get(category, 0.0), 2)
                    if (aws_daily if provider == "aws" else gcp_projects) is not None
                    else None
                )
        category_daily.append(category_row)
    totals = {}
    if aws_total is not None:
        totals["aws"] = round(aws_total, 2)
    if gcp_total is not None:
        totals["gcp"] = round(gcp_total, 2)
    if combined_total is not None:
        totals["combined"] = round(combined_total, 2)
    return {
        "start_date": start_date.isoformat(),
        "end_date": today.isoformat(),
        "currency": "USD",
        "daily": daily,
        "series": series,
        "category_daily": category_daily,
        "totals": totals,
        "gcp_configured": (
            gcp_raw is not None
            or (
                "gcp_billing" in sources
                and sources["gcp_billing"].get("status") != "skipped"
            )
        ),
        "warnings": warnings,
    }


def cached_cloudwatch_progress(job_id: int) -> tuple[dict[str, Any] | None, str | None]:
    """Parse training progress from the latest timestamped cached messages."""
    latest_messages: list[tuple[int, str]] = []
    for line in iter_cached_cloudwatch_event_lines(job_id):
        try:
            event = json.loads(line)
            message = cloudwatch_event_message(event)
            timestamp = int(event["timestamp"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue
        if "tok/s" in message:
            if len(latest_messages) < 1_000:
                heapq.heappush(latest_messages, (timestamp, message))
            else:
                heapq.heappushpop(latest_messages, (timestamp, message))
    if not latest_messages:
        return None, "no Flow training progress line found in cached CloudWatch events"
    return flow_progress_from_log_text(
        "\n".join(message for _timestamp, message in sorted(latest_messages))
    )


def cached_cloudwatch_attempts(
    job_id: int, expected_attempts: int
) -> list[dict[str, Any]]:
    """Derive process attempts with memory proportional only to attempt count."""
    first_event_timestamp: int | None = None
    process_ranges: dict[int, dict[str, int]] = {}
    directory = raw_cache_path("jobs", str(job_id), "cloudwatch")
    events_path = directory / "events.jsonl.zst"
    index = read_json(directory / "events.index.json", {})
    indexed_processes_available = (
        cloudwatch_events_index_is_valid(events_path, index)
        and all("processes" in block for block in index["blocks"])
    )
    if indexed_processes_available:
        for block in index["blocks"]:
            minimum_cursor = block.get("minimum_cursor")
            if minimum_cursor:
                timestamp = int(minimum_cursor.partition(":")[0])
                first_event_timestamp = (
                    timestamp
                    if first_event_timestamp is None
                    else min(first_event_timestamp, timestamp)
                )
            for pid_text, block_process_range in block["processes"].items():
                pid = int(pid_text)
                process_range = process_ranges.setdefault(
                    pid,
                    {
                        "started_at": block_process_range["minimum_timestamp"],
                        "tail_at": block_process_range["maximum_timestamp"],
                    },
                )
                process_range["started_at"] = min(
                    process_range["started_at"],
                    block_process_range["minimum_timestamp"],
                )
                process_range["tail_at"] = max(
                    process_range["tail_at"],
                    block_process_range["maximum_timestamp"],
                )
    else:
        for line in iter_cached_cloudwatch_event_lines(job_id):
            try:
                event = json.loads(line)
                timestamp = int(event["timestamp"])
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
            first_event_timestamp = (
                timestamp
                if first_event_timestamp is None
                else min(first_event_timestamp, timestamp)
            )
            pid = cloudwatch_event_pid(event)
            if pid is None:
                continue
            process_range = process_ranges.setdefault(
                pid, {"started_at": timestamp, "tail_at": timestamp}
            )
            process_range["started_at"] = min(
                process_range["started_at"], timestamp
            )
            process_range["tail_at"] = max(process_range["tail_at"], timestamp)

    attempts = []
    ordered_processes = sorted(
        process_ranges.items(), key=lambda item: item[1]["started_at"]
    )
    for index, (pid, process_range) in enumerate(ordered_processes):
        attempts.append(
            {
                "attempt": index + 1,
                "pid": pid,
                "started_at": process_range["started_at"],
                "scan_start_at": (
                    first_event_timestamp
                    if index == 0
                    else attempts[index - 1]["tail_at"] + 1
                ),
                "scan_end_at": None,
                "tail_at": process_range["tail_at"],
                "expected_attempts": expected_attempts,
            }
        )
    for attempt, next_attempt in pairwise(attempts):
        attempt["scan_end_at"] = next_attempt["scan_start_at"]
    return attempts


def cached_cloudwatch_log_page(
    job_id: int,
    *,
    pid: int | None = None,
    started_after: int | None = None,
    ended_before: int | None = None,
    before: str | None = None,
    after: str | None = None,
    from_start: bool = False,
    limit: int = 1_000,
) -> dict[str, Any]:
    """Stream one bounded page of cached events for the browser log viewer."""
    def parse_event_cursor(cursor: str | None) -> tuple[int, str] | None:
        if not cursor:
            return None
        timestamp_text, separator, event_id = cursor.partition(":")
        if separator:
            try:
                return int(timestamp_text), event_id
            except ValueError:
                return None
        return None

    before_key = parse_event_cursor(before)
    after_key = parse_event_cursor(after)

    def matching_events_from_lines(lines: Iterator[str]) -> Iterator[dict[str, Any]]:
        for line in lines:
            try:
                event = json.loads(line)
                timestamp = int(event["timestamp"])
                event_id = cloudwatch_event_id(event)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
            if started_after is not None and timestamp < started_after:
                continue
            if ended_before is not None and timestamp >= ended_before:
                continue
            if pid is not None and cloudwatch_event_pid(event) != pid:
                continue
            if before_key is not None and (timestamp, event_id) >= before_key:
                continue
            if after_key is not None and (timestamp, event_id) <= after_key:
                continue
            yield event

    def cloudwatch_event_order(event: dict[str, Any]) -> tuple[int, str]:
        return int(event["timestamp"]), cloudwatch_event_id(event)

    minimum_timestamp = started_after
    if after_key is not None:
        minimum_timestamp = max(minimum_timestamp or after_key[0], after_key[0])
    maximum_timestamp = ended_before - 1 if ended_before is not None else None
    if before_key is not None:
        maximum_timestamp = min(maximum_timestamp or before_key[0], before_key[0])

    reading_forward = after_key is not None or from_start
    directory = raw_cache_path("jobs", str(job_id), "cloudwatch")
    events_path = directory / "events.jsonl.zst"
    index = read_json(directory / "events.index.json", {})
    if cloudwatch_events_index_is_valid(events_path, index):
        # Traverse frames by event range and stop once untouched frames cannot win.
        blocks = []
        for block in index["blocks"]:
            minimum_cursor = parse_event_cursor(block.get("minimum_cursor"))
            maximum_cursor = parse_event_cursor(block.get("maximum_cursor"))
            if minimum_cursor is None or maximum_cursor is None:
                continue
            if (
                minimum_timestamp is not None
                and maximum_cursor[0] < minimum_timestamp
            ):
                continue
            if (
                maximum_timestamp is not None
                and minimum_cursor[0] > maximum_timestamp
            ):
                continue
            blocks.append((minimum_cursor, maximum_cursor, block))
        blocks.sort(
            key=lambda item: item[0] if reading_forward else item[1],
            reverse=not reading_forward,
        )
        page_events = []
        with events_path.open("rb") as events_file:
            for block_index, (_minimum_cursor, _maximum_cursor, block) in enumerate(
                blocks
            ):
                events_file.seek(int(block["compressed_offset"]))
                compressed_block = events_file.read(int(block["compressed_size"]))
                block_lines = iter(
                    pyzstd.decompress(compressed_block).decode().splitlines(
                        keepends=True
                    )
                )
                page_events.extend(matching_events_from_lines(block_lines))
                page_events = (
                    heapq.nsmallest(
                        limit + 1, page_events, key=cloudwatch_event_order
                    )
                    if reading_forward
                    else heapq.nlargest(
                        limit + 1, page_events, key=cloudwatch_event_order
                    )
                )
                if len(page_events) <= limit or block_index + 1 >= len(blocks):
                    continue
                next_minimum_cursor, next_maximum_cursor, _next_block = blocks[
                    block_index + 1
                ]
                boundary = cloudwatch_event_order(page_events[-1])
                if (
                    reading_forward and next_minimum_cursor > boundary
                ) or (
                    not reading_forward and next_maximum_cursor < boundary
                ):
                    break
    else:
        # Legacy streams lack frame ranges and require one bounded full scan.
        matching_events = matching_events_from_lines(
            iter_cached_cloudwatch_event_lines(
                job_id,
                minimum_timestamp=minimum_timestamp,
                maximum_timestamp=maximum_timestamp,
            )
        )
        page_events = (
            heapq.nsmallest(limit + 1, matching_events, key=cloudwatch_event_order)
            if reading_forward
            else heapq.nlargest(
                limit + 1, matching_events, key=cloudwatch_event_order
            )
        )

    has_older = after_key is not None or (not reading_forward and len(page_events) > limit)
    has_newer = before_key is not None or (reading_forward and len(page_events) > limit)
    events = sorted(
        page_events[:limit],
        key=lambda event: (int(event["timestamp"]), cloudwatch_event_id(event)),
    )
    items = [
        {
            "id": cloudwatch_event_id(event),
            "timestamp": int(event["timestamp"]),
            "html": ansi_log_text_to_safe_html(cloudwatch_event_message(event)),
        }
        for event in events
    ]
    return {
        "events": items,
        "has_older": has_older,
        "has_newer": has_newer,
        "oldest_cursor": (
            f'{items[0]["timestamp"]}:{items[0]["id"]}' if items else None
        ),
        "newest_cursor": (
            f'{items[-1]["timestamp"]}:{items[-1]["id"]}' if items else None
        ),
    }


def raw_cache_manifest() -> dict[str, Any]:
    """Load the collector manifest used for query status and cache provenance."""
    return read_json(raw_cache_path("manifest.json"), {"sources": {}, "cache_root": str(RAW_CACHE_ROOT)})
