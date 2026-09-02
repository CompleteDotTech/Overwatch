"""Standalone raw metric collector and versioned file cache.

This module owns every provider request made by Overwatch. It has no dependency
on the HTTP server or frontend and can be run directly for manual collection::

    uv run python -m overwatch.collector
    uv run python -m overwatch.collector --job-id 183 --full-logs
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import math
import os
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from itertools import islice
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

import boto3
import pyzstd
import sky
import sky.jobs
import wandb
from google.cloud import bigquery
from loguru import logger

from overwatch.constants import (
    ACTIVE_SKY_STATUSES,
    ANSI_ESCAPE_RE,
    BILLING_HISTORY_DAYS,
    CLOUDWATCH_LOG_GROUP,
    CLOUDWATCH_REGION,
    RESOURCE_HISTORY_DAYS,
    TERMINAL_SKY_STATUSES,
)
from overwatch.logs import (
    aws_cluster_name_from_job,
    cloudwatch_event_id,
    cloudwatch_event_message,
    cloudwatch_event_pid,
)
from overwatch.providers.wandb_flow import match_skypilot_jobs
from overwatch.utils import enum_value, isoformat

RAW_CACHE_SPEC_VERSION = 1
HUMAN_LOG_FORMAT_VERSION = 1
RAW_CACHE_ROOT = Path.home() / ".cache" / "overwatch" / "raw-v1"
CLOUDWATCH_EVENTS_FORMAT = "jsonl-zstd-frames-v1"
CLOUDWATCH_EVENTS_BLOCK_LINES = 1_000
CLOUDWATCH_EVENTS_BLOCK_BYTES = 1024 * 1024
_job_log_locks: dict[int, threading.Lock] = {}
_job_log_locks_guard = threading.Lock()


@dataclass(frozen=True)
class CollectorOptions:
    """Provider configuration for one raw collection pass."""

    limit: int = 20
    entity: str | None = None
    gcp_billing_table: str | None = None


def collect_managed_jobs() -> list[Any]:
    """Collect the unfiltered all-user SkyPilot managed-job response."""
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
    """Collect the unfiltered all-user SkyPilot cluster response."""
    return sky.get(sky.status(all_users=True))


async def collect_recent_flow_runs(
    api: wandb.Api, entity: str, limit: int
) -> list[Any]:
    """Collect raw hydrated W&B records for recent and running Flow runs."""
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
    recent_candidates = [
        run
        for project_runs_result in recent_by_project
        for run in project_runs_result
        if "/" in (run.name or "")
    ]
    running_candidates = [
        run
        for project_runs_result in running_by_project
        for run in project_runs_result
        if "/" in (run.name or "")
    ]
    recent_candidates.sort(key=lambda run: run.created_at or "", reverse=True)
    candidates_by_path = {
        "/".join(run.path): run
        for run in [*recent_candidates[: limit * 2], *running_candidates]
    }

    # Hydration preserves the raw config used to identify and render Flow runs.
    async def hydrate_run(run: Any) -> Any:
        async with semaphore:
            return await asyncio.to_thread(api.run, "/".join(run.path))

    hydrated_runs = await asyncio.gather(
        *(hydrate_run(run) for run in candidates_by_path.values())
    )
    flow_runs = [
        run for run in hydrated_runs if run.config.get("_id_") == "TrainConfig"
    ]
    flow_runs.sort(key=lambda run: run.created_at or "", reverse=True)
    selected_runs = flow_runs[:limit]
    selected_paths = {tuple(run.path) for run in selected_runs}
    selected_runs.extend(
        run
        for run in flow_runs
        if run.state == "running" and tuple(run.path) not in selected_paths
    )
    selected_runs.sort(key=lambda run: run.created_at or "", reverse=True)
    return selected_runs


def resolve_cloudwatch_stream_name(
    cloudwatch_client: Any, cluster_name: str
) -> str | None:
    """Resolve a job's newest raw CloudWatch stream inside the collector boundary."""
    response = cloudwatch_client.describe_log_streams(
        logGroupName=CLOUDWATCH_LOG_GROUP,
        logStreamNamePrefix=f"skypilot-{cluster_name}-",
        limit=50,
    )
    streams = response.get("logStreams", [])
    if not streams:
        return None
    return max(streams, key=lambda stream: stream.get("lastEventTimestamp", 0))[
        "logStreamName"
    ]


def raw_cache_path(*parts: str) -> Path:
    """Return a path inside the active raw-cache specification."""
    return RAW_CACHE_ROOT.joinpath(*parts)


def json_safe(value: Any) -> Any:
    """Preserve SDK data while converting it to JSON-compatible primitives."""
    if hasattr(value, "model_dump"):
        return json_safe(value.model_dump(mode="json"))
    if isinstance(value, dict):
        return {str(key): json_safe(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(child) for child in value]
    if isinstance(value, Enum):
        return json_safe(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def write_json_atomically(path: Path, value: Any) -> None:
    """Write one private JSON snapshot without exposing a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    temporary_path.write_text(
        json.dumps(json_safe(value), indent=2, sort_keys=True) + "\n"
    )
    temporary_path.chmod(0o600)
    temporary_path.replace(path)


def read_json(path: Path, default: Any = None) -> Any:
    """Read a cache file, returning a caller-provided value when absent."""
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return default


def cloudwatch_events_index_is_valid(
    events_path: Path, index: dict[str, Any]
) -> bool:
    """Check that an index describes every committed compressed byte."""
    return (
        index.get("format") == CLOUDWATCH_EVENTS_FORMAT
        and events_path.exists()
        and index.get("compressed_bytes") == events_path.stat().st_size
        and isinstance(index.get("blocks"), list)
    )


def cloudwatch_event_block_metadata(
    event_lines: list[bytes | str],
) -> dict[str, Any]:
    """Summarize one raw block for indexed time and process lookup."""
    event_keys = []
    process_ranges: dict[str, dict[str, int]] = {}
    for line in event_lines:
        try:
            event = json.loads(line)
            timestamp = int(event["timestamp"])
            event_keys.append((timestamp, cloudwatch_event_id(event)))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            continue
        pid = cloudwatch_event_pid(event)
        if pid is None:
            continue
        process_range = process_ranges.setdefault(
            str(pid), {"minimum_timestamp": timestamp, "maximum_timestamp": timestamp}
        )
        process_range["minimum_timestamp"] = min(
            process_range["minimum_timestamp"], timestamp
        )
        process_range["maximum_timestamp"] = max(
            process_range["maximum_timestamp"], timestamp
        )
    minimum_key = min(event_keys) if event_keys else None
    maximum_key = max(event_keys) if event_keys else None
    return {
        "minimum_cursor": (
            f"{minimum_key[0]}:{minimum_key[1]}" if minimum_key else None
        ),
        "maximum_cursor": (
            f"{maximum_key[0]}:{maximum_key[1]}" if maximum_key else None
        ),
        "processes": process_ranges,
    }


def enrich_cloudwatch_events_index(
    events_path: Path, index_path: Path, index: dict[str, Any]
) -> dict[str, Any]:
    """Add process metadata to indexes created before PID ranges were retained."""
    if all("processes" in block for block in index["blocks"]):
        return index
    with events_path.open("rb") as events_file:
        for block in index["blocks"]:
            if "processes" in block:
                continue
            events_file.seek(int(block["compressed_offset"]))
            compressed_block = events_file.read(int(block["compressed_size"]))
            event_lines = pyzstd.decompress(compressed_block).splitlines(
                keepends=True
            )
            block.update(cloudwatch_event_block_metadata(event_lines))
    write_json_atomically(index_path, index)
    return index


def append_cloudwatch_event_lines_to_zstd(
    events_path: Path,
    index_path: Path,
    event_lines: list[bytes],
) -> dict[str, Any]:
    """Append independently compressed, indexed JSONL frames."""
    index = read_json(
        index_path,
        {
            "format": CLOUDWATCH_EVENTS_FORMAT,
            "blocks": [],
            "line_count": 0,
            "uncompressed_bytes": 0,
            "compressed_bytes": 0,
        },
    )
    events_path.parent.mkdir(parents=True, exist_ok=True)
    committed_bytes = int(index.get("compressed_bytes") or 0)
    if events_path.exists() and events_path.stat().st_size > committed_bytes:
        # Discard an unindexed frame left by an interrupted append.
        with events_path.open("r+b") as events_file:
            events_file.truncate(committed_bytes)
    if events_path.exists() and events_path.stat().st_size != committed_bytes:
        raise ValueError(f"CloudWatch event index does not match {events_path}")

    def append_block(events_file: Any, lines: list[bytes]) -> None:
        raw_block = b"".join(lines)
        compressed_block = pyzstd.compress(raw_block, 3)
        compressed_offset = events_file.tell()
        events_file.write(compressed_block)
        index["blocks"].append(
            {
                "compressed_offset": compressed_offset,
                "compressed_size": len(compressed_block),
                "uncompressed_offset": index["uncompressed_bytes"],
                "uncompressed_size": len(raw_block),
                "first_line": index["line_count"],
                "line_count": len(lines),
                **cloudwatch_event_block_metadata(lines),
            }
        )
        index["line_count"] += len(lines)
        index["uncompressed_bytes"] += len(raw_block)
        index["compressed_bytes"] += len(compressed_block)

    # Bound both event count and decoded frame size for predictable random reads.
    block_lines: list[bytes] = []
    block_bytes = 0
    with events_path.open("ab") as events_file:
        for line in event_lines:
            if block_lines and (
                len(block_lines) >= CLOUDWATCH_EVENTS_BLOCK_LINES
                or block_bytes + len(line) > CLOUDWATCH_EVENTS_BLOCK_BYTES
            ):
                append_block(events_file, block_lines)
                block_lines = []
                block_bytes = 0
            block_lines.append(line)
            block_bytes += len(line)
        if block_lines:
            append_block(events_file, block_lines)
        events_file.flush()
        os.fsync(events_file.fileno())
    events_path.chmod(0o600)
    write_json_atomically(index_path, index)
    return index


def iter_cloudwatch_event_lines_from_file(path: Path) -> Iterator[str]:
    """Yield decoded lines from a legacy or Zstandard event cache."""
    if path.suffix == ".zst":
        with pyzstd.open(path, "rt") as events_file:
            yield from events_file
    elif path.suffix == ".gz":
        with gzip.open(path, "rt") as events_file:
            yield from events_file
    else:
        with path.open() as events_file:
            yield from events_file


def ensure_cloudwatch_zstd_cache(job_directory: Path) -> tuple[Path, Path]:
    """Migrate a legacy event stream to indexed Zstandard blocks once."""
    events_path = job_directory / "events.jsonl.zst"
    index_path = job_directory / "events.index.json"
    index = read_json(index_path, {})
    if (
        events_path.exists()
        and index.get("format") == CLOUDWATCH_EVENTS_FORMAT
        and isinstance(index.get("blocks"), list)
        and events_path.stat().st_size > int(index.get("compressed_bytes") or 0)
    ):
        # The cursor cannot reference an append that was not committed to the index.
        with events_path.open("r+b") as events_file:
            events_file.truncate(int(index.get("compressed_bytes") or 0))
    if cloudwatch_events_index_is_valid(events_path, index):
        enrich_cloudwatch_events_index(events_path, index_path, index)
        for legacy_path in (
            job_directory / "events.jsonl",
            job_directory / "events.jsonl.gz",
        ):
            legacy_path.unlink(missing_ok=True)
        return events_path, index_path

    source_path = next(
        (
            path
            for path in (
                events_path,
                job_directory / "events.jsonl",
                job_directory / "events.jsonl.gz",
            )
            if path.exists()
        ),
        None,
    )
    if source_path is None:
        return events_path, index_path

    temporary_events_path = job_directory / "events.jsonl.zst.migrating"
    temporary_index_path = job_directory / "events.index.json.migrating"
    temporary_events_path.unlink(missing_ok=True)
    temporary_index_path.unlink(missing_ok=True)
    buffered_lines = []
    for line in iter_cloudwatch_event_lines_from_file(source_path):
        buffered_lines.append(line.encode())
        if len(buffered_lines) >= CLOUDWATCH_EVENTS_BLOCK_LINES:
            append_cloudwatch_event_lines_to_zstd(
                temporary_events_path, temporary_index_path, buffered_lines
            )
            buffered_lines = []
    if buffered_lines:
        append_cloudwatch_event_lines_to_zstd(
            temporary_events_path, temporary_index_path, buffered_lines
        )
    if not temporary_events_path.exists():
        append_cloudwatch_event_lines_to_zstd(
            temporary_events_path, temporary_index_path, []
        )
    temporary_events_path.replace(events_path)
    temporary_index_path.replace(index_path)
    for legacy_path in (
        job_directory / "events.jsonl",
        job_directory / "events.jsonl.gz",
    ):
        legacy_path.unlink(missing_ok=True)
    return events_path, index_path


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


def collect_aws_billing_responses() -> dict[str, Any]:
    """Collect unmodified paginated Cost Explorer responses."""
    today = datetime.now(UTC).date()
    request_arguments: dict[str, Any] = {
        "TimePeriod": {
            "Start": (today - timedelta(days=BILLING_HISTORY_DAYS - 1)).isoformat(),
            "End": (today + timedelta(days=1)).isoformat(),
        },
        "Granularity": "DAILY",
        "Metrics": ["NetUnblendedCost"],
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
    }
    client = boto3.client("ce", region_name="us-east-1")
    responses = []
    next_page_token = None
    while True:
        page_arguments = dict(request_arguments)
        if next_page_token:
            page_arguments["NextPageToken"] = next_page_token
        response = client.get_cost_and_usage(**page_arguments)
        responses.append(response)
        next_page_token = response.get("NextPageToken")
        if not next_page_token:
            break
    return {"request": request_arguments, "responses": responses}


def collect_gcp_billing_rows(billing_table: str) -> dict[str, Any]:
    """Collect minimally aggregated raw BigQuery rows by project, service, and SKU."""
    today = datetime.now(UTC).date()
    start_date = today - timedelta(days=BILLING_HISTORY_DAYS - 1)
    end_date = today + timedelta(days=1)
    query = f"""
        SELECT
          DATE(usage_start_time) AS usage_date,
          project.id AS project_id,
          service.description AS service_description,
          sku.description AS sku_description,
          currency,
          CAST(SUM(cost) AS FLOAT64) AS cost,
          CAST(SUM(IFNULL((
            SELECT SUM(credit.amount) FROM UNNEST(credits) AS credit
          ), 0)) AS FLOAT64) AS credits,
          CAST(ANY_VALUE(currency_conversion_rate) AS FLOAT64) AS currency_conversion_rate
        FROM `{billing_table}`
        WHERE usage_start_time >= TIMESTAMP(@start_date)
          AND usage_start_time < TIMESTAMP(@end_date)
        GROUP BY usage_date, project_id, service_description, sku_description, currency
        ORDER BY usage_date, project_id, service_description, sku_description
    """
    parameters = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )
    rows = (
        bigquery.Client(project=billing_table.split(".", 1)[0])
        .query(query, job_config=job_config)
        .result()
    )
    return {
        "table": billing_table,
        "query": query,
        "parameters": parameters,
        "rows": [dict(row.items()) for row in rows],
    }


def format_cloudwatch_event_for_human_log(event: dict[str, Any]) -> str:
    """Render one raw event as timestamped, decoded plain log lines."""
    timestamp = datetime.fromtimestamp(int(event["timestamp"]) / 1000, UTC)
    timestamp_text = timestamp.isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )
    pid = cloudwatch_event_pid(event)
    prefix = f"{timestamp_text} pid={pid if pid is not None else '-'} | "
    message = ANSI_ESCAPE_RE.sub("", cloudwatch_event_message(event))
    message_lines = message.splitlines() or [""]
    return "\n".join(f"{prefix}{line}" for line in message_lines)


def append_cloudwatch_events(
    job: dict[str, Any], *, full_logs: bool = False
) -> dict[str, Any]:
    """Append only missing raw CloudWatch events to one job's local JSONL copy."""
    job_id = int(job["job_id"])
    with _job_log_locks_guard:
        job_lock = _job_log_locks.setdefault(job_id, threading.Lock())
    with job_lock:
        job_directory = raw_cache_path("jobs", str(job_id), "cloudwatch")
        events_path, events_index_path = ensure_cloudwatch_zstd_cache(job_directory)
        human_log_path = job_directory / "events.log"
        cursor_path = job_directory / "cursor.json"
        cursor = read_json(cursor_path, {})
        status = enum_value(job.get("status"))
        if (
            status in TERMINAL_SKY_STATUSES
            and cursor.get("settled")
            and (cursor.get("complete_from_head") or not full_logs)
        ):
            return cursor

        # Resolve the stable SkyPilot stream from the raw scheduler link.
        job_namespace = type("RawSkyJob", (), job)()
        cluster_name = aws_cluster_name_from_job(job_namespace)
        if str(job.get("cloud", "")).casefold() != "aws" or cluster_name is None:
            is_aws_job_waiting_for_cluster = (
                str(job.get("cloud", "")).casefold() == "aws"
                and cluster_name is None
            )
            cursor = {
                "available": False,
                "reason": (
                    "SkyPilot has not published the AWS cluster link yet"
                    if is_aws_job_waiting_for_cluster
                    else "CloudWatch is unavailable for this job"
                ),
                "complete_from_head": not is_aws_job_waiting_for_cluster,
                "settled": status in TERMINAL_SKY_STATUSES,
                "job_status": status,
                "updated_at": isoformat(datetime.now(UTC)),
            }
            write_json_atomically(cursor_path, cursor)
            return cursor
        client = boto3.client("logs", region_name=CLOUDWATCH_REGION)
        stream_name = resolve_cloudwatch_stream_name(client, cluster_name)
        if stream_name is None:
            cursor = {
                "available": False,
                "reason": "CloudWatch stream was not found",
                "complete_from_head": False,
                "settled": status in TERMINAL_SKY_STATUSES,
                "job_status": status,
                "updated_at": isoformat(datetime.now(UTC)),
            }
            write_json_atomically(cursor_path, cursor)
            return cursor
        if cursor.get("stream_name") != stream_name:
            cursor = {}

        job_directory.mkdir(parents=True, exist_ok=True)
        stream_response = client.describe_log_streams(
            logGroupName=CLOUDWATCH_LOG_GROUP,
            logStreamNamePrefix=stream_name,
            limit=1,
        )
        write_json_atomically(job_directory / "stream.json", stream_response)

        completing_history = full_logs and not cursor.get("complete_from_head")
        rebuild_human_log = (
            completing_history
            or not human_log_path.exists()
            or cursor.get("human_log_format_version") != HUMAN_LOG_FORMAT_VERSION
        )
        cached_events_by_id: dict[str, dict[str, Any]] = {}
        if (completing_history or rebuild_human_log) and events_path.exists():
            for line in iter_cloudwatch_event_lines_from_file(events_path):
                try:
                    event = json.loads(line)
                    cached_events_by_id[cloudwatch_event_id(event)] = event
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
        seen_event_ids = set(cached_events_by_id)

        # Resume interrupted head scans from their page token or contiguous cached prefix.
        request_arguments: dict[str, Any] = {
            "logGroupName": CLOUDWATCH_LOG_GROUP,
            "logStreamName": stream_name,
            "limit": 10_000,
        }
        backfill_scanned_events = int(cursor.get("backfill_scanned_events") or 0)
        if completing_history and cursor.get("backfill_next_token"):
            request_arguments["nextToken"] = cursor["backfill_next_token"]
        elif completing_history:
            stream_items = stream_response.get("logStreams", [])
            first_stream_timestamp = (
                int(stream_items[0].get("firstEventTimestamp") or 0)
                if stream_items
                else 0
            )
            cached_timestamps = [
                int(event["timestamp"]) for event in cached_events_by_id.values()
            ]
            has_contiguous_head_prefix = bool(
                cached_timestamps
                and first_stream_timestamp
                and min(cached_timestamps) <= first_stream_timestamp
            )
            request_arguments["startFromHead"] = True
            if has_contiguous_head_prefix:
                request_arguments["startTime"] = max(cached_timestamps)
                backfill_scanned_events = max(
                    backfill_scanned_events, len(cached_events_by_id)
                )
        elif cursor.get("next_forward_token"):
            request_arguments["nextToken"] = cursor["next_forward_token"]
        else:
            request_arguments["startFromHead"] = False

        appended_events = 0
        appended_event_values = []
        while True:
            response = client.get_log_events(**request_arguments)
            response_event_lines = []
            for event in response["events"]:
                event_id = cloudwatch_event_id(event)
                if event_id in seen_event_ids:
                    continue
                response_event_lines.append(
                    (
                        json.dumps(
                            json_safe(event),
                            separators=(",", ":"),
                            ensure_ascii=False,
                        )
                        + "\n"
                    ).encode()
                )
                seen_event_ids.add(event_id)
                cached_events_by_id[event_id] = event
                appended_event_values.append(event)
                appended_events += 1
            if response_event_lines:
                append_cloudwatch_event_lines_to_zstd(
                    events_path, events_index_path, response_event_lines
                )
            previous_token = request_arguments.get("nextToken")
            next_token = response["nextForwardToken"]
            if next_token == previous_token:
                break
            if completing_history:
                backfill_scanned_events += len(response["events"])
                write_json_atomically(
                    cursor_path,
                    {
                        "stream_name": stream_name,
                        "next_forward_token": cursor.get("next_forward_token"),
                        "backfill_in_progress": True,
                        "backfill_next_token": next_token,
                        "backfill_scanned_events": backfill_scanned_events,
                        "backfill_cached_events": len(seen_event_ids),
                        "backfill_appended_events": appended_events,
                        "complete_from_head": False,
                        "settled": status in TERMINAL_SKY_STATUSES,
                        "job_status": status,
                        "updated_at": isoformat(datetime.now(UTC)),
                        "total_bytes": (
                            events_path.stat().st_size if events_path.exists() else 0
                        ),
                        "events_file": events_path.name,
                        "events_index_file": events_index_path.name,
                        "human_events_file": human_log_path.name,
                        "human_log_format_version": HUMAN_LOG_FORMAT_VERSION,
                    },
                )
            request_arguments = {
                "logGroupName": CLOUDWATCH_LOG_GROUP,
                "logStreamName": stream_name,
                "nextToken": next_token,
                "limit": 10_000,
            }

        # Maintain a decoded sidecar for direct shell exploration.
        if rebuild_human_log:
            temporary_human_log_path = human_log_path.with_suffix(".log.tmp")
            with temporary_human_log_path.open("w") as human_log_file:
                for event in sorted(
                    cached_events_by_id.values(),
                    key=lambda value: (
                        int(value["timestamp"]),
                        int(value.get("ingestionTime") or 0),
                        cloudwatch_event_id(value),
                    ),
                ):
                    human_log_file.write(
                        format_cloudwatch_event_for_human_log(event) + "\n"
                    )
            temporary_human_log_path.chmod(0o600)
            temporary_human_log_path.replace(human_log_path)
        elif appended_event_values:
            with human_log_path.open("a") as human_log_file:
                for event in appended_event_values:
                    human_log_file.write(
                        format_cloudwatch_event_for_human_log(event) + "\n"
                    )
            human_log_path.chmod(0o600)

        cursor = {
            "stream_name": stream_name,
            "next_forward_token": response["nextForwardToken"],
            "complete_from_head": bool(
                cursor.get("complete_from_head") or completing_history
            ),
            "settled": status in TERMINAL_SKY_STATUSES,
            "job_status": status,
            "updated_at": isoformat(datetime.now(UTC)),
            "appended_events": appended_events,
            "total_bytes": events_path.stat().st_size if events_path.exists() else 0,
            "events_file": events_path.name,
            "events_index_file": events_index_path.name,
            "human_events_file": human_log_path.name,
            "human_log_format_version": HUMAN_LOG_FORMAT_VERSION,
        }
        write_json_atomically(cursor_path, cursor)

        return cursor


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

        # Routine collection advances only active job cursors and never scans history.
        active_jobs = [
            serialized_job
            for serialized_job in serialized_jobs
            if enum_value(serialized_job.get("status")) in ACTIVE_SKY_STATUSES
        ]
        cloudwatch_started = perf_counter()
        cloudwatch_results = await asyncio.gather(
            *(
                asyncio.to_thread(append_cloudwatch_events, job)
                for job in active_jobs
                if str(job.get("cloud", "")).casefold() == "aws"
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect raw Overwatch metrics")
    parser.add_argument("--job-id", type=int)
    parser.add_argument("--full-logs", action="store_true")
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
    )
    manifest = asyncio.run(
        collect_raw_metrics(options, job_id=args.job_id, full_logs=args.full_logs)
    )
    logger.success("Raw metrics cached at {}", RAW_CACHE_ROOT)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
