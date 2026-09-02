"""Durable, indexed CloudWatch log cache."""

from __future__ import annotations

import gzip
import json
import os
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import pyzstd

from overwatch.constants import (
    ANSI_ESCAPE_RE,
    CLOUDWATCH_LOG_GROUP,
    CLOUDWATCH_REGION,
    TERMINAL_SKY_STATUSES,
)
from overwatch.logs import (
    aws_cluster_name_from_job,
    cloudwatch_event_id,
    cloudwatch_event_message,
    cloudwatch_event_pid,
    cloudwatch_message_has_application_failure,
    flow_references_from_cloudwatch_message,
)
from overwatch.raw_cache import (
    cache_train_config,
    json_safe,
    raw_cache_path,
    read_json,
    write_json_atomically,
)
from overwatch.utils import enum_value, isoformat

HUMAN_LOG_FORMAT_VERSION = 1
CLOUDWATCH_EVENTS_FORMAT = "jsonl-zstd-frames-v1"
CLOUDWATCH_EVENTS_BLOCK_LINES = 1_000
CLOUDWATCH_EVENTS_BLOCK_BYTES = 1024 * 1024
_job_log_locks: dict[int, threading.Lock] = {}
_job_log_locks_guard = threading.Lock()


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
        if cloudwatch_message_has_application_failure(cloudwatch_event_message(event)):
            process_range["application_failure"] = True
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
    """Add process and failure metadata to older block indexes."""
    if all(block.get("process_metadata_version") == 2 for block in index["blocks"]):
        return index
    with events_path.open("rb") as events_file:
        for block in index["blocks"]:
            if block.get("process_metadata_version") == 2:
                continue
            events_file.seek(int(block["compressed_offset"]))
            compressed_block = events_file.read(int(block["compressed_size"]))
            event_lines = pyzstd.decompress(compressed_block).splitlines(
                keepends=True
            )
            block.update(cloudwatch_event_block_metadata(event_lines))
            block["process_metadata_version"] = 2
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
                "process_metadata_version": 2,
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

        # Cache the raw TrainConfig as soon as its URI appears in CloudWatch.
        if not raw_cache_path("jobs", str(job_id), "train_config.yaml").exists():
            references = {}
            for event in appended_event_values:
                references.update(
                    flow_references_from_cloudwatch_message(
                        cloudwatch_event_message(event)
                    )
                )
            if not references.get("config_uri") and events_path.exists():
                for line in iter_cloudwatch_event_lines_from_file(events_path):
                    try:
                        event = json.loads(line)
                        references.update(
                            flow_references_from_cloudwatch_message(
                                cloudwatch_event_message(event)
                            )
                        )
                    except (json.JSONDecodeError, KeyError, TypeError):
                        continue
            if config_uri := references.get("config_uri"):
                cache_train_config(job_id, config_uri)
        write_json_atomically(cursor_path, cursor)

        return cursor
