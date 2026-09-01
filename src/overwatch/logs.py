"""CloudWatch and SkyPilot log collection and parsing."""

import asyncio
import html
import json
import re
from concurrent.futures import ThreadPoolExecutor
from itertools import pairwise
from typing import Any
from urllib.parse import unquote

from botocore.exceptions import ClientError

from overwatch.constants import (
    ANSI_COLORS,
    ANSI_ESCAPE_RE,
    ANSI_SGR_RE,
    AWS_CLUSTER_LINK_RE,
    CLOUDWATCH_LOG_GROUP,
    TOKENS_PER_SECOND_EMA_ALPHA,
    TRAIN_PROGRESS_RE,
)
from overwatch.utils import seconds_from_duration

CLOUDWATCH_PID_RE = re.compile(r"\bpid=(?P<pid>\d+)\b")


def aws_cluster_name_from_job(job: Any) -> str | None:
    for url in (job.links or {}).values():
        if match := AWS_CLUSTER_LINK_RE.search(url):
            return unquote(match.group(1))
    return None


def cloudwatch_event_message(event: dict[str, Any]) -> str:
    message = event["message"]
    try:
        structured_message = json.loads(message)
    except json.JSONDecodeError:
        return message
    if isinstance(structured_message, dict):
        for field in ("message", "log", "log_line"):
            if isinstance(structured_message.get(field), str):
                return structured_message[field]
    return message


def cloudwatch_event_pid(event: dict[str, Any]) -> int | None:
    """Return the workload process ID attached to a structured log event."""
    message = event["message"]
    try:
        structured_message = json.loads(message)
    except json.JSONDecodeError:
        structured_message = None
    if isinstance(structured_message, dict):
        pid = structured_message.get("pid")
        if isinstance(pid, int):
            return pid
    match = CLOUDWATCH_PID_RE.search(message)
    return int(match.group("pid")) if match else None


def ansi_log_text_to_safe_html(log_text: str) -> str:
    """Render common ANSI foreground colors while escaping log-provided markup."""
    parts = []
    position = 0
    foreground: str | None = None
    bold = False
    span_open = False
    for match in ANSI_SGR_RE.finditer(log_text):
        parts.append(
            html.escape(ANSI_ESCAPE_RE.sub("", log_text[position : match.start()]))
        )
        if span_open:
            parts.append("</span>")
            span_open = False
        parameters = [int(value) if value else 0 for value in match.group(1).split(";")]
        for parameter in parameters:
            if parameter == 0:
                foreground = None
                bold = False
            elif parameter == 1:
                bold = True
            elif parameter == 22:
                bold = False
            elif 30 <= parameter <= 37:
                foreground = ANSI_COLORS[parameter - 30]
            elif 90 <= parameter <= 97:
                foreground = ANSI_COLORS[parameter - 90 + 8]
            elif parameter == 39:
                foreground = None
        styles = []
        if foreground:
            styles.append(f"color:{foreground}")
        if bold:
            styles.append("font-weight:700")
        if styles:
            parts.append(f'<span style="{";".join(styles)}">')
            span_open = True
        position = match.end()
    parts.append(html.escape(ANSI_ESCAPE_RE.sub("", log_text[position:])))
    if span_open:
        parts.append("</span>")
    return "".join(parts)


def flow_progress_from_log_text(
    log_text: str,
) -> tuple[dict[str, Any] | None, str | None]:
    matches = list(TRAIN_PROGRESS_RE.finditer(ANSI_ESCAPE_RE.sub("", log_text)))
    if not matches:
        return None, "no Flow training progress line found in workload log"
    values = matches[-1].groupdict()
    token_rates = [
        int(match.group("tokens_per_second").replace(",", "")) for match in matches
    ]
    tokens_per_second_ema = float(token_rates[0])
    for token_rate in token_rates[1:]:
        tokens_per_second_ema += TOKENS_PER_SECOND_EMA_ALPHA * (
            token_rate - tokens_per_second_ema
        )
    return {
        "epoch": int(values["epoch"]),
        "n_epochs": int(values["n_epochs"]),
        "batch": int(values["batch"]),
        "batches_per_epoch": int(values["batches_per_epoch"]),
        "completed_batches": int(values["completed"]),
        "total_batches": int(values["total"]),
        "elapsed_seconds": seconds_from_duration(values["elapsed"]),
        "remaining_seconds": seconds_from_duration(values["remaining"]),
        "tokens_per_second": round(tokens_per_second_ema),
        "tokens_per_second_ema_samples": len(token_rates),
    }, None


def resolve_cloudwatch_stream_name(
    cloudwatch_client: Any, cluster_name: str
) -> str | None:
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


def discover_cloudwatch_attempts(
    cloudwatch_client: Any,
    stream_name: str,
    expected_attempts: int,
) -> list[dict[str, Any]]:
    """Discover contiguous workload attempts without scanning the whole stream."""
    response = cloudwatch_client.describe_log_streams(
        logGroupName=CLOUDWATCH_LOG_GROUP,
        logStreamNamePrefix=stream_name,
        limit=1,
    )
    stream = next(
        (
            item
            for item in response.get("logStreams", [])
            if item["logStreamName"] == stream_name
        ),
        None,
    )
    if stream is None or stream.get("firstEventTimestamp") is None:
        return []

    first_timestamp = int(stream["firstEventTimestamp"])
    last_timestamp = int(stream.get("lastEventTimestamp", first_timestamp))

    # Random access by timestamp lets us locate process transitions in logarithmic requests.
    def event_at_or_after(timestamp: int) -> tuple[int, int] | None:
        events = cloudwatch_client.get_log_events(
            logGroupName=CLOUDWATCH_LOG_GROUP,
            logStreamName=stream_name,
            startTime=timestamp,
            endTime=last_timestamp + 1,
            startFromHead=True,
            limit=100,
        )["events"]
        for event in events:
            if (pid := cloudwatch_event_pid(event)) is not None:
                return int(event["timestamp"]), pid
        return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        endpoints = list(
            executor.map(event_at_or_after, (first_timestamp, last_timestamp))
        )
    samples = {
        timestamp: pid
        for endpoint in endpoints
        if endpoint
        for timestamp, pid in [endpoint]
    }
    if not samples:
        return []

    # Split intervals whose endpoint PIDs differ until every transition is tightly bounded.
    for _ in range(32):
        ordered_samples = sorted(samples.items())
        discovered_processes = set(samples.values())
        intervals = [
            (left_timestamp, right_timestamp)
            for (left_timestamp, left_pid), (right_timestamp, right_pid) in pairwise(
                ordered_samples
            )
            if left_pid != right_pid
            and (
                len(discovered_processes) < expected_attempts
                or right_timestamp - left_timestamp > 3_600_000
            )
        ]
        if not intervals:
            break
        targets = sorted(
            {
                left_timestamp + (right_timestamp - left_timestamp) // 2
                for left_timestamp, right_timestamp in intervals
            }
        )
        with ThreadPoolExecutor(max_workers=min(8, len(targets))) as executor:
            new_samples = list(executor.map(event_at_or_after, targets))
        added_sample = False
        for sample in new_samples:
            if sample is not None and sample[0] not in samples:
                samples[sample[0]] = sample[1]
                added_sample = True
        if not added_sample:
            break

    # Collapse ordered samples into attempt windows with padding around transitions.
    ordered_samples = sorted(samples.items())
    process_runs: list[dict[str, int]] = []
    for sample_index, (_, pid) in enumerate(ordered_samples):
        if not process_runs or process_runs[-1]["pid"] != pid:
            process_runs.append(
                {"pid": pid, "first_index": sample_index, "last_index": sample_index}
            )
        else:
            process_runs[-1]["last_index"] = sample_index
    attempts = []
    for index, process_run in enumerate(process_runs):
        pid = process_run["pid"]
        first_sample_index = process_run["first_index"]
        last_sample_index = process_run["last_index"]
        scan_start_at = (
            ordered_samples[first_sample_index - 1][0]
            if first_sample_index > 0
            else first_timestamp
        )
        scan_end_at = (
            ordered_samples[last_sample_index + 1][0]
            if last_sample_index + 1 < len(ordered_samples)
            else None
        )
        attempts.append(
            {
                "attempt": index + 1,
                "pid": pid,
                "started_at": ordered_samples[first_sample_index][0],
                "scan_start_at": scan_start_at,
                "scan_end_at": scan_end_at,
                "expected_attempts": expected_attempts,
            }
        )
    return attempts


async def tail_cloudwatch_job_log(
    job: Any, cloudwatch_client: Any, semaphore: asyncio.Semaphore
) -> tuple[str | None, str | None]:
    cluster_name = aws_cluster_name_from_job(job)
    if str(job.cloud).lower() != "aws" or cluster_name is None:
        return None, "CloudWatch stream is unavailable for this job"
    async with semaphore:
        try:
            stream_name = await asyncio.to_thread(
                resolve_cloudwatch_stream_name, cloudwatch_client, cluster_name
            )
            if stream_name is None:
                return None, f"no CloudWatch stream found for {cluster_name}"

            # Tail the stream once and filter locally to avoid scanning a long CloudWatch window.
            def collect_latest_progress_events() -> list[dict[str, Any]]:
                response = cloudwatch_client.get_log_events(
                    logGroupName=CLOUDWATCH_LOG_GROUP,
                    logStreamName=stream_name,
                    startFromHead=False,
                    limit=10_000,
                )
                return [
                    event
                    for event in response["events"]
                    if "tok/s" in cloudwatch_event_message(event)
                ][-1_000:]

            events = await asyncio.to_thread(collect_latest_progress_events)
        except ClientError as error:
            return None, str(error)
    return "\n".join(cloudwatch_event_message(event) for event in events), None


async def progress_from_job_log(
    job: Any, cloudwatch_client: Any, semaphore: asyncio.Semaphore
) -> tuple[dict[str, Any] | None, str | None]:
    """Read training progress from the fast CloudWatch copy."""
    log_text, error = await tail_cloudwatch_job_log(job, cloudwatch_client, semaphore)
    if not log_text:
        return None, error
    return flow_progress_from_log_text(log_text)
