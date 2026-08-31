"""CloudWatch and SkyPilot log collection and parsing."""

import asyncio
import html
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from botocore.exceptions import ClientError

from overwatch.constants import (
    ANSI_COLORS,
    ANSI_ESCAPE_RE,
    ANSI_SGR_RE,
    AWS_CLUSTER_LINK_RE,
    CLOUDWATCH_LOG_GROUP,
    ERROR_RETRY_RE,
    TOKENS_PER_SECOND_EMA_ALPHA,
    TRAIN_PROGRESS_RE,
)
from overwatch.utils import seconds_from_duration


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


async def progress_and_retries_from_job_log(
    job: Any, cloudwatch_client: Any, semaphore: asyncio.Semaphore
) -> tuple[dict[str, Any] | None, str | None]:
    """Read the fast CloudWatch copy, falling back to SkyPilot for non-AWS jobs."""
    log_text, error = await tail_cloudwatch_job_log(job, cloudwatch_client, semaphore)
    if not log_text:
        log_text, error = await tail_sky_job_log(job, semaphore)
    if log_text is None:
        return None, error
    return flow_progress_from_log_text(log_text)


async def tail_sky_job_log(
    job: Any, semaphore: asyncio.Semaphore, *, controller: bool = False
) -> tuple[str | None, str | None]:
    async with semaphore:
        sky_executable = Path(sys.executable).with_name("sky")
        command = [str(sky_executable), "jobs", "logs", str(job.job_id)]
        if controller:
            command.append("--controller")
        command.extend(("--no-follow", "--tail", "10000" if controller else "2000"))
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=20)
        except TimeoutError:
            process.kill()
            await process.communicate()
            log_name = "controller log" if controller else "workload log"
            return None, f"{log_name} timed out after 20 seconds"
    if process.returncode != 0:
        error = stderr.decode(errors="replace").strip()
        return None, error or f"sky jobs logs exited {process.returncode}"
    return stdout.decode(errors="replace"), None


async def error_retry_count_from_controller_log(
    job: Any, semaphore: asyncio.Semaphore
) -> tuple[int | None, str | None]:
    if not job.recovery_count:
        return 0, None
    controller_log, error = await tail_sky_job_log(job, semaphore, controller=True)
    if error:
        return None, error
    retry_counts = [
        int(value)
        for value in ERROR_RETRY_RE.findall(ANSI_ESCAPE_RE.sub("", controller_log))
    ]
    if not retry_counts:
        return (
            None,
            "controller log does not contain an application-error retry counter",
        )
    return max(retry_counts), None
