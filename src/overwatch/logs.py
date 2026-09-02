"""CloudWatch and SkyPilot log collection and parsing."""

import hashlib
import html
import json
import re
from typing import Any
from urllib.parse import unquote

from overwatch.constants import (
    ANSI_COLORS,
    ANSI_ESCAPE_RE,
    ANSI_SGR_RE,
    AWS_CLUSTER_LINK_RE,
    TOKENS_PER_SECOND_EMA_ALPHA,
    TRAIN_PROGRESS_RE,
)
from overwatch.utils import seconds_from_duration

CLOUDWATCH_PID_RE = re.compile(r"\bpid=(?P<pid>\d+)\b")
CLOUDWATCH_NONZERO_EXIT_RE = re.compile(
    r"\b(?:exitcode|returncode)\s*(?::|=)\s*(?!0\b)-?\d+\b"
)
CLOUDWATCH_CONTAINER_EXIT_RE = re.compile(r"\bcontainer\b.*\bexited \((?!0\))\d+\)")
CLOUDWATCH_TRAIN_CONFIG_RE = re.compile(
    r"(?P<uri>(?:gs|r2|s3)://\S+/train_config\.yaml)"
)
CLOUDWATCH_R2_LOCAL_PATH_RE = re.compile(
    r"/typesafe/r2/(?P<account>[^/]+)/(?P<bucket>[^/]+)/(?P<key>\S+/train_config\.yaml)"
)
CLOUDWATCH_RUN_DIR_RE = re.compile(r"\brun_dir:\s*(?P<uri>(?:gs|r2|s3)://\S+)")
CLOUDWATCH_WANDB_URL_RE = re.compile(
    r"https://wandb\.ai/[^/\s]+/[^/\s]+/runs/[A-Za-z0-9_-]+"
)


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


def cloudwatch_event_id(event: dict[str, Any]) -> str:
    """Build a stable identity for deduplicating tail and history responses."""
    identity = (
        f"{event['timestamp']}:{event.get('ingestionTime', '')}:{event['message']}"
    )
    return hashlib.blake2s(identity.encode(), digest_size=12).hexdigest()


def cloudwatch_message_has_application_failure(message: str) -> bool:
    """Identify explicit terminal application failures in a cached log line."""
    normalized_message = ANSI_ESCAPE_RE.sub("", message).casefold()
    return (
        "root cause (first observed failure):" in normalized_message
        or "childfailederror" in normalized_message
        or "setup failed. failed workers" in normalized_message
        or CLOUDWATCH_NONZERO_EXIT_RE.search(normalized_message) is not None
        or CLOUDWATCH_CONTAINER_EXIT_RE.search(normalized_message) is not None
    )


def flow_references_from_cloudwatch_message(message: str) -> dict[str, str]:
    """Extract durable training references present in one raw log message."""
    normalized_message = ANSI_ESCAPE_RE.sub("", message)
    references = {}
    if match := CLOUDWATCH_TRAIN_CONFIG_RE.search(normalized_message):
        config_uri = match.group("uri")
        if config_uri.startswith("s3://") and (
            r2_path_match := CLOUDWATCH_R2_LOCAL_PATH_RE.search(normalized_message)
        ):
            config_uri = (
                f'r2://{r2_path_match.group("account")}@'
                f'{r2_path_match.group("bucket")}/{r2_path_match.group("key")}'
            )
        references["config_uri"] = config_uri
    if match := CLOUDWATCH_RUN_DIR_RE.search(normalized_message):
        references["run_uri"] = match.group("uri")
    if match := CLOUDWATCH_WANDB_URL_RE.search(normalized_message):
        references["wandb_url"] = match.group(0)
    return references


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
