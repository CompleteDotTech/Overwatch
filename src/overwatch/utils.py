"""Small value-conversion helpers shared across collectors and reports."""

import re
from datetime import UTC, datetime
from typing import Any


def parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value).astimezone(UTC)


def timestamp_from_epoch(value: float | None) -> datetime | None:
    return datetime.fromtimestamp(value, UTC) if value else None


def isoformat(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds") if value else None


def enum_value(value: Any) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "value", value))


def seconds_from_duration(value: str) -> float | None:
    value = value.strip()
    if value == "?":
        return None
    match = re.fullmatch(r"(?:(\d+) days?,\s*)?(\d+):(\d+):(\d+)", value)
    if match is None:
        return None
    days, hours, minutes, seconds = (int(part or 0) for part in match.groups())
    return float(days * 86400 + hours * 3600 + minutes * 60 + seconds)


def format_progress(completed_batches: int | None, total_batches: int | None) -> str:
    if completed_batches is None:
        return "—"
    if not total_batches:
        return f"{completed_batches:,}b"
    percent_done = completed_batches / total_batches * 100
    return f"{completed_batches:,}/{total_batches:,}b {percent_done:.0f}%"
