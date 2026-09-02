"""Typed mutable state shared by Overwatch HTTP handlers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from aiohttp import web


@dataclass
class RefreshState:
    """Inspectable lifecycle state for periodic report refreshes."""

    in_progress: bool = False
    last_attempt_at: str | None = None
    last_success_at: str | None = None
    last_duration_seconds: float | None = None
    last_error: str | None = None


@dataclass
class ServiceState:
    """Mutable application state shared by the HTTP handlers."""

    startup_id: str
    report: dict[str, Any] | None
    report_error: str | None
    refresh: RefreshState
    query_diagnostics: dict[str, dict[str, Any]]


STATE_KEY = web.AppKey("state", ServiceState)
COLLECTOR_OPTIONS_KEY = web.AppKey("collector_options", object)
LOG_ATTEMPT_TASKS_KEY = web.AppKey(
    "log_attempt_tasks", dict[str, asyncio.Task[dict[str, Any]]]
)
LOG_ATTEMPT_CACHE_KEY = web.AppKey(
    "log_attempt_cache", dict[str, list[dict[str, Any]]]
)
LOG_REFRESH_TASKS_KEY = web.AppKey(
    "log_refresh_tasks", dict[str, asyncio.Task[dict[str, Any]]]
)
LOG_REFRESH_STARTED_KEY = web.AppKey("log_refresh_started", dict[str, float])
