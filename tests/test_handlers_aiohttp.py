"""Aiohttp handler integration coverage for the report and query endpoints.

The repo's existing tests drive handlers with ``make_mocked_request`` and
``asyncio.run`` rather than a full TestServer, so these integration tests follow
the same pattern: they wire the real ``web.Application`` state object used by
production and exercise the handler through its real request-param surface.
"""

import asyncio
import json
from argparse import Namespace
from datetime import UTC, datetime

import pytest
from aiohttp.test_utils import make_mocked_request

from overwatch import app


def _application(
    *,
    query_diagnostics: dict[str, dict[str, object]] | None = None,
) -> app.web.Application:
    application = app.web.Application()
    application[app.STATE_KEY] = app.ServiceState(
        startup_id="test",
        report={"resources": []},
        report_error=None,
        refresh=app.RefreshState(),
        query_diagnostics=query_diagnostics or {},
    )
    application[app.REPORT_ARGUMENTS_KEY] = Namespace()
    application[app.REPORT_REFRESH_TASK_KEY] = None
    return application


def _request(path: str, application: app.web.Application, match_info: dict[str, str] | None = None):
    return make_mocked_request(
        "GET", path, app=application, match_info=match_info or {}
    )


def test_raw_query_output_serves_inline_raw_output() -> None:
    application = _application(
        query_diagnostics={
            "sky": {
                "key": "sky",
                "label": "SkyPilot jobs",
                "status": "ok",
                "error": None,
                "updated_at": "2026-09-01T00:00:00Z",
                "duration_seconds": 1.0,
                "raw_output_updated_at": "2026-09-01T00:00:01Z",
                "raw_output": [1, 2, 3],
            }
        }
    )
    response = asyncio.run(app.handle_raw_query_output(_request("/api/status/sky/raw", application, {"query_key": "sky"})))
    body = json.loads(response.text)
    assert response.status == 200
    assert body["key"] == "sky"
    assert body["label"] == "SkyPilot jobs"
    assert body["output"] == [1, 2, 3]


def test_raw_query_output_serves_raw_files_and_marks_read_errors(tmp_path: object) -> None:
    from pathlib import Path  # noqa: PLC0415 - import the concrete type only where used

    good_path = Path(tmp_path) / "good.json"
    good_path.write_text('{"recovered": true}')
    bad_path = Path(tmp_path) / "bad.json"
    bad_path.write_text("{corrupt!!")

    application = _application(
        query_diagnostics={
            "gcp": {
                "key": "gcp",
                "label": "GCP billing",
                "status": "ok",
                "error": None,
                "updated_at": "2026-09-01T00:00:00Z",
                "duration_seconds": 2.0,
                "raw_output_updated_at": "2026-09-01T00:00:02Z",
                "raw_output": [],
                "raw_files": [str(good_path), str(bad_path)],
            }
        }
    )
    response = asyncio.run(app.handle_raw_query_output(_request("/api/status/gcp/raw", application, {"query_key": "gcp"})))
    output = json.loads(response.text)["output"]
    by_path = {item["path"]: item for item in output}
    assert by_path[str(good_path)]["value"] == {"recovered": True}
    assert "read_error" in by_path[str(bad_path)]["value"]


def test_raw_query_output_raises_404_for_unknown_key() -> None:
    application = _application(query_diagnostics={})
    with pytest.raises(app.web.HTTPNotFound) as exception_info:
        asyncio.run(app.handle_raw_query_output(_request("/api/status/missing/raw", application, {"query_key": "missing"})))
    assert exception_info.value.status == 404


def test_manual_report_refresh_starts_background_collection_and_updates_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application = _application()
    calls = 0

    async def update_report_state(*_args: object, **_kwargs: object) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {"generated_at": datetime.now(UTC).isoformat()}

    monkeypatch.setattr(app, "update_report_state", update_report_state)
    response = asyncio.run(app.handle_report_refresh(_request("POST", "/api/refresh", application)))
    assert json.loads(response.text) == {"started": True, "in_progress": True}
    assert response.status == 202

    # Wait for the background task the handler scheduled, then verify state landed.
    async def drain() -> None:
        await application[app.REPORT_REFRESH_TASK_KEY]

    asyncio.run(drain())
    assert calls == 1
    assert application[app.STATE_KEY].report is not None


def test_manual_report_refresh_does_not_start_while_a_task_is_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    application = _application()
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def update_report_state(*_args: object, **_kwargs: object) -> dict[str, object]:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return {"generated_at": "x"}

    monkeypatch.setattr(app, "update_report_state", update_report_state)
    application[app.REPORT_REFRESH_TASK_KEY] = asyncio.create_task(
        update_report_state()
    )

    async def refresh_while_pending() -> None:
        await started.wait()
        response = await app.handle_report_refresh(_request("POST", "/api/refresh", application))
        release.set()
        await application[app.REPORT_REFRESH_TASK_KEY]
        return response

    response = asyncio.run(refresh_while_pending())
    assert json.loads(response.text) == {"started": False, "in_progress": True}
    assert calls == 1