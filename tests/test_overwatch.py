"""Focused regression coverage for Overwatch."""

import asyncio
from datetime import date
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace

import pytest

from overwatch import collector
from overwatch.cached_metrics import (
    cached_cloudwatch_attempts,
    cached_cloudwatch_log_page,
)
from overwatch.logs import (
    ansi_log_text_to_safe_html,
    discover_cloudwatch_attempts,
    flow_progress_from_log_text,
    tail_cloudwatch_job_log,
)
from overwatch.providers import billing
from overwatch.providers.sky import collect_managed_jobs, collect_standalone_clusters
from overwatch.providers.wandb_flow import match_skypilot_jobs
from overwatch.report import (
    build_cluster_record,
    build_sky_only_record,
    estimated_hourly_cost,
)


def test_log_parsing_smooths_throughput_and_escapes_ansi_html() -> None:
    log_text = (
        "train x [e1/1 b1/10 10%] [s1/10 10%] [0:00:10<0:01:30 1ex/s 100tok/s]\n"
        "train x [e1/1 b2/10 20%] [s2/10 20%] [0:00:20<0:01:20 1ex/s 200tok/s]"
    )

    progress, error = flow_progress_from_log_text(log_text)

    assert error is None
    assert progress is not None
    assert progress["completed_batches"] == 2
    assert progress["tokens_per_second"] == 120
    assert progress["tokens_per_second_ema_samples"] == 2
    assert ansi_log_text_to_safe_html("\x1b[31m<script>bad</script>\x1b[0m") == (
        '<span style="color:#cd3131">&lt;script&gt;bad&lt;/script&gt;</span>'
    )


def test_wandb_matching_prefers_exact_haiku_name_then_normalizes_both_sides() -> None:
    run = SimpleNamespace(
        name="stage/q3_seed4_v2__loud_post",
        url="https://wandb.ai/entity/project/runs/run-id",
        state="running",
        created_at="2026-09-01T23:24:56Z",
    )
    exact_job = SimpleNamespace(
        job_id=187,
        job_name="q3_seed4_v2__loud_post",
        links={"W&B Run": run.url},
        status="RUNNING",
        submitted_at=2,
    )
    resumed_job = SimpleNamespace(
        job_id=186,
        job_name="q3_seed4_v2__avid_park",
        links={},
        status="RUNNING",
        submitted_at=1,
    )

    assert match_skypilot_jobs([run], [resumed_job, exact_job]) == [exact_job]
    assert match_skypilot_jobs([run], [resumed_job]) == [resumed_job]

def test_cloudwatch_progress_uses_one_bounded_latest_tail_request() -> None:
    requests = []

    class CloudWatchClient:
        def describe_log_streams(self, **_kwargs: object) -> dict[str, object]:
            return {
                "logStreams": [{"logStreamName": "stream", "lastEventTimestamp": 1}]
            }

        def get_log_events(self, **kwargs: object) -> dict[str, object]:
            requests.append(kwargs)
            return {
                "events": [
                    {
                        "message": f"event-{index} tok/s"
                        if index >= 400
                        else f"event-{index}"
                    }
                    for index in range(1_400)
                ]
            }

    job = SimpleNamespace(
        cloud="aws",
        links={"AWS Instances": "https://example.test/?tag:ray-cluster-name=cluster"},
    )

    log_text, error = asyncio.run(
        tail_cloudwatch_job_log(job, CloudWatchClient(), asyncio.Semaphore(1))
    )

    assert error is None
    assert log_text is not None
    assert log_text.splitlines()[0] == "event-400 tok/s"
    assert log_text.splitlines()[-1] == "event-1399 tok/s"
    assert len(log_text.splitlines()) == 1_000
    assert len(requests) == 1
    assert requests[0]["startFromHead"] is False
    assert requests[0]["limit"] == 10_000


def test_cloudwatch_attempt_discovery_finds_each_process_without_full_scan() -> None:
    attempts = [(1_000, 11), (401_000, 22), (801_000, 33)]
    requests = []

    class CloudWatchClient:
        def describe_log_streams(self, **_kwargs: object) -> dict[str, object]:
            return {
                "logStreams": [
                    {
                        "logStreamName": "stream",
                        "firstEventTimestamp": 1_000,
                        "lastEventTimestamp": 1_200_000,
                    }
                ]
            }

        def get_log_events(self, **kwargs: object) -> dict[str, object]:
            requests.append(kwargs)
            start_time = int(kwargs["startTime"])
            pid = next(
                pid
                for index, (timestamp, pid) in enumerate(attempts)
                if timestamp <= start_time
                and (index + 1 == len(attempts) or start_time < attempts[index + 1][0])
            )
            return {
                "events": [
                    {
                        "timestamp": start_time,
                        "message": f'{{"pid":{pid},"log_line":"line"}}',
                    }
                ]
            }

    discovered = discover_cloudwatch_attempts(CloudWatchClient(), "stream", 3)

    assert [attempt["pid"] for attempt in discovered] == [11, 22, 33]
    assert [attempt["attempt"] for attempt in discovered] == [1, 2, 3]
    assert 1_000 <= discovered[0]["tail_at"] < 401_000
    assert 401_000 <= discovered[1]["tail_at"] < 801_000
    assert 801_000 <= discovered[2]["tail_at"] <= 1_200_000
    assert all(request["limit"] == 100 for request in requests)

    # A new recovery resumes discovery at the last cached process instead of rescanning history.
    requests.clear()
    suffix = discover_cloudwatch_attempts(
        CloudWatchClient(), "stream", 2, start_timestamp=401_000
    )
    assert [attempt["pid"] for attempt in suffix] == [22, 33]
    assert all(int(request["startTime"]) >= 401_000 for request in requests)


def test_raw_cloudwatch_cache_only_appends_new_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    responses = [
        {
            "events": [
                {"timestamp": 1_000, "ingestionTime": 1_001, "message": '{"pid":11,"log":"one"}'},
                {"timestamp": 2_000, "ingestionTime": 2_001, "message": '{"pid":11,"log":"two"}'},
            ],
            "nextForwardToken": "first",
        },
        {"events": [], "nextForwardToken": "first"},
        {
            "events": [
                {"timestamp": 3_000, "ingestionTime": 3_001, "message": '{"pid":22,"log":"three"}'},
            ],
            "nextForwardToken": "second",
        },
        {"events": [], "nextForwardToken": "second"},
        {
            "events": [
                {"timestamp": 1_000, "ingestionTime": 1_001, "message": '{"pid":11,"log":"one"}'},
                {"timestamp": 2_000, "ingestionTime": 2_001, "message": '{"pid":11,"log":"two"}'},
                {"timestamp": 3_000, "ingestionTime": 3_001, "message": '{"pid":22,"log":"three"}'},
            ],
            "nextForwardToken": "complete",
        },
        {"events": [], "nextForwardToken": "complete"},
    ]

    class CloudWatchClient:
        def describe_log_streams(self, **_kwargs: object) -> dict[str, object]:
            return {
                "logStreams": [
                    {"logStreamName": "skypilot-cluster-worker", "lastEventTimestamp": 3_000}
                ]
            }

        def get_log_events(self, **_kwargs: object) -> dict[str, object]:
            return responses.pop(0)

    # Replace the cache root and AWS client so append/cursor behavior is deterministic.
    monkeypatch.setattr(collector, "RAW_CACHE_ROOT", tmp_path / "raw-v1")
    monkeypatch.setattr(collector.boto3, "client", lambda *_args, **_kwargs: CloudWatchClient())
    job = {
        "job_id": 183,
        "status": "RUNNING",
        "cloud": "aws",
        "links": {"AWS Instances": "https://example.test/?tag:ray-cluster-name=cluster"},
    }
    legacy_events_path = collector.raw_cache_path(
        "jobs", "183", "cloudwatch", "events.jsonl"
    )
    legacy_events_path.parent.mkdir(parents=True)
    legacy_events_path.write_text(
        '{"timestamp":1000,"ingestionTime":1001,"message":"{\\"pid\\":11,\\"log\\":\\"one\\"}"}\n'
    )

    collector.append_cloudwatch_events(job)
    collector.append_cloudwatch_events(job)

    events_path = collector.raw_cache_path(
        "jobs", "183", "cloudwatch", "events.jsonl.zst"
    )
    events_index_path = events_path.with_name("events.index.json")
    human_log_path = events_path.with_name("events.log")
    assert not legacy_events_path.exists()
    assert sum(1 for _line in collector.iter_cloudwatch_event_lines_from_file(events_path)) == 3
    assert '"format": "jsonl-zstd-frames-v1"' in events_index_path.read_text()
    assert "1970-01-01T00:00:03.000Z pid=22 | three" in human_log_path.read_text()
    assert [attempt["pid"] for attempt in cached_cloudwatch_attempts(183, 2)] == [11, 22]
    newest_page = cached_cloudwatch_log_page(183, limit=2)
    assert [item["timestamp"] for item in newest_page["events"]] == [2_000, 3_000]
    assert newest_page["has_older"] is True
    oldest_page = cached_cloudwatch_log_page(
        183, before=newest_page["oldest_cursor"], limit=2
    )
    assert [item["timestamp"] for item in oldest_page["events"]] == [1_000]
    assert oldest_page["has_older"] is False
    newer_page = cached_cloudwatch_log_page(
        183, after=oldest_page["newest_cursor"], limit=2
    )
    assert [item["timestamp"] for item in newer_page["events"]] == [2_000, 3_000]
    assert newer_page["has_older"] is True
    assert newer_page["has_newer"] is False
    first_page = cached_cloudwatch_log_page(183, from_start=True, limit=2)
    assert [item["timestamp"] for item in first_page["events"]] == [1_000, 2_000]
    assert first_page["has_older"] is False
    assert first_page["has_newer"] is True
    cursor_text = collector.raw_cache_path("jobs", "183", "cloudwatch", "cursor.json").read_text()
    assert '\n  "next_forward_token": "second"' in cursor_text

    # A terminal full-history pass retains the same appendable indexed Zstandard cache.
    job["status"] = "SUCCEEDED"
    collector.append_cloudwatch_events(job, full_logs=True)

    assert events_path.exists()
    assert events_index_path.exists()
    assert human_log_path.exists()
    assert [attempt["pid"] for attempt in cached_cloudwatch_attempts(183, 2)] == [11, 22]
    assert '"events_file": "events.jsonl.zst"' in collector.raw_cache_path(
        "jobs", "183", "cloudwatch", "cursor.json"
    ).read_text()


def test_cost_scaling_and_packaged_browser_assets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Replace catalog lookup and pricing so the multi-node scaling invariant is deterministic.
    monkeypatch.setattr(
        "overwatch.report.sky.catalog.get_instance_type_for_accelerator",
        lambda *_a, **_k: (["p5"], None),
    )
    monkeypatch.setattr(
        "overwatch.report.sky.catalog.get_hourly_cost", lambda *_a, **_k: 3.0
    )
    job = SimpleNamespace(resources="2x[B200:8][Spot]", cloud="aws", region="us-west-2")

    hourly_cost, basis = estimated_hourly_cost(job)
    frontend = files("overwatch").joinpath("static", "dist")
    frontend_index = frontend.joinpath("index.html").read_text()
    frontend_assets = {path.name for path in frontend.joinpath("assets").iterdir()}

    assert hourly_cost == 6.0
    assert basis is not None and basis.endswith("2 nodes")
    assert '<div id="root"></div>' in frontend_index
    assert any(asset.endswith(".css") for asset in frontend_assets)
    assert any(asset.endswith(".js") for asset in frontend_assets)


def test_sky_inventory_is_unbounded_and_includes_standalone_clusters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 1_800_000_000
    active_job = SimpleNamespace(
        job_id=7,
        job_name="linked-job",
        status=SimpleNamespace(value="RUNNING"),
        end_at=now - 30 * 86400,
        links={"W&B Run": "https://wandb.ai/entity/project/runs/wandb123"},
        resources="",
        submitted_at=now - 120,
        start_at=now - 60,
        job_duration=60,
        recovery_count=0,
        user_name="erik",
        cloud="aws",
        region="us-west-2",
    )
    recent_finished_job = SimpleNamespace(
        job_id=8, status=SimpleNamespace(value="SUCCEEDED"), end_at=now - 86400
    )
    old_finished_job = SimpleNamespace(
        job_id=9, status=SimpleNamespace(value="FAILED"), end_at=now - 3 * 86400
    )
    managed_cluster = SimpleNamespace(name="managed", is_managed=True)
    dev_cluster = SimpleNamespace(
        name="erik-dev-b200",
        is_managed=False,
        status=SimpleNamespace(value="UP"),
        launched_at=1_700_000_000,
        user_name="erik",
        resources_str="1x[B200:8]",
        cloud="gcp",
        region="europe-west1",
        nodes=2,
        handle=SimpleNamespace(
            launched_resources=SimpleNamespace(get_cost=lambda _seconds: 4.0),
        ),
    )
    recent_stopped_cluster = SimpleNamespace(
        name="recent-stopped",
        is_managed=False,
        status=SimpleNamespace(value="STOPPED"),
        status_updated_at=now - 86400,
    )
    old_stopped_cluster = SimpleNamespace(
        name="old-stopped",
        is_managed=False,
        status=SimpleNamespace(value="STOPPED"),
        status_updated_at=now - 3 * 86400,
    )
    requests = []

    # Replace Sky requests so the test verifies inventory semantics without a live API server.
    monkeypatch.setattr(
        "overwatch.providers.sky.sky.jobs.queue_v2",
        lambda **kwargs: requests.append(kwargs) or "queue",
    )
    monkeypatch.setattr(
        "overwatch.providers.sky.sky.status",
        lambda **kwargs: requests.append(kwargs) or "status",
    )
    monkeypatch.setattr(
        "overwatch.providers.sky.sky.get",
        lambda request: (
            ([recent_finished_job, old_finished_job, active_job], {}, {})
            if request == "queue"
            else [
                managed_cluster,
                dev_cluster,
                recent_stopped_cluster,
                old_stopped_cluster,
            ]
        ),
    )
    monkeypatch.setattr("overwatch.providers.sky.time.time", lambda: now)

    jobs = collect_managed_jobs()
    clusters = collect_standalone_clusters()
    record = build_cluster_record(dev_cluster, "project-id")
    sky_only_record = build_sky_only_record(active_job, None, None, "project-id")

    assert jobs == [active_job, recent_finished_job]
    assert clusters == [dev_cluster, recent_stopped_cluster]
    assert requests[0]["limit"] is None
    assert requests[0]["all_users"] is True
    assert requests[1]["all_users"] is True
    assert record["name"] == "erik-dev-b200"
    assert record["status"] == {"wandb": None, "skypilot": "UP"}
    assert record["skypilot"]["resource_kind"] == "cluster"
    assert record["skypilot"]["job_id"] is None
    assert record["cost"]["hourly_usd"] == 8.0
    assert record["cost"]["estimated_spend_usd"] is not None
    # Preserve scheduler-provided telemetry links even when W&B enrichment misses the run.
    assert sky_only_record["wandb_id"] == "wandb123"
    assert sky_only_record["links"]["wandb"].endswith("/runs/wandb123")

    # A malformed optional Sky price must not prevent the cluster from appearing.
    dev_cluster.handle.launched_resources = SimpleNamespace(
        get_cost=lambda _seconds: {}["missing"]
    )
    record_without_cost = build_cluster_record(dev_cluster, "project-id")
    assert record_without_cost["cost"]["hourly_usd"] is None


def test_daily_cloud_spend_aligns_sparse_provider_days_and_totals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    billing._billing_cache.clear()

    # Replace provider calls so date-window normalization is independent of cloud accounts.
    monkeypatch.setattr(
        billing,
        "collect_aws_daily_spend",
        lambda _start, _end: (
            {"2026-08-01": 10.125, "2026-08-30": 5.0},
            {
                "2026-08-01": {
                    "compute": 8.0,
                    "storage": 2.125,
                    "everything_else": 0.0,
                },
                "2026-08-30": {
                    "compute": 5.0,
                    "storage": 0.0,
                    "everything_else": 0.0,
                },
            },
        ),
    )
    monkeypatch.setattr(
        billing,
        "collect_gcp_daily_spend",
        lambda _table, _start, _end: (
            {
                "project-a": {"2026-08-30": 2.25},
                "project-b": {"2026-08-01": 1.0},
            },
            {
                "2026-08-01": {
                    "compute": 0.0,
                    "storage": 0.0,
                    "everything_else": 1.0,
                },
                "2026-08-30": {
                    "compute": 2.0,
                    "storage": 0.25,
                    "everything_else": 0.0,
                },
            },
        ),
    )

    result = billing.collect_daily_cloud_spend(
        "project.dataset.gcp_billing_export_v1_account",
        today=date(2026, 8, 30),
    )

    assert result["start_date"] == "2026-08-01"
    assert result["end_date"] == "2026-08-30"
    assert len(result["daily"]) == 30
    assert [series["label"] for series in result["series"]] == [
        "AWS",
        "project-a",
        "project-b",
        "Combined",
    ]
    assert result["daily"][0] == {
        "date": "2026-08-01",
        "aws": 10.12,
        "gcp_0": 0.0,
        "gcp_1": 1.0,
        "combined": 11.12,
    }
    assert result["daily"][-1] == {
        "date": "2026-08-30",
        "aws": 5.0,
        "gcp_0": 2.25,
        "gcp_1": 0.0,
        "combined": 7.25,
    }
    assert result["category_daily"][0] == {
        "date": "2026-08-01",
        "aws_compute": 8.0,
        "aws_storage": 2.12,
        "aws_everything_else": 0.0,
        "gcp_compute": 0.0,
        "gcp_storage": 0.0,
        "gcp_everything_else": 1.0,
    }
    assert result["totals"] == {"aws": 15.12, "gcp": 3.25, "combined": 18.38}
