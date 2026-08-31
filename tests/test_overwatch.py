"""Focused regression coverage for Overwatch."""

import asyncio
from importlib.resources import files
from types import SimpleNamespace

import pytest

from overwatch.logs import (
    ansi_log_text_to_safe_html,
    flow_progress_from_log_text,
    tail_cloudwatch_job_log,
)
from overwatch.providers.sky import collect_managed_jobs, collect_standalone_clusters
from overwatch.report import build_cluster_record, estimated_hourly_cost
from overwatch.view import render_html


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
    rendered = render_html(
        {
            "generated_at": "2026-08-30T00:00:00+00:00",
            "requested_limit": 20,
            "resources": [],
            "config_differences": [],
            "warnings": ["W&B unavailable <temporarily>"],
        }
    )
    javascript = files("overwatch").joinpath("static", "report.js").read_text()

    assert hourly_cost == 6.0
    assert basis is not None and basis.endswith("2 nodes")
    assert '<link rel="stylesheet" href="/static/report.css">' in rendered
    assert '<script src="/static/report.js"></script>' in rendered
    assert "W&amp;B unavailable &lt;temporarily&gt;" in rendered
    assert 'event.key === "Escape"' in javascript
    assert 'CloudWatch…\\n"' in javascript
    assert 'CloudWatch…\\\\n"' not in javascript


def test_sky_inventory_is_unbounded_and_includes_standalone_clusters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    managed_job = SimpleNamespace(job_id=7)
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
            ([managed_job], {}, {})
            if request == "queue"
            else [managed_cluster, dev_cluster]
        ),
    )

    jobs = collect_managed_jobs()
    clusters = collect_standalone_clusters()
    record = build_cluster_record(dev_cluster, "project-id")

    assert jobs == [managed_job]
    assert clusters == [dev_cluster]
    assert requests[0]["limit"] is None
    assert requests[0]["all_users"] is True
    assert requests[1]["all_users"] is True
    assert record["name"] == "erik-dev-b200"
    assert record["status"] == {"wandb": None, "skypilot": "UP"}
    assert record["skypilot"]["resource_kind"] == "cluster"
    assert record["skypilot"]["job_id"] is None
    assert record["cost"]["hourly_usd"] == 8.0
    assert record["cost"]["estimated_spend_usd"] is not None

    # A malformed optional Sky price must not prevent the cluster from appearing.
    dev_cluster.handle.launched_resources = SimpleNamespace(
        get_cost=lambda _seconds: {}["missing"]
    )
    record_without_cost = build_cluster_record(dev_cluster, "project-id")
    assert record_without_cost["cost"]["hourly_usd"] is None
