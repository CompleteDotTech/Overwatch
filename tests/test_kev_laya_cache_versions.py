"""Direct adapter boundary tests; these are synthetic cache data, not live telemetry."""
from copy import deepcopy
from datetime import datetime, timezone

import pytest

from overwatch.kev_laya_adapter import CACHE_VERSION, merge_snapshots, presentation
from overwatch.kev_laya_contract import validate_snapshot

NOW = datetime(2026, 9, 27, 0, 0, tzinfo=timezone.utc)
BAD_VERSIONS = [[], {}, None, True, False, 1, 2.0, "", "kev_laya/raw/3"]


def snapshot():
    """A schema-valid historical v1 snapshot, explicitly synthetic."""
    return {
        "schema_version": 1, "framework": "kev_laya", "framework_version": "test-fixture",
        "experiment_id": "test-experiment", "run_id": "test-run", "attempt_id": "test-attempt",
        "attempt_index": 0, "parent_attempt_id": None, "sequence": 1,
        "started_at": NOW.isoformat(), "updated_at": NOW.isoformat(),
        "heartbeat_at": NOW.isoformat(), "phase": "train", "scheduler": None, "wandb": None,
        "progress": {
            "optimizer_steps": 7, "microbatches": 14, "examples": 28, "forward_tokens": 100,
            "epoch": None, "totals": {}, "elapsed_seconds": 1, "eta_seconds": None,
            "tokens_per_second": None,
        },
        "metrics": {"loss/ce": 0.75}, "history": [], "artifacts": [],
        "provenance": {
            "model": "fixture", "backbone": "fixture", "backbone_revision": "fixture",
            "tokenizer": "fixture", "config_sha256": "0" * 64, "data_sha256": "1" * 64,
            "split_hashes": {}, "seed": 0, "repository": None, "commit": None,
            "precision": "fixture", "hardware": "fixture", "evidence_class": "synthetic",
            "context_limits": {"branch": 32, "aggregate": 64},
        },
        "serving": {
            "requests": 0, "errors": 0, "input_tokens": 0, "forward_tokens": 0,
            "output_tokens": 0, "questions": 0, "prefix_reuses": 0, "latency_ms": [],
        },
        "resume": {"capable": False, "checkpoint_sha256": None},
        "recoveries": {"total": 7, "infrastructure": None, "application": None},
        "cost": {"measured_usd": None, "estimated_usd": None, "basis": None},
        "monitoring_export_failures": 0,
    }


def envelope(version):
    snap = snapshot()
    validate_snapshot(snap, now=NOW)
    return {
        "schema_version": version, "warnings": [], "observations": [],
        "records": [{
            "snapshot": snap, "source_ids": ["local-fixture"], "transports": ["local"],
            "provider_status": {}, "received_at": NOW.isoformat(),
        }],
    }


@pytest.mark.parametrize("version", BAD_VERSIONS)
def test_merge_rejects_invalid_version_without_mutating_inputs(version):
    previous = envelope(version)
    collected = {"configured_ids": ["local-fixture"], "records": [], "warnings": []}
    originals = deepcopy((previous, collected))
    result = merge_snapshots(previous, collected, now=NOW)
    assert result["schema_version"] == CACHE_VERSION
    assert result["records"] == []
    assert result["warnings"] == [{"code": "unsupported_prior_cache_version"}]
    assert (previous, collected) == originals


@pytest.mark.parametrize("version", BAD_VERSIONS)
def test_presentation_rejects_invalid_version(version):
    previous = envelope(version)
    original = deepcopy(previous)
    assert presentation(previous, now=NOW) == {
        "runs": [], "warnings": [{"code": "missing_or_invalid_model_run_cache"}],
    }
    assert previous == original


@pytest.mark.parametrize("previous", [{}, {"records": []}, [], "bad-envelope"])
def test_missing_version_or_invalid_envelope_is_diagnostic(previous):
    result = merge_snapshots(previous, {}, now=NOW)
    assert {"code": "unsupported_prior_cache_version"} in result["warnings"]
    assert result["records"] == []
    assert presentation(previous, now=NOW)["warnings"] == [
        {"code": "missing_or_invalid_model_run_cache"},
    ]


def test_no_previous_cache_is_a_valid_cold_start():
    assert merge_snapshots(None, {}, now=NOW)["warnings"] == []


@pytest.mark.parametrize("version", ["kev_laya/raw/1", "kev_laya/raw/2"])
def test_supported_versions_preserve_cached_values_and_unknowns(version):
    previous = envelope(version)
    original = deepcopy(previous)
    result = merge_snapshots(previous, {"configured_ids": ["local-fixture"]}, now=NOW)
    assert result["warnings"] == []
    assert result["records"][0]["snapshot"] == original["records"][0]["snapshot"]
    direct = presentation(previous, now=NOW)
    view = presentation(result, now=NOW)
    for rendered in (direct, view):
        assert rendered["warnings"] == []
        run = rendered["runs"][0]
        assert run["run_id"] == "test-run"
        assert run["progress"]["optimizer_steps"] == 7
        assert run["recoveries"] == {"total": 7, "infrastructure": None, "application": None}
        assert run["cost"]["measured_usd"] is None
        assert run["association"] == {"status": "local", "scheduler": None}
        assert run["provider_status"]["skypilot"] is None
    assert previous == original


@pytest.mark.parametrize("version", ["kev_laya/raw/1", "kev_laya/raw/2"])
def test_scheduler_status_and_total_remain_authoritative(version):
    previous = envelope(version)
    previous["records"][0]["snapshot"]["scheduler"] = {
        "provider": "skypilot", "namespace": "fixture-ns", "job_id": 42,
    }
    result = presentation(previous, [{
        "scheduler_namespace": "fixture-ns", "job_id": 42,
        "status": "RUNNING", "recovery_count": 11,
    }], now=NOW)
    run = result["runs"][0]
    assert run["provider_status"]["skypilot"] == "RUNNING"
    assert run["recoveries"] == {"total": 11, "infrastructure": None, "application": None}


def test_invalid_prior_does_not_drop_fresh_valid_candidate():
    result = merge_snapshots({"schema_version": []}, {
        "configured_ids": ["local-fixture"], "records": [{
            "source_id": "local-fixture", "transport": "local", "snapshot": snapshot(),
        }],
    }, now=NOW)
    assert result["warnings"] == [{"code": "unsupported_prior_cache_version"}]
    assert len(result["records"]) == 1
    assert result["records"][0]["snapshot"]["run_id"] == "test-run"
