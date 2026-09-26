"""Native Overwatch E2E gate: run only in its real supported environment.

No reader/provider is replaced by a fake to obtain the initial report. The one
negative monkeypatch below is a trap proving cache-only rendering AFTER an actual
collector pass. Test caches/registries are always isolated from the running user.
"""
import argparse
import asyncio
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
import pytest


def test_real_export_through_collector_cache_and_report(tmp_path, monkeypatch):
    if sys.version_info < (3, 13, 12) or sys.version_info >= (3, 14):
        pytest.skip('Overwatch requires Python >=3.13.12,<3.14; do not relax the native gate')
    source = os.environ.get('KEV_LAYA_SNAPSHOT')
    if not source:
        pytest.skip('set KEV_LAYA_SNAPSHOT to a fresh real training export with checkpoint/evaluation artifacts')
    from overwatch import collector, raw_cache, cached_metrics
    from overwatch.report_service import collect_report_from_raw_cache
    from overwatch.kev_laya_contract import decode_snapshot
    original = Path(source).resolve()
    raw = original.read_bytes()
    snapshot = decode_snapshot(raw)
    assert snapshot['schema_version'] == 2
    assert snapshot['progress']['optimizer_steps'] > 0
    assert snapshot['extensions']['execution']['max_batch_size'] > 1
    assert any(a['kind'] == 'checkpoint' for a in snapshot['artifacts'])
    assert any(a['kind'] == 'evaluation' for a in snapshot['artifacts'])
    # Copy, never remove or rewrite the evidence-producing trainer's original.
    export = tmp_path / 'actual-export.json'
    export.write_bytes(raw)
    isolated = tmp_path / 'raw-v1'
    monkeypatch.setenv('OVERWATCH_RAW_CACHE_ROOT', str(isolated))
    for module in (raw_cache, collector, cached_metrics):
        monkeypatch.setattr(module, 'RAW_CACHE_ROOT', isolated)
    registry = tmp_path / 'registry.json'
    registry.write_text(json.dumps({'schema_version': 1, 'sources': [
        {'id': 'actual-training', 'transport': 'local', 'path': str(export)}]}))
    monkeypatch.setenv('OVERWATCH_KEV_LAYA_REGISTRY', str(registry))
    manifest = asyncio.run(collector.collect_raw_metrics(collector.CollectorOptions(local_models_only=True)))
    assert manifest['sources']['kev_laya']['status'] == 'ok'
    assert manifest['sources']['kev_laya']['raw']['runs'] == 1
    assert all(manifest['sources'][k]['status'] == 'skipped' for k in ('sky_jobs', 'wandb', 'cloudwatch'))
    cache = raw_cache.model_runs_cache_path()
    assert cache.name == 'runs-v2.json' and cache.is_file()
    args = argparse.Namespace(limit=20, entity=None, gcp_billing_table=None, no_log_enrichment=True,
                              zymtrace_project_id='00000000-0000-0000-0000-000000000000', local_models_only=True)
    report = asyncio.run(collect_report_from_raw_cache(args, refresh_cache=False))
    assert report['resources'] == []
    run = report['model_runs'][0]
    assert run['run_id'] == snapshot['run_id'] and run['verification'] == 'verified'
    assert run['association']['status'] == 'local' and run['provider_status']['skypilot'] is None
    assert run['progress']['optimizer_steps'] == snapshot['progress']['optimizer_steps']
    assert run['extensions']['execution'] == snapshot['extensions']['execution']
    # Model/report module reload stands in for a fresh reader process, retaining
    # actual persisted bytes, not an in-memory adapter result.
    importlib.reload(cached_metrics)
    export.unlink()
    def no_io(*args, **kwargs):
        raise AssertionError('cache-only report unexpectedly contacted collector/export/provider')
    monkeypatch.setattr(collector, 'collect_raw_metrics', no_io)
    from overwatch.providers import kev_laya as provider
    monkeypatch.setattr(provider, 'collect_snapshots', no_io)
    after = asyncio.run(collect_report_from_raw_cache(args, refresh_cache=False))
    assert after['model_runs'] == report['model_runs'] or (
        after['model_runs'][0]['extensions'] == run['extensions'] and
        after['model_runs'][0]['progress'] == run['progress'])  # freshness age legitimately advances
    assert hashlib.sha256(original.read_bytes()).hexdigest() == hashlib.sha256(raw).hexdigest()
    (tmp_path / 'native-e2e-report.json').write_text(json.dumps({
        'fresh_export_sha256': hashlib.sha256(raw).hexdigest(), 'report': after,
        'collector': manifest, 'cache_only_after_source_removed': True}, indent=2))
