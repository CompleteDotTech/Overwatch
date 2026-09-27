"""Synthetic transport regressions; no live W&B, SDK, model, or UI qualification."""
from __future__ import annotations

from datetime import datetime, timezone
import io
import json
import socket
from urllib.error import URLError

import pytest

from overwatch.kev_laya_contract import validate_snapshot
from overwatch.providers import kev_laya as provider


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('source regressions must not contact a network')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket.socket, 'connect_ex', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    monkeypatch.setenv('WANDB_API_KEY', 'synthetic-source-test-placeholder')
    monkeypatch.setenv('WANDB_BASE_URL', 'https://example.invalid')


def snapshot(run_id='run-1', sequence=1):
    now = datetime.now(timezone.utc).isoformat()
    result = {
        'schema_version': 1, 'framework': 'kev_laya', 'framework_version': 'test',
        'experiment_id': 'synthetic-test', 'run_id': run_id, 'attempt_id': 'attempt-0',
        'attempt_index': 0, 'parent_attempt_id': None, 'sequence': sequence,
        'started_at': now, 'updated_at': now, 'heartbeat_at': now, 'phase': 'train',
        'scheduler': None, 'wandb': {'entity': 'test', 'project': 'test', 'run_id': run_id},
        'progress': {'optimizer_steps': 1, 'microbatches': 1, 'examples': 1,
                     'forward_tokens': 1, 'epoch': None, 'totals': {},
                     'elapsed_seconds': None, 'eta_seconds': None, 'tokens_per_second': None},
        'metrics': {}, 'history': [],
        'provenance': {'model': 'synthetic', 'backbone': 'none', 'backbone_revision': 'none',
                       'tokenizer': 'none', 'config_sha256': '0' * 64, 'data_sha256': '0' * 64,
                       'split_hashes': {}, 'seed': 0, 'repository': None, 'commit': None,
                       'precision': 'none', 'hardware': 'none',
                       'context_limits': {'branch': 1, 'aggregate': 1}, 'evidence_class': 'fixture'},
        'artifacts': [],
        'serving': {'requests': 0, 'errors': 0, 'input_tokens': 0, 'forward_tokens': 0,
                    'output_tokens': 0, 'questions': 0, 'prefix_reuses': 0, 'latency_ms': []},
        'resume': {'capable': False, 'checkpoint_sha256': None},
        'recoveries': {'total': None, 'infrastructure': None, 'application': None},
        'cost': {'measured_usd': None, 'estimated_usd': None, 'basis': None},
        'monitoring_export_failures': 0,
    }
    return validate_snapshot(result)


def page(flag=False, *, cursor=None, snapshots=(), encoded_summary=False):
    edges = []
    for item in snapshots:
        summary = {'kev_laya/snapshot': item}
        edges.append({'cursor': cursor, 'node': {'name': item['run_id'], 'state': 'running',
                     'summaryMetrics': json.dumps(summary) if encoded_summary else summary}})
    return {'data': {'project': {'runs': {'edges': edges,
            'pageInfo': {'hasNextPage': flag, 'endCursor': cursor}}}}}


class RecordingBody(io.BytesIO):
    def __init__(self, raw, headers=None):
        super().__init__(raw)
        self.headers = {} if headers is None else headers
        self.read_sizes = []
        self.close_calls = 0

    def read(self, size=-1):
        self.read_sizes.append(size)
        assert size >= 0, 'unbounded read'
        return super().read(size)

    def close(self):
        self.close_calls += 1
        super().close()


class Pages:
    def __init__(self, pages, *, known_length=True):
        self.raw = [json.dumps(item, allow_nan=False).encode() for item in pages]
        self.bodies = [RecordingBody(raw) for raw in self.raw]
        self.calls = []
        self.known_length = known_length

    def fetch_snapshot_page(self, **kwargs):
        index = len(self.calls)
        self.calls.append(kwargs)
        assert index < len(self.bodies), 'unexpected extra provider request'
        return {'Body': self.bodies[index],
                'ContentLength': len(self.raw[index]) if self.known_length else None}


def collect(tmp_path, pages, *, limits=None, source_limit=8):
    registry = tmp_path / 'registry.json'
    registry.write_text(json.dumps({'schema_version': 1, 'sources': [
        {'id': 'registered-test', 'transport': 'wandb', 'entity': 'test',
         'project': 'test', 'limit': source_limit}]}), encoding='utf-8')
    return provider.collect_snapshots(registry, wandb_api=pages, limits=limits)


def assert_invalid(result, pages, *, retained=()):
    assert result['complete'] is False
    assert result['registry_complete'] is True  # An invalid response is not revocation.
    assert result['configured_ids'] == ['registered-test']
    assert result['sources'][0]['status'] == 'error'
    assert result['sources'][0]['refreshed_at'] is None
    assert [row['snapshot']['run_id'] for row in result['records']] == list(retained)
    assert result['warnings'] == [{'source_id': 'registered-test',
                                  'code': 'source_unavailable_or_invalid', 'error_type': 'ValueError'}]
    assert result['accounting']['payload_bytes'] == sum(len(raw) for raw in pages.raw[:len(pages.calls)])
    assert result['accounting']['requests'] == len(pages.calls)
    assert result['accounting']['pages'] == len(pages.calls)
    for body in pages.bodies[:len(pages.calls)]:
        assert body.closed and body.close_calls == 1
    assert 'synthetic-source-test-placeholder' not in json.dumps(result)


@pytest.mark.parametrize('flag', [None, 0, 1, -1, 0.0, 1.0, '', 'false', 'true', [], {}, [False]])
@pytest.mark.parametrize('populated', [False, True])
def test_non_boolean_page_completion_is_never_success(tmp_path, flag, populated):
    pages = Pages([page(flag, cursor='next', snapshots=[snapshot()] if populated else [])])
    result = collect(tmp_path, pages)
    assert len(pages.calls) == 1
    assert_invalid(result, pages)


@pytest.mark.parametrize('metadata', [None, [], '', {}, {'endCursor': None}])
def test_invalid_page_info_rejected_before_snapshot_acceptance(tmp_path, metadata):
    response = page(snapshots=[snapshot()])
    response['data']['project']['runs']['pageInfo'] = metadata
    pages = Pages([response])
    assert_invalid(collect(tmp_path, pages), pages)


@pytest.mark.parametrize('cursor', [None, '', 1, False, [], {}])
def test_invalid_continuation_rejected_before_snapshot_acceptance(tmp_path, cursor):
    pages = Pages([page(True, cursor=cursor, snapshots=[snapshot()])])
    assert_invalid(collect(tmp_path, pages), pages)


def test_repeated_cursor_rejects_current_page_but_retains_valid_previous_page(tmp_path):
    first = snapshot('run-1')
    pages = Pages([page(True, cursor='same', snapshots=[first]),
                   page(True, cursor='same', snapshots=[snapshot('run-2')])])
    result = collect(tmp_path, pages)
    assert_invalid(result, pages, retained=['run-1'])
    assert result['records'][0]['snapshot'] == first
    assert [call['cursor'] for call in pages.calls] == [None, 'same']


def test_bad_later_page_does_not_discard_prior_records_or_refresh_source(tmp_path):
    first = snapshot('run-1')
    pages = Pages([page(True, cursor='next', snapshots=[first]),
                   page(None, snapshots=[snapshot('run-2')])])
    result = collect(tmp_path, pages)
    assert_invalid(result, pages, retained=['run-1'])
    assert result['records'][0]['snapshot'] == first


@pytest.mark.parametrize('populated', [False, True])
@pytest.mark.parametrize('encoded_summary', [False, True])
@pytest.mark.parametrize('known_length', [False, True])
def test_valid_final_pages_remain_successful(tmp_path, populated, encoded_summary, known_length):
    item = snapshot()
    pages = Pages([page(False, snapshots=[item] if populated else [],
                        encoded_summary=encoded_summary)], known_length=known_length)
    result = collect(tmp_path, pages)
    assert result['complete'] is True and result['warnings'] == []
    assert result['sources'][0]['refreshed_at'] is not None
    assert len(result['records']) == int(populated)
    if populated:
        assert result['records'][0]['snapshot'] == item
        assert result['records'][0]['provider_status'] == 'running'
    assert result['accounting']['payload_bytes'] == len(pages.raw[0])
    assert pages.bodies[0].closed and pages.bodies[0].close_calls == 1


def test_valid_pagination_keeps_explicit_request_and_payload_accounting(tmp_path):
    pages = Pages([page(True, cursor='next', snapshots=[snapshot('run-1')]),
                   page(False, snapshots=[snapshot('run-2')])])
    result = collect(tmp_path, pages)
    assert result['complete'] and len(result['records']) == 2
    assert result['accounting']['payload_bytes'] == sum(map(len, pages.raw))
    assert result['accounting']['requests'] == result['accounting']['pages'] == 2
    assert [call['cursor'] for call in pages.calls] == [None, 'next']
    assert all(0 < call['timeout'] <= 3 for call in pages.calls)
    assert all(body.closed and body.close_calls == 1 for body in pages.bodies)


def test_source_listing_limit_stays_partial(tmp_path):
    pages = Pages([page(True, cursor='next', snapshots=[snapshot()])])
    result = collect(tmp_path, pages, source_limit=1)
    assert not result['complete'] and len(result['records']) == 1
    assert result['sources'][0]['status'] == 'partial_limit'
    assert result['sources'][0]['refreshed_at'] is None
    assert len(pages.calls) == 1


@pytest.mark.parametrize('limits,reason', [
    (provider.CollectionLimits(requests=1), 'request_budget'),
    (provider.CollectionLimits(pages=1), 'page_budget'),
])
def test_budget_stop_preserves_previous_valid_page(tmp_path, limits, reason):
    pages = Pages([page(True, cursor='next', snapshots=[snapshot()])])
    result = collect(tmp_path, pages, limits=limits)
    assert not result['complete'] and len(result['records']) == 1
    assert result['sources'][0]['status'] == 'partial_' + reason
    assert result['sources'][0]['refreshed_at'] is None
    assert result['accounting']['stop_reason'] == reason
    assert len(pages.calls) == 1 and pages.bodies[0].closed


def test_short_payload_budget_is_not_refreshed(tmp_path):
    pages = Pages([page(False)])
    result = collect(tmp_path, pages, limits=provider.CollectionLimits(payload_bytes=20))
    assert not result['complete'] and result['records'] == []
    assert result['sources'][0]['refreshed_at'] is None
    assert result['accounting']['payload_bytes'] <= 20
    assert pages.bodies[0].closed


def test_identity_mismatch_still_fails(tmp_path):
    response = page(snapshots=[snapshot()])
    response['data']['project']['runs']['edges'][0]['node']['name'] = 'other-run'
    pages = Pages([response])
    assert_invalid(collect(tmp_path, pages), pages)


@pytest.mark.parametrize('errors', [[{'message': 'synthetic remote failure'}], ['failure']])
def test_graphql_error_stays_non_success_and_secret_safe(tmp_path, errors):
    response = page(snapshots=[snapshot()])
    response['errors'] = errors
    pages = Pages([response])
    result = collect(tmp_path, pages)
    assert_invalid(result, pages)
    assert 'synthetic remote failure' not in json.dumps(result)


class Opener:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def open(self, request, *, timeout):
        self.calls.append((request, timeout))
        assert request.full_url == 'https://example.invalid/graphql'
        return self.response


def fetch_page(monkeypatch, response):
    opener = Opener(response)
    monkeypatch.setattr(provider, 'build_opener', lambda *handlers: opener)
    result = provider.WandbPages().fetch_snapshot_page(entity='test', project='test',
                                                     cursor=None, first=1, timeout=1)
    assert len(opener.calls) == 1
    return result


@pytest.mark.parametrize('length', ['', 'NaN', '1.5', 'not-a-length', '0x10', [], {}])
def test_invalid_content_length_closes_unread_response(monkeypatch, length):
    response = RecordingBody(b'never-read', {'Content-Length': length})
    with pytest.raises((ValueError, TypeError)):
        fetch_page(monkeypatch, response)
    assert response.closed and response.close_calls == 1
    assert response.read_sizes == []


@pytest.mark.parametrize('exception', [ValueError('bad headers'), KeyboardInterrupt()])
def test_header_extraction_failure_closes_response(monkeypatch, exception):
    class BrokenHeaders:
        def get(self, name):
            raise exception
    response = RecordingBody(b'never-read', BrokenHeaders())
    with pytest.raises(type(exception)):
        fetch_page(monkeypatch, response)
    assert response.closed and response.close_calls == 1
    assert response.read_sizes == []


@pytest.mark.parametrize('length,expected', [(None, None), ('0', 0), ('123', 123)])
def test_valid_header_transfers_body_ownership_without_reading(monkeypatch, length, expected):
    response = RecordingBody(b'not-read', {'Content-Length': length})
    result = fetch_page(monkeypatch, response)
    assert result == {'Body': response, 'ContentLength': expected}
    assert not response.closed and response.read_sizes == []
    response.close()


def test_request_failure_is_not_retried(monkeypatch):
    calls = []
    class FailedOpener:
        def open(self, request, *, timeout):
            calls.append(request.full_url)
            raise URLError('synthetic request failure')
    monkeypatch.setattr(provider, 'build_opener', lambda *handlers: FailedOpener())
    with pytest.raises(URLError):
        provider.WandbPages().fetch_snapshot_page(entity='test', project='test',
                                                 cursor=None, first=1, timeout=1)
    assert calls == ['https://example.invalid/graphql']
