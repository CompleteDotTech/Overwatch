"""Budgeted structured transports. Only this collector reads registered exports.

Local/S3 exposed stream bytes and the entire W&B JSON response body are charged
before another read or fetch. W&B envelope bytes are conservatively included;
TLS, headers, SDK credential discovery and other opaque transport overhead are
not claimed as measured payload. No implicit SDK pagination or retry loops.
"""
from __future__ import annotations
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import stat
import time
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler
from overwatch.kev_laya_contract import MAX_SNAPSHOT_BYTES, decode_snapshot, identifier

MAX_SOURCES = 512
MAX_TOTAL_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class CollectionLimits:
    payload_bytes: int = MAX_TOTAL_BYTES
    requests: int = 512
    pages: int = 8
    seconds: float = 15.0
    stream_reads: int = 4096
    def __post_init__(self):
        if any(type(x) is not int or x < 1 for x in (self.payload_bytes, self.requests, self.pages, self.stream_reads)):
            raise ValueError('collection counts must be positive integers')
        if not math.isfinite(self.seconds) or self.seconds < 0:
            raise ValueError('invalid collection deadline')


class BudgetStop(RuntimeError):
    pass


class Budget:
    def __init__(self, limits, clock, *, started=None):
        self.limits, self.clock, self.start = limits, clock, clock() if started is None else started
        self.bytes = self.requests = self.pages = self.reads = 0
    @property
    def remaining(self):
        return self.limits.payload_bytes - self.bytes
    def check(self):
        if self.clock() - self.start >= self.limits.seconds:
            raise BudgetStop('deadline')
        if self.remaining <= 0:
            raise BudgetStop('payload_budget')
    def request(self, *, page=False):
        self.check()
        if self.requests >= self.limits.requests:
            raise BudgetStop('request_budget')
        if page and self.pages >= self.limits.pages:
            raise BudgetStop('page_budget')
        self.requests += 1
        self.pages += int(page)
    def timeout(self):
        self.check()
        return max(.001, min(3., self.limits.seconds - (self.clock() - self.start)))
    def read(self, stream, *, known_length=None, maximum=MAX_SNAPSHOT_BYTES):
        self.check()
        if known_length is not None:
            if type(known_length) is not int or known_length < 0:
                raise ValueError('invalid declared payload length')
            if known_length > maximum:
                raise ValueError('payload exceeds snapshot bound')
        goal = known_length if known_length is not None else maximum + 1
        cap = min(goal, self.remaining)
        chunks, used, eof = [], 0, False
        while used < cap:
            self.check()
            if self.reads >= self.limits.stream_reads:
                raise BudgetStop('stream_read_budget')
            count = min(32768, cap - used, self.remaining)
            self.reads += 1
            raw = stream.read(count)
            if not isinstance(raw, bytes):
                raise ValueError('transport violated byte-read contract')
            self.bytes += len(raw)  # Charge even a misbehaving injected transport; do not hide actual I/O.
            if len(raw) > count:
                raise ValueError('transport violated bounded byte-read contract')
            used += len(raw)
            if not raw:
                eof = True
                break
            chunks.append(raw)
        if known_length is not None:
            if used != known_length:
                if self.remaining == 0:
                    raise BudgetStop('payload_budget')
                raise ValueError('short declared payload')
        elif not eof:
            if used > maximum:
                raise ValueError('snapshot overflow probe detected excess bytes')
            raise BudgetStop('payload_budget')  # Completeness unknown, no uncharged probe.
        raw = b''.join(chunks)
        if len(raw) > maximum:
            raise ValueError('snapshot byte bound exceeded')
        return raw


def registry_path() -> Path:
    return Path(os.environ.get('OVERWATCH_KEV_LAYA_REGISTRY', str(Path.home() / '.config/overwatch/kev-laya-sources.json')))


def unique_json(raw):
    def pairs(values):
        obj = {}
        for k, v in values:
            if k in obj:
                raise ValueError('duplicate JSON key')
            obj[k] = v
        return obj
    return json.loads(raw, object_pairs_hook=pairs)


def validate_registry(registry):
    if not isinstance(registry, dict) or set(registry) != {'schema_version', 'sources'} or type(registry['schema_version']) is not int or registry['schema_version'] != 1:
        raise ValueError('invalid Kev-Laya registry version/fields')
    sources = registry['sources']
    if not isinstance(sources, list) or len(sources) > MAX_SOURCES:
        raise ValueError('registry source bound exceeded')
    seen = set()
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError('registry source must be an object')
        source_id = source.get('id')
        identifier(source_id, 'source ID')
        if source_id in seen:
            raise ValueError('duplicate registry source ID')
        seen.add(source_id)
        transport = source.get('transport')
        if transport == 'local':
            if set(source) != {'id', 'transport', 'path'} or not isinstance(source['path'], str) or not Path(source['path']).is_absolute():
                raise ValueError('invalid local source; absolute registered file required')
        elif transport == 's3':
            if set(source) != {'id', 'transport', 'bucket', 'key'}:
                raise ValueError('invalid S3 source fields')
            for k in ('bucket', 'key'):
                val = source[k]
                if not isinstance(val, str) or not val or len(val) > 1024 or any(ord(c) < 32 for c in val):
                    raise ValueError('invalid S3 identifier')
            if '/' in source['bucket'] or '?' in source['bucket']:
                raise ValueError('bucket is not an identifier')
        elif transport == 'wandb':
            if set(source) != {'id', 'transport', 'entity', 'project', 'limit'} or type(source['limit']) is not int or not 1 <= source['limit'] <= MAX_SOURCES:
                raise ValueError('invalid W&B source fields/limit')
            for k in ('entity', 'project'):
                identifier(source[k], k)
                if '/' in source[k]:
                    raise ValueError('invalid W&B component')
        else:
            raise ValueError('unknown telemetry transport')
    return sources


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError('W&B redirects are disabled; request count and credential scope are explicit')


def open_regular(path: Path):
    """Open before fstat without a FIFO race; symlinks are rejected where supported."""
    flags = os.O_RDONLY | getattr(os, 'O_NONBLOCK', 0) | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_BINARY', 0)
    fd = os.open(path, flags)
    try:
        details = os.fstat(fd)
        if not stat.S_ISREG(details.st_mode):
            raise ValueError('registered source must be a regular file')
        stream = os.fdopen(fd, 'rb')
    except BaseException:
        os.close(fd)
        raise
    return stream, details.st_size


class WandbPages:
    """Explicit, streamed GraphQL pages; no eager public-SDK iterator hydration.

    fetch_snapshot_page is also the bounded SDK-double interface used by tests.
    The SDK's ordinary lazy iterator does not expose a pre-read byte cap and is
    deliberately not used by this transport. Each call makes one HTTP request.
    """
    def fetch_snapshot_page(self, *, entity, project, cursor, first, timeout):
        token = os.environ.get('WANDB_API_KEY')
        if not token:
            raise ValueError('explicit W&B read authentication unavailable')
        base = os.environ.get('WANDB_BASE_URL', 'https://api.wandb.ai').rstrip('/')
        parsed = urlsplit(base)
        if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError('invalid W&B base URL')
        query = '''query KevLayaSnapshots($entity: String!, $project: String!, $first: Int!, $after: String, $filters: JSONString) {
          project(name: $project, entityName: $entity) {
            runs(first: $first, after: $after, filters: $filters, order: "-updatedAt") {
              edges { cursor node { name state summaryMetrics } }
              pageInfo { hasNextPage endCursor }
            }
          }
        }'''
        body = json.dumps({'query': query, 'variables': {'entity': entity, 'project': project,
                           'first': first, 'after': cursor,
                           'filters': json.dumps({'config.framework': 'kev_laya', 'config.telemetry_schema_version': {'$in': [1, 2]}})}}).encode()
        auth = base64.b64encode(('api:' + token).encode()).decode()
        response = build_opener(NoRedirects()).open(Request(base + '/graphql', data=body,
                       headers={'Authorization': 'Basic ' + auth, 'Content-Type': 'application/json', 'Accept-Encoding': 'identity'}), timeout=timeout)
        # Until the body is returned, this method owns it. Header failures must
        # not leak an unread response outside the collector's cleanup scope.
        try:
            length = response.headers.get('Content-Length')
            declared_length = int(length) if length is not None else None
        except BaseException:
            response.close()
            raise
        return {'Body': response, 'ContentLength': declared_length}


def collect_snapshots(path: Path | None = None, *, wandb_api=None, s3_client=None,
                      limits: CollectionLimits | None = None, clock=time.monotonic) -> dict[str, Any]:
    started = clock()
    limits = limits or CollectionLimits()
    path = Path(path) if path is not None else registry_path()
    if not path.exists():
        # Missing is unavailable, NOT an explicit empty registration/revocation.
        return {'records': [], 'warnings': [{'code': 'registry_unavailable'}], 'configured_ids': [],
                'registry_complete': False, 'complete': False, 'sources': [], 'accounting': {}}
    registry_stream, _ = open_regular(path)
    with registry_stream:
        raw_registry = registry_stream.read(131073)
    if len(raw_registry) > 131072:
        raise ValueError('Kev-Laya registry exceeds 128 KiB')
    sources = validate_registry(unique_json(raw_registry))  # All validation BEFORE payload I/O.
    budget = Budget(limits, clock, started=started)
    cached_s3_client = s3_client
    records, warnings, statuses = [], [], []
    now = datetime.now(timezone.utc)
    stop_reason = None

    def append(raw, source, provider_status=None, wb_identity=None):
        snapshot = decode_snapshot(raw, now=now)
        if wb_identity is not None and snapshot['wandb'] != wb_identity:
            raise ValueError('W&B transport identity conflicts with snapshot')
        records.append({'snapshot': snapshot, 'source_id': source['id'], 'transport': source['transport'], 'provider_status': provider_status})

    for source in sources:
        source_id = source['id']
        before_bytes, before_requests, before_records = budget.bytes, budget.requests, len(records)
        status, error_type = 'ok', None
        try:
            if stop_reason:
                raise BudgetStop(stop_reason)
            budget.check()
            transport = source['transport']
            if transport == 'local':
                budget.request()
                stream, size = open_regular(Path(source['path']))
                with stream:
                    raw = budget.read(stream, known_length=size)
                append(raw, source)
            elif transport == 's3':
                budget.request()
                client = cached_s3_client
                if client is None:
                    import boto3
                    from botocore.config import Config
                    timeout = budget.timeout()
                    client = boto3.client('s3', config=Config(connect_timeout=timeout, read_timeout=timeout,
                                           retries={'total_max_attempts': 1}))
                    cached_s3_client = client
                budget.check()
                response = client.get_object(Bucket=source['bucket'], Key=source['key'])
                body = response['Body']
                try:
                    raw = budget.read(body, known_length=response.get('ContentLength'))
                finally:
                    body.close()
                append(raw, source)
            else:
                api = wandb_api if wandb_api is not None else WandbPages()
                if not hasattr(api, 'fetch_snapshot_page'):
                    raise ValueError('bounded W&B page transport required; eager SDK iterators are unsupported')
                cursor, seen, count = None, set(), 0
                while count < source['limit']:
                    budget.request(page=True)
                    first = min(32, source['limit'] - count, max(1, budget.remaining // MAX_SNAPSHOT_BYTES))
                    response = api.fetch_snapshot_page(entity=source['entity'], project=source['project'],
                                                        cursor=cursor, first=first, timeout=budget.timeout())
                    body = response['Body']
                    try:
                        page_raw = budget.read(body, known_length=response.get('ContentLength'), maximum=budget.remaining)
                    finally:
                        body.close()
                    page = unique_json(page_raw)
                    if page.get('errors'):
                        raise ValueError('W&B GraphQL errors')
                    runs = page['data']['project']['runs']
                    edges = runs['edges']
                    if not isinstance(edges, list) or len(edges) > first:
                        raise ValueError('W&B page exceeded requested node count')
                    page_info = runs.get('pageInfo')
                    if not isinstance(page_info, dict) or type(page_info.get('hasNextPage')) is not bool:
                        raise ValueError('invalid W&B page completion flag')
                    has_next = page_info['hasNextPage']
                    next_cursor = page_info.get('endCursor')
                    if has_next and (not isinstance(next_cursor, str) or not next_cursor
                                     or next_cursor in seen or not edges):
                        raise ValueError('invalid or repeating W&B cursor')
                    # Validate page completeness before accepting this page's
                    # records. Prior valid pages remain available on failure.
                    for edge in edges:
                        node = edge['node']
                        summary = node['summaryMetrics']
                        if isinstance(summary, str):
                            summary = unique_json(summary)
                        snapshot = summary.get('kev_laya/snapshot')
                        count += 1
                        if snapshot is not None:
                            identity = {'entity': source['entity'], 'project': source['project'], 'run_id': node['name']}
                            # Entire containing response was already charged; do not double count.
                            append(json.dumps(snapshot, allow_nan=False, ensure_ascii=False).encode(), source, node['state'], identity)
                    if not has_next:
                        break
                    cursor = next_cursor
                    seen.add(cursor)
                    if count >= source['limit']:
                        status = 'partial_limit'
                        warnings.append({'source_id': source_id, 'code': 'source_listing_limit'})
        except BudgetStop as exc:
            stop_reason = str(exc)
            status = 'partial_' + stop_reason if len(records) > before_records else 'skipped_' + stop_reason
            warnings.append({'source_id': source_id, 'code': stop_reason})
        except Exception as exc:
            status, error_type = 'error', type(exc).__name__
            warnings.append({'source_id': source_id, 'code': 'source_unavailable_or_invalid', 'error_type': error_type})
        statuses.append({'source_id': source_id, 'status': status, 'payload_bytes': budget.bytes - before_bytes,
                         'requests': budget.requests - before_requests, 'refreshed_at': now.isoformat() if status == 'ok' else None})
    return {'records': records, 'warnings': warnings, 'configured_ids': [s['id'] for s in sources],
            'registry_complete': True, 'complete': all(s['status'] == 'ok' for s in statuses), 'sources': statuses,
            'accounting': {'payload_bytes': budget.bytes, 'payload_limit': budget.limits.payload_bytes,
                           'requests': budget.requests, 'pages': budget.pages, 'stream_reads': budget.reads,
                           'registry_bytes': len(raw_registry), 'registry_limit': 131072,
                           'elapsed_seconds': max(0., clock() - budget.start), 'stop_reason': stop_reason,
                           'payload_convention': 'local/S3 exposed bytes; entire W&B JSON response charged including envelope',
                           'transport_overhead_bytes': None, 'deadline_kind': 'cooperative checks before calls/reads plus bounded network timeouts; in-flight I/O may finish after deadline'}}
