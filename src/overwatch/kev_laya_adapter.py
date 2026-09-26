"""Versioned run identity/ordering merge and cache-only presentation. No provider I/O."""
from __future__ import annotations
import copy
from datetime import datetime, timezone
import hashlib
import json
from urllib.parse import urlsplit
from overwatch.kev_laya_contract import validate_snapshot, timestamp

CACHE_VERSION = "kev_laya/raw/2"
READABLE_CACHE_VERSIONS = {"kev_laya/raw/1", CACHE_VERSION}


def key(snapshot: dict) -> str:
    return snapshot["experiment_id"] + "\0" + snapshot["run_id"]


def digest(snapshot: dict) -> str:
    return hashlib.sha256(json.dumps(snapshot, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def merge_snapshots(previous: dict | None, collected: dict, *, now: datetime | None = None, retention_days=14) -> dict:
    """Keep the highest verified position and separately retain disputed observations.

    The collector owns this envelope. No exporter, network call, or directory walk
    is made here. Missing/partial registries are not implicit revocations.
    """
    now = now or datetime.now(timezone.utc)
    warnings = list(collected.get("warnings", []))
    previous = previous if isinstance(previous, dict) else {}
    if previous and previous.get("schema_version") not in READABLE_CACHE_VERSIONS:
        warnings.append({"code": "unsupported_prior_cache_version"})
        previous = {}
    prior_records = previous.get("records", [])
    if not isinstance(prior_records, list):
        prior_records = []
        warnings.append({"code": "invalid_prior_cache_records"})
    allowed = set(collected.get("configured_ids", []))
    if not collected.get("registry_complete", True):
        for rec in prior_records:
            if isinstance(rec, dict) and isinstance(rec.get("source_ids"), list):
                allowed.update(x for x in rec["source_ids"] if isinstance(x, str))
    records, observations = {}, []
    for rec in prior_records:
        try:
            snapshot = validate_snapshot(rec["snapshot"], now=now)
            sources = sorted(set(rec["source_ids"]) & allowed)
            age = (now - timestamp(snapshot["heartbeat_at"])).total_seconds()
            if not isinstance(rec.get("provider_status"), dict) or not isinstance(rec.get("transports"), list):
                raise ValueError("invalid cache wrapper")
            if sources and age <= retention_days * 86400:
                records[key(snapshot)] = copy.deepcopy(rec) | {"source_ids": sources}
        except (ValueError, KeyError, TypeError):
            warnings.append({"code": "invalid_prior_cache_record"})
    old_observations = previous.get("observations", [])
    if not isinstance(old_observations, list):
        old_observations = []
        warnings.append({"code": "invalid_prior_cache_observations"})
    for item in old_observations:
        try:
            snap = validate_snapshot(item["snapshot"], now=now)
            if item["source_id"] in allowed and isinstance(item["reason"], str) and (
                now - timestamp(snap["heartbeat_at"])).total_seconds() <= retention_days * 86400:
                observations.append(copy.deepcopy(item))
        except (ValueError, KeyError, TypeError):
            warnings.append({"code": "invalid_prior_cache_observation"})

    def position(snap):
        return snap["attempt_index"], snap["sequence"]

    def unverified(candidate, code):
        snap = candidate["snapshot"]
        observations[:] = [o for o in observations if not (o["source_id"] == candidate["source_id"] and
                            key(o["snapshot"]) == key(snap) and position(o["snapshot"]) <= position(snap))]
        observations.append({"snapshot": copy.deepcopy(snap), "source_id": candidate["source_id"],
                             "transport": candidate["transport"], "reason": code, "received_at": now.isoformat()})

    grouped = {}
    for candidate in collected.get("records", []):
        try:
            snapshot = validate_snapshot(candidate["snapshot"], now=now)
            if (now - timestamp(snapshot["heartbeat_at"])).total_seconds() > retention_days * 86400:
                warnings.append({"code": "expired_snapshot_ignored", "run_id": snapshot["run_id"]})
                continue
            if candidate["source_id"] not in allowed or candidate["transport"] not in {"local", "s3", "wandb"}:
                raise ValueError("unregistered source/transport")
            provider = candidate.get("provider_status")
            if provider is not None and (not isinstance(provider, str) or len(provider) > 128):
                raise ValueError("invalid provider status")
            grouped.setdefault(key(snapshot), []).append(candidate)
        except (ValueError, KeyError, TypeError):
            warnings.append({"code": "invalid_candidate"})
    for run_key, candidates in grouped.items():
        prior = records.get(run_key)
        positions, attempt_ids, conflict = {}, {}, False
        for candidate in candidates:
            snap = candidate["snapshot"]
            pos, identity = position(snap), (snap["attempt_id"], digest(snap))
            conflict |= pos in positions and positions[pos] != identity
            positions[pos] = identity
            attempt_ids.setdefault(snap["attempt_index"], set()).add(snap["attempt_id"])
        if conflict or any(len(ids) > 1 for ids in attempt_ids.values()):
            warnings.append({"run_id": candidates[0]["snapshot"]["run_id"], "code": "conflicting_transport_snapshots"})
            for candidate in candidates:
                unverified(candidate, "conflicting_transport_snapshots")
            continue
        for candidate in sorted(candidates, key=lambda c: position(c["snapshot"])):
            snap = candidate["snapshot"]
            current = prior["snapshot"] if prior else None
            code = None
            if current is None:
                if not verified_attempt_transition(None, snap):
                    code = "unverified_attempt_lineage"
            elif snap["attempt_index"] < current["attempt_index"]:
                code = "older_attempt_ignored"
            elif snap["attempt_index"] == current["attempt_index"]:
                if snap["attempt_id"] != current["attempt_id"]:
                    code = "attempt_identity_conflict"
                elif snap["sequence"] < current["sequence"]:
                    code = "older_sequence_ignored"
                elif snap["sequence"] == current["sequence"] and digest(snap) != digest(current):
                    code = "sequence_payload_conflict"
                elif timestamp(snap["heartbeat_at"]) < timestamp(current["heartbeat_at"]):
                    code = "heartbeat_regression"
                elif any(snap["progress"][k] < current["progress"][k] for k in ("optimizer_steps", "microbatches", "examples", "forward_tokens")):
                    code = "progress_regression"
                elif any(snap[k] != current[k] for k in ("started_at", "provenance", "scheduler", "wandb")):
                    code = "attempt_metadata_conflict"
                elif current.get("extensions") and any(snap.get("extensions", {}).get(k) != current["extensions"].get(k)
                                                        for k in ("source", "serialization", "attempt_lineage")):
                    code = "attempt_metadata_conflict"
                elif current["phase"] in {"completed", "failed", "cancelled"} and snap["phase"] != current["phase"]:
                    code = "terminal_phase_regression"
            elif not verified_attempt_transition(current, snap):
                code = "unverified_attempt_lineage"
            elif snap["wandb"] != current["wandb"]:
                code = "run_wandb_identity_conflict"
            if code:
                warnings.append({"run_id": snap["run_id"], "code": code})
                if code not in {"older_attempt_ignored", "older_sequence_ignored"}:
                    unverified(candidate, code)
                continue
            # A strictly newer, independently verified observation resolves an
            # older conflict. Equal-position disagreement is never erased merely
            # because one transport is temporarily unavailable.
            observations[:] = [o for o in observations if not (key(o["snapshot"]) == key(snap) and
                              (position(o["snapshot"]) < position(snap) or
                               (digest(o["snapshot"]) == digest(snap) and o["reason"] != "conflicting_transport_snapshots")))]
            source_ids = sorted({candidate["source_id"], *(prior["source_ids"] if prior else [])})
            transports = sorted({candidate["transport"], *(prior["transports"] if prior else [])})
            provider_status = dict(prior.get("provider_status", {}) if prior else {})
            if candidate.get("provider_status") is not None:
                provider_status[candidate["transport"]] = candidate["provider_status"]
            prior = {"snapshot": copy.deepcopy(snap), "source_ids": source_ids, "transports": transports,
                     "provider_status": provider_status, "received_at": now.isoformat()}
        if prior:
            records[run_key] = prior
    previous_source_rows = previous.get("collection_sources", [])
    if not isinstance(previous_source_rows, list):
        previous_source_rows = []
        warnings.append({"code": "invalid_prior_collection_sources"})
    previous_sources = {s["source_id"]: s for s in previous_source_rows
                        if isinstance(s, dict) and isinstance(s.get("source_id"), str)}
    new_sources = {s["source_id"]: s for s in collected.get("sources", [])
                   if isinstance(s, dict) and isinstance(s.get("source_id"), str)}
    collection_sources = []
    for source_id in sorted(allowed):
        old = previous_sources.get(source_id, {})
        incoming = new_sources.get(source_id, {"source_id": source_id, "status": "skipped_unavailable",
                                               "refreshed_at": None, "payload_bytes": 0, "requests": 0})
        latest = incoming.get("refreshed_at") or old.get("last_successful_refresh_at") or old.get("refreshed_at")
        collection_sources.append(dict(incoming, last_successful_refresh_at=latest))
    if len(observations) > 64:
        warnings.append({"code": "unverified_observation_retention_bound", "retained": 64})
    return {"schema_version": CACHE_VERSION, "updated_at": now.isoformat(),
            "records": [records[k] for k in sorted(records)], "warnings": warnings[-256:],
            "observations": observations[-64:], "collection_sources": collection_sources,
            "collection_accounting": collected.get("accounting", {}),
            "registry_complete": collected.get("registry_complete", True)}


def verified_attempt_transition(current: dict | None, candidate: dict) -> bool:
    """An index is not authority: require a trusted observed parent or hashed receipts.

    Snapshot validation already checked each receipt's fields, hash, consecutive
    index/parent links, and final binding to this exact current attempt. Receipts
    are integrity evidence on trusted transports, not third-party signatures.
    """
    receipts = candidate.get("extensions", {}).get("attempt_lineage", [])
    if current is None:
        return candidate["attempt_index"] == 0 or bool(receipts and receipts[0]["attempt_index"] == 0)
    known_receipts = current.get("extensions", {}).get("attempt_lineage", [])
    known_hash = known_receipts[-1]["sha256"] if known_receipts else None
    if receipts:
        for row in receipts:
            if row["attempt_index"] == current["attempt_index"]:
                return (row["attempt_id"] == current["attempt_id"] and
                        row["parent_attempt_id"] == current["parent_attempt_id"] and
                        (known_hash is None or row["sha256"] == known_hash))
        first = receipts[0]
        if first["attempt_index"] == current["attempt_index"] + 1:
            return first["parent_attempt_id"] == current["attempt_id"] and (
                known_hash is None or first["previous_sha256"] == known_hash)
        return False
    # Compatible historical direct-parent v1 snapshots remain readable. A v2
    # writer with lost receipt storage may also provide exactly one observed
    # parent link, but never skip unobserved intervening attempts without proof.
    return candidate["attempt_index"] == current["attempt_index"] + 1 and candidate["parent_attempt_id"] == current["attempt_id"]

def presentation(envelope: dict, jobs: list | None = None, *, namespace: str | None = None,
                 now: datetime | None = None, stale_seconds=120) -> dict:
    """Interpret only already cached records. Sky status is authoritative, never inferred."""
    now = now or datetime.now(timezone.utc)
    if not isinstance(envelope, dict) or envelope.get("schema_version") not in READABLE_CACHE_VERSIONS:
        return {"runs": [], "warnings": [{"code": "missing_or_invalid_model_run_cache"}]}
    jobs = jobs or []
    index = {}
    for job in jobs:
        raw = job if isinstance(job, dict) else vars(job)
        ns = raw.get("scheduler_namespace") or namespace
        if ns is not None:
            index.setdefault((ns, raw.get("job_id")), []).append(raw)
    runs, warnings = [], list(envelope.get("warnings", [])) if isinstance(envelope.get("warnings", []), list) else [{"code": "invalid_cached_warnings"}]
    def cache_list(name):
        value = envelope.get(name, [])
        if not isinstance(value, list):
            warnings.append({"code": "invalid_cached_" + name})
            return []
        return value
    wb_owners = {}
    valid_records = []
    for rec in cache_list("records"):
        try:
            snap = validate_snapshot(rec["snapshot"], now=now)
            if not isinstance(rec.get("provider_status"), dict) or not isinstance(rec.get("source_ids"), list) or not isinstance(rec.get("transports"), list):
                raise ValueError("invalid cached wrapper")
        except (ValueError, KeyError, TypeError):
            warnings.append({"code": "invalid_cached_snapshot"})
            continue
        valid_records.append((rec, snap))
        wb = snap.get("wandb")
        if wb:
            wb_owners.setdefault(tuple(wb[k] for k in ("entity", "project", "run_id")), set()).add(key(snap))
    observation_by_run = {}
    for observation in cache_list("observations"):
        try:
            validate_snapshot(observation["snapshot"], now=now)
            if not all(isinstance(observation.get(k), str) for k in ("source_id", "transport", "reason")):
                raise ValueError("invalid unverified wrapper")
            observation_by_run.setdefault(key(observation["snapshot"]), []).append(observation)
        except (ValueError, KeyError, TypeError):
            warnings.append({"code": "invalid_unverified_observation"})
    verified_keys = {key(snap) for _, snap in valid_records}
    # New but unverifiable observations must not disappear just because no root
    # has reached the collector. They are displayed with explicit uncertainty.
    for run_key, observations in observation_by_run.items():
        if run_key not in verified_keys:
            newest = max(observations, key=lambda o: (o["snapshot"]["attempt_index"], o["snapshot"]["sequence"]))
            valid_records.append(({"provider_status": {}, "source_ids": [newest["source_id"]],
                                  "transports": [newest["transport"]], "unverified_only": True}, newest["snapshot"]))
    for rec, snap in valid_records:
        link = snap["scheduler"]
        candidates = index.get((link["namespace"], link["job_id"]), []) if link else []
        status = "local" if link is None else "matched" if len(candidates) == 1 else "ambiguous" if candidates else "unmatched"
        resource = candidates[0] if status == "matched" else None
        wb = snap["wandb"]
        if wb and len(wb_owners.get(tuple(wb[k] for k in ("entity", "project", "run_id")), set())) > 1:
            warnings.append({"code": "wandb_identity_conflict", "run_id": snap["run_id"]})
        age = max(0., (now - timestamp(snap["heartbeat_at"])).total_seconds())
        repo, commit = snap["provenance"]["repository"], snap["provenance"]["commit"]
        git_url = f"{repo.rstrip('/')}/commit/{commit}" if repo and commit and urlsplit(repo).scheme == "https" else None
        total_recoveries = resource.get("recovery_count") if resource else snap["recoveries"]["total"]
        phase = snap["phase"]
        pending = observation_by_run.get(key(snap), [])
        summaries = [{"attempt_id": o["snapshot"]["attempt_id"], "attempt_index": o["snapshot"]["attempt_index"],
                      "parent_attempt_id": o["snapshot"]["parent_attempt_id"], "sequence": o["snapshot"]["sequence"],
                      "phase": o["snapshot"]["phase"], "heartbeat_at": o["snapshot"]["heartbeat_at"],
                      "reason": o["reason"], "source_id": o["source_id"]} for o in pending]
        verification = "unverified" if rec.get("unverified_only") else "last_verified_with_newer_uncertainty" if pending else "verified"
        runs.append({"experiment_id": snap["experiment_id"], "run_id": snap["run_id"], "attempt_id": snap["attempt_id"],
                     "attempt_index": snap["attempt_index"], "parent_attempt_id": snap["parent_attempt_id"], "sequence": snap["sequence"],
                     "phase": phase, "verification": verification, "unverified_observations": summaries,
                     "extensions": snap.get("extensions"),
                     "collection_sources": [v for v in cache_list("collection_sources") if isinstance(v, dict) and v.get("source_id") in rec["source_ids"]],
                     "provider_status": {"skypilot": str(getattr(resource.get("status"), "value", resource.get("status"))) if resource and resource.get("status") is not None else None,
                                                          "wandb": rec["provider_status"].get("wandb")},
                     "association": {"status": status, "scheduler": link},
                     "heartbeat_at": snap["heartbeat_at"], "heartbeat_age_seconds": age,
                     "freshness": "terminal" if phase in {"completed", "failed", "cancelled"} else "stale" if age > stale_seconds else "fresh",
                     "progress": snap["progress"], "metrics": snap["metrics"], "history": snap["history"], "resume": snap["resume"],
                     "artifacts": snap["artifacts"], "provenance": snap["provenance"], "serving": snap["serving"],
                     "recoveries": {"total": total_recoveries, "infrastructure": None if resource else snap["recoveries"]["infrastructure"],
                                    "application": None if resource else snap["recoveries"]["application"]},
                     "cost": snap["cost"], "git_url": git_url,
                     "wandb_url": f"https://wandb.ai/{wb['entity']}/{wb['project']}/runs/{wb['run_id']}" if wb else None,
                     "source_ids": rec["source_ids"], "transports": rec["transports"],
                     "monitoring_export_failures": snap["monitoring_export_failures"]})
    return {"runs": runs, "warnings": warnings}
