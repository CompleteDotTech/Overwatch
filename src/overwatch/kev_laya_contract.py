"""Kev-Laya telemetry v1/v2: standalone stdlib contract; safe to vendor in Overwatch.

Copyright 2026 Kev-Laya contributors. SPDX-License-Identifier: Apache-2.0
No imports from the model, torch, training, cloud SDKs or Overwatch.
"""
from __future__ import annotations
import hashlib
import json
import math
import re
from datetime import datetime, timezone
from urllib.parse import urlsplit

SCHEMA_VERSION = 2
SUPPORTED_VERSIONS = {1, 2}
MAX_ATTEMPTS = 64
MAX_SNAPSHOT_BYTES = 262144
MAX_HISTORY = 128
MAX_ARTIFACTS = 32
PHASES = {"prepare", "train", "calibrate", "evaluate", "serve", "completed", "failed", "cancelled"}
METRICS = {"loss/total", "loss/ce", "loss/policy_gradient", "loss/ordinal", "reward/proper",
           "validation/nll", "validation/brier", "validation/ece", "validation/accuracy", "validation/ordinal_mae",
           "gradient/norm", "resource/rss_bytes", "resource/gpu_peak_bytes", "learning_rate"}
COUNTERS = {"optimizer_steps", "microbatches", "examples", "forward_tokens"}
IDENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,159}$")
HEX = re.compile(r"^[a-f0-9]{64}$")
TOP = {"schema_version", "framework", "framework_version", "experiment_id", "run_id", "attempt_id", "attempt_index",
       "parent_attempt_id", "sequence", "started_at", "updated_at", "heartbeat_at", "phase", "scheduler", "wandb",
       "progress", "metrics", "history", "provenance", "artifacts", "serving", "resume", "recoveries", "cost",
       "monitoring_export_failures"}


class TelemetryError(ValueError):
    pass


def require(condition: bool, message: str):
    if not condition:
        raise TelemetryError(message)


def identifier(value, name="identifier"):
    require(isinstance(value, str) and IDENT.fullmatch(value) is not None, f"invalid {name}")


def number(value, name, *, integer=False, nullable=False, minimum=0):
    if value is None and nullable:
        return
    require(not isinstance(value, bool) and isinstance(value, int if integer else (int, float)), f"invalid {name}")
    require(math.isfinite(value) and value >= minimum, f"invalid {name}")


def fields(obj, allowed, required=None):
    require(isinstance(obj, dict), "object required")
    require(not (set(obj) - set(allowed)), "unknown fields")
    require(not (set(allowed if required is None else required) - set(obj)), "missing fields")


def timestamp(value):
    require(isinstance(value, str) and len(value) <= 40, "invalid timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise TelemetryError("invalid timestamp") from exc
    require(result.tzinfo is not None and result.utcoffset() is not None, "timezone required")
    return result.astimezone(timezone.utc)


def uri(value):
    require(isinstance(value, str) and len(value) <= 2048, "invalid URI")
    parsed = urlsplit(value)
    require(parsed.scheme in {"https", "http", "file", "s3", "gs"}, "unsupported URI scheme")
    require(not parsed.username and not parsed.password and not parsed.query and not parsed.fragment,
            "credential/query/fragment-bearing URI forbidden")
    require(not any(ord(c) < 32 for c in value), "control character in URI")


def metrics(obj):
    fields(obj, METRICS, required=set())
    for name, value in obj.items():
        number(value, name, minimum=-1e30 if name.startswith(("reward/", "loss/policy", "loss/total")) else 0)


def validate_snapshot(data: dict, *, now: datetime | None = None) -> dict:
    version = data.get("schema_version")
    require(type(version) is int and version in SUPPORTED_VERSIONS, "schema version mismatch")
    fields(data, TOP | ({"extensions"} if version == 2 else set()))
    require(data["framework"] == "kev_laya", "framework mismatch")
    require(isinstance(data["framework_version"], str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+:/-]{0,159}", data["framework_version"]), "invalid framework version")
    for key in ("experiment_id", "run_id", "attempt_id"):
        identifier(data[key], key)
    for key in ("attempt_index", "sequence", "monitoring_export_failures"):
        number(data[key], key, integer=True)
    if data["parent_attempt_id"] is not None:
        identifier(data["parent_attempt_id"])
        require(data["parent_attempt_id"] != data["attempt_id"], "attempt cannot be its own parent")
    require((data["attempt_index"] == 0) == (data["parent_attempt_id"] is None), "attempt lineage mismatch")
    start, updated, heartbeat = (timestamp(data[k]) for k in ("started_at", "updated_at", "heartbeat_at"))
    require(start <= heartbeat <= updated, "timestamp ordering mismatch")
    if now is not None:
        require((updated - now).total_seconds() <= 30, "future snapshot")
    require(data["phase"] in PHASES, "invalid phase")
    scheduler = data["scheduler"]
    if scheduler is not None:
        fields(scheduler, {"provider", "namespace", "job_id"})
        require(scheduler["provider"] == "skypilot", "unsupported scheduler")
        identifier(scheduler["namespace"])
        number(scheduler["job_id"], "scheduler job_id", integer=True)
    wandb = data["wandb"]
    if wandb is not None:
        fields(wandb, {"entity", "project", "run_id"})
        for val in wandb.values():
            identifier(val)
            require("/" not in val, "W&B ID component contains slash")
    progress = data["progress"]
    fields(progress, COUNTERS | {"epoch", "totals", "elapsed_seconds", "eta_seconds", "tokens_per_second"})
    for key in COUNTERS:
        number(progress[key], key, integer=True)
    fields(progress["totals"], COUNTERS, required=set())
    for key, value in progress["totals"].items():
        number(value, key, integer=True, nullable=True)
        if value is not None:
            require(progress[key] <= value, "counter exceeds declared total")
    for key in ("epoch", "elapsed_seconds", "eta_seconds", "tokens_per_second"):
        number(progress[key], key, nullable=True)
    metrics(data["metrics"])
    require(isinstance(data["history"], list) and len(data["history"]) <= MAX_HISTORY, "history bound exceeded")
    previous_step = -1
    for row in data["history"]:
        fields(row, {"step", "phase", "metrics"})
        number(row["step"], "history step", integer=True)
        require(row["step"] >= previous_step, "history order invalid")
        previous_step = row["step"]
        require(row["phase"] in PHASES, "invalid history phase")
        metrics(row["metrics"])
    provenance = data["provenance"]
    pfields = {"model", "backbone", "backbone_revision", "tokenizer", "config_sha256", "data_sha256", "split_hashes",
               "seed", "repository", "commit", "precision", "hardware", "context_limits", "evidence_class"}
    fields(provenance, pfields)
    for key in ("model", "backbone", "backbone_revision", "tokenizer", "precision", "hardware", "evidence_class"):
        require(isinstance(provenance[key], str) and len(provenance[key]) <= 240, "invalid provenance string")
    for key in ("config_sha256", "data_sha256"):
        if version == 2 and provenance[key] is None:
            continue
        require(isinstance(provenance[key], str) and HEX.fullmatch(provenance[key]) is not None, "invalid hash")
    fields(provenance["split_hashes"], {"train", "development", "calibration", "test"}, required=set())
    for val in provenance["split_hashes"].values():
        require(isinstance(val, str) and HEX.fullmatch(val) is not None, "invalid split hash")
    number(provenance["seed"], "seed", integer=True)
    if provenance["repository"] is not None:
        uri(provenance["repository"])
    if provenance["commit"] is not None:
        require(isinstance(provenance["commit"], str) and re.fullmatch(r"[a-f0-9]{40,64}", provenance["commit"]) is not None, "invalid commit")
    fields(provenance["context_limits"], {"branch", "aggregate"})
    for value in provenance["context_limits"].values():
        number(value, "context limit", integer=True, minimum=1)
    require(isinstance(data["artifacts"], list) and len(data["artifacts"]) <= MAX_ARTIFACTS, "artifact bound exceeded")
    for artifact in data["artifacts"]:
        fields(artifact, {"kind", "uri", "sha256", "size_bytes", "parent_sha256", "resumable"})
        require(artifact["kind"] in {"checkpoint", "configuration", "evaluation", "manifest"}, "artifact kind invalid")
        uri(artifact["uri"])
        for key in ("sha256", "parent_sha256"):
            require((key == "parent_sha256" and artifact[key] is None) or
                    (isinstance(artifact[key], str) and HEX.fullmatch(artifact[key]) is not None), "invalid artifact hash")
        number(artifact["size_bytes"], "artifact bytes", integer=True)
        require(type(artifact["resumable"]) is bool, "invalid resumable flag")
    serving = data["serving"]
    sfields = {"requests", "errors", "input_tokens", "forward_tokens", "output_tokens", "questions", "prefix_reuses", "latency_ms"}
    fields(serving, sfields)
    for key in sfields - {"latency_ms"}:
        number(serving[key], key, integer=True)
    require(serving["errors"] <= serving["requests"], "serving errors exceed requests")
    require(isinstance(serving["latency_ms"], list) and len(serving["latency_ms"]) <= MAX_HISTORY, "latency bound exceeded")
    for value in serving["latency_ms"]:
        number(value, "latency")
    fields(data["resume"], {"capable", "checkpoint_sha256"})
    require(type(data["resume"]["capable"]) is bool, "resume flag invalid")
    resume_hash = data["resume"]["checkpoint_sha256"]
    require(resume_hash is None or (isinstance(resume_hash, str) and HEX.fullmatch(resume_hash) is not None), "resume hash invalid")
    require(not data["resume"]["capable"] or resume_hash is not None, "resume capability needs checkpoint")
    fields(data["recoveries"], {"total", "infrastructure", "application"})
    for key, value in data["recoveries"].items():
        number(value, key, nullable=True, integer=True)
    fields(data["cost"], {"measured_usd", "estimated_usd", "basis"})
    for key in ("measured_usd", "estimated_usd"):
        number(data["cost"][key], key, nullable=True)
    require(data["cost"]["basis"] is None or data["cost"]["basis"] in {"provider_billing", "configured_hourly_rate", "metered_local"}, "unknown cost basis")
    if version == 2:
        validate_extensions(data["extensions"], data)
    try:
        encoded = json.dumps(data, ensure_ascii=False, allow_nan=False).encode()
    except (ValueError, TypeError) as exc:
        raise TelemetryError("snapshot is not finite JSON") from exc
    require(len(encoded) <= MAX_SNAPSHOT_BYTES, "snapshot byte bound exceeded")
    return data


def decode_snapshot(raw: bytes, *, now: datetime | None = None) -> dict:
    require(len(raw) <= MAX_SNAPSHOT_BYTES, "snapshot byte bound exceeded")
    def pairs(items):
        obj = {}
        for key, value in items:
            require(key not in obj, "duplicate JSON key")
            obj[key] = value
        return obj
    try:
        data = json.loads(raw, object_pairs_hook=pairs)
    except (ValueError, UnicodeError) as exc:
        raise TelemetryError("invalid telemetry JSON") from exc
    return validate_snapshot(data, now=now)


ATTEMPT_FIELDS = {"experiment_id", "run_id", "attempt_id", "attempt_index", "parent_attempt_id", "previous_sha256", "sha256"}


def attempt_digest(record: dict) -> str:
    content = {k: v for k, v in record.items() if k != "sha256"}
    return hashlib.sha256(json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def validate_attempt_records(records: list, experiment_id: str, run_id: str) -> None:
    require(isinstance(records, list) and len(records) <= MAX_ATTEMPTS, "attempt receipts bound exceeded")
    previous = None
    seen = set()
    for record in records:
        fields(record, ATTEMPT_FIELDS)
        require(record["experiment_id"] == experiment_id and record["run_id"] == run_id, "receipt run identity mismatch")
        identifier(record["attempt_id"])
        require(record["attempt_id"] not in seen, "duplicate attempt receipt")
        seen.add(record["attempt_id"])
        number(record["attempt_index"], "attempt index", integer=True)
        require(record["sha256"] == attempt_digest(record), "attempt receipt hash mismatch")
        if record["attempt_index"] == 0:
            require(record["parent_attempt_id"] is None and record["previous_sha256"] is None, "root attempt receipt mismatch")
        else:
            identifier(record["parent_attempt_id"])
            require(record["parent_attempt_id"] != record["attempt_id"], "self parent receipt")
            require(isinstance(record["previous_sha256"], str) and HEX.fullmatch(record["previous_sha256"]), "missing receipt predecessor hash")
        if previous is not None:
            require(record["attempt_index"] == previous["attempt_index"] + 1 and
                    record["parent_attempt_id"] == previous["attempt_id"] and
                    record["previous_sha256"] == previous["sha256"], "nonconsecutive receipt chain")
        previous = record


def validate_extensions(ext: dict, snapshot: dict) -> None:
    fields(ext, {"serialization", "source", "attempt_lineage", "lineage_durable", "execution", "resources", "calibration"},
           required={"serialization", "source", "attempt_lineage", "lineage_durable", "execution", "resources"})
    calibration = ext.get("calibration")
    if calibration is not None:
        fields(calibration, {"status", "method", "split_sha256", "per_type"})
        require(isinstance(calibration["status"], str) and len(calibration["status"]) <= 80, "invalid calibration status")
        require(calibration["method"] is None or isinstance(calibration["method"], str) and len(calibration["method"]) <= 80, "invalid calibration method")
        require(calibration["split_sha256"] is None or isinstance(calibration["split_sha256"], str) and HEX.fullmatch(calibration["split_sha256"]), "invalid calibration split hash")
        fields(calibration["per_type"], {"choice", "score", "noul"}, required=set())
        for row in calibration["per_type"].values():
            fields(row, {"temperature", "count", "status", "before_nll", "after_nll"})
            number(row["temperature"], "temperature", minimum=.2)
            require(row["temperature"] <= 5, "temperature outside fitted bounds")
            number(row["count"], "calibration count", integer=True)
            require(isinstance(row["status"], str) and len(row["status"]) <= 80, "invalid fit status")
            for name in ("before_nll", "after_nll"):
                number(row[name], name, nullable=True)

    require(ext["lineage_durable"] is None or type(ext["lineage_durable"]) is bool, "invalid lineage durability")
    fields(ext["serialization"], {"tokenizer", "serialization", "literal_encoding"})
    for value in ext["serialization"].values():
        require(isinstance(value, str) and len(value) <= 240, "invalid serialization identity")
    require(ext["serialization"]["tokenizer"] == snapshot["provenance"]["tokenizer"], "serialization tokenizer mismatch")
    source = ext["source"]
    fields(source, {"source_tree_sha256", "archive_sha256", "git_dirty", "git_status", "package_version"})
    for key in ("source_tree_sha256", "archive_sha256"):
        require(source[key] is None or (isinstance(source[key], str) and HEX.fullmatch(source[key])), "invalid source hash")
    require(source["git_dirty"] is None or type(source["git_dirty"]) is bool, "invalid dirty flag")
    require(source["git_status"] in {"verified", "unavailable", "error"}, "invalid git source status")
    require(isinstance(source["package_version"], str) and len(source["package_version"]) <= 80, "invalid package version")
    records = ext["attempt_lineage"]
    validate_attempt_records(records, snapshot["experiment_id"], snapshot["run_id"])
    if records:
        for key in ("attempt_id", "attempt_index", "parent_attempt_id"):
            require(records[-1][key] == snapshot[key], "receipt/current attempt mismatch")
    execution = ext["execution"]
    if execution is not None:
        keys = {"prefix_passes", "branch_passes", "compute_tokens", "padding_tokens", "max_batch_size", "batch_size_histogram", "useful_forward_tokens", "questions"}
        fields(execution, keys)
        for key in keys - {"batch_size_histogram"}:
            number(execution[key], key, integer=True)
        hist = execution["batch_size_histogram"]
        require(isinstance(hist, dict) and len(hist) <= 1024, "batch histogram bound")
        for key, val in hist.items():
            require(isinstance(key, str) and key.isdigit() and 1 <= int(key) <= 1024, "invalid batch size")
            number(val, "batch count", integer=True)
        require(sum(hist.values()) == execution["branch_passes"], "branch pass/histogram mismatch")
        require(sum(int(k)*v for k,v in hist.items()) == execution["questions"], "question count/histogram mismatch")
        require(execution["compute_tokens"] == execution["useful_forward_tokens"] + execution["padding_tokens"], "compute token accounting mismatch")
    resource = ext["resources"]
    if resource is not None:
        fields(resource, {"sampled_at", "interval_seconds", "rss_bytes", "gpu_allocated_bytes", "gpu_peak_allocated_bytes", "units", "unavailable"})
        timestamp(resource["sampled_at"])
        number(resource["interval_seconds"], "sampling interval", minimum=0.001)
        for key in ("rss_bytes", "gpu_allocated_bytes", "gpu_peak_allocated_bytes"):
            number(resource[key], key, nullable=True, integer=True)
        require(resource["units"] == "bytes", "invalid resource units")
        require(isinstance(resource["unavailable"], list) and set(resource["unavailable"]) <= {"rss", "cuda"}, "invalid unavailable resource")
