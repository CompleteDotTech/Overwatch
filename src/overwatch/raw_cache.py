"""Versioned raw-cache paths and atomic file operations."""

from __future__ import annotations

import json
import math
import shutil
from datetime import UTC, date, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from overwatch.utils import isoformat

RAW_CACHE_ROOT = Path.home() / ".cache" / "overwatch" / "raw-v1"


def raw_cache_path(*parts: str) -> Path:
    """Return a path inside the active raw-cache specification."""
    return RAW_CACHE_ROOT.joinpath(*parts)


def json_safe(value: Any) -> Any:
    """Preserve SDK data while converting it to JSON-compatible primitives."""
    if hasattr(value, "model_dump"):
        return json_safe(value.model_dump(mode="json"))
    if isinstance(value, dict):
        return {str(key): json_safe(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(child) for child in value]
    if isinstance(value, Enum):
        return json_safe(value.value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def write_json_atomically(path: Path, value: Any) -> None:
    """Write one private JSON snapshot without exposing a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(f"{path.suffix}.tmp")
    temporary_path.write_text(
        json.dumps(json_safe(value), indent=2, sort_keys=True) + "\n"
    )
    temporary_path.chmod(0o600)
    temporary_path.replace(path)


def cache_train_config(job_id: int, config_uri: str) -> None:
    """Materialize one discovered raw TrainConfig beside its job cache."""
    from typesafe.tspath import TSPath

    train_config_path = raw_cache_path("jobs", str(job_id), "train_config.yaml")
    source_path = raw_cache_path("jobs", str(job_id), "train_config.source.json")
    error_path = raw_cache_path("jobs", str(job_id), "train_config.error.json")
    if train_config_path.exists():
        return
    try:
        tspath = TSPath(config_uri)
        tspath.sync_down()
        train_config_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = train_config_path.with_suffix(".yaml.tmp")
        shutil.copyfile(tspath.local, temporary_path)
        temporary_path.chmod(0o600)
        temporary_path.replace(train_config_path)
        write_json_atomically(
            source_path,
            {"uri": config_uri, "updated_at": isoformat(datetime.now(UTC))},
        )
        error_path.unlink(missing_ok=True)
    except Exception as error:  # noqa: BLE001 - storage backends vary by URI
        write_json_atomically(
            error_path,
            {
                "uri": config_uri,
                "error": f"{type(error).__name__}: {error}",
                "updated_at": isoformat(datetime.now(UTC)),
            },
        )


def read_json(path: Path, default: Any = None) -> Any:
    """Read a cache file, returning a caller-provided value when absent."""
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return default
