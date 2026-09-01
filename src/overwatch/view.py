"""Report data preparation shared with the React frontend."""

import json
from typing import Any


def flatten_config_fields(value: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten nested config mappings while keeping lists as comparable values."""
    if not isinstance(value, dict):
        return {prefix: value}
    flattened: dict[str, Any] = {}
    for key, child in value.items():
        field = f"{prefix}.{key}" if prefix else str(key)
        flattened.update(flatten_config_fields(child, field))
    return flattened


def json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def config_differences(
    records: list[dict[str, Any]], configs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Build categorical config differences grouped by W&B project."""
    rows = [
        (record, flatten_config_fields(config))
        for record, config in zip(records, configs)
        if record.get("project")
    ]
    projects = []
    for project in sorted({record["project"] for record, _ in rows}):
        project_rows = [
            (record, config) for record, config in rows if record["project"] == project
        ]
        all_fields = sorted(set().union(*(config.keys() for _, config in project_rows)))
        fields = []
        for field in all_fields:
            identities = []
            cells = []
            for record, config in project_rows:
                missing = field not in config
                value = None if missing else json_safe(config[field])
                identity = "<missing>" if missing else json.dumps(value, sort_keys=True)
                if identity not in identities:
                    identities.append(identity)
                cells.append(
                    {
                        "wandb_id": record["wandb_id"],
                        "value": value,
                        "missing": missing,
                        "value_group": identities.index(identity),
                    }
                )
            if len(identities) <= 1:
                continue
            fields.append({"field": field, "values": cells})
            for (record, config), cell in zip(project_rows, cells):
                record.setdefault("differing_config", {})[field] = (
                    None if cell["missing"] else json_safe(config[field])
                )
        if fields:
            projects.append(
                {
                    "project": project,
                    "runs": [
                        {
                            "wandb_id": record["wandb_id"],
                            "experiment_name": (record["name"] or "").split("/", 1)[-1],
                        }
                        for record, _ in project_rows
                    ],
                    "fields": fields,
                }
            )
    return projects
