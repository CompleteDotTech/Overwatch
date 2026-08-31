"""HTML report preparation and rendering."""

import html
import json
from datetime import timedelta
from typing import Any
from urllib.parse import quote

from jinja2 import Environment, PackageLoader, select_autoescape

from overwatch.constants import ACTIVE_RESOURCE_STATUSES, HAIKU_SUFFIX_RE, PACIFIC_TIME
from overwatch.utils import parse_utc

TEMPLATE_ENVIRONMENT = Environment(
    loader=PackageLoader("overwatch", "templates"),
    autoescape=select_autoescape(("html", "jinja")),
)
REPORT_TEMPLATE = TEMPLATE_ENVIRONMENT.get_template("report.html.jinja")


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
    """Build categorical config heatmaps grouped by W&B project."""
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


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    duration = timedelta(seconds=round(seconds))
    days = duration.days
    hours, remainder = divmod(duration.seconds, 3600)
    minutes = remainder // 60
    return f"{days}d {hours}h {minutes}m" if days else f"{hours}h {minutes}m"


def format_money(value: float | None) -> str:
    return "—" if value is None else f"${value:,.2f}"


def format_timestamp(value: str | None, *, include_timezone: bool = True) -> str:
    if value is None:
        return "—"
    timestamp = parse_utc(value)
    if timestamp is None:
        return "—"
    timestamp_format = "%Y-%m-%d %H:%M %Z" if include_timezone else "%Y-%m-%d %H:%M"
    return timestamp.astimezone(PACIFIC_TIME).strftime(timestamp_format)


LINK_ICON_PATHS = {
    "wandb": '<path d="M3 6.5 6.3 17 10 7l3.7 10L17 6.5"/>',
    "skypilot": '<path d="m3 11 17-7-7 17-2.5-7.5L3 11Zm7.5 2.5L20 4"/>',
    "zymtrace": '<path d="M2 12h3l2-6 3 12 3-12 2 6h5"/>',
}


def html_icon_link(url: str | None, label: str, icon: str) -> str:
    if not url:
        return ""
    return (
        f'<a class="link-icon link-icon-{icon}" href="{html.escape(url, quote=True)}" '
        f'target="_blank" rel="noreferrer" title="{label}" aria-label="{label}">'
        f'<svg viewBox="0 0 24 24" aria-hidden="true">{LINK_ICON_PATHS[icon]}</svg></a>'
    )


def html_config_value(value: Any, missing: bool) -> str:
    if missing:
        return '<span class="muted">&lt;missing&gt;</span>'
    serialized = json.dumps(value, sort_keys=True)
    shortened = serialized if len(serialized) <= 240 else f"{serialized[:239]}…"
    return f'<code title="{html.escape(serialized, quote=True)}">{html.escape(shortened)}</code>'


def render_html(report: dict[str, Any]) -> str:
    def sort_value(value: Any) -> str:
        return html.escape("" if value is None else str(value), quote=True)

    def experiment_name_html(experiment_name: str) -> str:
        haiku_match = HAIKU_SUFFIX_RE.search(experiment_name)
        if haiku_match is None:
            return html.escape(experiment_name)
        return (
            f"{html.escape(experiment_name[: haiku_match.start()])}"
            f'<span class="run-haiku">{html.escape(haiku_match.group())}</span>'
        )

    # Build deterministic money totals and state-aware memes from the current report.
    resources = report["resources"]
    active_resources = [
        resource
        for resource in resources
        if resource["status"]["skypilot"] in ACTIVE_RESOURCE_STATUSES
    ]
    total_hourly_cost = sum(
        resource["cost"]["hourly_usd"] or 0 for resource in active_resources
    )
    total_spent = sum(
        resource["cost"]["estimated_spend_usd"] or 0 for resource in resources
    )
    active_projected_cost = sum(
        resource["cost"]["estimated_total_usd"] or 0 for resource in active_resources
    )
    failed_runs = sum(
        run["status"]["wandb"] in {"failed", "crashed"}
        or run["status"]["skypilot"] in {"FAILED", "FAILED_SETUP", "CANCELLED"}
        for run in resources
    )
    recovery_count = sum(run["retries"]["total_recoveries"] or 0 for run in resources)
    near_finish_count = sum(
        (run["progress"].get("progress_fraction") or 0) >= 0.9
        for run in active_resources
    )
    unhinged_memes = [
        f"{len(active_resources)} RESOURCES COOKING 👨‍🍳",
        f"{format_money(total_hourly_cost)}/HR GPU RENT 💸",
        f"{format_money(total_spent)} FINANCIAL DAMAGE 📉",
    ]
    if failed_runs:
        unhinged_memes.append(f"{failed_runs} RUNS: IT'S SO OVER 💀")
    if recovery_count:
        unhinged_memes.append(f"{recovery_count} RECOVERIES: WE'RE SO BACK 🔥")
    if near_finish_count:
        unhinged_memes.append(f"{near_finish_count} FINAL BOSS FIGHTS 🐉")
    meme_layer_html = "".join(
        f'<span class="meme-sticker meme-slot-{index + 1}">{html.escape(message)}</span>'
        for index, message in enumerate(unhinged_memes)
    )

    rows = []
    for run in resources:
        status = run["status"]
        progress = run["progress"]
        retries = run["retries"]
        timing = run["timing"]
        cost = run["cost"]
        storage = run["storage"]
        links = run["links"]
        wandb_status = status["wandb"] or "unknown"
        skypilot_status = status["skypilot"] or "unmatched"
        run_name = run["name"] or ""
        run_name_html = experiment_name_html(run_name)
        project_name = run["project"] or ""
        user_name = run["user"] or "—"
        if links["wandb"]:
            project_url = (
                f"https://wandb.ai/{quote(report['wandb_entity'], safe='')}/{quote(project_name, safe='')}"
                if project_name
                else None
            )
            project_name_html = (
                f'<a class="run-project-link" href="{html.escape(project_url, quote=True)}" target="_blank" '
                f'rel="noreferrer" title="Open W&B project">{html.escape(project_name)}</a>'
                if project_url
                else "—"
            )
            run_name_html = (
                f'<a class="run-link" href="{html.escape(links["wandb"], quote=True)}" target="_blank" '
                f'rel="noreferrer" title="Open W&B run">{run_name_html}</a>'
            )
        else:
            project_name_html = html.escape(project_name or "—")
        user_project_html = (
            f'<span class="run-user">{html.escape(user_name)}</span>'
            f'<span class="project-user-separator"> / </span>{project_name_html}'
        )
        completed_batches = progress.get("completed_batches")
        total_batches = progress.get("total_batches")
        progress_fraction = progress.get("progress_fraction")
        log_elapsed_seconds = progress.get("log_elapsed_seconds")
        estimated_remaining_seconds = progress.get("estimated_remaining_seconds")
        throughput_text = (
            f"{progress['tokens_per_second']:,} tok/s"
            if progress.get("tokens_per_second") is not None
            else "— tok/s"
        )
        ema_samples = progress.get("tokens_per_second_ema_samples")
        throughput_title = (
            f' title="EMA over {ema_samples:,} recent progress samples"'
            if ema_samples is not None
            else ""
        )
        throughput_html = f'<div class="progress-throughput"{throughput_title}>{throughput_text}</div>'
        progress_timing_html = ""
        if log_elapsed_seconds is not None or estimated_remaining_seconds is not None:
            log_elapsed_text = (
                str(timedelta(seconds=round(log_elapsed_seconds)))
                if log_elapsed_seconds is not None
                else "—"
            )
            estimated_remaining_text = (
                str(timedelta(seconds=round(estimated_remaining_seconds)))
                if estimated_remaining_seconds is not None
                else "—"
            )
            progress_timing_html = (
                f'<div class="progress-timing" title="elapsed &lt; estimated remaining">'
                f"{html.escape(log_elapsed_text)} &lt; {html.escape(estimated_remaining_text)}</div>"
            )
        if (
            completed_batches is not None
            and total_batches
            and progress_fraction is not None
        ):
            percent_done = max(0, min(100, progress_fraction * 100))
            progress_html = (
                '<div class="progress-cell">'
                '<div class="progress-main">'
                f'<span class="batch-count">{completed_batches:,}/{total_batches:,}b</span>'
                f'<div class="progress-track" title="{percent_done:.1f}%">'
                f'<div class="progress-fill" style="width:{percent_done:.1f}%"></div>'
                f'<span class="progress-label">{percent_done:.0f}%</span></div></div>'
                f"{throughput_html}{progress_timing_html}</div>"
            )
        else:
            batch_text = (
                f'<span class="batch-count">{completed_batches:,}b</span>'
                if completed_batches
                else "—"
            )
            progress_html = f'<div class="progress-cell">{batch_text}{throughput_html}{progress_timing_html}</div>'
        cost_text = f"{format_money(cost['estimated_spend_usd'])} / {format_money(cost['estimated_total_usd'])}"
        cost_sort_value = cost["estimated_spend_usd"]
        hourly_cost_text = (
            f"{format_money(cost['hourly_usd'])}"
            if cost["hourly_usd"] is not None
            else "—"
        )
        if wandb_status in {"failed", "crashed"} or skypilot_status.startswith(
            "FAILED"
        ):
            run_meme = "IT'S SO OVER 💀"
        elif skypilot_status == "RECOVERING" or (retries["total_recoveries"] or 0) > 0:
            run_meme = "WE'RE SO BACK 🔥"
        elif (progress_fraction or 0) >= 0.9:
            run_meme = "FINAL BOSS 🐉"
        elif (cost["hourly_usd"] or 0) >= 20:
            run_meme = "BURN RATE GO BRRR 💸"
        else:
            run_meme = "LET IT COOK 👨‍🍳"
        time_left_seconds = progress.get("estimated_remaining_seconds")
        if (
            time_left_seconds is None
            and timing["estimated_total_seconds"] is not None
            and timing["elapsed_seconds"] is not None
        ):
            time_left_seconds = max(
                0, timing["estimated_total_seconds"] - timing["elapsed_seconds"]
            )
        time_left_text = (
            f"{format_duration(time_left_seconds)} left"
            if time_left_seconds is not None
            else "— left"
        )
        skypilot = run["skypilot"]
        cloud = skypilot.get("cloud")
        region = skypilot.get("region")
        cloud_region_text = " ".join(value for value in (cloud, region) if value) or "—"
        cloud_region_html = f'<div>{html.escape(cloud or "—")}</div><div class="muted cloud-region">{html.escape(region or "—")}</div>'
        storage_icon = (
            '<svg class="gcp-logo" viewBox="0 0 24 24" aria-hidden="true">'
            '<path class="gcp-red" d="M6.7 10.2A6.2 6.2 0 0 1 17.8 11"/>'
            '<path class="gcp-blue" d="M17.8 11A4 4 0 0 1 17 18h-5"/>'
            '<path class="gcp-green" d="M12 18H7.2a4.2 4.2 0 0 1-3.7-2.2"/>'
            '<path class="gcp-yellow" d="M3.5 15.8a4.8 4.8 0 0 1 3.2-5.6"/>'
            "</svg>"
            if storage["gcp_bucket"]
            else (
                '<svg viewBox="0 0 24 24" aria-hidden="true">'
                '<rect x="8" y="8" width="11" height="11" rx="2"/>'
                '<path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/>'
                "</svg>"
            )
        )
        storage_label = (
            "Copy GCS run URI" if storage["gcp_bucket"] else "Copy storage URI"
        )
        storage_copy_button = (
            f'<button class="copy-button" type="button" data-copy="{html.escape(storage["run_uri"], quote=True)}" '
            f'title="{storage_label}" aria-label="{storage_label}">{storage_icon}</button>'
            if storage["run_uri"]
            else ""
        )
        log_button = (
            f'<button class="log-button" type="button" data-job-id="{skypilot["job_id"]}" '
            f'data-job-name="{html.escape(skypilot["job_name"] or run_name, quote=True)}" '
            f'title="Stream CloudWatch logs" aria-label="Stream CloudWatch logs">'
            '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 5h16v14H4zM7 9l3 3-3 3m5 0h5"/>'
            "</svg><span>Live logs</span></button>"
            if skypilot.get("cluster_name") and skypilot.get("job_id") is not None
            else ""
        )
        retry_text = (
            f"{retries['preemption_or_infrastructure'] if retries['preemption_or_infrastructure'] is not None else '?'}"
            f" | {retries['application_error'] if retries['application_error'] is not None else '?'}"
        )
        rows.append(
            "<tr>"
            f'<td class="project-run" data-sort-value="{sort_value(f"{user_name}/{run['project'] or ''}/{run_name}".casefold())}">'
            f'<div class="run-project">{user_project_html}</div>'
            f'<div class="run-name">{run_name_html}</div></td>'
            f'<td class="status-pair" data-sort-value="{sort_value(f"{wandb_status}/{skypilot_status}")}">'
            '<div class="status-stack">'
            f'<span class="status status-{html.escape(wandb_status.casefold())}" title="W&amp;B status">'
            f'<svg class="status-source-icon" viewBox="0 0 24 24" aria-hidden="true">{LINK_ICON_PATHS["wandb"]}</svg>'
            f"{html.escape(wandb_status)}</span>"
            f'<span class="status status-{html.escape(skypilot_status.casefold())}" title="SkyPilot status">'
            f'<svg class="status-source-icon" viewBox="0 0 24 24" aria-hidden="true">{LINK_ICON_PATHS["skypilot"]}</svg>'
            f"{html.escape(skypilot_status)}</span></div></td>"
            f'<td data-sort-value="{sort_value(progress_fraction)}">{progress_html}</td>'
            f'<td data-sort-value="{sort_value(retries["total_recoveries"])}">{retry_text}'
            f"</td>"
            f'<td class="time-pair" data-sort-value="{sort_value(timing["started_at"])}">'
            f"<div>{format_timestamp(timing['started_at'], include_timezone=False)}</div>"
            f'<div class="muted">'
            f"{format_timestamp(timing['estimated_finish_at'], include_timezone=False)}</div></td>"
            f'<td class="time-pair" data-sort-value="{sort_value(timing["elapsed_seconds"])}">'
            f"<div>{format_duration(timing['elapsed_seconds'])}</div>"
            f'<div class="muted">{time_left_text}</div></td>'
            f'<td class="time-pair" data-sort-value="{sort_value(cost_sort_value)}">'
            f"<div>{cost_text}</div>"
            f'<div class="muted">{hourly_cost_text}/hr</div>'
            f'<div class="run-meme">{html.escape(run_meme)}</div></td>'
            f'<td data-sort-value="{sort_value(cloud_region_text.casefold())}">'
            f"{cloud_region_html}</td>"
            f'<td class="links" data-sort-value="{sort_value(links["wandb"])}">'
            f"{html_icon_link(links['wandb'], 'W&B', 'wandb')}"
            f"{html_icon_link(links['skypilot'], 'SkyPilot', 'skypilot')}"
            f"{html_icon_link(links['zymtrace'], 'Zymtrace', 'zymtrace')}"
            f"{storage_copy_button}{log_button}</td>"
            "</tr>"
        )

    config_differences = report.get("config_differences", [])
    config_field_count = sum(len(project["fields"]) for project in config_differences)
    config_sections = []
    for project in config_differences:
        headers = [
            f'<th data-sort-value="{sort_value(run["experiment_name"].casefold())}">'
            f'<div class="config-run-name">{experiment_name_html(run["experiment_name"])}</div></th>'
            for run in project["runs"]
        ]
        config_rows = []
        for difference in project["fields"]:
            cells = [
                f'<td class="heatmap-cell heatmap-{cell["value_group"] % 8}" '
                f'data-sort-value="{sort_value(json.dumps(cell["value"], sort_keys=True))}">'
                f"{html_config_value(cell['value'], cell['missing'])}</td>"
                for cell in difference["values"]
            ]
            config_rows.append(
                f'<tr><td data-sort-value="{sort_value(difference["field"].casefold())}">'
                f"<code>{html.escape(difference['field'])}</code></td>{''.join(cells)}</tr>"
            )
        config_sections.append(
            f'<section class="config-project"><h3>{html.escape(project["project"])}</h3>'
            f'<div class="table-wrap config-wrap"><table class="config-table">'
            f"<thead><tr><th>Field</th>{''.join(headers)}</tr></thead>"
            f"<tbody>{''.join(config_rows)}</tbody></table></div></section>"
        )

    generated_at = format_timestamp(report["generated_at"])
    return REPORT_TEMPLATE.render(
        active_projected_cost=format_money(active_projected_cost),
        config_field_count=config_field_count,
        config_sections_html="".join(config_sections),
        generated_at=generated_at,
        meme_layer_html=meme_layer_html,
        requested_limit=report["requested_limit"],
        row_count=len(resources),
        rows_html="".join(rows),
        total_hourly_cost=format_money(total_hourly_cost),
        total_spent=format_money(total_spent),
        warnings=report.get("warnings", []),
    )
