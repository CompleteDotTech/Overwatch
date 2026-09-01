"""Daily AWS and GCP billing-history collectors."""

from __future__ import annotations

import re
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

import boto3
from google.cloud import bigquery

from overwatch.constants import BILLING_HISTORY_DAYS

GCP_BILLING_TABLE_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_-]*\.[A-Za-z0-9][A-Za-z0-9_-]*\."
    r"[A-Za-z0-9][A-Za-z0-9_-]*"
)
BILLING_CACHE_SECONDS = 60 * 60
_billing_cache: dict[tuple[date, str | None], tuple[float, dict[str, Any]]] = {}


def billing_category_from_aws_service(service: str) -> str:
    normalized_service = service.casefold()
    if any(
        keyword in normalized_service
        for keyword in ("storage", "s3", "backup", "elastic file system", "fsx")
    ):
        return "storage"
    if any(
        keyword in normalized_service
        for keyword in (
            "elastic compute cloud",
            "ec2",
            "lambda",
            "sagemaker",
            "elastic container",
            "kubernetes",
            "batch",
        )
    ):
        return "compute"
    return "everything_else"


def collect_aws_daily_spend(
    start_date: date, end_date: date
) -> tuple[dict[str, float], dict[str, dict[str, float]]]:
    """Collect AWS spend totals and service-category breakdowns by day."""
    client = boto3.client("ce", region_name="us-east-1")
    request_arguments: dict[str, Any] = {
        "TimePeriod": {"Start": start_date.isoformat(), "End": end_date.isoformat()},
        "Granularity": "DAILY",
        "Metrics": ["NetUnblendedCost"],
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
    }
    daily: dict[str, float] = {}
    categories: dict[str, dict[str, float]] = {}

    # Cost Explorer can paginate grouped results, so merge every service page.
    while True:
        response = client.get_cost_and_usage(**request_arguments)
        for period in response["ResultsByTime"]:
            usage_date = period["TimePeriod"]["Start"]
            daily.setdefault(usage_date, 0.0)
            categories.setdefault(
                usage_date, {"compute": 0.0, "storage": 0.0, "everything_else": 0.0}
            )
            for group in period["Groups"]:
                amount = float(group["Metrics"]["NetUnblendedCost"]["Amount"])
                category = billing_category_from_aws_service(group["Keys"][0])
                daily[usage_date] += amount
                categories[usage_date][category] += amount
        next_page_token = response.get("NextPageToken")
        if not next_page_token:
            break
        request_arguments["NextPageToken"] = next_page_token
    return daily, categories


def collect_gcp_daily_spend(
    billing_table: str, start_date: date, end_date: date
) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, float]]]:
    """Collect GCP project spend and SKU-category breakdowns by day."""
    if GCP_BILLING_TABLE_RE.fullmatch(billing_table) is None:
        raise ValueError(
            "GCP_BILLING_EXPORT_TABLE must be a project.dataset.table identifier"
        )

    # Convert local-currency costs and credits to USD before aggregating by UTC usage day.
    query = f"""
        SELECT
          DATE(usage_start_time) AS usage_date,
          COALESCE(project.id, 'Unattributed GCP') AS project_id,
          CASE
            WHEN REGEXP_CONTAINS(
              LOWER(CONCAT(service.description, ' ', sku.description)),
              r'(storage|disk|snapshot|filestore|backup|archive)'
            ) THEN 'storage'
            WHEN REGEXP_CONTAINS(
              LOWER(CONCAT(service.description, ' ', sku.description)),
              r'(compute engine|vertex ai|kubernetes|cloud run|cloud function|cpu|gpu|tpu|instance|core running|ram running)'
            ) THEN 'compute'
            ELSE 'everything_else'
          END AS category,
          CAST(SUM(
            (CAST(cost AS NUMERIC) + IFNULL((
              SELECT SUM(CAST(credit.amount AS NUMERIC))
              FROM UNNEST(credits) AS credit
            ), 0)) / IFNULL(NULLIF(CAST(currency_conversion_rate AS NUMERIC), 0), 1)
          ) AS FLOAT64) AS amount_usd
        FROM `{billing_table}`
        WHERE usage_start_time >= TIMESTAMP(@start_date)
          AND usage_start_time < TIMESTAMP(@end_date)
          AND export_time >= TIMESTAMP(DATE_SUB(@start_date, INTERVAL 7 DAY))
          AND export_time < TIMESTAMP(@end_date)
        GROUP BY usage_date, project_id, category
        ORDER BY usage_date, project_id, category
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )
    query_project = billing_table.split(".", 1)[0]
    rows = (
        bigquery.Client(project=query_project)
        .query(query, job_config=job_config)
        .result()
    )
    projects: dict[str, dict[str, float]] = {}
    categories: dict[str, dict[str, float]] = {}
    for row in rows:
        usage_date = row.usage_date.isoformat()
        projects.setdefault(row.project_id, {}).setdefault(usage_date, 0.0)
        projects[row.project_id][usage_date] += float(row.amount_usd)
        categories.setdefault(
            usage_date, {"compute": 0.0, "storage": 0.0, "everything_else": 0.0}
        )
        categories[usage_date][row.category] += float(row.amount_usd)
    return projects, categories


def collect_daily_cloud_spend(
    gcp_billing_table: str | None, *, today: date | None = None
) -> dict[str, Any]:
    """Collect and cache a normalized 30-day AWS/GCP spend series."""
    today = today or datetime.now(UTC).date()
    cache_key = (today, gcp_billing_table)
    cached = _billing_cache.get(cache_key)
    if cached is not None and time.monotonic() - cached[0] < BILLING_CACHE_SECONDS:
        return cached[1]

    start_date = today - timedelta(days=BILLING_HISTORY_DAYS - 1)
    end_date = today + timedelta(days=1)
    warnings = []
    aws_daily: dict[str, float] | None = None
    gcp_projects: dict[str, dict[str, float]] | None = None
    category_sources: dict[str, dict[str, dict[str, float]]] = {}

    # Billing is optional telemetry, so provider permission or configuration errors stay local.
    try:
        aws_daily, aws_categories = collect_aws_daily_spend(start_date, end_date)
        category_sources["aws"] = aws_categories
    except Exception as error:  # noqa: BLE001
        warnings.append(f"Could not collect AWS billing history: {error}")

    if gcp_billing_table:
        try:
            gcp_projects, gcp_categories = collect_gcp_daily_spend(
                gcp_billing_table, start_date, end_date
            )
            category_sources["gcp"] = gcp_categories
        except Exception as error:  # noqa: BLE001
            warnings.append(f"Could not collect GCP billing history: {error}")

    aws_total = sum(aws_daily.values()) if aws_daily is not None else None
    gcp_total = (
        sum(sum(costs.values()) for costs in gcp_projects.values())
        if gcp_projects is not None
        else None
    )
    combined_total = (
        sum(total for total in (aws_total, gcp_total) if total is not None)
        if aws_total is not None or gcp_total is not None
        else None
    )
    series: list[dict[str, Any]] = [
        {
            "key": "aws",
            "label": "AWS",
            "provider": "aws",
            "total": round(aws_total, 2) if aws_total is not None else None,
        }
    ]
    gcp_series: list[dict[str, Any]] = []
    for index, (project, costs) in enumerate(sorted((gcp_projects or {}).items())):
        gcp_series.append(
            {
                "key": f"gcp_{index}",
                "label": project,
                "provider": "gcp",
                "total": round(sum(costs.values()), 2),
                "costs": costs,
            }
        )
        series.append(
            {key: value for key, value in gcp_series[-1].items() if key != "costs"}
        )
    series.append(
        {
            "key": "combined",
            "label": "Combined",
            "provider": "combined",
            "total": round(combined_total, 2) if combined_total is not None else None,
        }
    )

    daily = []
    category_daily = []
    for offset in range(BILLING_HISTORY_DAYS):
        usage_date = (start_date + timedelta(days=offset)).isoformat()
        aws_spend = aws_daily.get(usage_date, 0.0) if aws_daily is not None else None
        row: dict[str, str | float | None] = {
            "date": usage_date,
            "aws": round(aws_spend, 2) if aws_spend is not None else None,
        }
        combined_values = [aws_spend] if aws_spend is not None else []
        for gcp_project_series in gcp_series:
            project_spend = gcp_project_series["costs"].get(usage_date, 0.0)
            row[gcp_project_series["key"]] = round(project_spend, 2)
            combined_values.append(project_spend)
        row["combined"] = round(sum(combined_values), 2) if combined_values else None
        daily.append(row)

        category_row: dict[str, str | float | None] = {"date": usage_date}
        for provider in ("aws", "gcp"):
            for category in ("compute", "storage", "everything_else"):
                category_row[f"{provider}_{category}"] = (
                    round(
                        category_sources[provider]
                        .get(usage_date, {})
                        .get(category, 0.0),
                        2,
                    )
                    if provider in category_sources
                    else None
                )
        category_daily.append(category_row)

    totals: dict[str, float] = {}
    if aws_total is not None:
        totals["aws"] = round(aws_total, 2)
    if gcp_total is not None:
        totals["gcp"] = round(gcp_total, 2)
    if combined_total is not None:
        totals["combined"] = round(combined_total, 2)
    result = {
        "start_date": start_date.isoformat(),
        "end_date": today.isoformat(),
        "currency": "USD",
        "daily": daily,
        "series": series,
        "category_daily": category_daily,
        "totals": totals,
        "gcp_configured": bool(gcp_billing_table),
        "warnings": warnings,
    }
    _billing_cache[cache_key] = (time.monotonic(), result)
    return result
