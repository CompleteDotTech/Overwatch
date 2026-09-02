"""Raw AWS and GCP billing-history providers."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from google.cloud import bigquery

from overwatch.constants import BILLING_HISTORY_DAYS

GCP_BILLING_TABLE_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_-]*\.[A-Za-z0-9][A-Za-z0-9_-]*\."
    r"[A-Za-z0-9][A-Za-z0-9_-]*"
)


def collect_aws_billing_responses() -> dict[str, Any]:
    """Collect unmodified paginated Cost Explorer responses."""
    today = datetime.now(UTC).date()
    request_arguments: dict[str, Any] = {
        "TimePeriod": {
            "Start": (today - timedelta(days=BILLING_HISTORY_DAYS - 1)).isoformat(),
            "End": (today + timedelta(days=1)).isoformat(),
        },
        "Granularity": "DAILY",
        "Metrics": ["NetUnblendedCost"],
        "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}],
    }
    client = boto3.client("ce", region_name="us-east-1")
    responses = []
    next_page_token = None
    while True:
        page_arguments = dict(request_arguments)
        if next_page_token:
            page_arguments["NextPageToken"] = next_page_token
        response = client.get_cost_and_usage(**page_arguments)
        responses.append(response)
        next_page_token = response.get("NextPageToken")
        if not next_page_token:
            break
    return {"request": request_arguments, "responses": responses}


def collect_gcp_billing_rows(billing_table: str) -> dict[str, Any]:
    """Collect aggregated raw BigQuery rows by project, service, and SKU."""
    if GCP_BILLING_TABLE_RE.fullmatch(billing_table) is None:
        raise ValueError(
            "GCP_BILLING_EXPORT_TABLE must be a project.dataset.table identifier"
        )

    today = datetime.now(UTC).date()
    start_date = today - timedelta(days=BILLING_HISTORY_DAYS - 1)
    end_date = today + timedelta(days=1)
    query = f"""
        SELECT
          DATE(usage_start_time) AS usage_date,
          project.id AS project_id,
          service.description AS service_description,
          sku.description AS sku_description,
          currency,
          CAST(SUM(cost) AS FLOAT64) AS cost,
          CAST(SUM(IFNULL((
            SELECT SUM(credit.amount) FROM UNNEST(credits) AS credit
          ), 0)) AS FLOAT64) AS credits,
          CAST(ANY_VALUE(currency_conversion_rate) AS FLOAT64) AS currency_conversion_rate
        FROM `{billing_table}`
        WHERE usage_start_time >= TIMESTAMP(@start_date)
          AND usage_start_time < TIMESTAMP(@end_date)
        GROUP BY usage_date, project_id, service_description, sku_description, currency
        ORDER BY usage_date, project_id, service_description, sku_description
    """
    parameters = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )
    rows = (
        bigquery.Client(project=billing_table.split(".", 1)[0])
        .query(query, job_config=job_config)
        .result()
    )
    return {
        "table": billing_table,
        "query": query,
        "parameters": parameters,
        "rows": [dict(row.items()) for row in rows],
    }
