# Overwatch

Overwatch is a local browser service for cloud resources and training workloads.
It starts with a complete SkyPilot inventory so missing telemetry can never hide
a running machine, then adds training context when W&B, CloudWatch, durable
storage, or Zymtrace data is available.

## Capabilities

- Cloud resources
  - Shows SkyPilot managed jobs, standalone clusters, and development nodes,
    including resources without W&B telemetry.
  - Prioritizes running jobs and active clusters and hides terminal resources
    after two days.
- Training telemetry
  - Enriches matching jobs with W&B project, run status, progress, throughput,
    cost, retry, and ETA data.
  - Streams ANSI-colored CloudWatch logs in a full-screen drawer.
  - Links directly to SkyPilot, W&B, Zymtrace, and cloud storage.
- Cost and waste
  - Charts 30 days of AWS and GCP spend, stacked by cloud and by compute,
    storage, and other costs.
  - Flags idle GPUs, orphaned jobs, and zombie dev boxes.
  - Summarizes estimated spend by user, project, and cloud and reports retained
    recovery and preemption counts.
- Browser interface
  - Supports system, light, and dark color schemes.
  - Starts immediately, refreshes inventory periodically, and live-reloads
    Python and frontend changes while recovering from build or runtime errors.

The internal report is resource-oriented rather than run-oriented. SkyPilot is
currently the first cloud inventory provider, and Flow/W&B is the first optional
training enrichment provider. Additional cloud accounts, schedulers, and training
systems can be added without changing the rule that inventory determines which
resources appear.

## Install and run

Install the repository as an editable global uv tool:

```sh
uv run npm install
uv run npm run build
uv tool install --reinstall --editable /Users/erik/code/Overwatch \
  --overrides /Users/erik/code/Overwatch/overrides.txt
overwatch
```

Open <http://127.0.0.1:8765>. Chrome opens automatically unless `--no-open` is
passed. Auto-reload is enabled unless `--no-reload` is passed.

Useful options:

```sh
overwatch --help
overwatch --port 8877 --no-open
overwatch --no-log-enrichment
overwatch --gcp-billing-table my-project.billing.gcp_billing_export_v1_XXXXXX-XXXXXX-XXXXXX
```

## Billing history

AWS billing history uses the current AWS credentials and requires
`ce:GetCostAndUsage`. GCP billing history requires the Standard Cloud Billing
export in BigQuery. Set its fully qualified table in the environment or pass it
on the command line:

```sh
export GCP_BILLING_EXPORT_TABLE=my-project.billing.gcp_billing_export_v1_XXXXXX-XXXXXX-XXXXXX
overwatch
```

Both providers are optional: a missing permission or billing export produces a
warning without hiding the cloud resource inventory. GCP line series use the
billing export's project IDs. The stacked categories use AWS service names and
GCP service/SKU descriptions to classify compute and storage; all remaining
charges are grouped as everything else. Successful billing queries are cached
for one hour.
