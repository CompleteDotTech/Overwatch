export interface BillingDay {
  date: string;
  [series: string]: string | number | null;
}

export interface BillingSeries {
  key: string;
  label: string;
  provider: "aws" | "gcp" | "combined";
  total: number | null;
}

export interface Billing {
  start_date: string;
  end_date: string;
  currency: string;
  daily: BillingDay[];
  series: BillingSeries[];
  category_daily: Array<{
    date: string;
    aws_compute: number | null;
    aws_storage: number | null;
    aws_everything_else: number | null;
    gcp_compute: number | null;
    gcp_storage: number | null;
    gcp_everything_else: number | null;
  }>;
  totals: Partial<Record<"aws" | "gcp" | "combined", number>>;
  gcp_configured: boolean;
  warnings: string[];
}

export interface Resource {
  kind: "managed_job" | "cluster";
  name: string;
  project: string | null;
  user: string | null;
  wandb_id: string | null;
  submitted_at: string | null;
  status: { wandb: string | null; skypilot: string | null };
  progress: {
    completed_batches: number | null;
    total_batches: number | null;
    progress_fraction: number | null;
    tokens_per_second: number | null;
    estimated_remaining_seconds: number | null;
  };
  retries: {
    preemption_or_infrastructure: number | null;
    application_error: number | null;
    total_recoveries: number | null;
  };
  timing: {
    started_at: string | null;
    elapsed_seconds: number | null;
    estimated_finish_at: string | null;
  };
  cost: {
    hourly_usd: number | null;
    estimated_spend_usd: number | null;
    estimated_total_usd: number | null;
  };
  skypilot: {
    resource_kind: "job" | "cluster" | null;
    job_id: number | null;
    cluster_name: string | null;
    job_name: string | null;
    resources: string | null;
    cloud: string | null;
    region: string | null;
  };
  storage: { run_uri: string | null };
  links: {
    wandb: string | null;
    skypilot: string | null;
    zymtrace: string | null;
  };
}

export interface ConfigDifference {
  project: string;
  runs: Array<{ wandb_id: string; experiment_name: string }>;
  fields: Array<{
    field: string;
    values: Array<{
      wandb_id: string;
      value: unknown;
      missing: boolean;
      value_group: number;
    }>;
  }>;
}

export interface Report {
  generated_at: string;
  requested_limit: number;
  billing: Billing;
  resources: Resource[];
  config_differences: ConfigDifference[];
  warnings: string[];
}
