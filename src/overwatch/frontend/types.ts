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
  git: { commit: string | null; url: string | null };
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
  storage: { config_uri: string | null; run_uri: string | null };
  cache?: { missing_files: string[]; errors: Record<string, string> };
  links: {
    wandb: string | null;
    skypilot: string | null;
    zymtrace: string | null;
  };
}

export interface LogAttempt {
  attempt: number;
  pid: number;
  started_at: string;
  current: boolean;
}

export interface CloudWatchLogEvent {
  id: string;
  timestamp: number;
  html: string;
}

export interface CloudWatchLogPage {
  events: CloudWatchLogEvent[];
  has_older: boolean;
  has_newer: boolean;
  oldest_cursor: string | null;
  newest_cursor: string | null;
}

export interface ConfigDifference {
  project: string;
  runs: Array<{ wandb_id: string; wandb_url: string | null; experiment_name: string }>;
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

export interface QueryDiagnostic {
  key: string;
  label: string;
  status: "ok" | "warning" | "error" | "pending" | "skipped";
  summary: string;
  updated_at: string | null;
  duration_seconds: number | null;
  error: string | null;
}

export interface QueryStatusReport {
  refresh: {
    in_progress: boolean;
    last_attempt_at: string | null;
    last_success_at: string | null;
    last_duration_seconds: number | null;
    last_error: string | null;
  };
  queries: QueryDiagnostic[];
}

export interface Report {
  generated_at: string;
  requested_limit: number;
  raw_cache_root?: string;
  billing: Billing;
  resources: Resource[];
  config_differences: ConfigDifference[];
  warnings: string[];
}
