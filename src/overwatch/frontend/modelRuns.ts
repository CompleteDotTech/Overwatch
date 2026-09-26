/** Compatible cache presentation for v1 and v2 model telemetry; no training runtime. */
export interface AttemptReceipt {
  experiment_id: string; run_id: string; attempt_id: string; attempt_index: number;
  parent_attempt_id: string | null; previous_sha256: string | null; sha256: string;
}
export interface ModelExtensions {
  serialization: { tokenizer: string; serialization: string; literal_encoding: string };
  source: { source_tree_sha256: string | null; archive_sha256: string | null; git_dirty: boolean | null;
    git_status: "verified" | "unavailable" | "error"; package_version: string };
  attempt_lineage: AttemptReceipt[]; lineage_durable: boolean | null;
  execution: { prefix_passes: number; branch_passes: number; compute_tokens: number; padding_tokens: number;
    useful_forward_tokens: number; questions: number; max_batch_size: number; batch_size_histogram: Record<string, number> } | null;
  resources: { sampled_at: string; interval_seconds: number; rss_bytes: number | null; gpu_allocated_bytes: number | null;
    gpu_peak_allocated_bytes: number | null; units: "bytes"; unavailable: string[] } | null;
  calibration?: { status: string; method: string | null; split_sha256: string | null;
    per_type: Partial<Record<"choice" | "score" | "noul", { temperature: number; count: number; status: string;
      before_nll: number | null; after_nll: number | null }>> } | null;
}
export interface CollectionSource {
  source_id: string; status: string; payload_bytes: number; requests: number;
  refreshed_at: string | null; last_successful_refresh_at?: string | null;
}
export interface ModelRun {
  experiment_id: string; run_id: string; attempt_id: string; attempt_index: number;
  parent_attempt_id: string | null; sequence: number; phase: string;
  verification?: "verified" | "last_verified_with_newer_uncertainty" | "unverified";
  unverified_observations?: Array<{ attempt_id: string; attempt_index: number; parent_attempt_id: string | null;
    sequence: number; phase: string; heartbeat_at: string; reason: string; source_id: string }>;
  extensions?: ModelExtensions | null; collection_sources?: CollectionSource[];
  provider_status: { skypilot: string | null; wandb: string | null };
  association: { status: "local" | "matched" | "unmatched" | "ambiguous"; scheduler: { namespace: string; job_id: number } | null };
  heartbeat_at: string; heartbeat_age_seconds: number; freshness: "fresh" | "stale" | "terminal";
  progress: { optimizer_steps: number; microbatches: number; examples: number; forward_tokens: number; epoch: number | null;
    totals: Record<string, number | null>; elapsed_seconds: number | null; eta_seconds: number | null; tokens_per_second: number | null };
  metrics: Record<string, number>;
  history: Array<{ step: number; phase: string; metrics: Record<string, number> }>;
  resume: { capable: boolean; checkpoint_sha256: string | null };
  artifacts: Array<{ kind: string; uri: string; sha256: string; size_bytes: number; parent_sha256: string | null; resumable: boolean }>;
  provenance: { model: string; backbone: string; backbone_revision: string; tokenizer: string; precision: string; hardware: string;
    evidence_class: string; context_limits: { branch: number; aggregate: number }; repository?: string | null; commit?: string | null;
    config_sha256?: string | null; data_sha256?: string | null; split_hashes?: Record<string, string>; seed?: number };
  serving: { requests: number; errors: number; input_tokens: number; forward_tokens: number; output_tokens: number;
    questions: number; prefix_reuses: number; latency_ms: number[] };
  recoveries: { total: number | null; infrastructure: number | null; application: number | null };
  cost: { measured_usd: number | null; estimated_usd: number | null; basis: string | null };
  git_url: string | null; wandb_url: string | null; source_ids: string[]; transports: string[]; monitoring_export_failures: number;
}
export function safeWebURL(uri: string): string | null {
  try { const url = new URL(uri); return ["https:", "http:"].includes(url.protocol) && !url.username && !url.password ? uri : null; }
  catch { return null; }
}
export function curve(history: ModelRun["history"], metric: string): string {
  const values = history.filter(r => Number.isFinite(r.metrics[metric]));
  if (values.length < 2) return "";
  const minX = Math.min(...values.map(r => r.step)), maxX = Math.max(...values.map(r => r.step));
  const ys = values.map(r => r.metrics[metric]); const minY = Math.min(...ys), maxY = Math.max(...ys);
  return values.map(r => `${4 + 252 * (r.step - minX) / Math.max(1, maxX - minX)},${76 - 72 * (r.metrics[metric] - minY) / Math.max(1e-12, maxY - minY)}`).join(" ");
}
export function percentile(values: number[], quantile: number): number | null {
  if (!values.length) return null;
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(quantile * sorted.length) - 1))] ?? null;
}
export function verificationLabel(run: Pick<ModelRun, "verification">): string {
  if (run.verification === "unverified") return "Unverified observation";
  if (run.verification === "last_verified_with_newer_uncertainty") return "Last verified attempt; newer observation is uncertain";
  return "Verified attempt";
}
export function phaseLabel(run: Pick<ModelRun, "verification">): string {
  if (run.verification === "unverified") return "Unverified observed phase";
  if (run.verification === "last_verified_with_newer_uncertainty") return "Last verified trainer phase";
  return "Trainer phase";
}
export function formatBytes(bytes: number | null | undefined): string {
  return bytes == null || !Number.isFinite(bytes) ? "unknown" : `${(bytes / 1048576).toFixed(2)} MiB`;
}
export function executionSummary(ext: ModelExtensions | null | undefined): string {
  const e = ext?.execution;
  return e ? `${e.prefix_passes} prefix passes; ${e.branch_passes} question-batch passes; ${e.questions} questions; largest batch ${e.max_batch_size}`
    : "Parallel execution counters unknown in this historical snapshot";
}
