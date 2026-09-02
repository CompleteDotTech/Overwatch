import type { Resource } from "./types";

export const ACTIVE_STATUSES = new Set([
  "PENDING",
  "SUBMITTED",
  "STARTING",
  "RUNNING",
  "WINDING_DOWN",
  "RECOVERING",
  "CANCELLING",
  "AUTOSTOPPING",
  "INIT",
  "UP",
]);

export interface SpendBreakdownRow {
  name: string;
  spend: number;
  hourly: number;
}

export interface WasteCandidate {
  resource: Resource;
  reasons: string[];
}

const GPU_RESOURCE_PATTERN = /(gpu|tpu|b200|h200|h100|a100|v100|l40|l4|t4|a10)/i;

export function findWasteCandidates(resources: Resource[]): WasteCandidate[] {
  return resources
    .filter((resource) => ACTIVE_STATUSES.has(resource.status.skypilot ?? ""))
    .flatMap((resource) => {
      const reasons: string[] = [];
      const hasObservedProgress =
        resource.progress.completed_batches != null || resource.progress.tokens_per_second != null;
      if (GPU_RESOURCE_PATTERN.test(resource.skypilot.resources ?? "") && !hasObservedProgress) {
        reasons.push("Idle GPU candidate");
      }
      if (resource.kind === "managed_job" && resource.wandb_id == null) {
        reasons.push("Orphan candidate");
      }
      if (resource.kind === "cluster" && (resource.timing.elapsed_seconds ?? 0) >= 2 * 24 * 60 * 60) {
        reasons.push("Zombie dev box candidate");
      }
      return reasons.length ? [{ resource, reasons }] : [];
    });
}

export function resourcesWithRecoveries(resources: Resource[]): Resource[] {
  return resources
    .filter(
      (resource) =>
        (resource.retries.total_recoveries ?? 0) > 0 ||
        (resource.retries.preemption_or_infrastructure ?? 0) > 0 ||
        (resource.retries.application_error ?? 0) > 0,
    )
    .sort(
      (left, right) =>
        (right.retries.total_recoveries ?? 0) - (left.retries.total_recoveries ?? 0),
    );
}

export function aggregateResourceSpend(
  resources: Resource[],
  labelFor: (resource: Resource) => string,
): SpendBreakdownRow[] {
  const grouped = new Map<string, SpendBreakdownRow>();
  for (const resource of resources) {
    const name = labelFor(resource);
    const row = grouped.get(name) ?? { name, spend: 0, hourly: 0 };
    row.spend += resource.cost.estimated_spend_usd ?? 0;
    if (ACTIVE_STATUSES.has(resource.status.skypilot ?? "")) {
      row.hourly += resource.cost.hourly_usd ?? 0;
    }
    grouped.set(name, row);
  }
  return [...grouped.values()]
    .filter((row) => row.spend > 0 || row.hourly > 0)
    .sort((left, right) => right.spend - left.spend);
}
