import type { Resource } from "./types";

export type ResourceSortKey = "name" | "status" | "progress" | "started" | "cost" | "cloud";

function resourceSortValue(resource: Resource, sortKey: ResourceSortKey): string | number {
  if (sortKey === "name") return `${resource.user}/${resource.project}/${resource.name}`;
  if (sortKey === "progress") return resource.progress.progress_fraction ?? -1;
  if (sortKey === "started") return resource.timing.started_at ?? "";
  if (sortKey === "cost") return resource.cost.estimated_spend_usd ?? -1;
  return `${resource.skypilot.cloud}/${resource.skypilot.region}`;
}

export function compareResources(
  left: Resource,
  right: Resource,
  sortKey: ResourceSortKey,
  descending: boolean,
): number {
  // Keep running jobs at the top, then order both groups by newest submission.
  if (sortKey === "status") {
    const runningComparison =
      Number(right.status.skypilot?.toUpperCase() === "RUNNING") -
      Number(left.status.skypilot?.toUpperCase() === "RUNNING");
    const submittedComparison = (right.submitted_at ?? "").localeCompare(left.submitted_at ?? "");
    const comparison = runningComparison || submittedComparison ||
      left.name.localeCompare(right.name, undefined, { numeric: true });
    return descending ? -comparison : comparison;
  }

  const leftValue = resourceSortValue(left, sortKey);
  const rightValue = resourceSortValue(right, sortKey);
  const comparison =
    typeof leftValue === "number" && typeof rightValue === "number"
      ? leftValue - rightValue
      : String(leftValue).localeCompare(String(rightValue), undefined, { numeric: true });
  return descending ? -comparison : comparison;
}
