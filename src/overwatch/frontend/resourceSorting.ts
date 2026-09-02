import type { Resource } from "./types";

export type ResourceSortKey = "name" | "status" | "progress" | "started" | "cost" | "cloud";

const STATUS_LIFECYCLE_ORDER = new Map<string, number>([
  ["INIT", 0],
  ["PENDING", 1],
  ["SUBMITTED", 1],
  ["STARTING", 1],
  ["RUNNING", 2],
  ["UP", 2],
  ["RECOVERING", 2],
  ["WINDING_DOWN", 3],
  ["CANCELLING", 3],
  ["AUTOSTOPPING", 3],
  ["SUCCEEDED", 4],
  ["FINISHED", 4],
  ["DONE", 4],
  ["CANCELLED", 5],
  ["CANCELED", 5],
]);

function resourceStatusOrder(resource: Resource): number {
  const status = resource.status.skypilot?.toUpperCase() ?? "";
  if (status.startsWith("FAILED")) return 6;
  return STATUS_LIFECYCLE_ORDER.get(status) ?? 7;
}

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
  // Keep the operational lifecycle primary regardless of the selected detail column.
  const statusComparison = resourceStatusOrder(left) - resourceStatusOrder(right);
  if (statusComparison !== 0) {
    return sortKey === "status" && descending ? -statusComparison : statusComparison;
  }

  // Default to the newest start within each lifecycle stage.
  if (sortKey === "status") {
    const startedComparison = (right.timing.started_at ?? "").localeCompare(left.timing.started_at ?? "");
    if (startedComparison !== 0) return startedComparison;
    return left.name.localeCompare(right.name, undefined, { numeric: true });
  }

  const leftValue = resourceSortValue(left, sortKey);
  const rightValue = resourceSortValue(right, sortKey);
  const comparison =
    typeof leftValue === "number" && typeof rightValue === "number"
      ? leftValue - rightValue
      : String(leftValue).localeCompare(String(rightValue), undefined, { numeric: true });
  return descending ? -comparison : comparison;
}
