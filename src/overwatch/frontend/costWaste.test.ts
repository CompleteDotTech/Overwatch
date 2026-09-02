import { describe, expect, it } from "vitest";

import { aggregateResourceSpend, findWasteCandidates, resourcesWithRecoveries } from "./costWaste";
import type { Resource } from "./types";

function resource(
  name: string,
  overrides: {
    active?: boolean;
    kind?: Resource["kind"];
    wandbId?: string | null;
    resources?: string;
    elapsedSeconds?: number;
    totalRecoveries?: number;
    applicationErrors?: number;
    spend?: number;
    hourly?: number;
  } = {},
): Resource {
  return {
    kind: overrides.kind ?? "managed_job",
    name,
    project: "project",
    user: "owner",
    wandb_id: overrides.wandbId === undefined ? "run" : overrides.wandbId,
    git: { commit: null, url: null },
    submitted_at: null,
    status: { wandb: null, skypilot: overrides.active === false ? "SUCCEEDED" : "RUNNING" },
    progress: {
      completed_batches: null,
      total_batches: null,
      progress_fraction: null,
      tokens_per_second: null,
      estimated_remaining_seconds: null,
    },
    retries: {
      preemption_or_infrastructure: 0,
      application_error: overrides.applicationErrors ?? 0,
      total_recoveries: overrides.totalRecoveries ?? 0,
    },
    timing: { started_at: null, elapsed_seconds: overrides.elapsedSeconds ?? 0, estimated_finish_at: null },
    cost: {
      hourly_usd: overrides.hourly ?? 0,
      estimated_spend_usd: overrides.spend ?? 0,
      estimated_total_usd: null,
    },
    skypilot: {
      resource_kind: overrides.kind === "cluster" ? "cluster" : "job",
      job_id: null,
      cluster_name: null,
      job_name: null,
      resources: overrides.resources ?? "CPU",
      cloud: "aws",
      region: null,
    },
    storage: { config_uri: null, run_uri: null },
    links: { wandb: null, skypilot: null, zymtrace: null },
  };
}

describe("cost and waste report derivation", () => {
  it("separates active waste, retained spend, and recovery ordering", () => {
    const idleOrphan = resource("idle", {
      resources: "B200:8",
      wandbId: null,
      spend: 10,
      hourly: 4,
      totalRecoveries: 2,
    });
    const stoppedGpu = resource("stopped", {
      active: false,
      resources: "H100:8",
      spend: 20,
      hourly: 8,
      applicationErrors: 1,
    });
    const oldCluster = resource("devbox", {
      kind: "cluster",
      elapsedSeconds: 2 * 24 * 60 * 60,
      spend: 5,
      hourly: 2,
    });
    const resources = [stoppedGpu, oldCluster, idleOrphan];

    expect(findWasteCandidates(resources).map(({ resource: item, reasons }) => [item.name, reasons])).toEqual([
      ["devbox", ["Zombie dev box candidate"]],
      ["idle", ["Idle GPU candidate", "Orphan candidate"]],
    ]);
    expect(aggregateResourceSpend(resources, (item) => item.user ?? "Unknown")).toEqual([
      { name: "owner", spend: 35, hourly: 6 },
    ]);
    expect(resourcesWithRecoveries(resources).map((item) => item.name)).toEqual(["idle", "stopped"]);
  });
});
