import { describe, expect, it } from "vitest";

import { compareResources } from "./resourceSorting";
import type { Resource } from "./types";

function resource(name: string, status: string, submittedAt: string): Resource {
  return {
    name,
    submitted_at: submittedAt,
    status: { skypilot: status, wandb: null },
    timing: { started_at: null },
  } as Resource;
}

describe("resource status sorting", () => {
  it("puts running resources first and orders both groups by newest submission", () => {
    const resources = [
      resource("cancelled", "CANCELLED", "2026-09-01T08:00:00Z"),
      resource("running-old", "RUNNING", "2026-09-01T09:00:00Z"),
      resource("failed", "FAILED", "2026-09-01T12:00:00Z"),
      resource("done", "SUCCEEDED", "2026-09-01T11:00:00Z"),
      resource("initializing", "INIT", "2026-09-01T13:00:00Z"),
      resource("running-new", "RUNNING", "2026-09-01T10:00:00Z"),
    ];

    expect(resources.sort((left, right) => compareResources(left, right, "status", false)).map((item) => item.name)).toEqual([
      "running-new",
      "running-old",
      "initializing",
      "failed",
      "done",
      "cancelled",
    ]);
  });
});
