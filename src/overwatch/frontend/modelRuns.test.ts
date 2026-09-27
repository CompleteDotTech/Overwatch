import { describe, expect, it } from "vitest";
import { curve, executionSummary, formatBytes, percentile, phaseLabel, safeWebURL, verificationLabel } from "./modelRuns";

describe("versioned model telemetry presentation", () => {
  it("does not label stale verified values as the new verified attempt", () => {
    const run = { verification: "last_verified_with_newer_uncertainty" as const };
    expect(verificationLabel(run)).toContain("newer observation is uncertain");
    expect(phaseLabel(run)).toBe("Last verified trainer phase");
    expect(verificationLabel({ verification: "unverified" })).toBe("Unverified observation");
  });
  it("preserves historical unknown fields rather than inventing measurements", () => {
    expect(executionSummary(null)).toContain("unknown");
    expect(formatBytes(null)).toBe("unknown");
    expect(formatBytes(1048576)).toBe("1.00 MiB");
    expect(verificationLabel({})).toBe("Verified attempt");
  });
  it("uses only safe links, finite samples and bounded curve coordinates", () => {
    expect(safeWebURL("javascript:alert(1)")).toBe(null);
    expect(safeWebURL(["https://", "user", ":", "secret", "@example.org/file"].join(""))).toBe(null);
    expect(safeWebURL("https://example.org/commit/abc")).toContain("https://");
    expect(percentile([NaN, 8, 1, 3], 0.5)).toBe(3);
    expect(percentile([], 0.5)).toBe(null);
    expect(curve([{ step: 4, phase: "train", metrics: { loss: 2 } },
                  { step: 1, phase: "train", metrics: { loss: 3 } }], "loss")).toBe("256,76 4,4");
  });
});
