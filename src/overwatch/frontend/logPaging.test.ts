import { describe, expect, it } from "vitest";

import { mergeLogEvents } from "./logPaging";
import type { CloudWatchLogEvent } from "./types";

function event(id: string, timestamp: number, html = id): CloudWatchLogEvent {
  return { id, timestamp, html };
}

describe("mergeLogEvents", () => {
  it("deduplicates, orders, and replaces event payloads", () => {
    const result = mergeLogEvents(
      [event("b", 2), event("a", 1, "old")],
      [event("a", 1, "updated"), event("c", 3)],
      "live",
    );

    expect(result.events).toEqual([
      event("a", 1, "updated"),
      event("b", 2),
      event("c", 3),
    ]);
    expect(result.firstItemIndexDelta).toBe(0);
  });

  it.each([
    {
      direction: "older" as const,
      current: [event("c", 3), event("d", 4), event("e", 5)],
      incoming: [event("a", 1), event("b", 2)],
      expectedIds: ["a", "b", "c", "d"],
      expectedDelta: -2,
    },
    {
      direction: "newer" as const,
      current: [event("a", 1), event("b", 2), event("c", 3)],
      incoming: [event("d", 4), event("e", 5)],
      expectedIds: ["b", "c", "d", "e"],
      expectedDelta: 1,
    },
  ])("retains the $direction edge and adjusts the virtual index", (testCase) => {
    const result = mergeLogEvents(
      testCase.current,
      testCase.incoming,
      testCase.direction,
      4,
    );

    expect(result.events.map(({ id }) => id)).toEqual(testCase.expectedIds);
    expect(result.firstItemIndexDelta).toBe(testCase.expectedDelta);
  });
});
