import type { CloudWatchLogEvent } from "./types";

export const MAX_LOG_EVENTS = 2_000;
export const INITIAL_LOG_ITEM_INDEX = 1_000_000;

export type LogMergeDirection = "replace" | "live" | "older" | "newer";

export interface MergedLogEvents {
  events: CloudWatchLogEvent[];
  firstItemIndexDelta: number;
}

export function mergeLogEvents(
  current: CloudWatchLogEvent[],
  incoming: CloudWatchLogEvent[],
  direction: LogMergeDirection,
  maximumEvents = MAX_LOG_EVENTS,
): MergedLogEvents {
  const existing = direction === "replace" ? [] : current;
  const eventsById = new Map(existing.map((event) => [event.id, event]));
  for (const event of incoming) eventsById.set(event.id, event);

  const merged = [...eventsById.values()].sort(
    (left, right) => left.timestamp - right.timestamp || left.id.localeCompare(right.id),
  );
  const events = direction === "older"
    ? merged.slice(0, maximumEvents)
    : merged.slice(-maximumEvents);

  // Keep react-virtuoso's logical index aligned with rows removed from either edge.
  let firstItemIndexDelta = 0;
  if (direction === "older") {
    const oldFirstId = current[0]?.id;
    const oldFirstPosition = oldFirstId == null
      ? -1
      : events.findIndex((event) => event.id === oldFirstId);
    if (oldFirstPosition > 0) firstItemIndexDelta = -oldFirstPosition;
  } else if (direction === "newer") {
    const retainedIds = new Set(events.map((event) => event.id));
    const removedFromStart = current.findIndex((event) => retainedIds.has(event.id));
    if (removedFromStart > 0) firstItemIndexDelta = removedFromStart;
  }
  return { events, firstItemIndexDelta };
}
