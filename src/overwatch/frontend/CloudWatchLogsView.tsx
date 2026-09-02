import {
  ActionIcon,
  Alert,
  Badge,
  Box,
  Card,
  Code,
  Group,
  List,
  Loader,
  Popover,
  Select,
  Text,
  Title,
  Tooltip,
} from "@mantine/core";
import {
  IconAlertTriangle,
  IconArrowBarToDown,
  IconArrowBarToUp,
  IconHelpCircle,
  IconTerminal2,
} from "@tabler/icons-react";
import { useEffect, useRef, useState } from "react";
import { Virtuoso, type VirtuosoHandle } from "react-virtuoso";

import { INITIAL_LOG_ITEM_INDEX, MAX_LOG_EVENTS, mergeLogEvents } from "./logPaging";
import type { CloudWatchLogEvent, CloudWatchLogPage, LogAttempt, Resource } from "./types";

function CloudWatchAttemptHelp() {
  return (
    <Popover width={430} position="bottom-start" shadow="md">
      <Popover.Target>
        <ActionIcon variant="subtle" color="gray" aria-label="How CloudWatch attempts are detected">
          <IconHelpCircle size={19} />
        </ActionIcon>
      </Popover.Target>
      <Popover.Dropdown>
        <Text fw={700} mb="xs">How CloudWatch attempts are detected</Text>
        <List type="ordered" spacing="xs" size="sm">
          <List.Item>Read locally cached events in timestamp order.</List.Item>
          <List.Item>Extract <Code>pid</Code> from structured messages or plain-text <Code>pid=…</Code> markers.</List.Item>
          <List.Item>Order unique PIDs by first event; each PID establishes one attempt window.</List.Item>
          <List.Item>Show every cached event in that window, including untagged setup and teardown lines.</List.Item>
        </List>
        <Alert color="yellow" variant="light" mt="md" icon={<IconAlertTriangle size={17} />}>
          An auxiliary process with its own PID may be mistaken for a recovery attempt.
        </Alert>
      </Popover.Dropdown>
    </Popover>
  );
}
export function CloudWatchLogsView({ resource }: { resource: Resource }) {
  const jobId = resource.skypilot.job_id;
  const [logEvents, setLogEvents] = useState<CloudWatchLogEvent[]>([]);
  const [logStatus, setLogStatus] = useState("Loading cached logs…");
  const [logAttempts, setLogAttempts] = useState<LogAttempt[]>([]);
  const [expectedLogAttempts, setExpectedLogAttempts] = useState(0);
  const [logAttemptsIndexing, setLogAttemptsIndexing] = useState(false);
  const [logBackfillCachedEvents, setLogBackfillCachedEvents] = useState<number | null>(null);
  const [logAttemptError, setLogAttemptError] = useState<string | null>(null);
  const [selectedLogAttempt, setSelectedLogAttempt] = useState<string | null>(null);
  const [logHasOlder, setLogHasOlder] = useState(false);
  const [logHasNewer, setLogHasNewer] = useState(false);
  const [logLoadingDirection, setLogLoadingDirection] = useState<"older" | "newer" | "start" | "end" | null>(null);
  const [browsingOlderLogs, setBrowsingOlderLogs] = useState(false);
  const [logReloadKey, setLogReloadKey] = useState(0);
  const [firstLogItemIndex, setFirstLogItemIndex] = useState(INITIAL_LOG_ITEM_INDEX);
  const logPagingRef = useRef(false);
  const logViewerRef = useRef<VirtuosoHandle>(null);
  const pendingLogStartJumpRef = useRef(false);
  const pendingLogEndJumpRef = useRef(false);
  const logSelectionKey = `${jobId ?? "none"}:${selectedLogAttempt ?? "none"}`;
  const activeLogSelectionRef = useRef(logSelectionKey);
  activeLogSelectionRef.current = logSelectionKey;

  useEffect(() => {
    if (!pendingLogStartJumpRef.current || logEvents.length === 0) return;
    pendingLogStartJumpRef.current = false;
    logViewerRef.current?.scrollToIndex({
      index: firstLogItemIndex,
      align: "start",
    });
  }, [firstLogItemIndex, logEvents]);

  useEffect(() => {
    if (
      jobId == null ||
      resource.skypilot.cloud?.toLowerCase() !== "aws" ||
      !resource.skypilot.cluster_name
    ) return;
    const controller = new AbortController();
    let pollTimer: number | undefined;
    setLogAttemptsIndexing(true);

    // Poll cached attempt metadata while the collector backfills history independently.
    const loadAttempts = () => {
      fetch(`/api/logs/${jobId}/attempts`, { signal: controller.signal })
        .then(async (response) => {
          if (!response.ok) throw new Error(await response.text());
          return response.json() as Promise<{
            attempts: LogAttempt[];
            expected_attempts: number;
            indexing: boolean;
            backfill: { cached_events: number | null };
            error: string | null;
          }>;
        })
        .then(({ attempts, expected_attempts, indexing, backfill, error }) => {
          setLogAttempts(attempts);
          setExpectedLogAttempts(expected_attempts);
          setLogAttemptsIndexing(indexing);
          setLogBackfillCachedEvents(backfill.cached_events);
          setLogAttemptError(error);
          if (error) setLogStatus(error);
          const newestAttempt = attempts.find((attempt) => attempt.current) ?? attempts.at(-1);
          if (newestAttempt) {
            setSelectedLogAttempt((selected) =>
              selected == null || selected === "live" ? newestAttempt.attempt.toString() : selected,
            );
          } else {
            setSelectedLogAttempt((selected) => selected ?? "live");
          }
          if (indexing) pollTimer = window.setTimeout(loadAttempts, 5_000);
        })
        .catch((error: Error) => {
          if (error.name !== "AbortError") {
            setLogAttemptsIndexing(false);
            setLogAttemptError(error.message);
          }
        });
    };
    loadAttempts();
    return () => {
      controller.abort();
      if (pollTimer != null) window.clearTimeout(pollTimer);
    };
  }, [jobId, resource.skypilot.cloud, resource.skypilot.cluster_name]);

  useEffect(() => {
    if (
      jobId == null ||
      resource.skypilot.cloud?.toLowerCase() !== "aws" ||
      !resource.skypilot.cluster_name ||
      !selectedLogAttempt ||
      browsingOlderLogs
    ) return;
    logPagingRef.current = false;
    setLogEvents([]);
    setLogHasOlder(false);
    setLogHasNewer(false);
    setFirstLogItemIndex(INITIAL_LOG_ITEM_INDEX);
    setLogStatus("Loading cached logs…");
    const logUrl =
      selectedLogAttempt === "live"
        ? `/api/logs/${jobId}`
        : `/api/logs/${jobId}?attempt=${selectedLogAttempt}`;
    const eventSource = new EventSource(logUrl);
    const mergeLogBatch = (event: MessageEvent<string>, replace = false) => {
      const payload = JSON.parse(event.data) as CloudWatchLogPage;
      setLogEvents((current) => {
        return mergeLogEvents(
          current,
          payload.events,
          replace ? "replace" : "live",
        ).events;
      });
      setLogHasOlder(payload.has_older);
      setLogHasNewer(payload.has_newer);
      setLogStatus("");
      if (replace && pendingLogEndJumpRef.current) {
        pendingLogEndJumpRef.current = false;
        logPagingRef.current = false;
        setLogLoadingDirection(null);
        window.requestAnimationFrame(() =>
          logViewerRef.current?.scrollToIndex({
            index: INITIAL_LOG_ITEM_INDEX + payload.events.length - 1,
            align: "end",
          }),
        );
      }
    };
    eventSource.onmessage = mergeLogBatch;
    eventSource.addEventListener("tail", (event) =>
      mergeLogBatch(event as MessageEvent<string>, true),
    );
    eventSource.onerror = () => {
      pendingLogEndJumpRef.current = false;
      logPagingRef.current = false;
      setLogLoadingDirection(null);
      setLogStatus("Waiting for cached log updates…");
    };
    eventSource.addEventListener("complete", () => {
      eventSource.close();
      setLogStatus("— End of attempt —");
    });
    return () => eventSource.close();
  }, [jobId, resource.skypilot.cloud, resource.skypilot.cluster_name, selectedLogAttempt, browsingOlderLogs, logReloadKey]);

  const loadLogPage = async (direction: "older" | "newer") => {
    const requestedSelection = activeLogSelectionRef.current;
    const boundaryEvent = direction === "older" ? logEvents[0] : logEvents.at(-1);
    if (
      logPagingRef.current ||
      jobId == null ||
      !selectedLogAttempt ||
      selectedLogAttempt === "live" ||
      !boundaryEvent ||
      (direction === "older" ? !logHasOlder : !logHasNewer)
    ) return;
    logPagingRef.current = true;
    setLogLoadingDirection(direction);
    if (direction === "older") setBrowsingOlderLogs(true);
    try {
      const query = new URLSearchParams({
        attempt: selectedLogAttempt,
        [direction === "older" ? "before" : "after"]: `${boundaryEvent.timestamp}:${boundaryEvent.id}`,
      });
      const response = await fetch(`/api/logs/${jobId}/page?${query}`);
      if (!response.ok) throw new Error(await response.text());
      const page = await response.json() as CloudWatchLogPage;
      if (activeLogSelectionRef.current !== requestedSelection) return;
      const merged = mergeLogEvents(logEvents, page.events, direction);
      if (merged.firstItemIndexDelta !== 0) {
        setFirstLogItemIndex((index) => index + merged.firstItemIndexDelta);
      }
      setLogEvents(merged.events);
      setLogHasOlder(page.has_older);
      setLogHasNewer(page.has_newer);
      setLogStatus(
        page.events.length
          ? ""
          : direction === "older"
            ? "— Start of attempt —"
            : "— End of attempt —",
      );
      if (direction === "newer" && !page.has_newer) {
        setBrowsingOlderLogs(false);
        setLogReloadKey((key) => key + 1);
      }
    } catch (error) {
      setLogStatus(error instanceof Error ? error.message : String(error));
    } finally {
      if (activeLogSelectionRef.current === requestedSelection) {
        logPagingRef.current = false;
        setLogLoadingDirection(null);
      }
    }
  };

  const jumpToLogEdge = async (edge: "start" | "end") => {
    const requestedSelection = activeLogSelectionRef.current;
    if (
      logPagingRef.current ||
      jobId == null ||
      !selectedLogAttempt ||
      selectedLogAttempt === "live"
    ) return;
    logPagingRef.current = true;
    setLogLoadingDirection(edge);
    setLogStatus(`Loading ${edge} of attempt…`);
    if (edge === "end") {
      pendingLogEndJumpRef.current = true;
      setBrowsingOlderLogs(false);
      setLogReloadKey((key) => key + 1);
      return;
    }
    setBrowsingOlderLogs(true);
    try {
      const query = new URLSearchParams({
        attempt: selectedLogAttempt,
        edge: "start",
      });
      const response = await fetch(`/api/logs/${jobId}/page?${query}`);
      if (!response.ok) throw new Error(await response.text());
      const page = await response.json() as CloudWatchLogPage;
      if (activeLogSelectionRef.current !== requestedSelection) return;
      setLogEvents(page.events);
      setLogHasOlder(page.has_older);
      setLogHasNewer(page.has_newer);
      setFirstLogItemIndex(INITIAL_LOG_ITEM_INDEX);
      setLogStatus(page.events.length ? "" : "— Start of attempt —");
      pendingLogStartJumpRef.current = page.events.length > 0;
    } catch (error) {
      setLogStatus(error instanceof Error ? error.message : String(error));
    } finally {
      if (activeLogSelectionRef.current === requestedSelection) {
        logPagingRef.current = false;
        setLogLoadingDirection(null);
      }
    }
  };

  if (jobId == null || resource.skypilot.cloud?.toLowerCase() !== "aws") {
    return (
      <Alert color="gray" icon={<IconTerminal2 size={18} />}>
        CloudWatch logs are unavailable for this run.
      </Alert>
    );
  }
  if (!resource.skypilot.cluster_name) {
    return (
      <Card withBorder padding="lg">
        <Group gap="xs" mb="sm">
          <IconTerminal2 size={20} />
          <Title order={2}>CloudWatch logs</Title>
          <CloudWatchAttemptHelp />
        </Group>
        <Alert color="yellow" icon={<IconAlertTriangle size={18} />}>
          Logs are not populated yet. SkyPilot reports this job as <Code>{resource.status.skypilot ?? "unknown"}</Code> and has not published its AWS cluster link. The collector will retry during refreshes.
        </Alert>
      </Card>
    );
  }
  return (
    <Card withBorder padding="lg">
      <Group mb="md" justify="space-between" align="flex-end">
        <Box>
          <Group gap="xs">
            <IconTerminal2 size={20} />
            <Title order={2}>CloudWatch logs</Title>
            <CloudWatchAttemptHelp />
          </Group>
          <Text c="dimmed" size="sm">Served from the local raw cache</Text>
        </Box>
        <Group gap="xs" align="flex-end">
          <Select
            label="Attempt"
            placeholder="Finding attempts…"
            value={selectedLogAttempt}
            onChange={(attempt) => {
              setBrowsingOlderLogs(false);
              setFirstLogItemIndex(INITIAL_LOG_ITEM_INDEX);
              setSelectedLogAttempt(attempt);
            }}
            data={[
              ...(logAttempts.length === 0
                ? [{ value: "live", label: "Latest cached logs" }]
                : []),
              ...logAttempts.map((attempt) => ({
                value: attempt.attempt.toString(),
                label: `Attempt ${attempt.attempt}${attempt.current ? " · current/live" : ""} · PID ${attempt.pid}`,
              })),
            ]}
            w={360}
          />
          {logAttemptsIndexing && <Loader size="xs" mb="sm" />}
          <Tooltip label={logAttemptError ?? undefined} disabled={!logAttemptError}>
            <Badge
              mb="sm"
              color={logAttemptError ? "red" : logAttemptsIndexing ? "blue" : logAttempts.length === expectedLogAttempts ? "green" : "yellow"}
              variant="light"
            >
              {logAttemptsIndexing
                ? `Backfilling${logBackfillCachedEvents != null ? ` · ${logBackfillCachedEvents.toLocaleString()} events` : ""} · `
                : ""}
              {logAttempts.length}/{expectedLogAttempts || "?"} attempts
            </Badge>
          </Tooltip>
        </Group>
      </Group>
      <Group justify="space-between" mb="xs">
        <Group gap="xs">
          <Tooltip label="Jump to start of attempt">
            <ActionIcon
              aria-label="Jump to start of attempt"
              variant="light"
              loading={logLoadingDirection === "start"}
              disabled={!selectedLogAttempt || selectedLogAttempt === "live"}
              onClick={() => void jumpToLogEdge("start")}
            >
              <IconArrowBarToUp size={17} />
            </ActionIcon>
          </Tooltip>
          <Tooltip label="Jump to end of attempt">
            <ActionIcon
              aria-label="Jump to end of attempt"
              variant="light"
              loading={logLoadingDirection === "end"}
              disabled={!selectedLogAttempt || selectedLogAttempt === "live"}
              onClick={() => void jumpToLogEdge("end")}
            >
              <IconArrowBarToDown size={17} />
            </ActionIcon>
          </Tooltip>
          <Text size="xs" c="dimmed">
            {logLoadingDirection
              ? `Loading ${logLoadingDirection} cached events…`
              : "Scroll to the top or bottom to load more"}
          </Text>
        </Group>
        <Text size="xs" c="dimmed">
          {logEvents.length.toLocaleString()} events in memory · maximum {MAX_LOG_EVENTS.toLocaleString()}
        </Text>
      </Group>
      <Box bg="dark.9" style={{ height: 620, overflow: "hidden" }}>
        <Virtuoso
          ref={logViewerRef}
          data={logEvents}
          firstItemIndex={firstLogItemIndex}
          followOutput={browsingOlderLogs ? false : "auto"}
          startReached={() => void loadLogPage("older")}
          endReached={() => void loadLogPage("newer")}
          computeItemKey={(_index, item) => item.id}
          itemContent={(_index, item) => (
            <Box
              px="md"
              py={2}
              c="green.2"
              ff="monospace"
              fz="xs"
              style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}
            >
              <span>[{new Date(item.timestamp).toLocaleTimeString()}] </span>
              <span dangerouslySetInnerHTML={{ __html: item.html }} />
            </Box>
          )}
          components={{
            Footer: () => logStatus ? (
              <Text c="dimmed" ff="monospace" fz="xs" p="md">{logStatus}</Text>
            ) : null,
          }}
        />
      </Box>
    </Card>
  );
}
