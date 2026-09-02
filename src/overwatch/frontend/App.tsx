import {
  ActionIcon,
  Alert,
  Anchor,
  AppShell,
  Badge,
  Box,
  Button,
  Card,
  Center,
  Code,
  CopyButton,
  Drawer,
  Group,
  Loader,
  Modal,
  Progress,
  ScrollArea,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  TextInput,
  ThemeIcon,
  Title,
  Tooltip,
  UnstyledButton,
  useMantineColorScheme,
} from "@mantine/core";
import {
  IconAlertTriangle,
  IconActivityHeartbeat,
  IconArrowLeft,
  IconChartLine,
  IconCheck,
  IconCode,
  IconCopy,
  IconCurrencyDollar,
  IconDatabase,
  IconRefresh,
  IconSearch,
  IconServer,
} from "@tabler/icons-react";
import { Fragment, useEffect, useMemo, useRef, useState } from "react";

import { BillingPage } from "./BillingPage";
import { CloudWatchLogsView } from "./CloudWatchLogsView";
import {
  ACTIVE_STATUSES,
  aggregateResourceSpend,
  findWasteCandidates,
  resourcesWithRecoveries,
} from "./costWaste";
import type { SpendBreakdownRow } from "./costWaste";
import {
  formatDuration,
  formatMoney,
  formatTimestamp,
  queryDiagnosticColor,
  StatCard,
  statusColor,
  Warnings,
} from "./presentation";
import { compareResources } from "./resourceSorting";
import type { ResourceSortKey } from "./resourceSorting";
import type {
  ConfigDifference,
  QueryStatusReport,
  Report,
  Resource,
} from "./types";

type Page = "resources" | "billing" | "cost-waste" | "run";

function Navigation({
  page,
  queryStatus,
  onOpenStatus,
  onRefresh,
}: {
  page: Page;
  queryStatus: QueryStatusReport | null;
  onOpenStatus: () => void;
  onRefresh: () => void;
}) {
  const { colorScheme, setColorScheme } = useMantineColorScheme();
  const hasQueryError = Boolean(
    queryStatus?.refresh.last_error || queryStatus?.queries.some((query) => query.status === "error"),
  );
  const hasQueryWarning = Boolean(queryStatus?.queries.some((query) => query.status === "warning"));
  const queryStatusColor = hasQueryError ? "red" : hasQueryWarning ? "yellow" : "green";
  return (
    <AppShell.Header px="md" className="hud-header">
      <Group h="100%" justify="space-between" wrap="nowrap">
        <Group gap="sm" wrap="nowrap">
          <ThemeIcon className="hud-logo" size="lg">
            <img src="/static/dist/overwatch-favicon.svg" width={30} height={30} alt="" />
          </ThemeIcon>
          <Anchor href="/" c="inherit" underline="never" className="hud-brand">
            Overwatch
          </Anchor>
          <Badge color={queryStatusColor} variant="light" size="xs">
            {hasQueryError ? "Degraded" : "Live"}
          </Badge>
        </Group>
        <Group gap="xs" wrap="nowrap">
          <Button
            component="a"
            href="/"
            variant={page === "resources" || page === "run" ? "light" : "subtle"}
            leftSection={<IconServer size={16} />}
          >
            Resources
          </Button>
          <Button
            component="a"
            href="/cost-waste"
            variant={page === "cost-waste" ? "light" : "subtle"}
            leftSection={<IconCurrencyDollar size={16} />}
          >
            Cost & waste
          </Button>
          <Button
            component="a"
            href="/billing"
            variant={page === "billing" ? "light" : "subtle"}
            leftSection={<IconChartLine size={16} />}
          >
            Billing
          </Button>
          <Button
            variant="subtle"
            leftSection={<IconRefresh size={16} />}
            loading={queryStatus?.refresh.in_progress ?? false}
            onClick={onRefresh}
          >
            Refresh
          </Button>
          <Button
            variant="subtle"
            color={queryStatusColor}
            leftSection={<IconActivityHeartbeat size={16} />}
            onClick={onOpenStatus}
          >
            Status
          </Button>
          <Select
            aria-label="Color scheme"
            allowDeselect={false}
            data={[
              { value: "auto", label: "System" },
              { value: "light", label: "Light" },
              { value: "dark", label: "Dark" },
            ]}
            value={colorScheme}
            onChange={(value) => value && setColorScheme(value as "auto" | "light" | "dark")}
            w={105}
          />
        </Group>
      </Group>
    </AppShell.Header>
  );
}

function SortHeader({
  label,
  secondaryLabel,
  value,
  active,
  descending,
  onSort,
}: {
  label: string;
  secondaryLabel?: string;
  value: ResourceSortKey;
  active: boolean;
  descending: boolean;
  onSort: (value: ResourceSortKey) => void;
}) {
  return (
    <Table.Th>
      <UnstyledButton fw={700} onClick={() => onSort(value)}>
        <span>{label} {active ? (descending ? "↓" : "↑") : "↕"}</span>
        {secondaryLabel && <Text component="span" display="block" size="xs" c="dimmed" fw={400}>{secondaryLabel}</Text>}
      </UnstyledButton>
    </Table.Th>
  );
}

function ResourceLinks({ resource }: { resource: Resource }) {
  const links = [
    {
      label: "W&B",
      url: resource.links.wandb,
      color: "yellow",
      icon: (
        <Text component="span" size="sm" fw={900} c="yellow.8">
          W
        </Text>
      ),
    },
    {
      label: "SkyPilot",
      url: resource.links.skypilot,
      color: "blue",
      icon: (
        <svg width="18" height="18" viewBox="0 0 27 27" aria-hidden="true">
          <path
            fill="#094AD0"
            fillRule="evenodd"
            d="M19.94 25.75c-.08.31-.12.46-.18.55a.7.7 0 0 1-.91.25c-.1-.05-.22-.16-.45-.39l-7-7c-.15-.15-.22-.23-.28-.31a1.4 1.4 0 0 1-.24-.88c.01-.1.04-.2.1-.41l.6-2.27c.02-.07.03-.1.03-.13a.16.16 0 0 0-.15-.15c-.02 0-.06.01-.13.03l-2.26.61c-.21.06-.31.08-.41.09a1.4 1.4 0 0 1-.89-.23c-.08-.06-.15-.14-.3-.29l-7-7c-.23-.23-.34-.34-.4-.44a.7.7 0 0 1 .25-.91c.1-.06.25-.1.56-.19L25.38.12c.31-.08.47-.13.58-.12.36.02.65.31.67.67 0 .11-.04.27-.12.57l-6.57 24.51ZM5.49 8.37c-.14.04-.2.06-.25.09a.32.32 0 0 0-.1.41c.02.05.07.1.17.2l3.42 3.42c.06.06.09.09.12.11.1.07.21.1.33.09.04 0 .08-.02.15-.04l5.66-1.52c.14-.04.2-.05.25-.05.17.01.3.14.3.3.01.05 0 .12-.04.26l-1.52 5.65c-.02.08-.04.12-.04.16-.01.11.02.23.09.32.02.03.05.06.1.12l3.43 3.43c.1.1.15.15.2.17.14.08.32.03.4-.1.03-.05.05-.12.09-.26l4.5-16.76c.04-.14.06-.21.06-.26a.32.32 0 0 0-.3-.3c-.05 0-.12.01-.26.05L5.49 8.37Z"
          />
        </svg>
      ),
    },
    {
      label: "Zymtrace",
      url: resource.links.zymtrace,
      color: "grape",
      icon: <img alt="" src="https://zymtrace.com/favicon.ico" width={18} height={18} />,
    },
  ];
  return (
    <Group gap={4} wrap="wrap" maw={116}>
      {links.map(({ label, url, color, icon }) =>
        url ? (
          <Tooltip key={label} label={label}>
            <ActionIcon
              component="a"
              href={url}
              target="_blank"
              color={color}
              variant="subtle"
              aria-label={label}
            >
              {icon}
            </ActionIcon>
          </Tooltip>
        ) : null,
      )}
      {resource.storage.run_uri && (
        <CopyButton value={resource.storage.run_uri}>
          {({ copied, copy }) => (
            <Tooltip label={copied ? "Copied" : "Copy storage URI"}>
              <ActionIcon
                aria-label="Copy storage URI"
                color={copied ? "green" : "gray"}
                variant="subtle"
                onClick={copy}
              >
                {copied ? <IconCheck size={16} /> : <IconDatabase size={16} />}
              </ActionIcon>
            </Tooltip>
          )}
        </CopyButton>
      )}
    </Group>
  );
}

function ResourceNameLink({ resource }: { resource: Resource }) {
  return resource.skypilot.job_id != null ? (
    <Anchor fw={600} href={`/runs/${resource.skypilot.job_id}`}>
      {resource.name}
    </Anchor>
  ) : (
    <Text fw={600}>{resource.name}</Text>
  );
}

function ResourcesTable({ resources }: { resources: Resource[] }) {
  const [sortKey, setSortKey] = useState<ResourceSortKey>("status");
  const [descending, setDescending] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");

  const sortedResources = useMemo(() => {
    const normalizedQuery = searchQuery.trim().toLocaleLowerCase();
    const matchingResources = normalizedQuery
      ? resources.filter((resource) =>
          [
            resource.name,
            resource.user,
            resource.project,
            resource.wandb_id,
            resource.kind,
            resource.status.skypilot,
            resource.status.wandb,
            resource.skypilot.job_id,
            resource.skypilot.job_name,
            resource.skypilot.cluster_name,
            resource.skypilot.resources,
            resource.skypilot.cloud,
            resource.skypilot.region,
          ]
            .filter((value) => value != null)
            .some((value) => String(value).toLocaleLowerCase().includes(normalizedQuery)),
        )
      : resources;

    return [...matchingResources].sort((left, right) =>
      compareResources(left, right, sortKey, descending),
    );
  }, [descending, resources, searchQuery, sortKey]);


  const onSort = (value: ResourceSortKey) => {
    if (sortKey === value) setDescending((current) => !current);
    else {
      setSortKey(value);
      setDescending(false);
    }
  };

  return (
    <>
      <Card withBorder padding={0}>
        {resources.length > 1 && <Box p="md">
          <TextInput
            aria-label="Search resources"
            placeholder="Search resources by name, user, project, status, cloud, region, or job ID…"
            leftSection={<IconSearch size={17} />}
            rightSection={
              <Text size="xs" c="dimmed">
                {sortedResources.length}/{resources.length}
              </Text>
            }
            rightSectionWidth={64}
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.currentTarget.value)}
          />
        </Box>}
        <Table.ScrollContainer minWidth={1320}>
          <Table striped highlightOnHover verticalSpacing="sm">
            <Table.Thead>
              <Table.Tr>
                <SortHeader label="User / project / run" value="name" active={sortKey === "name"} descending={descending} onSort={onSort} />
                <SortHeader label="Status" secondaryLabel="then newest started" value="status" active={sortKey === "status"} descending={descending} onSort={onSort} />
                <SortHeader label="Progress" value="progress" active={sortKey === "progress"} descending={descending} onSort={onSort} />
                <Table.Th>
                  Retries
                  <Text size="xs" c="dimmed" fw={400}>total | preempts | error</Text>
                </Table.Th>
                <SortHeader label="Started" secondaryLabel="elapsed > time left" value="started" active={sortKey === "started"} descending={descending} onSort={onSort} />
                <SortHeader label="Cost" secondaryLabel="spent / projected · $/hr" value="cost" active={sortKey === "cost"} descending={descending} onSort={onSort} />
                <SortHeader label="Cloud / region" secondaryLabel="instance resources" value="cloud" active={sortKey === "cloud"} descending={descending} onSort={onSort} />
                <Table.Th>Links</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {sortedResources.length === 0 && (
                <Table.Tr>
                  <Table.Td colSpan={8}>
                    <Center py="xl">
                      <Text c="dimmed">No resources match “{searchQuery.trim()}”.</Text>
                    </Center>
                  </Table.Td>
                </Table.Tr>
              )}
              {sortedResources.map((resource) => {
                const progress = Math.max(0, Math.min(100, (resource.progress.progress_fraction ?? 0) * 100));
                return (
                  <Table.Tr key={`${resource.kind}-${resource.skypilot.job_id ?? resource.name}`}>
                    <Table.Td miw={280}>
                      <Text size="xs" c="dimmed">
                        {resource.user ?? "—"} / {resource.project ?? "—"}
                        {resource.skypilot.job_id != null && ` · Sky job ${resource.skypilot.job_id}`}
                      </Text>
                      <ResourceNameLink resource={resource} />
                    </Table.Td>
                    <Table.Td>
                      <Stack gap={4} align="flex-start">
                        {resource.status.wandb && <Badge color={statusColor(resource.status.wandb)} variant="light">W&B · {resource.status.wandb}</Badge>}
                        <Badge color={statusColor(resource.status.skypilot)} variant="light">Sky · {resource.status.skypilot ?? "unknown"}</Badge>
                      </Stack>
                    </Table.Td>
                    <Table.Td miw={180}>
                      {resource.progress.total_batches ? (
                        <Stack gap={4}>
                          <Text size="xs">{resource.progress.completed_batches?.toLocaleString()}/{resource.progress.total_batches.toLocaleString()} batches</Text>
                          <Progress.Root size="lg" autoContrast>
                            <Progress.Section value={progress}>
                              <Progress.Label>{Math.round(progress)}%</Progress.Label>
                            </Progress.Section>
                          </Progress.Root>
                        </Stack>
                      ) : <Text c="dimmed">—</Text>}
                      {resource.progress.tokens_per_second != null && <Text size="xs" c="dimmed" mt={4}>{resource.progress.tokens_per_second.toLocaleString()} tok/s</Text>}
                    </Table.Td>
                    <Table.Td>{resource.retries.total_recoveries ?? "?"} | {resource.retries.preemption_or_infrastructure ?? "?"} | {resource.retries.application_error ?? "?"}</Table.Td>
                    <Table.Td miw={150}>
                      <Text size="xs" c="dimmed">{formatTimestamp(resource.timing.started_at)}</Text>
                      <Text>{formatDuration(resource.timing.elapsed_seconds)} &gt; {formatDuration(resource.progress.estimated_remaining_seconds)}</Text>
                    </Table.Td>
                    <Table.Td miw={150}>
                      <Text>{formatMoney(resource.cost.estimated_spend_usd)} / {formatMoney(resource.cost.estimated_total_usd)}</Text>
                      <Text size="xs" c="dimmed">{formatMoney(resource.cost.hourly_usd)}/hr</Text>
                    </Table.Td>
                    <Table.Td miw={150}>
                      <Text>{resource.skypilot.cloud ?? "—"}</Text>
                      <Text size="xs" c="dimmed">{resource.skypilot.region ?? "—"}</Text>
                      <Text size="xs" ff="monospace" mt={3}>{resource.skypilot.resources ?? "—"}</Text>
                    </Table.Td>
                    <Table.Td><ResourceLinks resource={resource} /></Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      </Card>
    </>
  );
}

function ConfigDifferences({ projects }: { projects: ConfigDifference[] }) {
  if (!projects.length) return <Text c="dimmed">No differing TrainConfig fields.</Text>;
  return (
    <Stack gap="lg">
      {projects.map((project) => (
        <Card withBorder key={project.project}>
          <Title order={3} mb="md">{project.project}</Title>
          <Table.ScrollContainer minWidth={700}>
            <Table striped withColumnBorders>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Field</Table.Th>
                  {project.runs.map((run) => (
                    <Table.Th key={run.wandb_id}>
                      <Anchor href={run.wandb_url ?? undefined} target="_blank" rel="noreferrer">
                        {run.experiment_name}
                      </Anchor>
                    </Table.Th>
                  ))}
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {project.fields.map((field) => (
                  <Table.Tr key={field.field}>
                    <Table.Td><Code>{field.field}</Code></Table.Td>
                    {field.values.map((cell) => (
                      <Table.Td key={cell.wandb_id}>
                        <Code>{cell.missing ? "<missing>" : JSON.stringify(cell.value)}</Code>
                      </Table.Td>
                    ))}
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        </Card>
      ))}
    </Stack>
  );
}

function ResourcesPage({ report }: { report: Report }) {
  const activeResources = report.resources.filter((resource) =>
    ACTIVE_STATUSES.has(resource.status.skypilot ?? ""),
  );
  const hourly = activeResources.reduce((sum, resource) => sum + (resource.cost.hourly_usd ?? 0), 0);
  const spent = report.resources.reduce((sum, resource) => sum + (resource.cost.estimated_spend_usd ?? 0), 0);
  const projected = activeResources.reduce((sum, resource) => sum + (resource.cost.estimated_total_usd ?? 0), 0);
  return (
    <Stack gap="xl">
      <Box>
        <Title order={1}>Resource overview</Title>
        <Text c="dimmed">{report.resources.length} cloud resources · generated {formatTimestamp(report.generated_at)}</Text>
      </Box>
      <Warnings warnings={report.warnings} />
      <SimpleGrid cols={{ base: 1, sm: 3 }}>
        <StatCard label="Live burn rate" value={`${formatMoney(hourly)}/hr`} />
        <StatCard label="Spent so far" value={formatMoney(spent)} />
        <StatCard label="Active projected total" value={formatMoney(projected)} />
      </SimpleGrid>
      <Box id="resources">
        <Title order={2} mb="md">Resources</Title>
        <ResourcesTable resources={report.resources} />
      </Box>
      <Box id="config">
        <Title order={2} mb="md">Running Job TrainConfig Deltas</Title>
        <ConfigDifferences projects={report.config_differences} />
      </Box>
    </Stack>
  );
}

function RunPage({ report, jobId }: { report: Report; jobId: number }) {
  const resource = report.resources.find((item) => item.skypilot.job_id === jobId);
  if (!resource) {
    return (
      <Alert color="yellow" icon={<IconAlertTriangle size={18} />}>
        Job {jobId} is not present in the retained SkyPilot cache.
      </Alert>
    );
  }
  return (
    <Stack gap="xl">
      <Box>
        <Button
          component="a"
          href="/"
          variant="subtle"
          leftSection={<IconArrowLeft size={16} />}
          mb="sm"
        >
          All resources
        </Button>
        <Group justify="space-between" align="flex-start">
          <Box>
            <Title order={1}>{resource.name}</Title>
            <Text c="dimmed">
              SkyPilot job {jobId} · {resource.user ?? "Unknown user"} · {resource.project ?? "No W&B project"}
            </Text>
          </Box>
          <Group gap="xs">
            {resource.status.wandb && (
              <Badge color={statusColor(resource.status.wandb)} size="lg" variant="light">
                W&B · {resource.status.wandb}
              </Badge>
            )}
            <Badge color={statusColor(resource.status.skypilot)} size="lg" variant="light">
              Sky · {resource.status.skypilot ?? "unknown"}
            </Badge>
          </Group>
        </Group>
      </Box>

      {(resource.cache?.missing_files.length ?? 0) > 0 && (
        <Alert color="yellow" icon={<IconAlertTriangle size={18} />} title="Raw cache is incomplete">
          Missing cached {resource.cache!.missing_files.length === 1 ? "file" : "files"}: {resource.cache!.missing_files.map((path, index) => (
            <Fragment key={path}>
              {index > 0 && ", "}<Code>{path}</Code>
              {resource.cache!.errors[path] && ` (${resource.cache!.errors[path]})`}
            </Fragment>
          ))}. W&B and TrainConfig files may remain unavailable until the workload initializes them.
        </Alert>
      )}

      <Box>
        <Title order={2} mb="md">Run telemetry</Title>
        <ResourcesTable resources={[resource]} />
      </Box>

      <Box>
        <Title order={2} mb="md">Extra Info</Title>
        <Card withBorder padding={0}>
          <Table verticalSpacing="sm" withColumnBorders>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Raw cache directory</Table.Th>
                <Table.Th>TrainConfig run_dir</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              <Table.Tr>
                {[
                  {
                    label: "Raw cache directory",
                    value: `${report.raw_cache_root ?? "~/.cache/overwatch/raw-v1"}/jobs/${jobId}`,
                  },
                  {
                    label: "TrainConfig run_dir",
                    value: resource.storage.run_uri,
                  },
                ].map(({ label, value }) => (
                  <Table.Td key={label} w="50%">
                    <Group justify="space-between" wrap="nowrap" align="flex-start">
                      <Code style={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere" }}>
                        {value ?? "—"}
                      </Code>
                      <CopyButton value={value ?? ""}>
                        {({ copied, copy }) => (
                          <Tooltip label={copied ? "Copied" : `Copy ${label}`}>
                            <ActionIcon
                              aria-label={`Copy ${label}`}
                              color={copied ? "green" : "gray"}
                              variant="subtle"
                              disabled={!value}
                              onClick={copy}
                            >
                              {copied ? <IconCheck size={16} /> : <IconCopy size={16} />}
                            </ActionIcon>
                          </Tooltip>
                        )}
                      </CopyButton>
                    </Group>
                  </Table.Td>
                ))}
              </Table.Tr>
            </Table.Tbody>
          </Table>
        </Card>
      </Box>

      <CloudWatchLogsView resource={resource} />

    </Stack>
  );
}

function SpendBreakdown({ title, rows }: { title: string; rows: SpendBreakdownRow[] }) {
  const largestSpend = Math.max(...rows.map((row) => row.spend), 0);
  return (
    <Card withBorder>
      <Title order={3} mb="md">{title}</Title>
      {rows.length ? (
        <Stack gap="md">
          {rows.map((row) => (
            <Box key={row.name}>
              <Group justify="space-between" wrap="nowrap">
                <Text fw={600} truncate>{row.name}</Text>
                <Text size="sm">{formatMoney(row.spend)}</Text>
              </Group>
              <Progress value={largestSpend ? (row.spend / largestSpend) * 100 : 0} mt={4} />
              <Text c="dimmed" size="xs">{formatMoney(row.hourly)}/hr currently active</Text>
            </Box>
          ))}
        </Stack>
      ) : (
        <Text c="dimmed">No attributable resource spend.</Text>
      )}
    </Card>
  );
}

function CostWastePage({ report }: { report: Report }) {
  const wasteCandidates = findWasteCandidates(report.resources);
  const candidateHourly = wasteCandidates.reduce(
    (sum, candidate) => sum + (candidate.resource.cost.hourly_usd ?? 0),
    0,
  );
  const recoveries = resourcesWithRecoveries(report.resources);
  const aggregateSpend = (labelFor: (resource: Resource) => string) =>
    aggregateResourceSpend(report.resources, labelFor);
  return (
    <Stack gap="xl">
      <Box>
        <Title order={1}>Cost & waste</Title>
        <Text c="dimmed">
          Resource-cost estimates and actionable utilization signals from the retained SkyPilot inventory
        </Text>
      </Box>
      <Alert color="blue" icon={<IconAlertTriangle size={18} />}>
        Waste flags are candidates for review, not proof of inactivity. Progress is inferred from W&B and
        training logs; spend below uses Sky catalog estimates rather than provider billing allocation.
      </Alert>
      <SimpleGrid cols={{ base: 1, sm: 3 }}>
        <StatCard label="Waste candidates" value={wasteCandidates.length.toLocaleString()} />
        <StatCard label="Candidate burn rate" value={`${formatMoney(candidateHourly)}/hr`} />
        <StatCard
          label="Recoveries retained"
          value={recoveries
            .reduce((sum, resource) => sum + (resource.retries.total_recoveries ?? 0), 0)
            .toLocaleString()}
        />
      </SimpleGrid>

      <Box>
        <Title order={2} mb="md">Idle, orphaned, and zombie candidates</Title>
        <Card withBorder padding={0}>
          <Table.ScrollContainer minWidth={900}>
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Resource</Table.Th>
                  <Table.Th>Signals</Table.Th>
                  <Table.Th>Age</Table.Th>
                  <Table.Th>Burn rate</Table.Th>
                  <Table.Th>Cloud</Table.Th>
                  <Table.Th>Links</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {wasteCandidates.map(({ resource, reasons }) => (
                  <Table.Tr key={`${resource.kind}-${resource.skypilot.job_id ?? resource.name}`}>
                    <Table.Td>
                      <ResourceNameLink resource={resource} />
                      <Text c="dimmed" size="xs">{resource.user ?? "Unknown user"}</Text>
                    </Table.Td>
                    <Table.Td>
                      <Group gap={4}>{reasons.map((reason) => <Badge key={reason} color="yellow" variant="light">{reason}</Badge>)}</Group>
                    </Table.Td>
                    <Table.Td>{formatDuration(resource.timing.elapsed_seconds)}</Table.Td>
                    <Table.Td>{formatMoney(resource.cost.hourly_usd)}/hr</Table.Td>
                    <Table.Td>{resource.skypilot.cloud ?? "—"}</Table.Td>
                    <Table.Td><ResourceLinks resource={resource} /></Table.Td>
                  </Table.Tr>
                ))}
                {!wasteCandidates.length && (
                  <Table.Tr><Table.Td colSpan={6}><Text c="dimmed">No candidates detected.</Text></Table.Td></Table.Tr>
                )}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        </Card>
      </Box>

      <Box>
        <Title order={2} mb="md">Spend by owner and infrastructure</Title>
        <SimpleGrid cols={{ base: 1, lg: 3 }}>
          <SpendBreakdown title="By user" rows={aggregateSpend((resource) => resource.user ?? "Unknown user")} />
          <SpendBreakdown title="By project" rows={aggregateSpend((resource) => resource.project ?? "Unassigned")} />
          <SpendBreakdown title="By cloud" rows={aggregateSpend((resource) => resource.skypilot.cloud ?? "Unknown cloud")} />
        </SimpleGrid>
      </Box>

      <Box>
        <Title order={2} mb="md">Recovery and preemption history</Title>
        <Card withBorder padding={0}>
          <Table.ScrollContainer minWidth={800}>
            <Table striped highlightOnHover>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>Resource</Table.Th>
                  <Table.Th>Started</Table.Th>
                  <Table.Th>Total recoveries</Table.Th>
                  <Table.Th>Preemption / infrastructure</Table.Th>
                  <Table.Th>Application errors</Table.Th>
                  <Table.Th>Links</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {recoveries.map((resource) => (
                  <Table.Tr key={`${resource.kind}-${resource.skypilot.job_id ?? resource.name}`}>
                    <Table.Td><ResourceNameLink resource={resource} /></Table.Td>
                    <Table.Td>{formatTimestamp(resource.timing.started_at)}</Table.Td>
                    <Table.Td>{resource.retries.total_recoveries ?? "—"}</Table.Td>
                    <Table.Td>{resource.retries.preemption_or_infrastructure ?? "—"}</Table.Td>
                    <Table.Td>{resource.retries.application_error ?? "—"}</Table.Td>
                    <Table.Td><ResourceLinks resource={resource} /></Table.Td>
                  </Table.Tr>
                ))}
                {!recoveries.length && (
                  <Table.Tr><Table.Td colSpan={6}><Text c="dimmed">No recoveries in the retained inventory.</Text></Table.Td></Table.Tr>
                )}
              </Table.Tbody>
            </Table>
          </Table.ScrollContainer>
        </Card>
      </Box>
    </Stack>
  );
}

function QueryStatusModal({
  opened,
  onClose,
  queryStatus,
}: {
  opened: boolean;
  onClose: () => void;
  queryStatus: QueryStatusReport | null;
}) {
  const [rawQuery, setRawQuery] = useState<{ title: string; content: string } | null>(null);

  const openRawQuery = async (key: string, label: string) => {
    setRawQuery({ title: label, content: "Loading raw output…" });
    try {
      const response = await fetch(`/api/status/${key}/raw`, { cache: "no-store" });
      const output: unknown = await response.json();
      if (!response.ok) throw new Error(`Raw query request failed (${response.status})`);
      setRawQuery({ title: label, content: JSON.stringify(output, null, 2) });
    } catch (requestError: unknown) {
      setRawQuery({ title: label, content: String(requestError) });
    }
  };

  return (
    <>
      <Modal opened={opened} onClose={onClose} title="Overwatch query status" size="xl">
        {!queryStatus ? (
          <Center h={160}><Loader /></Center>
        ) : (
          <Stack gap="md">
            <Group gap="lg">
              <Text size="sm">Last attempt: {formatTimestamp(queryStatus.refresh.last_attempt_at)}</Text>
              <Text size="sm">Last success: {formatTimestamp(queryStatus.refresh.last_success_at)}</Text>
              <Text size="sm">
                Duration: {queryStatus.refresh.last_duration_seconds == null ? "—" : `${queryStatus.refresh.last_duration_seconds.toFixed(1)}s`}
              </Text>
              {queryStatus.refresh.in_progress && <Badge color="blue">Refreshing</Badge>}
            </Group>
            {queryStatus.refresh.last_error && (
              <Alert color="red" title="Last refresh failed" icon={<IconAlertTriangle size={18} />}>
                {queryStatus.refresh.last_error}
              </Alert>
            )}
            <Table.ScrollContainer minWidth={700}>
              <Table striped highlightOnHover>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>Query</Table.Th>
                    <Table.Th>Status</Table.Th>
                    <Table.Th>Duration</Table.Th>
                    <Table.Th>Summary</Table.Th>
                    <Table.Th>Updated</Table.Th>
                    <Table.Th>Output</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {queryStatus.queries.map((query) => (
                    <Table.Tr key={query.key}>
                      <Table.Td>{query.label}</Table.Td>
                      <Table.Td><Badge color={queryDiagnosticColor(query.status)}>{query.status}</Badge></Table.Td>
                      <Table.Td>{query.duration_seconds == null ? "—" : `${query.duration_seconds.toFixed(2)}s`}</Table.Td>
                      <Table.Td>{query.error ?? query.summary}</Table.Td>
                      <Table.Td>{formatTimestamp(query.updated_at)}</Table.Td>
                      <Table.Td>
                        <Anchor component="button" type="button" onClick={() => void openRawQuery(query.key, query.label)}>
                          <Group gap={4} wrap="nowrap"><IconCode size={15} /> Raw</Group>
                        </Anchor>
                      </Table.Td>
                    </Table.Tr>
                  ))}
                </Table.Tbody>
              </Table>
            </Table.ScrollContainer>
          </Stack>
        )}
      </Modal>
      <Drawer
        opened={rawQuery != null}
        onClose={() => setRawQuery(null)}
        title={`Raw query output · ${rawQuery?.title ?? ""}`}
        position="right"
        size="xl"
        zIndex={300}
      >
        <ScrollArea h="calc(100vh - 100px)" type="auto">
          <Code block>{rawQuery?.content}</Code>
        </ScrollArea>
      </Drawer>
    </>
  );
}

export function App() {
  const [report, setReport] = useState<Report | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [queryStatus, setQueryStatus] = useState<QueryStatusReport | null>(null);
  const [statusOpened, setStatusOpened] = useState(false);
  const serviceState = useRef<{
    startup_id: string;
    report_version: string | null;
  } | null>(null);
  const runMatch = window.location.pathname.match(/^\/runs\/(\d+)\/?$/);
  const runJobId = runMatch ? Number(runMatch[1]) : null;
  const page: Page =
    window.location.pathname === "/billing"
      ? "billing"
      : window.location.pathname === "/cost-waste"
        ? "cost-waste"
        : runJobId != null
          ? "run"
        : "resources";

  useEffect(() => {
    let stopped = false;
    let reportRetry: number | undefined;

    // Poll while the initial cloud inventory loads or a supervised restart is settling.
    const loadReport = async () => {
      try {
        const response = await fetch("/api/report", { cache: "no-store" });
        if (response.status === 202) {
          const pending = (await response.json()) as { error: string | null };
          if (!stopped) {
            setError(pending.error);
            reportRetry = window.setTimeout(loadReport, 750);
          }
          return;
        }
        if (!response.ok) throw new Error(`Report request failed (${response.status})`);
        const nextReport = (await response.json()) as Report;
        if (!stopped) {
          setReport(nextReport);
          setError(null);
        }
      } catch (requestError: unknown) {
        if (!stopped) {
          setError(String(requestError));
          reportRetry = window.setTimeout(loadReport, 1000);
        }
      }
    };
    void loadReport();

    const loadQueryStatus = async () => {
      try {
        const response = await fetch("/api/status", { cache: "no-store" });
        if (!response.ok) throw new Error(`Status request failed (${response.status})`);
        const nextQueryStatus = (await response.json()) as QueryStatusReport;
        if (!stopped) setQueryStatus(nextQueryStatus);
      } catch {
        // The source reloader briefly takes the local service offline.
      }
    };
    void loadQueryStatus();

    const interval = window.setInterval(async () => {
      try {
        const [healthResponse, statusResponse] = await Promise.all([
          fetch("/api/health", { cache: "no-store" }),
          fetch("/api/status", { cache: "no-store" }),
        ]);
        const nextState = (await healthResponse.json()) as {
          startup_id: string;
          report_version: string | null;
        };
        if (statusResponse.ok && !stopped) {
          setQueryStatus((await statusResponse.json()) as QueryStatusReport);
        }
        const previousState = serviceState.current;
        serviceState.current = nextState;
        if (previousState && nextState.startup_id !== previousState.startup_id) {
          window.location.reload();
        } else if (
          previousState &&
          nextState.report_version !== previousState.report_version
        ) {
          await loadReport();
        }
      } catch {
        // The source reloader briefly takes the local service offline.
      }
    }, 2000);
    return () => {
      stopped = true;
      window.clearInterval(interval);
      window.clearTimeout(reportRetry);
    };
  }, []);

  const refreshReport = async () => {
    try {
      const response = await fetch("/api/refresh", { method: "POST" });
      if (!response.ok) throw new Error(`Refresh request failed (${response.status})`);
      setQueryStatus((current) =>
        current
          ? { ...current, refresh: { ...current.refresh, in_progress: true } }
          : current,
      );
      setError(null);
    } catch (requestError: unknown) {
      setError(String(requestError));
    }
  };

  const failedQueries = queryStatus?.queries.filter((query) => query.status === "error") ?? [];

  return (
    <AppShell header={{ height: 60 }} padding="lg">
      <Navigation
        page={page}
        queryStatus={queryStatus}
        onOpenStatus={() => setStatusOpened(true)}
        onRefresh={() => void refreshReport()}
      />
      <AppShell.Main className="hud-main">
        {error && <Alert color="red">{error}</Alert>}
        {(queryStatus?.refresh.last_error || failedQueries.length > 0) && (
          <Alert color="red" title="Overwatch query failure" icon={<IconAlertTriangle size={18} />} mb="md">
            {queryStatus?.refresh.last_error ?? `${failedQueries.map((query) => query.label).join(", ")} failed.`} The last successful report remains visible; open Status for details and raw outputs.
          </Alert>
        )}
        {!report && (
          <Center h={300}>
            <Stack align="center" gap="sm">
              <Loader />
              <Text c="dimmed">Loading cloud inventory…</Text>
            </Stack>
          </Center>
        )}
        {report && page === "resources" && <ResourcesPage report={report} />}
        {report && page === "billing" && <BillingPage report={report} />}
        {report && page === "cost-waste" && <CostWastePage report={report} />}
        {report && page === "run" && runJobId != null && <RunPage report={report} jobId={runJobId} />}
      </AppShell.Main>
      <QueryStatusModal opened={statusOpened} onClose={() => setStatusOpened(false)} queryStatus={queryStatus} />
    </AppShell>
  );
}
