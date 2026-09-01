import { BarChart } from "@mantine/charts";
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
  Progress,
  ScrollArea,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  ThemeIcon,
  Title,
  Tooltip,
  UnstyledButton,
  useMantineColorScheme,
} from "@mantine/core";
import {
  IconAlertTriangle,
  IconChartLine,
  IconCheck,
  IconCurrencyDollar,
  IconDatabase,
  IconFileText,
  IconServer,
  IconTerminal2,
} from "@tabler/icons-react";
import { useEffect, useMemo, useRef, useState } from "react";

import type { ConfigDifference, Report, Resource } from "./types";

const ACTIVE_STATUSES = new Set([
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

function formatMoney(value: number | null | undefined): string {
  return value == null
    ? "—"
    : new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
      }).format(value);
}

function formatTimestamp(value: string | null): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZone: "America/Los_Angeles",
    timeZoneName: "short",
  }).format(new Date(value));
}

function formatDuration(seconds: number | null): string {
  if (seconds == null) return "—";
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return days ? `${days}d ${hours}h ${minutes}m` : `${hours}h ${minutes}m`;
}

function statusColor(status: string | null): string {
  if (!status) return "gray";
  const normalizedStatus = status.toUpperCase();
  if (["RUNNING", "UP"].includes(normalizedStatus)) return "blue";
  if (["SUCCEEDED", "FINISHED"].includes(normalizedStatus)) return "green";
  if (normalizedStatus.startsWith("FAILED") || ["CRASHED", "CANCELLED"].includes(normalizedStatus)) {
    return "red";
  }
  if (["RECOVERING", "AUTOSTOPPING", "STARTING", "INIT", "PENDING"].includes(normalizedStatus)) {
    return "yellow";
  }
  return "gray";
}

function Warnings({ warnings }: { warnings: string[] }) {
  if (!warnings.length) return null;
  return (
    <Stack gap="xs">
      {warnings.map((warning) => (
        <Alert key={warning} color="yellow" icon={<IconAlertTriangle size={18} />}>
          {warning}
        </Alert>
      ))}
    </Stack>
  );
}

type Page = "resources" | "billing" | "cost-waste";

function Navigation({ page }: { page: Page }) {
  const { colorScheme, setColorScheme } = useMantineColorScheme();
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
          <Badge color="green" variant="light" size="xs">
            Live
          </Badge>
        </Group>
        <Group gap="xs" wrap="nowrap">
          <Button
            component="a"
            href="/"
            variant={page === "resources" ? "light" : "subtle"}
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

function StatCard({ label, value, detail }: { label: string; value: string; detail?: string }) {
  return (
    <Card withBorder className="hud-stat">
      <Text c="dimmed" size="xs" tt="uppercase" fw={700}>
        {label}
      </Text>
      <Text fw={800} mt={4} className="hud-stat-value">
        {value}
      </Text>
      {detail && (
        <Text c="dimmed" size="xs" mt={2}>
          {detail}
        </Text>
      )}
    </Card>
  );
}

function BillingPage({ report }: { report: Report }) {
  const billing = report.billing;
  const dateFormatter = new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric" });
  const categoryData = billing.category_daily.map((day) => ({
    date: dateFormatter.format(new Date(`${day.date}T00:00:00Z`)),
    aws_compute: day.aws_compute ?? 0,
    aws_storage: day.aws_storage ?? 0,
    aws_everything_else: day.aws_everything_else ?? 0,
    gcp_compute: day.gcp_compute ?? 0,
    gcp_storage: day.gcp_storage ?? 0,
    gcp_everything_else: day.gcp_everything_else ?? 0,
  }));

  return (
    <Stack gap="lg">
      <Box>
        <Title order={1}>Cloud billing</Title>
        <Text c="dimmed">
          {billing.start_date} through {billing.end_date} · daily net spend in USD
        </Text>
      </Box>
      <Warnings warnings={billing.warnings} />
      {!billing.gcp_configured && (
        <Alert color="blue" icon={<IconFileText size={18} />}>
          Set <Code>GCP_BILLING_EXPORT_TABLE</Code> to load per-project GCP billing lines.
        </Alert>
      )}
      <SimpleGrid cols={{ base: 1, sm: 3 }}>
        <StatCard
          label="AWS · 30 days"
          value={formatMoney(billing.totals.aws)}
          detail={billing.totals.aws == null ? "Unavailable" : "Net unblended cost"}
        />
        <StatCard
          label="GCP · 30 days"
          value={formatMoney(billing.totals.gcp)}
          detail={billing.totals.gcp == null ? "Unavailable" : "Cost after credits"}
        />
        <StatCard
          label="Combined · 30 days"
          value={formatMoney(billing.totals.combined)}
          detail="Available providers"
        />
      </SimpleGrid>
      <Card withBorder padding="lg">
        <Box mb="lg">
          <Title order={3}>Daily spend</Title>
          <Text c="dimmed" size="sm">
            Stacked by cloud and major category · recent days may be provisional
          </Text>
        </Box>
        <BarChart
          h={420}
          data={categoryData}
          dataKey="date"
          type="stacked"
          series={[
            { name: "aws_compute", label: "AWS · compute", color: "orange.7" },
            { name: "aws_storage", label: "AWS · storage", color: "orange.4" },
            { name: "aws_everything_else", label: "AWS · other", color: "yellow.4" },
            { name: "gcp_compute", label: "GCP · compute", color: "blue.7" },
            { name: "gcp_storage", label: "GCP · storage", color: "blue.4" },
            { name: "gcp_everything_else", label: "GCP · other", color: "cyan.4" },
          ]}
          valueFormatter={(value) => formatMoney(value)}
          withLegend
          yAxisProps={{ width: 72 }}
        />
      </Card>
    </Stack>
  );
}

type SortKey = "name" | "status" | "progress" | "started" | "cost" | "cloud";

function resourceSortValue(resource: Resource, sortKey: SortKey): string | number {
  if (sortKey === "name") return `${resource.user}/${resource.project}/${resource.name}`;
  if (sortKey === "status") return resource.status.skypilot ?? "";
  if (sortKey === "progress") return resource.progress.progress_fraction ?? -1;
  if (sortKey === "started") return resource.timing.started_at ?? "";
  if (sortKey === "cost") return resource.cost.estimated_spend_usd ?? -1;
  return `${resource.skypilot.cloud}/${resource.skypilot.region}`;
}

function SortHeader({
  label,
  value,
  active,
  descending,
  onSort,
}: {
  label: string;
  value: SortKey;
  active: boolean;
  descending: boolean;
  onSort: (value: SortKey) => void;
}) {
  return (
    <Table.Th>
      <UnstyledButton fw={700} onClick={() => onSort(value)}>
        {label} {active ? (descending ? "↓" : "↑") : "↕"}
      </UnstyledButton>
    </Table.Th>
  );
}

function ResourceLinks({ resource, onLogs }: { resource: Resource; onLogs?: () => void }) {
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
    <Group gap={4} wrap="nowrap">
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
      {onLogs && resource.skypilot.job_id != null && resource.skypilot.cluster_name && (
        <Tooltip label="Live logs">
          <ActionIcon aria-label="Live logs" color="green" variant="subtle" onClick={onLogs}>
            <IconTerminal2 size={16} />
          </ActionIcon>
        </Tooltip>
      )}
    </Group>
  );
}

function ResourcesTable({ resources }: { resources: Resource[] }) {
  const [sortKey, setSortKey] = useState<SortKey>("started");
  const [descending, setDescending] = useState(true);
  const [logResource, setLogResource] = useState<Resource | null>(null);
  const [logHtml, setLogHtml] = useState("");

  const sortedResources = useMemo(() => {
    return [...resources].sort((left, right) => {
      const leftStatus = left.status.skypilot ?? "";
      const rightStatus = right.status.skypilot ?? "";
      const leftPriority =
        (left.kind === "managed_job" && ["RUNNING", "RECOVERING"].includes(leftStatus)) ||
        (left.kind === "cluster" && ["UP", "AUTOSTOPPING"].includes(leftStatus))
          ? 0
          : ACTIVE_STATUSES.has(leftStatus)
            ? 1
            : 2;
      const rightPriority =
        (right.kind === "managed_job" && ["RUNNING", "RECOVERING"].includes(rightStatus)) ||
        (right.kind === "cluster" && ["UP", "AUTOSTOPPING"].includes(rightStatus))
          ? 0
          : ACTIVE_STATUSES.has(rightStatus)
            ? 1
            : 2;
      if (leftPriority !== rightPriority) return leftPriority - rightPriority;

      const leftValue = resourceSortValue(left, sortKey);
      const rightValue = resourceSortValue(right, sortKey);
      const comparison =
        typeof leftValue === "number" && typeof rightValue === "number"
          ? leftValue - rightValue
          : String(leftValue).localeCompare(String(rightValue), undefined, { numeric: true });
      return descending ? -comparison : comparison;
    });
  }, [descending, resources, sortKey]);

  useEffect(() => {
    if (!logResource?.skypilot.job_id) return;
    setLogHtml("Connecting to CloudWatch…\n");
    const events = new EventSource(`/api/logs/${logResource.skypilot.job_id}`);
    events.onmessage = (event) => {
      const item = JSON.parse(event.data) as { timestamp: number; html: string };
      const timestamp = new Date(item.timestamp).toLocaleTimeString();
      setLogHtml((current) =>
        `${current.startsWith("Connecting") ? "" : current}[${timestamp}] ${item.html}\n`,
      );
    };
    events.onerror = () =>
      setLogHtml((current) =>
        current.endsWith("Reconnecting…\n") ? current : `${current}\nReconnecting…\n`,
      );
    return () => events.close();
  }, [logResource]);

  const onSort = (value: SortKey) => {
    if (sortKey === value) setDescending((current) => !current);
    else {
      setSortKey(value);
      setDescending(false);
    }
  };

  return (
    <>
      <Card withBorder padding={0}>
        <Table.ScrollContainer minWidth={1320}>
          <Table striped highlightOnHover verticalSpacing="sm">
            <Table.Thead>
              <Table.Tr>
                <SortHeader label="User / project / run" value="name" active={sortKey === "name"} descending={descending} onSort={onSort} />
                <SortHeader label="Status" value="status" active={sortKey === "status"} descending={descending} onSort={onSort} />
                <SortHeader label="Progress" value="progress" active={sortKey === "progress"} descending={descending} onSort={onSort} />
                <Table.Th>Retries</Table.Th>
                <SortHeader label="Started" value="started" active={sortKey === "started"} descending={descending} onSort={onSort} />
                <Table.Th>Elapsed</Table.Th>
                <SortHeader label="Cost" value="cost" active={sortKey === "cost"} descending={descending} onSort={onSort} />
                <SortHeader label="Cloud / region" value="cloud" active={sortKey === "cloud"} descending={descending} onSort={onSort} />
                <Table.Th>Links</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {sortedResources.map((resource) => {
                const progress = Math.max(0, Math.min(100, (resource.progress.progress_fraction ?? 0) * 100));
                return (
                  <Table.Tr key={`${resource.kind}-${resource.skypilot.job_id ?? resource.name}`}>
                    <Table.Td miw={280}>
                      <Text size="xs" c="dimmed">{resource.user ?? "—"} / {resource.project ?? "—"}</Text>
                      <Text fw={600}>{resource.name}</Text>
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
                          <Progress value={progress} size="md" />
                        </Stack>
                      ) : <Text c="dimmed">—</Text>}
                      {resource.progress.tokens_per_second != null && <Text size="xs" c="dimmed" mt={4}>{resource.progress.tokens_per_second.toLocaleString()} tok/s</Text>}
                    </Table.Td>
                    <Table.Td>{resource.retries.preemption_or_infrastructure ?? "?"} | {resource.retries.application_error ?? "?"}</Table.Td>
                    <Table.Td miw={150}>{formatTimestamp(resource.timing.started_at)}</Table.Td>
                    <Table.Td>{formatDuration(resource.timing.elapsed_seconds)}</Table.Td>
                    <Table.Td miw={150}>
                      <Text>{formatMoney(resource.cost.estimated_spend_usd)} / {formatMoney(resource.cost.estimated_total_usd)}</Text>
                      <Text size="xs" c="dimmed">{formatMoney(resource.cost.hourly_usd)}/hr</Text>
                    </Table.Td>
                    <Table.Td>{resource.skypilot.cloud ?? "—"}<Text size="xs" c="dimmed">{resource.skypilot.region ?? "—"}</Text></Table.Td>
                    <Table.Td><ResourceLinks resource={resource} onLogs={() => setLogResource(resource)} /></Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      </Card>
      <Drawer
        opened={logResource != null}
        onClose={() => setLogResource(null)}
        title={`Live logs · ${logResource?.name ?? ""}`}
        position="bottom"
        size="100%"
      >
        <ScrollArea h="calc(100vh - 100px)" type="auto">
          <Code block bg="dark.9" c="green.2" p="md">
            <span dangerouslySetInnerHTML={{ __html: logHtml }} />
          </Code>
        </ScrollArea>
      </Drawer>
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
                  {project.runs.map((run) => <Table.Th key={run.wandb_id}>{run.experiment_name}</Table.Th>)}
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
        <Title order={2} mb="md">TrainConfig differences</Title>
        <ConfigDifferences projects={report.config_differences} />
      </Box>
    </Stack>
  );
}

interface SpendBreakdownRow {
  name: string;
  spend: number;
  hourly: number;
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
  const activeResources = report.resources.filter((resource) =>
    ACTIVE_STATUSES.has(resource.status.skypilot ?? ""),
  );
  const gpuPattern = /(gpu|tpu|b200|h200|h100|a100|v100|l40|l4|t4|a10)/i;
  const wasteCandidates = activeResources.flatMap((resource) => {
    const reasons: string[] = [];
    const hasObservedProgress =
      resource.progress.completed_batches != null || resource.progress.tokens_per_second != null;
    if (gpuPattern.test(resource.skypilot.resources ?? "") && !hasObservedProgress) {
      reasons.push("Idle GPU candidate");
    }
    if (resource.kind === "managed_job" && resource.wandb_id == null) {
      reasons.push("Orphan candidate");
    }
    if (
      resource.kind === "cluster" &&
      (resource.timing.elapsed_seconds ?? 0) >= 2 * 24 * 60 * 60
    ) {
      reasons.push("Zombie dev box candidate");
    }
    return reasons.length ? [{ resource, reasons }] : [];
  });
  const candidateHourly = wasteCandidates.reduce(
    (sum, candidate) => sum + (candidate.resource.cost.hourly_usd ?? 0),
    0,
  );
  const recoveries = report.resources
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

  const aggregateSpend = (labelFor: (resource: Resource) => string) => {
    const grouped = new Map<string, SpendBreakdownRow>();
    for (const resource of report.resources) {
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
  };

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
                      <Text fw={600}>{resource.name}</Text>
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
                    <Table.Td><Text fw={600}>{resource.name}</Text></Table.Td>
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

export function App() {
  const [report, setReport] = useState<Report | null>(null);
  const [error, setError] = useState<string | null>(null);
  const serviceState = useRef<{
    startup_id: string;
    report_version: string | null;
  } | null>(null);
  const page: Page =
    window.location.pathname === "/billing"
      ? "billing"
      : window.location.pathname === "/cost-waste"
        ? "cost-waste"
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

    const interval = window.setInterval(async () => {
      try {
        const response = await fetch("/api/health", { cache: "no-store" });
        const nextState = (await response.json()) as {
          startup_id: string;
          report_version: string | null;
        };
        if (
          serviceState.current &&
          (nextState.startup_id !== serviceState.current.startup_id ||
            (serviceState.current.report_version !== null &&
              nextState.report_version !== serviceState.current.report_version))
        ) {
          window.location.reload();
        }
        serviceState.current = nextState;
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

  return (
    <AppShell header={{ height: 60 }} padding="lg">
      <Navigation page={page} />
      <AppShell.Main className="hud-main">
        {error && <Alert color="red">{error}</Alert>}
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
      </AppShell.Main>
    </AppShell>
  );
}
