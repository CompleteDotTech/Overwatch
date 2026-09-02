import { Alert, Card, Stack, Text } from "@mantine/core";
import { IconAlertTriangle } from "@tabler/icons-react";

import type { QueryStatusReport } from "./types";

export function formatMoney(value: number | null | undefined): string {
  return value == null
    ? "—"
    : new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: "USD",
      }).format(value);
}

export function formatTimestamp(value: string | null): string {
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

export function formatDuration(seconds: number | null): string {
  if (seconds == null) return "—";
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return days ? `${days}d ${hours}h ${minutes}m` : `${hours}h ${minutes}m`;
}

export function statusColor(status: string | null): string {
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

export function queryDiagnosticColor(status: QueryStatusReport["queries"][number]["status"]): string {
  if (status === "ok") return "green";
  if (status === "warning") return "yellow";
  if (status === "error") return "red";
  if (status === "pending") return "blue";
  return "gray";
}

export function Warnings({ warnings }: { warnings: string[] }) {
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

export function StatCard({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail?: string;
}) {
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
