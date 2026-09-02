import { BarChart } from "@mantine/charts";
import { Alert, Box, Card, Code, SimpleGrid, Stack, Text, Title } from "@mantine/core";
import { IconFileText } from "@tabler/icons-react";

import { formatMoney, StatCard, Warnings } from "./presentation";
import type { Report } from "./types";

export function BillingPage({ report }: { report: Report }) {
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
