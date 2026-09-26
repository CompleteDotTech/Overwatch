import { Alert, Anchor, Badge, Box, Card, Group, Stack, Table, Text, Title } from "@mantine/core";
import { curve, executionSummary, formatBytes, percentile, phaseLabel, safeWebURL, verificationLabel } from "./modelRuns";
import type { ModelRun } from "./modelRuns";

function MetricCurve({ run, metric }: { run: ModelRun; metric: string }) {
  const points = curve(run.history, metric);
  return <Box><Text size="xs">{metric}: {run.metrics[metric]?.toPrecision(4) ?? "unknown"}</Text>
    {points && <svg role="img" aria-label={`${metric} by optimizer step for ${run.run_id}`} viewBox="0 0 260 80" width="260" height="80">
      <polyline fill="none" stroke="currentColor" strokeWidth="2" points={points} />
    </svg>}</Box>;
}
function ArtifactLink({ uri, label }: { uri: string; label: string }) {
  const web = safeWebURL(uri);
  return web ? <Anchor href={web} target="_blank" rel="noreferrer">{label}</Anchor>
    : <Text size="xs" style={{ overflowWrap: "anywhere" }}>{label}: {uri}</Text>;
}
export function ModelRuns({ runs, warnings = [] }: { runs: ModelRun[]; warnings?: Array<Record<string, unknown>> }) {
  return <Stack gap="md" mt="xl" data-testid="model-runs">
    <Title order={2}>Decision model experiments</Title>
    {warnings.length > 0 && <Text role="status">{warnings.length} telemetry warning(s): {warnings.map(w => String(w.code ?? "unknown")).join(", ")}</Text>}
    {!runs.length && <Text c="dimmed">No registered Kev-Laya runs. Resource inventory above is unchanged.</Text>}
    {runs.map(run => {
      const ext = run.extensions, execution = ext?.execution, resource = ext?.resources;
      return <Card withBorder key={`${run.experiment_id}/${run.run_id}`} data-run-id={run.run_id}>
        <Group justify="space-between"><Title order={3}>{run.experiment_id} / {run.run_id}</Title>
          <Group><Badge>{verificationLabel(run)}</Badge><Badge variant="outline">{run.freshness}</Badge><Badge variant="outline">{run.provenance.evidence_class}</Badge></Group>
        </Group>
        {run.verification && run.verification !== "verified" && <Alert title="Attempt identity requires verification" role="status" mt="sm">
          {run.verification === "unverified" ? "No verified attempt is available for this observation." : "The values below describe the last verified attempt, not the newer unverified observation."}
          {(run.unverified_observations ?? []).map((o, index) => <Text size="sm" key={`${o.source_id}/${o.attempt_id}/${o.sequence}/${index}`}>
            Observed attempt {o.attempt_index} ({o.attempt_id}) · {o.phase} · sequence {o.sequence} · source {o.source_id} · {o.reason} · heartbeat {o.heartbeat_at}
          </Text>)}
        </Alert>}
        <Text size="sm">Attempt {run.attempt_index} · {run.attempt_id} · sequence {run.sequence}</Text>
        <Text size="sm">{phaseLabel(run)}: {run.phase} · Sky: {run.provider_status.skypilot ?? "unknown"} · W&amp;B: {run.provider_status.wandb ?? "unknown"}</Text>
        <Text size="sm">Association: {run.association.status}{run.association.scheduler ? ` · ${run.association.scheduler.namespace}/${run.association.scheduler.job_id}` : " (no scheduler resource)"}</Text>
        <Text size="xs">Heartbeat {run.heartbeat_at} · {Math.round(run.heartbeat_age_seconds)} seconds old · monitoring failures {run.monitoring_export_failures}</Text>
        <Text>{run.progress.optimizer_steps.toLocaleString()} / {run.progress.totals.optimizer_steps?.toLocaleString() ?? "unknown"} optimizer steps</Text>
        <Text size="sm">{run.progress.microbatches.toLocaleString()} accumulation microbatches · {run.progress.examples.toLocaleString()} examples · {run.progress.forward_tokens.toLocaleString()} useful forward tokens</Text>
        <Text size="sm">Elapsed {run.progress.elapsed_seconds?.toFixed(1) ?? "unknown"} s · ETA {run.progress.eta_seconds?.toFixed(1) ?? "unknown"} s · {run.progress.tokens_per_second?.toFixed(1) ?? "unknown"} tokens/s</Text>
        <Text size="sm">{executionSummary(ext)}</Text>
        {execution && <Text size="sm">Padding overhead {execution.padding_tokens.toLocaleString()} tokens · total forward compute {execution.compute_tokens.toLocaleString()} tokens (excludes backward recomputation) · batch sizes {Object.entries(execution.batch_size_histogram).map(([size, count]) => `${size} × ${count}`).join(", ")}</Text>}
        <Group align="start"><MetricCurve run={run} metric="loss/ce" /><MetricCurve run={run} metric="reward/proper" /><MetricCurve run={run} metric="validation/ece" /></Group>
        {ext?.calibration && <Box mt="sm"><Text size="sm">Calibration: {ext.calibration.status} · {ext.calibration.method ?? "unknown method"}</Text>
          {Object.entries(ext.calibration.per_type).map(([kind, fit]) => fit && <Text size="xs" key={kind}>
            {kind}: T={fit.temperature.toPrecision(4)} · {fit.count} calibration questions · calibration NLL {fit.before_nll?.toPrecision(4) ?? "unknown"} → {fit.after_nll?.toPrecision(4) ?? "unknown"}
          </Text>)}
          <Text size="xs" style={{ overflowWrap: "anywhere" }}>Calibration split SHA-256: {ext.calibration.split_sha256 ?? "unknown"}. These are fit diagnostics, not a general calibration guarantee.</Text>
        </Box>}
        <Text size="sm">Backbone {run.provenance.backbone} · {run.provenance.precision} · {run.provenance.hardware}</Text>
        <Text size="sm">RSS {formatBytes(resource?.rss_bytes)} · CUDA allocated {formatBytes(resource?.gpu_allocated_bytes)} · CUDA peak allocated {formatBytes(resource?.gpu_peak_allocated_bytes)}</Text>
        <Text size="xs">Resource sample {resource?.sampled_at ?? "unknown"} · minimum sampling interval {resource?.interval_seconds ?? "unknown"} s; samples are activity-driven. Cache-admission estimates are not total memory guarantees.</Text>
        <Text size="xs">Branch budget {run.provenance.context_limits.branch}; aggregate {run.provenance.context_limits.aggregate}. Budget is not a benchmark result.</Text>
        <details><summary>Source, preprocessing, and split provenance</summary>
          <Text size="xs" style={{ overflowWrap: "anywhere" }}>Tokenizer {run.provenance.tokenizer} · serialization {ext?.serialization.serialization ?? "historical / unknown"} · literals {ext?.serialization.literal_encoding ?? "unknown"}</Text>
          <Text size="xs">Package {ext?.source.package_version ?? "unknown"} · Git {ext?.source.git_status ?? "unknown"} · dirty state {ext?.source.git_dirty == null ? "unknown" : ext.source.git_dirty ? "dirty" : "clean"}</Text>
          <Text size="xs" style={{ overflowWrap: "anywhere" }}>Source tree SHA-256 {ext?.source.source_tree_sha256 ?? "unknown"} · archive SHA-256 {ext?.source.archive_sha256 ?? "unknown"}</Text>
          <Text size="xs" style={{ overflowWrap: "anywhere" }}>Config {run.provenance.config_sha256 ?? "unknown"} · data manifest {run.provenance.data_sha256 ?? "unknown"} · seed {run.provenance.seed ?? "unknown"}</Text>
          {Object.entries(run.provenance.split_hashes ?? {}).map(([name, hash]) => <Text key={name} size="xs" style={{ overflowWrap: "anywhere" }}>{name}: {hash}</Text>)}
          <Group>{run.git_url && <ArtifactLink uri={run.git_url} label="Source commit" />}{run.wandb_url && <ArtifactLink uri={run.wandb_url} label="W&B run" />}</Group>
        </details>
        <details><summary>Attempt lineage and collection freshness</summary>
          <Text size="xs">Receipt storage durability: {ext?.lineage_durable == null ? "unknown" : ext.lineage_durable ? "confirmed by writer" : "write failed; recovery may be uncertain"} · at most 64 consecutive receipts retained</Text>
          {(ext?.attempt_lineage ?? []).map(a => <Text size="xs" key={a.attempt_id} style={{ overflowWrap: "anywhere" }}>Attempt {a.attempt_index} · {a.attempt_id} · parent {a.parent_attempt_id ?? "none"} · receipt {a.sha256}</Text>)}
          {(run.collection_sources ?? []).map(s => <Text size="xs" key={s.source_id}>Source {s.source_id}: {s.status} · current refresh {s.refreshed_at ?? "not refreshed"} · last successful refresh {s.last_successful_refresh_at ?? "unknown"} · {s.payload_bytes} payload bytes / {s.requests} request slots</Text>)}
        </details>
        <Text size="sm">Resume: {run.resume.capable ? "optimization-boundary checkpoint available" : "not established"} · parent attempt: {run.parent_attempt_id ?? "none"}</Text>
        <Text size="sm">Recoveries: {run.recoveries.total ?? "unknown"}; infrastructure {run.recoveries.infrastructure ?? "unknown"}; application {run.recoveries.application ?? "unknown"}</Text>
        <Text size="sm">Measured cost: {run.cost.measured_usd == null ? "unknown" : `$${run.cost.measured_usd.toFixed(4)}`} · estimate: {run.cost.estimated_usd == null ? "unknown" : `$${run.cost.estimated_usd.toFixed(4)}`} · basis {run.cost.basis ?? "unknown"}</Text>
        <Text size="sm">Serving: {run.serving.requests} requests / {run.serving.errors} errors · recent latency (last 128): p50 {percentile(run.serving.latency_ms, .5)?.toFixed(1) ?? "unknown"} ms · p95 {percentile(run.serving.latency_ms, .95)?.toFixed(1) ?? "unknown"} ms · {run.serving.prefix_reuses} prefix reuses</Text>
        <Table><Table.Thead><Table.Tr><Table.Th>Artifact</Table.Th><Table.Th>Checksum / lineage</Table.Th></Table.Tr></Table.Thead>
          <Table.Tbody>{run.artifacts.map(a => <Table.Tr key={`${a.kind}/${a.sha256}`}><Table.Td><ArtifactLink uri={a.uri} label={a.kind} /></Table.Td>
            <Table.Td><Text size="xs" ff="monospace" style={{ overflowWrap: "anywhere" }}>{a.sha256}</Text><Text size="xs">Parent {a.parent_sha256 ?? "none"} · {a.size_bytes.toLocaleString()} bytes</Text></Table.Td></Table.Tr>)}</Table.Tbody></Table>
      </Card>;
    })}
  </Stack>;
}
