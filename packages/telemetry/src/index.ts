import { z } from "zod";

const durationMs = z.number().finite().nonnegative();
const byteCount = z.number().int().nonnegative().safe();
const frameCount = z.number().int().nonnegative().safe();
const monotonicAtMs = z.number().finite().nonnegative();

const base = {
  schemaVersion: z.literal(1),
  monotonicAtMs,
} as const;

const HostSnapshotCaptureReadyEventSchema = z.strictObject({
  ...base,
  name: z.literal("host.snapshot.capture"),
  outcome: z.literal("ready"),
  queueWaitMs: durationMs,
  actorPauseMs: durationMs,
  authorityEncodeExportMs: durationMs,
  ownershipCopyMs: durationMs,
  snapshotBytes: byteCount,
});

const HostSnapshotCaptureFailedEventSchema = z.strictObject({
  ...base,
  name: z.literal("host.snapshot.capture"),
  outcome: z.enum(["timeout", "cancelled", "failed"]),
  totalDurationMs: durationMs,
});

const HostSnapshotPublishFreshEventSchema = z.strictObject({
  ...base,
  name: z.literal("host.snapshot.publish-total"),
  source: z.literal("fresh"),
  outcome: z.enum(["ready", "retry", "unavailable", "timeout", "cancelled", "failed"]),
  totalDurationMs: durationMs,
  snapshotBytes: byteCount,
});

const HostSnapshotPublishPendingEventSchema = z.strictObject({
  ...base,
  name: z.literal("host.snapshot.publish-total"),
  source: z.literal("pending"),
  outcome: z.enum(["ready", "retry", "unavailable", "timeout", "cancelled", "failed"]),
  totalDurationMs: durationMs,
});

const JournalGapReasonSchema = z.enum([
  "epoch-mismatch",
  "base-evicted",
  "reversed",
  "head-ahead",
  "base-cursor-mismatch",
  "head-cursor-mismatch",
]);

const HostJournalRangeExactEventSchema = z.strictObject({
  ...base,
  name: z.literal("host.journal.range"),
  mode: z.enum(["warm", "snapshot"]),
  status: z.literal("exact"),
  encodedBytes: byteCount,
  deliveryCreditBytes: byteCount,
  frames: frameCount,
  oldestMutationAgeMs: durationMs,
});

const HostJournalRangeGapEventSchema = z.strictObject({
  ...base,
  name: z.literal("host.journal.range"),
  mode: z.enum(["warm", "snapshot"]),
  status: z.literal("gap"),
  reason: JournalGapReasonSchema,
});

export const TerminalTelemetryEventSchema = z.union([
  HostSnapshotCaptureReadyEventSchema,
  HostSnapshotCaptureFailedEventSchema,
  HostSnapshotPublishFreshEventSchema,
  HostSnapshotPublishPendingEventSchema,
  HostJournalRangeExactEventSchema,
  HostJournalRangeGapEventSchema,
]);

export type TerminalTelemetryEvent = z.output<typeof TerminalTelemetryEventSchema>;
export type TelemetrySink = (event: TerminalTelemetryEvent) => void;
export type MonotonicClock = () => number;

export const noopTelemetrySink: TelemetrySink = () => undefined;

export function elapsedMs(startedAt: number, finishedAt: number): number {
  return Math.max(0, finishedAt - startedAt);
}

export function emitTelemetry(
  sink: TelemetrySink | undefined,
  event: TerminalTelemetryEvent,
): void {
  if (sink === undefined) return;
  try {
    sink(TerminalTelemetryEventSchema.parse(event));
  } catch {
    // Telemetry is observational and must never alter terminal authority or recovery.
  }
}
