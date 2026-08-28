import { z } from "zod";

const durationMs = z.number().finite().nonnegative();
const byteCount = z.number().int().nonnegative().safe();
const frameCount = z.number().int().nonnegative().safe();
const monotonicAtMs = z.number().finite().nonnegative();
const byteSizeBucket = z.enum(["0", "1-8", "9-64", "65-1024", "1025-65536", "65537+"]);

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

const HostControlQueueEventSchema = z.strictObject({
  ...base,
  name: z.literal("host.control.queue"),
  messageClass: z.enum(["host-ready", "recovery", "input", "unknown"]),
  outcome: z.enum(["handled", "rejected", "failed", "capacity"]),
  admissionMs: durationMs,
  queueWaitMs: durationMs,
  handlingMs: durationMs,
  queuedBytesBucket: byteSizeBucket,
  queuedCount: frameCount,
});

const hostInputApplyBase = {
  ...base,
  name: z.literal("host.input.apply"),
  outcome: z.enum(["written", "duplicate", "rejected", "uncertain"]),
  effectStage: z.enum(["not-attempted", "completed", "threw"]),
  ackSendOutcome: z.enum(["send-returned", "not-attempted", "uncertain"]),
  controlAdmissionMs: durationMs,
  controlQueueWaitMs: durationMs,
  controlQueueDepth: frameCount,
  actorQueueWaitMs: durationMs,
  actorProcessingMs: durationMs,
  hostIngressToAckDecisionMs: durationMs,
} as const;

const HostInputWriteEventSchema = z.strictObject({
  ...hostInputApplyBase,
  inputKind: z.enum(["key", "text", "paste", "focus", "mouse"]),
  encodeKind: z.enum(["ghostty", "utf8", "none"]),
  inputEncodeMs: durationMs,
  ptyWriteAttempted: z.boolean(),
  ptyWriteMs: durationMs,
  ptyBytesBucket: byteSizeBucket,
});

const HostInputResizeEventSchema = z.strictObject({
  ...hostInputApplyBase,
  inputKind: z.literal("resize"),
  authorityResizeMs: durationMs,
  ptyResizeAttempted: z.boolean(),
  ptyResizeMs: durationMs,
  effectWriteAttempted: z.boolean(),
  effectWriteMs: durationMs,
  effectBytesBucket: byteSizeBucket,
});

const HostInputApplyEventSchema = z.union([HostInputWriteEventSchema, HostInputResizeEventSchema]);

const HostRelayRttReadyEventSchema = z.strictObject({
  ...base,
  name: z.literal("host.relay.rtt"),
  channel: z.enum(["control", "data"]),
  outcome: z.literal("ok"),
  durationMs,
  outstandingPings: frameCount,
});

const HostRelayRttTimeoutEventSchema = z.strictObject({
  ...base,
  name: z.literal("host.relay.rtt"),
  channel: z.enum(["control", "data"]),
  outcome: z.literal("timeout"),
  silenceMs: durationMs,
  outstandingPings: frameCount,
});

const HostRelayRttEventSchema = z.union([
  HostRelayRttReadyEventSchema,
  HostRelayRttTimeoutEventSchema,
]);

export const TerminalTelemetryEventSchema = z.union([
  HostSnapshotCaptureReadyEventSchema,
  HostSnapshotCaptureFailedEventSchema,
  HostSnapshotPublishFreshEventSchema,
  HostSnapshotPublishPendingEventSchema,
  HostJournalRangeExactEventSchema,
  HostJournalRangeGapEventSchema,
  HostControlQueueEventSchema,
  HostInputApplyEventSchema,
  HostRelayRttEventSchema,
]);

export type TerminalTelemetryEvent = z.output<typeof TerminalTelemetryEventSchema>;
export type TelemetrySink = (event: TerminalTelemetryEvent) => void;
export type MonotonicClock = () => number;

export interface BufferedTelemetrySink {
  readonly droppedEvents: number;
  readonly pendingEvents: number;
  readonly sink: TelemetrySink;
  flush(): Promise<void>;
  pause(): void;
  resume(): void;
}

export interface BufferedTelemetrySinkOptions {
  maxPendingEvents?: number;
  schedule?: (task: () => void) => void;
}

export const noopTelemetrySink: TelemetrySink = () => undefined;

export type TelemetryByteSizeBucket = z.output<typeof byteSizeBucket>;

export function telemetryByteSizeBucket(bytes: number): TelemetryByteSizeBucket {
  if (!Number.isSafeInteger(bytes) || bytes < 0) throw new RangeError("bytes must be non-negative");
  if (bytes === 0) return "0";
  if (bytes <= 8) return "1-8";
  if (bytes <= 64) return "9-64";
  if (bytes <= 1_024) return "65-1024";
  if (bytes <= 65_536) return "1025-65536";
  return "65537+";
}

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

export function createBufferedTelemetrySink(
  target: TelemetrySink,
  options: BufferedTelemetrySinkOptions = {},
): BufferedTelemetrySink {
  const maxPendingEvents = options.maxPendingEvents ?? 256;
  if (!Number.isSafeInteger(maxPendingEvents) || maxPendingEvents <= 0) {
    throw new RangeError("maxPendingEvents must be a positive safe integer");
  }
  const schedule = options.schedule ?? queueMicrotask;
  const queue: TerminalTelemetryEvent[] = [];
  const flushWaiters: Array<() => void> = [];
  let droppedEvents = 0;
  let paused = false;
  let scheduled = false;

  const settleFlush = () => {
    if (scheduled || queue.length > 0) return;
    for (const resolve of flushWaiters.splice(0)) resolve();
  };
  const dropQueue = () => {
    droppedEvents += queue.length;
    queue.length = 0;
    settleFlush();
  };
  const scheduleNext = () => {
    if (paused || scheduled || queue.length === 0) return;
    scheduled = true;
    try {
      schedule(() => {
        scheduled = false;
        if (paused) return;
        const event = queue.shift();
        if (event !== undefined) {
          try {
            const result = (target as (sample: TerminalTelemetryEvent) => unknown)(event);
            if (isPromiseLike(result)) void Promise.resolve(result).catch(() => undefined);
          } catch {
            // A collector failure only drops this diagnostic event.
          }
        }
        if (queue.length > 0) scheduleNext();
        else settleFlush();
      });
    } catch {
      scheduled = false;
      dropQueue();
    }
  };
  const sink: TelemetrySink = (event) => {
    if (queue.length >= maxPendingEvents) {
      droppedEvents += 1;
      return;
    }
    queue.push({ ...event });
    scheduleNext();
  };

  return {
    get droppedEvents() {
      return droppedEvents;
    },
    get pendingEvents() {
      return queue.length;
    },
    sink,
    flush() {
      if (!scheduled && queue.length === 0) return Promise.resolve();
      return new Promise((resolve) => flushWaiters.push(resolve));
    },
    pause() {
      paused = true;
    },
    resume() {
      if (!paused) return;
      paused = false;
      scheduleNext();
      settleFlush();
    },
  };
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    ((typeof value === "object" && value !== null) || typeof value === "function") &&
    "then" in value
  );
}
