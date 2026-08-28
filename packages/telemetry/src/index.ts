import { z } from "zod";

const durationMs = z.number().finite().nonnegative();
const byteCount = z.number().int().nonnegative().safe();
const frameCount = z.number().int().nonnegative().safe();
const monotonicAtMs = z.number().finite().nonnegative();
const byteSizeBucket = z.enum(["0", "1-8", "9-64", "65-1024", "1025-65536", "65537+"]);
const sampleWeight = z.number().int().positive().safe();

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
  ackSendMs: durationMs,
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

const browserBase = {
  ...base,
  clockKind: z.literal("browser-performance"),
} as const;

const BrowserRelayRttSuccessEventSchema = z.strictObject({
  ...browserBase,
  name: z.literal("browser.relay.rtt"),
  channel: z.enum(["control", "data"]),
  outcome: z.literal("success"),
  durationMs,
  outstandingPings: frameCount,
});

const BrowserRelayRttTimeoutEventSchema = z.strictObject({
  ...browserBase,
  name: z.literal("browser.relay.rtt"),
  channel: z.enum(["control", "data"]),
  outcome: z.literal("timeout"),
  silenceMs: durationMs,
  outstandingPings: frameCount,
});

const BrowserRelayRttEventSchema = z.union([
  BrowserRelayRttSuccessEventSchema,
  BrowserRelayRttTimeoutEventSchema,
]);

const BrowserRecoveryAttachStartReadyEventSchema = z.strictObject({
  ...browserBase,
  name: z.literal("browser.recovery.attach-start"),
  outcome: z.literal("matching-start-received"),
  startingReplica: z.enum(["live", "empty"]),
  mode: z.enum(["warm", "snapshot"]),
  durationMs,
});

const BrowserRecoveryAttachStartTimeoutEventSchema = z.strictObject({
  ...browserBase,
  name: z.literal("browser.recovery.attach-start"),
  outcome: z.literal("timeout"),
  startingReplica: z.enum(["live", "empty"]),
  durationMs,
});

const BrowserRecoveryAttachStartCancelledEventSchema = z.strictObject({
  ...browserBase,
  name: z.literal("browser.recovery.attach-start"),
  outcome: z.literal("cancelled"),
  reason: z.enum(["generation-replaced", "connection-invalidated", "session-closed"]),
  startingReplica: z.enum(["live", "empty"]),
  durationMs,
});

const BrowserRecoveryAttachStartEventSchema = z.union([
  BrowserRecoveryAttachStartReadyEventSchema,
  BrowserRecoveryAttachStartTimeoutEventSchema,
  BrowserRecoveryAttachStartCancelledEventSchema,
]);

const BrowserInputAckEventSchema = z.strictObject({
  ...browserBase,
  name: z.literal("browser.input.ack"),
  inputKind: z.enum(["key", "text", "paste", "focus", "mouse", "resize"]),
  status: z.enum(["written", "duplicate", "rejected", "uncertain"]),
  sendToAckMs: durationMs,
  outstandingInputs: frameCount,
});

const browserSnapshotEventBase = {
  ...browserBase,
  outcome: z.enum(["ready", "timeout", "cancelled", "failed"]),
  durationMs,
  snapshotBytesBucket: byteSizeBucket,
} as const;

const BrowserSnapshotLoadTotalEventSchema = z.strictObject({
  ...browserSnapshotEventBase,
  name: z.literal("browser.snapshot.load-total"),
});

const BrowserSnapshotRestoreEventSchema = z.strictObject({
  ...browserSnapshotEventBase,
  name: z.literal("browser.snapshot.restore"),
});

const BrowserSnapshotBufferFlushEventSchema = z.strictObject({
  ...browserBase,
  name: z.literal("browser.snapshot.buffer-flush"),
  outcome: z.enum(["applied", "failed"]),
  durationMs,
  bufferedFrames: frameCount,
  bufferedBytesBucket: byteSizeBucket,
});

const BrowserSnapshotAdoptEventSchema = z.strictObject({
  ...browserBase,
  name: z.literal("browser.snapshot.adopt"),
  outcome: z.enum(["call-returned", "threw"]),
  durationMs,
});

const browserRecoveryOutcomeBase = {
  ...browserBase,
  name: z.literal("browser.recovery.outcome"),
  mode: z.enum(["warm", "snapshot"]),
  startingReplica: z.enum(["empty", "live"]),
  totalDurationMs: durationMs,
} as const;

const BrowserRecoveryOutcomeEventSchema = z.union([
  z.strictObject({
    ...browserRecoveryOutcomeBase,
    outcome: z.literal("live"),
    reason: z.literal("none"),
  }),
  z.strictObject({
    ...browserRecoveryOutcomeBase,
    outcome: z.literal("resync"),
    reason: z.enum([
      "journal-gap",
      "slow-client",
      "engine-mismatch",
      "epoch-changed",
      "restore-failed",
    ]),
  }),
  z.strictObject({
    ...browserRecoveryOutcomeBase,
    outcome: z.literal("superseded"),
    reason: z.literal("generation-fenced"),
  }),
  z.strictObject({
    ...browserRecoveryOutcomeBase,
    outcome: z.literal("closed"),
    reason: z.literal("session-closed"),
  }),
]);

export const BrowserTelemetryEventSchema = z.union([
  BrowserRelayRttEventSchema,
  BrowserRecoveryAttachStartEventSchema,
  BrowserInputAckEventSchema,
  BrowserSnapshotLoadTotalEventSchema,
  BrowserSnapshotRestoreEventSchema,
  BrowserSnapshotBufferFlushEventSchema,
  BrowserSnapshotAdoptEventSchema,
  BrowserRecoveryOutcomeEventSchema,
]);

const cloudBase = {
  ...base,
  schemaVersion: z.literal(2),
  clockKind: z.literal("workers-io"),
  sampleWeight,
} as const;

const CloudRelayQueueProcessedEventSchema = z.strictObject({
  ...cloudBase,
  name: z.literal("cloud.relay.queue"),
  lane: z.enum(["browser-control", "host-control", "host-data"]),
  queueProfile: z.enum(["browser-control", "host-control", "host-data"]),
  outcome: z.enum(["completed", "failed"]),
  capacityReason: z.literal("not-applicable"),
  observedAdmissionMs: durationMs,
  observedQueueWaitMs: durationMs,
  observedHandlingMs: durationMs,
  frameBytesBucket: byteSizeBucket,
  globalQueuedBytesBucket: byteSizeBucket,
  socketQueuedBytesBucket: byteSizeBucket,
  globalQueuedCount: frameCount,
  socketQueuedCount: frameCount,
});

const CloudRelayQueueCapacityEventSchema = z.strictObject({
  ...cloudBase,
  name: z.literal("cloud.relay.queue"),
  lane: z.enum(["browser-control", "host-control", "host-data"]),
  queueProfile: z.enum(["browser-control", "host-control", "host-data"]),
  outcome: z.literal("capacity"),
  capacityReason: z.enum([
    "global-count",
    "global-bytes",
    "socket-count",
    "socket-bytes",
    "invalid-size",
  ]),
  observedAdmissionMs: durationMs,
  observedQueueWaitMs: durationMs,
  observedHandlingMs: durationMs,
  frameBytesBucket: byteSizeBucket,
  globalQueuedBytesBucket: byteSizeBucket,
  socketQueuedBytesBucket: byteSizeBucket,
  globalQueuedCount: frameCount,
  socketQueuedCount: frameCount,
});

const CloudRelayQueueEventSchema = z
  .union([CloudRelayQueueProcessedEventSchema, CloudRelayQueueCapacityEventSchema])
  .superRefine((event, context) => {
    if (event.lane !== event.queueProfile) {
      context.addIssue({
        code: "custom",
        message: "queueProfile must match the current relay lane",
        path: ["queueProfile"],
      });
    }
  });

const CloudInputForwardEventSchema = z.strictObject({
  ...cloudBase,
  name: z.literal("cloud.input.forward"),
  inputKind: z.enum(["key", "text", "paste", "focus", "mouse", "resize"]),
  leaseOutcome: z.enum(["active", "inactive", "not-attempted", "uncertain"]),
  outcome: z.enum([
    "send-returned",
    "send-rejected",
    "send-uncertain",
    "lease-rejected",
    "stale",
    "host-offline",
    "failed",
  ]),
  observedQueueWaitMs: durationMs,
  observedIngressToLeaseDecisionMs: durationMs,
  observedIngressToSendDecisionMs: durationMs,
  frameBytesBucket: byteSizeBucket,
  globalQueuedCount: frameCount,
  socketQueuedCount: frameCount,
});

const CloudInputAckForwardEventSchema = z.strictObject({
  ...cloudBase,
  name: z.literal("cloud.input.ack-forward"),
  status: z.enum(["written", "duplicate", "rejected", "uncertain"]),
  outcome: z.enum(["send-returned", "target-missing", "send-failed"]),
  observedQueueWaitMs: durationMs,
  observedIngressToSendDecisionMs: durationMs,
  frameBytesBucket: byteSizeBucket,
});

const CloudDataFanoutEventSchema = z
  .strictObject({
    ...cloudBase,
    name: z.literal("cloud.data.fanout"),
    path: z.enum(["canonical", "directed"]),
    frameKind: z.enum(["pty-output", "resize-applied", "replay-commit", "reset", "other"]),
    outcome: z.enum(["completed", "degraded", "not-targeted", "stale", "host-failed"]),
    reason: z.enum([
      "none",
      "no-eligible-browser",
      "generation-fenced",
      "client-gone",
      "credit-exceeded",
      "data-send-uncertain",
      "sequence-error",
      "canonical-rejected",
      "directed-reset",
      "pinned-commit-exceeded",
      "mixed",
    ]),
    observedQueueWaitMs: durationMs,
    observedIngressToFanoutDecisionMs: durationMs,
    frameBytesBucket: byteSizeBucket,
    selectedTargets: frameCount,
    sendReturnedTargets: frameCount,
    staleTargets: frameCount,
    sequenceErrorTargets: frameCount,
    creditResetTargets: frameCount,
    sendUncertainResetTargets: frameCount,
    maxCreditUtilization: z.enum([
      "not-evaluated",
      "0",
      "1-25%",
      "26-50%",
      "51-75%",
      "76-100%",
      ">100%",
    ]),
  })
  .superRefine((event, context) => {
    const accountedTargets =
      event.sendReturnedTargets +
      event.staleTargets +
      event.sequenceErrorTargets +
      event.creditResetTargets +
      event.sendUncertainResetTargets;
    if (accountedTargets !== event.selectedTargets) {
      context.addIssue({
        code: "custom",
        message: "fanout target outcomes must account for every selected target",
        path: ["selectedTargets"],
      });
    }
    const expectedReasons: Record<typeof event.outcome, ReadonlySet<typeof event.reason>> = {
      completed: new Set(["none"]),
      degraded: new Set(["credit-exceeded", "data-send-uncertain", "sequence-error", "mixed"]),
      "not-targeted": new Set(["no-eligible-browser", "client-gone"]),
      stale: new Set(["generation-fenced"]),
      "host-failed": new Set([
        "sequence-error",
        "canonical-rejected",
        "directed-reset",
        "pinned-commit-exceeded",
      ]),
    };
    if (!expectedReasons[event.outcome].has(event.reason)) {
      context.addIssue({
        code: "custom",
        message: "fanout reason is incompatible with outcome",
        path: ["reason"],
      });
    }
    if (event.outcome === "completed" && event.selectedTargets === 0) {
      context.addIssue({
        code: "custom",
        message: "completed fanout requires at least one selected target",
        path: ["selectedTargets"],
      });
    }
    if (event.outcome === "not-targeted" && event.selectedTargets !== 0) {
      context.addIssue({
        code: "custom",
        message: "not-targeted fanout cannot select a target",
        path: ["selectedTargets"],
      });
    }
    if (event.creditResetTargets > 0 && event.maxCreditUtilization !== ">100%") {
      context.addIssue({
        code: "custom",
        message: "credit reset requires over-limit utilization",
        path: ["maxCreditUtilization"],
      });
    }
  });

const CloudWriterLeaseAcquireEventSchema = z.strictObject({
  ...cloudBase,
  name: z.literal("cloud.writer.lease"),
  operation: z.literal("acquire"),
  trigger: z.literal("attach"),
  outcome: z.enum([
    "acquired-current",
    "acquired-released-stale",
    "unavailable-current",
    "unavailable-stale",
    "uncertain",
  ]),
  observedLeaseOutcomeMs: durationMs,
});

const CloudWriterLeaseRenewEventSchema = z.strictObject({
  ...cloudBase,
  name: z.literal("cloud.writer.lease"),
  operation: z.literal("verify-renew"),
  trigger: z.literal("heartbeat"),
  outcome: z.enum([
    "renewed-current",
    "renewed-stale",
    "inactive-current",
    "inactive-stale",
    "uncertain",
  ]),
  observedLeaseOutcomeMs: durationMs,
});

const CloudWriterLeaseEventSchema = z.union([
  CloudWriterLeaseAcquireEventSchema,
  CloudWriterLeaseRenewEventSchema,
]);

const CloudRecoveryBarrierEventSchema = z.strictObject({
  ...cloudBase,
  name: z.literal("cloud.recovery.barrier"),
  mode: z.enum(["warm", "snapshot", "unknown"]),
  outcome: z.enum([
    "ready",
    "stale",
    "rejected",
    "host-failed",
    "result-send-rejected",
    "result-send-uncertain",
    "failed",
  ]),
  reason: z.enum([
    "none",
    "generation-fenced",
    "client-gone",
    "missing-live-seed",
    "snapshot-missing",
    "snapshot-metadata-mismatch",
    "browser-control-send-failed",
    "cloud-head-behind-cut",
    "host-channels-not-current",
    "invalid-payload",
    "canonical-head-mismatch",
    "active-delivery-conflict",
    "pinned-delivery-conflict",
    "snapshot-seed-conflict",
    "unexpected",
  ]),
  retryScope: z.enum([
    "not-applicable",
    "same-generation",
    "refresh-checkpoint",
    "reset-generation",
    "drop-client",
  ]),
  observedDurationMs: durationMs,
});

const CloudRecoveryAttachTransitionEventSchema = z.strictObject({
  ...cloudBase,
  name: z.literal("cloud.recovery.transition"),
  transition: z.literal("attach"),
  replica: z.enum(["live", "empty"]),
  outcome: z.enum([
    "host-request-send-returned",
    "host-request-send-rejected",
    "host-request-send-uncertain",
    "host-offline",
    "rejected",
    "stale",
    "failed",
  ]),
  reason: z.enum([
    "none",
    "already-attached",
    "cursor-ahead",
    "data-missing",
    "engine-mismatch",
    "epoch-changed",
    "session-missing",
    "stale-delivery",
    "stale-control",
    "welcome-send-failed",
    "unexpected",
  ]),
  observedDurationMs: durationMs,
});

const CloudRecoveryResetTransitionEventSchema = z.strictObject({
  ...cloudBase,
  name: z.literal("cloud.recovery.transition"),
  transition: z.literal("reset"),
  trigger: z.enum([
    "journal-gap",
    "slow-client",
    "engine-mismatch",
    "epoch-changed",
    "data-disconnected",
    "host-reconnect",
  ]),
  outcome: z.enum(["issued", "not-applicable", "stale", "browser-send-failed", "failed"]),
  hostNotifyOutcome: z.enum([
    "not-requested",
    "not-attempted",
    "send-returned",
    "send-rejected",
    "send-uncertain",
  ]),
  observedDurationMs: durationMs,
});

const CloudRecoveryTransitionEventSchema = z.union([
  CloudRecoveryAttachTransitionEventSchema,
  CloudRecoveryResetTransitionEventSchema,
]);

export const CloudTelemetryEventSchema = z.union([
  CloudRelayQueueEventSchema,
  CloudInputForwardEventSchema,
  CloudInputAckForwardEventSchema,
  CloudDataFanoutEventSchema,
  CloudWriterLeaseEventSchema,
  CloudRecoveryBarrierEventSchema,
  CloudRecoveryTransitionEventSchema,
]);
export const CloudTelemetryWriteEventSchema = CloudTelemetryEventSchema;

const LegacyCloudRelayQueueProcessedEventSchema = CloudRelayQueueProcessedEventSchema.omit({
  schemaVersion: true,
}).extend({ schemaVersion: z.literal(1) });
const LegacyCloudRelayQueueCapacityEventSchema = CloudRelayQueueCapacityEventSchema.omit({
  schemaVersion: true,
}).extend({ schemaVersion: z.literal(1) });
const LegacyCloudRelayQueueEventSchema = z
  .union([LegacyCloudRelayQueueProcessedEventSchema, LegacyCloudRelayQueueCapacityEventSchema])
  .superRefine((event, context) => {
    if (event.lane !== event.queueProfile) {
      context.addIssue({
        code: "custom",
        message: "queueProfile must match the current relay lane",
        path: ["queueProfile"],
      });
    }
  });
const LegacyCloudInputForwardEventSchema = CloudInputForwardEventSchema.omit({
  schemaVersion: true,
}).extend({ schemaVersion: z.literal(1) });
const LegacyCloudRecoveryBarrierEventSchema = CloudRecoveryBarrierEventSchema.omit({
  schemaVersion: true,
}).extend({ schemaVersion: z.literal(1) });
const LegacyCloudRecoveryAttachTransitionEventSchema =
  CloudRecoveryAttachTransitionEventSchema.omit({ schemaVersion: true }).extend({
    schemaVersion: z.literal(1),
  });
const LegacyCloudRecoveryResetTransitionEventSchema = CloudRecoveryResetTransitionEventSchema.omit({
  schemaVersion: true,
}).extend({
  schemaVersion: z.literal(1),
});
const LegacyCloudRecoveryTransitionEventSchema = z.union([
  LegacyCloudRecoveryAttachTransitionEventSchema,
  LegacyCloudRecoveryResetTransitionEventSchema,
]);
const LegacyCloudTelemetryEventSchema = z.union([
  LegacyCloudRelayQueueEventSchema,
  LegacyCloudInputForwardEventSchema,
  LegacyCloudRecoveryBarrierEventSchema,
  LegacyCloudRecoveryTransitionEventSchema,
]);

export const CloudTelemetryReadEventSchema = z.union([
  LegacyCloudTelemetryEventSchema,
  CloudTelemetryWriteEventSchema,
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
  BrowserTelemetryEventSchema,
  CloudTelemetryEventSchema,
]);

export type TerminalTelemetryEvent = z.output<typeof TerminalTelemetryEventSchema>;
export type CloudTelemetryEvent = z.output<typeof CloudTelemetryEventSchema>;
export type BrowserTelemetryEvent = z.output<typeof BrowserTelemetryEventSchema>;
export type CloudTelemetryReadEvent = z.output<typeof CloudTelemetryReadEventSchema>;
export type TelemetrySink = (event: TerminalTelemetryEvent) => void;
export type BrowserTelemetrySink = (event: BrowserTelemetryEvent) => void;
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
            const parsed = TerminalTelemetryEventSchema.parse(event);
            const result = (target as (sample: TerminalTelemetryEvent) => unknown)(parsed);
            if (isPromiseLike(result)) void Promise.resolve(result).catch(() => undefined);
          } catch {
            // A schema or collector failure only drops this diagnostic event.
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
