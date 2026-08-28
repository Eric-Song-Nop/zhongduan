import { describe, expect, it, vi } from "vitest";

import {
  BrowserTelemetryEventSchema,
  CLOUD_TELEMETRY_RECORD_TYPE,
  CLOUD_TELEMETRY_RUNTIME,
  CloudflareCloudTelemetryLogEventSchema,
  CloudTelemetryProducerSamplePolicySchema,
  CloudTelemetryReadEventSchema,
  CloudTelemetryReadLogRecordSchema,
  CloudTelemetryWriteEventSchema,
  TerminalTelemetryEventSchema,
  createBufferedTelemetrySink,
  elapsedMs,
  emitTelemetry,
  telemetryByteSizeBucket,
  type BrowserTelemetryEvent,
  type TelemetrySink,
  type TerminalTelemetryEvent,
} from "./index";

const cloudRelayQueueEvent = {
  schemaVersion: 2 as const,
  monotonicAtMs: 30,
  clockKind: "workers-io" as const,
  sampleWeight: 64,
  name: "cloud.relay.queue" as const,
  lane: "host-data" as const,
  queueProfile: "host-data" as const,
  outcome: "completed" as const,
  capacityReason: "not-applicable" as const,
  observedAdmissionMs: 0,
  observedQueueWaitMs: 4,
  observedHandlingMs: 0,
  frameBytesBucket: "1025-65536" as const,
  globalQueuedBytesBucket: "65537+" as const,
  socketQueuedBytesBucket: "65537+" as const,
  globalQueuedCount: 8,
  socketQueuedCount: 4,
};

const cloudInputForwardEvent = {
  schemaVersion: 2 as const,
  monotonicAtMs: 32,
  clockKind: "workers-io" as const,
  sampleWeight: 16,
  name: "cloud.input.forward" as const,
  inputKind: "key" as const,
  leaseOutcome: "active" as const,
  outcome: "send-returned" as const,
  observedQueueWaitMs: 3,
  observedIngressToLeaseDecisionMs: 4,
  observedIngressToSendDecisionMs: 5,
  frameBytesBucket: "65-1024" as const,
  globalQueuedCount: 2,
  socketQueuedCount: 1,
};

const cloudInputAckForwardEvent = {
  schemaVersion: 2 as const,
  monotonicAtMs: 36,
  clockKind: "workers-io" as const,
  sampleWeight: 16,
  name: "cloud.input.ack-forward" as const,
  status: "written" as const,
  outcome: "send-returned" as const,
  observedQueueWaitMs: 2,
  observedIngressToSendDecisionMs: 4,
  frameBytesBucket: "65-1024" as const,
};

const cloudDataFanoutEvent = {
  schemaVersion: 2 as const,
  monotonicAtMs: 37,
  clockKind: "workers-io" as const,
  sampleWeight: 64,
  name: "cloud.data.fanout" as const,
  path: "canonical" as const,
  frameKind: "pty-output" as const,
  outcome: "completed" as const,
  reason: "none" as const,
  observedQueueWaitMs: 3,
  observedIngressToFanoutDecisionMs: 5,
  frameBytesBucket: "1025-65536" as const,
  selectedTargets: 2,
  sendReturnedTargets: 2,
  staleTargets: 0,
  sequenceErrorTargets: 0,
  creditResetTargets: 0,
  sendUncertainResetTargets: 0,
  maxCreditUtilization: "51-75%" as const,
};

const cloudWriterLeaseAcquireEvent = {
  schemaVersion: 2 as const,
  monotonicAtMs: 38,
  clockKind: "workers-io" as const,
  sampleWeight: 1,
  name: "cloud.writer.lease" as const,
  operation: "acquire" as const,
  trigger: "attach" as const,
  outcome: "acquired-current" as const,
  observedLeaseOutcomeMs: 2,
};

const cloudWriterLeaseRenewEvent = {
  schemaVersion: 2 as const,
  monotonicAtMs: 39,
  clockKind: "workers-io" as const,
  sampleWeight: 64,
  name: "cloud.writer.lease" as const,
  operation: "verify-renew" as const,
  trigger: "heartbeat" as const,
  outcome: "renewed-current" as const,
  observedLeaseOutcomeMs: 1,
};

const cloudRecoveryBarrierEvent = {
  schemaVersion: 2 as const,
  monotonicAtMs: 40,
  clockKind: "workers-io" as const,
  sampleWeight: 1,
  name: "cloud.recovery.barrier" as const,
  mode: "snapshot" as const,
  outcome: "rejected" as const,
  reason: "snapshot-missing" as const,
  retryScope: "refresh-checkpoint" as const,
  observedDurationMs: 1,
};

const cloudRecoveryAttachEvent = {
  schemaVersion: 2 as const,
  monotonicAtMs: 41,
  clockKind: "workers-io" as const,
  sampleWeight: 1,
  name: "cloud.recovery.transition" as const,
  transition: "attach" as const,
  replica: "live" as const,
  outcome: "host-request-send-returned" as const,
  reason: "none" as const,
  observedDurationMs: 2,
};

const cloudRecoveryResetEvent = {
  schemaVersion: 2 as const,
  monotonicAtMs: 42,
  clockKind: "workers-io" as const,
  sampleWeight: 1,
  name: "cloud.recovery.transition" as const,
  transition: "reset" as const,
  trigger: "journal-gap" as const,
  outcome: "issued" as const,
  hostNotifyOutcome: "not-requested" as const,
  observedDurationMs: 3,
};

const cloudV2ProducerEvents = [
  cloudRelayQueueEvent,
  cloudInputForwardEvent,
  cloudInputAckForwardEvent,
  cloudDataFanoutEvent,
  cloudWriterLeaseAcquireEvent,
  cloudWriterLeaseRenewEvent,
  cloudRecoveryBarrierEvent,
  cloudRecoveryAttachEvent,
  cloudRecoveryResetEvent,
] as const;

const cloudV1ProducerEvents = [
  { ...cloudRelayQueueEvent, schemaVersion: 1 as const },
  { ...cloudInputForwardEvent, schemaVersion: 1 as const },
  { ...cloudRecoveryBarrierEvent, schemaVersion: 1 as const },
  { ...cloudRecoveryAttachEvent, schemaVersion: 1 as const },
  { ...cloudRecoveryResetEvent, schemaVersion: 1 as const },
] as const;

function cloudTelemetryRecord(event: object) {
  return {
    type: CLOUD_TELEMETRY_RECORD_TYPE,
    runtime: CLOUD_TELEMETRY_RUNTIME,
    ...event,
  };
}

describe("terminal telemetry", () => {
  it("accepts the privacy-safe recovery event shapes", () => {
    expect(
      TerminalTelemetryEventSchema.parse({
        schemaVersion: 1,
        monotonicAtMs: 10,
        name: "host.journal.range",
        mode: "snapshot",
        status: "exact",
        deliveryCreditBytes: 704,
        encodedBytes: 512,
        frames: 3,
        oldestMutationAgeMs: 8,
      }),
    ).toMatchObject({ status: "exact", frames: 3 });
  });

  it.each([
    {
      schemaVersion: 1 as const,
      monotonicAtMs: 20,
      name: "host.control.queue" as const,
      messageClass: "input" as const,
      outcome: "handled" as const,
      admissionMs: 1,
      queueWaitMs: 3,
      handlingMs: 4,
      queuedBytesBucket: "65-1024" as const,
      queuedCount: 2,
    },
    {
      schemaVersion: 1 as const,
      monotonicAtMs: 21,
      name: "host.input.apply" as const,
      inputKind: "key" as const,
      outcome: "written" as const,
      effectStage: "completed" as const,
      encodeKind: "ghostty" as const,
      ackSendOutcome: "send-returned" as const,
      ackSendMs: 0.25,
      controlAdmissionMs: 1,
      controlQueueWaitMs: 2,
      controlQueueDepth: 1,
      actorQueueWaitMs: 2,
      actorProcessingMs: 1,
      hostIngressToAckDecisionMs: 7,
      inputEncodeMs: 0.5,
      ptyWriteAttempted: true,
      ptyWriteMs: 0.25,
      ptyBytesBucket: "1-8" as const,
    },
    {
      schemaVersion: 1 as const,
      monotonicAtMs: 22,
      name: "host.input.apply" as const,
      inputKind: "resize" as const,
      outcome: "written" as const,
      effectStage: "completed" as const,
      ackSendOutcome: "send-returned" as const,
      ackSendMs: 0.25,
      controlAdmissionMs: 1,
      controlQueueWaitMs: 2,
      controlQueueDepth: 1,
      actorQueueWaitMs: 2,
      actorProcessingMs: 1,
      hostIngressToAckDecisionMs: 7,
      authorityResizeMs: 0.5,
      ptyResizeAttempted: true,
      ptyResizeMs: 0.25,
      effectWriteAttempted: false,
      effectWriteMs: 0,
      effectBytesBucket: "0" as const,
    },
    {
      schemaVersion: 1 as const,
      monotonicAtMs: 23,
      name: "host.relay.rtt" as const,
      channel: "control" as const,
      outcome: "ok" as const,
      durationMs: 18,
      outstandingPings: 0,
    },
    {
      schemaVersion: 1 as const,
      monotonicAtMs: 24,
      name: "host.relay.rtt" as const,
      channel: "data" as const,
      outcome: "timeout" as const,
      silenceMs: 45_000,
      outstandingPings: 2,
    },
  ])("accepts bounded Host latency event $name", (event) => {
    expect(TerminalTelemetryEventSchema.parse(event)).toEqual(event);
  });

  it.each([
    {
      schemaVersion: 2 as const,
      monotonicAtMs: 30,
      clockKind: "workers-io" as const,
      sampleWeight: 64,
      name: "cloud.relay.queue" as const,
      lane: "host-data" as const,
      queueProfile: "host-data" as const,
      outcome: "completed" as const,
      capacityReason: "not-applicable" as const,
      observedAdmissionMs: 0,
      observedQueueWaitMs: 4,
      observedHandlingMs: 0,
      frameBytesBucket: "1025-65536" as const,
      globalQueuedBytesBucket: "65537+" as const,
      socketQueuedBytesBucket: "65537+" as const,
      globalQueuedCount: 8,
      socketQueuedCount: 4,
    },
    {
      schemaVersion: 2 as const,
      monotonicAtMs: 31,
      clockKind: "workers-io" as const,
      sampleWeight: 1,
      name: "cloud.relay.queue" as const,
      lane: "browser-control" as const,
      queueProfile: "browser-control" as const,
      outcome: "capacity" as const,
      capacityReason: "socket-count" as const,
      observedAdmissionMs: 0,
      observedQueueWaitMs: 0,
      observedHandlingMs: 0,
      frameBytesBucket: "65-1024" as const,
      globalQueuedBytesBucket: "65537+" as const,
      socketQueuedBytesBucket: "65537+" as const,
      globalQueuedCount: 9,
      socketQueuedCount: 8,
    },
    {
      schemaVersion: 2 as const,
      monotonicAtMs: 32,
      clockKind: "workers-io" as const,
      sampleWeight: 8,
      name: "cloud.input.forward" as const,
      inputKind: "key" as const,
      leaseOutcome: "active" as const,
      outcome: "send-returned" as const,
      observedQueueWaitMs: 3,
      observedIngressToLeaseDecisionMs: 4,
      observedIngressToSendDecisionMs: 5,
      frameBytesBucket: "65-1024" as const,
      globalQueuedCount: 2,
      socketQueuedCount: 1,
    },
    {
      schemaVersion: 2 as const,
      monotonicAtMs: 33,
      clockKind: "workers-io" as const,
      sampleWeight: 1,
      name: "cloud.recovery.barrier" as const,
      mode: "snapshot" as const,
      outcome: "rejected" as const,
      reason: "snapshot-missing" as const,
      retryScope: "refresh-checkpoint" as const,
      observedDurationMs: 1,
    },
    {
      schemaVersion: 2 as const,
      monotonicAtMs: 34,
      clockKind: "workers-io" as const,
      sampleWeight: 1,
      name: "cloud.recovery.transition" as const,
      transition: "attach" as const,
      replica: "live" as const,
      outcome: "host-request-send-returned" as const,
      reason: "none" as const,
      observedDurationMs: 2,
    },
    {
      schemaVersion: 2 as const,
      monotonicAtMs: 35,
      clockKind: "workers-io" as const,
      sampleWeight: 1,
      name: "cloud.recovery.transition" as const,
      transition: "reset" as const,
      trigger: "journal-gap" as const,
      outcome: "issued" as const,
      hostNotifyOutcome: "not-requested" as const,
      observedDurationMs: 3,
    },
    cloudInputAckForwardEvent,
    cloudDataFanoutEvent,
    cloudWriterLeaseAcquireEvent,
    cloudWriterLeaseRenewEvent,
  ])("accepts the content-free Cloud event $name", (event) => {
    expect(TerminalTelemetryEventSchema.parse(event)).toEqual(event);
  });

  it.each([
    {
      schemaVersion: 1,
      monotonicAtMs: 40,
      clockKind: "browser-performance",
      name: "browser.relay.rtt",
      channel: "control",
      outcome: "success",
      durationMs: 18,
      outstandingPings: 0,
    },
    {
      schemaVersion: 1,
      monotonicAtMs: 41,
      clockKind: "browser-performance",
      name: "browser.relay.rtt",
      channel: "data",
      outcome: "timeout",
      silenceMs: 45_000,
      outstandingPings: 2,
    },
    {
      schemaVersion: 1,
      monotonicAtMs: 42,
      clockKind: "browser-performance",
      name: "browser.recovery.attach-start",
      outcome: "matching-start-received",
      startingReplica: "live",
      mode: "warm",
      durationMs: 12,
    },
    {
      schemaVersion: 1,
      monotonicAtMs: 43,
      clockKind: "browser-performance",
      name: "browser.recovery.attach-start",
      outcome: "timeout",
      startingReplica: "empty",
      durationMs: 205_000,
    },
    {
      schemaVersion: 1,
      monotonicAtMs: 44,
      clockKind: "browser-performance",
      name: "browser.recovery.attach-start",
      outcome: "cancelled",
      reason: "generation-replaced",
      startingReplica: "live",
      durationMs: 7,
    },
    {
      schemaVersion: 1,
      monotonicAtMs: 45,
      clockKind: "browser-performance",
      name: "browser.input.ack",
      inputKind: "key",
      status: "written",
      sendToAckMs: 22,
      outstandingInputs: 3,
    },
    {
      schemaVersion: 1,
      monotonicAtMs: 46,
      clockKind: "browser-performance",
      name: "browser.snapshot.load-total",
      outcome: "ready",
      durationMs: 35,
      snapshotBytesBucket: "1025-65536",
    },
    {
      schemaVersion: 1,
      monotonicAtMs: 47,
      clockKind: "browser-performance",
      name: "browser.snapshot.restore",
      outcome: "cancelled",
      durationMs: 8,
      snapshotBytesBucket: "65537+",
    },
    {
      schemaVersion: 1,
      monotonicAtMs: 48,
      clockKind: "browser-performance",
      name: "browser.snapshot.buffer-flush",
      outcome: "applied",
      durationMs: 4,
      bufferedFrames: 6,
      bufferedBytesBucket: "1025-65536",
    },
    {
      schemaVersion: 1,
      monotonicAtMs: 49,
      clockKind: "browser-performance",
      name: "browser.snapshot.adopt",
      outcome: "call-returned",
      durationMs: 1,
    },
    {
      schemaVersion: 1,
      monotonicAtMs: 50,
      clockKind: "browser-performance",
      name: "browser.recovery.outcome",
      mode: "snapshot",
      startingReplica: "empty",
      outcome: "resync",
      reason: "journal-gap",
      totalDurationMs: 61,
    },
  ] satisfies BrowserTelemetryEvent[])("accepts strict Browser event $name", (event) => {
    expect(BrowserTelemetryEventSchema.parse(event)).toEqual(event);
    expect(TerminalTelemetryEventSchema.parse(event)).toEqual(event);
  });

  it("keeps attach-start outcomes as disjoint strict shapes", () => {
    const common = {
      schemaVersion: 1,
      monotonicAtMs: 42,
      clockKind: "browser-performance",
      name: "browser.recovery.attach-start",
      startingReplica: "empty",
      durationMs: 10,
    };
    expect(() =>
      BrowserTelemetryEventSchema.parse({
        ...common,
        outcome: "matching-start-received",
      }),
    ).toThrow();
    expect(() =>
      BrowserTelemetryEventSchema.parse({
        ...common,
        outcome: "timeout",
        mode: "snapshot",
      }),
    ).toThrow();
    expect(() =>
      BrowserTelemetryEventSchema.parse({
        ...common,
        outcome: "cancelled",
      }),
    ).toThrow();
  });

  it("rejects impossible recovery outcome and reason combinations", () => {
    const common = {
      schemaVersion: 1,
      monotonicAtMs: 50,
      clockKind: "browser-performance",
      name: "browser.recovery.outcome",
      mode: "snapshot",
      startingReplica: "empty",
      totalDurationMs: 10,
    };
    for (const event of [
      { ...common, outcome: "live", reason: "journal-gap" },
      { ...common, outcome: "closed", reason: "none" },
      { ...common, outcome: "superseded", reason: "restore-failed" },
    ]) {
      expect(() => BrowserTelemetryEventSchema.parse(event)).toThrow();
    }
  });

  it.each([
    "sessionId",
    "clientId",
    "connectionId",
    "streamId",
    "deliveryGeneration",
    "inputEpoch",
    "clientInputSeq",
    "writerLease",
    "snapshotId",
    "text",
    "key",
    "error",
  ])("rejects the Browser diagnostic identity/content field %s", (field) => {
    expect(() =>
      BrowserTelemetryEventSchema.parse({
        schemaVersion: 1,
        monotonicAtMs: 44,
        clockKind: "browser-performance",
        name: "browser.input.ack",
        inputKind: "text",
        status: "written",
        sendToAckMs: 22,
        outstandingInputs: 3,
        [field]: "must-stay-local",
      }),
    ).toThrow();
  });

  it("enforces the current queue lane/profile and capacity shape", () => {
    const baseEvent = {
      schemaVersion: 2,
      monotonicAtMs: 30,
      clockKind: "workers-io",
      sampleWeight: 1,
      name: "cloud.relay.queue",
      lane: "browser-control",
      queueProfile: "host-control",
      outcome: "completed",
      capacityReason: "not-applicable",
      observedAdmissionMs: 0,
      observedQueueWaitMs: 0,
      observedHandlingMs: 0,
      frameBytesBucket: "1-8",
      globalQueuedBytesBucket: "1-8",
      socketQueuedBytesBucket: "1-8",
      globalQueuedCount: 1,
      socketQueuedCount: 1,
    };
    expect(() => TerminalTelemetryEventSchema.parse(baseEvent)).toThrow();
    expect(() =>
      TerminalTelemetryEventSchema.parse({
        ...baseEvent,
        queueProfile: "browser-control",
        outcome: "capacity",
        capacityReason: "not-applicable",
      }),
    ).toThrow();
  });

  it("rejects Cloud v1 fixtures and impossible input ACK classifications", () => {
    for (const event of [
      { ...cloudInputAckForwardEvent, schemaVersion: 1 },
      { ...cloudInputAckForwardEvent, status: "delivered" },
      { ...cloudInputAckForwardEvent, outcome: "send-uncertain" },
    ]) {
      expect(() => TerminalTelemetryEventSchema.parse(event)).toThrow();
    }
  });

  it("reads legacy Cloud v1 facts without accepting them at the v2 write boundary", () => {
    const legacyInputForward = {
      schemaVersion: 1,
      monotonicAtMs: 32,
      clockKind: "workers-io",
      sampleWeight: 8,
      name: "cloud.input.forward",
      inputKind: "key",
      leaseOutcome: "active",
      outcome: "send-returned",
      observedQueueWaitMs: 3,
      observedIngressToLeaseDecisionMs: 4,
      observedIngressToSendDecisionMs: 5,
      frameBytesBucket: "65-1024",
      globalQueuedCount: 2,
      socketQueuedCount: 1,
    };
    expect(CloudTelemetryReadEventSchema.parse(legacyInputForward)).toEqual(legacyInputForward);
    expect(() => CloudTelemetryWriteEventSchema.parse(legacyInputForward)).toThrow();
    expect(CloudTelemetryReadEventSchema.parse(cloudInputAckForwardEvent)).toEqual(
      cloudInputAckForwardEvent,
    );
    expect(() =>
      CloudTelemetryReadEventSchema.parse({
        ...cloudInputAckForwardEvent,
        schemaVersion: 1,
      }),
    ).toThrow();
  });

  it.each([...cloudV1ProducerEvents, ...cloudV2ProducerEvents])(
    "strictly parses the wrapped Cloud producer record $schemaVersion/$name",
    (event) => {
      const record = cloudTelemetryRecord(event);
      expect(CloudTelemetryReadLogRecordSchema.parse(record)).toEqual(record);
      expect(CloudTelemetryProducerSamplePolicySchema.parse(event)).toEqual(event);
    },
  );

  it("extracts only a strict record from a Cloudflare API event envelope", () => {
    const record = cloudTelemetryRecord(cloudDataFanoutEvent);
    const parsed = CloudflareCloudTelemetryLogEventSchema.parse({
      source: record,
      id: "platform-invocation-id",
      url: "https://example.invalid/terminal/session-secret?ticket=one-time-secret",
      metadata: {
        durableObjectId: "platform-do-id",
        request: { url: "https://example.invalid/also-sensitive" },
      },
    });

    expect(parsed).toEqual(record);
    expect(parsed).not.toHaveProperty("id");
    expect(parsed).not.toHaveProperty("url");
    expect(parsed).not.toHaveProperty("metadata");
  });

  it("rejects an invalid or missing Cloud telemetry log wrapper", () => {
    const record = cloudTelemetryRecord(cloudInputForwardEvent);
    for (const invalid of [
      { ...record, type: "console.log" },
      { ...record, runtime: "host-daemon" },
      Object.fromEntries(Object.entries(record).filter(([key]) => key !== "type")),
      Object.fromEntries(Object.entries(record).filter(([key]) => key !== "runtime")),
    ]) {
      expect(() => CloudTelemetryReadLogRecordSchema.parse(invalid)).toThrow();
    }
  });

  it("never accepts a Phase 0c event as a legacy v1 wrapped record", () => {
    for (const event of [
      cloudInputAckForwardEvent,
      cloudDataFanoutEvent,
      cloudWriterLeaseAcquireEvent,
      cloudWriterLeaseRenewEvent,
    ]) {
      expect(() =>
        CloudTelemetryReadLogRecordSchema.parse(
          cloudTelemetryRecord({ ...event, schemaVersion: 1 }),
        ),
      ).toThrow();
    }
  });

  it.each([
    "sessionId",
    "clientId",
    "connectionId",
    "writerLease",
    "leaseDigest",
    "snapshotId",
    "url",
    "payload",
    "text",
    "error",
  ])("rejects the extra wrapped-record identity/content field %s", (field) => {
    expect(() =>
      CloudTelemetryReadLogRecordSchema.parse({
        ...cloudTelemetryRecord(cloudDataFanoutEvent),
        [field]: "must-not-enter-query-output",
      }),
    ).toThrow();
  });

  it.each([
    { ...cloudRelayQueueEvent, schemaVersion: 1 as const, sampleWeight: 16 },
    { ...cloudInputForwardEvent, schemaVersion: 1 as const, sampleWeight: 1 },
    { ...cloudRecoveryBarrierEvent, schemaVersion: 1 as const, sampleWeight: 16 },
    { ...cloudRecoveryAttachEvent, schemaVersion: 1 as const, sampleWeight: 16 },
    { ...cloudRelayQueueEvent, sampleWeight: 16 },
    { ...cloudInputForwardEvent, sampleWeight: 1 },
    { ...cloudInputAckForwardEvent, sampleWeight: 1 },
    { ...cloudDataFanoutEvent, sampleWeight: 1 },
    { ...cloudDataFanoutEvent, path: "directed" as const, sampleWeight: 64 },
    { ...cloudWriterLeaseAcquireEvent, sampleWeight: 64 },
    { ...cloudWriterLeaseRenewEvent, sampleWeight: 1 },
    {
      ...cloudWriterLeaseRenewEvent,
      outcome: "renewed-stale" as const,
      sampleWeight: 64,
    },
    { ...cloudRecoveryBarrierEvent, sampleWeight: 16 },
    { ...cloudRecoveryResetEvent, sampleWeight: 16 },
  ])("rejects the wrong producer sample policy for $schemaVersion/$name", (event) => {
    expect(() => CloudTelemetryProducerSamplePolicySchema.parse(event)).toThrow();
    expect(() => CloudTelemetryReadLogRecordSchema.parse(cloudTelemetryRecord(event))).toThrow();
  });

  it.each([
    {
      ...cloudRelayQueueEvent,
      lane: "browser-control" as const,
      queueProfile: "browser-control" as const,
      sampleWeight: 16,
    },
    {
      ...cloudRelayQueueEvent,
      outcome: "failed" as const,
      sampleWeight: 1,
    },
    {
      ...cloudInputForwardEvent,
      outcome: "lease-rejected" as const,
      leaseOutcome: "inactive" as const,
      sampleWeight: 1,
    },
    { ...cloudDataFanoutEvent, path: "directed" as const, sampleWeight: 1 },
    {
      ...cloudWriterLeaseRenewEvent,
      outcome: "renewed-stale" as const,
      sampleWeight: 1,
    },
  ])("accepts the outcome-specific v2 sample policy for $name", (event) => {
    expect(CloudTelemetryProducerSamplePolicySchema.parse(event)).toEqual(event);
  });

  it("rejects impossible data fanout outcome and reason combinations", () => {
    for (const event of [
      { ...cloudDataFanoutEvent, outcome: "completed", reason: "credit-exceeded" },
      { ...cloudDataFanoutEvent, outcome: "not-targeted", reason: "none" },
      { ...cloudDataFanoutEvent, outcome: "stale", reason: "client-gone" },
      { ...cloudDataFanoutEvent, outcome: "host-failed", reason: "data-send-uncertain" },
      { ...cloudDataFanoutEvent, selectedTargets: 3 },
      {
        ...cloudDataFanoutEvent,
        selectedTargets: 0,
        sendReturnedTargets: 0,
        maxCreditUtilization: "not-evaluated",
      },
      {
        ...cloudDataFanoutEvent,
        outcome: "not-targeted",
        reason: "client-gone",
        selectedTargets: 1,
        sendReturnedTargets: 1,
      },
      {
        ...cloudDataFanoutEvent,
        outcome: "degraded",
        reason: "credit-exceeded",
        sendReturnedTargets: 1,
        creditResetTargets: 1,
        maxCreditUtilization: "76-100%",
      },
    ]) {
      expect(() => TerminalTelemetryEventSchema.parse(event)).toThrow();
    }
  });

  it("keeps writer lease acquire and heartbeat renew as disjoint strict shapes", () => {
    for (const event of [
      { ...cloudWriterLeaseAcquireEvent, trigger: "heartbeat" },
      { ...cloudWriterLeaseAcquireEvent, outcome: "renewed-current" },
      { ...cloudWriterLeaseRenewEvent, trigger: "attach" },
      { ...cloudWriterLeaseRenewEvent, outcome: "acquired-current" },
    ]) {
      expect(() => TerminalTelemetryEventSchema.parse(event)).toThrow();
    }
  });

  it.each([
    [
      "cloud.input.ack-forward",
      cloudInputAckForwardEvent,
      [
        "sessionId",
        "clientId",
        "connectionId",
        "inputEpoch",
        "clientInputSeq",
        "writerLease",
        "writerFence",
        "error",
      ],
    ],
    [
      "cloud.data.fanout",
      cloudDataFanoutEvent,
      [
        "sessionId",
        "streamId",
        "deliveryGeneration",
        "eventSeq",
        "ptyOffset",
        "snapshotId",
        "payload",
        "text",
        "error",
      ],
    ],
    [
      "cloud.writer.lease",
      cloudWriterLeaseAcquireEvent,
      [
        "clientId",
        "writerLease",
        "leaseDigest",
        "lease_digest",
        "writerFence",
        "expiresAt",
        "error",
      ],
    ],
    [
      "cloud.writer.lease heartbeat",
      cloudWriterLeaseRenewEvent,
      [
        "clientId",
        "writerLease",
        "leaseDigest",
        "lease_digest",
        "writerFence",
        "expiresAt",
        "error",
      ],
    ],
  ] as const)("rejects extra private fields from %s", (_name, event, fields) => {
    for (const field of fields) {
      expect(() =>
        TerminalTelemetryEventSchema.parse({
          ...event,
          [field]: "must-not-leave-the-runtime",
        }),
      ).toThrow();
    }
  });

  it.each([
    "sessionId",
    "clientId",
    "connectionId",
    "streamId",
    "writerFence",
    "writerLease",
    "snapshotId",
    "inputEpoch",
    "text",
    "paste",
    "key",
    "error",
  ])("rejects the Cloud diagnostic identity/content field %s", (field) => {
    expect(() =>
      TerminalTelemetryEventSchema.parse({
        schemaVersion: 2,
        monotonicAtMs: 32,
        clockKind: "workers-io",
        sampleWeight: 8,
        name: "cloud.input.forward",
        inputKind: "text",
        leaseOutcome: "active",
        outcome: "send-returned",
        observedQueueWaitMs: 3,
        observedIngressToLeaseDecisionMs: 4,
        observedIngressToSendDecisionMs: 5,
        frameBytesBucket: "65-1024",
        globalQueuedCount: 2,
        socketQueuedCount: 1,
        [field]: "must-not-leave-the-runtime",
      }),
    ).toThrow();
  });

  it.each(["text", "paste", "command", "cells", "capability", "sessionId"])(
    "rejects the extra content-bearing field %s",
    (field) => {
      expect(() =>
        TerminalTelemetryEventSchema.parse({
          schemaVersion: 1,
          monotonicAtMs: 10,
          name: "host.snapshot.capture",
          outcome: "ready",
          queueWaitMs: 1,
          actorPauseMs: 2,
          authorityEncodeExportMs: 1.5,
          ownershipCopyMs: 0.5,
          snapshotBytes: 128,
          [field]: "secret",
        }),
      ).toThrow();
    },
  );

  it.each(["text", "key", "clientId", "connectionId", "inputEpoch", "error"])(
    "rejects the input diagnostic identity/content field %s",
    (field) => {
      expect(() =>
        TerminalTelemetryEventSchema.parse({
          schemaVersion: 1,
          monotonicAtMs: 21,
          name: "host.input.apply",
          inputKind: "text",
          outcome: "written",
          effectStage: "completed",
          encodeKind: "utf8",
          ackSendOutcome: "send-returned",
          ackSendMs: 0.25,
          controlAdmissionMs: 1,
          controlQueueWaitMs: 2,
          controlQueueDepth: 1,
          actorQueueWaitMs: 2,
          actorProcessingMs: 1,
          hostIngressToAckDecisionMs: 7,
          inputEncodeMs: 0.5,
          ptyWriteAttempted: true,
          ptyWriteMs: 0.25,
          ptyBytesBucket: "1-8",
          [field]: "secret",
        }),
      ).toThrow();
    },
  );

  it("buckets input and control byte counts without retaining exact lengths", () => {
    expect([0, 1, 8, 9, 64, 65, 1_024, 1_025, 65_536, 65_537].map(telemetryByteSizeBucket)).toEqual(
      [
        "0",
        "1-8",
        "1-8",
        "9-64",
        "9-64",
        "65-1024",
        "65-1024",
        "1025-65536",
        "1025-65536",
        "65537+",
      ],
    );
    expect(() => telemetryByteSizeBucket(-1)).toThrow();
  });

  it("contains sink and schema failures", () => {
    const sink = vi.fn(() => {
      throw new Error("collector unavailable");
    });
    expect(() =>
      emitTelemetry(sink, {
        schemaVersion: 1,
        monotonicAtMs: 10,
        name: "host.snapshot.publish-total",
        source: "fresh",
        outcome: "ready",
        totalDurationMs: 4,
        snapshotBytes: 128,
      }),
    ).not.toThrow();
    expect(sink).toHaveBeenCalledOnce();
  });

  it("clamps a regressed local monotonic clock without crossing runtimes", () => {
    expect(elapsedMs(10, 8)).toBe(0);
    expect(elapsedMs(8, 10)).toBe(2);
  });

  it("defers collectors behind a bounded, drop-on-full queue", async () => {
    const scheduled: Array<() => void> = [];
    const target = vi.fn<TelemetrySink>();
    const buffered = createBufferedTelemetrySink(target, {
      maxPendingEvents: 2,
      schedule: (task) => scheduled.push(task),
    });
    const event = {
      schemaVersion: 1 as const,
      monotonicAtMs: 1,
      name: "host.snapshot.capture" as const,
      outcome: "failed" as const,
      totalDurationMs: 2,
    };

    buffered.sink(event);
    buffered.sink({ ...event, monotonicAtMs: 2 });
    buffered.sink({ ...event, monotonicAtMs: 3 });
    expect(target).not.toHaveBeenCalled();
    expect(buffered.pendingEvents).toBe(2);
    expect(buffered.droppedEvents).toBe(1);

    const flushed = buffered.flush();
    while (scheduled.length > 0) scheduled.shift()!();
    await flushed;
    expect(target).toHaveBeenCalledTimes(2);
    expect(buffered.pendingEvents).toBe(0);
  });

  it("validates only from the deferred drain and drops malformed diagnostics", async () => {
    const scheduled: Array<() => void> = [];
    const target = vi.fn<TelemetrySink>();
    const buffered = createBufferedTelemetrySink(target, {
      schedule: (task) => scheduled.push(task),
    });

    expect(() =>
      buffered.sink({
        schemaVersion: 1,
        monotonicAtMs: 1,
        name: "host.snapshot.capture",
        outcome: "not-a-real-outcome",
        totalDurationMs: 1,
      } as unknown as TerminalTelemetryEvent),
    ).not.toThrow();
    expect(buffered.pendingEvents).toBe(1);
    expect(target).not.toHaveBeenCalled();

    scheduled.shift()!();
    await buffered.flush();
    expect(buffered.pendingEvents).toBe(0);
    expect(target).not.toHaveBeenCalled();
  });

  it("contains synchronous and asynchronous deferred collector failures", async () => {
    const failures = [
      (() => {
        throw new Error("sync collector failure");
      }) as TelemetrySink,
      (() => Promise.reject(new Error("async collector failure"))) as unknown as TelemetrySink,
    ];
    for (const target of failures) {
      const buffered = createBufferedTelemetrySink(target, { schedule: (task) => task() });
      buffered.sink({
        schemaVersion: 1,
        monotonicAtMs: 1,
        name: "host.snapshot.capture",
        outcome: "failed",
        totalDurationMs: 1,
      });
      await expect(buffered.flush()).resolves.toBeUndefined();
    }
  });

  it("does not invoke a collector while delivery has paused diagnostics", async () => {
    const scheduled: Array<() => void> = [];
    const target = vi.fn<TelemetrySink>();
    const buffered = createBufferedTelemetrySink(target, {
      schedule: (task) => scheduled.push(task),
    });
    buffered.pause();
    buffered.sink({
      schemaVersion: 1,
      monotonicAtMs: 1,
      name: "host.snapshot.capture",
      outcome: "failed",
      totalDurationMs: 1,
    });

    expect(scheduled).toEqual([]);
    expect(target).not.toHaveBeenCalled();
    buffered.resume();
    scheduled.shift()!();
    await buffered.flush();

    expect(target).toHaveBeenCalledOnce();
  });
});

describe("sampled Browser input and presentation telemetry", () => {
  const inputAck = {
    schemaVersion: 2 as const,
    monotonicAtMs: 24,
    clockKind: "browser-performance" as const,
    sampleWeight: 64 as const,
    name: "browser.input.lifecycle" as const,
    outcome: "ack-received" as const,
    inputKind: "key" as const,
    status: "written" as const,
    dispatchToSendDecisionMs: 2,
    sendDecisionToAckMs: 9,
    dispatchToAckMs: 11,
    outstandingInputs: 3,
  };
  const inputTerminal = {
    schemaVersion: 2 as const,
    monotonicAtMs: 25,
    clockKind: "browser-performance" as const,
    sampleWeight: 64 as const,
    name: "browser.input.lifecycle" as const,
    outcome: "terminal" as const,
    inputKind: "paste" as const,
    reason: "transport-replaced" as const,
    stage: "awaiting-ack" as const,
    observedDurationMs: 10,
  };
  const canonicalReady = {
    schemaVersion: 2 as const,
    monotonicAtMs: 40,
    clockKind: "browser-performance" as const,
    sampleWeight: 64 as const,
    name: "browser.presentation.canonical" as const,
    outcome: "next-frame-opportunity" as const,
    frameKind: "pty-output" as const,
    frameBytesBucket: "65-1024" as const,
    ingressToReplicaApplyMs: 2,
    replicaApplyToRenderCommitMs: 3,
    renderCommitToFrameOpportunityMs: 4,
    totalDurationMs: 9,
  };
  const canonicalTerminal = {
    schemaVersion: 2 as const,
    monotonicAtMs: 41,
    clockKind: "browser-performance" as const,
    sampleWeight: 64 as const,
    name: "browser.presentation.canonical" as const,
    outcome: "not-observed" as const,
    frameKind: "resize-applied" as const,
    frameBytesBucket: "9-64" as const,
    reason: "page-hidden" as const,
    stage: "applied" as const,
    observedDurationMs: 7,
  };

  it.each([inputAck, inputTerminal, canonicalReady, canonicalTerminal])(
    "accepts the strict sampled Browser event $name/$outcome",
    (event) => {
      expect(BrowserTelemetryEventSchema.parse(event)).toEqual(event);
      expect(TerminalTelemetryEventSchema.parse(event)).toEqual(event);
    },
  );

  it.each([inputAck, inputTerminal, canonicalReady, canonicalTerminal])(
    "requires literal sampleWeight 64 for $name/$outcome",
    (event) => {
      expect(() => BrowserTelemetryEventSchema.parse({ ...event, sampleWeight: 1 })).toThrow();
      expect(() => BrowserTelemetryEventSchema.parse({ ...event, schemaVersion: 1 })).toThrow();
    },
  );

  it("bounds sampled outstanding input cardinality", () => {
    expect(BrowserTelemetryEventSchema.parse({ ...inputAck, outstandingInputs: 64 })).toEqual({
      ...inputAck,
      outstandingInputs: 64,
    });
    expect(() =>
      BrowserTelemetryEventSchema.parse({ ...inputAck, outstandingInputs: 65 }),
    ).toThrow();
  });

  it.each([
    "sessionId",
    "clientId",
    "connectionId",
    "inputEpoch",
    "clientInputSeq",
    "deliveryGeneration",
    "streamId",
    "key",
    "text",
    "paste",
    "command",
    "cells",
    "content",
    "payload",
    "capability",
    "url",
    "error",
    "hash",
    "paintMs",
    "paintAt",
  ])("rejects identity/content/url/error/hash/paint field %s", (field) => {
    for (const event of [inputAck, inputTerminal, canonicalReady, canonicalTerminal]) {
      expect(() =>
        BrowserTelemetryEventSchema.parse({
          ...event,
          [field]: "must-not-leave-the-browser",
        }),
      ).toThrow();
    }
  });
});
