import type { CloudTelemetryEvent } from "@zhongduan/telemetry";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  CLOUD_TELEMETRY_RECORD_TYPE,
  CLOUD_TELEMETRY_RUNTIME,
  createCloudTelemetry,
} from "../src/worker/cloud-telemetry";

function inputForwardEvent(monotonicAtMs = 1): CloudTelemetryEvent {
  return {
    schemaVersion: 1,
    monotonicAtMs,
    clockKind: "workers-io",
    sampleWeight: 8,
    name: "cloud.input.forward",
    inputKind: "key",
    leaseOutcome: "active",
    outcome: "send-returned",
    observedQueueWaitMs: 0,
    observedIngressToLeaseDecisionMs: 0,
    observedIngressToSendDecisionMs: 0,
    frameBytesBucket: "65-1024",
    globalQueuedCount: 1,
    socketQueuedCount: 1,
  };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("Cloud telemetry", () => {
  it("defers a strict structured record to console.info by default", async () => {
    const info = vi.spyOn(console, "info").mockImplementation(() => undefined);
    const telemetry = createCloudTelemetry();

    telemetry.record(inputForwardEvent());
    expect(info).not.toHaveBeenCalled();
    await Promise.resolve();

    expect(info).toHaveBeenCalledOnce();
    expect(info).toHaveBeenCalledWith({
      type: CLOUD_TELEMETRY_RECORD_TYPE,
      runtime: CLOUD_TELEMETRY_RUNTIME,
      ...inputForwardEvent(),
    });
  });

  it("bounds pending diagnostics and drops on full without invoking the collector inline", () => {
    const scheduled: Array<() => void> = [];
    const collector = vi.fn();
    const telemetry = createCloudTelemetry({
      collector,
      maxPendingEvents: 2,
      schedule: (task) => scheduled.push(task),
    });

    telemetry.record(inputForwardEvent(1));
    telemetry.record(inputForwardEvent(2));
    telemetry.record(inputForwardEvent(3));
    expect(collector).not.toHaveBeenCalled();
    expect(telemetry.pendingEvents).toBe(2);
    expect(telemetry.droppedEvents).toBe(1);

    while (scheduled.length > 0) scheduled.shift()!();
    expect(collector).toHaveBeenCalledTimes(2);
    expect(telemetry.pendingEvents).toBe(0);
  });

  it("drops malformed diagnostics only from the deferred drain", () => {
    const scheduled: Array<() => void> = [];
    const collector = vi.fn();
    const telemetry = createCloudTelemetry({
      collector,
      schedule: (task) => scheduled.push(task),
    });
    const malformed = {
      ...inputForwardEvent(),
      writerLease: "must-not-enter-workers-logs",
    } as unknown as CloudTelemetryEvent;

    expect(() => telemetry.record(malformed)).not.toThrow();
    expect(telemetry.pendingEvents).toBe(1);
    scheduled.shift()!();

    expect(collector).not.toHaveBeenCalled();
    expect(telemetry.pendingEvents).toBe(0);
  });

  it("drops non-Cloud terminal diagnostics at the deferred sink boundary", () => {
    const scheduled: Array<() => void> = [];
    const collector = vi.fn();
    const telemetry = createCloudTelemetry({
      collector,
      schedule: (task) => scheduled.push(task),
    });
    const hostDiagnostic = {
      schemaVersion: 1,
      monotonicAtMs: 1,
      name: "host.snapshot.capture",
      outcome: "ready",
      queueWaitMs: 0,
      actorPauseMs: 1,
      authorityEncodeExportMs: 1,
      ownershipCopyMs: 0,
      snapshotBytes: 128,
    } as unknown as CloudTelemetryEvent;

    expect(() => telemetry.record(hostDiagnostic)).not.toThrow();
    expect(telemetry.pendingEvents).toBe(1);
    scheduled.shift()!();

    expect(collector).not.toHaveBeenCalled();
    expect(telemetry.pendingEvents).toBe(0);
  });

  it("contains synchronous and rejected asynchronous collectors", async () => {
    const collectors = [
      vi.fn(() => {
        throw new Error("collector failed");
      }),
      vi.fn(() => Promise.reject(new Error("collector rejected"))),
    ];

    for (const collector of collectors) {
      const telemetry = createCloudTelemetry({ collector, schedule: (task) => task() });
      expect(() => telemetry.record(inputForwardEvent())).not.toThrow();
      await Promise.resolve();
      expect(collector).toHaveBeenCalledOnce();
      expect(telemetry.pendingEvents).toBe(0);
    }
  });

  it("contains scheduler failure and accounts for the dropped diagnostic", () => {
    const collector = vi.fn();
    const telemetry = createCloudTelemetry({
      collector,
      schedule: () => {
        throw new Error("request context ended");
      },
    });

    expect(() => telemetry.record(inputForwardEvent())).not.toThrow();
    expect(collector).not.toHaveBeenCalled();
    expect(telemetry.pendingEvents).toBe(0);
    expect(telemetry.droppedEvents).toBe(1);
  });
});
