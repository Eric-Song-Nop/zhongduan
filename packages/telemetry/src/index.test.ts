import { describe, expect, it, vi } from "vitest";

import {
  TerminalTelemetryEventSchema,
  createBufferedTelemetrySink,
  elapsedMs,
  emitTelemetry,
  telemetryByteSizeBucket,
  type TelemetrySink,
  type TerminalTelemetryEvent,
} from "./index";

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
