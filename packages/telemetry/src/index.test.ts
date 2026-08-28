import { describe, expect, it, vi } from "vitest";

import {
  TerminalTelemetryEventSchema,
  createBufferedTelemetrySink,
  elapsedMs,
  emitTelemetry,
  type TelemetrySink,
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
