import { describe, expect, it, vi } from "vitest";

import { TerminalTelemetryEventSchema, elapsedMs, emitTelemetry } from "./index";

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
});
