import { describe, expect, it, vi } from "vitest";

import { createNdjsonTelemetrySink, telemetrySinkForTarget } from "./telemetry";

describe("Host telemetry", () => {
  it("writes one bounded NDJSON envelope without content-bearing fields", () => {
    const lines: string[] = [];
    const sink = createNdjsonTelemetrySink((line) => lines.push(line));

    sink({
      schemaVersion: 1,
      monotonicAtMs: 10,
      name: "host.journal.range",
      mode: "warm",
      status: "exact",
      deliveryCreditBytes: 65,
      encodedBytes: 49,
      frames: 1,
      oldestMutationAgeMs: 3,
    });

    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0]!)).toEqual({
      type: "zhongduan.telemetry",
      schemaVersion: 1,
      monotonicAtMs: 10,
      name: "host.journal.range",
      mode: "warm",
      status: "exact",
      deliveryCreditBytes: 65,
      encodedBytes: 49,
      frames: 1,
      oldestMutationAgeMs: 3,
    });
  });

  it("contains writer failures and only enables the explicit stderr target", () => {
    const failing = createNdjsonTelemetrySink(() => {
      throw new Error("stderr unavailable");
    });
    expect(() =>
      failing({
        schemaVersion: 1,
        monotonicAtMs: 1,
        name: "host.snapshot.capture",
        outcome: "failed",
        totalDurationMs: 1,
      }),
    ).not.toThrow();

    const write = vi.fn();
    expect(telemetrySinkForTarget(undefined, write)).toBeUndefined();
    expect(telemetrySinkForTarget("stderr", write)).toBeTypeOf("function");
    expect(() => telemetrySinkForTarget("network", write)).toThrow("must be unset");
  });
});
