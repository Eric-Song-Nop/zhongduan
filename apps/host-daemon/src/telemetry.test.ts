import { describe, expect, it, vi } from "vitest";

import { createNdjsonTelemetrySink, telemetrySinkForTarget } from "./telemetry";

describe("Host telemetry", () => {
  it("writes one bounded NDJSON envelope without content-bearing fields", () => {
    const lines: string[] = [];
    const sink = createNdjsonTelemetrySink({
      once: vi.fn(),
      write(line) {
        lines.push(line);
        return true;
      },
    });

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
    const failing = createNdjsonTelemetrySink({
      once: vi.fn(),
      write() {
        throw new Error("stderr unavailable");
      },
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

    const writer = { once: vi.fn(), write: vi.fn(() => true) };
    expect(telemetrySinkForTarget(undefined, writer)).toBeUndefined();
    expect(telemetrySinkForTarget("stderr", writer)).toBeTypeOf("function");
    expect(() => telemetrySinkForTarget("network", writer)).toThrow("must be unset");
  });

  it("drops while stderr is backpressured and resumes only after drain", () => {
    let drain: (() => void) | undefined;
    const write = vi.fn(() => false);
    const sink = createNdjsonTelemetrySink({
      once(event, listener) {
        if (event === "drain") drain = listener;
      },
      write,
    });
    const event = {
      schemaVersion: 1 as const,
      monotonicAtMs: 1,
      name: "host.snapshot.capture" as const,
      outcome: "failed" as const,
      totalDurationMs: 1,
    };

    sink(event);
    sink(event);
    expect(write).toHaveBeenCalledOnce();
    write.mockReturnValue(true);
    drain?.();
    sink(event);
    expect(write).toHaveBeenCalledTimes(2);
  });

  it("disables diagnostics after an asynchronous writer error", () => {
    let fail: (() => void) | undefined;
    const write = vi.fn(() => true);
    const sink = createNdjsonTelemetrySink({
      once(event, listener) {
        if (event === "error") fail = listener;
      },
      write,
    });
    const event = {
      schemaVersion: 1 as const,
      monotonicAtMs: 1,
      name: "host.snapshot.capture" as const,
      outcome: "failed" as const,
      totalDurationMs: 1,
    };

    sink(event);
    fail?.();
    sink(event);

    expect(write).toHaveBeenCalledOnce();
  });
});
