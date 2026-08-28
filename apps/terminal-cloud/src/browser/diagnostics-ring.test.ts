import type { BrowserTelemetryEvent } from "@zhongduan/telemetry";
import { describe, expect, it } from "vitest";

import { createBrowserDiagnostics } from "./diagnostics-ring";

function relayRtt(monotonicAtMs: number): BrowserTelemetryEvent {
  return {
    schemaVersion: 1,
    monotonicAtMs,
    clockKind: "browser-performance",
    name: "browser.relay.rtt",
    channel: "control",
    outcome: "success",
    durationMs: 4,
    outstandingPings: 0,
  };
}

describe("Browser diagnostics ring", () => {
  it("defers strict validation behind a bounded pending queue", async () => {
    const scheduled: Array<() => void> = [];
    const diagnostics = createBrowserDiagnostics({
      maxPendingEvents: 2,
      capacity: 4,
      schedule: (task) => scheduled.push(task),
    });

    diagnostics.record(relayRtt(1));
    diagnostics.record(relayRtt(2));
    diagnostics.record(relayRtt(3));

    expect(diagnostics.snapshot()).toMatchObject({
      droppedEvents: 1,
      events: [],
      pendingEvents: 2,
    });
    expect(diagnostics.pendingEvents).toBe(2);
    expect(diagnostics.droppedEvents).toBe(1);

    const flushed = diagnostics.flush();
    scheduled.shift()!();
    await flushed;

    expect(diagnostics.pendingEvents).toBe(0);
    expect(diagnostics.snapshot().events.map((event) => event.monotonicAtMs)).toEqual([1, 2]);
  });

  it("drops malformed records only from the deferred drain", async () => {
    const scheduled: Array<() => void> = [];
    const diagnostics = createBrowserDiagnostics({
      schedule: (task) => scheduled.push(task),
    });
    const malformed = {
      ...relayRtt(1),
      sessionId: "must-not-be-retained",
    } as unknown as BrowserTelemetryEvent;

    expect(() => diagnostics.record(malformed)).not.toThrow();
    diagnostics.record(relayRtt(2));
    expect(diagnostics.droppedEvents).toBe(0);
    expect(diagnostics.retainedEvents).toBe(0);

    const flushed = diagnostics.flush();
    scheduled.shift()!();
    await flushed;

    expect(diagnostics.droppedEvents).toBe(1);
    expect(diagnostics.snapshot().events).toEqual([relayRtt(2)]);
  });

  it("overwrites the oldest entry in O(1) ring order and returns copies", async () => {
    const diagnostics = createBrowserDiagnostics({
      capacity: 2,
      schedule: (task) => task(),
    });

    diagnostics.record(relayRtt(1));
    diagnostics.record(relayRtt(2));
    diagnostics.record(relayRtt(3));
    await diagnostics.flush();

    expect(diagnostics.retainedEvents).toBe(2);
    expect(diagnostics.droppedEvents).toBe(1);
    const snapshot = diagnostics.snapshot();
    expect(snapshot.events.map((event) => event.monotonicAtMs)).toEqual([2, 3]);
    (snapshot.events[0] as BrowserTelemetryEvent).monotonicAtMs = 99;
    expect(diagnostics.snapshot().events.map((event) => event.monotonicAtMs)).toEqual([2, 3]);
  });

  it("returns an event-time ordered snapshot when deferred producers complete out of order", async () => {
    const diagnostics = createBrowserDiagnostics({ schedule: (task) => task() });

    diagnostics.record(relayRtt(30));
    diagnostics.record(relayRtt(10));
    diagnostics.record(relayRtt(20));

    expect(diagnostics.snapshot().events.map((event) => event.monotonicAtMs)).toEqual([10, 20, 30]);
  });

  it("shares one bounded flush waiter while a drain is pending", async () => {
    const scheduled: Array<() => void> = [];
    const diagnostics = createBrowserDiagnostics({ schedule: (task) => scheduled.push(task) });
    diagnostics.record(relayRtt(1));

    const first = diagnostics.flush();
    const second = diagnostics.flush();
    expect(second).toBe(first);
    scheduled.shift()!();
    await Promise.all([first, second]);
  });

  it("clears retained and not-yet-drained diagnostics without resetting drop history", async () => {
    const scheduled: Array<() => void> = [];
    const diagnostics = createBrowserDiagnostics({
      maxPendingEvents: 1,
      capacity: 1,
      schedule: (task) => scheduled.push(task),
    });

    diagnostics.record(relayRtt(1));
    diagnostics.record(relayRtt(2));
    diagnostics.clear();
    scheduled.shift()!();
    await diagnostics.flush();

    expect(diagnostics.pendingEvents).toBe(0);
    expect(diagnostics.retainedEvents).toBe(0);
    expect(diagnostics.snapshot().events).toEqual([]);
    expect(diagnostics.droppedEvents).toBe(1);
  });

  it("contains scheduler failures and makes record non-throwing", async () => {
    const diagnostics = createBrowserDiagnostics({
      schedule: () => {
        throw new Error("scheduler unavailable");
      },
    });

    expect(() => diagnostics.record(relayRtt(1))).not.toThrow();
    expect(diagnostics.pendingEvents).toBe(0);
    expect(diagnostics.retainedEvents).toBe(0);
    expect(diagnostics.droppedEvents).toBe(1);
    await expect(diagnostics.flush()).resolves.toBeUndefined();
  });

  it.each([0, -1, Number.NaN, 257, Number.MAX_SAFE_INTEGER])(
    "rejects an invalid ring capacity %s",
    (capacity) => {
      expect(() => createBrowserDiagnostics({ capacity })).toThrow(RangeError);
    },
  );
});
