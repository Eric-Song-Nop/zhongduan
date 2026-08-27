import type { RelayToHostControlFrame } from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";

import { DeliveryRecoveryQueue, type AttachRequest } from "./delivery-recovery-queue";

function attach(streamId: number, deliveryGeneration = "1"): AttachRequest {
  return {
    type: "attach-request",
    connectionId: `connection_${String(streamId).padStart(16, "A")}`,
    streamId,
    deliveryGeneration,
    engineId: "ghostty/test",
    hasLiveReplica: false,
  } as Extract<RelayToHostControlFrame, { type: "attach-request" }>;
}

function queue(now: () => number): DeliveryRecoveryQueue {
  return new DeliveryRecoveryQueue({ maxRetryMs: 5_000, monotonicNow: now, quietMs: 250 });
}

describe("DeliveryRecoveryQueue", () => {
  it("lets a warm candidate pass a different delivery's cold deadline", () => {
    let now = 0;
    const recoveries = queue(() => now);
    const cold = attach(1);
    const warm = attach(2);
    recoveries.enqueue(cold);
    recoveries.markCold(cold);
    recoveries.defer(cold, 1_000);
    recoveries.enqueue(warm);

    expect(
      recoveries.takeRunnablePass(
        (request) => !recoveries.isCold(request) || recoveries.readyAt(request, 0) <= now,
      ),
    ).toEqual([warm]);
    expect(recoveries.pendingSize).toBe(1);

    now = 1_000;
    expect(recoveries.takeRunnablePass(() => true)).toEqual([cold]);
  });

  it("extends only a deferred cold capture through the trailing output quiet period", () => {
    let now = 100;
    const recoveries = queue(() => now);
    const request = attach(1);
    recoveries.defer(request, 250);

    expect(recoveries.readyAt(request, 300)).toBe(550);
    now = 550;
    expect(recoveries.readyAt(request, 300)).toBe(now);
  });

  it("applies per-delivery exponential failure backoff with a hard cap", () => {
    let now = 0;
    const recoveries = queue(() => now);
    const first = attach(1);
    const second = attach(2);

    expect(recoveries.deferAfterFailure(first, 0)).toBe(250);
    now = 250;
    expect(recoveries.deferAfterFailure(first, 0)).toBe(500);
    expect(recoveries.deferAfterFailure(second, 2_000)).toBe(2_000);
    for (let index = 0; index < 10; index += 1) {
      expect(recoveries.deferAfterFailure(first, 0)).toBeLessThanOrEqual(5_000);
    }
  });

  it("supersedes old generation state without duplicating a stream in the runnable order", () => {
    const recoveries = queue(() => 0);
    const old = attach(1, "1");
    const current = attach(1, "2");
    recoveries.enqueue(old);
    recoveries.markCold(old);

    expect(recoveries.enqueue(current)).toContain("1:1");
    expect(recoveries.takeRunnablePass(() => true)).toEqual([current]);
    expect(recoveries.pendingSize).toBe(0);
  });

  it("does not let a stale reset remove a newer pending generation", () => {
    const recoveries = queue(() => 0);
    const current = attach(1, "2");
    recoveries.enqueue(current);

    expect(recoveries.reset(1, "1")).toEqual([]);
    expect(recoveries.takeRunnablePass(() => true)).toEqual([current]);
  });
});
