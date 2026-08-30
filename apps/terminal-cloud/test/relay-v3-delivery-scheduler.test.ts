import { describe, expect, it } from "vitest";
import {
  RelayV3DeliveryRing,
  type RelayV3DeliveryRef,
  type RelayV3DeliveryRefIdentity,
} from "../src/worker/relay-v3-delivery-ring";
import {
  RelayV3DeliveryScheduler,
  type RelayV3DeliveryClass,
} from "../src/worker/relay-v3-delivery-scheduler";

const KIB = 1024;

function identity(
  flow: string,
  lane: "live" | "recovery",
  ordinal: number,
  encodedBytes: number,
): RelayV3DeliveryRefIdentity {
  return {
    recoveryId: `recovery-${flow}`,
    clientId: `client-${flow}`,
    connectionId: `connection-${flow}`,
    streamId: Number(flow.replace(/\D/g, "")) + 1,
    deliveryGeneration: "5",
    lane,
    deliveryOrdinal: String(ordinal),
    cumulativeEncodedBytes: String(ordinal * encodedBytes),
  };
}

function retain(
  ring: RelayV3DeliveryRing,
  flow: string,
  lane: "live" | "recovery",
  ordinal: number,
  bytes: number,
): RelayV3DeliveryRef {
  const payload = new Uint8Array(lane === "live" ? bytes - 40 : bytes);
  const retained =
    lane === "live"
      ? ring.retainLiveCanonical(payload, [identity(flow, lane, ordinal, bytes)], bytes)
      : ring.retainRecoveryEncoded(payload, identity(flow, lane, ordinal, bytes));
  if (!retained.ok) throw new Error(`failed to retain ${flow}/${ordinal}: ${retained.reason}`);
  return "refs" in retained ? retained.refs[0]! : retained.ref;
}

function schedulerRing(maxReferences = 512): RelayV3DeliveryRing {
  return new RelayV3DeliveryRing({
    maxPhysicalBytes: 16 * 1024 * KIB,
    maxPhysicalEntries: maxReferences,
    maxReferences,
  });
}

describe("RelayV3DeliveryScheduler", () => {
  it("serves the four classes in exact 4/2/2/1 weighted byte rounds", async () => {
    const ring = schedulerRing();
    const events: string[] = [];
    const scheduler = new RelayV3DeliveryScheduler({
      ring,
      yieldDataTurn: async (delayMs) => {
        expect(delayMs).toBe(0);
        events.push("control");
      },
      send: ({ deliveryClass, encodedBytes, payload }) => {
        expect(encodedBytes).toBe(64 * KIB);
        expect(payload.byteLength).toBe(deliveryClass.endsWith("live") ? 64 * KIB - 40 : 64 * KIB);
        events.push(deliveryClass);
        return "sent" as const;
      },
      onFailure: () => {
        throw new Error("unexpected delivery failure");
      },
    });
    const counts: Record<RelayV3DeliveryClass, number> = {
      "writer-live": 8,
      "observer-live": 4,
      "writer-recovery": 4,
      "observer-recovery": 2,
    };

    for (const [deliveryClass, count] of Object.entries(counts) as Array<
      [RelayV3DeliveryClass, number]
    >) {
      const lane = deliveryClass.endsWith("live") ? "live" : "recovery";
      for (let ordinal = 1; ordinal <= count; ordinal += 1) {
        expect(
          scheduler.enqueue({
            deliveryClass,
            ref: retain(ring, deliveryClass, lane, ordinal, 64 * KIB),
          }),
        ).toBe(true);
      }
    }

    await scheduler.whenIdle();
    const sends = events.filter((event) => event !== "control");
    expect(sends).toEqual([
      "writer-live",
      "writer-live",
      "writer-live",
      "writer-live",
      "observer-live",
      "observer-live",
      "writer-recovery",
      "writer-recovery",
      "observer-recovery",
      "writer-live",
      "writer-live",
      "writer-live",
      "writer-live",
      "observer-live",
      "observer-live",
      "writer-recovery",
      "writer-recovery",
      "observer-recovery",
    ]);
    expect(events).toHaveLength(sends.length * 2);
    for (let index = 0; index < events.length; index += 2) {
      expect(events[index]).toBe("control");
      expect(events[index + 1]).not.toBe("control");
    }
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("is byte-fair across 16 sustained independent flows with mixed record sizes", async () => {
    const ring = schedulerRing();
    const sends: string[] = [];
    const bytesByFlow = new Map<string, number>();
    const scheduler = new RelayV3DeliveryScheduler({
      ring,
      yieldDataTurn: async () => undefined,
      send: ({ identity: sentIdentity, payload }) => {
        const flow = sentIdentity.clientId.slice("client-".length);
        sends.push(flow);
        bytesByFlow.set(flow, (bytesByFlow.get(flow) ?? 0) + payload.byteLength);
        return "sent";
      },
      onFailure: () => {
        throw new Error("unexpected delivery failure");
      },
    });

    const expectedVisit: string[] = [];
    for (let flowIndex = 0; flowIndex < 16; flowIndex += 1) {
      const flow = `flow-${flowIndex}`;
      const bytes = flowIndex < 8 ? 16 * KIB : 64 * KIB;
      const records = flowIndex < 8 ? 16 : 4;
      expectedVisit.push(...Array(flowIndex < 8 ? 4 : 1).fill(flow));
      for (let ordinal = 1; ordinal <= records; ordinal += 1) {
        expect(
          scheduler.enqueue({
            deliveryClass: "observer-recovery",
            ref: retain(ring, flow, "recovery", ordinal, bytes),
          }),
        ).toBe(true);
      }
    }

    await scheduler.whenIdle();
    expect(sends).toEqual([...expectedVisit, ...expectedVisit, ...expectedVisit, ...expectedVisit]);
    expect([...bytesByFlow.values()]).toEqual(Array(16).fill(256 * KIB));
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("preserves 4/2/2/1 class weights for sustained 16 KiB records", async () => {
    const ring = schedulerRing();
    const sends: RelayV3DeliveryClass[] = [];
    const scheduler = new RelayV3DeliveryScheduler({
      ring,
      yieldDataTurn: async () => undefined,
      send: ({ deliveryClass }) => {
        sends.push(deliveryClass);
        return "sent";
      },
      onFailure: () => {
        throw new Error("unexpected delivery failure");
      },
    });
    const records: Record<RelayV3DeliveryClass, number> = {
      "writer-live": 32,
      "observer-live": 16,
      "writer-recovery": 16,
      "observer-recovery": 8,
    };

    for (const [deliveryClass, count] of Object.entries(records) as Array<
      [RelayV3DeliveryClass, number]
    >) {
      const lane = deliveryClass.endsWith("live") ? "live" : "recovery";
      for (let ordinal = 1; ordinal <= count; ordinal += 1) {
        expect(
          scheduler.enqueue({
            deliveryClass,
            ref: retain(ring, `small-${deliveryClass}`, lane, ordinal, 16 * KIB),
          }),
        ).toBe(true);
      }
    }

    await scheduler.whenIdle();
    const weightedRecordRound: RelayV3DeliveryClass[] = [
      ...Array<RelayV3DeliveryClass>(16).fill("writer-live"),
      ...Array<RelayV3DeliveryClass>(8).fill("observer-live"),
      ...Array<RelayV3DeliveryClass>(8).fill("writer-recovery"),
      ...Array<RelayV3DeliveryClass>(4).fill("observer-recovery"),
    ];
    expect(sends).toEqual([...weightedRecordRound, ...weightedRecordRound]);
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("cancels an exact generation while its yielded turn is pending", async () => {
    const ring = schedulerRing();
    let releaseTurn!: () => void;
    const turn = new Promise<void>((resolve) => {
      releaseTurn = resolve;
    });
    let sends = 0;
    const scheduler = new RelayV3DeliveryScheduler({
      ring,
      yieldDataTurn: () => turn,
      send: () => {
        sends += 1;
        return "sent";
      },
      onFailure: () => undefined,
    });
    const ref = retain(ring, "cancel", "recovery", 1, 64 * KIB);
    const exact = ring.identity(ref)!;

    expect(scheduler.enqueue({ deliveryClass: "observer-recovery", ref })).toBe(true);
    expect(
      scheduler.forgetGeneration({
        recoveryId: exact.recoveryId,
        clientId: exact.clientId,
        connectionId: exact.connectionId,
        streamId: exact.streamId,
        deliveryGeneration: exact.deliveryGeneration,
      }),
    ).toBe(1);
    releaseTurn();
    await scheduler.whenIdle();

    expect(sends).toBe(0);
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("fences a fatal exact generation while unrelated jobs continue without retry or reorder", async () => {
    const ring = schedulerRing();
    const sentJobs: string[] = [];
    const failedJobs: string[] = [];
    const scheduler = new RelayV3DeliveryScheduler({
      ring,
      yieldDataTurn: async () => undefined,
      send: ({ identity: sentIdentity }) => {
        const job = `${sentIdentity.clientId.slice("client-".length)}:${sentIdentity.deliveryOrdinal}`;
        sentJobs.push(job);
        if (sentIdentity.clientId === "client-outcomes") return "fatal";
        return sentIdentity.clientId === "client-stale" ? "stale" : "sent";
      },
      onFailure: ({ identity: failedIdentity }) => {
        failedJobs.push(
          `${failedIdentity.clientId.slice("client-".length)}:${failedIdentity.deliveryOrdinal}`,
        );
      },
    });

    for (let ordinal = 1; ordinal <= 3; ordinal += 1) {
      expect(
        scheduler.enqueue({
          deliveryClass: "observer-recovery",
          ref: retain(ring, "outcomes", "recovery", ordinal, 16 * KIB),
        }),
      ).toBe(true);
    }
    expect(
      scheduler.enqueue({
        deliveryClass: "observer-recovery",
        ref: retain(ring, "unrelated", "recovery", 1, 16 * KIB),
      }),
    ).toBe(true);
    expect(
      scheduler.enqueue({
        deliveryClass: "observer-recovery",
        ref: retain(ring, "stale", "recovery", 1, 16 * KIB),
      }),
    ).toBe(true);
    await scheduler.whenIdle();

    expect(sentJobs).toEqual(["outcomes:1", "unrelated:1", "stale:1"]);
    expect(failedJobs).toEqual(["outcomes:1"]);
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("treats stale as a single-record cancellation and preserves lane FIFO", async () => {
    const ring = schedulerRing();
    const sentOrdinals: string[] = [];
    let failures = 0;
    const scheduler = new RelayV3DeliveryScheduler({
      ring,
      yieldDataTurn: async () => undefined,
      send: ({ identity: sentIdentity }) => {
        sentOrdinals.push(sentIdentity.deliveryOrdinal);
        return sentIdentity.deliveryOrdinal === "1" ? "stale" : "sent";
      },
      onFailure: () => {
        failures += 1;
      },
    });

    for (let ordinal = 1; ordinal <= 3; ordinal += 1) {
      expect(
        scheduler.enqueue({
          deliveryClass: "observer-recovery",
          ref: retain(ring, "stale-fifo", "recovery", ordinal, 16 * KIB),
        }),
      ).toBe(true);
    }
    await scheduler.whenIdle();

    expect(sentOrdinals).toEqual(["1", "2", "3"]);
    expect(failures).toBe(0);
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("awaits and contains an asynchronous failure callback before unrelated delivery", async () => {
    const ring = schedulerRing();
    const sentFlows: string[] = [];
    let markFailureStarted!: () => void;
    const failureStarted = new Promise<void>((resolve) => {
      markFailureStarted = resolve;
    });
    let releaseFailure!: () => void;
    const failureGate = new Promise<void>((resolve) => {
      releaseFailure = resolve;
    });
    const scheduler = new RelayV3DeliveryScheduler({
      ring,
      yieldDataTurn: async () => undefined,
      send: ({ identity: sentIdentity }) => {
        const flow = sentIdentity.clientId.slice("client-".length);
        sentFlows.push(flow);
        return flow === "async-fatal" ? "fatal" : "sent";
      },
      onFailure: async (job) => {
        expect(ring.identity(job.ref)).toBeUndefined();
        markFailureStarted();
        await failureGate;
        throw new Error("contained asynchronous failure callback");
      },
    });
    expect(
      scheduler.enqueue({
        deliveryClass: "observer-recovery",
        ref: retain(ring, "async-fatal", "recovery", 1, 16 * KIB),
      }),
    ).toBe(true);
    expect(
      scheduler.enqueue({
        deliveryClass: "observer-recovery",
        ref: retain(ring, "after-async-fatal", "recovery", 1, 16 * KIB),
      }),
    ).toBe(true);

    await failureStarted;
    for (let turn = 0; turn < 32; turn += 1) await Promise.resolve();
    expect(sentFlows).toEqual(["async-fatal"]);

    releaseFailure();
    await scheduler.whenIdle();
    expect(sentFlows).toEqual(["async-fatal", "after-async-fatal"]);
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("re-fences exact-generation work admitted by a yielding failure callback", async () => {
    const ring = schedulerRing();
    const sentJobs: string[] = [];
    let reenteredRef: RelayV3DeliveryRef | undefined;
    let scheduler!: RelayV3DeliveryScheduler;
    scheduler = new RelayV3DeliveryScheduler({
      ring,
      yieldDataTurn: async () => undefined,
      send: ({ identity: sentIdentity }) => {
        const job = `${sentIdentity.clientId.slice("client-".length)}:${sentIdentity.deliveryOrdinal}`;
        sentJobs.push(job);
        return sentIdentity.clientId === "client-reentrant-fatal" ? "fatal" : "sent";
      },
      onFailure: async ({ identity: failedIdentity }) => {
        reenteredRef = retain(ring, "reentrant-fatal", "recovery", 3, 16 * KIB);
        expect(scheduler.enqueue({ deliveryClass: "observer-recovery", ref: reenteredRef })).toBe(
          true,
        );
        await Promise.resolve();
        expect(ring.identity(reenteredRef)).toEqual({
          ...failedIdentity,
          deliveryOrdinal: "3",
          cumulativeEncodedBytes: String(3 * 16 * KIB),
        });
      },
    });

    for (let ordinal = 1; ordinal <= 2; ordinal += 1) {
      expect(
        scheduler.enqueue({
          deliveryClass: "observer-recovery",
          ref: retain(ring, "reentrant-fatal", "recovery", ordinal, 16 * KIB),
        }),
      ).toBe(true);
    }
    expect(
      scheduler.enqueue({
        deliveryClass: "observer-recovery",
        ref: retain(ring, "after-reentrant-fatal", "recovery", 1, 16 * KIB),
      }),
    ).toBe(true);

    await scheduler.whenIdle();
    expect(sentJobs).toEqual(["reentrant-fatal:1", "after-reentrant-fatal:1"]);
    expect(reenteredRef).toBeDefined();
    expect(ring.identity(reenteredRef!)).toBeUndefined();
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("dispose releases scheduler-owned refs without taking unrelated ring ownership", async () => {
    const ring = schedulerRing();
    const ringOwnedRef = retain(ring, "ring-owner", "recovery", 1, 16 * KIB);
    const scheduledRef = retain(ring, "scheduler-owner", "recovery", 1, 16 * KIB);
    const scheduler = new RelayV3DeliveryScheduler({
      ring,
      yieldDataTurn: async () => undefined,
      send: () => {
        throw new Error("disposed delivery must not send");
      },
      onFailure: () => {
        throw new Error("disposed delivery must not fail");
      },
    });

    expect(scheduler.enqueue({ deliveryClass: "observer-recovery", ref: scheduledRef })).toBe(true);
    expect(scheduler.dispose()).toBe(1);
    await scheduler.whenIdle();

    expect(ring.identity(scheduledRef)).toBeUndefined();
    expect(ring.identity(ringOwnedRef)).toBeDefined();
    expect(ring.usage).toEqual({
      physicalBytes: 16 * KIB,
      physicalEntries: 1,
      references: 1,
    });
    expect(ring.cancel(ringOwnedRef)).toBe(true);
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });

  it("never overlaps send callbacks and dispose cancels every queued turn", async () => {
    const ring = schedulerRing();
    let active = 0;
    let maxActive = 0;
    let firstSend = true;
    let markFirstStarted!: () => void;
    const firstStarted = new Promise<void>((resolve) => {
      markFirstStarted = resolve;
    });
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const scheduler = new RelayV3DeliveryScheduler({
      ring,
      yieldDataTurn: async () => undefined,
      send: async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        if (firstSend) {
          firstSend = false;
          markFirstStarted();
          await firstGate;
        }
        active -= 1;
        return "sent" as const;
      },
      onFailure: () => undefined,
    });
    for (let ordinal = 1; ordinal <= 4; ordinal += 1) {
      expect(
        scheduler.enqueue({
          deliveryClass: "observer-recovery",
          ref: retain(ring, "dispose", "recovery", ordinal, 16 * KIB),
        }),
      ).toBe(true);
    }

    await firstStarted;
    expect(scheduler.dispose()).toBe(3);
    releaseFirst();
    await scheduler.whenIdle();

    expect(maxActive).toBe(1);
    expect(ring.usage).toEqual({ physicalBytes: 0, physicalEntries: 0, references: 0 });
  });
});
