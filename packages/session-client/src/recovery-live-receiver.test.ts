import {
  DELIVERY_ENVELOPE_V3_HEADER_BYTES,
  DataFrameKind,
  encodeDataFrame,
  encodeDeliveryEnvelopeV3,
  encodeResizePayload,
  type DeliveryLane,
  type DeliveryLaneCursor,
  type ReplicaApplied,
  type RecoverySourceClosed,
  type RecoveryStart,
  type ReplicaCursor,
  type ResizePayload,
} from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";

import { RecoveryAssembler, type RecoveryAssemblerCompletion } from "./recovery-assembler";
import { RecoveryLiveReceiver } from "./recovery-live-receiver";
import type { ReplicaSink } from "./types";

const DELIVERY_GENERATION = 9n;
const STREAM_ID = 23;
const SESSION_EPOCH = 7n;
const ENGINE_ID = "engine-recovery-live";
const RECOVERY_ID = "recovery_live_0001";

class FakeReplica implements ReplicaSink {
  readonly writes: number[][] = [];
  readonly resizes: ResizePayload[] = [];
  afterResize: (() => void) | null = null;
  afterWrite: (() => void) | null = null;
  disposeCalls = 0;
  throwAfterNextResize = false;
  throwAfterNextWrite = false;

  constructor(readonly engineId = ENGINE_ID) {}

  writePty(data: Uint8Array): void {
    this.writes.push([...data]);
    this.afterWrite?.();
    if (this.throwAfterNextWrite) {
      this.throwAfterNextWrite = false;
      throw new Error("injected replica write outcome uncertainty");
    }
  }

  resize(dimensions: ResizePayload): void {
    this.resizes.push({ ...dimensions });
    this.afterResize?.();
    if (this.throwAfterNextResize) {
      this.throwAfterNextResize = false;
      throw new Error("injected replica resize outcome uncertainty");
    }
  }

  dispose(): void {
    this.disposeCalls += 1;
  }
}

const SEED_LANE: DeliveryLaneCursor = {
  cumulativeEncodedBytes: 400n,
  deliveryGeneration: DELIVERY_GENERATION,
  deliveryOrdinal: 2n,
  lane: "live",
  streamId: STREAM_ID,
};

const SEED_APPLIED: ReplicaApplied = {
  type: "replica-applied",
  deliveryGeneration: DELIVERY_GENERATION.toString(),
  authorityCursor: {
    sessionEpoch: SESSION_EPOCH.toString(),
    eventSeq: "12",
    nextPtyOffset: "100",
  },
};

const SEED_COMPLETION: RecoveryAssemblerCompletion = {
  laneCursors: {
    live: SEED_LANE,
    recovery: {
      cumulativeEncodedBytes: 300n,
      deliveryGeneration: DELIVERY_GENERATION,
      deliveryOrdinal: 3n,
      lane: "recovery",
      streamId: STREAM_ID,
    },
  },
  recoveryAdopted: {
    type: "recovery-adopted",
    recoveryId: RECOVERY_ID,
    deliveryGeneration: DELIVERY_GENERATION.toString(),
    replicaApplied: { ...SEED_APPLIED.authorityCursor },
  },
  replicaApplied: SEED_APPLIED,
};

interface EnvelopeOptions {
  previousCumulativeEncodedBytes?: bigint;
  cumulativeEncodedBytes?: bigint;
  deliveryGeneration?: bigint;
  deliveryOrdinal: bigint;
  eventSeq: bigint;
  lane?: DeliveryLane;
  payload?: readonly number[];
  ptyOffset: bigint;
  resize?: ResizePayload;
  streamId?: number;
}

function envelope(options: EnvelopeOptions): Uint8Array {
  const payload = encodeDataFrame({
    kind: options.resize === undefined ? DataFrameKind.PtyOutput : DataFrameKind.ResizeApplied,
    flags: 0,
    sessionEpoch: SESSION_EPOCH,
    deliveryGeneration: 0n,
    eventSeq: options.eventSeq,
    ptyOffset: options.ptyOffset,
    streamId: 0,
    payload:
      options.resize === undefined
        ? Uint8Array.from(options.payload ?? [])
        : encodeResizePayload(options.resize),
  });
  const encodedBytes = BigInt(DELIVERY_ENVELOPE_V3_HEADER_BYTES + payload.byteLength);
  return encodeDeliveryEnvelopeV3({
    cumulativeEncodedBytes:
      options.cumulativeEncodedBytes ??
      (options.previousCumulativeEncodedBytes ?? SEED_LANE.cumulativeEncodedBytes) + encodedBytes,
    deliveryGeneration: options.deliveryGeneration ?? DELIVERY_GENERATION,
    deliveryOrdinal: options.deliveryOrdinal,
    lane: options.lane ?? "live",
    payload,
    streamId: options.streamId ?? STREAM_ID,
  });
}

function createReceiver(replica = new FakeReplica()): {
  receiver: RecoveryLiveReceiver;
  replica: FakeReplica;
} {
  return {
    receiver: new RecoveryLiveReceiver(SEED_COMPLETION, {
      engineId: ENGINE_ID,
      recoveryId: RECOVERY_ID,
      replica,
    }),
    replica,
  };
}

describe("RecoveryLiveReceiver", () => {
  it("applies contiguous live mutations immediately and reports independent scalar copies", () => {
    const { receiver, replica } = createReceiver();
    const pty = envelope({
      deliveryOrdinal: 3n,
      eventSeq: 13n,
      ptyOffset: 100n,
      payload: [0x61, 0x62],
    });

    expect(receiver.acceptEnvelope(pty)).toBe(true);
    expect(replica.writes).toEqual([[0x61, 0x62]]);
    expect(receiver.liveLaneCursor).toEqual({
      cumulativeEncodedBytes: BigInt(pty.byteLength) + SEED_LANE.cumulativeEncodedBytes,
      deliveryGeneration: DELIVERY_GENERATION,
      deliveryOrdinal: 3n,
      lane: "live",
      streamId: STREAM_ID,
    });
    expect(receiver.latestReceipt).toEqual({
      type: "delivery-received",
      deliveryGeneration: "9",
      lane: "live",
      contiguousDeliveryOrdinal: "3",
      cumulativeEncodedBytes: (BigInt(pty.byteLength) + 400n).toString(),
    });
    expect(receiver.replicaApplied).toEqual({
      type: "replica-applied",
      deliveryGeneration: "9",
      authorityCursor: { sessionEpoch: "7", eventSeq: "13", nextPtyOffset: "102" },
    });
    expect(receiver.targetTainted).toBe(false);

    const receipt = receiver.latestReceipt!;
    const applied = receiver.replicaApplied;
    receipt.contiguousDeliveryOrdinal = "999";
    applied.authorityCursor.eventSeq = "999";
    expect(receiver.latestReceipt?.contiguousDeliveryOrdinal).toBe("3");
    expect(receiver.replicaApplied.authorityCursor.eventSeq).toBe("13");

    const resize = { cols: 120, rows: 40, widthPx: 960, heightPx: 800 };
    const resized = envelope({
      cumulativeEncodedBytes: receiver.liveLaneCursor.cumulativeEncodedBytes + 104n,
      deliveryOrdinal: 4n,
      eventSeq: 14n,
      ptyOffset: 102n,
      resize,
    });
    expect(resized.byteLength).toBe(104);
    expect(receiver.acceptEnvelope(resized.slice().buffer as ArrayBuffer)).toBe(true);
    expect(replica.resizes).toEqual([resize]);
    expect(receiver.replicaApplied.authorityCursor).toEqual({
      sessionEpoch: "7",
      eventSeq: "14",
      nextPtyOffset: "102",
    });
  });

  it("copies the last accepted raw envelope and makes only its exact retry idempotent", () => {
    const { receiver, replica } = createReceiver();
    const accepted = envelope({
      deliveryOrdinal: 3n,
      eventSeq: 13n,
      ptyOffset: 100n,
      payload: [0x61],
    });
    const exactRetry = accepted.slice();

    expect(receiver.acceptEnvelope(accepted)).toBe(true);
    accepted.fill(0);
    expect(receiver.acceptEnvelope(exactRetry)).toBe(true);
    expect(replica.writes).toEqual([[0x61]]);

    const divergent = envelope({
      deliveryOrdinal: 3n,
      eventSeq: 13n,
      ptyOffset: 100n,
      payload: [0x62],
    });
    expect(receiver.acceptEnvelope(divergent)).toBe(false);
    expect(receiver.state).toBe("failed");
    expect(receiver.failure).toBe("protocol-conflict");
    expect(replica.writes).toEqual([[0x61]]);
  });

  it("fails closed on an unverifiable retry of the ordinal supplied only as a seed cursor", () => {
    const { receiver, replica } = createReceiver();
    const seedOrdinalRetry = envelope({
      cumulativeEncodedBytes: SEED_LANE.cumulativeEncodedBytes,
      deliveryOrdinal: SEED_LANE.deliveryOrdinal,
      eventSeq: 12n,
      ptyOffset: 100n,
      payload: [],
    });

    expect(receiver.acceptEnvelope(seedOrdinalRetry)).toBe(false);
    expect(receiver.state).toBe("failed");
    expect(receiver.failure).toBe("protocol-conflict");
    expect(replica.writes).toEqual([]);
  });

  it.each([
    [
      "cross-lane input",
      () =>
        envelope({
          deliveryOrdinal: 3n,
          eventSeq: 13n,
          lane: "recovery",
          ptyOffset: 100n,
          payload: [0x61],
        }),
    ],
    [
      "a different generation",
      () =>
        envelope({
          deliveryGeneration: DELIVERY_GENERATION + 1n,
          deliveryOrdinal: 3n,
          eventSeq: 13n,
          ptyOffset: 100n,
          payload: [0x61],
        }),
    ],
    [
      "a different stream",
      () =>
        envelope({
          deliveryOrdinal: 3n,
          eventSeq: 13n,
          ptyOffset: 100n,
          payload: [0x61],
          streamId: STREAM_ID + 1,
        }),
    ],
    [
      "an ordinal gap",
      () =>
        envelope({
          deliveryOrdinal: 4n,
          eventSeq: 13n,
          ptyOffset: 100n,
          payload: [0x61],
        }),
    ],
    [
      "a cumulative byte gap",
      () => {
        const valid = envelope({
          deliveryOrdinal: 3n,
          eventSeq: 13n,
          ptyOffset: 100n,
          payload: [0x61],
        });
        return envelope({
          cumulativeEncodedBytes: SEED_LANE.cumulativeEncodedBytes + BigInt(valid.byteLength) + 1n,
          deliveryOrdinal: 3n,
          eventSeq: 13n,
          ptyOffset: 100n,
          payload: [0x61],
        });
      },
    ],
    [
      "a canonical event gap",
      () =>
        envelope({
          deliveryOrdinal: 3n,
          eventSeq: 14n,
          ptyOffset: 100n,
          payload: [0x61],
        }),
    ],
    [
      "a canonical PTY offset gap",
      () =>
        envelope({
          deliveryOrdinal: 3n,
          eventSeq: 13n,
          ptyOffset: 101n,
          payload: [0x61],
        }),
    ],
  ])("rejects %s before touching the replica", (_label, makeEnvelope) => {
    const { receiver, replica } = createReceiver();

    expect(receiver.acceptEnvelope(makeEnvelope())).toBe(false);
    expect(receiver.state).toBe("failed");
    expect(receiver.failure).toBe("protocol-conflict");
    expect(replica.writes).toEqual([]);
    expect(replica.resizes).toEqual([]);
    expect(receiver.liveLaneCursor).toEqual(SEED_LANE);
    expect(receiver.replicaApplied).toEqual(SEED_APPLIED);
    expect(receiver.targetTainted).toBe(false);
  });

  it("does not apply twice when a sink synchronously reenters acceptEnvelope", () => {
    const { receiver, replica } = createReceiver();
    const next = envelope({
      deliveryOrdinal: 3n,
      eventSeq: 13n,
      ptyOffset: 100n,
      payload: [0x61],
    });
    let nestedResult: boolean | null = null;
    replica.afterWrite = () => {
      nestedResult = receiver.acceptEnvelope(next);
    };

    expect(receiver.acceptEnvelope(next)).toBe(true);
    expect(nestedResult).toBe(false);
    expect(replica.writes).toEqual([[0x61]]);
    expect(receiver.state).toBe("live");
    expect(receiver.liveLaneCursor.deliveryOrdinal).toBe(3n);
  });

  it("does not publish safe progress or success when the sink closes it synchronously", () => {
    const { receiver, replica } = createReceiver();
    const next = envelope({
      deliveryOrdinal: 3n,
      eventSeq: 13n,
      ptyOffset: 100n,
      payload: [0x61],
    });
    replica.afterWrite = () => receiver.close();

    expect(receiver.acceptEnvelope(next)).toBe(false);
    expect(replica.writes).toEqual([[0x61]]);
    expect(receiver.state).toBe("closed");
    expect(receiver.liveLaneCursor).toEqual(SEED_LANE);
    expect(receiver.replicaApplied).toEqual(SEED_APPLIED);
    expect(receiver.targetTainted).toBe(true);
  });

  it.each(["pty", "resize"] as const)(
    "taints the adopted target when a %s sink effects then throws without advancing safe progress",
    (kind) => {
      const replica = new FakeReplica();
      if (kind === "pty") replica.throwAfterNextWrite = true;
      else replica.throwAfterNextResize = true;
      const { receiver } = createReceiver(replica);
      const next = envelope({
        deliveryOrdinal: 3n,
        eventSeq: 13n,
        ptyOffset: 100n,
        ...(kind === "pty"
          ? { payload: [0x61] }
          : { resize: { cols: 120, rows: 40, widthPx: 960, heightPx: 800 } }),
      });

      expect(receiver.acceptEnvelope(next)).toBe(false);
      expect(receiver.state).toBe("failed");
      expect(receiver.failure).toBe("apply-outcome-uncertain");
      expect(receiver.targetTainted).toBe(true);
      expect(receiver.liveLaneCursor).toEqual(SEED_LANE);
      expect(receiver.replicaApplied).toEqual(SEED_APPLIED);
      expect(receiver.acceptEnvelope(next)).toBe(false);
      expect(replica.writes).toEqual(kind === "pty" ? [[0x61]] : []);
      expect(replica.resizes).toEqual(
        kind === "resize" ? [{ cols: 120, rows: 40, widthPx: 960, heightPx: 800 }] : [],
      );
    },
  );

  it("starts only from a real assembler completion bound to its adopted target", () => {
    const replica = new FakeReplica();
    const assembler = new RecoveryAssembler({
      deliveryGeneration: DELIVERY_GENERATION,
      engineId: ENGINE_ID,
      limits: {
        maxApplyFramesPerCall: 2,
        maxGapSpan: 4n,
        maxOwnedBytes: 4 * 1024,
        maxOwnedFrames: 8,
        noProgressDeadlineMs: 1_000,
        recoveryDeadlineMs: 10_000,
      },
      startedAtMs: 0,
      streamId: STREAM_ID,
      warm: { cursor: { ...SEED_APPLIED.authorityCursor }, replica },
    });
    const start: RecoveryStart = {
      type: "recovery-start",
      recoveryId: RECOVERY_ID,
      deliveryGeneration: DELIVERY_GENERATION.toString(),
      streamId: STREAM_ID,
      engineId: ENGINE_ID,
      authorityDataVersion: 2,
      base: { ...SEED_APPLIED.authorityCursor },
      source: { kind: "warm" },
      committedThrough: { ...SEED_APPLIED.authorityCursor },
      liveFloor: {
        sessionEpoch: SESSION_EPOCH.toString(),
        nextEventSeq: "13",
        nextPtyOffset: "100",
      },
    };
    const donePayload = encodeDataFrame({
      kind: DataFrameKind.ReplayCommit,
      flags: 0,
      sessionEpoch: SESSION_EPOCH,
      deliveryGeneration: 0n,
      eventSeq: 12n,
      ptyOffset: 100n,
      streamId: 0,
      payload: new Uint8Array(),
    });
    const doneCumulativeBytes = BigInt(DELIVERY_ENVELOPE_V3_HEADER_BYTES + donePayload.byteLength);
    const done = encodeDeliveryEnvelopeV3({
      cumulativeEncodedBytes: doneCumulativeBytes,
      deliveryGeneration: DELIVERY_GENERATION,
      deliveryOrdinal: 1n,
      lane: "recovery",
      payload: donePayload,
      streamId: STREAM_ID,
    });
    const handoffCursor: ReplicaCursor = {
      sessionEpoch: SESSION_EPOCH,
      deliveryGeneration: DELIVERY_GENERATION,
      lastEventSeq: 12n,
      nextPtyOffset: 100n,
    };
    const sourceClosed: RecoverySourceClosed = {
      type: "recovery-source-closed",
      recoveryId: RECOVERY_ID,
      deliveryGeneration: DELIVERY_GENERATION.toString(),
      throughRecoveryOrdinal: "1",
      throughRecoveryCumulativeEncodedBytes: doneCumulativeBytes.toString(),
    };

    expect(assembler.acceptStart(start, 0)).toBe(true);
    expect(assembler.acceptEnvelope(done, 1)).toBe(true);
    expect(assembler.confirmHandoff(handoffCursor, 2)).toBe(true);
    expect(assembler.acceptSourceClosed(sourceClosed, 3)).toBe(true);
    expect(assembler.state).toBe("complete");
    const completion = assembler.completion;
    expect(completion).not.toBeNull();

    const receiver = new RecoveryLiveReceiver(completion!, {
      engineId: ENGINE_ID,
      recoveryId: RECOVERY_ID,
      replica,
    });
    expect(receiver.liveLaneCursor.deliveryOrdinal).toBe(0n);
    expect(receiver.latestReceipt).toBeNull();
    const firstLive = envelope({
      previousCumulativeEncodedBytes: 0n,
      deliveryOrdinal: 1n,
      eventSeq: 13n,
      ptyOffset: 100n,
      payload: [0x61],
    });
    expect(receiver.acceptEnvelope(firstLive)).toBe(true);
    expect(replica.writes).toEqual([[0x61]]);
    expect(receiver.liveLaneCursor.deliveryOrdinal).toBe(1n);
  });

  it("validates one completion and its adopted target binding", () => {
    const replica = new FakeReplica();
    const target = { engineId: ENGINE_ID, recoveryId: RECOVERY_ID, replica };
    expect(
      () =>
        new RecoveryLiveReceiver(
          {
            ...SEED_COMPLETION,
            laneCursors: {
              ...SEED_COMPLETION.laneCursors,
              live: { ...SEED_LANE, lane: "recovery" },
            },
          },
          target,
        ),
    ).toThrow("live lane cursor does not match the receiver identity");
    expect(
      () =>
        new RecoveryLiveReceiver(
          {
            ...SEED_COMPLETION,
            replicaApplied: { ...SEED_APPLIED, deliveryGeneration: "10" },
          },
          target,
        ),
    ).toThrow("replica applied progress does not match the receiver generation");
    expect(
      () =>
        new RecoveryLiveReceiver(
          {
            ...SEED_COMPLETION,
            recoveryAdopted: {
              ...SEED_COMPLETION.recoveryAdopted,
              deliveryGeneration: "10",
            },
          },
          target,
        ),
    ).toThrow("recovery adopted progress does not match the receiver generation");
    expect(
      () =>
        new RecoveryLiveReceiver(
          {
            ...SEED_COMPLETION,
            recoveryAdopted: {
              ...SEED_COMPLETION.recoveryAdopted,
              replicaApplied: {
                ...SEED_COMPLETION.recoveryAdopted.replicaApplied,
                eventSeq: "13",
              },
            },
          },
          target,
        ),
    ).toThrow("replica applied progress precedes the adopted target");
    expect(
      () =>
        new RecoveryLiveReceiver(
          {
            ...SEED_COMPLETION,
            laneCursors: {
              ...SEED_COMPLETION.laneCursors,
              live: { ...SEED_LANE, cumulativeEncodedBytes: 1n },
            },
          },
          target,
        ),
    ).toThrow("live lane cursor is not valid scalar progress");
    expect(
      () =>
        new RecoveryLiveReceiver(
          {
            ...SEED_COMPLETION,
            laneCursors: {
              ...SEED_COMPLETION.laneCursors,
              recovery: {
                ...SEED_COMPLETION.laneCursors.recovery,
                deliveryGeneration: DELIVERY_GENERATION + 1n,
              },
            },
          },
          target,
        ),
    ).toThrow("recovery lane cursor does not match the receiver identity");
    expect(
      () =>
        new RecoveryLiveReceiver(SEED_COMPLETION, {
          ...target,
          engineId: `${ENGINE_ID}_other`,
        }),
    ).toThrow("adopted target engine does not match its replica");
    expect(
      () =>
        new RecoveryLiveReceiver(SEED_COMPLETION, {
          ...target,
          recoveryId: `${RECOVERY_ID}_other`,
        }),
    ).toThrow("adopted target does not match the assembler completion");
  });

  it("never owns or mutates the replica after close", () => {
    const replica = new FakeReplica();
    const { receiver } = createReceiver(replica);
    receiver.close();
    receiver.close();
    expect(
      receiver.acceptEnvelope(
        envelope({
          deliveryOrdinal: 3n,
          eventSeq: 13n,
          ptyOffset: 100n,
          payload: [0x61],
        }),
      ),
    ).toBe(false);
    expect(receiver.state).toBe("closed");
    expect(replica.writes).toEqual([]);
    expect(replica.disposeCalls).toBe(0);
  });
});
