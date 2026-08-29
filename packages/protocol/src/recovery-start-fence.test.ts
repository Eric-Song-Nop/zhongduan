import { describe, expect, it } from "vitest";

import { DataFrameKind, decodeDataFrame } from "./data-frame";
import {
  RecoveryStartFenceSchema,
  decodeRecoveryStartFence,
  encodeRecoveryStartFence,
} from "./recovery-start-fence";

const fence = {
  type: "recovery-start-fence",
  recoveryId: "recovery_AAAAAAAAAAA",
  connectionId: "connection_AAAAAAAAA",
  deliveryGeneration: "3",
  streamId: 42,
  engineId: "engine",
  base: { sessionEpoch: "7", eventSeq: "10", nextPtyOffset: "100" },
  source: { kind: "snapshot", snapshotId: "snapshot_AAAAAAAAAAA" },
  committedThrough: { sessionEpoch: "7", eventSeq: "12", nextPtyOffset: "120" },
  liveFloor: { sessionEpoch: "7", nextEventSeq: "13", nextPtyOffset: "120" },
} as const;

describe("RecoveryStartFence", () => {
  it("maps the committed cursor onto an ordered canonical data marker", () => {
    const encoded = encodeRecoveryStartFence(fence);
    const frame = decodeDataFrame(encoded);

    expect(frame).toMatchObject({
      kind: DataFrameKind.DeliveryBarrier,
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      eventSeq: 12n,
      ptyOffset: 120n,
      streamId: 42,
    });
    expect(decodeRecoveryStartFence(encoded)).toEqual(fence);
  });

  it("rejects a live floor that is not the exact successor of committed-through", () => {
    expect(() =>
      encodeRecoveryStartFence({
        ...fence,
        liveFloor: { ...fence.liveFloor, nextPtyOffset: "121" },
      }),
    ).toThrow();
  });

  it("rejects two PTY offsets for the same base and committed event", () => {
    expect(() =>
      encodeRecoveryStartFence({
        ...fence,
        base: { ...fence.committedThrough, nextPtyOffset: "119" },
      }),
    ).toThrow();
  });

  it("rejects unknown top-level fields", () => {
    expect(() => RecoveryStartFenceSchema.parse({ ...fence, unknown: true })).toThrow();
  });
});
