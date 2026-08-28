import { DataFrameKind, type DataFrame } from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";
import {
  MAX_UNACKED_BYTES,
  advanceDeliveryCursor,
  type DeliveryCursorState,
} from "../src/worker/relay-delivery";

const baseline: DeliveryCursorState = {
  dataState: "catching-up",
  firstEventSeq: "0",
  ackedEventSeq: "0",
  sentEventSeq: "0",
  firstPtyOffset: "0",
  ackedPtyOffset: "0",
  sentPtyOffset: "0",
};

function frame(overrides: Partial<DataFrame> = {}): DataFrame {
  return {
    kind: DataFrameKind.PtyOutput,
    flags: 0,
    sessionEpoch: 7n,
    deliveryGeneration: 1n,
    eventSeq: 1n,
    ptyOffset: 0n,
    streamId: 1,
    payload: new Uint8Array([1, 2, 3]),
    ...overrides,
  };
}

describe("delivery cursor validation", () => {
  it("requires an explicit baseline and a zero-tail replay commit", () => {
    expect(
      advanceDeliveryCursor(
        {
          ...baseline,
          ackedEventSeq: null,
          ackedPtyOffset: null,
          sentEventSeq: null,
          sentPtyOffset: null,
        },
        frame({ kind: DataFrameKind.ReplayCommit, eventSeq: 0n, payload: new Uint8Array() }),
      ),
    ).toEqual({ kind: "sequence-error" });

    expect(
      advanceDeliveryCursor(
        baseline,
        frame({ kind: DataFrameKind.ReplayCommit, eventSeq: 0n, payload: new Uint8Array() }),
      ),
    ).toMatchObject({ kind: "ok", nextState: { dataState: "synced" } });
  });

  it("advances strict event and PTY cursors", () => {
    expect(advanceDeliveryCursor(baseline, frame())).toMatchObject({
      kind: "ok",
      nextState: { sentEventSeq: "1", sentPtyOffset: "3" },
    });
    expect(advanceDeliveryCursor(baseline, frame({ eventSeq: 2n }))).toEqual({
      kind: "sequence-error",
    });
  });

  it("enforces credit against the acknowledged cursor", () => {
    const payload = new Uint8Array(16 * 1024);
    const state = {
      ...baseline,
      sentEventSeq: "31",
      sentPtyOffset: String(31 * payload.byteLength),
    };
    const result = advanceDeliveryCursor(
      state,
      frame({ eventSeq: 32n, ptyOffset: BigInt(31 * payload.byteLength), payload }),
    );
    expect(result).toEqual({ kind: "credit-exceeded" });
    expect(MAX_UNACKED_BYTES).toBe(512 * 1024);
  });
});
