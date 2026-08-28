import { describe, expect, it } from "vitest";

import {
  DATA_HEADER_BYTES,
  DataFrameKind,
  decodeDataFrame,
  encodeDataFrame,
  encodeResizePayload,
  rewriteDelivery,
} from "./data-frame";
import { ProtocolError } from "./errors";
import { applyMutationCursor } from "./replica-cursor";

const outputFrame = {
  kind: DataFrameKind.PtyOutput,
  flags: 0,
  sessionEpoch: 7n,
  deliveryGeneration: 3n,
  eventSeq: 19n,
  ptyOffset: 4_200n,
  streamId: 0,
  payload: new TextEncoder().encode("\u001b[?2004huser@host:~$ "),
};

describe("data frame codec", () => {
  it("round-trips a PTY output frame without changing bytes", () => {
    const encoded = encodeDataFrame(outputFrame);
    const decoded = decodeDataFrame(encoded);

    expect(encoded.byteLength).toBe(DATA_HEADER_BYTES + outputFrame.payload.length);
    expect(decoded).toEqual(outputFrame);
  });

  it("rewrites only relay-owned delivery fields", () => {
    const encoded = encodeDataFrame(outputFrame);
    const rewritten = decodeDataFrame(rewriteDelivery(encoded, 9n, 42));

    expect(rewritten).toEqual({
      ...outputFrame,
      deliveryGeneration: 9n,
      streamId: 42,
    });
    expect(decodeDataFrame(encoded)).toEqual(outputFrame);
  });

  it("rejects a payload length that does not match the websocket message", () => {
    const encoded = encodeDataFrame(outputFrame);
    const truncated = encoded.subarray(0, encoded.byteLength - 1);

    expect(() => decodeDataFrame(truncated)).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_LENGTH" }),
    );
  });
});

describe("replica continuity", () => {
  it("orders PTY output and resize in one mutation sequence", () => {
    const initial = {
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      lastEventSeq: 18n,
      nextPtyOffset: 4_200n,
    };
    const afterOutput = applyMutationCursor(initial, outputFrame).cursor;
    const resize = {
      ...outputFrame,
      kind: DataFrameKind.ResizeApplied,
      eventSeq: 20n,
      ptyOffset: afterOutput.nextPtyOffset,
      payload: encodeResizePayload({
        cols: 120,
        rows: 40,
        widthPx: 960,
        heightPx: 800,
      }),
    };

    const applied = applyMutationCursor(afterOutput, resize);

    expect(applied.cursor).toEqual({
      ...afterOutput,
      lastEventSeq: 20n,
    });
    expect(applied.resize).toEqual({
      cols: 120,
      rows: 40,
      widthPx: 960,
      heightPx: 800,
    });
  });

  it("rejects a duplicated PTY range even when event sequence is next", () => {
    const cursor = {
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      lastEventSeq: 18n,
      nextPtyOffset: 4_201n,
    };

    expect(() => applyMutationCursor(cursor, outputFrame)).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({
        code: "PTY_OFFSET_GAP",
      }),
    );
  });
});
