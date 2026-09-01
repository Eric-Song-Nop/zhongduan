import { describe, expect, it } from "vitest";

import {
  DATA_HEADER_BYTES,
  DATA_PROTOCOL_VERSION,
  DataFrameKind,
  decodeDataFrame,
  decodeDataFrameBatch,
  decodeDataFrameBatchEntries,
  encodeDataFrame,
  encodeDataFrameBatch,
  encodeResizePayload,
  rewriteDelivery,
} from "./data-frame";
import { decodeDeliveryBarrierPayload, encodeDeliveryBarrierPayload } from "./delivery-barrier";
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
    expect(DATA_PROTOCOL_VERSION).toBe(2);
    expect(new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength).getUint8(4)).toBe(
      2,
    );
    expect(decoded).toEqual(outputFrame);
  });

  it("rejects the pre-barrier v1 wire version", () => {
    const encoded = encodeDataFrame(outputFrame);
    new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength).setUint8(4, 1);
    expect(() => decodeDataFrame(encoded)).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_VERSION" }),
    );
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

  it("rejects unimplemented data-frame flags", () => {
    expect(() => encodeDataFrame({ ...outputFrame, flags: 1 })).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_FLAGS" }),
    );
    const encoded = encodeDataFrame(outputFrame);
    new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength).setUint16(6, 1, true);
    expect(() => decodeDataFrame(encoded)).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_FLAGS" }),
    );
  });

  it("round-trips a bounded concatenated transport batch without changing logical frames", () => {
    const first = encodeDataFrame(outputFrame);
    const secondFrame = {
      ...outputFrame,
      eventSeq: outputFrame.eventSeq + 1n,
      ptyOffset: outputFrame.ptyOffset + BigInt(outputFrame.payload.byteLength),
      payload: new TextEncoder().encode("next"),
    };
    const second = encodeDataFrame(secondFrame);
    const batch = encodeDataFrameBatch([first, second]);

    expect(batch.byteLength).toBe(first.byteLength + second.byteLength);
    expect(decodeDataFrameBatch(batch)).toEqual([outputFrame, secondFrame]);
    expect(decodeDataFrameBatchEntries(batch)).toEqual([
      { encoded: first, frame: outputFrame },
      { encoded: second, frame: secondFrame },
    ]);
    expect(() => decodeDataFrame(batch)).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_LENGTH" }),
    );
    expect(() => decodeDataFrameBatch(batch.subarray(0, batch.byteLength - 1))).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_LENGTH" }),
    );
    expect(() => encodeDataFrameBatch([])).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_LENGTH" }),
    );
  });

  it("round-trips strict warm and snapshot delivery barrier payloads", () => {
    const warm = { mode: "warm", connectionId: "connection_AAAAAAAAA" } as const;
    const snapshot = {
      mode: "snapshot",
      connectionId: "connection_AAAAAAAAA",
      snapshotId: "snapshot_AAAAAAAAAAA",
    } as const;

    expect(decodeDeliveryBarrierPayload(encodeDeliveryBarrierPayload(warm))).toEqual(warm);
    expect(decodeDeliveryBarrierPayload(encodeDeliveryBarrierPayload(snapshot))).toEqual(snapshot);
    expect(() =>
      decodeDeliveryBarrierPayload(
        new TextEncoder().encode(JSON.stringify({ ...warm, mode: "unknown", privilege: "host" })),
      ),
    ).toThrowError(expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_PAYLOAD" }));
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
