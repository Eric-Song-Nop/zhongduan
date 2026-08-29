import { describe, expect, it } from "vitest";

import { DataFrameKind, decodeDataFrame, encodeDataFrame, encodeResizePayload } from "./data-frame";
import {
  DELIVERY_ENVELOPE_V3_HEADER_BYTES,
  advanceDeliveryLaneCursor,
  decodeDeliveryEnvelopeV3,
  encodeDeliveryEnvelopeV3,
  initialDeliveryLaneCursor,
} from "./delivery-envelope-v3";
import { ProtocolError } from "./errors";
import { MAX_U64 } from "./scalars";

function canonicalOutput() {
  return encodeDataFrame({
    kind: DataFrameKind.PtyOutput,
    flags: 0,
    sessionEpoch: 7n,
    deliveryGeneration: 0n,
    eventSeq: 19n,
    ptyOffset: 4n,
    streamId: 0,
    payload: new TextEncoder().encode("hello"),
  });
}

function firstEnvelope() {
  const payload = canonicalOutput();
  return {
    lane: "live" as const,
    deliveryGeneration: 3n,
    deliveryOrdinal: 1n,
    cumulativeEncodedBytes: BigInt(DELIVERY_ENVELOPE_V3_HEADER_BYTES + payload.byteLength),
    streamId: 42,
    payload,
  };
}

function corruptEnvelope(encoded: Uint8Array, mutate: (view: DataView) => void): Uint8Array {
  const corrupted = encoded.slice();
  mutate(new DataView(corrupted.buffer, corrupted.byteOffset, corrupted.byteLength));
  return corrupted;
}

describe("DeliveryEnvelope v3", () => {
  it("round-trips explicit generation-scoped delivery progress around canonical v2", () => {
    const envelope = firstEnvelope();

    const decoded = decodeDeliveryEnvelopeV3(encodeDeliveryEnvelopeV3(envelope));
    expect(decoded).toEqual(envelope);
    expect(advanceDeliveryLaneCursor(initialDeliveryLaneCursor(3n, "live", 42), decoded)).toEqual({
      deliveryGeneration: 3n,
      deliveryOrdinal: 1n,
      cumulativeEncodedBytes: envelope.cumulativeEncodedBytes,
      lane: "live",
      streamId: 42,
    });
  });

  it("uses per-lane state to validate non-first cumulative progress", () => {
    const payload = canonicalOutput();
    const encodedBytes = BigInt(DELIVERY_ENVELOPE_V3_HEADER_BYTES + payload.byteLength);
    const previous = {
      deliveryGeneration: 3n,
      deliveryOrdinal: 1n,
      cumulativeEncodedBytes: encodedBytes,
      lane: "live" as const,
      streamId: 42,
    };
    const next = {
      lane: "live" as const,
      deliveryGeneration: 3n,
      deliveryOrdinal: 2n,
      cumulativeEncodedBytes: encodedBytes * 2n,
      streamId: 42,
      payload,
    };

    expect(advanceDeliveryLaneCursor(previous, next)).toEqual({
      deliveryGeneration: 3n,
      deliveryOrdinal: 2n,
      cumulativeEncodedBytes: encodedBytes * 2n,
      lane: "live",
      streamId: 42,
    });
    expect(() =>
      advanceDeliveryLaneCursor(previous, {
        ...next,
        cumulativeEncodedBytes: next.cumulativeEncodedBytes + 1n,
      }),
    ).toThrowError(expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_PAYLOAD" }));
  });

  it("rejects cursor stream, generation, lane, ordinal, and byte-count changes", () => {
    const next = firstEnvelope();
    const previous = initialDeliveryLaneCursor(3n, "live", 42);
    const mismatches = [
      [{ ...next, streamId: 43 }, "BAD_KIND"],
      [{ ...next, deliveryGeneration: 4n }, "BAD_GENERATION"],
      [{ ...next, lane: "recovery" as const }, "BAD_KIND"],
      [{ ...next, deliveryOrdinal: 2n }, "EVENT_GAP"],
      [{ ...next, cumulativeEncodedBytes: next.cumulativeEncodedBytes + 1n }, "BAD_PAYLOAD"],
    ] as const;

    for (const [envelope, code] of mismatches) {
      expect(() => advanceDeliveryLaneCursor(previous, envelope)).toThrowError(
        expect.objectContaining<Partial<ProtocolError>>({ code }),
      );
    }
  });

  it("rejects malformed envelope magic, version, lane, flags, and length", () => {
    const encoded = encodeDeliveryEnvelopeV3(firstEnvelope());
    const corruptions = [
      [corruptEnvelope(encoded, (view) => view.setUint32(0, 0, false)), "BAD_MAGIC"],
      [corruptEnvelope(encoded, (view) => view.setUint8(4, 2)), "BAD_VERSION"],
      [corruptEnvelope(encoded, (view) => view.setUint8(5, 3)), "BAD_KIND"],
      [corruptEnvelope(encoded, (view) => view.setUint16(6, 1, true)), "BAD_FLAGS"],
      [corruptEnvelope(encoded, (view) => view.setUint32(36, 0, true)), "BAD_LENGTH"],
    ] as const;

    for (const [corrupted, code] of corruptions) {
      expect(() => decodeDeliveryEnvelopeV3(corrupted)).toThrowError(
        expect.objectContaining<Partial<ProtocolError>>({ code }),
      );
    }
  });

  it("keeps v2 data frames and v3 delivery envelopes decoder-isolated", () => {
    const v2 = canonicalOutput();
    const v3 = encodeDeliveryEnvelopeV3(firstEnvelope());

    expect(() => decodeDeliveryEnvelopeV3(v2)).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_MAGIC" }),
    );
    expect(() => decodeDataFrame(v3)).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_MAGIC" }),
    );
  });

  it("rejects an invalid lane at the encoder boundary", () => {
    expect(() =>
      encodeDeliveryEnvelopeV3({ ...firstEnvelope(), lane: "unknown" as "live" }),
    ).toThrowError(expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_KIND" }));
  });

  it("allows an empty canonical v2 replay commit only on the recovery lane", () => {
    const commit = encodeDataFrame({
      kind: DataFrameKind.ReplayCommit,
      flags: 0,
      sessionEpoch: 7n,
      deliveryGeneration: 0n,
      eventSeq: 19n,
      ptyOffset: 9n,
      streamId: 0,
      payload: new Uint8Array(),
    });
    const envelope = {
      lane: "recovery" as const,
      deliveryGeneration: 3n,
      deliveryOrdinal: 2n,
      cumulativeEncodedBytes: 200n,
      streamId: 42,
      payload: commit,
    };

    expect(decodeDeliveryEnvelopeV3(encodeDeliveryEnvelopeV3(envelope))).toEqual(envelope);
    expect(() => encodeDeliveryEnvelopeV3({ ...envelope, lane: "live" })).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_KIND" }),
    );
  });

  it("rejects relay-owned fields in the canonical inner v2 frame", () => {
    const rewritten = encodeDataFrame({
      kind: DataFrameKind.ResizeApplied,
      flags: 0,
      sessionEpoch: 7n,
      deliveryGeneration: 3n,
      eventSeq: 19n,
      ptyOffset: 4n,
      streamId: 42,
      payload: encodeResizePayload({ cols: 80, rows: 24, widthPx: 800, heightPx: 600 }),
    });
    expect(() =>
      encodeDeliveryEnvelopeV3({
        lane: "recovery",
        deliveryGeneration: 3n,
        deliveryOrdinal: 1n,
        cumulativeEncodedBytes: 200n,
        streamId: 42,
        payload: rewritten,
      }),
    ).toThrowError(expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_PAYLOAD" }));
  });

  it("rejects a canonical PTY range that overflows uint64 on encode and decode", () => {
    const overflowPayload = encodeDataFrame({
      kind: DataFrameKind.PtyOutput,
      flags: 0,
      sessionEpoch: 7n,
      deliveryGeneration: 0n,
      eventSeq: 19n,
      ptyOffset: MAX_U64,
      streamId: 0,
      payload: new Uint8Array([1]),
    });
    const overflowEnvelope = {
      ...firstEnvelope(),
      cumulativeEncodedBytes: BigInt(
        DELIVERY_ENVELOPE_V3_HEADER_BYTES + overflowPayload.byteLength,
      ),
      payload: overflowPayload,
    };
    expect(() => encodeDeliveryEnvelopeV3(overflowEnvelope)).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_PAYLOAD" }),
    );

    const lastBytePayload = encodeDataFrame({
      ...decodeDataFrame(overflowPayload),
      ptyOffset: MAX_U64 - 1n,
    });
    const encoded = encodeDeliveryEnvelopeV3({ ...overflowEnvelope, payload: lastBytePayload });
    encoded.set(overflowPayload, DELIVERY_ENVELOPE_V3_HEADER_BYTES);
    expect(() => decodeDeliveryEnvelopeV3(encoded)).toThrowError(
      expect.objectContaining<Partial<ProtocolError>>({ code: "BAD_PAYLOAD" }),
    );
  });
});
