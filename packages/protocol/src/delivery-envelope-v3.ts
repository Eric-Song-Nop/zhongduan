import {
  DATA_HEADER_BYTES,
  DataFrameKind,
  MAX_DATA_PAYLOAD_BYTES,
  decodeDataFrame,
  decodeResizePayload,
} from "./data-frame";
import { ProtocolError } from "./errors";
import { MAX_U64 } from "./scalars";

export const DELIVERY_ENVELOPE_V3_MAGIC = 0x5a454e56;
export const DELIVERY_ENVELOPE_V3_VERSION = 3;
export const DELIVERY_ENVELOPE_V3_HEADER_BYTES = 40;

const MAX_DELIVERY_ENVELOPE_PAYLOAD_BYTES = DATA_HEADER_BYTES + MAX_DATA_PAYLOAD_BYTES;

const DeliveryLaneCode = {
  live: 1,
  recovery: 2,
} as const;

export type DeliveryLane = keyof typeof DeliveryLaneCode;

export interface DeliveryEnvelopeV3 {
  readonly cumulativeEncodedBytes: bigint;
  readonly deliveryGeneration: bigint;
  readonly deliveryOrdinal: bigint;
  readonly lane: DeliveryLane;
  readonly payload: Uint8Array;
  readonly streamId: number;
}

export interface DeliveryLaneCursor {
  readonly cumulativeEncodedBytes: bigint;
  readonly deliveryGeneration: bigint;
  readonly deliveryOrdinal: bigint;
  readonly lane: DeliveryLane;
  readonly streamId: number;
}

export function initialDeliveryLaneCursor(
  deliveryGeneration: bigint,
  lane: DeliveryLane,
  streamId: number,
): DeliveryLaneCursor {
  assertPositiveU64(deliveryGeneration, "deliveryGeneration");
  assertDeliveryLane(lane);
  assertU32(streamId, "streamId", true);
  return { cumulativeEncodedBytes: 0n, deliveryGeneration, deliveryOrdinal: 0n, lane, streamId };
}

function assertU32(value: number, field: string, positive = false): void {
  if (!Number.isInteger(value) || value < (positive ? 1 : 0) || value > 0xffff_ffff) {
    throw new ProtocolError("OUT_OF_RANGE", `${field} must be a uint32`);
  }
}

function assertPositiveU64(value: bigint, field: string): void {
  if (value <= 0n || value > MAX_U64) {
    throw new ProtocolError("OUT_OF_RANGE", `${field} must be a positive uint64`);
  }
}

function laneForCode(code: number): DeliveryLane {
  if (code === DeliveryLaneCode.live) return "live";
  if (code === DeliveryLaneCode.recovery) return "recovery";
  throw new ProtocolError("BAD_KIND", `unknown delivery lane: ${code}`);
}

function assertDeliveryLane(lane: DeliveryLane): void {
  if (lane !== "live" && lane !== "recovery") {
    throw new ProtocolError("BAD_KIND", `unknown delivery lane: ${String(lane)}`);
  }
}

function validateCanonicalPayload(lane: DeliveryLane, payload: Uint8Array): void {
  if (payload.byteLength > MAX_DELIVERY_ENVELOPE_PAYLOAD_BYTES) {
    throw new ProtocolError("BAD_LENGTH", "DeliveryEnvelope payload is too large");
  }
  const frame = decodeDataFrame(payload);
  if (frame.deliveryGeneration !== 0n || frame.streamId !== 0 || frame.flags !== 0) {
    throw new ProtocolError("BAD_PAYLOAD", "DeliveryEnvelope payload is not canonical data v2");
  }
  if (frame.kind === DataFrameKind.ResizeApplied) {
    try {
      decodeResizePayload(frame.payload);
    } catch {
      throw new ProtocolError("BAD_PAYLOAD", "DeliveryEnvelope contains an invalid resize");
    }
    return;
  }
  if (frame.kind === DataFrameKind.PtyOutput) {
    if (frame.ptyOffset > MAX_U64 - BigInt(frame.payload.byteLength)) {
      throw new ProtocolError("BAD_PAYLOAD", "canonical PTY range exceeds uint64");
    }
    return;
  }
  if (
    lane === "recovery" &&
    frame.kind === DataFrameKind.ReplayCommit &&
    frame.payload.byteLength === 0
  ) {
    return;
  }
  throw new ProtocolError("BAD_KIND", "data kind is not valid for the DeliveryEnvelope lane");
}

function validateEnvelope(envelope: DeliveryEnvelopeV3): void {
  assertDeliveryLane(envelope.lane);
  assertPositiveU64(envelope.deliveryGeneration, "deliveryGeneration");
  assertPositiveU64(envelope.deliveryOrdinal, "deliveryOrdinal");
  assertPositiveU64(envelope.cumulativeEncodedBytes, "cumulativeEncodedBytes");
  assertU32(envelope.streamId, "streamId", true);
  validateCanonicalPayload(envelope.lane, envelope.payload);
  const encodedBytes = BigInt(DELIVERY_ENVELOPE_V3_HEADER_BYTES + envelope.payload.byteLength);
  if (
    envelope.cumulativeEncodedBytes < encodedBytes ||
    (envelope.deliveryOrdinal === 1n && envelope.cumulativeEncodedBytes !== encodedBytes)
  ) {
    throw new ProtocolError("BAD_PAYLOAD", "DeliveryEnvelope cumulative bytes are inconsistent");
  }
}

export function encodeDeliveryEnvelopeV3(envelope: DeliveryEnvelopeV3): Uint8Array {
  validateEnvelope(envelope);
  const encoded = new Uint8Array(DELIVERY_ENVELOPE_V3_HEADER_BYTES + envelope.payload.byteLength);
  const view = new DataView(encoded.buffer);
  view.setUint32(0, DELIVERY_ENVELOPE_V3_MAGIC, false);
  view.setUint8(4, DELIVERY_ENVELOPE_V3_VERSION);
  view.setUint8(5, DeliveryLaneCode[envelope.lane]);
  view.setUint16(6, 0, true);
  view.setBigUint64(8, envelope.deliveryGeneration, true);
  view.setBigUint64(16, envelope.deliveryOrdinal, true);
  view.setBigUint64(24, envelope.cumulativeEncodedBytes, true);
  view.setUint32(32, envelope.streamId, true);
  view.setUint32(36, envelope.payload.byteLength, true);
  encoded.set(envelope.payload, DELIVERY_ENVELOPE_V3_HEADER_BYTES);
  return encoded;
}

export function decodeDeliveryEnvelopeV3(input: ArrayBuffer | Uint8Array): DeliveryEnvelopeV3 {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  if (bytes.byteLength < DELIVERY_ENVELOPE_V3_HEADER_BYTES) {
    throw new ProtocolError("BAD_LENGTH", "DeliveryEnvelope header is truncated");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(0, false) !== DELIVERY_ENVELOPE_V3_MAGIC) {
    throw new ProtocolError("BAD_MAGIC", "invalid DeliveryEnvelope magic");
  }
  if (view.getUint8(4) !== DELIVERY_ENVELOPE_V3_VERSION) {
    throw new ProtocolError("BAD_VERSION", "unsupported DeliveryEnvelope version");
  }
  if (view.getUint16(6, true) !== 0) {
    throw new ProtocolError("BAD_FLAGS", "DeliveryEnvelope flags must be zero");
  }
  const payloadLength = view.getUint32(36, true);
  if (
    payloadLength > MAX_DELIVERY_ENVELOPE_PAYLOAD_BYTES ||
    bytes.byteLength !== DELIVERY_ENVELOPE_V3_HEADER_BYTES + payloadLength
  ) {
    throw new ProtocolError("BAD_LENGTH", "DeliveryEnvelope payload length mismatch");
  }
  const envelope: DeliveryEnvelopeV3 = {
    lane: laneForCode(view.getUint8(5)),
    deliveryGeneration: view.getBigUint64(8, true),
    deliveryOrdinal: view.getBigUint64(16, true),
    cumulativeEncodedBytes: view.getBigUint64(24, true),
    streamId: view.getUint32(32, true),
    payload: bytes.subarray(DELIVERY_ENVELOPE_V3_HEADER_BYTES),
  };
  validateEnvelope(envelope);
  return envelope;
}

/** Validates cumulative progress for one exact generation, lane, and stream. */
export function advanceDeliveryLaneCursor(
  previous: DeliveryLaneCursor,
  envelope: DeliveryEnvelopeV3,
): DeliveryLaneCursor {
  if (
    previous.deliveryOrdinal < 0n ||
    previous.deliveryOrdinal > MAX_U64 ||
    previous.cumulativeEncodedBytes < 0n ||
    previous.cumulativeEncodedBytes > MAX_U64
  ) {
    throw new ProtocolError("OUT_OF_RANGE", "delivery lane cursor must contain uint64 values");
  }
  assertPositiveU64(previous.deliveryGeneration, "deliveryGeneration");
  assertDeliveryLane(previous.lane);
  assertU32(previous.streamId, "streamId", true);
  validateEnvelope(envelope);
  if (envelope.deliveryGeneration !== previous.deliveryGeneration) {
    throw new ProtocolError("BAD_GENERATION", "delivery lane cursor generation changed");
  }
  if (envelope.lane !== previous.lane) {
    throw new ProtocolError("BAD_KIND", "delivery lane cursor changed lanes");
  }
  if (envelope.streamId !== previous.streamId) {
    throw new ProtocolError("BAD_KIND", "delivery lane cursor stream changed");
  }
  if (envelope.deliveryOrdinal !== previous.deliveryOrdinal + 1n) {
    throw new ProtocolError("EVENT_GAP", "delivery lane ordinal is not contiguous");
  }
  const encodedBytes = BigInt(DELIVERY_ENVELOPE_V3_HEADER_BYTES + envelope.payload.byteLength);
  if (
    previous.cumulativeEncodedBytes > MAX_U64 - encodedBytes ||
    envelope.cumulativeEncodedBytes !== previous.cumulativeEncodedBytes + encodedBytes
  ) {
    throw new ProtocolError("BAD_PAYLOAD", "delivery lane cumulative bytes are not contiguous");
  }
  return {
    deliveryOrdinal: envelope.deliveryOrdinal,
    cumulativeEncodedBytes: envelope.cumulativeEncodedBytes,
    deliveryGeneration: envelope.deliveryGeneration,
    lane: envelope.lane,
    streamId: envelope.streamId,
  };
}
