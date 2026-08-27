import { ProtocolError } from "./errors";
import { MAX_U64 } from "./scalars";

export const DATA_MAGIC = 0x5a54524d;
export const DATA_PROTOCOL_VERSION = 1;
export const DATA_HEADER_BYTES = 48;
export const MAX_DATA_PAYLOAD_BYTES = 16 * 1024 * 1024;

export const DataFrameKind = {
  PtyOutput: 1,
  ResizeApplied: 2,
  ReplayCommit: 3,
  Reset: 4,
} as const;

export type DataFrameKind = (typeof DataFrameKind)[keyof typeof DataFrameKind];

export const DataFrameFlag = {
  None: 0,
  Compressed: 1 << 0,
  Final: 1 << 1,
} as const;

export interface DataFrameHeader {
  kind: DataFrameKind;
  flags: number;
  sessionEpoch: bigint;
  deliveryGeneration: bigint;
  eventSeq: bigint;
  ptyOffset: bigint;
  streamId: number;
}

export interface DataFrame extends DataFrameHeader {
  payload: Uint8Array;
}

const validKinds = new Set<number>(Object.values(DataFrameKind));

function assertU32(value: number, field: string): void {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
    throw new ProtocolError("OUT_OF_RANGE", `${field} must be a uint32`);
  }
}

function assertU16(value: number, field: string): void {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff) {
    throw new ProtocolError("OUT_OF_RANGE", `${field} must be a uint16`);
  }
}

function assertU64(value: bigint, field: string): void {
  if (value < 0n || value > MAX_U64) {
    throw new ProtocolError("OUT_OF_RANGE", `${field} must be a uint64`);
  }
}

function assertKind(kind: number): asserts kind is DataFrameKind {
  if (!validKinds.has(kind)) {
    throw new ProtocolError("BAD_KIND", `unknown data frame kind: ${kind}`);
  }
}

export function encodeDataFrame(frame: DataFrame): Uint8Array {
  assertKind(frame.kind);
  assertU16(frame.flags, "flags");
  assertU64(frame.sessionEpoch, "sessionEpoch");
  assertU64(frame.deliveryGeneration, "deliveryGeneration");
  assertU64(frame.eventSeq, "eventSeq");
  assertU64(frame.ptyOffset, "ptyOffset");
  assertU32(frame.streamId, "streamId");

  if (frame.payload.byteLength > MAX_DATA_PAYLOAD_BYTES) {
    throw new ProtocolError("BAD_LENGTH", `payload exceeds ${MAX_DATA_PAYLOAD_BYTES} bytes`);
  }

  const encoded = new Uint8Array(DATA_HEADER_BYTES + frame.payload.byteLength);
  const view = new DataView(encoded.buffer);

  view.setUint32(0, DATA_MAGIC, false);
  view.setUint8(4, DATA_PROTOCOL_VERSION);
  view.setUint8(5, frame.kind);
  view.setUint16(6, frame.flags, true);
  view.setBigUint64(8, frame.sessionEpoch, true);
  view.setBigUint64(16, frame.deliveryGeneration, true);
  view.setBigUint64(24, frame.eventSeq, true);
  view.setBigUint64(32, frame.ptyOffset, true);
  view.setUint32(40, frame.streamId, true);
  view.setUint32(44, frame.payload.byteLength, true);
  encoded.set(frame.payload, DATA_HEADER_BYTES);

  return encoded;
}

export function decodeDataFrame(encoded: ArrayBuffer | Uint8Array): DataFrame {
  const bytes = encoded instanceof Uint8Array ? encoded : new Uint8Array(encoded);

  if (bytes.byteLength < DATA_HEADER_BYTES) {
    throw new ProtocolError("BAD_LENGTH", "data frame header is truncated");
  }

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(0, false) !== DATA_MAGIC) {
    throw new ProtocolError("BAD_MAGIC", "invalid data frame magic");
  }
  if (view.getUint8(4) !== DATA_PROTOCOL_VERSION) {
    throw new ProtocolError("BAD_VERSION", "unsupported data frame version");
  }

  const kind = view.getUint8(5);
  assertKind(kind);
  const payloadLength = view.getUint32(44, true);
  if (
    payloadLength > MAX_DATA_PAYLOAD_BYTES ||
    bytes.byteLength !== DATA_HEADER_BYTES + payloadLength
  ) {
    throw new ProtocolError("BAD_LENGTH", "data frame payload length mismatch");
  }

  return {
    kind,
    flags: view.getUint16(6, true),
    sessionEpoch: view.getBigUint64(8, true),
    deliveryGeneration: view.getBigUint64(16, true),
    eventSeq: view.getBigUint64(24, true),
    ptyOffset: view.getBigUint64(32, true),
    streamId: view.getUint32(40, true),
    payload: bytes.subarray(DATA_HEADER_BYTES),
  };
}

export function rewriteDelivery(
  encoded: Uint8Array,
  deliveryGeneration: bigint,
  streamId: number,
): Uint8Array {
  assertU64(deliveryGeneration, "deliveryGeneration");
  assertU32(streamId, "streamId");
  decodeDataFrame(encoded);

  const rewritten = encoded.slice();
  const view = new DataView(rewritten.buffer, rewritten.byteOffset, rewritten.byteLength);
  view.setBigUint64(16, deliveryGeneration, true);
  view.setUint32(40, streamId, true);
  return rewritten;
}

export interface ResizePayload {
  cols: number;
  rows: number;
  widthPx: number;
  heightPx: number;
}

export function encodeResizePayload(resize: ResizePayload): Uint8Array {
  assertU32(resize.cols, "cols");
  assertU32(resize.rows, "rows");
  assertU32(resize.widthPx, "widthPx");
  assertU32(resize.heightPx, "heightPx");
  if (resize.cols === 0 || resize.rows === 0) {
    throw new ProtocolError("BAD_PAYLOAD", "terminal dimensions must be non-zero");
  }

  const payload = new Uint8Array(16);
  const view = new DataView(payload.buffer);
  view.setUint32(0, resize.cols, true);
  view.setUint32(4, resize.rows, true);
  view.setUint32(8, resize.widthPx, true);
  view.setUint32(12, resize.heightPx, true);
  return payload;
}

export function decodeResizePayload(payload: Uint8Array): ResizePayload {
  if (payload.byteLength !== 16) {
    throw new ProtocolError("BAD_PAYLOAD", "resize payload must be 16 bytes");
  }
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const resize = {
    cols: view.getUint32(0, true),
    rows: view.getUint32(4, true),
    widthPx: view.getUint32(8, true),
    heightPx: view.getUint32(12, true),
  };
  if (resize.cols === 0 || resize.rows === 0) {
    throw new ProtocolError("BAD_PAYLOAD", "terminal dimensions must be non-zero");
  }
  return resize;
}
