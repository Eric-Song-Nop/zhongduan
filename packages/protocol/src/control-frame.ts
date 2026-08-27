import { z } from "zod";

const u64 = z.string().regex(/^(0|[1-9][0-9]*)$/);
const id = z.string().min(1).max(128);
const engineId = z.string().min(1).max(512);
const dimensions = {
  cols: z.number().int().min(1).max(1_000),
  rows: z.number().int().min(1).max(1_000),
  widthPx: z.number().int().min(0).max(1_000_000),
  heightPx: z.number().int().min(0).max(1_000_000),
};
const inputIdentity = {
  writerLease: id,
  inputEpoch: id,
  clientInputSeq: u64,
};

const attach = z.strictObject({
  type: z.literal("attach"),
  engineId,
  hasLiveReplica: z.boolean(),
  lastSessionEpoch: u64.optional(),
  lastEventSeq: u64.optional(),
  nextPtyOffset: u64.optional(),
});

const ack = z.strictObject({
  type: z.literal("ack"),
  sessionEpoch: u64,
  deliveryGeneration: u64,
  eventSeq: u64,
  nextPtyOffset: u64,
});

const key = z.strictObject({
  type: z.literal("key"),
  ...inputIdentity,
  observedEventSeq: u64,
  code: z.string().max(128),
  key: z.string().max(128),
  text: z.string().max(1024).optional(),
  modifiers: z.number().int().min(0).max(0xffff),
  repeat: z.boolean(),
});

const paste = z.strictObject({
  type: z.literal("paste"),
  ...inputIdentity,
  data: z.string().max(1024 * 1024),
});

const resizeRequest = z.strictObject({
  type: z.literal("resize-request"),
  ...inputIdentity,
  ...dimensions,
});

export const ClientControlFrameSchema = z.discriminatedUnion("type", [
  attach,
  ack,
  key,
  paste,
  resizeRequest,
]);

export type ClientControlFrame = z.infer<typeof ClientControlFrameSchema>;

const hostReady = z.strictObject({
  type: z.literal("host-ready"),
  engineId,
  sessionEpoch: u64,
  headEventSeq: u64,
  nextPtyOffset: u64,
});

const inputAck = z.strictObject({
  type: z.literal("input-ack"),
  connectionId: id,
  inputEpoch: id,
  clientInputSeq: u64,
  status: z.enum(["written", "duplicate", "rejected", "uncertain"]),
  authorityEventSeq: u64,
});

const replayUnavailable = z.strictObject({
  type: z.literal("replay-unavailable"),
  connectionId: id,
  reason: z.enum(["journal-gap", "engine-mismatch", "epoch-changed"]),
});

export const HostControlFrameSchema = z.discriminatedUnion("type", [
  hostReady,
  inputAck,
  replayUnavailable,
]);

export type HostControlFrame = z.infer<typeof HostControlFrameSchema>;

const welcome = z.strictObject({
  type: z.literal("welcome"),
  connectionId: id,
  streamId: z.number().int().min(1).max(0xffff_ffff),
  writerLease: id.optional(),
  engineId,
  sessionEpoch: u64,
  deliveryGeneration: u64,
  headEventSeq: u64,
  nextPtyOffset: u64,
});

const hostOffline = z.strictObject({
  type: z.literal("host-offline"),
});

const resyncRequired = z.strictObject({
  type: z.literal("resync-required"),
  deliveryGeneration: u64,
  reason: z.enum(["journal-gap", "slow-client", "engine-mismatch", "epoch-changed"]),
});

const snapshotManifest = z.strictObject({
  type: z.literal("snapshot-manifest"),
  snapshotId: id,
  engineId,
  sessionEpoch: u64,
  deliveryGeneration: u64,
  cutEventSeq: u64,
  nextPtyOffset: u64,
  commitEventSeq: u64,
  commitPtyOffset: u64,
  compression: z.enum(["none", "zstd"]),
  compressedLength: u64,
  uncompressedLength: u64,
  sha256: z.string().regex(/^[a-f0-9]{64}$/),
  downloadPath: z.string().startsWith("/api/v1/sessions/"),
  restoreThrough: z.enum(["ready", "finish"]),
});

export const ServerControlFrameSchema = z.discriminatedUnion("type", [
  welcome,
  inputAck.omit({ connectionId: true }),
  hostOffline,
  resyncRequired,
  snapshotManifest,
]);

export type ServerControlFrame = z.infer<typeof ServerControlFrameSchema>;

const attachRequest = z.strictObject({
  type: z.literal("attach-request"),
  connectionId: id,
  streamId: z.number().int().min(1).max(0xffff_ffff),
  deliveryGeneration: u64,
  engineId,
  hasLiveReplica: z.boolean(),
  lastSessionEpoch: u64.optional(),
  lastEventSeq: u64.optional(),
  nextPtyOffset: u64.optional(),
});

const verifiedClient = { connectionId: id, clientId: id };
const forwardedKey = key.extend(verifiedClient);
const forwardedPaste = paste.extend(verifiedClient);
const forwardedResize = resizeRequest.extend(verifiedClient);

export const RelayToHostControlFrameSchema = z.discriminatedUnion("type", [
  attachRequest,
  forwardedKey,
  forwardedPaste,
  forwardedResize,
]);

export type RelayToHostControlFrame = z.infer<typeof RelayToHostControlFrameSchema>;

export function decodeControlFrame<T>(input: string, schema: z.ZodType<T>): T {
  return schema.parse(JSON.parse(input) as unknown);
}

export function encodeControlFrame(frame: object): string {
  return JSON.stringify(frame);
}
