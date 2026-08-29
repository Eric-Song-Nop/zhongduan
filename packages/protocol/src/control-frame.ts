import { z } from "zod";
import {
  RecoveryV3CloudToHostControlFrameSchema,
  RecoveryV3HostToCloudControlFrameSchema,
} from "./recovery-v3-host-control";
import {
  RecoveryV3ClientControlFrameSchema,
  RecoveryV3ServerControlFrameSchema,
} from "./recovery-v3-control";
import { DecimalU64Schema } from "./scalars";
import { SnapshotContentMetadataSchema, SnapshotResourceIdSchema } from "./snapshot";
import { RecoveryStrategySchema, type RecoveryStrategy } from "./wire-capabilities";

const u64 = DecimalU64Schema;
const id = z.string().min(1).max(128);
const engineId = z.string().min(1).max(512);
const dimensions = {
  cols: z.number().int().min(1).max(1_000),
  rows: z.number().int().min(1).max(1_000),
  widthPx: z.number().int().min(0).max(1_000_000),
  heightPx: z.number().int().min(0).max(1_000_000),
};
const MAX_PASTE_BYTES = 1024 * 1024;
const MAX_TEXT_BYTES = 1024 * 1024;
const MAX_SURFACE_COORDINATE = 1_000_000;
const MAX_WHEEL_DELTA = 1_000_000;
const textEncoder = new TextEncoder();
const inputIdentity = {
  writerLease: id,
  inputEpoch: id,
  clientInputSeq: u64,
};
const unicodeScalar = z
  .number()
  .int()
  .min(0)
  .max(0x10ffff)
  .refine((value) => value < 0xd800 || value > 0xdfff, "must be a Unicode scalar value");

function boundedUtf8String(maximumBytes: number, label: string) {
  return z
    .string()
    .max(maximumBytes)
    .superRefine((value, context) => {
      if (value.length <= maximumBytes && textEncoder.encode(value).byteLength > maximumBytes) {
        context.addIssue({ code: "custom", message: `${label} exceeds UTF-8 byte limit` });
      }
    });
}

const attachBase = {
  type: z.literal("attach"),
  engineId,
  deliveryGeneration: u64,
};
const attach = z.discriminatedUnion("hasLiveReplica", [
  z.strictObject({
    ...attachBase,
    hasLiveReplica: z.literal(false),
  }),
  z.strictObject({
    ...attachBase,
    hasLiveReplica: z.literal(true),
    lastSessionEpoch: u64,
    lastEventSeq: u64,
    nextPtyOffset: u64,
  }),
]);

const deliveryCursor = {
  sessionEpoch: u64,
  deliveryGeneration: u64,
  eventSeq: u64,
  nextPtyOffset: u64,
};

const ack = z.strictObject({
  type: z.literal("ack"),
  ...deliveryCursor,
});

const key = z
  .strictObject({
    type: z.literal("key"),
    ...inputIdentity,
    observedEventSeq: u64,
    code: z.string().max(128),
    key: z.string().max(128),
    text: z.string().max(1024).optional(),
    modifiers: z.number().int().min(0).max(0x3f),
    action: z.enum(["press", "repeat", "release"]),
    altGraph: z.boolean(),
    composing: z.boolean(),
    consumedModifiers: z.number().int().min(0).max(0x3f),
    unshiftedCodepoint: unicodeScalar.optional(),
  })
  .refine((frame) => (frame.consumedModifiers & ~frame.modifiers) === 0, {
    message: "consumedModifiers must be a subset of modifiers",
    path: ["consumedModifiers"],
  });

const paste = z.strictObject({
  type: z.literal("paste"),
  ...inputIdentity,
  data: boundedUtf8String(MAX_PASTE_BYTES, "paste"),
});

const text = z.strictObject({
  type: z.literal("text"),
  ...inputIdentity,
  data: boundedUtf8String(MAX_TEXT_BYTES, "text"),
});

const focus = z.strictObject({
  type: z.literal("focus"),
  ...inputIdentity,
  focused: z.boolean(),
});

const mouseBase = {
  type: z.literal("mouse"),
  ...inputIdentity,
  buttons: z.number().int().min(0).max(31),
  modifiers: z.number().int().min(0).max(0x3f),
  altGraph: z.boolean(),
  surface: z.strictObject({
    x: z.number().int().min(0).max(MAX_SURFACE_COORDINATE),
    y: z.number().int().min(0).max(MAX_SURFACE_COORDINATE),
  }),
};
const mouseButton = z.discriminatedUnion("action", [
  z.strictObject({
    ...mouseBase,
    action: z.literal("press"),
    button: z.number().int().min(0).max(4),
  }),
  z.strictObject({
    ...mouseBase,
    action: z.literal("release"),
    button: z.number().int().min(0).max(4),
  }),
  z.strictObject({ ...mouseBase, action: z.literal("move"), button: z.null() }),
]);
const wheelDelta = z.number().finite().min(-MAX_WHEEL_DELTA).max(MAX_WHEEL_DELTA).optional();
const mouseWheel = z
  .strictObject({
    ...mouseBase,
    action: z.literal("wheel"),
    button: z.null(),
    deltaX: wheelDelta,
    deltaY: wheelDelta,
    deltaMode: z.enum(["pixel", "line", "page"]),
  })
  .refine((frame) => (frame.deltaX ?? 0) !== 0 || (frame.deltaY ?? 0) !== 0, {
    message: "wheel input requires a non-zero delta",
  });
const mouse = z.union([mouseButton, mouseWheel]);

const resizeRequest = z.strictObject({
  type: z.literal("resize-request"),
  ...inputIdentity,
  ...dimensions,
});

const writerLeaseRenew = z.strictObject({
  type: z.literal("writer-lease-renew"),
  writerLease: id,
});

export const ClientControlFrameSchema = z.union([
  attach,
  ack,
  writerLeaseRenew,
  key,
  text,
  paste,
  focus,
  mouse,
  resizeRequest,
]);

export type ClientControlFrame = z.infer<typeof ClientControlFrameSchema>;

/**
 * Browser-to-Cloud production controls for a generation that explicitly
 * negotiated Recovery v3. Delivery receipt replaces the v2 `ack`; attach,
 * lease, and semantic input remain shared controls.
 */
export const ClientControlFrameV3Schema = z.union([
  attach,
  writerLeaseRenew,
  key,
  text,
  paste,
  focus,
  mouse,
  resizeRequest,
  RecoveryV3ClientControlFrameSchema,
]);

export type ClientControlFrameV3 = z.infer<typeof ClientControlFrameV3Schema>;

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

/**
 * Host-to-Cloud controls accepted on a capability-enabled Host pair. The pair
 * is session-scoped and may serve v2 and v3 Browser generations concurrently,
 * so legacy Host outcomes remain valid beside Recovery v3 source outcomes.
 */
export const HostControlFrameV3Schema = z.union([
  HostControlFrameSchema,
  RecoveryV3HostToCloudControlFrameSchema,
]);

export type HostControlFrameV3 = z.infer<typeof HostControlFrameV3Schema>;

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

const writerLeaseStatus = z
  .strictObject({
    type: z.literal("writer-lease-status"),
    active: z.boolean(),
    expiresAt: z.number().int().positive().optional(),
  })
  .refine((frame) => frame.active === (frame.expiresAt !== undefined), {
    message: "active writer lease status requires expiresAt",
  });

const resyncRequired = z
  .strictObject({
    type: z.literal("resync-required"),
    deliveryGeneration: u64,
    reason: z.enum([
      "journal-gap",
      "slow-client",
      "engine-mismatch",
      "epoch-changed",
      "data-disconnected",
      "host-reconnect",
    ]),
    dataTicket: id.optional(),
    expiresAt: z.number().int().positive().optional(),
  })
  .refine((frame) => (frame.dataTicket === undefined) === (frame.expiresAt === undefined), {
    message: "dataTicket and expiresAt must be provided together",
  });

const snapshotManifest = SnapshotContentMetadataSchema.safeExtend({
  type: z.literal("snapshot-manifest"),
  snapshotId: SnapshotResourceIdSchema,
  streamId: z.number().int().min(1).max(0xffff_ffff),
  deliveryGeneration: u64,
  commitEventSeq: u64,
  commitPtyOffset: u64,
  downloadPath: z.string().startsWith("/api/v1/sessions/"),
  restoreThrough: z.enum(["ready", "finish"]),
}).refine(
  (frame) =>
    BigInt(frame.commitEventSeq) >= BigInt(frame.cutEventSeq) &&
    BigInt(frame.commitPtyOffset) >= BigInt(frame.nextPtyOffset),
  { message: "snapshot commit must not precede its cut" },
);

const replayStart = z
  .strictObject({
    type: z.literal("replay-start"),
    sessionEpoch: u64,
    streamId: z.number().int().min(1).max(0xffff_ffff),
    deliveryGeneration: u64,
    baseEventSeq: u64,
    basePtyOffset: u64,
    commitEventSeq: u64,
    commitPtyOffset: u64,
  })
  .refine(
    (frame) =>
      BigInt(frame.commitEventSeq) >= BigInt(frame.baseEventSeq) &&
      BigInt(frame.commitPtyOffset) >= BigInt(frame.basePtyOffset),
    { message: "replay commit must not precede its baseline" },
  );

export const ServerControlFrameSchema = z.discriminatedUnion("type", [
  welcome,
  inputAck.omit({ connectionId: true }),
  hostOffline,
  writerLeaseStatus,
  resyncRequired,
  replayStart,
  snapshotManifest,
]);

export type ServerControlFrame = z.infer<typeof ServerControlFrameSchema>;

/**
 * Cloud-to-Browser production controls for Recovery v3. Recovery start owns
 * the replacement delivery plan, so v2 replay-start and snapshot-manifest are
 * intentionally absent.
 */
export const ServerControlFrameV3Schema = z.union([
  welcome,
  inputAck.omit({ connectionId: true }),
  hostOffline,
  writerLeaseStatus,
  resyncRequired,
  RecoveryV3ServerControlFrameSchema,
]);

export type ServerControlFrameV3 = z.infer<typeof ServerControlFrameV3Schema>;

const attachRequestBase = {
  type: z.literal("attach-request"),
  connectionId: id,
  streamId: z.number().int().min(1).max(0xffff_ffff),
  deliveryGeneration: u64,
  engineId,
};
const attachRequest = z.discriminatedUnion("hasLiveReplica", [
  z.strictObject({
    ...attachRequestBase,
    hasLiveReplica: z.literal(false),
  }),
  z.strictObject({
    ...attachRequestBase,
    hasLiveReplica: z.literal(true),
    lastSessionEpoch: u64,
    lastEventSeq: u64,
    nextPtyOffset: u64,
  }),
]);

const verifiedClient = { connectionId: id, clientId: id, writerFence: u64 };
const forwardedKey = key.safeExtend(verifiedClient);
const forwardedText = text.extend(verifiedClient);
const forwardedPaste = paste.extend(verifiedClient);
const forwardedFocus = focus.extend(verifiedClient);
const forwardedMouse = z.union([
  ...mouseButton.options.map((option) => option.extend(verifiedClient)),
  mouseWheel.safeExtend(verifiedClient),
]);
const forwardedResize = resizeRequest.extend(verifiedClient);
const deliveryReset = z.strictObject({
  type: z.literal("delivery-reset"),
  connectionId: id,
  streamId: z.number().int().min(1).max(0xffff_ffff),
  deliveryGeneration: u64,
  reason: z.literal("slow-client"),
});
const hostReadyAck = z.strictObject({
  type: z.literal("host-ready-ack"),
  sessionEpoch: u64,
  headEventSeq: u64,
  nextPtyOffset: u64,
});
const deliveryBarrierResultBase = {
  type: z.literal("delivery-barrier-result"),
  connectionId: id,
  streamId: z.number().int().min(1).max(0xffff_ffff),
  deliveryGeneration: u64,
  commitEventSeq: u64,
  commitPtyOffset: u64,
};
const deliveryBarrierWarm = { ...deliveryBarrierResultBase, mode: z.literal("warm") };
const deliveryBarrierSnapshot = {
  ...deliveryBarrierResultBase,
  mode: z.literal("snapshot"),
  snapshotId: SnapshotResourceIdSchema,
};
const staleDeliveryBarrierStatus = {
  status: z.literal("stale"),
  reason: z.enum(["generation-fenced", "client-gone"]),
};
const deliveryBarrierLegacyStatus = { status: z.enum(["ready", "stale", "rejected"]) };
const rejectedDeliveryBarrierWarm = z.union([
  z.strictObject({
    ...deliveryBarrierWarm,
    status: z.literal("rejected"),
    reason: z.literal("missing-live-seed"),
    retryScope: z.literal("same-generation"),
  }),
  z.strictObject({
    ...deliveryBarrierWarm,
    status: z.literal("rejected"),
    reason: z.literal("browser-control-send-failed"),
    retryScope: z.literal("drop-client"),
  }),
]);
const rejectedDeliveryBarrierSnapshot = z.union([
  z.strictObject({
    ...deliveryBarrierSnapshot,
    status: z.literal("rejected"),
    reason: z.enum(["snapshot-missing", "snapshot-metadata-mismatch"]),
    retryScope: z.literal("refresh-checkpoint"),
  }),
  z.strictObject({
    ...deliveryBarrierSnapshot,
    status: z.literal("rejected"),
    reason: z.literal("browser-control-send-failed"),
    retryScope: z.literal("drop-client"),
  }),
]);
const deliveryBarrierResult = z.union([
  z.strictObject({ ...deliveryBarrierWarm, ...deliveryBarrierLegacyStatus }),
  z.strictObject({ ...deliveryBarrierSnapshot, ...deliveryBarrierLegacyStatus }),
  z.strictObject({ ...deliveryBarrierWarm, ...staleDeliveryBarrierStatus }),
  z.strictObject({ ...deliveryBarrierSnapshot, ...staleDeliveryBarrierStatus }),
  rejectedDeliveryBarrierWarm,
  rejectedDeliveryBarrierSnapshot,
]);

export const RelayToHostControlFrameSchema = z.union([
  hostReadyAck,
  deliveryBarrierResult,
  attachRequest,
  forwardedKey,
  forwardedText,
  forwardedPaste,
  forwardedFocus,
  forwardedMouse,
  forwardedResize,
  deliveryReset,
]);

export type RelayToHostControlFrame = z.infer<typeof RelayToHostControlFrameSchema>;

/**
 * Cloud-to-Host controls for a Recovery v3 source. This is a per-generation
 * orchestration subset, not a connection-wide decoder: a capability-enabled
 * Host pair may concurrently receive RelayToHostControlFrameSchema controls
 * for v2 Browser generations. The v3 source itself replaces v2 attach-request,
 * delivery-barrier-result, and delivery-reset.
 */
export const RelayToHostControlFrameV3Schema = z.union([
  hostReadyAck,
  forwardedKey,
  forwardedText,
  forwardedPaste,
  forwardedFocus,
  forwardedMouse,
  forwardedResize,
  RecoveryV3CloudToHostControlFrameSchema,
]);

export type RelayToHostControlFrameV3 = z.infer<typeof RelayToHostControlFrameV3Schema>;

export function decodeControlFrame<T>(input: string, schema: z.ZodType<T>): T {
  return schema.parse(JSON.parse(input) as unknown);
}

type StrategyControlFrame<
  Strategy extends RecoveryStrategy,
  V2Frame,
  V3Frame,
> = Strategy extends "v3" ? V3Frame : V2Frame;

function decodeStrategyControlFrame<V2Frame, V3Frame>(
  input: string,
  strategy: RecoveryStrategy,
  v2Schema: z.ZodType<V2Frame>,
  v3Schema: z.ZodType<V3Frame>,
): V2Frame | V3Frame {
  const selected = RecoveryStrategySchema.parse(strategy);
  if (selected === "v3") return decodeControlFrame(input, v3Schema);
  return decodeControlFrame(input, v2Schema);
}

/** Defaults to the isolated v2 Browser-to-Cloud decoder. */
export function decodeClientControlFrame<Strategy extends RecoveryStrategy = "v2">(
  input: string,
  strategy: Strategy = "v2" as Strategy,
): StrategyControlFrame<Strategy, ClientControlFrame, ClientControlFrameV3> {
  return decodeStrategyControlFrame(
    input,
    strategy,
    ClientControlFrameSchema,
    ClientControlFrameV3Schema,
  ) as StrategyControlFrame<Strategy, ClientControlFrame, ClientControlFrameV3>;
}

/** Defaults to the isolated v2 Host-to-Cloud decoder. */
export function decodeHostControlFrame<Strategy extends RecoveryStrategy = "v2">(
  input: string,
  strategy: Strategy = "v2" as Strategy,
): StrategyControlFrame<Strategy, HostControlFrame, HostControlFrameV3> {
  return decodeStrategyControlFrame(
    input,
    strategy,
    HostControlFrameSchema,
    HostControlFrameV3Schema,
  ) as StrategyControlFrame<Strategy, HostControlFrame, HostControlFrameV3>;
}

/** Defaults to the isolated v2 Cloud-to-Browser decoder. */
export function decodeServerControlFrame<Strategy extends RecoveryStrategy = "v2">(
  input: string,
  strategy: Strategy = "v2" as Strategy,
): StrategyControlFrame<Strategy, ServerControlFrame, ServerControlFrameV3> {
  return decodeStrategyControlFrame(
    input,
    strategy,
    ServerControlFrameSchema,
    ServerControlFrameV3Schema,
  ) as StrategyControlFrame<Strategy, ServerControlFrame, ServerControlFrameV3>;
}

/** Defaults to the isolated v2 Cloud-to-Host decoder. */
export function decodeRelayToHostControlFrame<Strategy extends RecoveryStrategy = "v2">(
  input: string,
  strategy: Strategy = "v2" as Strategy,
): StrategyControlFrame<Strategy, RelayToHostControlFrame, RelayToHostControlFrameV3> {
  return decodeStrategyControlFrame(
    input,
    strategy,
    RelayToHostControlFrameSchema,
    RelayToHostControlFrameV3Schema,
  ) as StrategyControlFrame<Strategy, RelayToHostControlFrame, RelayToHostControlFrameV3>;
}

export function encodeControlFrame(frame: object): string {
  return JSON.stringify(frame);
}
