import {
  DecimalU64Schema,
  RecoveryResourceIdSchema,
  RelayCapabilitySchema,
} from "@zhongduan/protocol";
import { z } from "zod";

const identifier = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);
const relayCapabilities = z.array(RelayCapabilitySchema).max(16);

const socketIdentity = {
  peer: z.enum(["host", "browser"]),
  channel: z.enum(["control", "data"]),
  connectionSetId: identifier,
  connectionId: identifier,
  subject: identifier,
  clientId: identifier.nullable(),
  role: z.enum(["host", "writer", "observer"]),
  streamId: z.number().int().min(0).max(0xffff_ffff),
  deliveryGeneration: DecimalU64Schema,
  hostFence: DecimalU64Schema.nullable(),
  leaseFence: DecimalU64Schema.nullable(),
};

const legacyDeliveryState = {
  controlState: z.enum(["awaiting-attach", "active"]).nullable(),
  dataState: z.enum(["awaiting-attach", "catching-up", "synced"]).nullable(),
  firstEventSeq: DecimalU64Schema.nullable(),
  ackedEventSeq: DecimalU64Schema.nullable(),
  sentEventSeq: DecimalU64Schema.nullable(),
  firstPtyOffset: DecimalU64Schema.nullable(),
  ackedPtyOffset: DecimalU64Schema.nullable(),
  sentPtyOffset: DecimalU64Schema.nullable(),
  replayMode: z.enum(["warm", "snapshot"]).nullable().default(null),
  snapshotId: identifier.nullable().default(null),
  replayCommitEventSeq: DecimalU64Schema.nullable().default(null),
  replayCommitPtyOffset: DecimalU64Schema.nullable().default(null),
};

export const SocketAttachmentV2Schema = z.strictObject({
  version: z.literal(2),
  ...socketIdentity,
  ...legacyDeliveryState,
  relayCapabilities: relayCapabilities.default([]),
});

const socketAttachmentV3Fields = {
  version: z.literal(3),
  ...socketIdentity,
  relayCapabilities,
  recoveryStrategy: z.literal("v3"),
  recoveryLookupKey: RecoveryResourceIdSchema.nullable(),
};

function validateV3Identity(
  attachment: {
    clientId: string | null;
    deliveryGeneration: string;
    hostFence: string | null;
    leaseFence: string | null;
    peer: "host" | "browser";
    recoveryLookupKey: string | null;
    role: "host" | "observer" | "writer";
    streamId: number;
  },
  context: z.RefinementCtx,
): void {
  const reject = (message: string, path: string): void => {
    context.addIssue({ code: "custom", message, path: [path] });
  };
  if (attachment.peer === "host") {
    if (attachment.clientId !== null) reject("host sockets cannot bind a client", "clientId");
    if (attachment.role !== "host") reject("host sockets require the host role", "role");
    if (attachment.streamId !== 0) reject("host sockets require stream zero", "streamId");
    if (attachment.deliveryGeneration !== "0") {
      reject("host sockets require generation zero", "deliveryGeneration");
    }
    if (attachment.hostFence === null) reject("host sockets require a host fence", "hostFence");
    if (attachment.leaseFence !== null) reject("host sockets cannot bind a lease", "leaseFence");
    if (attachment.recoveryLookupKey !== null) {
      reject("host sockets cannot bind one browser recovery lookup key", "recoveryLookupKey");
    }
    return;
  }

  if (attachment.clientId === null) reject("browser sockets require a client", "clientId");
  if (attachment.role === "host") reject("browser sockets cannot use the host role", "role");
  if (attachment.streamId === 0) reject("browser sockets require a nonzero stream", "streamId");
  if (attachment.deliveryGeneration === "0") {
    reject("browser sockets require a positive generation", "deliveryGeneration");
  }
  if (attachment.hostFence !== null)
    reject("browser sockets cannot bind a host fence", "hostFence");
}

export const SocketAttachmentV3Schema = z
  .strictObject(socketAttachmentV3Fields)
  .superRefine(validateV3Identity);

const NormalizedSocketAttachmentV2Schema = z.strictObject({
  ...SocketAttachmentV2Schema.shape,
  recoveryStrategy: z.literal("v2").default("v2"),
  recoveryLookupKey: z.null().default(null),
});

const NormalizedSocketAttachmentV3Schema = z
  .strictObject({
    ...socketAttachmentV3Fields,
    controlState: z.null().default(null),
    dataState: z.null().default(null),
    firstEventSeq: z.null().default(null),
    ackedEventSeq: z.null().default(null),
    sentEventSeq: z.null().default(null),
    firstPtyOffset: z.null().default(null),
    ackedPtyOffset: z.null().default(null),
    sentPtyOffset: z.null().default(null),
    replayMode: z.null().default(null),
    snapshotId: z.null().default(null),
    replayCommitEventSeq: z.null().default(null),
    replayCommitPtyOffset: z.null().default(null),
  })
  .superRefine(validateV3Identity);

// Runtime callers use one normalized shape while the serialized hibernation
// attachment remains versioned. V3 intentionally exposes null legacy delivery
// fields in memory; their durable authority belongs to the recovery store.
export const SocketAttachmentSchema = z.discriminatedUnion("version", [
  NormalizedSocketAttachmentV2Schema,
  NormalizedSocketAttachmentV3Schema,
]);

type LegacySocketAttachment = z.infer<typeof SocketAttachmentV2Schema>;

// Keep the caller-facing transition shape wide enough for the existing V2
// state machine to update its legacy fields after a version check. The strict
// normalized schema above still rejects those fields on V3 at the write
// boundary instead of silently persisting or discarding them.
export type SocketAttachment = Omit<LegacySocketAttachment, "version"> & {
  version: 2 | 3;
  recoveryStrategy: "v2" | "v3";
  recoveryLookupKey: string | null;
};
export type RelayChannel = SocketAttachment["channel"];

type AttachmentReader = Pick<WebSocket, "deserializeAttachment">;
type AttachmentWriter = Pick<WebSocket, "serializeAttachment">;

export function normalizeSocketAttachment(value: unknown): SocketAttachment | undefined {
  const v2 = SocketAttachmentV2Schema.safeParse(value);
  if (v2.success) return SocketAttachmentSchema.parse(v2.data);

  const v3 = SocketAttachmentV3Schema.safeParse(value);
  return v3.success ? SocketAttachmentSchema.parse(v3.data) : undefined;
}

export function readSocketAttachment(webSocket: AttachmentReader): SocketAttachment | undefined {
  return normalizeSocketAttachment(webSocket.deserializeAttachment());
}

export function writeSocketAttachment(
  webSocket: AttachmentWriter,
  attachment: SocketAttachment,
): void {
  const normalized = SocketAttachmentSchema.parse(attachment);
  if (normalized.recoveryStrategy === "v2") {
    const {
      recoveryLookupKey: _recoveryLookupKey,
      recoveryStrategy: _recoveryStrategy,
      ...v2
    } = normalized;
    webSocket.serializeAttachment(SocketAttachmentV2Schema.parse(v2));
    return;
  }

  webSocket.serializeAttachment(
    SocketAttachmentV3Schema.parse({
      version: 3,
      peer: normalized.peer,
      channel: normalized.channel,
      connectionSetId: normalized.connectionSetId,
      connectionId: normalized.connectionId,
      subject: normalized.subject,
      clientId: normalized.clientId,
      role: normalized.role,
      streamId: normalized.streamId,
      deliveryGeneration: normalized.deliveryGeneration,
      hostFence: normalized.hostFence,
      leaseFence: normalized.leaseFence,
      relayCapabilities: normalized.relayCapabilities,
      recoveryStrategy: normalized.recoveryStrategy,
      recoveryLookupKey: normalized.recoveryLookupKey,
    }),
  );
}
