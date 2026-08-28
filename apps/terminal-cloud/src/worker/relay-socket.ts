import { DecimalU64Schema } from "@zhongduan/protocol";
import { z } from "zod";

const identifier = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);

export const SocketAttachmentSchema = z.strictObject({
  version: z.literal(2),
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
  controlState: z.enum(["awaiting-attach", "active"]).nullable(),
  dataState: z.enum(["catching-up", "synced"]).nullable(),
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
});

export type SocketAttachment = z.infer<typeof SocketAttachmentSchema>;
export type RelayChannel = SocketAttachment["channel"];

export function readSocketAttachment(webSocket: WebSocket): SocketAttachment | undefined {
  const parsed = SocketAttachmentSchema.safeParse(webSocket.deserializeAttachment());
  return parsed.success ? parsed.data : undefined;
}

export function writeSocketAttachment(webSocket: WebSocket, attachment: SocketAttachment): void {
  webSocket.serializeAttachment(SocketAttachmentSchema.parse(attachment));
}
