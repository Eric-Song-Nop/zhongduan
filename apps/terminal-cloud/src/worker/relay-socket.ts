import { DecimalU64Schema, RecoveryResourceIdSchema } from "@zhongduan/protocol";
import { z } from "zod";

const identifier = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);

export const SocketAttachmentSchema = z
  .strictObject({
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
    recoveryLookupKey: RecoveryResourceIdSchema.nullable(),
    ready: z.boolean(),
  })
  .superRefine((attachment, context) => {
    const reject = (message: string, path: keyof typeof attachment): void => {
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
        reject("host sockets cannot bind a browser recovery", "recoveryLookupKey");
      }
      return;
    }

    if (attachment.clientId === null) reject("browser sockets require a client", "clientId");
    if (attachment.role === "host") reject("browser sockets cannot use the host role", "role");
    if (attachment.streamId === 0) reject("browser sockets require a nonzero stream", "streamId");
    if (attachment.deliveryGeneration === "0") {
      reject("browser sockets require a positive generation", "deliveryGeneration");
    }
    if (attachment.hostFence !== null) {
      reject("browser sockets cannot bind a host fence", "hostFence");
    }
  });

export type SocketAttachment = z.infer<typeof SocketAttachmentSchema>;
export type RelayChannel = SocketAttachment["channel"];

type AttachmentReader = Pick<WebSocket, "deserializeAttachment">;
type AttachmentWriter = Pick<WebSocket, "serializeAttachment">;

export function normalizeSocketAttachment(value: unknown): SocketAttachment | undefined {
  const parsed = SocketAttachmentSchema.safeParse(value);
  return parsed.success ? parsed.data : undefined;
}

export function readSocketAttachment(webSocket: AttachmentReader): SocketAttachment | undefined {
  return normalizeSocketAttachment(webSocket.deserializeAttachment());
}

export function writeSocketAttachment(
  webSocket: AttachmentWriter,
  attachment: SocketAttachment,
): void {
  webSocket.serializeAttachment(SocketAttachmentSchema.parse(attachment));
}
