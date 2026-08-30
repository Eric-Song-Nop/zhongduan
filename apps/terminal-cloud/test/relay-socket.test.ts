import { describe, expect, it } from "vitest";
import {
  normalizeSocketAttachment,
  readSocketAttachment,
  type SocketAttachment,
  SocketAttachmentSchema,
  writeSocketAttachment,
} from "../src/worker/relay-socket";

class AttachmentSlot {
  constructor(public value: unknown) {}

  deserializeAttachment(): unknown {
    return this.value;
  }

  serializeAttachment(value: unknown): void {
    this.value = value;
  }
}

const hostAttachment = {
  peer: "host",
  channel: "control",
  connectionSetId: "connection_set_0000000001",
  connectionId: "connection_id_00000000001",
  subject: "subject_attachment_000000001",
  clientId: null,
  role: "host",
  streamId: 0,
  deliveryGeneration: "0",
  hostFence: "3",
  leaseFence: null,
  recoveryLookupKey: null,
  ready: true,
} as const satisfies SocketAttachment;

const browserAttachment = {
  peer: "browser",
  channel: "data",
  connectionSetId: "connection_set_0000000002",
  connectionId: "connection_id_00000000002",
  subject: "subject_attachment_000000002",
  clientId: "client_attachment_0000000002",
  role: "observer",
  streamId: 13,
  deliveryGeneration: "17",
  hostFence: null,
  leaseFence: null,
  recoveryLookupKey: "recovery_attachment_00000002",
  ready: false,
} as const satisfies SocketAttachment;

describe("relay socket attachments", () => {
  it.each([
    ["Host", hostAttachment],
    ["Browser", browserAttachment],
  ] as const)("roundtrips an exact %s attachment", (_name, attachment) => {
    expect(SocketAttachmentSchema.safeParse(attachment).success).toBe(true);
    expect(normalizeSocketAttachment(attachment)).toEqual(attachment);

    const slot = new AttachmentSlot(undefined);
    writeSocketAttachment(slot, attachment);

    expect(slot.value).toEqual(attachment);
    expect(readSocketAttachment(slot)).toEqual(attachment);
  });

  it("fails closed on every unknown attachment field", () => {
    for (const invalid of [
      { ...browserAttachment, obsoleteWireMarker: 7 },
      { ...browserAttachment, obsoleteDeliveryMode: "other" },
      { ...browserAttachment, obsoleteCapabilities: [] },
      { ...browserAttachment, obsoleteControlState: null },
      { ...browserAttachment, obsoleteCursor: "1" },
    ]) {
      expect(normalizeSocketAttachment(invalid)).toBeUndefined();
      expect(readSocketAttachment(new AttachmentSlot(invalid))).toBeUndefined();
    }

    const slot = new AttachmentSlot(undefined);
    expect(() =>
      writeSocketAttachment(slot, { ...browserAttachment, obsoleteField: true } as never),
    ).toThrow();
    expect(slot.value).toBeUndefined();
  });

  it("rejects invalid Host and Browser identities", () => {
    const invalidAttachments = [
      { ...hostAttachment, clientId: browserAttachment.clientId },
      { ...hostAttachment, role: "writer" },
      { ...hostAttachment, streamId: 1 },
      { ...hostAttachment, deliveryGeneration: "1" },
      { ...hostAttachment, hostFence: null },
      { ...hostAttachment, leaseFence: "1" },
      { ...hostAttachment, recoveryLookupKey: browserAttachment.recoveryLookupKey },
      { ...browserAttachment, clientId: null },
      { ...browserAttachment, role: "host" },
      { ...browserAttachment, streamId: 0 },
      { ...browserAttachment, deliveryGeneration: "0" },
      { ...browserAttachment, hostFence: "3" },
    ];

    for (const attachment of invalidAttachments) {
      expect(SocketAttachmentSchema.safeParse(attachment).success).toBe(false);
      expect(normalizeSocketAttachment(attachment)).toBeUndefined();
    }
  });
});
