import { describe, expect, it } from "vitest";
import {
  normalizeSocketAttachment,
  readSocketAttachment,
  SocketAttachmentSchema,
  SocketAttachmentV2Schema,
  SocketAttachmentV3Schema,
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

const v2Attachment = {
  version: 2,
  peer: "browser",
  channel: "data",
  connectionSetId: "connection_set_0000000001",
  connectionId: "connection_id_00000000001",
  subject: "subject_attachment_000000001",
  clientId: "client_attachment_0000000001",
  role: "observer",
  streamId: 7,
  deliveryGeneration: "9",
  hostFence: null,
  leaseFence: null,
  controlState: null,
  dataState: "catching-up",
  firstEventSeq: "10",
  ackedEventSeq: "11",
  sentEventSeq: "12",
  firstPtyOffset: "100",
  ackedPtyOffset: "110",
  sentPtyOffset: "120",
  replayMode: "snapshot",
  snapshotId: "snapshot_attachment_0000001",
  replayCommitEventSeq: "12",
  replayCommitPtyOffset: "120",
  relayCapabilities: [],
} as const;

const browserV3Attachment = {
  version: 3,
  peer: "browser",
  channel: "data",
  connectionSetId: "connection_set_0000000003",
  connectionId: "connection_id_00000000003",
  subject: "subject_attachment_000000003",
  clientId: "client_attachment_0000000003",
  role: "observer",
  streamId: 13,
  deliveryGeneration: "17",
  hostFence: null,
  leaseFence: null,
  relayCapabilities: [],
  recoveryStrategy: "v3",
  recoveryLookupKey: "recovery_attachment_00000001",
} as const;

const normalizedV3LegacyDefaults = {
  controlState: null,
  dataState: null,
  firstEventSeq: null,
  ackedEventSeq: null,
  sentEventSeq: null,
  firstPtyOffset: null,
  ackedPtyOffset: null,
  sentPtyOffset: null,
  replayMode: null,
  snapshotId: null,
  replayCommitEventSeq: null,
  replayCommitPtyOffset: null,
} as const;

describe("versioned relay socket attachments", () => {
  it("normalizes legacy V2 and preserves its complete serialized delivery state", () => {
    const normalized = normalizeSocketAttachment(v2Attachment);
    expect(normalized).toEqual({
      ...v2Attachment,
      recoveryStrategy: "v2",
      recoveryLookupKey: null,
    });
    expect(normalized).toBeDefined();
    if (normalized === undefined) throw new Error("expected a normalized V2 attachment");

    const slot = new AttachmentSlot(undefined);
    writeSocketAttachment(slot, normalized);

    expect(slot.value).toEqual(SocketAttachmentV2Schema.parse(v2Attachment));
    expect(readSocketAttachment(slot)).toEqual(normalized);
  });

  it("roundtrips V3 through the normalized shape without durable delivery payload state", () => {
    const normalized = normalizeSocketAttachment(browserV3Attachment);
    expect(normalized).toEqual({
      ...browserV3Attachment,
      ...normalizedV3LegacyDefaults,
    });
    expect(normalized).toBeDefined();
    if (normalized === undefined) throw new Error("expected a normalized V3 attachment");

    const slot = new AttachmentSlot(undefined);
    writeSocketAttachment(slot, normalized);
    const serialized = SocketAttachmentV3Schema.parse(slot.value);

    expect(serialized).toEqual(browserV3Attachment);
    expect(Object.keys(serialized).sort()).toEqual(
      [
        "channel",
        "clientId",
        "connectionId",
        "connectionSetId",
        "deliveryGeneration",
        "hostFence",
        "leaseFence",
        "peer",
        "recoveryLookupKey",
        "recoveryStrategy",
        "relayCapabilities",
        "role",
        "streamId",
        "subject",
        "version",
      ].sort(),
    );
    const encoded = JSON.stringify(serialized);
    expect(new TextEncoder().encode(encoded).byteLength).toBeLessThanOrEqual(16 * 1024);
    expect(readSocketAttachment(slot)).toEqual(normalized);
    expect(() => writeSocketAttachment(slot, { ...normalized, firstEventSeq: "18" })).toThrow();
  });

  it("fails closed on unknown versions, unknown fields, and invalid V3 identity", () => {
    expect(normalizeSocketAttachment({ ...browserV3Attachment, version: 4 })).toBeUndefined();
    expect(
      normalizeSocketAttachment({ ...browserV3Attachment, payload: "not durable" }),
    ).toBeUndefined();
    expect(
      normalizeSocketAttachment({ ...browserV3Attachment, recoveryStrategy: "v2" }),
    ).toBeUndefined();
    expect(
      normalizeSocketAttachment({ ...browserV3Attachment, recoveryLookupKey: "too-short" }),
    ).toBeUndefined();
    expect(normalizeSocketAttachment({ ...v2Attachment, futureField: true })).toBeUndefined();
  });

  it("keeps V2 and host lookup keys empty while allowing an optional Browser V3 key", () => {
    const v2 = normalizeSocketAttachment(v2Attachment);
    expect(v2).toBeDefined();
    if (v2 === undefined) throw new Error("expected a normalized V2 attachment");
    expect(
      SocketAttachmentSchema.safeParse({
        ...v2,
        recoveryLookupKey: browserV3Attachment.recoveryLookupKey,
      }).success,
    ).toBe(false);

    const hostV3 = {
      ...browserV3Attachment,
      peer: "host",
      clientId: null,
      role: "host",
      streamId: 0,
      deliveryGeneration: "0",
      hostFence: "3",
      recoveryLookupKey: null,
    } as const;
    expect(normalizeSocketAttachment(hostV3)).toMatchObject({
      peer: "host",
      recoveryLookupKey: null,
      recoveryStrategy: "v3",
    });
    expect(
      normalizeSocketAttachment({
        ...hostV3,
        recoveryLookupKey: browserV3Attachment.recoveryLookupKey,
      }),
    ).toBeUndefined();
    expect(
      normalizeSocketAttachment({ ...browserV3Attachment, recoveryLookupKey: null }),
    ).toMatchObject({ peer: "browser", recoveryLookupKey: null, recoveryStrategy: "v3" });

    for (const invalid of [
      { ...browserV3Attachment, clientId: null },
      { ...browserV3Attachment, role: "host" },
      { ...browserV3Attachment, streamId: 0 },
      { ...browserV3Attachment, deliveryGeneration: "0" },
      { ...browserV3Attachment, hostFence: "3" },
      { ...hostV3, clientId: browserV3Attachment.clientId },
      { ...hostV3, role: "observer" },
      { ...hostV3, streamId: 1 },
      { ...hostV3, deliveryGeneration: "1" },
      { ...hostV3, hostFence: null },
      { ...hostV3, leaseFence: "1" },
    ]) {
      expect(normalizeSocketAttachment(invalid)).toBeUndefined();
    }
  });
});
