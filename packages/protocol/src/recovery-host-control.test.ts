import { describe, expect, it } from "vitest";

import { HostControlFrameSchema, RelayToHostControlFrameSchema } from "./control-frame";
import {
  RecoveryCloudToHostControlFrameSchema,
  RecoveryHostPrepareRejectedSchema,
  RecoveryHostPrepareSchema,
  RecoveryHostRoutingIdentitySchema,
  RecoveryHostSourceClosedSchema,
  RecoveryHostSourceGrantSchema,
  RecoveryHostSourceReceivedSchema,
  RecoveryHostSourceResetSchema,
  RecoveryHostStartReadySchema,
  RecoveryHostToCloudControlFrameSchema,
  toBrowserRecoverySourceClosed,
} from "./recovery-host-control";
import { RecoverySourceClosedSchema } from "./recovery-control";
import { MAX_U64 } from "./scalars";

const routing = {
  recoveryId: "recovery_AAAAAAAAAAA",
  connectionId: "connection_AAAAAAAAA",
  streamId: 42,
  deliveryGeneration: "3",
} as const;
const base = { sessionEpoch: "7", eventSeq: "10", nextPtyOffset: "100" } as const;
const committedThrough = {
  sessionEpoch: "7",
  eventSeq: "12",
  nextPtyOffset: "120",
} as const;
const prepare = {
  type: "recovery-prepare",
  ...routing,
  engineId: "engine",
  base,
  source: { kind: "warm" },
} as const;
const startReady = {
  type: "recovery-start-ready",
  ...routing,
  committedThrough,
  cumulativeGrantedEncodedBytes: "4096",
} as const;
const sourceGrant = {
  type: "recovery-source-grant",
  ...routing,
  cumulativeGrantedEncodedBytes: "8192",
} as const;
const sourceReceived = {
  type: "recovery-source-received",
  ...routing,
  lane: "recovery",
  contiguousDeliveryOrdinal: "4",
  cumulativeEncodedBytes: "400",
} as const;
const sourceReset = {
  type: "recovery-source-reset",
  ...routing,
  reason: "generation-reset",
} as const;
const prepareRejected = {
  type: "recovery-prepare-rejected",
  ...routing,
  reason: "journal-gap",
} as const;
const sourceClosed = {
  type: "recovery-source-closed",
  ...routing,
  throughRecoveryOrdinal: "4",
  throughRecoveryCumulativeEncodedBytes: "400",
} as const;

const cloudToHostCases = [
  [RecoveryHostPrepareSchema, prepare],
  [RecoveryHostStartReadySchema, startReady],
  [RecoveryHostSourceGrantSchema, sourceGrant],
  [RecoveryHostSourceReceivedSchema, sourceReceived],
  [RecoveryHostSourceResetSchema, sourceReset],
] as const;
const hostToCloudCases = [
  [RecoveryHostPrepareRejectedSchema, prepareRejected],
  [RecoveryHostSourceClosedSchema, sourceClosed],
] as const;
const allCases = [...cloudToHostCases, ...hostToCloudCases] as const;

describe("Recovery Host control contracts", () => {
  it("accepts every exact frame in only its isolated direction union", () => {
    for (const [schema, frame] of cloudToHostCases) {
      expect(schema.parse(frame)).toEqual(frame);
      expect(RecoveryCloudToHostControlFrameSchema.parse(frame)).toEqual(frame);
      expect(RecoveryHostToCloudControlFrameSchema.safeParse(frame).success).toBe(false);
    }
    for (const [schema, frame] of hostToCloudCases) {
      expect(schema.parse(frame)).toEqual(frame);
      expect(RecoveryHostToCloudControlFrameSchema.parse(frame)).toEqual(frame);
      expect(RecoveryCloudToHostControlFrameSchema.safeParse(frame).success).toBe(false);
    }
  });

  it("makes each frame reachable through its production direction union", () => {
    for (const [, frame] of cloudToHostCases) {
      expect(RelayToHostControlFrameSchema.safeParse(frame).success).toBe(true);
    }
    for (const [, frame] of hostToCloudCases) {
      expect(HostControlFrameSchema.safeParse(frame).success).toBe(true);
    }
  });

  it("requires the complete bounded routing identity", () => {
    expect(RecoveryHostRoutingIdentitySchema.parse(routing)).toEqual(routing);
    for (const field of ["recoveryId", "connectionId", "streamId", "deliveryGeneration"] as const) {
      const invalid = { ...routing } as Record<string, unknown>;
      delete invalid[field];
      expect(RecoveryHostRoutingIdentitySchema.safeParse(invalid).success).toBe(false);
      for (const [schema, frame] of allCases) {
        expect(schema.safeParse({ ...frame, [field]: undefined }).success).toBe(false);
      }
    }
    expect(RecoveryHostRoutingIdentitySchema.safeParse({ ...routing, unknown: true }).success).toBe(
      false,
    );
    expect(
      RecoveryHostRoutingIdentitySchema.safeParse({ ...routing, recoveryId: "too-short" }).success,
    ).toBe(false);
    expect(
      RecoveryHostRoutingIdentitySchema.safeParse({ ...routing, connectionId: "too-short" })
        .success,
    ).toBe(false);
  });

  it("strictly binds warm or snapshot source to prepare", () => {
    expect(
      RecoveryHostPrepareSchema.parse({
        ...prepare,
        source: { kind: "snapshot", snapshotId: "snapshot_AAAAAAAAAAA" },
      }).source,
    ).toEqual({ kind: "snapshot", snapshotId: "snapshot_AAAAAAAAAAA" });
    expect(
      RecoveryHostPrepareSchema.safeParse({
        ...prepare,
        source: { kind: "warm", snapshotId: "snapshot_AAAAAAAAAAA" },
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostPrepareSchema.safeParse({
        ...prepare,
        source: { kind: "snapshot" },
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostPrepareSchema.safeParse({
        ...prepare,
        source: { kind: "snapshot", snapshotId: "too-short" },
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostPrepareSchema.safeParse({ ...prepare, base: { ...base, unknown: true } }).success,
    ).toBe(false);
  });

  it("accepts only recovery-lane source receipts", () => {
    expect(RecoveryHostSourceReceivedSchema.parse(sourceReceived)).toEqual(sourceReceived);
    expect(
      RecoveryHostSourceReceivedSchema.safeParse({ ...sourceReceived, lane: "live" }).success,
    ).toBe(false);
  });

  it("requires the exact committed-through cursor on start-ready", () => {
    expect(RecoveryHostStartReadySchema.parse(startReady).committedThrough).toEqual(
      committedThrough,
    );
    expect(
      RecoveryHostStartReadySchema.safeParse({
        ...startReady,
        committedThrough: { ...committedThrough, unknown: true },
      }).success,
    ).toBe(false);
    const { eventSeq: _eventSeq, ...incomplete } = committedThrough;
    expect(
      RecoveryHostStartReadySchema.safeParse({ ...startReady, committedThrough: incomplete })
        .success,
    ).toBe(false);
  });

  it("rejects unknown fields in every frame", () => {
    for (const [schema, frame] of allCases) {
      expect(schema.safeParse({ ...frame, unknown: true }).success).toBe(false);
    }
    expect(
      RecoveryCloudToHostControlFrameSchema.safeParse({ ...prepare, type: "recovery-unknown" })
        .success,
    ).toBe(false);
    expect(
      RecoveryHostSourceResetSchema.safeParse({ ...sourceReset, reason: "unknown" }).success,
    ).toBe(false);
    expect(
      RecoveryHostPrepareRejectedSchema.safeParse({
        ...prepareRejected,
        reason: "unknown",
      }).success,
    ).toBe(false);
  });

  it("enforces decimal u64 and stream boundaries without excluding a zero grant", () => {
    const max = MAX_U64.toString();
    const overflow = (MAX_U64 + 1n).toString();

    expect(
      RecoveryHostStartReadySchema.parse({
        ...startReady,
        deliveryGeneration: max,
        streamId: 0xffff_ffff,
        committedThrough: {
          sessionEpoch: max,
          eventSeq: max,
          nextPtyOffset: max,
        },
        cumulativeGrantedEncodedBytes: "0",
      }),
    ).toBeDefined();
    expect(
      RecoveryHostStartReadySchema.safeParse({
        ...startReady,
        deliveryGeneration: "0",
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostStartReadySchema.safeParse({
        ...startReady,
        deliveryGeneration: overflow,
      }).success,
    ).toBe(false);
    expect(RecoveryHostStartReadySchema.safeParse({ ...startReady, streamId: 0 }).success).toBe(
      false,
    );
    expect(
      RecoveryHostStartReadySchema.safeParse({
        ...startReady,
        streamId: 0x1_0000_0000,
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostStartReadySchema.safeParse({
        ...startReady,
        cumulativeGrantedEncodedBytes: overflow,
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostStartReadySchema.safeParse({
        ...startReady,
        committedThrough: { ...committedThrough, eventSeq: overflow },
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostPrepareSchema.parse({
        ...prepare,
        base: { sessionEpoch: max, eventSeq: max, nextPtyOffset: max },
      }),
    ).toBeDefined();
    expect(
      RecoveryHostPrepareSchema.safeParse({
        ...prepare,
        base: { ...base, nextPtyOffset: overflow },
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostSourceGrantSchema.parse({
        ...sourceGrant,
        cumulativeGrantedEncodedBytes: max,
      }),
    ).toBeDefined();
    expect(
      RecoveryHostSourceReceivedSchema.parse({
        ...sourceReceived,
        contiguousDeliveryOrdinal: max,
        cumulativeEncodedBytes: max,
      }),
    ).toBeDefined();
    expect(
      RecoveryHostSourceReceivedSchema.safeParse({
        ...sourceReceived,
        contiguousDeliveryOrdinal: "0",
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostSourceReceivedSchema.safeParse({
        ...sourceReceived,
        cumulativeEncodedBytes: "0",
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostSourceReceivedSchema.safeParse({
        ...sourceReceived,
        contiguousDeliveryOrdinal: overflow,
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostSourceClosedSchema.parse({
        ...sourceClosed,
        throughRecoveryOrdinal: max,
        throughRecoveryCumulativeEncodedBytes: max,
      }),
    ).toBeDefined();
    expect(
      RecoveryHostSourceClosedSchema.safeParse({
        ...sourceClosed,
        throughRecoveryOrdinal: "0",
      }).success,
    ).toBe(false);
    expect(
      RecoveryHostSourceClosedSchema.safeParse({
        ...sourceClosed,
        throughRecoveryCumulativeEncodedBytes: overflow,
      }).success,
    ).toBe(false);
  });

  it("losslessly projects the routed closure onto the Browser certificate", () => {
    const browserSourceClosed = toBrowserRecoverySourceClosed(sourceClosed);

    expect(browserSourceClosed).toEqual({
      type: "recovery-source-closed",
      recoveryId: sourceClosed.recoveryId,
      deliveryGeneration: sourceClosed.deliveryGeneration,
      throughRecoveryOrdinal: sourceClosed.throughRecoveryOrdinal,
      throughRecoveryCumulativeEncodedBytes: sourceClosed.throughRecoveryCumulativeEncodedBytes,
    });
    expect(RecoverySourceClosedSchema.parse(browserSourceClosed)).toEqual(browserSourceClosed);
  });
});
