import { describe, expect, it } from "vitest";

import { HostControlFrameSchema, RelayToHostControlFrameSchema } from "./control-frame";
import {
  RecoveryV3CloudToHostControlFrameSchema,
  RecoveryV3HostPrepareRejectedSchema,
  RecoveryV3HostPrepareSchema,
  RecoveryV3HostRoutingIdentitySchema,
  RecoveryV3HostSourceClosedSchema,
  RecoveryV3HostSourceGrantSchema,
  RecoveryV3HostSourceReceivedSchema,
  RecoveryV3HostSourceResetSchema,
  RecoveryV3HostStartReadySchema,
  RecoveryV3HostToCloudControlFrameSchema,
  toBrowserRecoverySourceClosed,
} from "./recovery-v3-host-control";
import { RecoverySourceClosedSchema } from "./recovery-v3-control";
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
  [RecoveryV3HostPrepareSchema, prepare],
  [RecoveryV3HostStartReadySchema, startReady],
  [RecoveryV3HostSourceGrantSchema, sourceGrant],
  [RecoveryV3HostSourceReceivedSchema, sourceReceived],
  [RecoveryV3HostSourceResetSchema, sourceReset],
] as const;
const hostToCloudCases = [
  [RecoveryV3HostPrepareRejectedSchema, prepareRejected],
  [RecoveryV3HostSourceClosedSchema, sourceClosed],
] as const;
const allCases = [...cloudToHostCases, ...hostToCloudCases] as const;

describe("Recovery v3 Host control contracts", () => {
  it("accepts every exact frame in only its isolated direction union", () => {
    for (const [schema, frame] of cloudToHostCases) {
      expect(schema.parse(frame)).toEqual(frame);
      expect(RecoveryV3CloudToHostControlFrameSchema.parse(frame)).toEqual(frame);
      expect(RecoveryV3HostToCloudControlFrameSchema.safeParse(frame).success).toBe(false);
    }
    for (const [schema, frame] of hostToCloudCases) {
      expect(schema.parse(frame)).toEqual(frame);
      expect(RecoveryV3HostToCloudControlFrameSchema.parse(frame)).toEqual(frame);
      expect(RecoveryV3CloudToHostControlFrameSchema.safeParse(frame).success).toBe(false);
    }
  });

  it("does not make the candidate frames reachable through production control unions", () => {
    for (const [, frame] of cloudToHostCases) {
      expect(RelayToHostControlFrameSchema.safeParse(frame).success).toBe(false);
    }
    for (const [, frame] of hostToCloudCases) {
      expect(HostControlFrameSchema.safeParse(frame).success).toBe(false);
    }
  });

  it("requires the complete bounded routing identity", () => {
    expect(RecoveryV3HostRoutingIdentitySchema.parse(routing)).toEqual(routing);
    for (const field of ["recoveryId", "connectionId", "streamId", "deliveryGeneration"] as const) {
      const invalid = { ...routing } as Record<string, unknown>;
      delete invalid[field];
      expect(RecoveryV3HostRoutingIdentitySchema.safeParse(invalid).success).toBe(false);
      for (const [schema, frame] of allCases) {
        expect(schema.safeParse({ ...frame, [field]: undefined }).success).toBe(false);
      }
    }
    expect(
      RecoveryV3HostRoutingIdentitySchema.safeParse({ ...routing, unknown: true }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostRoutingIdentitySchema.safeParse({ ...routing, recoveryId: "too-short" })
        .success,
    ).toBe(false);
    expect(
      RecoveryV3HostRoutingIdentitySchema.safeParse({ ...routing, connectionId: "too-short" })
        .success,
    ).toBe(false);
  });

  it("strictly binds warm or snapshot source to prepare", () => {
    expect(
      RecoveryV3HostPrepareSchema.parse({
        ...prepare,
        source: { kind: "snapshot", snapshotId: "snapshot_AAAAAAAAAAA" },
      }).source,
    ).toEqual({ kind: "snapshot", snapshotId: "snapshot_AAAAAAAAAAA" });
    expect(
      RecoveryV3HostPrepareSchema.safeParse({
        ...prepare,
        source: { kind: "warm", snapshotId: "snapshot_AAAAAAAAAAA" },
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostPrepareSchema.safeParse({
        ...prepare,
        source: { kind: "snapshot" },
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostPrepareSchema.safeParse({
        ...prepare,
        source: { kind: "snapshot", snapshotId: "too-short" },
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostPrepareSchema.safeParse({ ...prepare, base: { ...base, unknown: true } })
        .success,
    ).toBe(false);
  });

  it("accepts only recovery-lane source receipts", () => {
    expect(RecoveryV3HostSourceReceivedSchema.parse(sourceReceived)).toEqual(sourceReceived);
    expect(
      RecoveryV3HostSourceReceivedSchema.safeParse({ ...sourceReceived, lane: "live" }).success,
    ).toBe(false);
  });

  it("requires the exact committed-through cursor on start-ready", () => {
    expect(RecoveryV3HostStartReadySchema.parse(startReady).committedThrough).toEqual(
      committedThrough,
    );
    expect(
      RecoveryV3HostStartReadySchema.safeParse({
        ...startReady,
        committedThrough: { ...committedThrough, unknown: true },
      }).success,
    ).toBe(false);
    const { eventSeq: _eventSeq, ...incomplete } = committedThrough;
    expect(
      RecoveryV3HostStartReadySchema.safeParse({ ...startReady, committedThrough: incomplete })
        .success,
    ).toBe(false);
  });

  it("rejects unknown fields in every frame", () => {
    for (const [schema, frame] of allCases) {
      expect(schema.safeParse({ ...frame, unknown: true }).success).toBe(false);
    }
    expect(
      RecoveryV3CloudToHostControlFrameSchema.safeParse({ ...prepare, type: "recovery-unknown" })
        .success,
    ).toBe(false);
    expect(
      RecoveryV3HostSourceResetSchema.safeParse({ ...sourceReset, reason: "unknown" }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostPrepareRejectedSchema.safeParse({
        ...prepareRejected,
        reason: "unknown",
      }).success,
    ).toBe(false);
  });

  it("enforces decimal u64 and stream boundaries without excluding a zero grant", () => {
    const max = MAX_U64.toString();
    const overflow = (MAX_U64 + 1n).toString();

    expect(
      RecoveryV3HostStartReadySchema.parse({
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
      RecoveryV3HostStartReadySchema.safeParse({
        ...startReady,
        deliveryGeneration: "0",
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostStartReadySchema.safeParse({
        ...startReady,
        deliveryGeneration: overflow,
      }).success,
    ).toBe(false);
    expect(RecoveryV3HostStartReadySchema.safeParse({ ...startReady, streamId: 0 }).success).toBe(
      false,
    );
    expect(
      RecoveryV3HostStartReadySchema.safeParse({
        ...startReady,
        streamId: 0x1_0000_0000,
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostStartReadySchema.safeParse({
        ...startReady,
        cumulativeGrantedEncodedBytes: overflow,
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostStartReadySchema.safeParse({
        ...startReady,
        committedThrough: { ...committedThrough, eventSeq: overflow },
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostPrepareSchema.parse({
        ...prepare,
        base: { sessionEpoch: max, eventSeq: max, nextPtyOffset: max },
      }),
    ).toBeDefined();
    expect(
      RecoveryV3HostPrepareSchema.safeParse({
        ...prepare,
        base: { ...base, nextPtyOffset: overflow },
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostSourceGrantSchema.parse({
        ...sourceGrant,
        cumulativeGrantedEncodedBytes: max,
      }),
    ).toBeDefined();
    expect(
      RecoveryV3HostSourceReceivedSchema.parse({
        ...sourceReceived,
        contiguousDeliveryOrdinal: max,
        cumulativeEncodedBytes: max,
      }),
    ).toBeDefined();
    expect(
      RecoveryV3HostSourceReceivedSchema.safeParse({
        ...sourceReceived,
        contiguousDeliveryOrdinal: "0",
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostSourceReceivedSchema.safeParse({
        ...sourceReceived,
        cumulativeEncodedBytes: "0",
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostSourceReceivedSchema.safeParse({
        ...sourceReceived,
        contiguousDeliveryOrdinal: overflow,
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostSourceClosedSchema.parse({
        ...sourceClosed,
        throughRecoveryOrdinal: max,
        throughRecoveryCumulativeEncodedBytes: max,
      }),
    ).toBeDefined();
    expect(
      RecoveryV3HostSourceClosedSchema.safeParse({
        ...sourceClosed,
        throughRecoveryOrdinal: "0",
      }).success,
    ).toBe(false);
    expect(
      RecoveryV3HostSourceClosedSchema.safeParse({
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
