import { describe, expect, it } from "vitest";

import {
  DeliveryReceivedSchema,
  RecoveryAdoptedSchema,
  RecoveryDoneRecordSchema,
  RecoverySourceClosedSchema,
  RecoveryStartSchema,
  ReplicaAppliedSchema,
} from "./recovery-v3-control";

const base = { sessionEpoch: "7", eventSeq: "10", nextPtyOffset: "100" } as const;
const committedThrough = {
  sessionEpoch: "7",
  eventSeq: "12",
  nextPtyOffset: "120",
} as const;
const start = {
  type: "recovery-start",
  recoveryId: "recovery_AAAAAAAAAAA",
  deliveryGeneration: "3",
  streamId: 42,
  engineId: "engine",
  authorityDataVersion: 2,
  base,
  source: { kind: "warm" },
  committedThrough,
  liveFloor: { sessionEpoch: "7", nextEventSeq: "13", nextPtyOffset: "120" },
} as const;
const deliveryReceived = {
  type: "delivery-received",
  deliveryGeneration: "3",
  lane: "recovery",
  contiguousDeliveryOrdinal: "4",
  cumulativeEncodedBytes: "400",
} as const;
const replicaApplied = {
  type: "replica-applied",
  deliveryGeneration: "3",
  authorityCursor: committedThrough,
} as const;
const adopted = {
  type: "recovery-adopted",
  recoveryId: start.recoveryId,
  deliveryGeneration: "3",
  replicaApplied: committedThrough,
} as const;
const doneRecord = {
  type: "recovery-done",
  recoveryId: start.recoveryId,
  deliveryGeneration: "3",
  replayedThrough: committedThrough,
  recoveryOrdinal: "4",
  cumulativeEncodedBytes: "400",
} as const;
const sourceClosed = {
  type: "recovery-source-closed",
  recoveryId: start.recoveryId,
  deliveryGeneration: "3",
  throughRecoveryOrdinal: "4",
  throughRecoveryCumulativeEncodedBytes: "400",
} as const;

describe("Recovery v3 control contracts", () => {
  it("accepts an exact warm start and rejects a non-successor live floor", () => {
    expect(RecoveryStartSchema.parse(start)).toEqual(start);
    expect(() =>
      RecoveryStartSchema.parse({
        ...start,
        liveFloor: { ...start.liveFloor, nextEventSeq: "14" },
      }),
    ).toThrow();
  });

  it("rejects two PTY offsets for the same base and committed event", () => {
    expect(() =>
      RecoveryStartSchema.parse({
        ...start,
        base: { ...committedThrough, nextPtyOffset: "119" },
      }),
    ).toThrow();
  });

  it("requires complete snapshot content metadata to bind a cold base", () => {
    const cold = {
      ...start,
      source: {
        kind: "snapshot",
        sessionId: "session_AAAAAAAAA",
        snapshotId: "snapshot_AAAAAAAAAAA",
        engineId: "engine",
        sessionEpoch: "7",
        cutEventSeq: "10",
        nextPtyOffset: "100",
        compression: "zstd",
        compressedLength: "90",
        uncompressedLength: "120",
        sha256: "a".repeat(64),
        downloadPath: "/api/v1/sessions/session_AAAAAAAAA/snapshots/snapshot_AAAAAAAAAAA",
        restoreThrough: "finish",
      },
    } as const;
    expect(RecoveryStartSchema.parse(cold)).toEqual(cold);
    const { sha256: _sha256, ...incomplete } = cold.source;
    expect(() => RecoveryStartSchema.parse({ ...cold, source: incomplete })).toThrow();
    const { sessionId: _sessionId, ...withoutSessionId } = cold.source;
    expect(() => RecoveryStartSchema.parse({ ...cold, source: withoutSessionId })).toThrow();
    expect(() =>
      RecoveryStartSchema.parse({
        ...cold,
        source: { ...cold.source, cutEventSeq: "11" },
      }),
    ).toThrow();

    const alternateSessionId = "session_BBBBBBBBB";
    const alternateSnapshotId = "snapshot_BBBBBBBBBBB";
    const invalidPaths = [
      `/api/v1/sessions/${alternateSessionId}/snapshots/${cold.source.snapshotId}`,
      `/api/v1/sessions/${cold.source.sessionId}/snapshots/${alternateSnapshotId}`,
      `/api/v1/sessions/../snapshots/${cold.source.snapshotId}`,
      `${cold.source.downloadPath}?download=1`,
      `${cold.source.downloadPath}#fragment`,
      `${cold.source.downloadPath}/suffix`,
      "x".repeat(285),
    ];
    for (const downloadPath of invalidPaths) {
      expect(() =>
        RecoveryStartSchema.parse({
          ...cold,
          source: { ...cold.source, downloadPath },
        }),
      ).toThrow();
    }
  });

  it("keeps receipt, apply, adoption, and closure control shapes distinct", () => {
    expect(DeliveryReceivedSchema.parse(deliveryReceived)).toBeDefined();
    expect(ReplicaAppliedSchema.parse(replicaApplied)).toBeDefined();
    expect(RecoveryAdoptedSchema.parse(adopted)).toBeDefined();
    expect(RecoverySourceClosedSchema.parse(sourceClosed)).toBeDefined();
  });

  it("validates only the reconstructed RecoveryDone record shape", () => {
    expect(RecoveryDoneRecordSchema.parse(doneRecord)).toEqual(doneRecord);
  });

  it("rejects unknown fields in every top-level recovery record", () => {
    const parses = [
      () => RecoveryStartSchema.parse({ ...start, unknown: true }),
      () => DeliveryReceivedSchema.parse({ ...deliveryReceived, unknown: true }),
      () => ReplicaAppliedSchema.parse({ ...replicaApplied, unknown: true }),
      () => RecoveryAdoptedSchema.parse({ ...adopted, unknown: true }),
      () => RecoveryDoneRecordSchema.parse({ ...doneRecord, unknown: true }),
      () => RecoverySourceClosedSchema.parse({ ...sourceClosed, unknown: true }),
    ];

    for (const parse of parses) expect(parse).toThrow();
  });
});
