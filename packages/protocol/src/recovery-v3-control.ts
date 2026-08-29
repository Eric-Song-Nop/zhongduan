import { z } from "zod";

import {
  AuthorityCursorSchema,
  AuthorityDataVersionSchema,
  MutationBoundarySchema,
  successorBoundary,
} from "./authority-cursor";
import { PositiveDecimalU64Schema } from "./scalars";
import { SnapshotMetadataSchema } from "./snapshot";

export const RecoveryResourceIdSchema = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);

const streamId = z.number().int().min(1).max(0xffff_ffff);
const engineId = z.string().min(1).max(512);
const snapshotDownloadPrefix = "/api/v1/sessions/";
const snapshotDownloadSeparator = "/snapshots/";
const maxSnapshotDownloadPathChars =
  snapshotDownloadPrefix.length + 128 + snapshotDownloadSeparator.length + 128;

const WarmRecoverySourceSchema = z.strictObject({ kind: z.literal("warm") });
const SnapshotRecoverySourceSchema = SnapshotMetadataSchema.safeExtend({
  kind: z.literal("snapshot"),
  downloadPath: z.string().max(maxSnapshotDownloadPathChars),
  restoreThrough: z.literal("finish"),
}).refine(
  (source) =>
    source.downloadPath ===
    `${snapshotDownloadPrefix}${source.sessionId}${snapshotDownloadSeparator}${source.snapshotId}`,
  {
    message: "snapshot download path must exactly match its session and snapshot IDs",
    path: ["downloadPath"],
  },
);

export const RecoverySourceSchema = z.discriminatedUnion("kind", [
  WarmRecoverySourceSchema,
  SnapshotRecoverySourceSchema,
]);
export type RecoverySource = z.infer<typeof RecoverySourceSchema>;

function addCursorIssue(context: z.RefinementCtx, message: string, path: PropertyKey[]): void {
  context.addIssue({ code: "custom", message, path });
}

export const RecoveryStartSchema = z
  .strictObject({
    type: z.literal("recovery-start"),
    recoveryId: RecoveryResourceIdSchema,
    deliveryGeneration: PositiveDecimalU64Schema,
    streamId,
    engineId,
    authorityDataVersion: AuthorityDataVersionSchema,
    base: AuthorityCursorSchema,
    source: RecoverySourceSchema,
    committedThrough: AuthorityCursorSchema,
    liveFloor: MutationBoundarySchema,
  })
  .superRefine((start, context) => {
    if (
      start.base.sessionEpoch !== start.committedThrough.sessionEpoch ||
      start.liveFloor.sessionEpoch !== start.committedThrough.sessionEpoch
    ) {
      addCursorIssue(context, "recovery cursors must share one session epoch", ["base"]);
    }
    if (
      BigInt(start.base.eventSeq) > BigInt(start.committedThrough.eventSeq) ||
      BigInt(start.base.nextPtyOffset) > BigInt(start.committedThrough.nextPtyOffset)
    ) {
      addCursorIssue(context, "recovery base must not exceed committed-through", ["base"]);
    }
    if (
      start.base.eventSeq === start.committedThrough.eventSeq &&
      start.base.nextPtyOffset !== start.committedThrough.nextPtyOffset
    ) {
      addCursorIssue(context, "the same authority event must have one PTY offset", ["base"]);
    }
    try {
      const expected = successorBoundary(start.committedThrough);
      if (
        start.liveFloor.sessionEpoch !== expected.sessionEpoch ||
        start.liveFloor.nextEventSeq !== expected.nextEventSeq ||
        start.liveFloor.nextPtyOffset !== expected.nextPtyOffset
      ) {
        addCursorIssue(context, "live floor must be the exact successor boundary", ["liveFloor"]);
      }
    } catch {
      addCursorIssue(context, "committed-through has no live successor", ["committedThrough"]);
    }
    if (
      start.source.kind === "snapshot" &&
      (start.source.engineId !== start.engineId ||
        start.source.sessionEpoch !== start.base.sessionEpoch ||
        start.source.cutEventSeq !== start.base.eventSeq ||
        start.source.nextPtyOffset !== start.base.nextPtyOffset)
    ) {
      addCursorIssue(context, "snapshot content metadata must exactly bind the recovery base", [
        "source",
      ]);
    }
  });
export type RecoveryStart = z.infer<typeof RecoveryStartSchema>;

export const DeliveryReceivedSchema = z.strictObject({
  type: z.literal("delivery-received"),
  deliveryGeneration: PositiveDecimalU64Schema,
  lane: z.enum(["live", "recovery"]),
  contiguousDeliveryOrdinal: PositiveDecimalU64Schema,
  cumulativeEncodedBytes: PositiveDecimalU64Schema,
});
export type DeliveryReceived = z.infer<typeof DeliveryReceivedSchema>;

export const ReplicaAppliedSchema = z.strictObject({
  type: z.literal("replica-applied"),
  deliveryGeneration: PositiveDecimalU64Schema,
  authorityCursor: AuthorityCursorSchema,
});
export type ReplicaApplied = z.infer<typeof ReplicaAppliedSchema>;

export const RecoveryAdoptedSchema = z.strictObject({
  type: z.literal("recovery-adopted"),
  recoveryId: RecoveryResourceIdSchema,
  deliveryGeneration: PositiveDecimalU64Schema,
  replicaApplied: AuthorityCursorSchema,
});
export type RecoveryAdopted = z.infer<typeof RecoveryAdoptedSchema>;

// Reconstructed from a recovery-lane DeliveryEnvelope carrying canonical v2
// REPLAY_COMMIT; this is deliberately not a standalone control frame.
export const RecoveryDoneRecordSchema = z.strictObject({
  type: z.literal("recovery-done"),
  recoveryId: RecoveryResourceIdSchema,
  deliveryGeneration: PositiveDecimalU64Schema,
  replayedThrough: AuthorityCursorSchema,
  recoveryOrdinal: PositiveDecimalU64Schema,
  cumulativeEncodedBytes: PositiveDecimalU64Schema,
});
export type RecoveryDoneRecord = z.infer<typeof RecoveryDoneRecordSchema>;

export const RecoverySourceClosedSchema = z.strictObject({
  type: z.literal("recovery-source-closed"),
  recoveryId: RecoveryResourceIdSchema,
  deliveryGeneration: PositiveDecimalU64Schema,
  throughRecoveryOrdinal: PositiveDecimalU64Schema,
  throughRecoveryCumulativeEncodedBytes: PositiveDecimalU64Schema,
});
export type RecoverySourceClosed = z.infer<typeof RecoverySourceClosedSchema>;

export const RecoveryV3ClientControlFrameSchema = z.discriminatedUnion("type", [
  DeliveryReceivedSchema,
  ReplicaAppliedSchema,
  RecoveryAdoptedSchema,
]);
export type RecoveryV3ClientControlFrame = z.infer<typeof RecoveryV3ClientControlFrameSchema>;

export const RecoveryV3ServerControlFrameSchema = z.discriminatedUnion("type", [
  RecoveryStartSchema,
  RecoverySourceClosedSchema,
]);
export type RecoveryV3ServerControlFrame = z.infer<typeof RecoveryV3ServerControlFrameSchema>;
