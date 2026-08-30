import { z } from "zod";

import { AuthorityCursorSchema } from "./authority-cursor";
import {
  RecoveryResourceIdSchema,
  RecoverySourceClosedSchema,
  type RecoverySourceClosed,
} from "./recovery-control";
import { DecimalU64Schema, PositiveDecimalU64Schema } from "./scalars";
import { SnapshotResourceIdSchema } from "./snapshot";

const streamId = z.number().int().min(1).max(0xffff_ffff);
const engineId = z.string().min(1).max(512);

export const RecoveryHostRoutingIdentitySchema = z.strictObject({
  recoveryId: RecoveryResourceIdSchema,
  connectionId: RecoveryResourceIdSchema,
  streamId,
  deliveryGeneration: PositiveDecimalU64Schema,
});
export type RecoveryHostRoutingIdentity = z.infer<typeof RecoveryHostRoutingIdentitySchema>;

export const RecoveryHostRecoverySourceSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("warm") }),
  z.strictObject({
    kind: z.literal("snapshot"),
    snapshotId: SnapshotResourceIdSchema,
  }),
]);
export type RecoveryHostRecoverySource = z.infer<typeof RecoveryHostRecoverySourceSchema>;

const routingIdentity = RecoveryHostRoutingIdentitySchema.shape;

export const RecoveryHostPrepareSchema = z.strictObject({
  type: z.literal("recovery-prepare"),
  ...routingIdentity,
  engineId,
  base: AuthorityCursorSchema,
  source: RecoveryHostRecoverySourceSchema,
});
export type RecoveryHostPrepare = z.infer<typeof RecoveryHostPrepareSchema>;

export const RecoveryHostStartReadySchema = z.strictObject({
  type: z.literal("recovery-start-ready"),
  ...routingIdentity,
  committedThrough: AuthorityCursorSchema,
  cumulativeGrantedEncodedBytes: DecimalU64Schema,
});
export type RecoveryHostStartReady = z.infer<typeof RecoveryHostStartReadySchema>;

export const RecoveryHostSourceGrantSchema = z.strictObject({
  type: z.literal("recovery-source-grant"),
  ...routingIdentity,
  cumulativeGrantedEncodedBytes: DecimalU64Schema,
});
export type RecoveryHostSourceGrant = z.infer<typeof RecoveryHostSourceGrantSchema>;

export const RecoveryHostSourceReceivedSchema = z.strictObject({
  type: z.literal("recovery-source-received"),
  ...routingIdentity,
  lane: z.literal("recovery"),
  contiguousDeliveryOrdinal: PositiveDecimalU64Schema,
  cumulativeEncodedBytes: PositiveDecimalU64Schema,
});
export type RecoveryHostSourceReceived = z.infer<typeof RecoveryHostSourceReceivedSchema>;

export const RecoveryHostSourceResetSchema = z.strictObject({
  type: z.literal("recovery-source-reset"),
  ...routingIdentity,
  reason: z.enum([
    "generation-reset",
    "start-send-failed",
    "ack-outcome-uncertain",
    "deadline",
    "pair-fenced",
    "session-disposed",
  ]),
});
export type RecoveryHostSourceReset = z.infer<typeof RecoveryHostSourceResetSchema>;

export const RecoveryHostPrepareRejectedSchema = z.strictObject({
  type: z.literal("recovery-prepare-rejected"),
  ...routingIdentity,
  reason: z.enum([
    "engine-mismatch",
    "epoch-changed",
    "journal-gap",
    "snapshot-missing",
    "capacity-exceeded",
    "generation-fenced",
    "client-gone",
  ]),
});
export type RecoveryHostPrepareRejected = z.infer<typeof RecoveryHostPrepareRejectedSchema>;

export const RecoveryHostSourceClosedSchema = z.strictObject({
  type: z.literal("recovery-source-closed"),
  ...routingIdentity,
  throughRecoveryOrdinal: PositiveDecimalU64Schema,
  throughRecoveryCumulativeEncodedBytes: PositiveDecimalU64Schema,
});
export type RecoveryHostSourceClosed = z.infer<typeof RecoveryHostSourceClosedSchema>;

export const RecoveryCloudToHostControlFrameSchema = z.discriminatedUnion("type", [
  RecoveryHostPrepareSchema,
  RecoveryHostStartReadySchema,
  RecoveryHostSourceGrantSchema,
  RecoveryHostSourceReceivedSchema,
  RecoveryHostSourceResetSchema,
]);
export type RecoveryCloudToHostControlFrame = z.infer<typeof RecoveryCloudToHostControlFrameSchema>;

export const RecoveryHostToCloudControlFrameSchema = z.discriminatedUnion("type", [
  RecoveryHostPrepareRejectedSchema,
  RecoveryHostSourceClosedSchema,
]);
export type RecoveryHostToCloudControlFrame = z.infer<typeof RecoveryHostToCloudControlFrameSchema>;

export function toBrowserRecoverySourceClosed(
  input: RecoveryHostSourceClosed,
): RecoverySourceClosed {
  const sourceClosed = RecoveryHostSourceClosedSchema.parse(input);
  return RecoverySourceClosedSchema.parse({
    type: sourceClosed.type,
    recoveryId: sourceClosed.recoveryId,
    deliveryGeneration: sourceClosed.deliveryGeneration,
    throughRecoveryOrdinal: sourceClosed.throughRecoveryOrdinal,
    throughRecoveryCumulativeEncodedBytes: sourceClosed.throughRecoveryCumulativeEncodedBytes,
  });
}
