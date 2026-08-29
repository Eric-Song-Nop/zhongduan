import { z } from "zod";

import { AuthorityCursorSchema } from "./authority-cursor";
import {
  RecoveryResourceIdSchema,
  RecoverySourceClosedSchema,
  type RecoverySourceClosed,
} from "./recovery-v3-control";
import { DecimalU64Schema, PositiveDecimalU64Schema } from "./scalars";
import { SnapshotResourceIdSchema } from "./snapshot";

const streamId = z.number().int().min(1).max(0xffff_ffff);
const engineId = z.string().min(1).max(512);

export const RecoveryV3HostRoutingIdentitySchema = z.strictObject({
  recoveryId: RecoveryResourceIdSchema,
  connectionId: RecoveryResourceIdSchema,
  streamId,
  deliveryGeneration: PositiveDecimalU64Schema,
});
export type RecoveryV3HostRoutingIdentity = z.infer<typeof RecoveryV3HostRoutingIdentitySchema>;

export const RecoveryV3HostRecoverySourceSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("warm") }),
  z.strictObject({
    kind: z.literal("snapshot"),
    snapshotId: SnapshotResourceIdSchema,
  }),
]);
export type RecoveryV3HostRecoverySource = z.infer<typeof RecoveryV3HostRecoverySourceSchema>;

const routingIdentity = RecoveryV3HostRoutingIdentitySchema.shape;

export const RecoveryV3HostPrepareSchema = z.strictObject({
  type: z.literal("recovery-prepare"),
  ...routingIdentity,
  engineId,
  base: AuthorityCursorSchema,
  source: RecoveryV3HostRecoverySourceSchema,
});
export type RecoveryV3HostPrepare = z.infer<typeof RecoveryV3HostPrepareSchema>;

export const RecoveryV3HostStartReadySchema = z.strictObject({
  type: z.literal("recovery-start-ready"),
  ...routingIdentity,
  committedThrough: AuthorityCursorSchema,
  cumulativeGrantedEncodedBytes: DecimalU64Schema,
});
export type RecoveryV3HostStartReady = z.infer<typeof RecoveryV3HostStartReadySchema>;

export const RecoveryV3HostSourceGrantSchema = z.strictObject({
  type: z.literal("recovery-source-grant"),
  ...routingIdentity,
  cumulativeGrantedEncodedBytes: DecimalU64Schema,
});
export type RecoveryV3HostSourceGrant = z.infer<typeof RecoveryV3HostSourceGrantSchema>;

export const RecoveryV3HostSourceReceivedSchema = z.strictObject({
  type: z.literal("recovery-source-received"),
  ...routingIdentity,
  lane: z.literal("recovery"),
  contiguousDeliveryOrdinal: PositiveDecimalU64Schema,
  cumulativeEncodedBytes: PositiveDecimalU64Schema,
});
export type RecoveryV3HostSourceReceived = z.infer<typeof RecoveryV3HostSourceReceivedSchema>;

export const RecoveryV3HostSourceResetSchema = z.strictObject({
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
export type RecoveryV3HostSourceReset = z.infer<typeof RecoveryV3HostSourceResetSchema>;

export const RecoveryV3HostPrepareRejectedSchema = z.strictObject({
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
export type RecoveryV3HostPrepareRejected = z.infer<typeof RecoveryV3HostPrepareRejectedSchema>;

export const RecoveryV3HostSourceClosedSchema = z.strictObject({
  type: z.literal("recovery-source-closed"),
  ...routingIdentity,
  throughRecoveryOrdinal: PositiveDecimalU64Schema,
  throughRecoveryCumulativeEncodedBytes: PositiveDecimalU64Schema,
});
export type RecoveryV3HostSourceClosed = z.infer<typeof RecoveryV3HostSourceClosedSchema>;

export const RecoveryV3CloudToHostControlFrameSchema = z.discriminatedUnion("type", [
  RecoveryV3HostPrepareSchema,
  RecoveryV3HostStartReadySchema,
  RecoveryV3HostSourceGrantSchema,
  RecoveryV3HostSourceReceivedSchema,
  RecoveryV3HostSourceResetSchema,
]);
export type RecoveryV3CloudToHostControlFrame = z.infer<
  typeof RecoveryV3CloudToHostControlFrameSchema
>;

export const RecoveryV3HostToCloudControlFrameSchema = z.discriminatedUnion("type", [
  RecoveryV3HostPrepareRejectedSchema,
  RecoveryV3HostSourceClosedSchema,
]);
export type RecoveryV3HostToCloudControlFrame = z.infer<
  typeof RecoveryV3HostToCloudControlFrameSchema
>;

export function toBrowserRecoverySourceClosed(
  input: RecoveryV3HostSourceClosed,
): RecoverySourceClosed {
  const sourceClosed = RecoveryV3HostSourceClosedSchema.parse(input);
  return RecoverySourceClosedSchema.parse({
    type: sourceClosed.type,
    recoveryId: sourceClosed.recoveryId,
    deliveryGeneration: sourceClosed.deliveryGeneration,
    throughRecoveryOrdinal: sourceClosed.throughRecoveryOrdinal,
    throughRecoveryCumulativeEncodedBytes: sourceClosed.throughRecoveryCumulativeEncodedBytes,
  });
}
