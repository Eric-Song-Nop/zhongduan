import { z } from "zod";

import { DecimalU64Schema, PositiveDecimalU64Schema } from "./scalars";
import { SnapshotMetadataSchema } from "./snapshot";
import { RelayCapabilitySchema } from "./wire-capabilities";

export {
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  RelayCapabilitySchema,
  confirmedRelayCapabilities,
  selectRecoveryStrategy,
  selectRelayCapabilities,
  type RecoveryStrategy,
} from "./wire-capabilities";

export const CloudResourceIdSchema = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);
export const CapabilityRoleSchema = z.enum(["host", "writer", "observer"]);
export const BrowserCapabilityRoleSchema = z.enum(["writer", "observer"]);
const engineId = z.string().min(1).max(512);
const capability = z.string().min(1).max(4_096);
const relayCapabilities = z
  .array(RelayCapabilitySchema)
  .max(16)
  .refine((capabilities) => new Set(capabilities).size === capabilities.length, {
    message: "relay capabilities must be unique",
  });

export const CreateSessionRequestSchema = z.strictObject({
  sessionId: CloudResourceIdSchema,
  engineId,
  sessionEpoch: PositiveDecimalU64Schema,
});

export const CreateSessionResponseSchema = z.strictObject({
  sessionId: CloudResourceIdSchema,
  engineId,
  sessionEpoch: PositiveDecimalU64Schema,
  hostCapability: capability,
  hostCapabilityExpiresAt: z.number().int().positive(),
  writerCapability: capability,
  writerCapabilityExpiresAt: z.number().int().positive(),
  observerCapability: capability,
  observerCapabilityExpiresAt: z.number().int().positive(),
});

export const ConnectionSetRequestSchema = z.strictObject({
  clientId: CloudResourceIdSchema.optional(),
});

export const ConnectionSetResponseSchema = z.strictObject({
  connectionSetId: CloudResourceIdSchema,
  connectionId: CloudResourceIdSchema,
  clientId: CloudResourceIdSchema.nullable(),
  streamId: z.number().int().min(0).max(0xffff_ffff),
  deliveryGeneration: DecimalU64Schema,
  expiresAt: z.number().int().positive(),
  controlTicket: CloudResourceIdSchema,
  dataTicket: CloudResourceIdSchema,
  // Decode-only rolling shim for Durable Objects that still echo the selected intersection.
  selectedCapabilities: relayCapabilities.optional(),
  // Emitted only after the caller offers capability-negotiation-v1.
  negotiatedCapabilities: relayCapabilities.optional(),
});

export const CapabilityMintRequestSchema = z.strictObject({
  role: BrowserCapabilityRoleSchema,
});

export const CapabilityRefreshRequestSchema = z.strictObject({});

export const HostCapabilityReclaimRequestSchema = z.strictObject({
  engineId,
  sessionEpoch: PositiveDecimalU64Schema,
});

export const CapabilityResponseSchema = z.strictObject({
  capability,
  expiresAt: z.number().int().positive(),
  role: CapabilityRoleSchema,
});

export const SnapshotUploadResponseSchema = z.strictObject({
  created: z.boolean(),
  snapshot: SnapshotMetadataSchema,
});

export type CapabilityRole = z.infer<typeof CapabilityRoleSchema>;
export type BrowserCapabilityRole = z.infer<typeof BrowserCapabilityRoleSchema>;
export type CreateSessionRequest = z.infer<typeof CreateSessionRequestSchema>;
export type CreateSessionResponse = z.infer<typeof CreateSessionResponseSchema>;
export type ConnectionSetRequest = z.infer<typeof ConnectionSetRequestSchema>;
export type ConnectionSetResponse = z.infer<typeof ConnectionSetResponseSchema>;
export type CapabilityResponse = z.infer<typeof CapabilityResponseSchema>;
export type SnapshotUploadResponse = z.infer<typeof SnapshotUploadResponseSchema>;
