import { z } from "zod";

import { DecimalU64Schema, PositiveDecimalU64Schema } from "./scalars";

export const CloudResourceIdSchema = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);
export const CapabilityRoleSchema = z.enum(["host", "writer", "observer"]);
export const BrowserCapabilityRoleSchema = z.enum(["writer", "observer"]);

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
});

export const CapabilityMintRequestSchema = z.strictObject({
  role: BrowserCapabilityRoleSchema,
});

export const CapabilityRefreshRequestSchema = z.strictObject({});

export const HostCapabilityReclaimRequestSchema = z.strictObject({
  engineId: z.string().min(1).max(512),
  sessionEpoch: PositiveDecimalU64Schema,
});

export const CapabilityResponseSchema = z.strictObject({
  capability: z.string().min(1).max(4_096),
  expiresAt: z.number().int().positive(),
  role: CapabilityRoleSchema,
});

export type CapabilityRole = z.infer<typeof CapabilityRoleSchema>;
export type BrowserCapabilityRole = z.infer<typeof BrowserCapabilityRoleSchema>;
export type ConnectionSetRequest = z.infer<typeof ConnectionSetRequestSchema>;
export type ConnectionSetResponse = z.infer<typeof ConnectionSetResponseSchema>;
export type CapabilityResponse = z.infer<typeof CapabilityResponseSchema>;
