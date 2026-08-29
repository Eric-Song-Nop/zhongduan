import { z } from "zod";

import { DecimalU64Schema, MAX_U64, PositiveDecimalU64Schema } from "./scalars";

export const AuthorityDataVersionSchema = z.literal(2);
export type AuthorityDataVersion = z.infer<typeof AuthorityDataVersionSchema>;

export const AuthorityCursorSchema = z.strictObject({
  sessionEpoch: PositiveDecimalU64Schema,
  eventSeq: DecimalU64Schema,
  nextPtyOffset: DecimalU64Schema,
});
export type AuthorityCursor = z.infer<typeof AuthorityCursorSchema>;

export const MutationBoundarySchema = z.strictObject({
  sessionEpoch: PositiveDecimalU64Schema,
  nextEventSeq: PositiveDecimalU64Schema,
  nextPtyOffset: DecimalU64Schema,
});
export type MutationBoundary = z.infer<typeof MutationBoundarySchema>;

export function successorBoundary(cursor: AuthorityCursor): MutationBoundary {
  const parsed = AuthorityCursorSchema.parse(cursor);
  const eventSeq = BigInt(parsed.eventSeq);
  if (eventSeq === MAX_U64) {
    throw new RangeError("authority cursor has no uint64 successor");
  }
  return {
    sessionEpoch: parsed.sessionEpoch,
    nextEventSeq: (eventSeq + 1n).toString(),
    nextPtyOffset: parsed.nextPtyOffset,
  };
}
