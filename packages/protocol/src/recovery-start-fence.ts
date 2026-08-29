import { z } from "zod";

import {
  AuthorityCursorSchema,
  MutationBoundarySchema,
  successorBoundary,
} from "./authority-cursor";
import { DataFrameFlag, DataFrameKind, decodeDataFrame, encodeDataFrame } from "./data-frame";
import { ProtocolError } from "./errors";
import { PositiveDecimalU64Schema } from "./scalars";
import { RecoveryResourceIdSchema } from "./recovery-v3-control";
import { SnapshotResourceIdSchema } from "./snapshot";

const MAX_RECOVERY_START_FENCE_PAYLOAD_BYTES = 2_048;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });
const streamId = z.number().int().min(1).max(0xffff_ffff);
const engineId = z.string().min(1).max(512);

const StartFenceSourceSchema = z.discriminatedUnion("kind", [
  z.strictObject({ kind: z.literal("warm") }),
  z.strictObject({
    kind: z.literal("snapshot"),
    snapshotId: SnapshotResourceIdSchema,
  }),
]);

const RecoveryStartFencePayloadSchema = z.strictObject({
  mode: z.literal("recovery-v3"),
  recoveryId: RecoveryResourceIdSchema,
  connectionId: RecoveryResourceIdSchema,
  engineId,
  base: AuthorityCursorSchema,
  source: StartFenceSourceSchema,
  liveFloor: MutationBoundarySchema,
});

export const RecoveryStartFenceSchema = z
  .strictObject({
    type: z.literal("recovery-start-fence"),
    recoveryId: RecoveryResourceIdSchema,
    connectionId: RecoveryResourceIdSchema,
    deliveryGeneration: PositiveDecimalU64Schema,
    streamId,
    engineId,
    base: AuthorityCursorSchema,
    source: StartFenceSourceSchema,
    committedThrough: AuthorityCursorSchema,
    liveFloor: MutationBoundarySchema,
  })
  .superRefine((fence, context) => {
    if (
      fence.base.sessionEpoch !== fence.committedThrough.sessionEpoch ||
      BigInt(fence.base.eventSeq) > BigInt(fence.committedThrough.eventSeq) ||
      BigInt(fence.base.nextPtyOffset) > BigInt(fence.committedThrough.nextPtyOffset) ||
      (fence.base.eventSeq === fence.committedThrough.eventSeq &&
        fence.base.nextPtyOffset !== fence.committedThrough.nextPtyOffset)
    ) {
      context.addIssue({
        code: "custom",
        message: "start fence base must precede committed-through in one epoch",
        path: ["base"],
      });
    }
    try {
      const expected = successorBoundary(fence.committedThrough);
      if (
        fence.liveFloor.sessionEpoch !== expected.sessionEpoch ||
        fence.liveFloor.nextEventSeq !== expected.nextEventSeq ||
        fence.liveFloor.nextPtyOffset !== expected.nextPtyOffset
      ) {
        context.addIssue({
          code: "custom",
          message: "start fence live floor must be the exact successor boundary",
          path: ["liveFloor"],
        });
      }
    } catch {
      context.addIssue({
        code: "custom",
        message: "start fence committed-through has no successor",
        path: ["committedThrough"],
      });
    }
  });
export type RecoveryStartFence = z.infer<typeof RecoveryStartFenceSchema>;

export function encodeRecoveryStartFence(input: RecoveryStartFence): Uint8Array {
  const fence = RecoveryStartFenceSchema.parse(input);
  const payload = textEncoder.encode(
    JSON.stringify({
      mode: "recovery-v3",
      recoveryId: fence.recoveryId,
      connectionId: fence.connectionId,
      engineId: fence.engineId,
      base: fence.base,
      source: fence.source,
      liveFloor: fence.liveFloor,
    } satisfies z.input<typeof RecoveryStartFencePayloadSchema>),
  );
  if (payload.byteLength > MAX_RECOVERY_START_FENCE_PAYLOAD_BYTES) {
    throw new ProtocolError("BAD_LENGTH", "RecoveryStartFence payload is too large");
  }
  return encodeDataFrame({
    kind: DataFrameKind.DeliveryBarrier,
    flags: DataFrameFlag.None,
    sessionEpoch: BigInt(fence.committedThrough.sessionEpoch),
    deliveryGeneration: BigInt(fence.deliveryGeneration),
    eventSeq: BigInt(fence.committedThrough.eventSeq),
    ptyOffset: BigInt(fence.committedThrough.nextPtyOffset),
    streamId: fence.streamId,
    payload,
  });
}

export function decodeRecoveryStartFence(input: ArrayBuffer | Uint8Array): RecoveryStartFence {
  const frame = decodeDataFrame(input);
  if (
    frame.kind !== DataFrameKind.DeliveryBarrier ||
    frame.deliveryGeneration === 0n ||
    frame.streamId === 0
  ) {
    throw new ProtocolError("BAD_KIND", "data frame is not a RecoveryStartFence");
  }
  if (
    frame.payload.byteLength === 0 ||
    frame.payload.byteLength > MAX_RECOVERY_START_FENCE_PAYLOAD_BYTES
  ) {
    throw new ProtocolError("BAD_LENGTH", "invalid RecoveryStartFence payload length");
  }
  let payload: z.output<typeof RecoveryStartFencePayloadSchema>;
  try {
    payload = RecoveryStartFencePayloadSchema.parse(
      JSON.parse(textDecoder.decode(frame.payload)) as unknown,
    );
  } catch {
    throw new ProtocolError("BAD_PAYLOAD", "invalid RecoveryStartFence payload");
  }
  try {
    return RecoveryStartFenceSchema.parse({
      type: "recovery-start-fence",
      recoveryId: payload.recoveryId,
      connectionId: payload.connectionId,
      deliveryGeneration: frame.deliveryGeneration.toString(),
      streamId: frame.streamId,
      engineId: payload.engineId,
      base: payload.base,
      source: payload.source,
      committedThrough: {
        sessionEpoch: frame.sessionEpoch.toString(),
        eventSeq: frame.eventSeq.toString(),
        nextPtyOffset: frame.ptyOffset.toString(),
      },
      liveFloor: payload.liveFloor,
    });
  } catch {
    throw new ProtocolError("BAD_PAYLOAD", "inconsistent RecoveryStartFence cursors");
  }
}
