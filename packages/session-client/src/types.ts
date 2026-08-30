import type { ReplicaCursor, ResizePayload, SnapshotRecoverySource } from "@zhongduan/protocol";

export type SnapshotRestoreSource = SnapshotRecoverySource;

export interface ReplicaSink {
  readonly engineId: string;
  writePty(data: Uint8Array): void;
  resize(dimensions: ResizePayload): void;
  dispose(): void;
}

export interface ReplicaHost {
  readonly engineId: string;
  readonly active: ReplicaSink | null;
  restore(
    snapshot: Uint8Array,
    source: SnapshotRestoreSource,
    signal: AbortSignal,
  ): Promise<ReplicaSink>;
  adopt(replica: ReplicaSink, cursor: ReplicaCursor): void;
}

export interface SnapshotTransport {
  load(source: SnapshotRestoreSource, signal: AbortSignal): Promise<Uint8Array>;
}

export type DeliveryState =
  | "idle"
  | "awaiting-control"
  | "replaying"
  | "restoring"
  | "live"
  | "resyncing"
  | "closed";
