import type { ReplicaCursor, ResizePayload, ServerControlFrame } from "@zhongduan/protocol";
import type { BrowserTelemetrySink, MonotonicClock } from "@zhongduan/telemetry";

export type SnapshotManifest = Extract<ServerControlFrame, { type: "snapshot-manifest" }>;
export type WarmReplayStart = Extract<ServerControlFrame, { type: "replay-start" }>;

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
    manifest: SnapshotManifest,
    signal: AbortSignal,
  ): Promise<ReplicaSink>;
  adopt(replica: ReplicaSink, cursor: ReplicaCursor): void;
}

export interface SnapshotTransport {
  load(manifest: SnapshotManifest, signal: AbortSignal): Promise<Uint8Array>;
}

export type DeliveryState =
  | "idle"
  | "awaiting-control"
  | "replaying"
  | "restoring"
  | "live"
  | "resyncing"
  | "closed";

export type ResyncReason =
  | "journal-gap"
  | "slow-client"
  | "engine-mismatch"
  | "epoch-changed"
  | "restore-failed";

export interface SessionCoordinatorOptions {
  host: ReplicaHost;
  /** Cursor represented by `host.active`; omit when no active session replica exists. */
  initialCursor?: ReplicaCursor;
  snapshots: SnapshotTransport;
  onAcknowledge: (cursor: ReplicaCursor) => void;
  onReplicaProgress: (cursor: ReplicaCursor) => void;
  onResync: (reason: ResyncReason) => void;
  maxBufferedTailBytes?: number;
  maxBufferedTailFrames?: number;
  restoreDeadlineMs?: number;
  setTimer?: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
  telemetry?: BrowserTelemetrySink;
  monotonicNow?: MonotonicClock;
}
