import {
  RelayCapability,
  ServerControlFrameSchema,
  decodeDeliveryBarrierPayload,
  type DataFrame,
  type DeliveryBarrierPayload,
} from "@zhongduan/protocol";
import { z } from "zod";

import {
  readSocketAttachment as readAttachment,
  writeSocketAttachment as writeAttachment,
  type SocketAttachment,
} from "./relay-socket";
import type { RelaySocketRuntime } from "./relay-socket-runtime";
import type { RelayStore, SessionRow } from "./relay-store";
import type { SnapshotStore } from "./snapshot-store";
import type { SnapshotUploadCoordinator } from "./snapshot-upload-coordinator";

interface DeliveryBarrierControllerOptions {
  snapshotUploads: SnapshotUploadCoordinator;
  snapshots: SnapshotStore;
  sockets: RelaySocketRuntime;
  store: RelayStore;
}

function hasUnadvancedDeliveryCursor(attachment: SocketAttachment): boolean {
  const eventCursorUnchanged =
    (attachment.firstEventSeq === null &&
      attachment.ackedEventSeq === null &&
      attachment.sentEventSeq === null) ||
    (attachment.firstEventSeq !== null &&
      attachment.firstEventSeq === attachment.ackedEventSeq &&
      attachment.firstEventSeq === attachment.sentEventSeq);
  const ptyCursorUnchanged =
    (attachment.firstPtyOffset === null &&
      attachment.ackedPtyOffset === null &&
      attachment.sentPtyOffset === null) ||
    (attachment.firstPtyOffset !== null &&
      attachment.firstPtyOffset === attachment.ackedPtyOffset &&
      attachment.firstPtyOffset === attachment.sentPtyOffset);
  return eventCursorUnchanged && ptyCursorUnchanged;
}

/**
 * Owns the recovery barrier transaction: validate the Host pair and canonical commit, freeze one
 * Browser recovery plan, publish that plan to the Browser, then report the exact outcome to Host.
 */
export class DeliveryBarrierController {
  readonly #snapshotUploads: SnapshotUploadCoordinator;
  readonly #snapshots: SnapshotStore;
  readonly #sockets: RelaySocketRuntime;
  readonly #store: RelayStore;

  constructor(options: DeliveryBarrierControllerOptions) {
    this.#snapshotUploads = options.snapshotUploads;
    this.#snapshots = options.snapshots;
    this.#sockets = options.sockets;
    this.#store = options.store;
  }

  handle(
    hostData: WebSocket,
    hostAttachment: SocketAttachment,
    session: SessionRow,
    frame: DataFrame,
  ): void {
    const hostControl = this.#sockets.matchingSocket(hostAttachment, "control");
    const hostControlAttachment =
      hostControl === undefined ? undefined : readAttachment(hostControl);
    if (
      hostData !== this.#sockets.currentHostData() ||
      hostControl === undefined ||
      hostControl !== this.#sockets.currentHostControl() ||
      hostControlAttachment === undefined ||
      hostControlAttachment.hostFence !== hostAttachment.hostFence ||
      hostControlAttachment.connectionSetId !== hostAttachment.connectionSetId ||
      hostControlAttachment.connectionId !== hostAttachment.connectionId
    ) {
      this.#sockets.failCurrentHost(
        hostAttachment,
        "delivery barrier host channels are not current",
      );
      return;
    }
    let payload: DeliveryBarrierPayload;
    try {
      payload = decodeDeliveryBarrierPayload(frame.payload);
    } catch {
      this.#sockets.failCurrentHost(hostAttachment, "invalid delivery barrier payload");
      return;
    }
    if (
      frame.streamId === 0 ||
      frame.eventSeq !== BigInt(session.head_event_seq) ||
      frame.ptyOffset !== BigInt(session.next_pty_offset)
    ) {
      this.#sockets.failCurrentHost(
        hostAttachment,
        "delivery barrier does not match canonical head",
      );
      return;
    }

    const client = this.#store.clientByStream(frame.streamId);
    if (client === undefined || frame.deliveryGeneration !== BigInt(client.delivery_generation)) {
      this.#sendResult(hostControl, frame, payload, {
        status: "stale",
        reason: "generation-fenced",
      });
      return;
    }
    const browserControl = this.#sockets.activeBrowserControl(client.client_id);
    const controlAttachment =
      browserControl === undefined ? undefined : readAttachment(browserControl);
    const browserData = this.#sockets.browserDataByClient(client.client_id);
    const dataAttachment = browserData === undefined ? undefined : readAttachment(browserData);
    if (
      browserControl === undefined ||
      controlAttachment === undefined ||
      browserData === undefined ||
      dataAttachment === undefined ||
      controlAttachment.controlState !== "active" ||
      controlAttachment.clientId !== client.client_id ||
      controlAttachment.connectionId !== payload.connectionId ||
      controlAttachment.streamId !== client.stream_id ||
      controlAttachment.deliveryGeneration !== client.delivery_generation ||
      dataAttachment.clientId !== client.client_id ||
      dataAttachment.connectionSetId !== controlAttachment.connectionSetId ||
      dataAttachment.connectionId !== controlAttachment.connectionId ||
      dataAttachment.streamId !== client.stream_id ||
      dataAttachment.deliveryGeneration !== client.delivery_generation
    ) {
      this.#sendResult(hostControl, frame, payload, {
        status: "stale",
        reason: "client-gone",
      });
      return;
    }
    if (
      dataAttachment.dataState !== "catching-up" ||
      !hasUnadvancedDeliveryCursor(dataAttachment)
    ) {
      this.#sockets.failCurrentHost(
        hostAttachment,
        "delivery barrier conflicts with active delivery",
      );
      return;
    }

    const expectedSnapshotId = payload.mode === "snapshot" ? payload.snapshotId : null;
    const hasNoPin =
      dataAttachment.replayMode === null &&
      dataAttachment.snapshotId === null &&
      dataAttachment.replayCommitEventSeq === null &&
      dataAttachment.replayCommitPtyOffset === null;
    const hasExactPin =
      dataAttachment.replayMode === payload.mode &&
      dataAttachment.snapshotId === expectedSnapshotId &&
      dataAttachment.replayCommitEventSeq === frame.eventSeq.toString() &&
      dataAttachment.replayCommitPtyOffset === frame.ptyOffset.toString();
    if (!hasNoPin && !hasExactPin) {
      this.#sockets.failCurrentHost(
        hostAttachment,
        "delivery barrier conflicts with pinned delivery",
      );
      return;
    }

    let nextAttachment = dataAttachment;
    let browserFrame: z.input<typeof ServerControlFrameSchema>;
    if (payload.mode === "warm") {
      if (dataAttachment.firstEventSeq === null || dataAttachment.firstPtyOffset === null) {
        this.#sendResult(hostControl, frame, payload, {
          status: "rejected",
          reason: "missing-live-seed",
          retryScope: "same-generation",
        });
        return;
      }
      nextAttachment = {
        ...dataAttachment,
        replayMode: "warm",
        snapshotId: null,
        replayCommitEventSeq: frame.eventSeq.toString(),
        replayCommitPtyOffset: frame.ptyOffset.toString(),
      };
      browserFrame = {
        type: "replay-start",
        sessionEpoch: session.session_epoch,
        streamId: client.stream_id,
        deliveryGeneration: client.delivery_generation,
        baseEventSeq: dataAttachment.firstEventSeq,
        basePtyOffset: dataAttachment.firstPtyOffset,
        commitEventSeq: frame.eventSeq.toString(),
        commitPtyOffset: frame.ptyOffset.toString(),
      };
    } else {
      const snapshot = this.#snapshots.published(payload.snapshotId);
      if (snapshot === undefined) {
        this.#sendResult(hostControl, frame, payload, {
          status: "rejected",
          reason: "snapshot-missing",
          retryScope: "refresh-checkpoint",
        });
        return;
      }
      if (
        snapshot.sessionId !== session.session_id ||
        snapshot.sessionEpoch !== session.session_epoch ||
        snapshot.engineId !== session.engine_id ||
        BigInt(snapshot.cutEventSeq) > frame.eventSeq ||
        BigInt(snapshot.nextPtyOffset) > frame.ptyOffset
      ) {
        this.#sendResult(hostControl, frame, payload, {
          status: "rejected",
          reason: "snapshot-metadata-mismatch",
          retryScope: "refresh-checkpoint",
        });
        return;
      }
      if (
        hasExactPin &&
        (dataAttachment.firstEventSeq !== snapshot.cutEventSeq ||
          dataAttachment.firstPtyOffset !== snapshot.nextPtyOffset)
      ) {
        this.#sockets.failCurrentHost(
          hostAttachment,
          "snapshot barrier conflicts with pinned seed",
        );
        return;
      }
      nextAttachment = {
        ...dataAttachment,
        firstEventSeq: snapshot.cutEventSeq,
        ackedEventSeq: snapshot.cutEventSeq,
        sentEventSeq: snapshot.cutEventSeq,
        firstPtyOffset: snapshot.nextPtyOffset,
        ackedPtyOffset: snapshot.nextPtyOffset,
        sentPtyOffset: snapshot.nextPtyOffset,
        replayMode: "snapshot",
        snapshotId: payload.snapshotId,
        replayCommitEventSeq: frame.eventSeq.toString(),
        replayCommitPtyOffset: frame.ptyOffset.toString(),
      };
      browserFrame = {
        type: "snapshot-manifest",
        snapshotId: snapshot.snapshotId,
        engineId: snapshot.engineId,
        sessionEpoch: snapshot.sessionEpoch,
        streamId: client.stream_id,
        deliveryGeneration: client.delivery_generation,
        cutEventSeq: snapshot.cutEventSeq,
        nextPtyOffset: snapshot.nextPtyOffset,
        commitEventSeq: frame.eventSeq.toString(),
        commitPtyOffset: frame.ptyOffset.toString(),
        compression: snapshot.compression,
        compressedLength: snapshot.compressedLength,
        uncompressedLength: snapshot.uncompressedLength,
        sha256: snapshot.sha256,
        downloadPath: `/api/v1/sessions/${session.session_id}/snapshots/${snapshot.snapshotId}`,
        restoreThrough: "finish",
      };
    }

    if (hasNoPin) {
      writeAttachment(browserData, nextAttachment);
      if (nextAttachment.snapshotId !== null) this.#snapshotUploads.scheduleMaintenance();
    }
    if (!this.#sockets.sendBrowserControl(browserControl, browserFrame, "delivery start failed")) {
      this.#sendResult(hostControl, frame, payload, {
        status: "rejected",
        reason: "browser-control-send-failed",
        retryScope: "drop-client",
      });
      return;
    }
    this.#sendResult(hostControl, frame, payload, { status: "ready" });
  }

  hasPinnedDeliveryInProgress(): boolean {
    return this.#sockets.browserDataSockets().some((socket) => {
      const attachment = readAttachment(socket);
      return (
        attachment?.dataState === "catching-up" &&
        attachment.replayCommitEventSeq !== null &&
        attachment.replayCommitPtyOffset !== null
      );
    });
  }

  #sendResult(
    webSocket: WebSocket,
    frame: DataFrame,
    payload: DeliveryBarrierPayload,
    outcome:
      | { status: "ready" }
      | { status: "stale"; reason: "generation-fenced" | "client-gone" }
      | {
          status: "rejected";
          reason: "missing-live-seed";
          retryScope: "same-generation";
        }
      | {
          status: "rejected";
          reason: "snapshot-missing" | "snapshot-metadata-mismatch";
          retryScope: "refresh-checkpoint";
        }
      | {
          status: "rejected";
          reason: "browser-control-send-failed";
          retryScope: "drop-client";
        },
  ): void {
    const supportsOutcomeDetails = readAttachment(webSocket)?.relayCapabilities.includes(
      RelayCapability.deliveryBarrierOutcomeV1,
    );
    const common = {
      type: "delivery-barrier-result" as const,
      ...(supportsOutcomeDetails ? outcome : { status: outcome.status }),
      connectionId: payload.connectionId,
      streamId: frame.streamId,
      deliveryGeneration: frame.deliveryGeneration.toString(),
      commitEventSeq: frame.eventSeq.toString(),
      commitPtyOffset: frame.ptyOffset.toString(),
    };
    this.#sockets.sendHostControl(
      webSocket,
      payload.mode === "warm"
        ? { ...common, mode: "warm" }
        : { ...common, mode: "snapshot", snapshotId: payload.snapshotId },
      "delivery barrier result delivery failed",
    );
  }
}
