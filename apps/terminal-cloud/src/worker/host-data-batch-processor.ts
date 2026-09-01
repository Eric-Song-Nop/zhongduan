import {
  DataFrameKind,
  MAX_U64,
  RelayCapability,
  rewriteDelivery,
  type DataFrame,
} from "@zhongduan/protocol";

import { advanceDeliveryCursorBatch } from "./relay-delivery";
import { readSocketAttachment, writeSocketAttachment, type SocketAttachment } from "./relay-socket";
import type { RelayStore } from "./relay-store";

export interface CanonicalDataFrameEntry {
  encoded: Uint8Array;
  frame: DataFrame;
}

interface HostDataBatchProcessorOptions {
  browserDataSockets: () => WebSocket[];
  failCurrentHost: (attachment: SocketAttachment, reason: string) => void;
  hasPinnedDeliveryInProgress: () => boolean;
  now?: () => number;
  resetBrowserAfterDataSendFailure: (
    webSocket: WebSocket,
    attachment: SocketAttachment,
  ) => Promise<void> | undefined;
  resetBrowserDelivery: (
    clientId: string,
    reason: "journal-gap" | "slow-client",
    notifyHost: boolean,
  ) => Promise<void>;
  sql: SqlStorage;
  store: RelayStore;
}

function rewriteDecodedDeliveryInPlace(
  encoded: Uint8Array,
  deliveryGeneration: bigint,
  streamId: number,
): Uint8Array {
  // The negotiated batch decoder has already validated this exact frame view. WebSocket.send()
  // snapshots BufferSource bytes before returning, so a sole synced Browser can consume the owned
  // inbound buffer without allocating a second payload-sized copy.
  const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
  view.setBigUint64(16, deliveryGeneration, true);
  view.setUint32(40, streamId, true);
  return encoded;
}

/** Validates, commits, and fans out one canonical Host data batch as a single cursor plan. */
export class HostDataBatchProcessor {
  readonly #browserDataSockets: () => WebSocket[];
  readonly #failCurrentHost: (attachment: SocketAttachment, reason: string) => void;
  readonly #hasPinnedDeliveryInProgress: () => boolean;
  readonly #now: () => number;
  readonly #resetBrowserAfterDataSendFailure: (
    webSocket: WebSocket,
    attachment: SocketAttachment,
  ) => Promise<void> | undefined;
  readonly #resetBrowserDelivery: HostDataBatchProcessorOptions["resetBrowserDelivery"];
  readonly #sql: SqlStorage;
  readonly #store: RelayStore;

  constructor(options: HostDataBatchProcessorOptions) {
    this.#browserDataSockets = options.browserDataSockets;
    this.#failCurrentHost = options.failCurrentHost;
    this.#hasPinnedDeliveryInProgress = options.hasPinnedDeliveryInProgress;
    this.#now = options.now ?? Date.now;
    this.#resetBrowserAfterDataSendFailure = options.resetBrowserAfterDataSendFailure;
    this.#resetBrowserDelivery = options.resetBrowserDelivery;
    this.#sql = options.sql;
    this.#store = options.store;
  }

  process(
    attachment: SocketAttachment,
    logicalFrames: CanonicalDataFrameEntry[],
  ): Promise<void> | undefined {
    if (
      logicalFrames.length < 2 ||
      this.#hasPinnedDeliveryInProgress() ||
      logicalFrames.some(
        ({ frame }) =>
          frame.streamId !== 0 ||
          (frame.kind !== DataFrameKind.PtyOutput && frame.kind !== DataFrameKind.ResizeApplied),
      )
    ) {
      return undefined;
    }

    const session = this.#store.session();
    if (session === undefined) {
      this.#failCurrentHost(attachment, "host session epoch mismatch");
      return Promise.resolve();
    }
    const sessionEpoch = BigInt(session.session_epoch);
    let headEventSeq = BigInt(session.head_event_seq);
    let nextPtyOffset = BigInt(session.next_pty_offset);
    for (const { frame } of logicalFrames) {
      if (
        frame.sessionEpoch !== sessionEpoch ||
        frame.deliveryGeneration !== 0n ||
        frame.eventSeq !== headEventSeq + 1n ||
        frame.ptyOffset !== nextPtyOffset ||
        (frame.kind === DataFrameKind.ResizeApplied && frame.payload.byteLength !== 16) ||
        (frame.kind === DataFrameKind.PtyOutput &&
          frame.ptyOffset > MAX_U64 - BigInt(frame.payload.byteLength))
      ) {
        this.#failCurrentHost(attachment, "canonical data sequence gap");
        return Promise.resolve();
      }
      headEventSeq = frame.eventSeq;
      if (frame.kind === DataFrameKind.PtyOutput) {
        nextPtyOffset = frame.ptyOffset + BigInt(frame.payload.byteLength);
      }
    }

    this.#sql.exec(
      `UPDATE session_state
       SET head_event_seq = ?, next_pty_offset = ?, updated_at = ?
       WHERE singleton = 1 AND host_fence = ?`,
      headEventSeq.toString(),
      nextPtyOffset.toString(),
      this.#now(),
      session.host_fence,
    );

    const pendingResets: Promise<void>[] = [];
    const frames = logicalFrames.map(({ frame }) => frame);
    const browserDataSockets = this.#browserDataSockets();
    const rewriteInPlace =
      browserDataSockets.filter((socket) => readSocketAttachment(socket)?.dataState === "synced")
        .length === 1;
    for (const browserData of browserDataSockets) {
      const initialAttachment = readSocketAttachment(browserData);
      if (
        initialAttachment?.dataState !== "synced" ||
        initialAttachment.clientId === null ||
        browserData.readyState !== WebSocket.OPEN
      ) {
        continue;
      }
      const client = this.#store.clientById(initialAttachment.clientId);
      if (
        client === undefined ||
        client.stream_id !== initialAttachment.streamId ||
        client.delivery_generation !== initialAttachment.deliveryGeneration
      ) {
        continue;
      }

      const cursor = advanceDeliveryCursorBatch(initialAttachment, frames);
      if (cursor.kind === "sequence-error") {
        pendingResets.push(
          this.#resetBrowserDelivery(initialAttachment.clientId, "journal-gap", false),
        );
        continue;
      }
      if (cursor.kind === "credit-exceeded") {
        pendingResets.push(
          this.#resetBrowserDelivery(initialAttachment.clientId, "slow-client", true),
        );
        continue;
      }
      let deliveredAll = true;
      const browserBatchEnabled =
        rewriteInPlace &&
        initialAttachment.relayCapabilities.includes(RelayCapability.browserDataBatchV1);
      if (browserBatchEnabled) {
        try {
          for (const logical of logicalFrames) {
            rewriteDecodedDeliveryInPlace(
              logical.encoded,
              BigInt(client.delivery_generation),
              client.stream_id,
            );
          }
          const first = logicalFrames[0]!.encoded;
          const batchBytes = logicalFrames.reduce(
            (total, logical) => total + logical.encoded.byteLength,
            0,
          );
          browserData.send(new Uint8Array(first.buffer, first.byteOffset, batchBytes));
        } catch {
          const reset = this.#resetBrowserAfterDataSendFailure(browserData, initialAttachment);
          if (reset !== undefined) pendingResets.push(reset);
          deliveredAll = false;
        }
      } else {
        for (const logical of logicalFrames) {
          const rewritten = rewriteInPlace
            ? rewriteDecodedDeliveryInPlace(
                logical.encoded,
                BigInt(client.delivery_generation),
                client.stream_id,
              )
            : rewriteDelivery(
                logical.encoded,
                BigInt(client.delivery_generation),
                client.stream_id,
              );
          try {
            browserData.send(rewritten);
          } catch {
            const reset = this.#resetBrowserAfterDataSendFailure(browserData, initialAttachment);
            if (reset !== undefined) pendingResets.push(reset);
            deliveredAll = false;
            break;
          }
        }
      }
      if (deliveredAll) {
        writeSocketAttachment(browserData, { ...initialAttachment, ...cursor.nextState });
      }
    }
    return pendingResets.length === 0
      ? Promise.resolve()
      : Promise.all(pendingResets).then(() => undefined);
  }
}
