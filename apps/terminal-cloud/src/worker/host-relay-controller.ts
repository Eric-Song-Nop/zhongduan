import {
  DataFrameKind,
  HostControlFrameSchema,
  MAX_U64,
  RelayCapability,
  decodeControlFrame,
  decodeDataFrame,
  decodeDataFrameBatchEntries,
  type DataFrame,
} from "@zhongduan/protocol";
import { z } from "zod";

import type { BrowserDeliveryController } from "./browser-delivery-controller";
import type { DeliveryBarrierController } from "./delivery-barrier-controller";
import { HostDataBatchProcessor } from "./host-data-batch-processor";
import {
  readSocketAttachment as readAttachment,
  writeSocketAttachment as writeAttachment,
  type SocketAttachment,
} from "./relay-socket";
import type { RelaySocketRuntime } from "./relay-socket-runtime";
import { closeProtocol } from "./relay-socket-runtime";
import type { RelayStore, SessionRow } from "./relay-store";

const HOST_DATA_BATCH_YIELD_FRAMES = 1;
const HOST_DATA_BATCH_YIELD_MS = 1;
const HOST_DATA_BATCH_ACK = "data-ack";

interface HostRelayControllerOptions {
  barriers: DeliveryBarrierController;
  delivery: BrowserDeliveryController;
  sockets: RelaySocketRuntime;
  sql: SqlStorage;
  store: RelayStore;
}

/** Owns Host readiness, canonical data commits, directed replay, and Host data credit ACKs. */
export class HostRelayController {
  readonly #barriers: DeliveryBarrierController;
  readonly #delivery: BrowserDeliveryController;
  readonly #batchProcessor: HostDataBatchProcessor;
  readonly #sockets: RelaySocketRuntime;
  readonly #sql: SqlStorage;
  readonly #store: RelayStore;

  constructor(options: HostRelayControllerOptions) {
    this.#barriers = options.barriers;
    this.#delivery = options.delivery;
    this.#sockets = options.sockets;
    this.#sql = options.sql;
    this.#store = options.store;
    this.#batchProcessor = new HostDataBatchProcessor({
      browserDataSockets: () => this.#sockets.browserDataSockets(),
      failCurrentHost: (attachment, reason) => this.#sockets.failCurrentHost(attachment, reason),
      hasPinnedDeliveryInProgress: () => this.#barriers.hasPinnedDeliveryInProgress(),
      resetBrowserAfterDataSendFailure: (webSocket, attachment) =>
        this.#delivery.resetAfterDataSendFailure(webSocket, attachment),
      resetBrowserDelivery: (clientId, reason, notifyHost) =>
        this.#delivery.reset(clientId, reason, notifyHost),
      sql: this.#sql,
      store: this.#store,
    });
  }

  async handleControl(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    message: string,
  ): Promise<void> {
    if (!this.#sockets.isCurrentHost(attachment)) return;

    let frame: z.output<typeof HostControlFrameSchema>;
    try {
      frame = decodeControlFrame(message, HostControlFrameSchema);
    } catch {
      closeProtocol(webSocket, "invalid host control frame");
      return;
    }

    if (frame.type === "host-ready") {
      const session = this.#store.session();
      const connection = this.#sockets.connection(attachment);
      if (
        session === undefined ||
        connection.control !== webSocket ||
        connection.phase !== "paired" ||
        frame.engineId !== session.engine_id ||
        frame.sessionEpoch !== session.session_epoch ||
        BigInt(frame.headEventSeq) < BigInt(session.head_event_seq) ||
        BigInt(frame.nextPtyOffset) < BigInt(session.next_pty_offset)
      ) {
        this.#sockets.failCurrentHost(attachment, "host state does not match session");
        return;
      }
      this.#sql.exec(
        `UPDATE session_state
         SET head_event_seq = ?, next_pty_offset = ?, updated_at = ?
         WHERE singleton = 1`,
        frame.headEventSeq,
        frame.nextPtyOffset,
        Date.now(),
      );
      writeAttachment(webSocket, { ...attachment, controlState: "active" });
      const activeBrowsers = this.#sockets.browserControlSockets().flatMap((browser) => {
        const browserAttachment = readAttachment(browser);
        return browserAttachment?.controlState === "active" && browserAttachment.clientId !== null
          ? [browserAttachment]
          : [];
      });
      await Promise.all(
        activeBrowsers.map((browserAttachment) =>
          this.#delivery.reset(browserAttachment.clientId!, "host-reconnect", false, {
            webSocket,
            hostFence: attachment.hostFence!,
          }),
        ),
      );
      const readyConnection = this.#sockets.connection(attachment);
      if (
        webSocket.readyState !== WebSocket.OPEN ||
        this.#sockets.currentHostControl() !== webSocket ||
        readyConnection.control !== webSocket ||
        readyConnection.phase !== "ready"
      ) {
        return;
      }
      this.#sockets.sendHostControl(
        webSocket,
        {
          type: "host-ready-ack",
          sessionEpoch: frame.sessionEpoch,
          headEventSeq: frame.headEventSeq,
          nextPtyOffset: frame.nextPtyOffset,
        },
        "host ready acknowledgement delivery failed",
      );
      return;
    }

    if (frame.type === "input-ack") {
      const browser = this.#sockets.browserControlByConnection(frame.connectionId);
      if (browser === undefined) return;
      const browserAttachment = readAttachment(browser);
      if (browserAttachment === undefined) {
        closeProtocol(browser, "input acknowledgement has no writer fence");
        return;
      }
      const { connectionId: _connectionId, ...browserFrame } = frame;
      const includeWriterFence = browserAttachment.relayCapabilities.includes(
        RelayCapability.browserInputAdmissionV1,
      );
      if (includeWriterFence && browserAttachment.leaseFence === null) {
        closeProtocol(browser, "input acknowledgement has no writer fence");
        return;
      }
      this.#sockets.sendBrowserControl(
        browser,
        {
          ...browserFrame,
          ...(includeWriterFence ? { writerFence: browserAttachment.leaseFence! } : {}),
        },
        "input acknowledgement delivery failed",
      );
      return;
    }

    const browser = this.#sockets.browserControlByConnection(frame.connectionId);
    const browserAttachment = browser === undefined ? undefined : readAttachment(browser);
    if (browserAttachment?.clientId !== null && browserAttachment?.clientId !== undefined) {
      await this.#delivery.reset(browserAttachment.clientId, frame.reason, false);
    }
  }

  async handleData(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    encoded: Uint8Array,
  ): Promise<void> {
    if (webSocket.readyState !== WebSocket.OPEN || !this.#sockets.isCurrentHost(attachment)) return;
    const connection = this.#sockets.connection(attachment);
    if (connection.data !== webSocket) return;
    if (
      connection.phase !== "ready" ||
      connection.control !== this.#sockets.currentHostControl() ||
      this.#sockets.currentHostData() !== webSocket
    ) {
      this.#sockets.failCurrentHost(attachment, "host data received before ready acknowledgement");
      return;
    }

    let logicalFrames: Array<{ encoded: Uint8Array; frame: DataFrame }>;
    try {
      logicalFrames = attachment.relayCapabilities.includes(RelayCapability.hostDataBatchV1)
        ? decodeDataFrameBatchEntries(encoded)
        : [{ encoded, frame: decodeDataFrame(encoded) }];
    } catch {
      this.#sockets.failCurrentHost(attachment, "invalid host data frame");
      return;
    }
    const canonicalBatch = this.#batchProcessor.process(attachment, logicalFrames);
    if (canonicalBatch !== undefined) {
      await canonicalBatch;
    } else {
      for (let index = 0; index < logicalFrames.length; index += 1) {
        const logical = logicalFrames[index]!;
        if (webSocket.readyState !== WebSocket.OPEN || !this.#sockets.isCurrentHost(attachment)) {
          return;
        }
        await this.#handleDataFrame(webSocket, attachment, logical.encoded, logical.frame);
        if ((index + 1) % HOST_DATA_BATCH_YIELD_FRAMES === 0 && index + 1 < logicalFrames.length) {
          await new Promise<void>((resolve) => setTimeout(resolve, HOST_DATA_BATCH_YIELD_MS));
        }
      }
    }
    if (
      attachment.relayCapabilities.includes(RelayCapability.hostDataBatchV1) &&
      webSocket.readyState === WebSocket.OPEN &&
      this.#sockets.isCurrentHost(attachment) &&
      this.#sockets.currentHostData() === webSocket
    ) {
      try {
        webSocket.send(HOST_DATA_BATCH_ACK);
      } catch {
        this.#sockets.failCurrentHost(attachment, "host data acknowledgement failed");
      }
    }
  }

  async #handleDataFrame(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    encoded: Uint8Array,
    frame: DataFrame,
  ): Promise<void> {
    const session = this.#store.session();
    if (session === undefined || frame.sessionEpoch !== BigInt(session.session_epoch)) {
      this.#sockets.failCurrentHost(attachment, "host session epoch mismatch");
      return;
    }
    if (frame.kind === DataFrameKind.DeliveryBarrier) {
      this.#barriers.handle(webSocket, attachment, session, frame);
      return;
    }

    if (frame.streamId === 0) {
      if (
        frame.deliveryGeneration !== 0n ||
        this.#barriers.hasPinnedDeliveryInProgress() ||
        !this.#commitCanonicalFrame(session, frame)
      ) {
        this.#sockets.failCurrentHost(attachment, "canonical data sequence gap");
        return;
      }
      const pendingResets: Promise<void>[] = [];
      for (const browserData of this.#sockets.browserDataSockets()) {
        const browserAttachment = readAttachment(browserData);
        if (browserAttachment?.dataState !== "synced") continue;
        const result = this.#delivery.deliverToBrowser(
          browserData,
          browserAttachment,
          encoded,
          frame,
        );
        if (result === "sequence-error") {
          if (browserAttachment.clientId !== null) {
            pendingResets.push(
              this.#delivery.reset(browserAttachment.clientId, "journal-gap", false),
            );
          }
        } else if (result !== undefined) {
          pendingResets.push(result);
        }
      }
      await Promise.all(pendingResets);
      return;
    }

    const client = this.#store.clientByStream(frame.streamId);
    if (client === undefined || frame.deliveryGeneration !== BigInt(client.delivery_generation)) {
      return;
    }
    const browserData = this.#sockets.browserDataByClient(client.client_id);
    const browserAttachment = browserData === undefined ? undefined : readAttachment(browserData);
    if (
      browserData !== undefined &&
      browserAttachment !== undefined &&
      frame.kind === DataFrameKind.Reset
    ) {
      this.#sockets.failCurrentHost(attachment, "directed reset is not supported");
      return;
    }
    if (
      browserData === undefined ||
      browserAttachment === undefined ||
      browserAttachment.deliveryGeneration !== client.delivery_generation ||
      browserAttachment.dataState === "synced"
    ) {
      return;
    }
    if (!this.#isDirectedFrameWithinPinnedCommit(browserAttachment, frame)) {
      this.#sockets.failCurrentHost(attachment, "directed replay exceeds pinned commit");
      return;
    }
    const result = this.#delivery.deliverToBrowser(browserData, browserAttachment, encoded, frame);
    if (result === "sequence-error") {
      this.#sockets.failCurrentHost(attachment, "directed replay sequence gap");
    } else {
      await result;
    }
  }

  #isDirectedFrameWithinPinnedCommit(attachment: SocketAttachment, frame: DataFrame): boolean {
    if (
      attachment.replayMode === null ||
      attachment.replayCommitEventSeq === null ||
      attachment.replayCommitPtyOffset === null
    ) {
      return false;
    }
    const commitEventSeq = BigInt(attachment.replayCommitEventSeq);
    const commitPtyOffset = BigInt(attachment.replayCommitPtyOffset);
    if (frame.kind === DataFrameKind.Reset) return false;
    if (frame.kind === DataFrameKind.ReplayCommit) {
      return frame.eventSeq === commitEventSeq && frame.ptyOffset === commitPtyOffset;
    }
    if (frame.eventSeq > commitEventSeq || frame.ptyOffset > commitPtyOffset) return false;
    if (frame.kind === DataFrameKind.ResizeApplied) return true;
    return (
      frame.kind === DataFrameKind.PtyOutput &&
      frame.ptyOffset <= MAX_U64 - BigInt(frame.payload.byteLength) &&
      frame.ptyOffset + BigInt(frame.payload.byteLength) <= commitPtyOffset
    );
  }

  #commitCanonicalFrame(session: SessionRow, frame: DataFrame): boolean {
    if (frame.kind !== DataFrameKind.PtyOutput && frame.kind !== DataFrameKind.ResizeApplied) {
      return false;
    }
    if (
      frame.eventSeq !== BigInt(session.head_event_seq) + 1n ||
      frame.ptyOffset !== BigInt(session.next_pty_offset) ||
      (frame.kind === DataFrameKind.ResizeApplied && frame.payload.byteLength !== 16) ||
      (frame.kind === DataFrameKind.PtyOutput &&
        frame.ptyOffset > MAX_U64 - BigInt(frame.payload.byteLength))
    ) {
      return false;
    }
    const nextPtyOffset =
      frame.kind === DataFrameKind.PtyOutput
        ? frame.ptyOffset + BigInt(frame.payload.byteLength)
        : frame.ptyOffset;
    this.#sql.exec(
      `UPDATE session_state
       SET head_event_seq = ?, next_pty_offset = ?, updated_at = ?
       WHERE singleton = 1 AND host_fence = ?`,
      frame.eventSeq.toString(),
      nextPtyOffset.toString(),
      Date.now(),
      session.host_fence,
    );
    return true;
  }
}
