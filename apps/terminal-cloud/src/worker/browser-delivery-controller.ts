import {
  ClientControlFrameSchema,
  MAX_U64,
  RelayCapability,
  rewriteDelivery,
  type DataFrame,
} from "@zhongduan/protocol";
import { z } from "zod";

import { randomId, sha256Hex } from "./auth";
import type { RelayConnectionStore } from "./relay-connection-store";
import { advanceDeliveryCursor } from "./relay-delivery";
import {
  readSocketAttachment as readAttachment,
  SocketAttachmentSchema,
  writeSocketAttachment as writeAttachment,
  type SocketAttachment,
} from "./relay-socket";
import type { RelaySocketRuntime } from "./relay-socket-runtime";
import { closeProtocol } from "./relay-socket-runtime";
import type { ClientRow, RelayStore, SessionRow } from "./relay-store";
import type { WriterAuthority } from "./writer-authority";

const TICKET_LIFETIME_MS = 30_000;
const SOCKET_REPLACED = 4001;
const SLOW_CLIENT = 4008;

export type BrowserDeliveryResetReason =
  | "journal-gap"
  | "slow-client"
  | "engine-mismatch"
  | "epoch-changed"
  | "data-disconnected"
  | "host-reconnect";

interface BrowserAttachContext {
  client: ClientRow;
  controlAttachment: SocketAttachment;
  dataAttachment: SocketAttachment;
  dataSocket: WebSocket;
  session: SessionRow;
}

type BrowserAttachContextResult =
  | { ok: true; value: BrowserAttachContext }
  | {
      ok: false;
      client?: ClientRow;
      reason:
        | "already-attached"
        | "cursor-ahead"
        | "data-missing"
        | "engine-mismatch"
        | "epoch-changed"
        | "session-missing"
        | "stale-delivery"
        | "stale-control";
    };

interface BrowserDeliveryControllerOptions {
  connections: RelayConnectionStore;
  sockets: RelaySocketRuntime;
  store: RelayStore;
  writerAuthority: WriterAuthority;
}

/** Owns generation-fenced Browser delivery activation, recovery pins, cursors, and resets. */
export class BrowserDeliveryController {
  readonly #connections: RelayConnectionStore;
  readonly #sockets: RelaySocketRuntime;
  readonly #store: RelayStore;
  readonly #writerAuthority: WriterAuthority;

  constructor(options: BrowserDeliveryControllerOptions) {
    this.#connections = options.connections;
    this.#sockets = options.sockets;
    this.#store = options.store;
    this.#writerAuthority = options.writerAuthority;
  }

  async attach(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<z.output<typeof ClientControlFrameSchema>, { type: "attach" }>,
  ): Promise<void> {
    const initialResult = this.#resolveAttachContext(webSocket, attachment, frame);
    if (!initialResult.ok) {
      this.#rejectAttach(webSocket, initialResult);
      return;
    }
    const initialActivation =
      initialResult.value.controlAttachment.controlState === "awaiting-attach";
    const lease =
      initialActivation && attachment.role === "writer"
        ? await this.#writerAuthority.acquire(
            attachment.clientId!,
            attachment.connectionId,
            Date.now(),
          )
        : undefined;
    const currentResult = this.#resolveAttachContext(webSocket, attachment, frame);
    if (!currentResult.ok) {
      if (lease !== undefined) {
        this.#writerAuthority.release(attachment.clientId!, lease.fence, attachment.connectionId);
      }
      return;
    }
    const current = currentResult.value;

    const { type: _type, deliveryGeneration: _deliveryGeneration, ...attachPayload } = frame;
    const activeAttachment = SocketAttachmentSchema.parse({
      ...current.controlAttachment,
      deliveryGeneration: current.client.delivery_generation,
      leaseFence: lease?.fence ?? current.controlAttachment.leaseFence,
      leaseExpiresAt: lease?.expiresAt ?? current.controlAttachment.leaseExpiresAt,
      controlState: "active",
    });
    if (initialActivation) writeAttachment(webSocket, activeAttachment);
    const baselineEventSeq = frame.hasLiveReplica ? frame.lastEventSeq : null;
    const baselinePtyOffset = frame.hasLiveReplica ? frame.nextPtyOffset : null;
    writeAttachment(current.dataSocket, {
      ...current.dataAttachment,
      dataState: "catching-up",
      firstEventSeq: baselineEventSeq,
      ackedEventSeq: baselineEventSeq,
      sentEventSeq: baselineEventSeq,
      firstPtyOffset: baselinePtyOffset,
      ackedPtyOffset: baselinePtyOffset,
      sentPtyOffset: baselinePtyOffset,
      replayMode: null,
      snapshotId: null,
      replayCommitEventSeq: null,
      replayCommitPtyOffset: null,
    });
    if (
      initialActivation &&
      !this.#sockets.sendBrowserControl(
        webSocket,
        {
          type: "welcome",
          connectionId: current.controlAttachment.connectionId,
          streamId: current.client.stream_id,
          ...(lease === undefined
            ? {}
            : {
                writerLease: lease.token,
                ...(activeAttachment.relayCapabilities.includes(
                  RelayCapability.browserInputAdmissionV1,
                )
                  ? { writerFence: lease.fence }
                  : {}),
              }),
          engineId: current.session.engine_id,
          sessionEpoch: current.session.session_epoch,
          deliveryGeneration: current.client.delivery_generation,
          headEventSeq: current.session.head_event_seq,
          nextPtyOffset: current.session.next_pty_offset,
        },
        "welcome delivery failed",
      )
    ) {
      return;
    }

    const host = this.#sockets.currentHostControl();
    if (host === undefined) {
      this.#sockets.sendBrowserControl(
        webSocket,
        { type: "host-offline" },
        "host status delivery failed",
      );
      return;
    }
    this.#sockets.sendHostControl(
      host,
      {
        type: "attach-request",
        connectionId: current.controlAttachment.connectionId,
        streamId: current.client.stream_id,
        deliveryGeneration: current.client.delivery_generation,
        ...attachPayload,
      },
      "browser attach request delivery failed",
    );
  }

  acknowledge(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<z.output<typeof ClientControlFrameSchema>, { type: "ack" }>,
  ): void {
    if (attachment.clientId === null) return;
    const session = this.#store.session();
    const client = this.#store.clientById(attachment.clientId);
    if (session === undefined || client === undefined) return;
    if (frame.deliveryGeneration !== client.delivery_generation) return;
    if (frame.sessionEpoch !== session.session_epoch) {
      this.#sockets.sendBrowserControl(
        webSocket,
        {
          type: "resync-required",
          deliveryGeneration: client.delivery_generation,
          reason: "epoch-changed",
        },
        "resync notification failed",
      );
      return;
    }

    const eventSeq = BigInt(frame.eventSeq);
    const ptyOffset = BigInt(frame.nextPtyOffset);
    if (eventSeq > BigInt(session.head_event_seq) || ptyOffset > BigInt(session.next_pty_offset)) {
      closeProtocol(webSocket, "invalid acknowledgement cursor");
      return;
    }

    const dataSocket = this.#sockets.browserDataByClient(attachment.clientId);
    const dataAttachment = dataSocket === undefined ? undefined : readAttachment(dataSocket);
    if (
      dataSocket === undefined ||
      dataAttachment === undefined ||
      dataAttachment.deliveryGeneration !== frame.deliveryGeneration ||
      dataAttachment.sentEventSeq === null ||
      dataAttachment.sentPtyOffset === null
    ) {
      closeProtocol(webSocket, "acknowledgement has no delivered data cursor");
      return;
    }
    const sentEventSeq = BigInt(dataAttachment.sentEventSeq);
    const sentPtyOffset = BigInt(dataAttachment.sentPtyOffset);
    const ackedEventSeq =
      dataAttachment.ackedEventSeq === null ? undefined : BigInt(dataAttachment.ackedEventSeq);
    const ackedPtyOffset =
      dataAttachment.ackedPtyOffset === null ? undefined : BigInt(dataAttachment.ackedPtyOffset);
    if (
      eventSeq > sentEventSeq ||
      ptyOffset > sentPtyOffset ||
      (ackedEventSeq !== undefined && eventSeq < ackedEventSeq) ||
      (ackedPtyOffset !== undefined && ptyOffset < ackedPtyOffset)
    ) {
      closeProtocol(webSocket, "acknowledgement exceeds delivered cursor");
      return;
    }
    if (eventSeq === ackedEventSeq && ptyOffset === ackedPtyOffset) return;
    writeAttachment(dataSocket, {
      ...dataAttachment,
      ackedEventSeq: frame.eventSeq,
      ackedPtyOffset: frame.nextPtyOffset,
    });
  }

  deliverToBrowser(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    encoded: Uint8Array,
    frame: DataFrame,
  ): Promise<void> | "sequence-error" | undefined {
    if (attachment.clientId === null || webSocket.readyState !== WebSocket.OPEN) return;
    const client = this.#store.clientById(attachment.clientId);
    if (
      client === undefined ||
      client.stream_id !== attachment.streamId ||
      client.delivery_generation !== attachment.deliveryGeneration
    ) {
      return;
    }
    const cursor = advanceDeliveryCursor(attachment, frame);
    if (cursor.kind === "sequence-error") return "sequence-error";
    if (cursor.kind === "credit-exceeded") {
      return this.reset(attachment.clientId, "slow-client", true);
    }

    const rewritten = rewriteDelivery(
      encoded,
      BigInt(client.delivery_generation),
      client.stream_id,
    );
    try {
      webSocket.send(rewritten);
    } catch {
      return this.resetAfterDataSendFailure(webSocket, attachment);
    }
    writeAttachment(webSocket, { ...attachment, ...cursor.nextState });
  }

  resetAfterDataSendFailure(
    webSocket: WebSocket,
    attachment: SocketAttachment,
  ): Promise<void> | undefined {
    if (attachment.clientId === null) return;
    return this.reset(attachment.clientId, "data-disconnected", false).catch(() => {
      this.#sockets.isolateBrowserConnection(webSocket, attachment, "browser data delivery failed");
    });
  }

  async reset(
    clientId: string,
    reason: BrowserDeliveryResetReason,
    notifyHost: boolean,
    expectedHost?: { webSocket: WebSocket; hostFence: string },
  ): Promise<void> {
    const client = this.#store.clientById(clientId);
    if (client === undefined) return;
    const issuingControl = this.#sockets.activeBrowserControl(clientId);
    const issuingAttachment =
      issuingControl === undefined ? undefined : readAttachment(issuingControl);
    if (
      issuingControl === undefined ||
      issuingAttachment?.clientId !== clientId ||
      issuingAttachment.controlState !== "active"
    ) {
      return;
    }
    const nextGeneration = (BigInt(client.delivery_generation) + 1n).toString();
    if (BigInt(nextGeneration) > MAX_U64) throw new Error("delivery generation space exhausted");
    const dataTicket = randomId(32);
    const ticketDigest = await sha256Hex(dataTicket);
    const currentClient = this.#store.clientById(clientId);
    const currentControlAttachment = readAttachment(issuingControl);
    const currentHostAttachment =
      expectedHost === undefined ? undefined : readAttachment(expectedHost.webSocket);
    if (
      issuingControl.readyState !== WebSocket.OPEN ||
      this.#sockets.activeBrowserControl(clientId) !== issuingControl ||
      currentClient?.delivery_generation !== client.delivery_generation ||
      currentControlAttachment?.clientId !== clientId ||
      currentControlAttachment.controlState !== "active" ||
      currentControlAttachment.connectionId !== issuingAttachment.connectionId ||
      currentControlAttachment.connectionSetId !== issuingAttachment.connectionSetId ||
      (expectedHost !== undefined &&
        (expectedHost.webSocket.readyState !== WebSocket.OPEN ||
          currentHostAttachment?.hostFence !== expectedHost.hostFence ||
          this.#sockets.currentHostControl() !== expectedHost.webSocket))
    ) {
      return;
    }

    const expiresAt = Date.now() + TICKET_LIFETIME_MS;
    if (
      !this.#connections.replaceBrowserDelivery({
        clientId,
        connectionId: currentControlAttachment.connectionId,
        connectionSetId: currentControlAttachment.connectionSetId,
        currentGeneration: client.delivery_generation,
        expiresAt,
        nextGeneration,
        role: currentControlAttachment.role,
        streamId: currentClient.stream_id,
        subject: currentControlAttachment.subject,
        ticketDigest,
      })
    ) {
      return;
    }

    this.#sockets.closeSockets(
      (attachment) =>
        attachment.peer === "browser" &&
        attachment.channel === "data" &&
        attachment.clientId === clientId,
      undefined,
      reason === "slow-client" ? SLOW_CLIENT : SOCKET_REPLACED,
      reason,
    );
    const resetAttachment = SocketAttachmentSchema.parse({
      ...currentControlAttachment,
      deliveryGeneration: nextGeneration,
      controlState: "active",
    });
    writeAttachment(issuingControl, resetAttachment);
    if (
      !this.#sockets.sendBrowserControl(
        issuingControl,
        {
          type: "resync-required",
          deliveryGeneration: nextGeneration,
          reason,
          dataTicket,
          expiresAt,
        },
        "browser delivery reset failed",
      )
    ) {
      return;
    }

    if (!notifyHost || reason !== "slow-client") return;
    const host = this.#sockets.currentHostControl();
    if (host !== undefined) {
      this.#sockets.sendHostControl(
        host,
        {
          type: "delivery-reset",
          connectionId: resetAttachment.connectionId,
          streamId: client.stream_id,
          deliveryGeneration: nextGeneration,
          reason: "slow-client",
        },
        "browser delivery reset notification failed",
      );
    }
  }

  #resolveAttachContext(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<z.output<typeof ClientControlFrameSchema>, { type: "attach" }>,
  ): BrowserAttachContextResult {
    if (attachment.clientId === null) return { ok: false, reason: "session-missing" };
    const session = this.#store.session();
    const client = this.#store.clientById(attachment.clientId);
    if (session === undefined || client === undefined) {
      return { ok: false, reason: "session-missing" };
    }
    if (frame.deliveryGeneration !== client.delivery_generation) {
      return { ok: false, client, reason: "stale-delivery" };
    }
    const controlAttachment = readAttachment(webSocket);
    if (
      webSocket.readyState !== WebSocket.OPEN ||
      controlAttachment?.clientId !== attachment.clientId ||
      controlAttachment.connectionId !== attachment.connectionId ||
      controlAttachment.connectionSetId !== attachment.connectionSetId ||
      this.#sockets.activeBrowserControl(attachment.clientId) !== webSocket
    ) {
      return { ok: false, client, reason: "stale-control" };
    }
    if (
      (controlAttachment.controlState !== "awaiting-attach" &&
        controlAttachment.controlState !== "active") ||
      controlAttachment.deliveryGeneration !== client.delivery_generation
    ) {
      return { ok: false, client, reason: "stale-control" };
    }
    const dataSocket = this.#sockets.browserDataByClient(attachment.clientId);
    const dataAttachment = dataSocket === undefined ? undefined : readAttachment(dataSocket);
    if (
      dataSocket === undefined ||
      dataAttachment?.connectionId !== attachment.connectionId ||
      dataAttachment.connectionSetId !== attachment.connectionSetId ||
      dataAttachment.deliveryGeneration !== client.delivery_generation
    ) {
      return {
        ok: false,
        client,
        reason:
          controlAttachment.controlState === "awaiting-attach" ? "data-missing" : "stale-delivery",
      };
    }
    if (dataAttachment.dataState !== "awaiting-attach") {
      return { ok: false, client, reason: "already-attached" };
    }
    if (frame.engineId !== session.engine_id) {
      return { ok: false, client, reason: "engine-mismatch" };
    }
    if (frame.hasLiveReplica && frame.lastSessionEpoch !== session.session_epoch) {
      return { ok: false, client, reason: "epoch-changed" };
    }
    if (
      frame.hasLiveReplica &&
      (BigInt(frame.lastEventSeq) > BigInt(session.head_event_seq) ||
        BigInt(frame.nextPtyOffset) > BigInt(session.next_pty_offset))
    ) {
      return { ok: false, client, reason: "cursor-ahead" };
    }
    return { ok: true, value: { client, controlAttachment, dataAttachment, dataSocket, session } };
  }

  #rejectAttach(
    webSocket: WebSocket,
    result: Extract<BrowserAttachContextResult, { ok: false }>,
  ): void {
    if (result.reason === "stale-control" || result.reason === "stale-delivery") return;
    if (result.reason === "already-attached") {
      const attachment = readAttachment(webSocket);
      if (attachment === undefined) closeProtocol(webSocket, "delivery is already attached");
      else {
        this.#sockets.isolateBrowserConnection(
          webSocket,
          attachment,
          "delivery is already attached",
        );
      }
      return;
    }
    if (
      result.client !== undefined &&
      (result.reason === "engine-mismatch" || result.reason === "epoch-changed")
    ) {
      this.#sockets.sendBrowserControl(
        webSocket,
        {
          type: "resync-required",
          deliveryGeneration: result.client.delivery_generation,
          reason: result.reason,
        },
        "resync notification failed",
      );
      return;
    }
    closeProtocol(
      webSocket,
      result.reason === "cursor-ahead"
        ? "replica cursor exceeds host head"
        : "matching session data channel required before attach",
    );
  }
}
