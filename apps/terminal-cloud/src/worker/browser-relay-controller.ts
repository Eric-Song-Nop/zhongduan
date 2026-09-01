import { ClientControlFrameSchema, RelayCapability, decodeControlFrame } from "@zhongduan/protocol";
import { z } from "zod";

import type { BrowserDeliveryController } from "./browser-delivery-controller";
import {
  BrowserControlLane,
  isSemanticInputFrame,
  type BrowserControlTiming,
} from "./browser-control-lane";
import { readSocketAttachment as readAttachment, type SocketAttachment } from "./relay-socket";
import type { RelaySocketRuntime } from "./relay-socket-runtime";
import { closeProtocol } from "./relay-socket-runtime";
import type { RelayStore } from "./relay-store";
import type { WriterAuthority } from "./writer-authority";

interface BrowserRelayControllerOptions {
  controlLane: BrowserControlLane;
  delivery: BrowserDeliveryController;
  sockets: RelaySocketRuntime;
  store: RelayStore;
  writerAuthority: WriterAuthority;
}

/** Owns Browser control admission after lane scheduling: attach, writer lease, ACK, and input. */
export class BrowserRelayController {
  readonly #controlLane: BrowserControlLane;
  readonly #delivery: BrowserDeliveryController;
  readonly #sockets: RelaySocketRuntime;
  readonly #store: RelayStore;
  readonly #writerAuthority: WriterAuthority;

  constructor(options: BrowserRelayControllerOptions) {
    this.#controlLane = options.controlLane;
    this.#delivery = options.delivery;
    this.#sockets = options.sockets;
    this.#store = options.store;
    this.#writerAuthority = options.writerAuthority;
  }

  handle(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    message: string,
    timing?: BrowserControlTiming,
  ): Promise<void> | void {
    let frame: z.output<typeof ClientControlFrameSchema>;
    try {
      frame = decodeControlFrame(message, ClientControlFrameSchema);
    } catch {
      closeProtocol(webSocket, "invalid browser control frame");
      return;
    }
    if (attachment.clientId === null) {
      closeProtocol(webSocket, "browser identity missing");
      return;
    }

    if (frame.type === "attach") {
      return this.#delivery.attach(webSocket, attachment, frame);
    }
    if (
      webSocket.readyState !== WebSocket.OPEN ||
      this.#sockets.activeBrowserControl(attachment.clientId) !== webSocket
    ) {
      if (isSemanticInputFrame(frame)) {
        this.#controlLane.recordInput(
          frame,
          attachment,
          timing,
          "rejected",
          null,
          attachment.leaseFence,
        );
      }
      return;
    }
    if (frame.type === "writer-lease-renew") {
      return this.#handleWriterLeaseRenew(webSocket, attachment, frame.writerLease);
    }
    if (frame.type === "ack") {
      const client = this.#store.clientById(attachment.clientId);
      if (
        attachment.controlState !== "active" ||
        this.#sockets.currentHostControl() === undefined ||
        client?.delivery_generation !== frame.deliveryGeneration
      ) {
        return;
      }
      this.#delivery.acknowledge(webSocket, attachment, frame);
      return;
    }
    if (attachment.controlState !== "active") {
      this.#recordRejectedInput(frame, attachment, timing);
      this.#rejectInput(webSocket, frame.inputEpoch, frame.clientInputSeq);
      return;
    }
    const latestAttachment = this.#writerAuthority.liveAttachment(
      webSocket,
      attachment,
      Date.now(),
    );
    if (latestAttachment === undefined) {
      this.#recordRejectedInput(frame, attachment, timing);
      this.#rejectInput(webSocket, frame.inputEpoch, frame.clientInputSeq);
      return;
    }

    const host = this.#sockets.currentInputHostControl();
    if (host === undefined) {
      this.#controlLane.recordInput(
        frame,
        attachment,
        timing,
        "rejected",
        null,
        latestAttachment.leaseFence,
      );
      if (
        !this.#sockets.sendBrowserControl(
          webSocket,
          { type: "host-offline" },
          "host status delivery failed",
        )
      ) {
        return;
      }
      this.#rejectInput(webSocket, frame.inputEpoch, frame.clientInputSeq);
      return;
    }
    const sendOutcome = this.#sockets.sendInputToHost(
      host,
      {
        ...frame,
        connectionId: attachment.connectionId,
        clientId: attachment.clientId,
        writerFence: latestAttachment.leaseFence,
      },
      "semantic input delivery failed",
    );
    this.#controlLane.recordInput(
      frame,
      attachment,
      timing,
      sendOutcome.result === "sent"
        ? "host-sent"
        : sendOutcome.result === "uncertain"
          ? "host-send-uncertain"
          : "rejected",
      sendOutcome.hostSendAtMs,
      latestAttachment.leaseFence,
    );
    if (sendOutcome.result !== "sent") {
      this.#rejectInput(
        webSocket,
        frame.inputEpoch,
        frame.clientInputSeq,
        sendOutcome.result === "uncertain" ? "uncertain" : "rejected",
      );
    }
  }

  async #handleWriterLeaseRenew(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    writerLease: string,
  ): Promise<void> {
    const clientId = attachment.clientId;
    if (clientId === null) return;
    const expiresAt =
      attachment.controlState === "active" && attachment.role === "writer"
        ? await this.#writerAuthority.renew(webSocket, attachment, writerLease)
        : undefined;
    const latestAttachment = readAttachment(webSocket);
    const active =
      expiresAt !== undefined &&
      webSocket.readyState === WebSocket.OPEN &&
      latestAttachment?.controlState === "active" &&
      latestAttachment.clientId === clientId &&
      latestAttachment.leaseFence === attachment.leaseFence &&
      latestAttachment.leaseExpiresAt === expiresAt &&
      this.#sockets.activeBrowserControl(clientId) === webSocket;
    this.#sockets.sendBrowserControl(
      webSocket,
      {
        type: "writer-lease-status",
        active,
        ...(active
          ? {
              expiresAt,
              ...(latestAttachment.relayCapabilities.includes(
                RelayCapability.browserInputAdmissionV1,
              )
                ? { writerFence: latestAttachment.leaseFence! }
                : {}),
            }
          : {}),
      },
      "writer lease status delivery failed",
    );
  }

  #recordRejectedInput(
    frame: z.output<typeof ClientControlFrameSchema>,
    attachment: SocketAttachment,
    timing?: BrowserControlTiming,
  ): void {
    if (!isSemanticInputFrame(frame)) return;
    this.#controlLane.recordInput(
      frame,
      attachment,
      timing,
      "rejected",
      null,
      attachment.leaseFence,
    );
  }

  #rejectInput(
    webSocket: WebSocket,
    inputEpoch: string,
    clientInputSeq: string,
    status: "rejected" | "uncertain" = "rejected",
  ): void {
    const attachment = readAttachment(webSocket);
    if (attachment === undefined) {
      closeProtocol(webSocket, "input rejection has no writer fence");
      return;
    }
    const includeWriterFence = attachment.relayCapabilities.includes(
      RelayCapability.browserInputAdmissionV1,
    );
    if (includeWriterFence && attachment.leaseFence === null) {
      closeProtocol(webSocket, "input rejection has no writer fence");
      return;
    }
    const mustFenceInputEpoch = attachment.role === "writer" && attachment.leaseFence !== null;
    const delivered = this.#sockets.sendBrowserControl(
      webSocket,
      {
        type: "input-ack",
        ...(includeWriterFence ? { writerFence: attachment.leaseFence! } : {}),
        inputEpoch,
        clientInputSeq,
        status: mustFenceInputEpoch ? "uncertain" : status,
        authorityEventSeq: this.#store.session()?.head_event_seq ?? "0",
      },
      "input rejection delivery failed",
    );
    if (delivered && mustFenceInputEpoch) {
      this.#sockets.isolateBrowserConnection(webSocket, attachment, "input epoch fenced");
    }
  }
}
