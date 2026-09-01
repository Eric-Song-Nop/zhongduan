import {
  RelayToHostControlFrameSchema,
  ServerControlFrameSchema,
  encodeControlFrame,
} from "@zhongduan/protocol";
import { z } from "zod";

import { connectionSockets, matchingSocket, sameConnection } from "./relay-connection-state";
import type { RelayConnectionStore } from "./relay-connection-store";
import {
  readSocketAttachment as readAttachment,
  writeSocketAttachment as writeAttachment,
  type SocketAttachment,
} from "./relay-socket";
import type { ClientRow, RelayStore, SessionRow, TicketRow } from "./relay-store";

const MAX_HOST_INPUT_BUFFERED_BYTES = 8 * 1024 * 1024;
const SOCKET_REPLACED = 4001;
const controlTextEncoder = new TextEncoder();

export type HostControlSendResult = "sent" | "rejected" | "uncertain";

export interface HostInputSendOutcome {
  hostSendAtMs: number | null;
  result: HostControlSendResult;
}

interface RelaySocketRuntimeOptions {
  connections: RelayConnectionStore;
  ctx: DurableObjectState;
  releaseWriter: (clientId: string, leaseFence: string | null, connectionId: string) => void;
  scheduleSnapshotMaintenance: () => void;
  store: RelayStore;
}

export function closeProtocol(webSocket: WebSocket, reason: string): void {
  if (webSocket.readyState < WebSocket.CLOSING) {
    webSocket.close(4400, reason);
  }
}

/**
 * Owns the live WebSocket topology for one relay session.
 *
 * Controllers use this object instead of independently scanning Durable Object sockets. That keeps
 * "current" identity, send-failure isolation, and pair replacement as one atomic vocabulary.
 */
export class RelaySocketRuntime {
  readonly #connections: RelayConnectionStore;
  readonly #ctx: DurableObjectState;
  readonly #releaseWriter: RelaySocketRuntimeOptions["releaseWriter"];
  readonly #scheduleSnapshotMaintenance: () => void;
  readonly #store: RelayStore;

  constructor(options: RelaySocketRuntimeOptions) {
    this.#connections = options.connections;
    this.#ctx = options.ctx;
    this.#releaseWriter = options.releaseWriter;
    this.#scheduleSnapshotMaintenance = options.scheduleSnapshotMaintenance;
    this.#store = options.store;
  }

  session(): SessionRow | undefined {
    return this.#store.session();
  }

  clientById(clientId: string): ClientRow | undefined {
    return this.#store.clientById(clientId);
  }

  clientByStream(streamId: number): ClientRow | undefined {
    return this.#store.clientByStream(streamId);
  }

  ticket(ticketDigest: string): TicketRow | undefined {
    return this.#store.ticket(ticketDigest);
  }

  connection(attachment: SocketAttachment): ReturnType<typeof connectionSockets> {
    return connectionSockets(this.#ctx, attachment);
  }

  matchingSocket(
    attachment: SocketAttachment,
    channel: "control" | "data",
    except?: WebSocket,
  ): WebSocket | undefined {
    return matchingSocket(this.#ctx, attachment, channel, except);
  }

  isCurrentHost(attachment: SocketAttachment): boolean {
    return (
      attachment.peer === "host" &&
      attachment.hostFence !== null &&
      attachment.hostFence === this.session()?.host_fence
    );
  }

  currentHostControl(except?: WebSocket): WebSocket | undefined {
    const session = this.session();
    if (session === undefined) return undefined;
    return this.#ctx.getWebSockets("peer:host").find((socket) => {
      if (socket === except || socket.readyState !== WebSocket.OPEN) return false;
      const attachment = readAttachment(socket);
      return (
        attachment?.channel === "control" &&
        attachment.hostFence === session.host_fence &&
        attachment.controlState === "active"
      );
    });
  }

  currentInputHostControl(): WebSocket | undefined {
    const candidates = this.#ctx.getWebSockets("peer:host").filter((socket) => {
      if (socket.readyState !== WebSocket.OPEN) return false;
      const attachment = readAttachment(socket);
      return (
        attachment?.channel === "control" &&
        attachment.hostFence !== null &&
        attachment.controlState === "active"
      );
    });
    return candidates.length === 1 ? candidates[0] : undefined;
  }

  currentHostData(except?: WebSocket): WebSocket | undefined {
    const control = this.currentHostControl();
    const controlAttachment = control === undefined ? undefined : readAttachment(control);
    if (controlAttachment === undefined) return undefined;
    return matchingSocket(this.#ctx, controlAttachment, "data", except);
  }

  activeBrowserControl(clientId: string, except?: WebSocket): WebSocket | undefined {
    return this.#ctx.getWebSockets(`client:${clientId}`).find((socket) => {
      if (socket === except || socket.readyState !== WebSocket.OPEN) return false;
      return readAttachment(socket)?.channel === "control";
    });
  }

  browserControlByConnection(connectionId: string): WebSocket | undefined {
    return this.browserControlSockets().find((socket) => {
      const attachment = readAttachment(socket);
      return attachment?.connectionId === connectionId && attachment.controlState === "active";
    });
  }

  browserDataByClient(clientId: string, except?: WebSocket): WebSocket | undefined {
    return this.#ctx.getWebSockets(`client:${clientId}`).find((socket) => {
      return (
        socket !== except &&
        socket.readyState === WebSocket.OPEN &&
        readAttachment(socket)?.channel === "data"
      );
    });
  }

  browserControlSockets(): WebSocket[] {
    return this.#ctx.getWebSockets("peer:browser").filter((socket) => {
      return socket.readyState === WebSocket.OPEN && readAttachment(socket)?.channel === "control";
    });
  }

  browserDataSockets(): WebSocket[] {
    return this.#ctx.getWebSockets("peer:browser").filter((socket) => {
      return socket.readyState === WebSocket.OPEN && readAttachment(socket)?.channel === "data";
    });
  }

  markBrowserDataCatchingUp(): void {
    for (const socket of this.browserDataSockets()) {
      const attachment = readAttachment(socket);
      if (attachment !== undefined) {
        writeAttachment(socket, {
          ...attachment,
          dataState: attachment.dataState === "awaiting-attach" ? "awaiting-attach" : "catching-up",
        });
      }
    }
  }

  sendHostControl(
    webSocket: WebSocket,
    frame: z.input<typeof RelayToHostControlFrameSchema>,
    failureReason: string,
  ): HostControlSendResult {
    const attachment = readAttachment(webSocket);
    if (
      attachment?.peer !== "host" ||
      attachment.channel !== "control" ||
      !this.isCurrentHost(attachment)
    ) {
      return "rejected";
    }
    if (webSocket.readyState !== WebSocket.OPEN || this.currentHostControl() !== webSocket) {
      this.failCurrentHost(attachment, failureReason);
      return "rejected";
    }

    let encoded: string;
    try {
      encoded = encodeControlFrame(RelayToHostControlFrameSchema.parse(frame));
    } catch {
      this.failCurrentHost(attachment, failureReason);
      return "rejected";
    }
    try {
      webSocket.send(encoded);
      return "sent";
    } catch {
      this.failCurrentHost(attachment, failureReason);
      return "uncertain";
    }
  }

  sendInputToHost(
    webSocket: WebSocket,
    frame: z.input<typeof RelayToHostControlFrameSchema>,
    failureReason: string,
  ): HostInputSendOutcome {
    const attachment = readAttachment(webSocket);
    if (
      attachment?.peer !== "host" ||
      attachment.channel !== "control" ||
      attachment.hostFence === null ||
      attachment.controlState !== "active" ||
      webSocket.readyState !== WebSocket.OPEN ||
      this.currentInputHostControl() !== webSocket
    ) {
      return { hostSendAtMs: null, result: "rejected" };
    }

    let encoded: string;
    try {
      encoded = encodeControlFrame(RelayToHostControlFrameSchema.parse(frame));
    } catch {
      return { hostSendAtMs: null, result: "rejected" };
    }
    const encodedBytes = controlTextEncoder.encode(encoded).byteLength;
    if (webSocket.bufferedAmount + encodedBytes > MAX_HOST_INPUT_BUFFERED_BYTES) {
      return { hostSendAtMs: null, result: "rejected" };
    }
    const hostSendAtMs = Date.now();
    try {
      webSocket.send(encoded);
      return { hostSendAtMs, result: "sent" };
    } catch {
      this.failCurrentHost(attachment, failureReason);
      return { hostSendAtMs, result: "uncertain" };
    }
  }

  sendBrowserControl(
    webSocket: WebSocket,
    frame: z.input<typeof ServerControlFrameSchema>,
    failureReason: string,
  ): boolean {
    if (webSocket.readyState === WebSocket.OPEN) {
      try {
        webSocket.send(encodeControlFrame(ServerControlFrameSchema.parse(frame)));
        return true;
      } catch {
        // Pair-scoped isolation below owns all Browser control send failures.
      }
    }
    const attachment = readAttachment(webSocket);
    if (attachment?.peer === "browser") {
      this.isolateBrowserConnection(webSocket, attachment, failureReason);
    } else {
      closeProtocol(webSocket, failureReason);
    }
    return false;
  }

  broadcastBrowserControl(frame: z.input<typeof ServerControlFrameSchema>): void {
    for (const socket of this.browserControlSockets()) {
      this.sendBrowserControl(socket, frame, "browser control broadcast failed");
    }
  }

  failCurrentHost(attachment: SocketAttachment, reason: string): void {
    if (!this.isCurrentHost(attachment) || attachment.hostFence === null) return;
    const fence = attachment.hostFence;
    const matchingControl = matchingSocket(this.#ctx, attachment, "control");
    const wasReady =
      (attachment.channel === "control" && attachment.controlState === "active") ||
      (matchingControl !== undefined && readAttachment(matchingControl)?.controlState === "active");
    if (!this.#connections.invalidateHostConnection(attachment.connectionSetId, fence)) return;
    this.closeSockets(
      (candidate) =>
        candidate.peer === "host" &&
        candidate.hostFence === fence &&
        sameConnection(candidate, attachment),
      undefined,
      4400,
      reason,
    );
    if (wasReady) {
      this.broadcastBrowserControl({ type: "host-offline" });
      this.markBrowserDataCatchingUp();
    }
  }

  isolateBrowserConnection(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    reason: string,
  ): void {
    if (attachment.clientId === null) {
      closeProtocol(webSocket, reason);
      return;
    }
    this.#releaseWriter(attachment.clientId, attachment.leaseFence, attachment.connectionId);
    this.#connections.closeConnectionSet(attachment.connectionSetId);
    this.closeSockets(
      (candidate) =>
        candidate.peer === "browser" &&
        candidate.clientId === attachment.clientId &&
        sameConnection(candidate, attachment),
      undefined,
      4400,
      reason,
    );
  }

  closeSockets(
    predicate: (attachment: SocketAttachment) => boolean,
    except?: WebSocket,
    code = SOCKET_REPLACED,
    reason = "connection replaced",
  ): void {
    let releasedSnapshotPin = false;
    for (const socket of this.#ctx.getWebSockets()) {
      if (socket === except || socket.readyState >= WebSocket.CLOSING) continue;
      const attachment = readAttachment(socket);
      if (attachment !== undefined && predicate(attachment)) {
        if (
          attachment.peer === "browser" &&
          attachment.channel === "data" &&
          attachment.snapshotId !== null
        ) {
          releasedSnapshotPin = true;
        }
        socket.close(code, reason);
      }
    }
    if (releasedSnapshotPin) this.#scheduleSnapshotMaintenance();
  }

  pinnedSnapshotIds(): ReadonlySet<string> {
    const pinned = new Set<string>();
    for (const socket of this.#ctx.getWebSockets("peer:browser")) {
      if (socket.readyState !== WebSocket.OPEN) continue;
      const attachment = readAttachment(socket);
      if (attachment?.channel === "data" && attachment.snapshotId !== null) {
        pinned.add(attachment.snapshotId);
      }
    }
    return pinned;
  }
}
