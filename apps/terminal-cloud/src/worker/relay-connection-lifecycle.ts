import {
  ConnectionSetResponseSchema,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  RelayCapabilitySchema,
  selectRelayCapabilities,
} from "@zhongduan/protocol";
import { z } from "zod";

import { randomId, sha256Hex } from "./auth";
import type { BrowserDeliveryController } from "./browser-delivery-controller";
import { eligibleControlForDataTicket } from "./relay-connection-state";
import type { RelayConnectionStore } from "./relay-connection-store";
import {
  readSocketAttachment as readAttachment,
  SocketAttachmentSchema,
  writeSocketAttachment as writeAttachment,
  type RelayChannel,
} from "./relay-socket";
import type { RelaySocketRuntime } from "./relay-socket-runtime";
import type { SnapshotUploadCoordinator } from "./snapshot-upload-coordinator";
import type { WriterAuthority } from "./writer-authority";

const identifier = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);

export const CreateConnectionSetSchema = z.strictObject({
  sessionId: identifier,
  subject: identifier,
  role: z.enum(["host", "writer", "observer"]),
  clientId: identifier.optional(),
});

const RelayCapabilitiesSchema = z.array(RelayCapabilitySchema).max(16);
const TICKET_LIFETIME_MS = 30_000;

interface RelayConnectionLifecycleOptions {
  connections: RelayConnectionStore;
  ctx: DurableObjectState;
  delivery: BrowserDeliveryController;
  onSocketClosed: (webSocket: WebSocket) => void;
  snapshotUploads: SnapshotUploadCoordinator;
  sockets: RelaySocketRuntime;
  writerAuthority: WriterAuthority;
}

function json(data: unknown, status = 200): Response {
  return Response.json(data, {
    status,
    headers: { "cache-control": "no-store" },
  });
}

async function parseJson(request: Request): Promise<unknown> {
  try {
    return await request.json();
  } catch {
    return undefined;
  }
}

/** Owns connection-set reservation, ticket consumption, socket pairing, and close transitions. */
export class RelayConnectionLifecycle {
  readonly #connections: RelayConnectionStore;
  readonly #ctx: DurableObjectState;
  readonly #delivery: BrowserDeliveryController;
  readonly #onSocketClosed: (webSocket: WebSocket) => void;
  readonly #snapshotUploads: SnapshotUploadCoordinator;
  readonly #sockets: RelaySocketRuntime;
  readonly #writerAuthority: WriterAuthority;

  constructor(options: RelayConnectionLifecycleOptions) {
    this.#connections = options.connections;
    this.#ctx = options.ctx;
    this.#delivery = options.delivery;
    this.#onSocketClosed = options.onSocketClosed;
    this.#snapshotUploads = options.snapshotUploads;
    this.#sockets = options.sockets;
    this.#writerAuthority = options.writerAuthority;
  }

  async createConnectionSet(request: Request): Promise<Response> {
    const parsed = CreateConnectionSetSchema.safeParse(await parseJson(request));
    if (!parsed.success) return json({ error: "invalid-connection-set" }, 400);

    const session = this.#sockets.session();
    if (session === undefined || session.session_id !== parsed.data.sessionId) {
      return json({ error: "session-not-found" }, 404);
    }
    if (parsed.data.role === "host" && parsed.data.clientId !== undefined) {
      return json({ error: "host-cannot-have-client-id" }, 400);
    }

    const peer = parsed.data.role === "host" ? "host" : "browser";
    const connectionSetId = randomId();
    const connectionId = connectionSetId;
    const controlTicket = randomId(32);
    const dataTicket = randomId(32);
    const requestedClientId = peer === "browser" ? (parsed.data.clientId ?? randomId()) : null;
    const selectedCapabilitiesResult = selectRelayCapabilities(
      request.headers.get(RELAY_CAPABILITIES_HEADER),
    );
    if (selectedCapabilitiesResult === undefined) {
      return json({ error: "invalid-connection-set" }, 400);
    }
    const selectedCapabilities = selectedCapabilitiesResult.filter((capability) =>
      parsed.data.role === "host"
        ? capability === RelayCapability.deliveryBarrierOutcomeV1 ||
          capability === RelayCapability.hostDataBatchV1
        : capability === RelayCapability.browserDataBatchV1 ||
          capability === RelayCapability.browserInputAdmissionV1,
    );
    const [controlDigest, dataDigest, principalIdHash] = await Promise.all([
      sha256Hex(controlTicket),
      sha256Hex(dataTicket),
      peer === "browser" ? sha256Hex(parsed.data.subject) : Promise.resolve(undefined),
    ]);
    const now = Date.now();
    const expiresAt = now + TICKET_LIFETIME_MS;
    const reservation = this.#connections.reserveConnectionSet({
      clientId: requestedClientId,
      connectionId,
      connectionSetId,
      controlTicketDigest: controlDigest,
      dataTicketDigest: dataDigest,
      expiresAt,
      now,
      peer,
      principalIdHash,
      relayCapabilitiesJson: JSON.stringify(selectedCapabilities),
      role: parsed.data.role,
      subject: parsed.data.subject,
    });
    if (!reservation.ok) {
      return json(
        { error: reservation.reason },
        reservation.reason === "client-owner-mismatch" ? 403 : 429,
      );
    }

    return json(
      ConnectionSetResponseSchema.parse({
        connectionSetId,
        connectionId,
        clientId: requestedClientId,
        streamId: reservation.client?.stream_id ?? 0,
        deliveryGeneration: reservation.deliveryGeneration,
        expiresAt,
        controlTicket,
        dataTicket,
        ...(selectedCapabilities.length === 0 ? {} : { selectedCapabilities }),
      }),
    );
  }

  async acceptSocket(request: Request, channel: RelayChannel): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return json({ error: "websocket-upgrade-required" }, 426);
    }
    const ticketValue = new URL(request.url).searchParams.get("ticket");
    if (ticketValue === null || ticketValue.length > 256) {
      return json({ error: "invalid-ticket" }, 401);
    }
    const ticketDigest = await sha256Hex(ticketValue);
    let ticket = this.#sockets.ticket(ticketDigest);
    if (ticket === undefined || ticket.channel !== channel || ticket.expires_at <= Date.now()) {
      if (ticket?.expires_at !== undefined && ticket.expires_at <= Date.now()) {
        this.#connections.expireTicket(ticketDigest, Date.now());
      }
      return json({ error: "invalid-ticket" }, 401);
    }
    let relayCapabilities: z.output<typeof RelayCapabilitiesSchema>;
    try {
      relayCapabilities = RelayCapabilitiesSchema.parse(
        JSON.parse(ticket.relay_capabilities_json) as unknown,
      );
    } catch {
      return json({ error: "invalid-ticket" }, 401);
    }

    const eligibleControl = eligibleControlForDataTicket(
      this.#ctx,
      ticket,
      this.#sockets.session()?.host_fence,
    );
    if (channel === "data" && eligibleControl === undefined) {
      return json({ error: "control-channel-required" }, 409);
    }
    if (ticket.peer === "browser" && channel === "control") {
      const claimedTicket = this.#connections.claimBrowserControl(ticket);
      if (claimedTicket === undefined) {
        this.#connections.discardReservation(ticket);
        return json({ error: "client-reservation-unavailable" }, 409);
      }
      ticket = claimedTicket;
    }

    let hostFence = ticket.host_fence;
    if (ticket.peer === "host" && channel === "control") {
      hostFence = this.#connections.advanceHostFence(ticket.connection_set_id);
      this.#sockets.closeSockets(
        (candidate) => candidate.peer === "host" && candidate.hostFence !== hostFence,
      );
      this.#sockets.broadcastBrowserControl({ type: "host-offline" });
      this.#sockets.markBrowserDataCatchingUp();
    } else if (ticket.peer === "host" && channel === "data") {
      hostFence = eligibleControl?.hostFence ?? null;
      this.#sockets.closeSockets(
        (candidate) => candidate.peer === "host" && candidate.channel === "data",
      );
    } else if (ticket.peer === "browser" && ticket.client_id !== null) {
      const client = this.#sockets.clientById(ticket.client_id);
      if (
        client === undefined ||
        client.registered_at === null ||
        client.delivery_generation !== ticket.delivery_generation
      ) {
        return json({ error: "stale-ticket" }, 409);
      }
      if (channel === "control") {
        for (const socket of this.#ctx.getWebSockets(`client:${ticket.client_id}`)) {
          const previous = readAttachment(socket);
          if (previous?.channel === "control") {
            this.#writerAuthority.release(
              ticket.client_id,
              previous.leaseFence,
              previous.connectionId,
            );
          }
        }
        this.#sockets.closeSockets(
          (candidate) => candidate.peer === "browser" && candidate.clientId === ticket.client_id,
        );
      } else {
        this.#sockets.closeSockets(
          (candidate) =>
            candidate.peer === "browser" &&
            candidate.channel === "data" &&
            candidate.clientId === ticket.client_id,
        );
      }
    }
    if (ticket.peer === "host" && hostFence === null) {
      return json({ error: "host-control-channel-required" }, 409);
    }

    this.#connections.consumeTicket(ticketDigest);
    const attachment = SocketAttachmentSchema.parse({
      version: 2,
      peer: ticket.peer,
      channel,
      connectionSetId: ticket.connection_set_id,
      connectionId: ticket.connection_id,
      subject: ticket.subject,
      clientId: ticket.client_id,
      role: ticket.role,
      streamId: ticket.stream_id,
      deliveryGeneration: ticket.delivery_generation,
      hostFence,
      leaseFence: null,
      leaseExpiresAt: null,
      controlState: ticket.peer === "browser" && channel === "control" ? "awaiting-attach" : null,
      dataState: ticket.peer === "browser" && channel === "data" ? "awaiting-attach" : null,
      firstEventSeq: null,
      ackedEventSeq: null,
      sentEventSeq: null,
      firstPtyOffset: null,
      ackedPtyOffset: null,
      sentPtyOffset: null,
      replayMode: null,
      snapshotId: null,
      replayCommitEventSeq: null,
      replayCommitPtyOffset: null,
      relayCapabilities,
    });
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    const tags = [
      `peer:${attachment.peer}`,
      `channel:${attachment.channel}`,
      `set:${attachment.connectionSetId}`,
    ];
    if (attachment.clientId !== null) tags.push(`client:${attachment.clientId}`);
    if (attachment.streamId > 0) tags.push(`stream:${attachment.streamId}`);
    this.#ctx.acceptWebSocket(server, tags);
    writeAttachment(server, attachment);
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketClose(webSocket: WebSocket): Promise<void> {
    return this.#processWebSocketClose(webSocket).catch(() => undefined);
  }

  async webSocketError(webSocket: WebSocket): Promise<void> {
    try {
      await this.#processWebSocketClose(webSocket);
    } catch {
      // The lifecycle transition is best-effort during a runtime socket failure.
    }
    try {
      if (webSocket.readyState < WebSocket.CLOSING) {
        webSocket.close(1011, "relay socket error");
      }
    } catch {
      // Workerd may already have released the errored native socket.
    }
  }

  async #processWebSocketClose(webSocket: WebSocket): Promise<void> {
    this.#onSocketClosed(webSocket);
    const attachment = readAttachment(webSocket);
    if (attachment === undefined) return;

    if (
      attachment.peer === "browser" &&
      attachment.channel === "data" &&
      attachment.snapshotId !== null
    ) {
      this.#snapshotUploads.scheduleMaintenance();
    }

    if (attachment.channel === "data") {
      if (attachment.peer === "host") {
        if (
          !this.#sockets.isCurrentHost(attachment) ||
          this.#sockets.matchingSocket(attachment, "control") === undefined ||
          this.#sockets.matchingSocket(attachment, "data", webSocket) !== undefined
        ) {
          return;
        }
        this.#sockets.failCurrentHost(attachment, "host data channel disconnected");
        return;
      }

      if (attachment.clientId === null) return;
      const client = this.#sockets.clientById(attachment.clientId);
      const control = this.#sockets.activeBrowserControl(attachment.clientId);
      const controlAttachment = control === undefined ? undefined : readAttachment(control);
      if (
        client?.delivery_generation !== attachment.deliveryGeneration ||
        this.#sockets.browserDataByClient(attachment.clientId, webSocket) !== undefined ||
        control === undefined ||
        controlAttachment?.connectionSetId !== attachment.connectionSetId ||
        controlAttachment.connectionId !== attachment.connectionId
      ) {
        return;
      }
      if (controlAttachment.controlState !== "active") {
        this.#sockets.isolateBrowserConnection(
          control,
          controlAttachment,
          "browser data disconnected before attach",
        );
        return;
      }
      await this.#delivery.reset(attachment.clientId, "data-disconnected", false);
      return;
    }

    if (attachment.peer === "host") {
      const session = this.#sockets.session();
      if (
        session?.host_fence !== attachment.hostFence ||
        this.#sockets.matchingSocket(attachment, "control", webSocket) !== undefined
      ) {
        return;
      }
      this.#sockets.failCurrentHost(attachment, "host control channel disconnected");
      return;
    }

    if (attachment.clientId === null) return;
    this.#writerAuthority.release(
      attachment.clientId,
      attachment.leaseFence,
      attachment.connectionId,
    );
    if (this.#sockets.activeBrowserControl(attachment.clientId, webSocket) !== undefined) return;
    this.#connections.closeConnectionSet(attachment.connectionSetId);
    this.#sockets.closeSockets(
      (candidate) => candidate.peer === "browser" && candidate.clientId === attachment.clientId,
      webSocket,
    );
  }
}
