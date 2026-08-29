import {
  ClientControlFrameSchema,
  ConnectionSetResponseSchema,
  DATA_HEADER_BYTES,
  DataFrameKind,
  HostControlFrameSchema,
  HostCapabilityReclaimRequestSchema,
  MAX_U64,
  PositiveDecimalU64Schema,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  RelayCapabilitySchema,
  RelayToHostControlFrameSchema,
  selectRelayCapabilities,
  ServerControlFrameSchema,
  decodeControlFrame,
  decodeDataFrame,
  decodeDeliveryBarrierPayload,
  encodeControlFrame,
  rewriteDelivery,
  type DataFrame,
  type DeliveryBarrierPayload,
} from "@zhongduan/protocol";
import { DurableObject } from "cloudflare:workers";
import { z } from "zod";
import { randomId, sha256Hex } from "./auth";
import {
  connectionSockets,
  eligibleControlForDataTicket,
  matchingSocket,
  sameConnection,
} from "./relay-connection-state";
import { RelayConnectionStore } from "./relay-connection-store";
import type { CloudEnv } from "./env";
import { advanceDeliveryCursor } from "./relay-delivery";
import {
  BoundedSerialQueue,
  RELAY_MESSAGE_QUEUE_PROFILES,
  type SocketQueueLimits,
} from "./relay-message-queue";
import {
  readSocketAttachment as readAttachment,
  SocketAttachmentSchema,
  writeSocketAttachment as writeAttachment,
  type RelayChannel as Channel,
  type SocketAttachment,
} from "./relay-socket";
import {
  migrateRelayStore,
  RelayStore,
  type ClientRow,
  type SessionRow,
  type TicketRow,
  type WriterLeaseRow,
} from "./relay-store";
import { SnapshotStore } from "./snapshot-store";
import { SnapshotUploadCoordinator } from "./snapshot-upload-coordinator";

const identifier = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);
const engineId = z.string().min(1).max(512);

const InitializeSessionSchema = z.strictObject({
  sessionId: identifier,
  sessionEpoch: PositiveDecimalU64Schema,
  engineId,
});

const VerifySessionIdentitySchema = HostCapabilityReclaimRequestSchema.extend({
  sessionId: identifier,
});

const CreateConnectionSetSchema = z.strictObject({
  sessionId: identifier,
  subject: identifier,
  role: z.enum(["host", "writer", "observer"]),
  clientId: identifier.optional(),
});
const RelayCapabilitiesSchema = z.array(RelayCapabilitySchema).max(16);
const BROWSER_RELAY_CAPABILITIES = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
] as const;
const HOST_RELAY_CAPABILITIES = [
  ...BROWSER_RELAY_CAPABILITIES,
  RelayCapability.deliveryBarrierOutcomeV1,
] as const;

interface AcquiredLease {
  fence: string;
  token: string;
}

type HostControlSendResult = "sent" | "rejected" | "uncertain";

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

const TICKET_LIFETIME_MS = 30_000;
const WRITER_LEASE_MS = 30_000;
const MAX_BROWSER_CONNECTIONS = 16;
const MAX_PENDING_CONNECTION_SETS = MAX_BROWSER_CONNECTIONS + 1;
const MAX_CONTROL_MESSAGE_CHARS = 6 * 1024 * 1024 + 4_096;
const MAX_HOST_DATA_BYTES = DATA_HEADER_BYTES + 16 * 1024;
const SOCKET_REPLACED = 4001;
const SLOW_CLIENT = 4008;

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

function trySendServerControl(
  webSocket: WebSocket,
  frame: z.input<typeof ServerControlFrameSchema>,
): boolean {
  if (webSocket.readyState !== WebSocket.OPEN) return false;
  const encoded = encodeControlFrame(ServerControlFrameSchema.parse(frame));
  try {
    webSocket.send(encoded);
    return true;
  } catch {
    return false;
  }
}

function closeProtocol(webSocket: WebSocket, reason: string): void {
  if (webSocket.readyState < WebSocket.CLOSING) {
    webSocket.close(4400, reason);
  }
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

function selectEnabledRelayCapabilities(
  header: string | null,
  peer: "browser" | "host",
): RelayCapability[] | undefined {
  const selected = selectRelayCapabilities(header);
  if (selected === undefined) return undefined;
  const requested = new Set(selected);
  const enabled = peer === "host" ? HOST_RELAY_CAPABILITIES : BROWSER_RELAY_CAPABILITIES;
  return enabled.filter((capability) => requested.has(capability));
}

export class TerminalSessionDO extends DurableObject<CloudEnv> {
  private readonly sql: SqlStorage;
  private readonly store: RelayStore;
  private readonly connections: RelayConnectionStore;
  private readonly snapshots: SnapshotStore;
  private readonly snapshotUploads: SnapshotUploadCoordinator;
  private readonly messageQueue = new BoundedSerialQueue<WebSocket>();

  constructor(ctx: DurableObjectState, env: CloudEnv) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.store = new RelayStore(this.sql);
    migrateRelayStore(ctx, this.sql);
    this.connections = new RelayConnectionStore(
      ctx,
      this.sql,
      this.store,
      MAX_BROWSER_CONNECTIONS,
      MAX_PENDING_CONNECTION_SETS,
    );
    this.snapshots = new SnapshotStore(ctx, this.sql, this.store, MAX_BROWSER_CONNECTIONS);
    this.snapshotUploads = new SnapshotUploadCoordinator(ctx, env.SNAPSHOTS, this.snapshots, () =>
      this.pinnedSnapshotIds(),
    );
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    void this.ctx.blockConcurrencyWhile(() => this.snapshotUploads.initialize());
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/internal/initialize") {
      return this.initializeSession(request);
    }
    if (request.method === "POST" && url.pathname === "/internal/session-identity/verify") {
      return this.verifySessionIdentity(request);
    }
    if (request.method === "POST" && url.pathname === "/internal/connection-sets") {
      return this.createConnectionSet(request);
    }
    if (request.method === "POST" && url.pathname.startsWith("/internal/snapshots/upload/")) {
      return this.uploadSnapshot(request, url.pathname.slice("/internal/snapshots/upload/".length));
    }
    if (request.method === "GET" && url.pathname.startsWith("/internal/snapshots/")) {
      return this.getPublishedSnapshot(url.pathname.slice("/internal/snapshots/".length));
    }
    if (request.method === "GET" && url.pathname.startsWith("/internal/ws/")) {
      const channel = url.pathname.slice("/internal/ws/".length);
      if (channel === "control" || channel === "data") {
        return this.acceptSocket(request, channel);
      }
    }
    return json({ error: "not-found" }, 404);
  }

  webSocketMessage(webSocket: WebSocket, message: ArrayBuffer | string): Promise<void> {
    const inbound = this.validateInboundMessage(webSocket, message);
    if (inbound === undefined) return Promise.resolve();
    const processing = this.messageQueue.enqueue(
      webSocket,
      inbound.bytes,
      () => this.processWebSocketMessage(webSocket, message),
      inbound.queueLimits,
    );
    if (processing === undefined) {
      this.rejectInboundMessage(webSocket, inbound.attachment, "relay message queue exceeded");
      return Promise.resolve();
    }
    return processing.catch(() => {
      this.rejectInboundMessage(webSocket, inbound.attachment, "relay message failed");
    });
  }

  async alarm(): Promise<void> {
    await this.snapshotUploads.maintain();
  }

  private validateInboundMessage(
    webSocket: WebSocket,
    message: ArrayBuffer | string,
  ): { attachment: SocketAttachment; bytes: number; queueLimits: SocketQueueLimits } | undefined {
    const attachment = readAttachment(webSocket);
    if (attachment === undefined) {
      closeProtocol(webSocket, "invalid attachment");
      return undefined;
    }
    if (attachment.channel === "control") {
      if (typeof message !== "string") {
        this.rejectInboundMessage(webSocket, attachment, "control channel requires text frames");
        return undefined;
      }
      if (message.length > MAX_CONTROL_MESSAGE_CHARS) {
        this.rejectInboundMessage(webSocket, attachment, "control frame is too large");
        return undefined;
      }
      return {
        attachment,
        bytes: message.length * 2,
        queueLimits:
          attachment.peer === "host"
            ? RELAY_MESSAGE_QUEUE_PROFILES.hostControl
            : RELAY_MESSAGE_QUEUE_PROFILES.browserControl,
      };
    }
    if (attachment.peer !== "host") {
      closeProtocol(webSocket, "browser data channel is receive-only");
      return undefined;
    }
    if (typeof message === "string") {
      this.rejectInboundMessage(webSocket, attachment, "host data channel requires binary frames");
      return undefined;
    }
    if (message.byteLength > MAX_HOST_DATA_BYTES) {
      this.rejectInboundMessage(webSocket, attachment, "host data frame is too large");
      return undefined;
    }
    return {
      attachment,
      bytes: message.byteLength,
      queueLimits: RELAY_MESSAGE_QUEUE_PROFILES.hostData,
    };
  }

  private rejectInboundMessage(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    reason: string,
  ): void {
    if (attachment.peer === "host") {
      this.failCurrentHost(attachment, reason);
      if (webSocket.readyState < WebSocket.CLOSING) closeProtocol(webSocket, reason);
      return;
    }
    closeProtocol(webSocket, reason);
  }

  private async processWebSocketMessage(
    webSocket: WebSocket,
    message: ArrayBuffer | string,
  ): Promise<void> {
    const attachment = readAttachment(webSocket);
    if (attachment === undefined) {
      closeProtocol(webSocket, "invalid attachment");
      return;
    }

    if (attachment.channel === "control") {
      if (typeof message !== "string") {
        closeProtocol(webSocket, "control channel requires text frames");
        return;
      }
      if (message.length > MAX_CONTROL_MESSAGE_CHARS) {
        closeProtocol(webSocket, "control frame is too large");
        return;
      }
      if (attachment.peer === "host") {
        await this.handleHostControl(webSocket, attachment, message);
      } else {
        await this.handleBrowserControl(webSocket, attachment, message);
      }
      return;
    }

    if (attachment.peer !== "host") {
      closeProtocol(webSocket, "browser data channel is receive-only");
      return;
    }
    if (typeof message === "string") {
      closeProtocol(webSocket, "host data channel requires binary frames");
      return;
    }
    if (message.byteLength > MAX_HOST_DATA_BYTES) {
      this.failCurrentHost(attachment, "host data frame is too large");
      return;
    }
    await this.handleHostData(webSocket, attachment, new Uint8Array(message));
  }

  webSocketClose(
    webSocket: WebSocket,
    _code: number,
    _reason: string,
    _wasClean: boolean,
  ): Promise<void> {
    return this.processWebSocketClose(webSocket).catch(() => undefined);
  }

  private async processWebSocketClose(webSocket: WebSocket): Promise<void> {
    const attachment = readAttachment(webSocket);
    if (attachment === undefined) return;

    if (
      attachment.peer === "browser" &&
      attachment.channel === "data" &&
      attachment.snapshotId !== null
    ) {
      this.snapshotUploads.scheduleMaintenance();
    }

    if (attachment.channel === "data") {
      if (attachment.peer === "host") {
        if (
          !this.isCurrentHost(attachment) ||
          matchingSocket(this.ctx, attachment, "control") === undefined ||
          matchingSocket(this.ctx, attachment, "data", webSocket) !== undefined
        ) {
          return;
        }
        this.failCurrentHost(attachment, "host data channel disconnected");
        return;
      }

      if (attachment.clientId === null) return;
      const client = this.clientById(attachment.clientId);
      const control = this.activeBrowserControl(attachment.clientId);
      const controlAttachment = control === undefined ? undefined : readAttachment(control);
      if (
        client?.delivery_generation !== attachment.deliveryGeneration ||
        this.browserDataByClient(attachment.clientId, webSocket) !== undefined ||
        control === undefined ||
        controlAttachment?.connectionSetId !== attachment.connectionSetId ||
        controlAttachment.connectionId !== attachment.connectionId
      ) {
        return;
      }
      if (controlAttachment.controlState !== "active") {
        this.isolateBrowserConnection(
          control,
          controlAttachment,
          "browser data disconnected before attach",
        );
        return;
      }
      await this.resetBrowserDelivery(attachment.clientId, "data-disconnected", false);
      return;
    }

    if (attachment.peer === "host") {
      const session = this.session();
      if (
        session?.host_fence !== attachment.hostFence ||
        matchingSocket(this.ctx, attachment, "control", webSocket) !== undefined
      ) {
        return;
      }
      this.failCurrentHost(attachment, "host control channel disconnected");
      return;
    }

    if (attachment.clientId === null) return;
    this.releaseWriterLease(attachment.clientId, attachment.leaseFence);
    if (this.activeBrowserControl(attachment.clientId, webSocket) !== undefined) return;
    this.connections.closeConnectionSet(attachment.connectionSetId);
    this.closeSockets((candidate) => {
      return candidate.peer === "browser" && candidate.clientId === attachment.clientId;
    }, webSocket);
  }

  async webSocketError(webSocket: WebSocket, _error: unknown): Promise<void> {
    try {
      await this.processWebSocketClose(webSocket);
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

  private async initializeSession(request: Request): Promise<Response> {
    const parsed = InitializeSessionSchema.safeParse(await parseJson(request));
    if (!parsed.success) {
      return json({ error: "invalid-session" }, 400);
    }

    const existing = this.session();
    if (existing !== undefined) {
      if (
        existing.session_id !== parsed.data.sessionId ||
        existing.session_epoch !== parsed.data.sessionEpoch ||
        existing.engine_id !== parsed.data.engineId
      ) {
        return json({ error: "session-conflict" }, 409);
      }
      return json({ created: false });
    }

    this.sql.exec(
      `INSERT INTO session_state
        (singleton, session_id, session_epoch, engine_id, updated_at,
         snapshot_recent_scan_done, snapshot_retention_backlog)
       VALUES (1, ?, ?, ?, ?, 1, 0)`,
      parsed.data.sessionId,
      parsed.data.sessionEpoch,
      parsed.data.engineId,
      Date.now(),
    );
    return json({ created: true }, 201);
  }

  private async verifySessionIdentity(request: Request): Promise<Response> {
    const parsed = VerifySessionIdentitySchema.safeParse(await parseJson(request));
    if (!parsed.success) return json({ error: "invalid-session-identity" }, 400);
    const session = this.session();
    if (session === undefined || session.session_id !== parsed.data.sessionId) {
      return json({ error: "session-not-found" }, 404);
    }
    if (
      session.engine_id !== parsed.data.engineId ||
      session.session_epoch !== parsed.data.sessionEpoch
    ) {
      return json({ error: "session-identity-mismatch" }, 409);
    }
    return json({ matches: true });
  }

  private uploadSnapshot(request: Request, snapshotId: string): Promise<Response> {
    const session = this.session();
    const sessionId =
      session !== undefined && identifier.safeParse(snapshotId).success
        ? session.session_id
        : undefined;
    return this.snapshotUploads.upload(request, snapshotId, sessionId);
  }

  private getPublishedSnapshot(snapshotId: string): Response {
    if (!identifier.safeParse(snapshotId).success) return json({ error: "not-found" }, 404);
    const snapshot = this.snapshots.published(snapshotId);
    return snapshot === undefined ? json({ error: "snapshot-not-found" }, 404) : json(snapshot);
  }

  private async createConnectionSet(request: Request): Promise<Response> {
    const parsed = CreateConnectionSetSchema.safeParse(await parseJson(request));
    if (!parsed.success) {
      return json({ error: "invalid-connection-set" }, 400);
    }

    const session = this.session();
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
    const selectedCapabilitiesHeader = request.headers.get(RELAY_CAPABILITIES_HEADER);
    const selectedCapabilitiesResult = selectEnabledRelayCapabilities(
      selectedCapabilitiesHeader,
      peer,
    );
    if (selectedCapabilitiesResult === undefined) {
      return json({ error: "invalid-connection-set" }, 400);
    }
    const selectedCapabilities = selectedCapabilitiesResult;
    const [controlDigest, dataDigest, principalIdHash] = await Promise.all([
      sha256Hex(controlTicket),
      sha256Hex(dataTicket),
      peer === "browser" ? sha256Hex(parsed.data.subject) : Promise.resolve(undefined),
    ]);
    const now = Date.now();
    const expiresAt = now + TICKET_LIFETIME_MS;
    const reservation = this.connections.reserveConnectionSet({
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
    const client = reservation.client;

    return json(
      ConnectionSetResponseSchema.parse({
        connectionSetId,
        connectionId,
        clientId: requestedClientId,
        streamId: client?.stream_id ?? 0,
        deliveryGeneration: reservation.deliveryGeneration,
        expiresAt,
        controlTicket,
        dataTicket,
        ...(selectedCapabilities.includes(RelayCapability.capabilityNegotiationV1)
          ? { negotiatedCapabilities: selectedCapabilities }
          : {}),
      }),
    );
  }

  private async acceptSocket(request: Request, channel: Channel): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return json({ error: "websocket-upgrade-required" }, 426);
    }
    const ticketValue = new URL(request.url).searchParams.get("ticket");
    if (ticketValue === null || ticketValue.length > 256) {
      return json({ error: "invalid-ticket" }, 401);
    }
    const ticketDigest = await sha256Hex(ticketValue);
    let ticket = this.ticket(ticketDigest);
    if (ticket === undefined || ticket.channel !== channel || ticket.expires_at <= Date.now()) {
      if (ticket?.expires_at !== undefined && ticket.expires_at <= Date.now()) {
        this.connections.expireTicket(ticketDigest, Date.now());
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
      this.ctx,
      ticket,
      this.session()?.host_fence,
    );
    if (channel === "data" && eligibleControl === undefined) {
      return json({ error: "control-channel-required" }, 409);
    }
    if (ticket.peer === "browser" && channel === "control") {
      const claimedTicket = this.connections.claimBrowserControl(ticket);
      if (claimedTicket === undefined) {
        this.connections.discardReservation(ticket);
        return json({ error: "client-reservation-unavailable" }, 409);
      }
      ticket = claimedTicket;
    }

    let hostFence = ticket.host_fence;
    if (ticket.peer === "host" && channel === "control") {
      hostFence = this.connections.advanceHostFence(ticket.connection_set_id);
      this.closeSockets(
        (candidate) => candidate.peer === "host" && candidate.hostFence !== hostFence,
      );
      this.broadcastBrowserControl({ type: "host-offline" });
      this.markBrowserDataCatchingUp();
    } else if (ticket.peer === "host" && channel === "data") {
      hostFence = eligibleControl?.hostFence ?? null;
      this.closeSockets((candidate) => {
        return candidate.peer === "host" && candidate.channel === "data";
      });
    } else if (ticket.peer === "browser" && ticket.client_id !== null) {
      const client = this.clientById(ticket.client_id);
      if (
        client === undefined ||
        client.registered_at === null ||
        client.delivery_generation !== ticket.delivery_generation
      ) {
        return json({ error: "stale-ticket" }, 409);
      }
      if (channel === "control") {
        for (const socket of this.ctx.getWebSockets(`client:${ticket.client_id}`)) {
          const previous = readAttachment(socket);
          if (previous?.channel === "control") {
            this.releaseWriterLease(ticket.client_id, previous.leaseFence);
          }
        }
        this.closeSockets((candidate) => {
          return candidate.peer === "browser" && candidate.clientId === ticket.client_id;
        });
      } else {
        this.closeSockets((candidate) => {
          return (
            candidate.peer === "browser" &&
            candidate.channel === "data" &&
            candidate.clientId === ticket.client_id
          );
        });
      }
    }
    if (ticket.peer === "host" && hostFence === null) {
      return json({ error: "host-control-channel-required" }, 409);
    }

    this.connections.consumeTicket(ticketDigest);
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
    this.ctx.acceptWebSocket(server, tags);
    writeAttachment(server, attachment);
    return new Response(null, { status: 101, webSocket: client });
  }

  private async handleHostControl(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    message: string,
  ): Promise<void> {
    if (!this.isCurrentHost(attachment)) return;

    let frame: z.output<typeof HostControlFrameSchema>;
    try {
      frame = decodeControlFrame(message, HostControlFrameSchema);
    } catch {
      closeProtocol(webSocket, "invalid host control frame");
      return;
    }

    if (frame.type === "host-ready") {
      const session = this.session();
      const connection = connectionSockets(this.ctx, attachment);
      if (
        session === undefined ||
        connection.control !== webSocket ||
        connection.phase !== "paired" ||
        frame.engineId !== session.engine_id ||
        frame.sessionEpoch !== session.session_epoch ||
        BigInt(frame.headEventSeq) < BigInt(session.head_event_seq) ||
        BigInt(frame.nextPtyOffset) < BigInt(session.next_pty_offset)
      ) {
        this.failCurrentHost(attachment, "host state does not match session");
        return;
      }
      this.sql.exec(
        `UPDATE session_state
         SET head_event_seq = ?, next_pty_offset = ?, updated_at = ?
         WHERE singleton = 1`,
        frame.headEventSeq,
        frame.nextPtyOffset,
        Date.now(),
      );
      writeAttachment(webSocket, { ...attachment, controlState: "active" });
      const activeBrowsers = this.browserControlSockets().flatMap((browser) => {
        const browserAttachment = readAttachment(browser);
        return browserAttachment?.controlState === "active" && browserAttachment.clientId !== null
          ? [{ browser, attachment: browserAttachment }]
          : [];
      });
      await Promise.all(
        activeBrowsers.map(({ attachment: browserAttachment }) => {
          return this.resetBrowserDelivery(browserAttachment.clientId!, "host-reconnect", false, {
            webSocket,
            hostFence: attachment.hostFence!,
          });
        }),
      );
      const readyConnection = connectionSockets(this.ctx, attachment);
      if (
        webSocket.readyState !== WebSocket.OPEN ||
        this.currentHostControl() !== webSocket ||
        readyConnection.control !== webSocket ||
        readyConnection.phase !== "ready"
      ) {
        return;
      }
      this.sendHostControl(
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
      const browser = this.browserControlByConnection(frame.connectionId);
      if (browser === undefined) return;
      const { connectionId: _connectionId, ...browserFrame } = frame;
      this.sendBrowserControl(browser, browserFrame, "input acknowledgement delivery failed");
      return;
    }

    const browser = this.browserControlByConnection(frame.connectionId);
    const browserAttachment = browser === undefined ? undefined : readAttachment(browser);
    if (browserAttachment?.clientId !== null && browserAttachment?.clientId !== undefined) {
      await this.resetBrowserDelivery(browserAttachment.clientId, frame.reason, false);
    }
  }

  private handleDeliveryBarrier(
    hostData: WebSocket,
    hostAttachment: SocketAttachment,
    session: SessionRow,
    frame: DataFrame,
  ): void {
    const hostControl = matchingSocket(this.ctx, hostAttachment, "control");
    const hostControlAttachment =
      hostControl === undefined ? undefined : readAttachment(hostControl);
    if (
      hostData !== this.currentHostData() ||
      hostControl === undefined ||
      hostControl !== this.currentHostControl() ||
      hostControlAttachment === undefined ||
      hostControlAttachment.hostFence !== hostAttachment.hostFence ||
      !sameConnection(hostControlAttachment, hostAttachment)
    ) {
      this.failCurrentHost(hostAttachment, "delivery barrier host channels are not current");
      return;
    }
    let payload: DeliveryBarrierPayload;
    try {
      payload = decodeDeliveryBarrierPayload(frame.payload);
    } catch {
      this.failCurrentHost(hostAttachment, "invalid delivery barrier payload");
      return;
    }
    if (
      frame.streamId === 0 ||
      frame.eventSeq !== BigInt(session.head_event_seq) ||
      frame.ptyOffset !== BigInt(session.next_pty_offset)
    ) {
      this.failCurrentHost(hostAttachment, "delivery barrier does not match canonical head");
      return;
    }

    const client = this.clientByStream(frame.streamId);
    if (client === undefined || frame.deliveryGeneration !== BigInt(client.delivery_generation)) {
      this.sendDeliveryBarrierResult(hostControl, frame, payload, {
        status: "stale",
        reason: "generation-fenced",
      });
      return;
    }
    const browserControl = this.activeBrowserControl(client.client_id);
    const controlAttachment =
      browserControl === undefined ? undefined : readAttachment(browserControl);
    const browserData = this.browserDataByClient(client.client_id);
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
      this.sendDeliveryBarrierResult(hostControl, frame, payload, {
        status: "stale",
        reason: "client-gone",
      });
      return;
    }
    if (
      dataAttachment.dataState !== "catching-up" ||
      !hasUnadvancedDeliveryCursor(dataAttachment)
    ) {
      this.failCurrentHost(hostAttachment, "delivery barrier conflicts with active delivery");
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
      this.failCurrentHost(hostAttachment, "delivery barrier conflicts with pinned delivery");
      return;
    }

    let nextAttachment = dataAttachment;
    let browserFrame: z.input<typeof ServerControlFrameSchema>;
    if (payload.mode === "warm") {
      if (dataAttachment.firstEventSeq === null || dataAttachment.firstPtyOffset === null) {
        this.sendDeliveryBarrierResult(hostControl, frame, payload, {
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
      const snapshot = this.snapshots.published(payload.snapshotId);
      if (snapshot === undefined) {
        this.sendDeliveryBarrierResult(hostControl, frame, payload, {
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
        this.sendDeliveryBarrierResult(hostControl, frame, payload, {
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
        this.failCurrentHost(hostAttachment, "snapshot barrier conflicts with pinned seed");
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
      if (nextAttachment.snapshotId !== null) this.snapshotUploads.scheduleMaintenance();
    }
    if (!this.sendBrowserControl(browserControl, browserFrame, "delivery start failed")) {
      this.sendDeliveryBarrierResult(hostControl, frame, payload, {
        status: "rejected",
        reason: "browser-control-send-failed",
        retryScope: "drop-client",
      });
      return;
    }
    this.sendDeliveryBarrierResult(hostControl, frame, payload, { status: "ready" });
  }

  private sendDeliveryBarrierResult(
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
    this.sendHostControl(
      webSocket,
      payload.mode === "warm"
        ? { ...common, mode: "warm" }
        : { ...common, mode: "snapshot", snapshotId: payload.snapshotId },
      "delivery barrier result delivery failed",
    );
  }

  private async handleBrowserControl(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    message: string,
  ): Promise<void> {
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
      await this.attachBrowser(webSocket, attachment, frame);
      return;
    }
    if (
      webSocket.readyState !== WebSocket.OPEN ||
      this.activeBrowserControl(attachment.clientId) !== webSocket
    ) {
      return;
    }
    if (frame.type === "writer-lease-renew") {
      const expiresAt =
        attachment.controlState === "active" && attachment.role === "writer"
          ? await this.renewWriterLease(
              attachment.clientId,
              attachment.leaseFence,
              frame.writerLease,
            )
          : undefined;
      const latestAttachment = readAttachment(webSocket);
      const active =
        expiresAt !== undefined &&
        webSocket.readyState === WebSocket.OPEN &&
        latestAttachment?.controlState === "active" &&
        latestAttachment.clientId === attachment.clientId &&
        latestAttachment.leaseFence === attachment.leaseFence &&
        this.activeBrowserControl(attachment.clientId) === webSocket;
      this.sendBrowserControl(
        webSocket,
        {
          type: "writer-lease-status",
          active,
          ...(active ? { expiresAt } : {}),
        },
        "writer lease status delivery failed",
      );
      return;
    }
    if (frame.type === "ack") {
      const client = this.clientById(attachment.clientId);
      if (
        attachment.controlState !== "active" ||
        this.currentHostControl() === undefined ||
        client?.delivery_generation !== frame.deliveryGeneration
      ) {
        return;
      }
      this.acknowledgeBrowser(webSocket, attachment, frame);
      return;
    }
    if (attachment.controlState !== "active") {
      this.rejectInput(webSocket, frame.inputEpoch, frame.clientInputSeq);
      return;
    }
    const validLease =
      attachment.role === "writer" &&
      (await this.renewWriterLease(
        attachment.clientId,
        attachment.leaseFence,
        frame.writerLease,
      )) !== undefined;
    const latestAttachment = readAttachment(webSocket);
    if (
      !validLease ||
      webSocket.readyState !== WebSocket.OPEN ||
      latestAttachment === undefined ||
      latestAttachment.clientId !== attachment.clientId ||
      latestAttachment.controlState !== "active" ||
      latestAttachment.leaseFence === null ||
      latestAttachment.leaseFence !== attachment.leaseFence ||
      this.activeBrowserControl(attachment.clientId) !== webSocket
    ) {
      this.rejectInput(webSocket, frame.inputEpoch, frame.clientInputSeq);
      return;
    }

    const host = this.currentHostControl();
    if (host === undefined) {
      if (
        !this.sendBrowserControl(webSocket, { type: "host-offline" }, "host status delivery failed")
      ) {
        return;
      }
      this.rejectInput(webSocket, frame.inputEpoch, frame.clientInputSeq);
      return;
    }
    const sendResult = this.sendHostControl(
      host,
      {
        ...frame,
        connectionId: attachment.connectionId,
        clientId: attachment.clientId,
        writerFence: latestAttachment.leaseFence,
      },
      "semantic input delivery failed",
    );
    if (sendResult !== "sent") {
      this.rejectInput(
        webSocket,
        frame.inputEpoch,
        frame.clientInputSeq,
        sendResult === "uncertain" ? "uncertain" : "rejected",
      );
    }
  }

  private async attachBrowser(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<z.output<typeof ClientControlFrameSchema>, { type: "attach" }>,
  ): Promise<void> {
    const initialResult = this.resolveBrowserAttachContext(webSocket, attachment, frame);
    if (!initialResult.ok) {
      this.rejectBrowserAttach(webSocket, initialResult);
      return;
    }
    const initial = initialResult.value;
    const initialActivation = initial.controlAttachment.controlState === "awaiting-attach";
    const lease =
      initialActivation && attachment.role === "writer"
        ? await this.acquireWriterLease(attachment.clientId!, Date.now())
        : undefined;
    const currentResult = this.resolveBrowserAttachContext(webSocket, attachment, frame);
    if (!currentResult.ok) {
      if (lease !== undefined) this.releaseWriterLease(attachment.clientId!, lease.fence);
      return;
    }
    const current = currentResult.value;

    const { type: _type, deliveryGeneration: _deliveryGeneration, ...attachPayload } = frame;
    const activeAttachment = SocketAttachmentSchema.parse({
      ...current.controlAttachment,
      deliveryGeneration: current.client.delivery_generation,
      leaseFence: lease?.fence ?? current.controlAttachment.leaseFence,
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
      !this.sendBrowserControl(
        webSocket,
        {
          type: "welcome",
          connectionId: current.controlAttachment.connectionId,
          streamId: current.client.stream_id,
          ...(lease === undefined ? {} : { writerLease: lease.token }),
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

    const host = this.currentHostControl();
    if (host === undefined) {
      this.sendBrowserControl(webSocket, { type: "host-offline" }, "host status delivery failed");
      return;
    }
    this.sendHostControl(
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

  private resolveBrowserAttachContext(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<z.output<typeof ClientControlFrameSchema>, { type: "attach" }>,
  ): BrowserAttachContextResult {
    if (attachment.clientId === null) return { ok: false, reason: "session-missing" };
    const session = this.session();
    const client = this.clientById(attachment.clientId);
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
      this.activeBrowserControl(attachment.clientId) !== webSocket
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
    const dataSocket = this.browserDataByClient(attachment.clientId);
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
    return {
      ok: true,
      value: { client, controlAttachment, dataAttachment, dataSocket, session },
    };
  }

  private rejectBrowserAttach(
    webSocket: WebSocket,
    result: Extract<BrowserAttachContextResult, { ok: false }>,
  ): void {
    if (result.reason === "stale-control" || result.reason === "stale-delivery") return;
    if (result.reason === "already-attached") {
      const attachment = readAttachment(webSocket);
      if (attachment === undefined) closeProtocol(webSocket, "delivery is already attached");
      else this.isolateBrowserConnection(webSocket, attachment, "delivery is already attached");
      return;
    }
    if (
      result.client !== undefined &&
      (result.reason === "engine-mismatch" || result.reason === "epoch-changed")
    ) {
      this.sendBrowserControl(
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

  private acknowledgeBrowser(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<z.output<typeof ClientControlFrameSchema>, { type: "ack" }>,
  ): void {
    if (attachment.clientId === null) return;
    const session = this.session();
    const client = this.clientById(attachment.clientId);
    if (session === undefined || client === undefined) return;
    if (frame.deliveryGeneration !== client.delivery_generation) return;
    if (frame.sessionEpoch !== session.session_epoch) {
      this.sendBrowserControl(
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

    const dataSocket = this.browserDataByClient(attachment.clientId);
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

  private async handleHostData(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    encoded: Uint8Array,
  ): Promise<void> {
    if (webSocket.readyState !== WebSocket.OPEN || !this.isCurrentHost(attachment)) return;
    const connection = connectionSockets(this.ctx, attachment);
    if (connection.data !== webSocket) return;
    if (
      connection.phase !== "ready" ||
      connection.control !== this.currentHostControl() ||
      this.currentHostData() !== webSocket
    ) {
      this.failCurrentHost(attachment, "host data received before ready acknowledgement");
      return;
    }

    let frame: DataFrame;
    try {
      frame = decodeDataFrame(encoded);
    } catch {
      this.failCurrentHost(attachment, "invalid host data frame");
      return;
    }
    const session = this.session();
    if (session === undefined || frame.sessionEpoch !== BigInt(session.session_epoch)) {
      this.failCurrentHost(attachment, "host session epoch mismatch");
      return;
    }

    if (frame.kind === DataFrameKind.DeliveryBarrier) {
      this.handleDeliveryBarrier(webSocket, attachment, session, frame);
      return;
    }

    if (frame.streamId === 0) {
      if (
        frame.deliveryGeneration !== 0n ||
        this.hasPinnedDeliveryInProgress() ||
        !this.commitCanonicalFrame(session, frame)
      ) {
        this.failCurrentHost(attachment, "canonical data sequence gap");
        return;
      }
      const pendingResets: Promise<void>[] = [];
      for (const browserData of this.browserDataSockets()) {
        const browserAttachment = readAttachment(browserData);
        if (browserAttachment?.dataState === "synced") {
          const result = this.deliverToBrowser(browserData, browserAttachment, encoded, frame);
          if (result === "sequence-error") {
            if (browserAttachment.clientId !== null) {
              pendingResets.push(
                this.resetBrowserDelivery(browserAttachment.clientId, "journal-gap", false),
              );
            }
          } else if (result !== undefined) {
            pendingResets.push(result);
          }
        }
      }
      await Promise.all(pendingResets);
      return;
    }

    const client = this.clientByStream(frame.streamId);
    if (client === undefined || frame.deliveryGeneration !== BigInt(client.delivery_generation)) {
      return;
    }
    const browserData = this.browserDataByClient(client.client_id);
    const browserAttachment = browserData === undefined ? undefined : readAttachment(browserData);
    if (
      browserData !== undefined &&
      browserAttachment !== undefined &&
      frame.kind === DataFrameKind.Reset
    ) {
      this.failCurrentHost(attachment, "directed reset is not supported");
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
    if (!this.isDirectedFrameWithinPinnedCommit(browserAttachment, frame)) {
      this.failCurrentHost(attachment, "directed replay exceeds pinned commit");
      return;
    }
    const result = this.deliverToBrowser(browserData, browserAttachment, encoded, frame);
    if (result === "sequence-error") {
      this.failCurrentHost(attachment, "directed replay sequence gap");
    } else {
      await result;
    }
  }

  private isDirectedFrameWithinPinnedCommit(
    attachment: SocketAttachment,
    frame: DataFrame,
  ): boolean {
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

  private hasPinnedDeliveryInProgress(): boolean {
    return this.browserDataSockets().some((socket) => {
      const attachment = readAttachment(socket);
      return (
        attachment?.dataState === "catching-up" &&
        attachment.replayCommitEventSeq !== null &&
        attachment.replayCommitPtyOffset !== null
      );
    });
  }

  private commitCanonicalFrame(session: SessionRow, frame: DataFrame): boolean {
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
    this.sql.exec(
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

  private deliverToBrowser(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    encoded: Uint8Array,
    frame: DataFrame,
  ): Promise<void> | "sequence-error" | undefined {
    if (attachment.clientId === null || webSocket.readyState !== WebSocket.OPEN) return;
    const client = this.clientById(attachment.clientId);
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
      return this.resetBrowserDelivery(attachment.clientId, "slow-client", true);
    }

    const rewritten = rewriteDelivery(
      encoded,
      BigInt(client.delivery_generation),
      client.stream_id,
    );
    try {
      webSocket.send(rewritten);
    } catch {
      return this.resetBrowserAfterDataSendFailure(webSocket, attachment);
    }
    writeAttachment(webSocket, {
      ...attachment,
      ...cursor.nextState,
    });
  }

  private resetBrowserAfterDataSendFailure(
    webSocket: WebSocket,
    attachment: SocketAttachment,
  ): Promise<void> | undefined {
    if (attachment.clientId === null) return;
    return this.resetBrowserDelivery(attachment.clientId, "data-disconnected", false).catch(() => {
      this.isolateBrowserConnection(webSocket, attachment, "browser data delivery failed");
    });
  }

  private rejectInput(
    webSocket: WebSocket,
    inputEpoch: string,
    clientInputSeq: string,
    status: "rejected" | "uncertain" = "rejected",
  ): void {
    this.sendBrowserControl(
      webSocket,
      {
        type: "input-ack",
        inputEpoch,
        clientInputSeq,
        status,
        authorityEventSeq: this.session()?.head_event_seq ?? "0",
      },
      "input rejection delivery failed",
    );
  }

  private async resetBrowserDelivery(
    clientId: string,
    reason:
      | "journal-gap"
      | "slow-client"
      | "engine-mismatch"
      | "epoch-changed"
      | "data-disconnected"
      | "host-reconnect",
    notifyHost: boolean,
    expectedHost?: { webSocket: WebSocket; hostFence: string },
  ): Promise<void> {
    const client = this.clientById(clientId);
    if (client === undefined) return;
    const issuingControl = this.activeBrowserControl(clientId);
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
    if (BigInt(nextGeneration) > MAX_U64) {
      throw new Error("delivery generation space exhausted");
    }
    const dataTicket = randomId(32);
    const ticketDigest = await sha256Hex(dataTicket);
    const currentClient = this.clientById(clientId);
    const currentControlAttachment = readAttachment(issuingControl);
    const currentHostAttachment =
      expectedHost === undefined ? undefined : readAttachment(expectedHost.webSocket);
    if (
      issuingControl.readyState !== WebSocket.OPEN ||
      this.activeBrowserControl(clientId) !== issuingControl ||
      currentClient?.delivery_generation !== client.delivery_generation ||
      currentControlAttachment?.clientId !== clientId ||
      currentControlAttachment.controlState !== "active" ||
      currentControlAttachment.connectionId !== issuingAttachment.connectionId ||
      currentControlAttachment.connectionSetId !== issuingAttachment.connectionSetId ||
      (expectedHost !== undefined &&
        (expectedHost.webSocket.readyState !== WebSocket.OPEN ||
          currentHostAttachment?.hostFence !== expectedHost.hostFence ||
          this.currentHostControl() !== expectedHost.webSocket))
    ) {
      return;
    }

    const expiresAt = Date.now() + TICKET_LIFETIME_MS;
    if (
      !this.connections.replaceBrowserDelivery({
        clientId,
        connectionId: currentControlAttachment.connectionId,
        connectionSetId: currentControlAttachment.connectionSetId,
        currentGeneration: client.delivery_generation,
        expiresAt,
        nextGeneration,
        relayCapabilitiesJson: JSON.stringify(currentControlAttachment.relayCapabilities),
        role: currentControlAttachment.role,
        streamId: currentClient.stream_id,
        subject: currentControlAttachment.subject,
        ticketDigest,
      })
    ) {
      return;
    }

    this.closeSockets(
      (attachment) => {
        return (
          attachment.peer === "browser" &&
          attachment.channel === "data" &&
          attachment.clientId === clientId
        );
      },
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
      !this.sendBrowserControl(
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
    const host = this.currentHostControl();
    if (host !== undefined) {
      this.sendHostControl(
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

  private async acquireWriterLease(
    clientId: string,
    now: number,
  ): Promise<AcquiredLease | undefined> {
    const existing = this.writerLease();
    if (existing !== undefined && existing.expires_at > now) return undefined;
    const token = randomId(32);
    const digest = await sha256Hex(token);
    const current = this.writerLease();
    if (current !== undefined && current.expires_at > Date.now()) return undefined;
    const currentFence = BigInt(current?.fence ?? "0");
    if (currentFence >= MAX_U64) throw new Error("writer lease fence space exhausted");
    const fence = (currentFence + 1n).toString();
    this.sql.exec(
      `INSERT INTO writer_lease (singleton, client_id, lease_digest, fence, expires_at)
       VALUES (1, ?, ?, ?, ?)
       ON CONFLICT(singleton) DO UPDATE SET
         client_id = excluded.client_id,
         lease_digest = excluded.lease_digest,
         fence = excluded.fence,
         expires_at = excluded.expires_at`,
      clientId,
      digest,
      fence,
      Date.now() + WRITER_LEASE_MS,
    );
    return { fence, token };
  }

  private async renewWriterLease(
    clientId: string,
    fence: string | null,
    token: string,
  ): Promise<number | undefined> {
    if (fence === null || token.length > 256) return undefined;
    const digest = await sha256Hex(token);
    const lease = this.writerLease();
    if (
      lease === undefined ||
      lease.client_id !== clientId ||
      lease.fence !== fence ||
      lease.lease_digest !== digest ||
      lease.expires_at <= Date.now()
    ) {
      return undefined;
    }
    const expiresAt = Date.now() + WRITER_LEASE_MS;
    this.sql.exec(
      `UPDATE writer_lease SET expires_at = ?
       WHERE singleton = 1 AND client_id = ? AND fence = ? AND lease_digest = ?`,
      expiresAt,
      clientId,
      fence,
      digest,
    );
    return expiresAt;
  }

  private releaseWriterLease(clientId: string, fence: string | null): void {
    if (fence === null) return;
    this.sql.exec(
      `UPDATE writer_lease SET expires_at = 0
       WHERE singleton = 1 AND client_id = ? AND fence = ?`,
      clientId,
      fence,
    );
  }

  private session(): SessionRow | undefined {
    return this.store.session();
  }

  private clientById(clientId: string): ClientRow | undefined {
    return this.store.clientById(clientId);
  }

  private clientByStream(streamId: number): ClientRow | undefined {
    return this.store.clientByStream(streamId);
  }

  private writerLease(): WriterLeaseRow | undefined {
    return this.store.writerLease();
  }

  private ticket(ticketDigest: string): TicketRow | undefined {
    return this.store.ticket(ticketDigest);
  }

  private isCurrentHost(attachment: SocketAttachment): boolean {
    return (
      attachment.peer === "host" &&
      attachment.hostFence !== null &&
      attachment.hostFence === this.session()?.host_fence
    );
  }

  private currentHostControl(except?: WebSocket): WebSocket | undefined {
    const session = this.session();
    if (session === undefined) return undefined;
    return this.ctx.getWebSockets("peer:host").find((socket) => {
      if (socket === except || socket.readyState !== WebSocket.OPEN) return false;
      const attachment = readAttachment(socket);
      return (
        attachment?.channel === "control" &&
        attachment.hostFence === session.host_fence &&
        attachment.controlState === "active"
      );
    });
  }

  private currentHostData(except?: WebSocket): WebSocket | undefined {
    const control = this.currentHostControl();
    const controlAttachment = control === undefined ? undefined : readAttachment(control);
    if (controlAttachment === undefined) return undefined;
    return matchingSocket(this.ctx, controlAttachment, "data", except);
  }

  private activeBrowserControl(clientId: string, except?: WebSocket): WebSocket | undefined {
    return this.ctx.getWebSockets(`client:${clientId}`).find((socket) => {
      if (socket === except || socket.readyState !== WebSocket.OPEN) return false;
      return readAttachment(socket)?.channel === "control";
    });
  }

  private browserControlByConnection(connectionId: string): WebSocket | undefined {
    return this.browserControlSockets().find((socket) => {
      const attachment = readAttachment(socket);
      return attachment?.connectionId === connectionId && attachment.controlState === "active";
    });
  }

  private browserDataByClient(clientId: string, except?: WebSocket): WebSocket | undefined {
    return this.ctx.getWebSockets(`client:${clientId}`).find((socket) => {
      return (
        socket !== except &&
        socket.readyState === WebSocket.OPEN &&
        readAttachment(socket)?.channel === "data"
      );
    });
  }

  private browserControlSockets(): WebSocket[] {
    return this.ctx.getWebSockets("peer:browser").filter((socket) => {
      return socket.readyState === WebSocket.OPEN && readAttachment(socket)?.channel === "control";
    });
  }

  private browserDataSockets(): WebSocket[] {
    return this.ctx.getWebSockets("peer:browser").filter((socket) => {
      return socket.readyState === WebSocket.OPEN && readAttachment(socket)?.channel === "data";
    });
  }

  private markBrowserDataCatchingUp(): void {
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

  private sendHostControl(
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

  private sendBrowserControl(
    webSocket: WebSocket,
    frame: z.input<typeof ServerControlFrameSchema>,
    failureReason: string,
  ): boolean {
    if (trySendServerControl(webSocket, frame)) return true;
    const attachment = readAttachment(webSocket);
    if (attachment?.peer === "browser") {
      this.isolateBrowserConnection(webSocket, attachment, failureReason);
    } else {
      closeProtocol(webSocket, failureReason);
    }
    return false;
  }

  private broadcastBrowserControl(frame: z.input<typeof ServerControlFrameSchema>): void {
    for (const socket of this.browserControlSockets()) {
      this.sendBrowserControl(socket, frame, "browser control broadcast failed");
    }
  }

  private failCurrentHost(attachment: SocketAttachment, reason: string): void {
    if (!this.isCurrentHost(attachment) || attachment.hostFence === null) return;
    const fence = attachment.hostFence;
    const matchingControl = matchingSocket(this.ctx, attachment, "control");
    const wasReady =
      (attachment.channel === "control" && attachment.controlState === "active") ||
      (matchingControl !== undefined && readAttachment(matchingControl)?.controlState === "active");
    if (!this.connections.invalidateHostConnection(attachment.connectionSetId, fence)) return;
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

  private isolateBrowserConnection(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    reason: string,
  ): void {
    if (attachment.clientId === null) {
      closeProtocol(webSocket, reason);
      return;
    }
    this.releaseWriterLease(attachment.clientId, attachment.leaseFence);
    this.connections.closeConnectionSet(attachment.connectionSetId);
    this.closeSockets(
      (candidate) => {
        return (
          candidate.peer === "browser" &&
          candidate.clientId === attachment.clientId &&
          sameConnection(candidate, attachment)
        );
      },
      undefined,
      4400,
      reason,
    );
  }

  private closeSockets(
    predicate: (attachment: SocketAttachment) => boolean,
    except?: WebSocket,
    code = SOCKET_REPLACED,
    reason = "connection replaced",
  ): void {
    let releasedSnapshotPin = false;
    for (const socket of this.ctx.getWebSockets()) {
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
    if (releasedSnapshotPin) this.snapshotUploads.scheduleMaintenance();
  }

  private pinnedSnapshotIds(): ReadonlySet<string> {
    const pinned = new Set<string>();
    for (const socket of this.ctx.getWebSockets("peer:browser")) {
      if (socket.readyState !== WebSocket.OPEN) continue;
      const attachment = readAttachment(socket);
      if (attachment?.channel === "data" && attachment.snapshotId !== null) {
        pinned.add(attachment.snapshotId);
      }
    }
    return pinned;
  }
}
