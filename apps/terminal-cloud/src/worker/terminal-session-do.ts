import {
  ConnectionSetResponseSchema,
  DATA_HEADER_BYTES,
  DELIVERY_ENVELOPE_HEADER_BYTES,
  DataFrameKind,
  HostCapabilityReclaimRequestSchema,
  MAX_U64,
  PositiveDecimalU64Schema,
  RelayToHostControlFrameSchema,
  RecoveryCloudToHostControlFrameSchema,
  RecoveryHostPrepareSchema,
  RecoveryServerControlFrameSchema,
  ServerControlFrameSchema,
  decodeClientControlFrame,
  decodeDataFrame,
  decodeDeliveryEnvelope,
  decodeHostControlFrame,
  decodeRecoveryStartFence,
  decodeRelayToHostControlFrame,
  decodeServerControlFrame,
  encodeControlFrame,
  encodeDeliveryEnvelope,
  type AuthorityCursor,
  type ClientControlFrame,
  type DataFrame,
  type HostControlFrame,
  type RecoveryStart,
  type RecoveryStartFence,
  type RecoveryProgressFrame,
  type RecoveryHostPrepare,
  type RecoveryHostPrepareRejected,
  type RecoveryHostSourceClosed,
  type RecoveryHostSourceReset,
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
import { RelayConnectionStore, type RecoveryFenceScope } from "./relay-connection-store";
import type { CloudEnv } from "./env";
import { DurableAlarmComponent, DurableAlarmMux } from "./durable-alarm-mux";
import {
  BoundedSerialQueue,
  RELAY_MESSAGE_QUEUE_PROFILES,
  type SocketQueueLimits,
} from "./relay-message-queue";
import { RelayRecoveryMaintenance } from "./relay-recovery-maintenance";
import {
  RelayRecoveryStore,
  type RecoveryAttemptRow,
  type RecoveryControlOutboxRow,
  type RecoveryDeliveryRecordIdentity,
} from "./relay-recovery-store";
import {
  RelayDeliveryRing,
  type RelayDeliveryGenerationIdentity,
  type RelayDeliveryRef,
  type RelayDeliveryRefIdentity,
} from "./relay-delivery-ring";
import {
  RelayDeliveryScheduler,
  type RelayDeliveryClass,
  type RelayDeliveryJobView,
  type RelayDeliverySendResult,
  type RelayDeliverySendTurn,
} from "./relay-delivery-scheduler";
import {
  readSocketAttachment as readAttachment,
  SocketAttachmentSchema,
  writeSocketAttachment as writeAttachment,
  type RelayChannel as Channel,
  type SocketAttachment,
} from "./relay-socket";
import {
  initializeRelayStore,
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
interface AcquiredLease {
  fence: string;
  token: string;
}

type HostControlSendResult = "sent" | "rejected" | "uncertain";

type RecoveryDataQueuePlan = {
  attempt: RecoveryAttemptRow;
  deliveryClass: RelayDeliveryClass;
  identity: RelayDeliveryRefIdentity;
  record: RecoveryDeliveryRecordIdentity;
};

type RecoveryDeliveryRefRecord = {
  generation: RelayDeliveryGenerationIdentity;
  record: RecoveryDeliveryRecordIdentity;
};

type BrowserSockets = {
  control: WebSocket;
  controlAttachment: SocketAttachment;
  data: WebSocket;
  dataAttachment: SocketAttachment;
};

interface BrowserAttachContext {
  client: ClientRow;
  controlAttachment: SocketAttachment;
  dataAttachment: SocketAttachment;
  dataSocket: WebSocket;
  hostAttachment: SocketAttachment;
  hostControl: WebSocket;
  session: SessionRow;
}

const TICKET_LIFETIME_MS = 30_000;
const WRITER_LEASE_MS = 30_000;
const MAX_BROWSER_CONNECTIONS = 16;
const MAX_PENDING_CONNECTION_SETS = MAX_BROWSER_CONNECTIONS + 1;
const MAX_RECOVERY_ATTEMPTS = MAX_BROWSER_CONNECTIONS;
const MAX_RECOVERY_OUTBOX_ENTRIES = MAX_RECOVERY_ATTEMPTS * 7;
const MAX_RECOVERY_DELIVERY_RECORDS = 1_024;
const MAX_RECOVERY_DELIVERY_ENCODED_BYTES = 2 * 1024 * 1024;
const MAX_RECOVERY_ATTEMPT_LIVE_RECORDS = 64;
const MAX_RECOVERY_ATTEMPT_LIVE_ENCODED_BYTES = 512 * 1024;
const MAX_RECOVERY_GRANT_WINDOW_ENCODED_BYTES = 96 * 1024;
const MAX_RECOVERY_SESSION_ENCODED_BYTES = 2 * 1024 * 1024;
const MAX_RECOVERY_SESSION_RECORDS = 1_024;
const MAX_RECOVERY_SESSION_RECOVERY_ENCODED_BYTES = 1_536 * 1024;
const MAX_RECOVERY_SESSION_RECOVERY_RECORDS = 1_008;
const MAX_CONTROL_MESSAGE_CHARS = 6 * 1024 * 1024 + 4_096;
const MAX_CANONICAL_DATA_BYTES = DATA_HEADER_BYTES + 16 * 1024;
const MAX_HOST_DATA_BYTES = DELIVERY_ENVELOPE_HEADER_BYTES + MAX_CANONICAL_DATA_BYTES;
const MIN_HOST_DATA_BYTES = DELIVERY_ENVELOPE_HEADER_BYTES + DATA_HEADER_BYTES;
const RECOVERY_WRITER_DELIVERY_RESERVE_ENCODED_BYTES = MAX_HOST_DATA_BYTES;
const RECOVERY_WRITER_DELIVERY_RESERVE_RECORDS = 1;
const RECOVERY_HARD_DEADLINE_MS = 60_000;
const RECOVERY_NO_PROGRESS_TIMEOUT_MS = 15_000;
const RECOVERY_INITIAL_CUMULATIVE_GRANT = "0";
const RECOVERY_OUTBOX_DRAIN_BATCH = 16;
const SOCKET_REPLACED = 4001;

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

function sameAuthorityCursor(left: AuthorityCursor, right: AuthorityCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.eventSeq === right.eventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}

export class TerminalSessionDO extends DurableObject<CloudEnv> {
  private readonly sql: SqlStorage;
  private readonly store: RelayStore;
  private readonly recoveries: RelayRecoveryStore;
  private readonly connections: RelayConnectionStore;
  private readonly snapshots: SnapshotStore;
  private readonly alarmMux: DurableAlarmMux;
  private readonly recoveryMaintenance: RelayRecoveryMaintenance;
  private readonly snapshotUploads: SnapshotUploadCoordinator;
  private readonly messageQueue = new BoundedSerialQueue<WebSocket>();
  private readonly recoveryDeliveryRing: RelayDeliveryRing;
  private readonly recoveryDeliveryScheduler: RelayDeliveryScheduler;
  private readonly recoveryDeliveryRefRecords = new Map<
    RelayDeliveryRef,
    RecoveryDeliveryRefRecord
  >();
  private recoveryOutboxDrainScheduled = false;

  constructor(ctx: DurableObjectState, env: CloudEnv) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.store = new RelayStore(this.sql);
    const relayStoreReset = initializeRelayStore(ctx, this.sql);
    if (relayStoreReset) {
      for (const socket of ctx.getWebSockets()) {
        if (socket.readyState < WebSocket.CLOSING) socket.close(4400, "relay storage reset");
      }
    }
    this.recoveries = new RelayRecoveryStore(this.sql, this.store, {
      maxAttempts: MAX_RECOVERY_ATTEMPTS,
      maxAttemptLiveEncodedBytes: MAX_RECOVERY_ATTEMPT_LIVE_ENCODED_BYTES,
      maxAttemptLiveRecords: MAX_RECOVERY_ATTEMPT_LIVE_RECORDS,
      maxDeliveryEncodedBytes: MAX_RECOVERY_DELIVERY_ENCODED_BYTES,
      maxDeliveryRecords: MAX_RECOVERY_DELIVERY_RECORDS,
      maxDeliveryEnvelopeEncodedBytes: MAX_HOST_DATA_BYTES,
      maxOutboxEntries: MAX_RECOVERY_OUTBOX_ENTRIES,
      maxRecoveryGrantWindowEncodedBytes: MAX_RECOVERY_GRANT_WINDOW_ENCODED_BYTES,
      maxSessionDeliveryEncodedBytes: MAX_RECOVERY_SESSION_ENCODED_BYTES,
      maxSessionDeliveryRecords: MAX_RECOVERY_SESSION_RECORDS,
      maxSessionRecoveryEncodedBytes: MAX_RECOVERY_SESSION_RECOVERY_ENCODED_BYTES,
      maxSessionRecoveryRecords: MAX_RECOVERY_SESSION_RECOVERY_RECORDS,
      minDeliveryEnvelopeEncodedBytes: MIN_HOST_DATA_BYTES,
      writerDeliveryReserveEncodedBytes: RECOVERY_WRITER_DELIVERY_RESERVE_ENCODED_BYTES,
      writerDeliveryReserveRecords: RECOVERY_WRITER_DELIVERY_RESERVE_RECORDS,
    });
    this.recoveryDeliveryRing = new RelayDeliveryRing({
      maxPhysicalBytes: MAX_RECOVERY_SESSION_ENCODED_BYTES,
      maxPhysicalEntries: MAX_RECOVERY_SESSION_RECORDS,
      maxReferences: MAX_RECOVERY_SESSION_RECORDS,
    });
    this.recoveryDeliveryScheduler = new RelayDeliveryScheduler({
      ring: this.recoveryDeliveryRing,
      yieldDataTurn: (delayMs) => this.yieldRecoveryDeliveryTurn(delayMs),
      send: (turn) => this.sendScheduledRecoveryDelivery(turn),
      onFailure: (job) => this.failScheduledRecoveryDelivery(job),
    });
    this.snapshots = new SnapshotStore(ctx, this.sql, this.store, MAX_BROWSER_CONNECTIONS);
    this.alarmMux = new DurableAlarmMux(ctx, {
      [DurableAlarmComponent.snapshot]: () => this.snapshotUploads.maintain(),
      [DurableAlarmComponent.recovery]: async () => {
        const result = await this.recoveryMaintenance.maintain();
        for (const recoveryId of result.expired) {
          const attempt = this.recoveries.attempt(recoveryId);
          if (attempt !== undefined) {
            this.isolateRecoveryAttempt(attempt, "recovery deadline expired");
          } else {
            this.cancelRecoveryDeliveriesByRecoveryId(recoveryId);
          }
        }
        for (const recoveryId of result.pruned) {
          this.cancelRecoveryDeliveriesByRecoveryId(recoveryId);
        }
        if (result.expired.length > 0) {
          this.snapshotUploads.scheduleMaintenance();
          this.scheduleRecoveryOutboxDrain();
        }
      },
    });
    this.snapshotUploads = new SnapshotUploadCoordinator(
      ctx,
      env.SNAPSHOTS,
      this.snapshots,
      () => this.pinnedSnapshotIds(),
      this.alarmMux.scheduler(DurableAlarmComponent.snapshot),
    );
    this.recoveryMaintenance = new RelayRecoveryMaintenance(
      ctx,
      this.recoveries,
      this.alarmMux.scheduler(DurableAlarmComponent.recovery),
    );
    this.connections = new RelayConnectionStore(
      ctx,
      this.sql,
      this.store,
      this.recoveries,
      (scope) => this.recoveriesFenced(scope),
      MAX_BROWSER_CONNECTIONS,
      MAX_PENDING_CONNECTION_SETS,
    );
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    void this.ctx.blockConcurrencyWhile(async () => {
      if (relayStoreReset) await this.ctx.storage.deleteAlarm();
      await this.alarmMux.initialize();
      this.reconcileRecoveryDeliveryOwnersAfterWake();
      await this.snapshotUploads.initialize();
      await this.recoveryMaintenance.initialize();
      this.scheduleRecoveryOutboxDrain();
    });
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
      async () => {
        await this.processWebSocketMessage(webSocket, message);
        this.scheduleRecoveryOutboxDrain();
      },
      inbound.queueLimits,
    );
    if (processing === undefined) {
      this.rejectInboundMessage(webSocket, inbound.attachment, "relay message queue exceeded");
      return Promise.resolve();
    }
    return processing.catch(() => {
      this.rejectInboundMessage(webSocket, inbound.attachment, "relay message failed");
      this.scheduleRecoveryOutboxDrain();
    });
  }

  async alarm(): Promise<void> {
    await this.alarmMux.alarm();
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

      if (
        attachment.clientId !== null &&
        this.browserDataByClient(attachment.clientId, webSocket) === undefined
      ) {
        this.failBrowser(attachment, "browser data disconnected");
      }
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

    if (
      attachment.clientId !== null &&
      this.activeBrowserControl(attachment.clientId, webSocket) === undefined
    ) {
      this.failBrowser(attachment, "browser control disconnected");
    }
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
    const [controlDigest, dataDigest, principalIdHash] = await Promise.all([
      sha256Hex(controlTicket),
      sha256Hex(dataTicket),
      peer === "browser" ? sha256Hex(parsed.data.subject) : Promise.resolve(undefined),
    ]);
    const now = Date.now();
    const expiresAt = now + TICKET_LIFETIME_MS;
    const currentReadyHost = this.currentReadyHostAttachment();
    const hasPublishedSnapshot =
      session.latest_snapshot_id !== null &&
      this.snapshots.published(session.latest_snapshot_id) !== undefined;
    if (peer === "browser" && (!hasPublishedSnapshot || currentReadyHost?.hostFence == null)) {
      return json({ error: "recovery-unavailable" }, 409);
    }
    const hostFenceWitness = peer === "browser" ? currentReadyHost!.hostFence : null;
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
      hostFenceWitness,
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
    const readyHostFence = this.currentReadyHostAttachment()?.hostFence ?? undefined;
    const eligibleControl = eligibleControlForDataTicket(
      this.ctx,
      ticket,
      this.session()?.host_fence,
      readyHostFence,
    );
    if (channel === "data" && eligibleControl === undefined) {
      return json({ error: "control-channel-required" }, 409);
    }
    if (ticket.peer === "browser" && channel === "control") {
      if (readyHostFence === undefined) {
        this.connections.discardReservation(ticket);
        return json({ error: "client-reservation-unavailable" }, 409);
      }
      const claimedTicket = this.connections.claimBrowserControl(ticket, readyHostFence);
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
      peer: ticket.peer,
      channel,
      connectionSetId: ticket.connection_set_id,
      connectionId: ticket.connection_id,
      subject: ticket.subject,
      clientId: ticket.client_id,
      role: ticket.role,
      streamId: ticket.stream_id,
      deliveryGeneration: ticket.delivery_generation,
      hostFence: ticket.peer === "host" ? hostFence : null,
      leaseFence: null,
      recoveryLookupKey: null,
      ready: false,
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
    this.scheduleRecoveryOutboxDrain();
    return new Response(null, { status: 101, webSocket: client });
  }

  private async handleHostControl(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    message: string,
  ): Promise<void> {
    if (!this.isCurrentHost(attachment)) return;

    let frame: HostControlFrame;
    try {
      frame = decodeHostControlFrame(message);
    } catch {
      closeProtocol(webSocket, "invalid host control frame");
      return;
    }

    if (frame.type === "recovery-prepare-rejected") {
      this.handleRecoveryPrepareRejected(webSocket, attachment, frame);
      return;
    }
    if (frame.type === "recovery-source-closed") {
      this.handleRecoverySourceClosed(webSocket, attachment, frame);
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
      this.ctx.storage.transactionSync(() => {
        this.sql.exec(
          `UPDATE session_state
           SET head_event_seq = ?, next_pty_offset = ?, updated_at = ?
           WHERE singleton = 1`,
          frame.headEventSeq,
          frame.nextPtyOffset,
          Date.now(),
        );
      });
      writeAttachment(webSocket, { ...attachment, ready: true });
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
  }

  private async handleBrowserControl(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    message: string,
  ): Promise<void> {
    let frame: ClientControlFrame;
    try {
      frame = decodeClientControlFrame(message);
    } catch {
      this.failBrowser(attachment, "invalid browser control frame");
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
      frame.type === "delivery-received" ||
      frame.type === "replica-applied" ||
      frame.type === "recovery-adopted"
    ) {
      this.handleBrowserProgress(webSocket, attachment, frame);
      return;
    }
    if (
      webSocket.readyState !== WebSocket.OPEN ||
      this.activeBrowserControl(attachment.clientId) !== webSocket
    ) {
      return;
    }
    if (frame.type === "writer-lease-renew") {
      const activeBeforeRenew = this.isExactActiveBrowserControl(webSocket, attachment);
      const expiresAt =
        activeBeforeRenew && attachment.role === "writer"
          ? await this.renewWriterLease(
              attachment.clientId,
              attachment.leaseFence,
              frame.writerLease,
            )
          : undefined;
      const latestAttachment = readAttachment(webSocket);
      const active =
        expiresAt !== undefined &&
        latestAttachment !== undefined &&
        latestAttachment.leaseFence === attachment.leaseFence &&
        this.isExactActiveBrowserControl(webSocket, latestAttachment);
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
    if (!this.isExactActiveBrowserControl(webSocket, attachment)) {
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
      latestAttachment === undefined ||
      latestAttachment.clientId !== attachment.clientId ||
      latestAttachment.leaseFence === null ||
      latestAttachment.leaseFence !== attachment.leaseFence ||
      !this.isExactActiveBrowserControl(webSocket, latestAttachment)
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
    const hostAttachment = readAttachment(host);
    const sendResult = this.sendHostControl(
      host,
      {
        ...frame,
        connectionId: attachment.connectionId,
        clientId: attachment.clientId,
        writerFence: latestAttachment.leaseFence,
      },
      "semantic input delivery failed",
      true,
    );
    if (sendResult !== "sent") {
      this.rejectInput(
        webSocket,
        frame.inputEpoch,
        frame.clientInputSeq,
        sendResult === "uncertain" ? "uncertain" : "rejected",
      );
      if (sendResult === "uncertain" && hostAttachment?.peer === "host") {
        this.failCurrentHost(hostAttachment, "semantic input delivery failed");
      }
    }
  }

  private async attachBrowser(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<ClientControlFrame, { type: "attach" }>,
  ): Promise<void> {
    const initial = this.resolveBrowserAttachContext(webSocket, attachment, frame);
    if (initial === undefined) {
      this.failBrowser(attachment, "invalid recovery attach identity");
      return;
    }
    const initialActivation = initial.controlAttachment.recoveryLookupKey === null;
    const lease =
      initialActivation && attachment.role === "writer"
        ? await this.acquireRecoveryWriterLease(attachment.clientId!, Date.now())
        : undefined;
    if (initialActivation && attachment.role === "writer" && lease === undefined) {
      this.failBrowser(attachment, "recovery writer lease is unavailable");
      return;
    }
    const current = this.resolveBrowserAttachContext(webSocket, attachment, frame);
    if (current === undefined) {
      if (lease !== undefined)
        this.releaseWriterLeaseTransaction(attachment.clientId!, lease.fence);
      return;
    }

    const existing = this.recoveries.attemptByConnectionIdentity(
      current.client.client_id,
      current.controlAttachment.connectionId,
      current.client.stream_id,
      current.client.delivery_generation,
    );
    let prepare: RecoveryHostPrepare;
    if (existing !== undefined) {
      try {
        prepare = RecoveryHostPrepareSchema.parse(JSON.parse(existing.prepare_json) as unknown);
      } catch {
        if (lease !== undefined)
          this.releaseWriterLeaseTransaction(attachment.clientId!, lease.fence);
        this.failBrowser(attachment, "invalid durable recovery prepare");
        return;
      }
      const expectedBase: AuthorityCursor | undefined = frame.hasLiveReplica
        ? {
            sessionEpoch: frame.lastSessionEpoch,
            eventSeq: frame.lastEventSeq,
            nextPtyOffset: frame.nextPtyOffset,
          }
        : prepare.source.kind === "snapshot"
          ? (() => {
              const snapshot = this.snapshots.published(prepare.source.snapshotId);
              return snapshot === undefined
                ? undefined
                : {
                    sessionEpoch: snapshot.sessionEpoch,
                    eventSeq: snapshot.cutEventSeq,
                    nextPtyOffset: snapshot.nextPtyOffset,
                  };
            })()
          : undefined;
      if (
        expectedBase === undefined ||
        !sameAuthorityCursor(expectedBase, prepare.base) ||
        frame.hasLiveReplica !== (prepare.source.kind === "warm") ||
        existing.host_fence !== current.hostAttachment.hostFence
      ) {
        if (lease !== undefined)
          this.releaseWriterLeaseTransaction(attachment.clientId!, lease.fence);
        this.failBrowser(attachment, "recovery attach retry diverged");
        return;
      }
    } else {
      let base: AuthorityCursor;
      let source: RecoveryHostPrepare["source"];
      if (frame.hasLiveReplica) {
        base = {
          sessionEpoch: frame.lastSessionEpoch,
          eventSeq: frame.lastEventSeq,
          nextPtyOffset: frame.nextPtyOffset,
        };
        source = { kind: "warm" };
      } else {
        const snapshotId = current.session.latest_snapshot_id;
        const snapshot = snapshotId === null ? undefined : this.snapshots.published(snapshotId);
        if (
          snapshot === undefined ||
          snapshot.sessionId !== current.session.session_id ||
          snapshot.sessionEpoch !== current.session.session_epoch ||
          snapshot.engineId !== current.session.engine_id
        ) {
          if (lease !== undefined)
            this.releaseWriterLeaseTransaction(attachment.clientId!, lease.fence);
          this.failBrowser(attachment, "recovery snapshot is unavailable");
          return;
        }
        base = {
          sessionEpoch: snapshot.sessionEpoch,
          eventSeq: snapshot.cutEventSeq,
          nextPtyOffset: snapshot.nextPtyOffset,
        };
        source = { kind: "snapshot", snapshotId: snapshot.snapshotId };
      }
      prepare = RecoveryHostPrepareSchema.parse({
        type: "recovery-prepare",
        recoveryId: randomId(18),
        connectionId: current.controlAttachment.connectionId,
        streamId: current.client.stream_id,
        deliveryGeneration: current.client.delivery_generation,
        engineId: current.session.engine_id,
        base,
        source,
      });
      const now = Date.now();
      let result: ReturnType<RelayRecoveryStore["beginPreparing"]> | undefined;
      let capacityVictims: RecoveryAttemptRow[] = [];
      try {
        this.ctx.storage.transactionSync(() => {
          result = this.recoveries.beginPreparing({
            clientId: current.client.client_id,
            hardDeadlineAt: now + RECOVERY_HARD_DEADLINE_MS,
            hostFence: current.hostAttachment.hostFence!,
            noProgressTimeoutMs: RECOVERY_NO_PROGRESS_TIMEOUT_MS,
            now,
            prepare,
          });
          if (result?.ok && result.changed) {
            capacityVictims = this.recoveries
              .reconcileSessionDeliveryCapacity(now)
              .flatMap((recoveryId) => {
                const victim = this.recoveries.attempt(recoveryId);
                return victim === undefined ? [] : [victim];
              });
          }
        });
      } catch {
        if (lease !== undefined)
          this.releaseWriterLeaseTransaction(attachment.clientId!, lease.fence);
        this.failBrowser(attachment, "recovery delivery capacity could not reconcile");
        return;
      }
      if (result === undefined || !result.ok) {
        if (lease !== undefined)
          this.releaseWriterLeaseTransaction(attachment.clientId!, lease.fence);
        this.failBrowser(attachment, "recovery prepare could not start");
        return;
      }
      for (const victim of capacityVictims) {
        this.isolateRecoveryAttempt(victim, "recovery delivery capacity reserved for writer");
      }
      if (capacityVictims.length > 0) this.afterRecoveryTransition();
      else {
        this.ctx.waitUntil(this.recoveryMaintenance.refresh());
        this.snapshotUploads.scheduleMaintenance();
      }
    }

    const activeControl = SocketAttachmentSchema.parse({
      ...current.controlAttachment,
      leaseFence: lease?.fence ?? current.controlAttachment.leaseFence,
      recoveryLookupKey: prepare.recoveryId,
      ready: true,
    });
    const activeData = SocketAttachmentSchema.parse({
      ...current.dataAttachment,
      recoveryLookupKey: prepare.recoveryId,
    });
    if (
      initialActivation &&
      !this.sendBrowserControl(
        webSocket,
        {
          type: "welcome",
          connectionId: activeControl.connectionId,
          streamId: current.client.stream_id,
          ...(lease === undefined ? {} : { writerLease: lease.token }),
          engineId: current.session.engine_id,
          sessionEpoch: current.session.session_epoch,
          deliveryGeneration: current.client.delivery_generation,
          headEventSeq: current.session.head_event_seq,
          nextPtyOffset: current.session.next_pty_offset,
        },
        "recovery welcome delivery failed",
      )
    ) {
      // The Welcome failed before the attachment could durably reference the
      // freshly minted fence. Release it explicitly; connection isolation can
      // only see the old null attachment fence at this crash seam.
      if (lease !== undefined) {
        this.releaseWriterLeaseTransaction(attachment.clientId!, lease.fence);
      }
      return;
    }
    // Welcome is synchronous. Only after its send returns successfully do the
    // hibernation attachments advertise an active attempt. A crash before
    // these writes leaves both keys null, so an exact attach retry rotates any
    // same-client lease and emits a fresh usable token before activation.
    writeAttachment(webSocket, activeControl);
    writeAttachment(current.dataSocket, activeData);
    this.scheduleRecoveryOutboxDrain();
  }

  private resolveBrowserAttachContext(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<ClientControlFrame, { type: "attach" }>,
  ): BrowserAttachContext | undefined {
    if (attachment.clientId === null) {
      return undefined;
    }
    const session = this.session();
    const client = this.clientById(attachment.clientId);
    const controlAttachment = readAttachment(webSocket);
    const dataSocket = this.browserDataByClient(attachment.clientId);
    const dataAttachment = dataSocket === undefined ? undefined : readAttachment(dataSocket);
    const hostControl = this.currentHostControl();
    const hostAttachment = hostControl === undefined ? undefined : readAttachment(hostControl);
    if (
      session === undefined ||
      client === undefined ||
      client.registered_at === null ||
      client.delivery_generation !== attachment.deliveryGeneration ||
      client.stream_id !== attachment.streamId ||
      frame.deliveryGeneration !== client.delivery_generation ||
      frame.engineId !== session.engine_id ||
      webSocket.readyState !== WebSocket.OPEN ||
      this.activeBrowserControl(client.client_id) !== webSocket ||
      controlAttachment?.clientId !== client.client_id ||
      controlAttachment.connectionSetId !== attachment.connectionSetId ||
      controlAttachment.connectionId !== attachment.connectionId ||
      dataSocket === undefined ||
      dataAttachment?.clientId !== client.client_id ||
      dataAttachment.connectionSetId !== attachment.connectionSetId ||
      dataAttachment.connectionId !== attachment.connectionId ||
      dataAttachment.streamId !== client.stream_id ||
      dataAttachment.deliveryGeneration !== client.delivery_generation ||
      hostControl === undefined ||
      hostAttachment === undefined ||
      hostAttachment.hostFence === null ||
      !hostAttachment.ready
    ) {
      return undefined;
    }
    const lookupKeys = new Set(
      [controlAttachment.recoveryLookupKey, dataAttachment.recoveryLookupKey].filter(
        (value): value is string => value !== null,
      ),
    );
    if (lookupKeys.size > 1) return undefined;
    if (frame.hasLiveReplica) {
      if (
        frame.lastSessionEpoch !== session.session_epoch ||
        BigInt(frame.lastEventSeq) > BigInt(session.head_event_seq) ||
        BigInt(frame.nextPtyOffset) > BigInt(session.next_pty_offset)
      ) {
        return undefined;
      }
    }
    return {
      client,
      controlAttachment,
      dataAttachment,
      dataSocket,
      hostAttachment,
      hostControl,
      session,
    };
  }

  private handleBrowserProgress(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: RecoveryProgressFrame,
  ): void {
    const attempt = this.browserAttempt(
      webSocket,
      attachment,
      frame.type === "recovery-adopted" ? frame.recoveryId : undefined,
    );
    if (attempt === undefined) {
      this.failBrowser(attachment, "recovery progress identity is stale");
      return;
    }

    const now = Date.now();
    let result: ReturnType<RelayRecoveryStore["markLaneReceived"]> | undefined;
    this.ctx.storage.transactionSync(() => {
      if (frame.type === "delivery-received") {
        result = this.recoveries.markLaneReceived(
          { receipt: frame, recoveryId: attempt.recovery_id },
          now,
        );
      } else if (frame.type === "replica-applied") {
        result = this.recoveries.markReplicaApplied(
          attempt.recovery_id,
          frame.deliveryGeneration,
          frame.authorityCursor,
          now,
        );
      } else {
        result = this.recoveries.markAdopted(frame, now);
      }
      if (result !== undefined && !result.ok && attempt.state !== "complete") {
        this.recoveries.reset(attempt.recovery_id, "generation-reset", now);
      }
    });
    if (result === undefined || !result.ok) {
      this.failBrowser(attachment, "invalid recovery progress");
      return;
    }
    if (result.changed) {
      this.ctx.waitUntil(this.recoveryMaintenance.refresh());
      this.snapshotUploads.scheduleMaintenance();
    }
    this.scheduleRecoveryOutboxDrain();
  }

  private handleRecoveryPrepareRejected(
    webSocket: WebSocket,
    hostAttachment: SocketAttachment,
    frame: RecoveryHostPrepareRejected,
  ): void {
    const attempt = this.recoveries.attemptByDeliveryIdentity(
      frame.streamId,
      frame.deliveryGeneration,
    );
    if (attempt === undefined) return;
    if (
      !this.isExactRecoveryIdentity(attempt, frame) ||
      attempt.host_fence !== hostAttachment.hostFence ||
      this.currentHostControl() !== webSocket
    ) {
      this.failCurrentHost(hostAttachment, "recovery prepare rejection identity mismatch");
      return;
    }
    this.resetAndIsolateRecovery(
      attempt,
      frame.reason === "client-gone" ? "pair-fenced" : "generation-reset",
      "recovery source rejected",
    );
  }

  private handleRecoverySourceClosed(
    webSocket: WebSocket,
    hostAttachment: SocketAttachment,
    frame: RecoveryHostSourceClosed,
  ): void {
    const attempt = this.recoveries.attemptByDeliveryIdentity(
      frame.streamId,
      frame.deliveryGeneration,
    );
    if (attempt === undefined) return;
    if (
      !this.isExactRecoveryIdentity(attempt, frame) ||
      attempt.host_fence !== hostAttachment.hostFence ||
      this.currentHostControl() !== webSocket
    ) {
      this.failCurrentHost(hostAttachment, "recovery source closure identity mismatch");
      return;
    }
    const now = Date.now();
    let result: ReturnType<RelayRecoveryStore["markSourceClosed"]> | undefined;
    this.ctx.storage.transactionSync(() => {
      result = this.recoveries.markSourceClosed(frame, now);
      if (result !== undefined && !result.ok && attempt.state !== "complete") {
        this.recoveries.reset(attempt.recovery_id, "generation-reset", now);
      }
    });
    if (result === undefined || !result.ok) {
      this.resetAndIsolateRecovery(attempt, "generation-reset", "invalid recovery source closure");
      return;
    }
    this.afterRecoveryTransition();
  }

  private handleRecoveryStartFence(
    webSocket: WebSocket,
    hostAttachment: SocketAttachment,
    fence: RecoveryStartFence,
  ): void {
    const attempt = this.recoveries.attemptByDeliveryIdentity(
      fence.streamId,
      fence.deliveryGeneration,
    );
    if (attempt === undefined) return;
    if (
      !this.isExactRecoveryIdentity(attempt, fence) ||
      attempt.engine_id !== fence.engineId ||
      attempt.host_fence !== hostAttachment.hostFence ||
      this.currentHostData() !== webSocket
    ) {
      this.failCurrentHost(hostAttachment, "recovery start fence identity mismatch");
      return;
    }
    const browser = this.browserSockets(attempt);
    if (browser === undefined) {
      this.resetAndIsolateRecovery(attempt, "generation-reset", "recovery browser is gone");
      return;
    }

    let start: RecoveryStart;
    if (fence.source.kind === "warm") {
      start = {
        type: "recovery-start",
        recoveryId: fence.recoveryId,
        deliveryGeneration: fence.deliveryGeneration,
        streamId: fence.streamId,
        engineId: fence.engineId,
        authorityDataFormat: 1,
        base: fence.base,
        source: { kind: "warm" },
        committedThrough: fence.committedThrough,
        liveFloor: fence.liveFloor,
      };
    } else {
      const snapshot = this.snapshots.published(fence.source.snapshotId);
      if (
        snapshot === undefined ||
        snapshot.engineId !== fence.engineId ||
        snapshot.sessionEpoch !== fence.base.sessionEpoch ||
        snapshot.cutEventSeq !== fence.base.eventSeq ||
        snapshot.nextPtyOffset !== fence.base.nextPtyOffset
      ) {
        this.resetAndIsolateRecovery(
          attempt,
          "generation-reset",
          "recovery snapshot changed before start",
        );
        return;
      }
      start = {
        type: "recovery-start",
        recoveryId: fence.recoveryId,
        deliveryGeneration: fence.deliveryGeneration,
        streamId: fence.streamId,
        engineId: fence.engineId,
        authorityDataFormat: 1,
        base: fence.base,
        source: {
          kind: "snapshot",
          sessionId: snapshot.sessionId,
          snapshotId: snapshot.snapshotId,
          engineId: snapshot.engineId,
          sessionEpoch: snapshot.sessionEpoch,
          cutEventSeq: snapshot.cutEventSeq,
          nextPtyOffset: snapshot.nextPtyOffset,
          compression: snapshot.compression,
          compressedLength: snapshot.compressedLength,
          uncompressedLength: snapshot.uncompressedLength,
          sha256: snapshot.sha256,
          downloadPath: `/api/v1/sessions/${snapshot.sessionId}/snapshots/${snapshot.snapshotId}`,
          restoreThrough: "finish",
        },
        committedThrough: fence.committedThrough,
        liveFloor: fence.liveFloor,
      };
    }

    const now = Date.now();
    let result: ReturnType<RelayRecoveryStore["installFence"]> | undefined;
    this.ctx.storage.transactionSync(() => {
      result = this.recoveries.installFence({
        cumulativeGrantedEncodedBytes: RECOVERY_INITIAL_CUMULATIVE_GRANT,
        fence,
        now,
        start,
      });
      if (result !== undefined && !result.ok && attempt.state !== "complete") {
        this.recoveries.reset(attempt.recovery_id, "generation-reset", now);
      }
    });
    if (result === undefined || !result.ok) {
      this.resetAndIsolateRecovery(attempt, "generation-reset", "recovery start fence rejected");
      if (result?.reason === "head-mismatch" || result?.reason === "identity-mismatch") {
        this.failCurrentHost(hostAttachment, "recovery start fence conflicts with authority");
      }
      return;
    }
    this.afterRecoveryTransition();
  }

  private handleRecoveryEnvelope(
    webSocket: WebSocket,
    hostAttachment: SocketAttachment,
    encoded: Uint8Array,
    envelope: ReturnType<typeof decodeDeliveryEnvelope>,
  ): void {
    if (envelope.lane !== "recovery") {
      this.failCurrentHost(hostAttachment, "host cannot inject a live recovery envelope");
      return;
    }
    const attempt = this.recoveries.attemptByDeliveryIdentity(
      envelope.streamId,
      envelope.deliveryGeneration.toString(),
    );
    if (attempt === undefined) return;
    if (
      attempt.host_fence !== hostAttachment.hostFence ||
      this.currentHostData() !== webSocket ||
      attempt.stream_id !== envelope.streamId ||
      attempt.delivery_generation !== envelope.deliveryGeneration.toString()
    ) {
      this.failCurrentHost(hostAttachment, "recovery envelope identity mismatch");
      return;
    }
    const browser = this.browserSockets(attempt);
    if (browser === undefined) {
      this.resetAndIsolateRecovery(attempt, "generation-reset", "recovery browser is gone");
      return;
    }

    const now = Date.now();
    let plan: RecoveryDataQueuePlan | undefined;
    this.ctx.storage.transactionSync(() => {
      const enqueued = this.recoveries.enqueueValidatedLaneDelivery(
        attempt.recovery_id,
        encoded,
        now,
      );
      if (!enqueued.ok) {
        this.recoveries.resetUndeliverableDeliveryOwner(attempt.recovery_id, now);
        return;
      }
      plan = this.recoveryDataQueuePlan(attempt, enqueued.record);
    });
    if (plan === undefined) {
      this.isolateRecoveryAttempt(attempt, "invalid or repeated recovery envelope");
      this.afterRecoveryTransition();
      return;
    }
    this.queueRecoveryEncodedAfterCommit(plan, encoded);
    // The store owns grant arithmetic and may have upserted a new exact grant
    // intent while admitting this recovery record. The runtime only schedules
    // its bounded durable outbox owner after the enclosing commit.
    this.scheduleRecoveryOutboxDrain();
  }

  private async handleHostData(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    encoded: Uint8Array,
  ): Promise<void> {
    if (webSocket.readyState !== WebSocket.OPEN || !this.isCurrentHost(attachment)) return;
    const connection = connectionSockets(this.ctx, attachment);
    if (
      connection.data !== webSocket ||
      connection.phase !== "ready" ||
      connection.control !== this.currentHostControl() ||
      this.currentHostData() !== webSocket
    ) {
      this.failCurrentHost(attachment, "host data received before ready acknowledgement");
      return;
    }

    try {
      const fence = decodeRecoveryStartFence(encoded);
      this.handleRecoveryStartFence(webSocket, attachment, fence);
      return;
    } catch {
      // Envelopes have their own magic; recovery fences and canonical data use strict frame kinds.
    }
    try {
      const envelope = decodeDeliveryEnvelope(encoded);
      this.handleRecoveryEnvelope(webSocket, attachment, encoded, envelope);
      return;
    } catch {
      // Fall through to the canonical data decoder.
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
    if (
      frame.streamId !== 0 ||
      frame.deliveryGeneration !== 0n ||
      !this.isNextCanonicalFrame(session, frame)
    ) {
      this.failCurrentHost(attachment, "canonical data sequence gap");
      return;
    }
    const recoveryPlans = this.commitCanonicalAndPlanRecovery(session, frame, encoded);
    if (recoveryPlans === undefined) {
      this.failCurrentHost(attachment, "canonical data sequence gap");
      return;
    }
    this.queueLiveCanonicalAfterCommit(recoveryPlans, encoded);
  }

  private isNextCanonicalFrame(session: SessionRow, frame: DataFrame): boolean {
    if (frame.kind !== DataFrameKind.PtyOutput && frame.kind !== DataFrameKind.ResizeApplied) {
      return false;
    }
    return !(
      frame.eventSeq !== BigInt(session.head_event_seq) + 1n ||
      frame.ptyOffset !== BigInt(session.next_pty_offset) ||
      (frame.kind === DataFrameKind.ResizeApplied && frame.payload.byteLength !== 16) ||
      (frame.kind === DataFrameKind.PtyOutput &&
        frame.ptyOffset > MAX_U64 - BigInt(frame.payload.byteLength))
    );
  }

  private commitCanonicalAndPlanRecovery(
    session: SessionRow,
    frame: DataFrame,
    canonical: Uint8Array,
  ): RecoveryDataQueuePlan[] | undefined {
    if (!this.isNextCanonicalFrame(session, frame)) return undefined;
    const attempts = this.sql
      .exec(
        `SELECT * FROM recovery_attempt
         WHERE state IN ('preparing', 'installed', 'assembling', 'complete')
         ORDER BY created_at, recovery_id LIMIT ?`,
        MAX_RECOVERY_ATTEMPTS + 1,
      )
      .toArray() as unknown as RecoveryAttemptRow[];
    if (attempts.length > MAX_RECOVERY_ATTEMPTS) {
      throw new Error("durable recovery attempt bound exceeded");
    }
    const sockets = new Map<string, BrowserSockets | undefined>();
    for (const attempt of attempts) {
      sockets.set(attempt.recovery_id, this.browserSockets(attempt));
    }

    const nextPtyOffset =
      frame.kind === DataFrameKind.PtyOutput
        ? frame.ptyOffset + BigInt(frame.payload.byteLength)
        : frame.ptyOffset;
    const plans: RecoveryDataQueuePlan[] = [];
    const isolated: RecoveryAttemptRow[] = [];
    const now = Date.now();
    let committed = false;
    this.ctx.storage.transactionSync(() => {
      const currentSession = this.session();
      if (
        currentSession === undefined ||
        currentSession.host_fence !== session.host_fence ||
        !this.isNextCanonicalFrame(currentSession, frame)
      ) {
        return;
      }
      this.sql.exec(
        `UPDATE session_state
         SET head_event_seq = ?, next_pty_offset = ?, updated_at = ?
         WHERE singleton = 1 AND host_fence = ?`,
        frame.eventSeq.toString(),
        nextPtyOffset.toString(),
        now,
        session.host_fence,
      );
      committed = true;

      for (const candidate of attempts) {
        const attempt = this.recoveries.attempt(candidate.recovery_id);
        if (
          attempt === undefined ||
          attempt.host_fence !== session.host_fence ||
          attempt.state === "preparing"
        ) {
          continue;
        }
        const browser = sockets.get(attempt.recovery_id);
        const live = this.recoveries
          .lanes(attempt.recovery_id)
          .find((lane) => lane.lane === "live");
        if (browser === undefined || live === undefined) {
          this.recoveries.resetUndeliverableDeliveryOwner(attempt.recovery_id, now);
          isolated.push(attempt);
          continue;
        }

        let envelope: Uint8Array;
        try {
          const tail = this.recoveries
            .deliveryRecords(attempt.recovery_id)
            .filter((record) => record.lane === "live")
            .at(-1);
          const encodedBytes =
            BigInt(DELIVERY_ENVELOPE_HEADER_BYTES) + BigInt(canonical.byteLength);
          const previousOrdinal = BigInt(tail?.delivery_ordinal ?? live.sent_delivery_ordinal);
          const previousCumulative = BigInt(
            tail?.cumulative_encoded_bytes ?? live.sent_cumulative_encoded_bytes,
          );
          if (previousCumulative > MAX_U64 - encodedBytes) throw new Error("delivery overflow");
          envelope = encodeDeliveryEnvelope({
            lane: "live",
            deliveryGeneration: BigInt(attempt.delivery_generation),
            deliveryOrdinal: previousOrdinal + 1n,
            cumulativeEncodedBytes: previousCumulative + encodedBytes,
            streamId: attempt.stream_id,
            payload: canonical,
          });
        } catch {
          this.recoveries.resetUndeliverableDeliveryOwner(attempt.recovery_id, now);
          isolated.push(attempt);
          continue;
        }
        const enqueued = this.recoveries.enqueueValidatedLaneDelivery(
          attempt.recovery_id,
          envelope,
          now,
        );
        if (!enqueued.ok) {
          this.recoveries.resetUndeliverableDeliveryOwner(attempt.recovery_id, now);
          isolated.push(attempt);
          continue;
        }
        plans.push(this.recoveryDataQueuePlan(attempt, enqueued.record));
      }
    });
    if (!committed) return undefined;
    for (const attempt of isolated) {
      this.isolateRecoveryAttempt(attempt, "recovery generation reset");
    }
    if (isolated.length > 0) this.afterRecoveryTransition();
    return plans;
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

  private isExactRecoveryIdentity(
    attempt: RecoveryAttemptRow,
    identity: {
      connectionId: string;
      deliveryGeneration: string;
      recoveryId: string;
      streamId: number;
    },
  ): boolean {
    return (
      attempt.recovery_id === identity.recoveryId &&
      attempt.connection_id === identity.connectionId &&
      attempt.stream_id === identity.streamId &&
      attempt.delivery_generation === identity.deliveryGeneration
    );
  }

  private browserSockets(attempt: RecoveryAttemptRow): BrowserSockets | undefined {
    const client = this.clientById(attempt.client_id);
    if (
      client === undefined ||
      client.registered_at === null ||
      client.stream_id !== attempt.stream_id ||
      client.delivery_generation !== attempt.delivery_generation
    ) {
      return undefined;
    }
    const controls: Array<{ attachment: SocketAttachment; socket: WebSocket }> = [];
    const dataSockets: Array<{ attachment: SocketAttachment; socket: WebSocket }> = [];
    for (const socket of this.ctx.getWebSockets(`client:${attempt.client_id}`)) {
      if (socket.readyState !== WebSocket.OPEN) continue;
      const attachment = readAttachment(socket);
      if (
        attachment?.clientId !== attempt.client_id ||
        attachment.connectionId !== attempt.connection_id ||
        attachment.streamId !== attempt.stream_id ||
        attachment.deliveryGeneration !== attempt.delivery_generation
      ) {
        continue;
      }
      if (attachment.channel === "control") controls.push({ attachment, socket });
      else dataSockets.push({ attachment, socket });
    }
    if (controls.length !== 1 || dataSockets.length !== 1) return undefined;
    const control = controls[0]!;
    const data = dataSockets[0]!;
    if (
      control.attachment.recoveryLookupKey !== attempt.recovery_id ||
      data.attachment.recoveryLookupKey !== attempt.recovery_id ||
      this.activeBrowserControl(attempt.client_id) !== control.socket ||
      this.browserDataByClient(attempt.client_id) !== data.socket
    ) {
      return undefined;
    }
    return {
      control: control.socket,
      controlAttachment: control.attachment,
      data: data.socket,
      dataAttachment: data.attachment,
    };
  }

  private browserAttempt(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    explicitRecoveryId?: string,
  ): RecoveryAttemptRow | undefined {
    if (attachment.clientId === null) {
      return undefined;
    }
    const recoveryId = attachment.recoveryLookupKey ?? explicitRecoveryId;
    const attempt =
      recoveryId === undefined
        ? this.recoveries.attemptByConnectionIdentity(
            attachment.clientId,
            attachment.connectionId,
            attachment.streamId,
            attachment.deliveryGeneration,
          )
        : this.recoveries.attempt(recoveryId);
    if (
      attempt === undefined ||
      (explicitRecoveryId !== undefined && attempt.recovery_id !== explicitRecoveryId) ||
      attempt.client_id !== attachment.clientId ||
      attempt.connection_id !== attachment.connectionId ||
      attempt.stream_id !== attachment.streamId ||
      attempt.delivery_generation !== attachment.deliveryGeneration
    ) {
      return undefined;
    }
    const browser = this.browserSockets(attempt);
    return browser?.control === webSocket ? attempt : undefined;
  }

  private recoveryDataQueuePlan(
    attempt: RecoveryAttemptRow,
    record: RecoveryDeliveryRecordIdentity,
  ): RecoveryDataQueuePlan {
    const writer = this.clientById(attempt.client_id)?.role === "writer";
    const deliveryClass: RelayDeliveryClass =
      record.lane === "live"
        ? writer
          ? "writer-live"
          : "observer-live"
        : writer
          ? "writer-recovery"
          : "observer-recovery";
    return {
      attempt,
      deliveryClass,
      identity: {
        recoveryId: attempt.recovery_id,
        clientId: attempt.client_id,
        connectionId: attempt.connection_id,
        streamId: attempt.stream_id,
        deliveryGeneration: attempt.delivery_generation,
        lane: record.lane,
        deliveryOrdinal: record.deliveryOrdinal,
        cumulativeEncodedBytes: record.cumulativeEncodedBytes,
      },
      record,
    };
  }

  private queueRecoveryEncodedAfterCommit(plan: RecoveryDataQueuePlan, encoded: Uint8Array): void {
    const retained = this.recoveryDeliveryRing.retainRecoveryEncoded(encoded, plan.identity);
    if (!retained.ok) {
      this.fenceQueuedRecoveryDeliveries(
        [plan],
        `delivery ring rejected recovery payload: ${retained.reason}`,
      );
      return;
    }
    if (!this.enqueueRecoveryDeliveryRef(plan, retained.ref)) {
      this.fenceQueuedRecoveryDeliveries([plan], "scheduler rejected recovery payload");
    }
  }

  private queueLiveCanonicalAfterCommit(
    plans: readonly RecoveryDataQueuePlan[],
    canonical: Uint8Array,
  ): void {
    if (plans.length === 0) return;
    const ordered = [...plans].sort((left, right) => {
      const leftWriter = left.deliveryClass === "writer-live";
      const rightWriter = right.deliveryClass === "writer-live";
      if (leftWriter !== rightWriter) return leftWriter ? -1 : 1;
      if (left.attempt.created_at !== right.attempt.created_at) {
        return left.attempt.created_at - right.attempt.created_at;
      }
      return left.attempt.recovery_id < right.attempt.recovery_id
        ? -1
        : left.attempt.recovery_id > right.attempt.recovery_id
          ? 1
          : 0;
    });
    const encodedBytes = ordered[0]!.record.encodedBytes;
    if (ordered.some((plan) => plan.record.encodedBytes !== encodedBytes)) {
      this.fenceQueuedRecoveryDeliveries(ordered, "live fanout encoded sizes diverged");
      return;
    }
    const retained = this.recoveryDeliveryRing.retainLiveCanonical(
      canonical,
      ordered.map((plan) => plan.identity),
      encodedBytes,
    );
    if (!retained.ok) {
      this.fenceQueuedRecoveryDeliveries(
        ordered,
        `delivery ring rejected live fanout: ${retained.reason}`,
      );
      return;
    }

    const rejected: RecoveryDataQueuePlan[] = [];
    for (let index = 0; index < ordered.length; index += 1) {
      const plan = ordered[index]!;
      const ref = retained.refs[index]!;
      if (!this.enqueueRecoveryDeliveryRef(plan, ref)) rejected.push(plan);
    }
    if (rejected.length > 0) {
      this.fenceQueuedRecoveryDeliveries(rejected, "scheduler rejected live fanout");
    }
  }

  private enqueueRecoveryDeliveryRef(plan: RecoveryDataQueuePlan, ref: RelayDeliveryRef): boolean {
    this.recoveryDeliveryRefRecords.set(ref, {
      generation: plan.identity,
      record: plan.record,
    });
    if (
      this.recoveryDeliveryScheduler.enqueue({
        deliveryClass: plan.deliveryClass,
        ref,
      })
    ) {
      return true;
    }
    this.recoveryDeliveryRefRecords.delete(ref);
    this.recoveryDeliveryRing.cancel(ref);
    return false;
  }

  private yieldRecoveryDeliveryTurn(delayMs: number): Promise<void> {
    return scheduler.wait(delayMs);
  }

  private fenceQueuedRecoveryDeliveries(
    plans: readonly RecoveryDataQueuePlan[],
    closeReason: string,
  ): void {
    const attempts = new Map(plans.map((plan) => [plan.attempt.recovery_id, plan.attempt]));
    const now = Date.now();
    this.ctx.storage.transactionSync(() => {
      for (const recoveryId of attempts.keys()) {
        const current = this.recoveries.attempt(recoveryId);
        if (current === undefined || current.state === "resetting") continue;
        const reset = this.recoveries.resetUnsafeDeliveryOutcome(recoveryId, now);
        if (!reset.ok && current.state !== "complete") {
          this.recoveries.reset(recoveryId, "generation-reset", now);
        }
      }
    });
    for (const attempt of attempts.values()) {
      this.isolateRecoveryAttempt(attempt, closeReason);
    }
    this.afterRecoveryTransition();
  }

  private sendScheduledRecoveryDelivery(turn: RelayDeliverySendTurn): RelayDeliverySendResult {
    const owner = this.recoveryDeliveryRefRecords.get(turn.ref);
    if (owner === undefined) return "stale";
    if (!this.sameScheduledDeliveryOwner(owner, turn)) {
      this.recoveryDeliveryRefRecords.delete(turn.ref);
      return "stale";
    }

    const attempt = this.recoveries.attempt(turn.identity.recoveryId);
    if (attempt === undefined || !this.isExactScheduledAttempt(attempt, turn.identity)) {
      this.recoveryDeliveryRefRecords.delete(turn.ref);
      return "stale";
    }
    const browser = this.browserSockets(attempt);
    if (browser === undefined || browser.data.readyState !== WebSocket.OPEN) return "fatal";

    let encoded: Uint8Array;
    try {
      encoded = this.encodedScheduledDelivery(turn);
    } catch {
      return "fatal";
    }

    let begun: ReturnType<RelayRecoveryStore["beginLaneDeliverySend"]> | undefined;
    try {
      this.ctx.storage.transactionSync(() => {
        begun = this.recoveries.beginLaneDeliverySend(owner.record, Date.now());
      });
    } catch {
      return "fatal";
    }
    if (begun === undefined || !begun.ok || !begun.changed) {
      const current = this.recoveries.attempt(turn.identity.recoveryId);
      if (current === undefined || current.state === "resetting") {
        this.recoveryDeliveryRefRecords.delete(turn.ref);
        return "stale";
      }
      return "fatal";
    }

    const exactBrowser = this.browserSockets(attempt);
    if (
      exactBrowser?.data !== browser.data ||
      browser.data.readyState !== WebSocket.OPEN ||
      !this.isExactScheduledAttempt(
        this.recoveries.attempt(turn.identity.recoveryId),
        turn.identity,
      )
    ) {
      return "fatal";
    }
    try {
      browser.data.send(encoded);
    } catch {
      return "fatal";
    }

    let confirmed: ReturnType<RelayRecoveryStore["confirmLaneDeliverySend"]> | undefined;
    try {
      this.ctx.storage.transactionSync(() => {
        confirmed = this.recoveries.confirmLaneDeliverySend(owner.record, Date.now());
      });
    } catch {
      return "fatal";
    }
    if (confirmed === undefined || !confirmed.ok || !confirmed.changed) return "fatal";
    this.recoveryDeliveryRefRecords.delete(turn.ref);
    return "sent";
  }

  private encodedScheduledDelivery(turn: RelayDeliverySendTurn): Uint8Array {
    if (turn.identity.lane === "live") {
      const encoded = encodeDeliveryEnvelope({
        lane: "live",
        deliveryGeneration: BigInt(turn.identity.deliveryGeneration),
        deliveryOrdinal: BigInt(turn.identity.deliveryOrdinal),
        cumulativeEncodedBytes: BigInt(turn.identity.cumulativeEncodedBytes),
        streamId: turn.identity.streamId,
        payload: turn.payload,
      });
      if (encoded.byteLength !== turn.encodedBytes) {
        throw new Error("scheduled live delivery size diverged");
      }
      return encoded;
    }

    if (turn.payload.byteLength !== turn.encodedBytes) {
      throw new Error("scheduled recovery delivery size diverged");
    }
    const envelope = decodeDeliveryEnvelope(turn.payload);
    if (
      envelope.lane !== "recovery" ||
      envelope.streamId !== turn.identity.streamId ||
      envelope.deliveryGeneration.toString() !== turn.identity.deliveryGeneration ||
      envelope.deliveryOrdinal.toString() !== turn.identity.deliveryOrdinal ||
      envelope.cumulativeEncodedBytes.toString() !== turn.identity.cumulativeEncodedBytes
    ) {
      throw new Error("scheduled recovery delivery identity diverged");
    }
    return turn.payload;
  }

  private sameScheduledDeliveryOwner(
    owner: RecoveryDeliveryRefRecord,
    turn: RelayDeliveryJobView,
  ): boolean {
    return (
      owner.generation.recoveryId === turn.identity.recoveryId &&
      owner.generation.clientId === turn.identity.clientId &&
      owner.generation.connectionId === turn.identity.connectionId &&
      owner.generation.streamId === turn.identity.streamId &&
      owner.generation.deliveryGeneration === turn.identity.deliveryGeneration &&
      owner.record.recoveryId === turn.identity.recoveryId &&
      owner.record.lane === turn.identity.lane &&
      owner.record.deliveryOrdinal === turn.identity.deliveryOrdinal &&
      owner.record.cumulativeEncodedBytes === turn.identity.cumulativeEncodedBytes &&
      owner.record.encodedBytes === turn.encodedBytes
    );
  }

  private isExactScheduledAttempt(
    attempt: RecoveryAttemptRow | undefined,
    identity: RelayDeliveryRefIdentity,
  ): attempt is RecoveryAttemptRow {
    return (
      attempt !== undefined &&
      attempt.state !== "resetting" &&
      attempt.recovery_id === identity.recoveryId &&
      attempt.client_id === identity.clientId &&
      attempt.connection_id === identity.connectionId &&
      attempt.stream_id === identity.streamId &&
      attempt.delivery_generation === identity.deliveryGeneration
    );
  }

  private failScheduledRecoveryDelivery(job: RelayDeliveryJobView): void {
    const owner = this.recoveryDeliveryRefRecords.get(job.ref);
    this.recoveryDeliveryRefRecords.delete(job.ref);
    const attempt = this.recoveries.attempt(job.identity.recoveryId);
    this.cancelRecoveryDeliveryGeneration(job.identity);
    if (
      owner === undefined ||
      attempt === undefined ||
      attempt.state === "resetting" ||
      !this.isExactScheduledAttempt(attempt, job.identity)
    ) {
      return;
    }
    this.ctx.storage.transactionSync(() => {
      const now = Date.now();
      const reset = this.recoveries.resetUnsafeDeliveryOutcome(attempt.recovery_id, now);
      if (!reset.ok) {
        this.recoveries.resetUndeliverableDeliveryOwner(attempt.recovery_id, now);
      }
    });
    this.isolateRecoveryAttempt(attempt, "recovery data send outcome is uncertain");
    this.afterRecoveryTransition();
  }

  private cancelRecoveryDeliveryGeneration(identity: RelayDeliveryGenerationIdentity): void {
    this.recoveryDeliveryScheduler.forgetGeneration(identity);
    for (const [ref, owner] of this.recoveryDeliveryRefRecords) {
      if (
        owner.generation.recoveryId !== identity.recoveryId ||
        owner.generation.clientId !== identity.clientId ||
        owner.generation.connectionId !== identity.connectionId ||
        owner.generation.streamId !== identity.streamId ||
        owner.generation.deliveryGeneration !== identity.deliveryGeneration
      ) {
        continue;
      }
      this.recoveryDeliveryScheduler.cancel(ref);
      this.recoveryDeliveryRing.cancel(ref);
      this.recoveryDeliveryRefRecords.delete(ref);
    }
  }

  private cancelRecoveryDeliveriesByRecoveryId(recoveryId: string): void {
    const generations = new Map<string, RelayDeliveryGenerationIdentity>();
    for (const owner of this.recoveryDeliveryRefRecords.values()) {
      if (owner.generation.recoveryId !== recoveryId) continue;
      generations.set(JSON.stringify(owner.generation), owner.generation);
    }
    for (const generation of generations.values()) {
      this.cancelRecoveryDeliveryGeneration(generation);
    }
  }

  private reconcileRecoveryDeliveryOwnersAfterWake(): void {
    const attempts = this.sql
      .exec(
        `SELECT * FROM recovery_attempt
         WHERE state IN ('preparing', 'installed', 'assembling', 'complete', 'resetting')
         ORDER BY created_at, recovery_id LIMIT ?`,
        MAX_RECOVERY_ATTEMPTS + 1,
      )
      .toArray() as unknown as RecoveryAttemptRow[];
    if (attempts.length > MAX_RECOVERY_ATTEMPTS) {
      throw new Error("durable recovery attempt bound exceeded during wake reconciliation");
    }

    const isolate = new Map<string, RecoveryAttemptRow>();
    const unsafe = new Set(this.recoveries.deliveryRecoveryIdsRequiringReset());
    const undeliverable = new Set<string>();
    for (const attempt of attempts) {
      if (attempt.state === "resetting") {
        isolate.set(attempt.recovery_id, attempt);
        continue;
      }
      if (unsafe.has(attempt.recovery_id)) continue;
      if (
        (attempt.state === "installed" ||
          attempt.state === "assembling" ||
          attempt.state === "complete") &&
        this.browserSockets(attempt) === undefined
      ) {
        undeliverable.add(attempt.recovery_id);
      }
    }

    const now = Date.now();
    this.ctx.storage.transactionSync(() => {
      for (const recoveryId of unsafe) {
        const attempt = this.recoveries.attempt(recoveryId);
        if (attempt === undefined) continue;
        this.recoveries.resetUnsafeDeliveryOutcome(recoveryId, now);
        isolate.set(recoveryId, attempt);
      }
      for (const recoveryId of undeliverable) {
        const attempt = this.recoveries.attempt(recoveryId);
        if (attempt === undefined) continue;
        this.recoveries.resetUndeliverableDeliveryOwner(recoveryId, now);
        isolate.set(recoveryId, attempt);
      }
      for (const recoveryId of this.recoveries.reconcileSessionDeliveryCapacity(now)) {
        const attempt =
          attempts.find((candidate) => candidate.recovery_id === recoveryId) ??
          this.recoveries.attempt(recoveryId);
        if (attempt !== undefined) isolate.set(recoveryId, attempt);
      }
    });

    for (const attempt of isolate.values()) {
      this.isolateRecoveryAttempt(attempt, "recovery delivery owner fenced after wake");
    }
    if (isolate.size > 0) this.snapshotUploads.scheduleMaintenance();
  }

  private resetAndIsolateRecovery(
    attempt: RecoveryAttemptRow,
    reason: RecoveryHostSourceReset["reason"],
    closeReason: string,
  ): void {
    const current = this.recoveries.attempt(attempt.recovery_id);
    if (current !== undefined && current.state !== "resetting") {
      const now = Date.now();
      this.ctx.storage.transactionSync(() => {
        if (current.state === "complete") {
          this.recoveries.resetUndeliverableDeliveryOwner(attempt.recovery_id, now);
        } else {
          this.recoveries.reset(attempt.recovery_id, reason, now);
        }
      });
    }
    this.isolateRecoveryAttempt(attempt, closeReason);
    this.afterRecoveryTransition();
  }

  private failBrowser(attachment: SocketAttachment, reason: string): void {
    if (attachment.clientId === null) {
      this.closeSockets(
        (candidate) =>
          candidate.peer === "browser" && candidate.connectionSetId === attachment.connectionSetId,
        undefined,
        4400,
        reason,
      );
      return;
    }
    const attempt =
      attachment.recoveryLookupKey === null
        ? this.recoveries.attemptByConnectionIdentity(
            attachment.clientId,
            attachment.connectionId,
            attachment.streamId,
            attachment.deliveryGeneration,
          )
        : this.recoveries.attempt(attachment.recoveryLookupKey);
    if (attempt !== undefined) {
      this.resetAndIsolateRecovery(attempt, "generation-reset", reason);
      return;
    }
    const socket = this.ctx.getWebSockets(`client:${attachment.clientId}`).find((candidate) => {
      const candidateAttachment = readAttachment(candidate);
      return candidateAttachment !== undefined && sameConnection(candidateAttachment, attachment);
    });
    if (socket !== undefined) this.isolateBrowserConnection(socket, attachment, reason);
  }

  private isolateRecoveryAttempt(attempt: RecoveryAttemptRow, reason: string): void {
    this.cancelRecoveryDeliveryGeneration({
      recoveryId: attempt.recovery_id,
      clientId: attempt.client_id,
      connectionId: attempt.connection_id,
      streamId: attempt.stream_id,
      deliveryGeneration: attempt.delivery_generation,
    });
    const exactSockets = this.ctx
      .getWebSockets(`client:${attempt.client_id}`)
      .map((socket) => ({ attachment: readAttachment(socket), socket }))
      .filter(
        (candidate): candidate is { attachment: SocketAttachment; socket: WebSocket } =>
          candidate.attachment?.peer === "browser" &&
          candidate.attachment.clientId === attempt.client_id &&
          candidate.attachment.connectionId === attempt.connection_id &&
          candidate.attachment.streamId === attempt.stream_id &&
          candidate.attachment.deliveryGeneration === attempt.delivery_generation,
      );
    const control = exactSockets.find(({ attachment }) => attachment.channel === "control");
    const connectionWitness = control ?? exactSockets[0];
    this.ctx.storage.transactionSync(() => {
      if (control !== undefined) {
        this.releaseWriterLease(attempt.client_id, control.attachment.leaseFence);
      } else {
        const client = this.clientById(attempt.client_id);
        const lease = this.writerLease();
        if (
          client?.role === "writer" &&
          client.stream_id === attempt.stream_id &&
          client.delivery_generation === attempt.delivery_generation &&
          lease?.client_id === attempt.client_id
        ) {
          this.releaseWriterLease(attempt.client_id, lease.fence);
        }
      }
      if (connectionWitness !== undefined) {
        this.connections.closeConnectionSet(connectionWitness.attachment.connectionSetId);
      }
    });
    this.closeSockets(
      (attachment) =>
        attachment.peer === "browser" &&
        attachment.clientId === attempt.client_id &&
        attachment.connectionId === attempt.connection_id &&
        attachment.streamId === attempt.stream_id &&
        attachment.deliveryGeneration === attempt.delivery_generation,
      undefined,
      4400,
      reason,
    );
  }

  private scheduleRecoveryOutboxDrain(): void {
    if (this.recoveryOutboxDrainScheduled) return;
    this.recoveryOutboxDrainScheduled = true;
    this.ctx.waitUntil(
      Promise.resolve()
        .then(() => {
          this.recoveryOutboxDrainScheduled = false;
          const progressed = this.drainRecoveryOutboxBatch();
          if (progressed && this.recoveries.outbox(1).length > 0) {
            this.scheduleRecoveryOutboxDrain();
          }
        })
        .catch(() => {
          this.recoveryOutboxDrainScheduled = false;
        }),
    );
  }

  private drainRecoveryOutboxBatch(): boolean {
    // Scan the full bounded outbox so an unreachable prefix cannot hide an
    // exact current destination, while limiting actual work done this turn.
    const entries = this.recoveries.outbox(MAX_RECOVERY_OUTBOX_ENTRIES);
    let progressed = false;
    let transitions = 0;
    for (const candidate of entries) {
      const entry = this.recoveries.outboxEntry(candidate.recovery_id, candidate.kind);
      if (entry === undefined || entry.payload_json !== candidate.payload_json) continue;
      const attempt = this.recoveries.attempt(entry.recovery_id);
      if (attempt === undefined) continue;
      if (attempt.state === "resetting") {
        this.isolateRecoveryAttempt(attempt, "recovery generation reset");
      }
      if (
        entry.destination === "host"
          ? this.drainRecoveryHostOutbox(entry, attempt)
          : this.drainRecoveryBrowserOutbox(entry, attempt)
      ) {
        progressed = true;
        transitions += 1;
        if (transitions >= RECOVERY_OUTBOX_DRAIN_BATCH) break;
      }
    }
    if (progressed) {
      this.snapshotUploads.scheduleMaintenance();
      this.ctx.waitUntil(this.recoveryMaintenance.refresh());
    }
    return progressed;
  }

  private drainRecoveryHostOutbox(
    entry: RecoveryControlOutboxRow,
    attempt: RecoveryAttemptRow,
  ): boolean {
    let frame: ReturnType<typeof decodeRelayToHostControlFrame>;
    try {
      frame = decodeRelayToHostControlFrame(entry.payload_json);
    } catch {
      this.resetAndIsolateRecovery(
        attempt,
        "generation-reset",
        "invalid durable recovery Host outbox",
      );
      return true;
    }
    if (
      (frame.type !== "recovery-prepare" &&
        frame.type !== "recovery-start-ready" &&
        frame.type !== "recovery-source-grant" &&
        frame.type !== "recovery-source-received" &&
        frame.type !== "recovery-source-reset") ||
      frame.type !== entry.kind ||
      !RecoveryCloudToHostControlFrameSchema.safeParse(frame).success ||
      !this.isExactRecoveryIdentity(attempt, frame)
    ) {
      this.resetAndIsolateRecovery(
        attempt,
        "generation-reset",
        "recovery Host outbox identity diverged",
      );
      return true;
    }

    const session = this.session();
    if (attempt.host_fence !== session?.host_fence) {
      return frame.type === "recovery-source-reset" ? this.acknowledgeRecoveryOutbox(entry) : false;
    }
    const host = this.currentHostControl();
    const hostAttachment = host === undefined ? undefined : readAttachment(host);
    if (host === undefined || hostAttachment === undefined) return false;
    if (
      host.readyState !== WebSocket.OPEN ||
      !hostAttachment.ready ||
      hostAttachment.hostFence !== attempt.host_fence ||
      this.currentHostControl() !== host
    ) {
      return false;
    }
    try {
      host.send(entry.payload_json);
    } catch {
      this.failCurrentHost(hostAttachment, "recovery Host control send outcome is uncertain");
      return true;
    }
    return this.acknowledgeRecoveryOutbox(entry);
  }

  private drainRecoveryBrowserOutbox(
    entry: RecoveryControlOutboxRow,
    attempt: RecoveryAttemptRow,
  ): boolean {
    const client = this.clientById(attempt.client_id);
    if (
      client === undefined ||
      client.delivery_generation !== attempt.delivery_generation ||
      client.stream_id !== attempt.stream_id
    ) {
      return this.acknowledgeRecoveryOutbox(entry);
    }
    const browser = this.browserSockets(attempt);
    if (browser === undefined) return false;
    let frame: ReturnType<typeof decodeServerControlFrame>;
    try {
      frame = decodeServerControlFrame(entry.payload_json);
    } catch {
      this.resetAndIsolateRecovery(
        attempt,
        "generation-reset",
        "invalid durable recovery Browser outbox",
      );
      return true;
    }
    const identityMatches =
      frame.type === "recovery-start"
        ? frame.recoveryId === attempt.recovery_id &&
          frame.deliveryGeneration === attempt.delivery_generation &&
          frame.streamId === attempt.stream_id &&
          frame.engineId === attempt.engine_id
        : frame.type === "recovery-source-closed"
          ? frame.recoveryId === attempt.recovery_id &&
            frame.deliveryGeneration === attempt.delivery_generation
          : false;
    if (
      !identityMatches ||
      frame.type !== entry.kind ||
      !RecoveryServerControlFrameSchema.safeParse(frame).success
    ) {
      this.resetAndIsolateRecovery(
        attempt,
        "generation-reset",
        "recovery Browser outbox identity diverged",
      );
      return true;
    }
    try {
      browser.control.send(entry.payload_json);
    } catch {
      if (attempt.state === "complete") {
        this.acknowledgeRecoveryOutbox(entry);
        this.resetAndIsolateRecovery(
          attempt,
          "ack-outcome-uncertain",
          "recovery Browser control send failed",
        );
      } else {
        this.resetAndIsolateRecovery(
          attempt,
          "ack-outcome-uncertain",
          "recovery Browser control send outcome is uncertain",
        );
      }
      return true;
    }
    return this.acknowledgeRecoveryOutbox(entry);
  }

  private acknowledgeRecoveryOutbox(entry: RecoveryControlOutboxRow): boolean {
    let result: ReturnType<RelayRecoveryStore["acknowledgeOutbox"]> | undefined;
    this.ctx.storage.transactionSync(() => {
      result = this.recoveries.acknowledgeOutbox(
        entry.recovery_id,
        entry.kind,
        entry.payload_json,
        Date.now(),
      );
    });
    return result?.ok === true;
  }

  private afterRecoveryTransition(): void {
    this.snapshotUploads.scheduleMaintenance();
    this.ctx.waitUntil(this.recoveryMaintenance.refresh());
    this.scheduleRecoveryOutboxDrain();
  }

  private async acquireRecoveryWriterLease(
    clientId: string,
    now: number,
  ): Promise<AcquiredLease | undefined> {
    const token = randomId(32);
    const digest = await sha256Hex(token);
    let acquired: AcquiredLease | undefined;
    this.ctx.storage.transactionSync(() => {
      const existing = this.writerLease();
      if (existing !== undefined && existing.expires_at > now && existing.client_id !== clientId) {
        return;
      }
      const currentFence = BigInt(existing?.fence ?? "0");
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
        now + WRITER_LEASE_MS,
      );
      acquired = { fence, token };
    });
    return acquired;
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
    let expiresAt: number | undefined;
    this.ctx.storage.transactionSync(() => {
      const lease = this.writerLease();
      if (
        lease === undefined ||
        lease.client_id !== clientId ||
        lease.fence !== fence ||
        lease.lease_digest !== digest ||
        lease.expires_at <= Date.now()
      ) {
        return;
      }
      expiresAt = Date.now() + WRITER_LEASE_MS;
      this.sql.exec(
        `UPDATE writer_lease SET expires_at = ?
         WHERE singleton = 1 AND client_id = ? AND fence = ? AND lease_digest = ?`,
        expiresAt,
        clientId,
        fence,
        digest,
      );
    });
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

  private releaseWriterLeaseTransaction(clientId: string, fence: string | null): void {
    this.ctx.storage.transactionSync(() => {
      this.releaseWriterLease(clientId, fence);
    });
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
        attachment.ready
      );
    });
  }

  private currentReadyHostAttachment(): SocketAttachment | undefined {
    const control = this.currentHostControl();
    if (control === undefined) return undefined;
    const attachment = readAttachment(control);
    if (attachment === undefined) return undefined;
    const connection = connectionSockets(this.ctx, attachment);
    return connection.control === control && connection.phase === "ready" ? attachment : undefined;
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

  private isExactActiveBrowserControl(webSocket: WebSocket, attachment: SocketAttachment): boolean {
    if (
      webSocket.readyState !== WebSocket.OPEN ||
      attachment.peer !== "browser" ||
      attachment.channel !== "control" ||
      attachment.clientId === null ||
      !attachment.ready ||
      this.activeBrowserControl(attachment.clientId) !== webSocket
    ) {
      return false;
    }
    if (
      attachment.recoveryLookupKey === null ||
      (attachment.role === "writer" && attachment.leaseFence === null)
    ) {
      return false;
    }
    const attempt = this.recoveries.attempt(attachment.recoveryLookupKey);
    if (
      attempt === undefined ||
      attempt.state === "resetting" ||
      attempt.client_id !== attachment.clientId ||
      attempt.connection_id !== attachment.connectionId ||
      attempt.stream_id !== attachment.streamId ||
      attempt.delivery_generation !== attachment.deliveryGeneration ||
      attempt.host_fence !== this.session()?.host_fence
    ) {
      return false;
    }
    const pair = this.browserSockets(attempt);
    return (
      pair?.control === webSocket &&
      pair.controlAttachment.recoveryLookupKey === attempt.recovery_id &&
      pair.dataAttachment.recoveryLookupKey === attempt.recovery_id
    );
  }

  private browserControlByConnection(connectionId: string): WebSocket | undefined {
    const matches = this.browserControlSockets().filter((socket) => {
      const attachment = readAttachment(socket);
      return (
        attachment?.connectionId === connectionId &&
        this.isExactActiveBrowserControl(socket, attachment)
      );
    });
    return matches.length === 1 ? matches[0] : undefined;
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

  private sendHostControl(
    webSocket: WebSocket,
    frame: z.input<typeof RelayToHostControlFrameSchema>,
    failureReason: string,
    deferUncertainFailure = false,
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
      if (!deferUncertainFailure) this.failCurrentHost(attachment, failureReason);
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
      (attachment.channel === "control" && attachment.ready) ||
      (matchingControl !== undefined && readAttachment(matchingControl)?.ready === true);
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
    const recoveryAttempt =
      attachment.recoveryLookupKey === null
        ? this.recoveries.attemptByConnectionIdentity(
            attachment.clientId,
            attachment.connectionId,
            attachment.streamId,
            attachment.deliveryGeneration,
          )
        : this.recoveries.attempt(attachment.recoveryLookupKey);
    if (recoveryAttempt !== undefined) {
      this.cancelRecoveryDeliveryGeneration({
        recoveryId: recoveryAttempt.recovery_id,
        clientId: recoveryAttempt.client_id,
        connectionId: recoveryAttempt.connection_id,
        streamId: recoveryAttempt.stream_id,
        deliveryGeneration: recoveryAttempt.delivery_generation,
      });
    }
    let recoveryReset = false;
    this.ctx.storage.transactionSync(() => {
      const currentAttempt =
        recoveryAttempt === undefined
          ? undefined
          : this.recoveries.attempt(recoveryAttempt.recovery_id);
      if (currentAttempt !== undefined && currentAttempt.state !== "resetting") {
        const reset =
          currentAttempt.state === "complete"
            ? this.recoveries.resetUndeliverableDeliveryOwner(
                currentAttempt.recovery_id,
                Date.now(),
              )
            : this.recoveries.reset(currentAttempt.recovery_id, "generation-reset", Date.now());
        recoveryReset = reset.ok && reset.changed;
      }
      this.releaseWriterLease(attachment.clientId!, attachment.leaseFence);
      this.connections.closeConnectionSet(attachment.connectionSetId);
    });
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
    if (recoveryReset) this.afterRecoveryTransition();
  }

  private closeSockets(
    predicate: (attachment: SocketAttachment) => boolean,
    except?: WebSocket,
    code = SOCKET_REPLACED,
    reason = "connection replaced",
  ): void {
    for (const socket of this.ctx.getWebSockets()) {
      if (socket === except || socket.readyState >= WebSocket.CLOSING) continue;
      const attachment = readAttachment(socket);
      if (attachment !== undefined && predicate(attachment)) {
        socket.close(code, reason);
      }
    }
  }

  private pinnedSnapshotIds(): ReadonlySet<string> {
    return this.recoveries.pinnedSnapshotIds();
  }

  private recoveriesFenced(scope: RecoveryFenceScope): void {
    // The durable fence transition commits before this callback. Its explicit
    // scope prevents an unrelated client activation from closing a paired
    // socket that has not sent Attach yet.
    const scheduledGenerations = new Map<string, RelayDeliveryGenerationIdentity>();
    for (const owner of this.recoveryDeliveryRefRecords.values()) {
      const attempt = this.recoveries.attempt(owner.generation.recoveryId);
      const fenced =
        scope.kind === "removed-client"
          ? owner.generation.clientId === scope.clientId
          : scope.kind === "client-generation"
            ? owner.generation.clientId === scope.clientId &&
              owner.generation.deliveryGeneration !== scope.currentGeneration
            : attempt === undefined ||
              attempt.state === "resetting" ||
              attempt.host_fence !== scope.currentHostFence;
      if (fenced) {
        scheduledGenerations.set(JSON.stringify(owner.generation), owner.generation);
      }
    }
    for (const generation of scheduledGenerations.values()) {
      this.cancelRecoveryDeliveryGeneration(generation);
    }

    const stale = new Map<string, { attachment: SocketAttachment; socket: WebSocket }>();
    for (const socket of this.browserControlSockets()) {
      const attachment = readAttachment(socket);
      if (attachment?.clientId === null || attachment?.peer !== "browser") {
        continue;
      }
      let fenced = false;
      if (scope.kind === "removed-client") {
        fenced = attachment.clientId === scope.clientId;
      } else if (scope.kind === "client-generation") {
        fenced =
          attachment.clientId === scope.clientId &&
          attachment.deliveryGeneration !== scope.currentGeneration;
      } else {
        const attempt = this.recoveries.attemptByConnectionIdentity(
          attachment.clientId,
          attachment.connectionId,
          attachment.streamId,
          attachment.deliveryGeneration,
        );
        // A paired pre-Attach socket has no attempt yet but still carries the
        // ticket's old Host witness semantically, so a Host fence closes it.
        fenced =
          attempt === undefined ||
          attempt.host_fence !== scope.currentHostFence ||
          attempt.state === "resetting";
      }
      if (fenced) {
        stale.set(attachment.connectionId, { attachment, socket });
      }
    }
    for (const { attachment, socket } of stale.values()) {
      this.isolateBrowserConnection(socket, attachment, "recovery generation fenced");
    }
    this.snapshotUploads.scheduleMaintenance();
    this.ctx.waitUntil(this.recoveryMaintenance.refresh());
    this.scheduleRecoveryOutboxDrain();
  }
}
