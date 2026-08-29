import {
  ConnectionSetResponseSchema,
  DATA_HEADER_BYTES,
  DELIVERY_ENVELOPE_V3_HEADER_BYTES,
  DataFrameKind,
  HostCapabilityReclaimRequestSchema,
  MAX_U64,
  PositiveDecimalU64Schema,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  RelayCapabilitySchema,
  RelayToHostControlFrameSchema,
  RecoveryV3CloudToHostControlFrameSchema,
  RecoveryV3HostPrepareSchema,
  RecoveryV3ServerControlFrameSchema,
  selectRelayCapabilities,
  selectRecoveryStrategy,
  ServerControlFrameSchema,
  decodeClientControlFrame,
  decodeDataFrame,
  decodeDeliveryEnvelopeV3,
  decodeDeliveryBarrierPayload,
  decodeHostControlFrame,
  decodeRecoveryStartFence,
  decodeRelayToHostControlFrame,
  decodeServerControlFrame,
  encodeControlFrame,
  encodeDeliveryEnvelopeV3,
  rewriteDelivery,
  type AuthorityCursor,
  type ClientControlFrame,
  type ClientControlFrameV3,
  type DataFrame,
  type DeliveryBarrierPayload,
  type HostControlFrameV3,
  type RecoveryStart,
  type RecoveryStartFence,
  type RecoveryStrategy,
  type RecoveryV3ClientControlFrame,
  type RecoveryV3HostPrepare,
  type RecoveryV3HostPrepareRejected,
  type RecoveryV3HostSourceClosed,
  type RecoveryV3HostSourceReset,
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
import { advanceDeliveryCursor } from "./relay-delivery";
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
const HOST_RECOVERY_V3_CAPABILITIES = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.wireEndpointV3,
  RelayCapability.recoveryV3GapFillV1,
] as const;
const BROWSER_RECOVERY_V3_CAPABILITIES = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.wireEndpointV3,
  RelayCapability.deliveryEnvelopeV3,
  RelayCapability.deliveryReceiveCreditV1,
  RelayCapability.authorityApplyProgressV1,
  RelayCapability.recoveryV3GapFillV1,
] as const;
const CLOUD_RECOVERY_V3_CAPABILITIES = BROWSER_RECOVERY_V3_CAPABILITIES;

interface AcquiredLease {
  fence: string;
  token: string;
}

type HostControlSendResult = "sent" | "rejected" | "uncertain";

type RecoveryDataSendPlan = {
  attempt: RecoveryAttemptRow;
  browserData: WebSocket;
  encoded: Uint8Array;
  record: RecoveryDeliveryRecordIdentity;
};

type RecoveryBrowserSockets = {
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
  session: SessionRow;
}

interface RecoveryBrowserAttachContext {
  client: ClientRow;
  controlAttachment: SocketAttachment;
  dataAttachment: SocketAttachment;
  dataSocket: WebSocket;
  hostAttachment: SocketAttachment;
  hostControl: WebSocket;
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
const MAX_RECOVERY_ATTEMPTS = MAX_BROWSER_CONNECTIONS;
const MAX_RECOVERY_OUTBOX_ENTRIES = MAX_RECOVERY_ATTEMPTS * 7;
const MAX_RECOVERY_DELIVERY_RECORDS = 1_024;
const MAX_RECOVERY_DELIVERY_ENCODED_BYTES = 2 * 1024 * 1024;
const MAX_CONTROL_MESSAGE_CHARS = 6 * 1024 * 1024 + 4_096;
const MAX_HOST_DATA_BYTES = DATA_HEADER_BYTES + 16 * 1024;
const MAX_HOST_RECOVERY_DATA_BYTES = DELIVERY_ENVELOPE_V3_HEADER_BYTES + MAX_HOST_DATA_BYTES;
const RECOVERY_HARD_DEADLINE_MS = 60_000;
const RECOVERY_NO_PROGRESS_TIMEOUT_MS = 15_000;
// P2.5a keeps the P2.4 runtime stop-and-wait policy while making its scalar
// delivery ownership durable. Later scheduler slices may spend this bounded
// ledger as a wider window without changing the storage cap.
const RECOVERY_INITIAL_CUMULATIVE_GRANT = MAX_RECOVERY_DELIVERY_ENCODED_BYTES.toString();
const RECOVERY_OUTBOX_DRAIN_BATCH = 16;
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

function sameAuthorityCursor(left: AuthorityCursor, right: AuthorityCursor): boolean {
  return (
    left.sessionEpoch === right.sessionEpoch &&
    left.eventSeq === right.eventSeq &&
    left.nextPtyOffset === right.nextPtyOffset
  );
}

function selectEnabledRelayCapabilities(
  header: string | null,
  peer: "browser" | "host",
  recoveryV3Enabled: boolean,
): RelayCapability[] | undefined {
  const selected = selectRelayCapabilities(header);
  if (selected === undefined) return undefined;
  const requested = new Set(selected);
  const baseline = peer === "host" ? HOST_RELAY_CAPABILITIES : BROWSER_RELAY_CAPABILITIES;
  const recoveryFamily =
    peer === "host" ? HOST_RECOVERY_V3_CAPABILITIES : BROWSER_RECOVERY_V3_CAPABILITIES;
  const familyOffered = recoveryFamily.every((capability) => requested.has(capability));
  const enabled = new Set<RelayCapability>(
    recoveryV3Enabled && familyOffered ? [...baseline, ...recoveryFamily] : baseline,
  );
  return selected.filter((capability) => enabled.has(capability));
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
  private readonly recoveryV3Enabled: boolean;
  private recoveryOutboxDrainScheduled = false;

  constructor(ctx: DurableObjectState, env: CloudEnv) {
    super(ctx, env);
    this.recoveryV3Enabled = env.RECOVERY_V3_ENABLED === "true";
    this.sql = ctx.storage.sql;
    this.store = new RelayStore(this.sql);
    migrateRelayStore(ctx, this.sql);
    this.recoveries = new RelayRecoveryStore(this.sql, this.store, {
      maxAttempts: MAX_RECOVERY_ATTEMPTS,
      maxDeliveryEncodedBytes: MAX_RECOVERY_DELIVERY_ENCODED_BYTES,
      maxDeliveryRecords: MAX_RECOVERY_DELIVERY_RECORDS,
      maxOutboxEntries: MAX_RECOVERY_OUTBOX_ENTRIES,
    });
    this.snapshots = new SnapshotStore(ctx, this.sql, this.store, MAX_BROWSER_CONNECTIONS);
    this.alarmMux = new DurableAlarmMux(ctx, {
      [DurableAlarmComponent.snapshot]: () => this.snapshotUploads.maintain(),
      [DurableAlarmComponent.recovery]: async () => {
        const result = await this.recoveryMaintenance.maintain();
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
      await this.alarmMux.initialize();
      this.reconcileRecoveryDeliveryOwnersAfterWake();
      await this.snapshotUploads.initialize();
      await this.recoveryMaintenance.initialize();
      if (this.recoveryV3Enabled) this.scheduleRecoveryOutboxDrain();
      else this.fenceDisabledRecoveryV3();
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
    // A rollback must never reinterpret a durable v3 attachment as v2, even
    // when the inbound variant is shared by both strategy decoders.
    if (attachment.recoveryStrategy === "v3" && !this.recoveryV3Enabled) {
      this.failRecoveryBrowser(attachment, "Recovery v3 is disabled");
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
    const maximumBytes = this.isRecoveryV3Host(attachment)
      ? MAX_HOST_RECOVERY_DATA_BYTES
      : MAX_HOST_DATA_BYTES;
    if (message.byteLength > maximumBytes) {
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
    const maximumBytes = this.isRecoveryV3Host(attachment)
      ? MAX_HOST_RECOVERY_DATA_BYTES
      : MAX_HOST_DATA_BYTES;
    if (message.byteLength > maximumBytes) {
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
      if (attachment.recoveryStrategy === "v3") {
        if (this.browserDataByClient(attachment.clientId, webSocket) === undefined) {
          this.failRecoveryBrowser(attachment, "browser recovery data disconnected");
        }
        return;
      }
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
    if (attachment.recoveryStrategy === "v3") {
      if (this.activeBrowserControl(attachment.clientId, webSocket) === undefined) {
        this.failRecoveryBrowser(attachment, "browser recovery control disconnected");
      }
      return;
    }
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
      this.recoveryV3Enabled,
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
    const currentSession = this.session();
    const currentReadyHost = this.currentReadyHostAttachment();
    const hasPublishedSnapshot =
      currentSession?.latest_snapshot_id !== null &&
      currentSession?.latest_snapshot_id !== undefined &&
      this.snapshots.published(currentSession.latest_snapshot_id) !== undefined;
    const recoveryStrategy: RecoveryStrategy =
      peer === "browser" && currentSession !== undefined
        ? selectRecoveryStrategy({
            authorityDataVersion: currentSession.authority_data_version,
            browserCapabilities: selectedCapabilities,
            cloudCapabilities: CLOUD_RECOVERY_V3_CAPABILITIES,
            enabled: this.recoveryV3Enabled && hasPublishedSnapshot,
            hostCapabilities: currentReadyHost?.relayCapabilities ?? [],
          })
        : "v2";
    const recoveryHostFence =
      recoveryStrategy === "v3" ? (currentReadyHost?.hostFence ?? null) : null;
    if (recoveryStrategy === "v3" && recoveryHostFence === null) {
      throw new Error("v3 reservation lost its ready Host witness");
    }
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
      recoveryHostFence,
      recoveryStrategy,
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
        ...(reservation.recoveryStrategy === "v3" ? { recoveryStrategy: "v3" } : {}),
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
    if (JSON.stringify(relayCapabilities) !== ticket.relay_capabilities_json) {
      return json({ error: "invalid-ticket" }, 401);
    }
    if (ticket.recovery_strategy === "v3" && !this.recoveryV3Enabled) {
      if (ticket.peer === "browser" && channel === "control") {
        this.connections.discardReservation(ticket);
      }
      return json({ error: "client-reservation-unavailable" }, 409);
    }

    const readyHostFence = this.currentReadyHostAttachment()?.hostFence ?? undefined;
    const ticketClient =
      ticket.peer === "browser" && ticket.client_id !== null
        ? this.clientById(ticket.client_id)
        : undefined;
    const eligibleControl = eligibleControlForDataTicket(
      this.ctx,
      ticket,
      this.session()?.host_fence,
      readyHostFence,
      ticketClient?.recovery_strategy,
      relayCapabilities,
    );
    if (channel === "data" && eligibleControl === undefined) {
      return json({ error: "control-channel-required" }, 409);
    }
    if (ticket.peer === "browser" && channel === "control") {
      const claimedTicket = this.connections.claimBrowserControl(
        ticket,
        readyHostFence,
        this.recoveryV3Enabled,
      );
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
    const attachment =
      ticket.peer === "browser" && ticket.recovery_strategy === "v3"
        ? SocketAttachmentSchema.parse({
            version: 3,
            peer: "browser",
            channel,
            connectionSetId: ticket.connection_set_id,
            connectionId: ticket.connection_id,
            subject: ticket.subject,
            clientId: ticket.client_id,
            role: ticket.role,
            streamId: ticket.stream_id,
            deliveryGeneration: ticket.delivery_generation,
            hostFence: null,
            leaseFence: null,
            relayCapabilities,
            recoveryStrategy: "v3",
            recoveryLookupKey: null,
          })
        : SocketAttachmentSchema.parse({
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
            controlState:
              ticket.peer === "browser" && channel === "control" ? "awaiting-attach" : null,
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
    this.scheduleRecoveryOutboxDrain();
    return new Response(null, { status: 101, webSocket: client });
  }

  private async handleHostControl(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    message: string,
  ): Promise<void> {
    if (!this.isCurrentHost(attachment)) return;

    let frame: HostControlFrameV3;
    try {
      frame = this.isRecoveryV3Host(attachment)
        ? decodeHostControlFrame(message, "v3")
        : decodeHostControlFrame(message, "v2");
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
    if (attachment.recoveryStrategy === "v3" && !this.recoveryV3Enabled) {
      this.failRecoveryBrowser(attachment, "Recovery v3 is disabled");
      return;
    }
    let frame: ClientControlFrame | ClientControlFrameV3;
    try {
      frame =
        attachment.recoveryStrategy === "v3"
          ? decodeClientControlFrame(message, "v3")
          : decodeClientControlFrame(message, "v2");
    } catch {
      if (attachment.recoveryStrategy === "v3") {
        this.failRecoveryBrowser(attachment, "invalid browser recovery control frame");
      } else {
        closeProtocol(webSocket, "invalid browser control frame");
      }
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
      this.handleRecoveryBrowserProgress(webSocket, attachment, frame);
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
    if (frame.type === "ack") {
      const client = this.clientById(attachment.clientId);
      if (
        !this.isExactActiveBrowserControl(webSocket, attachment) ||
        this.currentHostControl() === undefined ||
        client?.delivery_generation !== frame.deliveryGeneration
      ) {
        return;
      }
      this.acknowledgeBrowser(webSocket, attachment, frame);
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
    frame: Extract<ClientControlFrame, { type: "attach" }>,
  ): Promise<void> {
    if (attachment.recoveryStrategy === "v3") {
      await this.attachRecoveryBrowser(webSocket, attachment, frame);
      return;
    }
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

  private async attachRecoveryBrowser(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<ClientControlFrame, { type: "attach" }>,
  ): Promise<void> {
    if (!this.recoveryV3Enabled) {
      this.failRecoveryBrowser(attachment, "Recovery v3 is disabled");
      return;
    }
    const initial = this.resolveRecoveryBrowserAttachContext(webSocket, attachment, frame);
    if (initial === undefined) {
      this.failRecoveryBrowser(attachment, "invalid Recovery v3 attach identity");
      return;
    }
    const initialActivation = initial.controlAttachment.recoveryLookupKey === null;
    const lease =
      initialActivation && attachment.role === "writer"
        ? await this.acquireRecoveryWriterLease(attachment.clientId!, Date.now())
        : undefined;
    if (initialActivation && attachment.role === "writer" && lease === undefined) {
      this.failRecoveryBrowser(attachment, "Recovery v3 writer lease is unavailable");
      return;
    }
    const current = this.resolveRecoveryBrowserAttachContext(webSocket, attachment, frame);
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
    let prepare: RecoveryV3HostPrepare;
    if (existing !== undefined) {
      try {
        prepare = RecoveryV3HostPrepareSchema.parse(JSON.parse(existing.prepare_json) as unknown);
      } catch {
        if (lease !== undefined)
          this.releaseWriterLeaseTransaction(attachment.clientId!, lease.fence);
        this.failRecoveryBrowser(attachment, "invalid durable Recovery v3 prepare");
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
        this.failRecoveryBrowser(attachment, "Recovery v3 attach retry diverged");
        return;
      }
    } else {
      let base: AuthorityCursor;
      let source: RecoveryV3HostPrepare["source"];
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
          this.failRecoveryBrowser(attachment, "Recovery v3 snapshot is unavailable");
          return;
        }
        base = {
          sessionEpoch: snapshot.sessionEpoch,
          eventSeq: snapshot.cutEventSeq,
          nextPtyOffset: snapshot.nextPtyOffset,
        };
        source = { kind: "snapshot", snapshotId: snapshot.snapshotId };
      }
      prepare = RecoveryV3HostPrepareSchema.parse({
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
      this.ctx.storage.transactionSync(() => {
        result = this.recoveries.beginPreparing({
          clientId: current.client.client_id,
          hardDeadlineAt: now + RECOVERY_HARD_DEADLINE_MS,
          hostFence: current.hostAttachment.hostFence!,
          noProgressTimeoutMs: RECOVERY_NO_PROGRESS_TIMEOUT_MS,
          now,
          prepare,
        });
      });
      if (result === undefined || !result.ok) {
        if (lease !== undefined)
          this.releaseWriterLeaseTransaction(attachment.clientId!, lease.fence);
        this.failRecoveryBrowser(attachment, "Recovery v3 prepare could not start");
        return;
      }
      this.ctx.waitUntil(this.recoveryMaintenance.refresh());
      this.snapshotUploads.scheduleMaintenance();
    }

    const activeControl = SocketAttachmentSchema.parse({
      ...current.controlAttachment,
      leaseFence: lease?.fence ?? current.controlAttachment.leaseFence,
      recoveryLookupKey: prepare.recoveryId,
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
        "Recovery v3 welcome delivery failed",
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

  private resolveRecoveryBrowserAttachContext(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<ClientControlFrame, { type: "attach" }>,
  ): RecoveryBrowserAttachContext | undefined {
    if (
      attachment.version !== 3 ||
      attachment.recoveryStrategy !== "v3" ||
      attachment.clientId === null
    ) {
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
      client.recovery_strategy !== "v3" ||
      client.delivery_generation !== attachment.deliveryGeneration ||
      client.stream_id !== attachment.streamId ||
      frame.deliveryGeneration !== client.delivery_generation ||
      frame.engineId !== session.engine_id ||
      webSocket.readyState !== WebSocket.OPEN ||
      this.activeBrowserControl(client.client_id) !== webSocket ||
      controlAttachment?.version !== 3 ||
      controlAttachment.recoveryStrategy !== "v3" ||
      controlAttachment.clientId !== client.client_id ||
      controlAttachment.connectionSetId !== attachment.connectionSetId ||
      controlAttachment.connectionId !== attachment.connectionId ||
      dataSocket === undefined ||
      dataAttachment?.version !== 3 ||
      dataAttachment.recoveryStrategy !== "v3" ||
      dataAttachment.clientId !== client.client_id ||
      dataAttachment.connectionSetId !== attachment.connectionSetId ||
      dataAttachment.connectionId !== attachment.connectionId ||
      dataAttachment.streamId !== client.stream_id ||
      dataAttachment.deliveryGeneration !== client.delivery_generation ||
      hostControl === undefined ||
      hostAttachment === undefined ||
      hostAttachment.hostFence === null ||
      !this.isRecoveryV3Host(hostAttachment)
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

  private resolveBrowserAttachContext(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: Extract<ClientControlFrame, { type: "attach" }>,
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
    frame: Extract<ClientControlFrame, { type: "ack" }>,
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

  private handleRecoveryBrowserProgress(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    frame: RecoveryV3ClientControlFrame,
  ): void {
    const attempt = this.recoveryAttemptForBrowser(
      webSocket,
      attachment,
      frame.type === "recovery-adopted" ? frame.recoveryId : undefined,
    );
    if (attempt === undefined) {
      this.failRecoveryBrowser(attachment, "Recovery v3 progress identity is stale");
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
      this.failRecoveryBrowser(attachment, "invalid Recovery v3 progress");
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
    frame: RecoveryV3HostPrepareRejected,
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
      this.failCurrentHost(hostAttachment, "Recovery v3 prepare rejection identity mismatch");
      return;
    }
    this.resetAndIsolateRecovery(
      attempt,
      frame.reason === "client-gone" ? "pair-fenced" : "generation-reset",
      "Recovery v3 source rejected",
    );
  }

  private handleRecoverySourceClosed(
    webSocket: WebSocket,
    hostAttachment: SocketAttachment,
    frame: RecoveryV3HostSourceClosed,
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
      this.failCurrentHost(hostAttachment, "Recovery v3 source closure identity mismatch");
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
      this.resetAndIsolateRecovery(
        attempt,
        "generation-reset",
        "invalid Recovery v3 source closure",
      );
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
      this.failCurrentHost(hostAttachment, "Recovery v3 start fence identity mismatch");
      return;
    }
    const browser = this.recoveryBrowserSockets(attempt);
    if (browser === undefined) {
      this.resetAndIsolateRecovery(attempt, "generation-reset", "Recovery v3 Browser is gone");
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
        authorityDataVersion: 2,
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
          "Recovery v3 snapshot changed before start",
        );
        return;
      }
      start = {
        type: "recovery-start",
        recoveryId: fence.recoveryId,
        deliveryGeneration: fence.deliveryGeneration,
        streamId: fence.streamId,
        engineId: fence.engineId,
        authorityDataVersion: 2,
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
      this.resetAndIsolateRecovery(attempt, "generation-reset", "Recovery v3 start fence rejected");
      if (result?.reason === "head-mismatch" || result?.reason === "identity-mismatch") {
        this.failCurrentHost(hostAttachment, "Recovery v3 start fence conflicts with authority");
      }
      return;
    }
    this.afterRecoveryTransition();
  }

  private handleRecoveryEnvelope(
    webSocket: WebSocket,
    hostAttachment: SocketAttachment,
    encoded: Uint8Array,
    envelope: ReturnType<typeof decodeDeliveryEnvelopeV3>,
  ): void {
    if (envelope.lane !== "recovery") {
      this.failCurrentHost(hostAttachment, "Host cannot inject a Recovery v3 live envelope");
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
      this.failCurrentHost(hostAttachment, "Recovery v3 envelope identity mismatch");
      return;
    }
    const browser = this.recoveryBrowserSockets(attempt);
    if (browser === undefined) {
      this.resetAndIsolateRecovery(attempt, "generation-reset", "Recovery v3 Browser is gone");
      return;
    }

    const now = Date.now();
    let record: RecoveryDeliveryRecordIdentity | undefined;
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
      const begun = this.recoveries.beginLaneDeliverySend(enqueued.record, now);
      if (!begun.ok) {
        this.recoveries.resetUnsafeDeliveryOutcome(attempt.recovery_id, now);
        return;
      }
      record = enqueued.record;
    });
    if (record === undefined) {
      this.isolateRecoveryAttempt(attempt, "invalid or repeated Recovery v3 envelope");
      this.afterRecoveryTransition();
      return;
    }
    this.sendRecoveryEnvelopeToBrowser({
      attempt,
      browserData: browser.data,
      encoded,
      record,
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

    if (this.isRecoveryV3Host(attachment)) {
      try {
        const fence = decodeRecoveryStartFence(encoded);
        this.handleRecoveryStartFence(webSocket, attachment, fence);
        return;
      } catch {
        // A normal canonical v2 frame and a v3 envelope are both expected to
        // fail this strict marker decoder; dispatch continues below.
      }
      try {
        const envelope = decodeDeliveryEnvelopeV3(encoded);
        this.handleRecoveryEnvelope(webSocket, attachment, encoded, envelope);
        return;
      } catch {
        // Fall through to the shared canonical v2 decoder. Unknown encodings
        // will fail there and fence the Host pair.
      }
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
      const pendingResets: Promise<void>[] = [];
      for (const browserData of this.browserDataSockets()) {
        const browserAttachment = readAttachment(browserData);
        if (
          browserAttachment?.recoveryStrategy === "v2" &&
          browserAttachment.dataState === "synced"
        ) {
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
      for (const plan of recoveryPlans) this.sendRecoveryEnvelopeToBrowser(plan);
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
  ): RecoveryDataSendPlan[] | undefined {
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
      throw new Error("durable Recovery v3 attempt bound exceeded");
    }
    const sockets = new Map<string, RecoveryBrowserSockets | undefined>();
    for (const attempt of attempts) {
      sockets.set(attempt.recovery_id, this.recoveryBrowserSockets(attempt));
    }

    const nextPtyOffset =
      frame.kind === DataFrameKind.PtyOutput
        ? frame.ptyOffset + BigInt(frame.payload.byteLength)
        : frame.ptyOffset;
    const plans: RecoveryDataSendPlan[] = [];
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
        if (
          browser === undefined ||
          live === undefined ||
          live.sent_delivery_ordinal !== live.received_delivery_ordinal ||
          live.sent_cumulative_encoded_bytes !== live.received_cumulative_encoded_bytes
        ) {
          this.recoveries.resetUndeliverableDeliveryOwner(attempt.recovery_id, now);
          isolated.push(attempt);
          continue;
        }

        let envelope: Uint8Array;
        try {
          const encodedBytes =
            BigInt(DELIVERY_ENVELOPE_V3_HEADER_BYTES) + BigInt(canonical.byteLength);
          const previousCumulative = BigInt(live.sent_cumulative_encoded_bytes);
          if (previousCumulative > MAX_U64 - encodedBytes) throw new Error("delivery overflow");
          envelope = encodeDeliveryEnvelopeV3({
            lane: "live",
            deliveryGeneration: BigInt(attempt.delivery_generation),
            deliveryOrdinal: BigInt(live.sent_delivery_ordinal) + 1n,
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
        const begun = this.recoveries.beginLaneDeliverySend(enqueued.record, now);
        if (!begun.ok) {
          this.recoveries.resetUnsafeDeliveryOutcome(attempt.recovery_id, now);
          isolated.push(attempt);
          continue;
        }
        plans.push({
          attempt,
          browserData: browser.data,
          encoded: envelope,
          record: enqueued.record,
        });
      }
    });
    if (!committed) return undefined;
    for (const attempt of isolated) {
      this.isolateRecoveryAttempt(attempt, "Recovery v3 generation reset");
    }
    if (isolated.length > 0) this.afterRecoveryTransition();
    return plans;
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

  private isRecoveryV3Host(attachment: SocketAttachment): boolean {
    if (!this.recoveryV3Enabled || attachment.peer !== "host") return false;
    const selected = new Set(attachment.relayCapabilities);
    return HOST_RECOVERY_V3_CAPABILITIES.every((capability) => selected.has(capability));
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

  private recoveryBrowserSockets(attempt: RecoveryAttemptRow): RecoveryBrowserSockets | undefined {
    const client = this.clientById(attempt.client_id);
    if (
      client === undefined ||
      client.registered_at === null ||
      client.recovery_strategy !== "v3" ||
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
        attachment?.version !== 3 ||
        attachment.recoveryStrategy !== "v3" ||
        attachment.clientId !== attempt.client_id ||
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

  private recoveryAttemptForBrowser(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    explicitRecoveryId?: string,
  ): RecoveryAttemptRow | undefined {
    if (
      attachment.version !== 3 ||
      attachment.recoveryStrategy !== "v3" ||
      attachment.clientId === null
    ) {
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
    const browser = this.recoveryBrowserSockets(attempt);
    return browser?.control === webSocket ? attempt : undefined;
  }

  private sendRecoveryEnvelopeToBrowser(plan: RecoveryDataSendPlan): void {
    const browser = this.recoveryBrowserSockets(plan.attempt);
    if (browser?.data !== plan.browserData || plan.browserData.readyState !== WebSocket.OPEN) {
      this.fenceUncertainRecoveryDelivery(
        plan.attempt,
        "Recovery v3 data socket changed before delivery",
      );
      return;
    }
    try {
      plan.browserData.send(plan.encoded);
    } catch {
      this.fenceUncertainRecoveryDelivery(
        plan.attempt,
        "Recovery v3 data send outcome is uncertain",
      );
      return;
    }

    let confirmed: ReturnType<RelayRecoveryStore["confirmLaneDeliverySend"]> | undefined;
    try {
      this.ctx.storage.transactionSync(() => {
        confirmed = this.recoveries.confirmLaneDeliverySend(plan.record, Date.now());
      });
    } catch {
      this.fenceUncertainRecoveryDelivery(
        plan.attempt,
        "Recovery v3 data send confirmation failed",
      );
      return;
    }
    if (confirmed === undefined || !confirmed.ok || !confirmed.changed) {
      this.fenceUncertainRecoveryDelivery(
        plan.attempt,
        "Recovery v3 data send confirmation is uncertain",
      );
    }
  }

  private fenceUncertainRecoveryDelivery(attempt: RecoveryAttemptRow, closeReason: string): void {
    this.ctx.storage.transactionSync(() => {
      this.recoveries.resetUnsafeDeliveryOutcome(attempt.recovery_id, Date.now());
    });
    this.isolateRecoveryAttempt(attempt, closeReason);
    this.afterRecoveryTransition();
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
      throw new Error("durable Recovery v3 attempt bound exceeded during wake reconciliation");
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
        this.recoveryBrowserSockets(attempt) === undefined
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
    });

    for (const attempt of isolate.values()) {
      this.isolateRecoveryAttempt(attempt, "Recovery v3 delivery owner fenced after wake");
    }
    if (isolate.size > 0) this.snapshotUploads.scheduleMaintenance();
  }

  private resetAndIsolateRecovery(
    attempt: RecoveryAttemptRow,
    reason: RecoveryV3HostSourceReset["reason"],
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

  private failRecoveryBrowser(attachment: SocketAttachment, reason: string): void {
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
          client?.recovery_strategy === "v3" &&
          client.role === "writer" &&
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
    if (!this.recoveryV3Enabled) return;
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
        this.isolateRecoveryAttempt(attempt, "Recovery v3 generation reset");
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
    let frame: ReturnType<typeof decodeRelayToHostControlFrame<"v3">>;
    try {
      frame = decodeRelayToHostControlFrame(entry.payload_json, "v3");
    } catch {
      this.resetAndIsolateRecovery(
        attempt,
        "generation-reset",
        "invalid durable Recovery v3 Host outbox",
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
      !RecoveryV3CloudToHostControlFrameSchema.safeParse(frame).success ||
      !this.isExactRecoveryIdentity(attempt, frame)
    ) {
      this.resetAndIsolateRecovery(
        attempt,
        "generation-reset",
        "Recovery v3 Host outbox identity diverged",
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
    if (!this.isRecoveryV3Host(hostAttachment)) {
      this.failCurrentHost(hostAttachment, "Recovery v3 Host capability was rolled back");
      return true;
    }
    if (
      host.readyState !== WebSocket.OPEN ||
      hostAttachment.hostFence !== attempt.host_fence ||
      this.currentHostControl() !== host
    ) {
      return false;
    }
    try {
      host.send(entry.payload_json);
    } catch {
      this.failCurrentHost(hostAttachment, "Recovery v3 Host control send outcome is uncertain");
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
      client.recovery_strategy !== "v3" ||
      client.delivery_generation !== attempt.delivery_generation ||
      client.stream_id !== attempt.stream_id
    ) {
      return this.acknowledgeRecoveryOutbox(entry);
    }
    const browser = this.recoveryBrowserSockets(attempt);
    if (browser === undefined) return false;
    let frame: ReturnType<typeof decodeServerControlFrame<"v3">>;
    try {
      frame = decodeServerControlFrame(entry.payload_json, "v3");
    } catch {
      this.resetAndIsolateRecovery(
        attempt,
        "generation-reset",
        "invalid durable Recovery v3 Browser outbox",
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
      !RecoveryV3ServerControlFrameSchema.safeParse(frame).success
    ) {
      this.resetAndIsolateRecovery(
        attempt,
        "generation-reset",
        "Recovery v3 Browser outbox identity diverged",
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
          "Recovery v3 Browser control send failed",
        );
      } else {
        this.resetAndIsolateRecovery(
          attempt,
          "ack-outcome-uncertain",
          "Recovery v3 Browser control send outcome is uncertain",
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

  private fenceDisabledRecoveryV3(): void {
    const attempts = this.sql
      .exec(
        `SELECT * FROM recovery_attempt
         WHERE state IN ('preparing', 'installed', 'assembling', 'complete')
         ORDER BY created_at, recovery_id LIMIT ?`,
        MAX_RECOVERY_ATTEMPTS,
      )
      .toArray() as unknown as RecoveryAttemptRow[];
    const now = Date.now();
    this.ctx.storage.transactionSync(() => {
      for (const attempt of attempts) {
        if (attempt.state === "complete") {
          this.recoveries.resetUndeliverableDeliveryOwner(attempt.recovery_id, now);
        } else {
          this.recoveries.reset(attempt.recovery_id, "generation-reset", now);
        }
      }
    });

    const isolatedConnections = new Set<string>();
    for (const attempt of attempts) {
      isolatedConnections.add(attempt.connection_id);
      this.isolateRecoveryAttempt(attempt, "Recovery v3 is disabled");
    }
    for (const socket of this.browserControlSockets()) {
      const attachment = readAttachment(socket);
      if (
        attachment?.version === 3 &&
        attachment.recoveryStrategy === "v3" &&
        !isolatedConnections.has(attachment.connectionId)
      ) {
        this.isolateBrowserConnection(socket, attachment, "Recovery v3 is disabled");
      }
    }
    this.snapshotUploads.scheduleMaintenance();
    this.ctx.waitUntil(this.recoveryMaintenance.refresh());
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
        attachment.controlState === "active"
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
      this.activeBrowserControl(attachment.clientId) !== webSocket
    ) {
      return false;
    }
    if (attachment.recoveryStrategy === "v2") return attachment.controlState === "active";
    if (
      attachment.version !== 3 ||
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
    const pair = this.recoveryBrowserSockets(attempt);
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

  private browserDataSockets(): WebSocket[] {
    return this.ctx.getWebSockets("peer:browser").filter((socket) => {
      return socket.readyState === WebSocket.OPEN && readAttachment(socket)?.channel === "data";
    });
  }

  private markBrowserDataCatchingUp(): void {
    for (const socket of this.browserDataSockets()) {
      const attachment = readAttachment(socket);
      // Recovery v3 progress is durable in recovery_lane; its strict
      // attachment deliberately has no legacy replay cursor/state fields.
      if (attachment !== undefined && attachment.recoveryStrategy === "v2") {
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
    this.ctx.storage.transactionSync(() => {
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
    for (const snapshotId of this.recoveries.pinnedSnapshotIds()) pinned.add(snapshotId);
    return pinned;
  }

  private recoveriesFenced(scope: RecoveryFenceScope): void {
    // The durable fence transition commits before this callback. Its explicit
    // scope prevents an unrelated client activation from closing a paired v3
    // socket that has not sent Attach yet.
    const stale = new Map<string, { attachment: SocketAttachment; socket: WebSocket }>();
    for (const socket of this.browserControlSockets()) {
      const attachment = readAttachment(socket);
      if (
        attachment?.version !== 3 ||
        attachment.recoveryStrategy !== "v3" ||
        attachment.clientId === null
      ) {
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
      this.isolateBrowserConnection(socket, attachment, "Recovery v3 generation fenced");
    }
    this.snapshotUploads.scheduleMaintenance();
    this.ctx.waitUntil(this.recoveryMaintenance.refresh());
    this.scheduleRecoveryOutboxDrain();
  }
}
