import {
  DATA_HEADER_BYTES,
  HostCapabilityReclaimRequestSchema,
  MAX_DATA_BATCH_BYTES,
  PositiveDecimalU64Schema,
  RelayCapability,
} from "@zhongduan/protocol";
import { DurableObject } from "cloudflare:workers";
import { z } from "zod";

import { BrowserDeliveryController } from "./browser-delivery-controller";
import { BrowserControlLane, type BrowserControlTiming } from "./browser-control-lane";
import { BrowserRelayController } from "./browser-relay-controller";
import { DeliveryBarrierController } from "./delivery-barrier-controller";
import type { CloudEnv } from "./env";
import { HostRelayController } from "./host-relay-controller";
import { RelayConnectionLifecycle } from "./relay-connection-lifecycle";
import { RelayConnectionStore } from "./relay-connection-store";
import {
  BoundedSerialQueue,
  RELAY_MESSAGE_QUEUE_PROFILES,
  type SocketQueueLimits,
} from "./relay-message-queue";
import { readSocketAttachment as readAttachment, type SocketAttachment } from "./relay-socket";
import { closeProtocol, RelaySocketRuntime } from "./relay-socket-runtime";
import { migrateRelayStore, RelayStore } from "./relay-store";
import { SnapshotStore } from "./snapshot-store";
import { SnapshotUploadCoordinator } from "./snapshot-upload-coordinator";
import { WriterAuthority } from "./writer-authority";

export { CreateConnectionSetSchema } from "./relay-connection-lifecycle";

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

const MAX_BROWSER_CONNECTIONS = 16;
const MAX_PENDING_CONNECTION_SETS = MAX_BROWSER_CONNECTIONS + 1;
const MAX_CONTROL_MESSAGE_CHARS = 6 * 1024 * 1024 + 4_096;
const MAX_HOST_SINGLE_DATA_BYTES = DATA_HEADER_BYTES + 16 * 1024;
const HOST_DATA_DISPATCH_YIELD_MS = 0;
const HOST_DATA_DISPATCH_YIELD_MIN_BYTES = DATA_HEADER_BYTES + 256;

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

/** Durable Object adapter: HTTP routing, bounded ingress scheduling, and controller composition. */
export class TerminalSessionDO extends DurableObject<CloudEnv> {
  private readonly sql: SqlStorage;
  private readonly store: RelayStore;
  private readonly snapshots: SnapshotStore;
  private readonly snapshotUploads: SnapshotUploadCoordinator;
  private readonly sockets: RelaySocketRuntime;
  private readonly lifecycle: RelayConnectionLifecycle;
  private readonly host: HostRelayController;
  private readonly browser: BrowserRelayController;
  private readonly browserControlLane: BrowserControlLane;
  private readonly hostDataInFlight = new Set<WebSocket>();
  private readonly messageQueue = new BoundedSerialQueue<WebSocket>();

  constructor(ctx: DurableObjectState, env: CloudEnv) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.store = new RelayStore(this.sql);
    migrateRelayStore(ctx, this.sql);
    const connections = new RelayConnectionStore(
      ctx,
      this.sql,
      this.store,
      MAX_BROWSER_CONNECTIONS,
      MAX_PENDING_CONNECTION_SETS,
    );
    this.snapshots = new SnapshotStore(ctx, this.sql, this.store, MAX_BROWSER_CONNECTIONS);
    this.snapshotUploads = new SnapshotUploadCoordinator(ctx, env.SNAPSHOTS, this.snapshots, () =>
      this.sockets.pinnedSnapshotIds(),
    );
    let writerAuthority!: WriterAuthority;
    this.sockets = new RelaySocketRuntime({
      connections,
      ctx,
      releaseWriter: (clientId, leaseFence, connectionId) =>
        writerAuthority.release(clientId, leaseFence, connectionId),
      scheduleSnapshotMaintenance: () => this.snapshotUploads.scheduleMaintenance(),
      store: this.store,
    });
    writerAuthority = new WriterAuthority({
      activeBrowserControl: (clientId) => this.sockets.activeBrowserControl(clientId),
      closeDisplacedWriters: (connectionId, fence) => {
        this.sockets.closeSockets(
          (candidate) =>
            candidate.peer === "browser" &&
            candidate.role === "writer" &&
            candidate.connectionId !== connectionId &&
            candidate.leaseFence !== null &&
            candidate.leaseFence !== fence,
        );
      },
      sql: this.sql,
      store: this.store,
    });
    const delivery = new BrowserDeliveryController({
      connections,
      sockets: this.sockets,
      store: this.store,
      writerAuthority,
    });
    const barriers = new DeliveryBarrierController({
      snapshotUploads: this.snapshotUploads,
      snapshots: this.snapshots,
      sockets: this.sockets,
      store: this.store,
    });
    this.host = new HostRelayController({
      barriers,
      delivery,
      sockets: this.sockets,
      sql: this.sql,
      store: this.store,
    });
    this.browserControlLane = new BrowserControlLane({
      process: (webSocket, message, timing) =>
        this.processWebSocketMessage(webSocket, message, timing),
      reject: (webSocket, attachment, reason) =>
        this.rejectInboundMessage(webSocket, attachment, reason),
    });
    this.browser = new BrowserRelayController({
      controlLane: this.browserControlLane,
      delivery,
      sockets: this.sockets,
      store: this.store,
      writerAuthority,
    });
    this.lifecycle = new RelayConnectionLifecycle({
      connections,
      ctx,
      delivery,
      onSocketClosed: (webSocket) => this.hostDataInFlight.delete(webSocket),
      snapshotUploads: this.snapshotUploads,
      sockets: this.sockets,
      writerAuthority,
    });
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
      return this.lifecycle.createConnectionSet(request);
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
        return this.lifecycle.acceptSocket(request, channel);
      }
    }
    return json({ error: "not-found" }, 404);
  }

  webSocketMessage(webSocket: WebSocket, message: ArrayBuffer | string): Promise<void> {
    const receivedAtMs = Date.now();
    const inbound = this.validateInboundMessage(webSocket, message);
    if (inbound === undefined) return Promise.resolve();
    if (inbound.attachment.peer === "browser" && inbound.attachment.channel === "control") {
      this.browserControlLane.dispatch(
        webSocket,
        inbound.attachment,
        message,
        inbound.bytes,
        inbound.queueLimits,
        receivedAtMs,
      );
      return Promise.resolve();
    }
    const usesDataCredit =
      inbound.attachment.peer === "host" &&
      inbound.attachment.channel === "data" &&
      inbound.attachment.relayCapabilities.includes(RelayCapability.hostDataBatchV1);
    const yieldsBeforeBulkData =
      usesDataCredit &&
      typeof message !== "string" &&
      message.byteLength > HOST_DATA_DISPATCH_YIELD_MIN_BYTES;
    if (usesDataCredit && this.hostDataInFlight.has(webSocket)) {
      this.rejectInboundMessage(webSocket, inbound.attachment, "host data credit exceeded");
      return Promise.resolve();
    }
    const processing = this.messageQueue.enqueue(
      webSocket,
      inbound.bytes,
      () =>
        yieldsBeforeBulkData
          ? this.processNegotiatedBulkHostDataAfterDispatch(webSocket, message)
          : Promise.resolve(this.processWebSocketMessage(webSocket, message)),
      inbound.queueLimits,
    );
    if (processing === undefined) {
      this.rejectInboundMessage(webSocket, inbound.attachment, "relay message queue exceeded");
      return Promise.resolve();
    }
    if (usesDataCredit) {
      this.hostDataInFlight.add(webSocket);
      void processing
        .catch(() => {
          this.rejectInboundMessage(webSocket, inbound.attachment, "relay message failed");
        })
        .finally(() => this.hostDataInFlight.delete(webSocket));
      return Promise.resolve();
    }
    return processing.catch(() => {
      this.rejectInboundMessage(webSocket, inbound.attachment, "relay message failed");
    });
  }

  webSocketClose(
    webSocket: WebSocket,
    _code: number,
    _reason: string,
    _wasClean: boolean,
  ): Promise<void> {
    return this.lifecycle.webSocketClose(webSocket);
  }

  webSocketError(webSocket: WebSocket, _error: unknown): Promise<void> {
    return this.lifecycle.webSocketError(webSocket);
  }

  async alarm(): Promise<void> {
    await this.snapshotUploads.maintain();
  }

  private async processNegotiatedBulkHostDataAfterDispatch(
    webSocket: WebSocket,
    message: ArrayBuffer | string,
  ): Promise<void> {
    // Yield one event turn so an already-arrived Browser control event can enter its independent
    // lane before negotiated Host bulk work resumes.
    await new Promise<void>((resolve) => setTimeout(resolve, HOST_DATA_DISPATCH_YIELD_MS));
    await this.processWebSocketMessage(webSocket, message);
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
    const maxHostDataBytes = attachment.relayCapabilities.includes(RelayCapability.hostDataBatchV1)
      ? MAX_DATA_BATCH_BYTES
      : MAX_HOST_SINGLE_DATA_BYTES;
    if (message.byteLength > maxHostDataBytes) {
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
      this.sockets.failCurrentHost(attachment, reason);
      if (webSocket.readyState < WebSocket.CLOSING) closeProtocol(webSocket, reason);
      return;
    }
    closeProtocol(webSocket, reason);
  }

  private processWebSocketMessage(
    webSocket: WebSocket,
    message: ArrayBuffer | string,
    browserTiming?: BrowserControlTiming,
  ): Promise<void> | void {
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
      return attachment.peer === "host"
        ? this.host.handleControl(webSocket, attachment, message)
        : this.browser.handle(webSocket, attachment, message, browserTiming);
    }
    if (attachment.peer !== "host") {
      closeProtocol(webSocket, "browser data channel is receive-only");
      return;
    }
    if (typeof message === "string") {
      closeProtocol(webSocket, "host data channel requires binary frames");
      return;
    }
    const maxHostDataBytes = attachment.relayCapabilities.includes(RelayCapability.hostDataBatchV1)
      ? MAX_DATA_BATCH_BYTES
      : MAX_HOST_SINGLE_DATA_BYTES;
    if (message.byteLength > maxHostDataBytes) {
      this.sockets.failCurrentHost(attachment, "host data frame is too large");
      return;
    }
    return this.host.handleData(webSocket, attachment, new Uint8Array(message));
  }

  private async initializeSession(request: Request): Promise<Response> {
    const parsed = InitializeSessionSchema.safeParse(await parseJson(request));
    if (!parsed.success) return json({ error: "invalid-session" }, 400);

    const existing = this.store.session();
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
    const session = this.store.session();
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
    const session = this.store.session();
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
}
