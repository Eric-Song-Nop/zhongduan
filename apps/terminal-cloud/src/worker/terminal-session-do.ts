import { DurableObject } from "cloudflare:workers";
import { z } from "zod";
import { randomId, sha256Hex, type CapabilityRole } from "./auth";
import type { CloudEnv } from "./env";

const identifier = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);
const decimalU64 = z.string().regex(/^(0|[1-9][0-9]*)$/);
const engineId = z.string().min(1).max(512);

const InitializeSessionSchema = z.strictObject({
  sessionId: identifier,
  sessionEpoch: decimalU64,
  engineId,
});

const CreateConnectionSetSchema = z.strictObject({
  sessionId: identifier,
  subject: identifier,
  role: z.enum(["host", "writer", "observer"]),
  clientId: identifier.optional(),
});

const SocketAttachmentSchema = z.strictObject({
  version: z.literal(1),
  peer: z.enum(["host", "browser"]),
  channel: z.enum(["control", "data"]),
  connectionSetId: identifier,
  connectionId: identifier,
  clientId: identifier.nullable(),
  role: z.enum(["host", "writer", "observer"]),
  streamId: z.number().int().min(0).max(0xffff_ffff),
  deliveryGeneration: decimalU64,
  hostFence: decimalU64.nullable(),
});

type SocketAttachment = z.infer<typeof SocketAttachmentSchema>;
type Channel = SocketAttachment["channel"];

interface TicketRow {
  channel: Channel;
  client_id: string | null;
  connection_id: string;
  connection_set_id: string;
  delivery_generation: string;
  expires_at: number;
  host_fence: string | null;
  peer: "host" | "browser";
  role: CapabilityRole;
  stream_id: number;
  subject: string;
  ticket_digest: string;
}

interface SessionRow {
  engine_id: string;
  host_fence: string;
  next_stream_id: number;
  session_epoch: string;
  session_id: string;
}

interface ClientRow {
  delivery_generation: string;
  principal_id_hash: string;
  role: "writer" | "observer";
  stream_id: number;
}

const TICKET_LIFETIME_MS = 30_000;
const MAX_BROWSER_CONNECTIONS = 16;

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

function readAttachment(webSocket: WebSocket): SocketAttachment | undefined {
  const parsed = SocketAttachmentSchema.safeParse(webSocket.deserializeAttachment());
  return parsed.success ? parsed.data : undefined;
}

export class TerminalSessionDO extends DurableObject<CloudEnv> {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: CloudEnv) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.migrate();
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/internal/initialize") {
      return this.initializeSession(request);
    }
    if (request.method === "POST" && url.pathname === "/internal/connection-sets") {
      return this.createConnectionSet(request);
    }
    if (request.method === "GET" && url.pathname.startsWith("/internal/ws/")) {
      const channel = url.pathname.slice("/internal/ws/".length);
      if (channel === "control" || channel === "data") {
        return this.acceptSocket(request, channel);
      }
    }
    return json({ error: "not-found" }, 404);
  }

  webSocketMessage(webSocket: WebSocket, _message: ArrayBuffer | string): void {
    const attachment = readAttachment(webSocket);
    if (attachment === undefined) {
      webSocket.close(4400, "invalid attachment");
      return;
    }
    webSocket.close(4404, "channel handler not implemented");
  }

  webSocketClose(webSocket: WebSocket): void {
    const attachment = readAttachment(webSocket);
    if (attachment?.channel === "control") {
      this.closeTaggedSockets(`set:${attachment.connectionSetId}`, webSocket);
    }
  }

  webSocketError(webSocket: WebSocket): void {
    webSocket.close(1011, "relay socket error");
  }

  private migrate(): void {
    if ((this.ctx.storage.kv.get<number>("schema-version") ?? 0) >= 1) {
      return;
    }

    this.ctx.storage.transactionSync(() => {
      this.sql.exec(`
        CREATE TABLE IF NOT EXISTS session_state (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          session_id TEXT NOT NULL UNIQUE,
          session_epoch TEXT NOT NULL,
          engine_id TEXT NOT NULL,
          host_fence TEXT NOT NULL DEFAULT '0',
          host_agent_epoch TEXT,
          latest_snapshot_id TEXT,
          next_stream_id INTEGER NOT NULL DEFAULT 1,
          terminated_at INTEGER,
          updated_at INTEGER NOT NULL
        )
      `);
      this.sql.exec(`
        CREATE TABLE IF NOT EXISTS client_delivery (
          client_id TEXT PRIMARY KEY,
          principal_id_hash TEXT NOT NULL,
          role TEXT NOT NULL CHECK (role IN ('writer', 'observer')),
          stream_id INTEGER NOT NULL UNIQUE,
          delivery_generation TEXT NOT NULL,
          last_ack_event_seq TEXT,
          next_ack_pty_offset TEXT,
          updated_at INTEGER NOT NULL
        )
      `);
      this.sql.exec(`
        CREATE TABLE IF NOT EXISTS writer_lease (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          client_id TEXT NOT NULL,
          lease_digest TEXT NOT NULL,
          fence TEXT NOT NULL,
          expires_at INTEGER NOT NULL
        )
      `);
      this.sql.exec(`
        CREATE TABLE IF NOT EXISTS snapshot (
          snapshot_id TEXT PRIMARY KEY,
          session_epoch TEXT NOT NULL,
          cut_event_seq TEXT NOT NULL,
          next_pty_offset TEXT NOT NULL,
          engine_id TEXT NOT NULL,
          object_key TEXT NOT NULL UNIQUE,
          r2_version TEXT NOT NULL,
          etag TEXT NOT NULL,
          sha256 TEXT NOT NULL,
          compressed_length INTEGER NOT NULL,
          uncompressed_length TEXT NOT NULL,
          compression TEXT NOT NULL CHECK (compression IN ('none', 'zstd')),
          state TEXT NOT NULL CHECK (state IN ('servable', 'retired')),
          created_at INTEGER NOT NULL
        )
      `);
      this.sql.exec(`
        CREATE TABLE IF NOT EXISTS connection_ticket (
          ticket_digest TEXT PRIMARY KEY,
          connection_set_id TEXT NOT NULL,
          connection_id TEXT NOT NULL,
          peer TEXT NOT NULL CHECK (peer IN ('host', 'browser')),
          channel TEXT NOT NULL CHECK (channel IN ('control', 'data')),
          client_id TEXT,
          subject TEXT NOT NULL,
          role TEXT NOT NULL CHECK (role IN ('host', 'writer', 'observer')),
          stream_id INTEGER NOT NULL,
          delivery_generation TEXT NOT NULL,
          host_fence TEXT,
          expires_at INTEGER NOT NULL
        )
      `);
      this.sql.exec(
        "CREATE INDEX IF NOT EXISTS connection_ticket_expiry ON connection_ticket(expires_at)",
      );
      this.ctx.storage.kv.put("schema-version", 1);
    });
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
        (singleton, session_id, session_epoch, engine_id, updated_at)
       VALUES (1, ?, ?, ?, ?)`,
      parsed.data.sessionId,
      parsed.data.sessionEpoch,
      parsed.data.engineId,
      Date.now(),
    );
    return json({ created: true }, 201);
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
    if (peer === "browser") {
      const activeClients = new Set(
        this.ctx.getWebSockets("peer:browser").flatMap((socket) => {
          if (socket.readyState !== WebSocket.OPEN) return [];
          const attachment = readAttachment(socket);
          return attachment?.channel === "control" && attachment.clientId !== null
            ? [attachment.clientId]
            : [];
        }),
      );
      const replacingActiveClient =
        parsed.data.clientId !== undefined && activeClients.has(parsed.data.clientId);
      if (activeClients.size >= MAX_BROWSER_CONNECTIONS && !replacingActiveClient) {
        return json({ error: "too-many-clients" }, 429);
      }
    }

    const browserRole =
      parsed.data.role === "writer" || parsed.data.role === "observer"
        ? parsed.data.role
        : undefined;
    const client =
      peer === "browser" && browserRole !== undefined
        ? await this.resolveClient(
            parsed.data.clientId ?? randomId(),
            parsed.data.subject,
            browserRole,
          )
        : undefined;
    if (client === undefined && peer === "browser") {
      return json({ error: "client-owner-mismatch" }, 403);
    }

    const connectionSetId = randomId();
    const connectionId = connectionSetId;
    const controlTicket = randomId(32);
    const dataTicket = randomId(32);
    const [controlDigest, dataDigest] = await Promise.all([
      sha256Hex(controlTicket),
      sha256Hex(dataTicket),
    ]);
    const expiresAt = Date.now() + TICKET_LIFETIME_MS;
    const streamId = client?.stream_id ?? 0;
    const deliveryGeneration = client?.delivery_generation ?? "0";
    const clientId = peer === "browser" ? (parsed.data.clientId ?? client?.client_id) : null;

    this.ctx.storage.transactionSync(() => {
      this.sql.exec("DELETE FROM connection_ticket WHERE expires_at <= ?", Date.now());
      for (const [ticketDigest, channel] of [
        [controlDigest, "control"],
        [dataDigest, "data"],
      ] as const) {
        this.sql.exec(
          `INSERT INTO connection_ticket
            (ticket_digest, connection_set_id, connection_id, peer, channel,
             client_id, subject, role, stream_id, delivery_generation, expires_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          ticketDigest,
          connectionSetId,
          connectionId,
          peer,
          channel,
          clientId,
          parsed.data.subject,
          parsed.data.role,
          streamId,
          deliveryGeneration,
          expiresAt,
        );
      }
    });

    return json({
      connectionSetId,
      connectionId,
      clientId,
      streamId,
      deliveryGeneration,
      expiresAt,
      controlTicket,
      dataTicket,
    });
  }

  private async resolveClient(
    clientId: string,
    subject: string,
    role: "writer" | "observer",
  ): Promise<(ClientRow & { client_id: string }) | undefined> {
    const principalIdHash = await sha256Hex(subject);
    const existing = this.sql
      .exec("SELECT * FROM client_delivery WHERE client_id = ?", clientId)
      .toArray()[0] as (ClientRow & { client_id: string }) | undefined;

    if (existing !== undefined) {
      return existing.principal_id_hash === principalIdHash && existing.role === role
        ? existing
        : undefined;
    }

    const session = this.session();
    if (session === undefined) {
      throw new Error("session disappeared while resolving client");
    }
    const streamId = session.next_stream_id;
    if (streamId < 1 || streamId > 0xffff_ffff) {
      throw new Error("stream id space exhausted");
    }
    const now = Date.now();
    this.ctx.storage.transactionSync(() => {
      this.sql.exec(
        `INSERT INTO client_delivery
          (client_id, principal_id_hash, role, stream_id, delivery_generation, updated_at)
         VALUES (?, ?, ?, ?, '1', ?)`,
        clientId,
        principalIdHash,
        role,
        streamId,
        now,
      );
      this.sql.exec(
        "UPDATE session_state SET next_stream_id = ?, updated_at = ? WHERE singleton = 1",
        streamId + 1,
        now,
      );
    });
    return {
      client_id: clientId,
      principal_id_hash: principalIdHash,
      role,
      stream_id: streamId,
      delivery_generation: "1",
    };
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
    const ticket = this.ticket(ticketDigest);
    if (ticket === undefined || ticket.channel !== channel || ticket.expires_at <= Date.now()) {
      if (ticket?.expires_at !== undefined && ticket.expires_at <= Date.now()) {
        this.sql.exec("DELETE FROM connection_ticket WHERE ticket_digest = ?", ticketDigest);
      }
      return json({ error: "invalid-ticket" }, 401);
    }

    if (channel === "data" && !this.hasControlSocket(ticket.connection_set_id)) {
      return json({ error: "control-channel-required" }, 409);
    }

    let hostFence = ticket.host_fence;
    if (ticket.peer === "host" && channel === "control") {
      this.closeTaggedSockets("peer:host");
      hostFence = this.advanceHostFence(ticket.connection_set_id);
    } else if (ticket.peer === "browser" && channel === "control" && ticket.client_id !== null) {
      this.closeTaggedSockets(`client:${ticket.client_id}`);
    }
    if (ticket.peer === "host" && hostFence === null) {
      return json({ error: "host-control-channel-required" }, 409);
    }

    this.sql.exec("DELETE FROM connection_ticket WHERE ticket_digest = ?", ticketDigest);

    const attachment = SocketAttachmentSchema.parse({
      version: 1,
      peer: ticket.peer,
      channel,
      connectionSetId: ticket.connection_set_id,
      connectionId: ticket.connection_id,
      clientId: ticket.client_id,
      role: ticket.role,
      streamId: ticket.stream_id,
      deliveryGeneration: ticket.delivery_generation,
      hostFence,
    });
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    const tags = [
      `peer:${attachment.peer}`,
      `channel:${attachment.channel}`,
      `set:${attachment.connectionSetId}`,
    ];
    if (attachment.clientId !== null) {
      tags.push(`client:${attachment.clientId}`);
    }
    this.ctx.acceptWebSocket(server, tags);
    server.serializeAttachment(attachment);
    return new Response(null, { status: 101, webSocket: client });
  }

  private session(): SessionRow | undefined {
    return this.sql.exec("SELECT * FROM session_state WHERE singleton = 1").toArray()[0] as
      | SessionRow
      | undefined;
  }

  private ticket(ticketDigest: string): TicketRow | undefined {
    return this.sql
      .exec("SELECT * FROM connection_ticket WHERE ticket_digest = ?", ticketDigest)
      .toArray()[0] as TicketRow | undefined;
  }

  private hasControlSocket(connectionSetId: string): boolean {
    return this.ctx.getWebSockets(`set:${connectionSetId}`).some((socket) => {
      return socket.readyState === WebSocket.OPEN && readAttachment(socket)?.channel === "control";
    });
  }

  private advanceHostFence(connectionSetId: string): string {
    let nextFence = "0";
    this.ctx.storage.transactionSync(() => {
      const session = this.session();
      if (session === undefined) {
        throw new Error("session is not initialized");
      }
      nextFence = (BigInt(session.host_fence) + 1n).toString();
      const now = Date.now();
      this.sql.exec(
        "UPDATE session_state SET host_fence = ?, updated_at = ? WHERE singleton = 1",
        nextFence,
        now,
      );
      this.sql.exec(
        "UPDATE connection_ticket SET host_fence = ? WHERE connection_set_id = ?",
        nextFence,
        connectionSetId,
      );
    });
    return nextFence;
  }

  private closeTaggedSockets(tag: string, except?: WebSocket): void {
    for (const socket of this.ctx.getWebSockets(tag)) {
      if (socket !== except && socket.readyState < WebSocket.CLOSING) {
        socket.close(4001, "connection replaced");
      }
    }
  }
}
