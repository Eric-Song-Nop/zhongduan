import {
  ConnectionSetRequestSchema,
  CreateSessionRequestSchema,
  CreateSessionResponseSchema,
} from "@zhongduan/protocol";
import { AuthError, secretsEqual, verifyBearerCapability } from "./auth";
import { handleCapabilityRequest, issueSessionCapability } from "./capability-http";
import type { CloudEnv } from "./env";
import { handleSnapshotRequest } from "./snapshot-http";

export { TerminalSessionDO } from "./terminal-session-do";

const sessionIdPattern = "[A-Za-z0-9_-]{16,128}";
const ConnectionSetRoute = new RegExp(
  `^/api/v1/sessions/(${sessionIdPattern})/connection-sets$`,
  "u",
);
const WebSocketRoute = new RegExp(
  `^/api/v1/sessions/(${sessionIdPattern})/ws/(control|data)$`,
  "u",
);
const SnapshotRoute = new RegExp(
  `^/api/v1/sessions/(${sessionIdPattern})/snapshots/(${sessionIdPattern})$`,
  "u",
);
const CapabilityRoute = new RegExp(`^/api/v1/sessions/(${sessionIdPattern})/capabilities$`, "u");
const CapabilityRefreshRoute = new RegExp(
  `^/api/v1/sessions/(${sessionIdPattern})/capabilities/refresh$`,
  "u",
);
const HostCapabilityReclaimRoute = new RegExp(
  `^/api/v1/sessions/(${sessionIdPattern})/capabilities/host/reclaim$`,
  "u",
);

function json(data: unknown, status = 200): Response {
  return Response.json(data, {
    status,
    headers: { "cache-control": "no-store" },
  });
}

async function requestJson(request: Request): Promise<unknown> {
  try {
    return await request.json();
  } catch {
    return undefined;
  }
}

function bearerToken(request: Request): string | undefined {
  const authorization = request.headers.get("authorization");
  return authorization?.startsWith("Bearer ") ? authorization.slice("Bearer ".length) : undefined;
}

function sessionStub(env: CloudEnv, sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

async function createSession(request: Request, env: CloudEnv): Promise<Response> {
  const bootstrap = bearerToken(request);
  if (bootstrap === undefined || !(await secretsEqual(bootstrap, env.BOOTSTRAP_TOKEN))) {
    return json({ error: "unauthorized" }, 401);
  }
  const input = CreateSessionRequestSchema.safeParse(await requestJson(request));
  if (!input.success) {
    return json({ error: "invalid-session" }, 400);
  }

  const { sessionId } = input.data;
  const initialized = await sessionStub(env, sessionId).fetch(
    "https://do.internal/internal/initialize",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(input.data),
    },
  );
  if (initialized.status === 409) {
    await initialized.body?.cancel();
    return json({ error: "session-conflict" }, 409);
  }
  if (!initialized.ok) {
    await initialized.body?.cancel();
    return json({ error: "session-initialization-failed" }, 503);
  }
  const initialization = (await initialized.json()) as { created?: unknown };
  if (typeof initialization.created !== "boolean") {
    return json({ error: "session-initialization-failed" }, 503);
  }

  const [hostCapability, writerCapability, observerCapability] = await Promise.all([
    issueSessionCapability(env.CAPABILITY_SIGNING_KEY, sessionId, "host"),
    issueSessionCapability(env.CAPABILITY_SIGNING_KEY, sessionId, "writer"),
    issueSessionCapability(env.CAPABILITY_SIGNING_KEY, sessionId, "observer"),
  ]);
  return json(
    CreateSessionResponseSchema.parse({
      sessionId,
      engineId: input.data.engineId,
      sessionEpoch: input.data.sessionEpoch,
      hostCapability: hostCapability.capability,
      hostCapabilityExpiresAt: hostCapability.expiresAt,
      writerCapability: writerCapability.capability,
      writerCapabilityExpiresAt: writerCapability.expiresAt,
      observerCapability: observerCapability.capability,
      observerCapabilityExpiresAt: observerCapability.expiresAt,
    }),
    initialization.created ? 201 : 200,
  );
}

async function createConnectionSet(
  request: Request,
  env: CloudEnv,
  sessionId: string,
): Promise<Response> {
  let claims;
  try {
    claims = await verifyBearerCapability(request, env.CAPABILITY_SIGNING_KEY, { sessionId });
  } catch (error) {
    if (error instanceof AuthError) {
      return json({ error: "unauthorized" }, 401);
    }
    throw error;
  }

  const input = ConnectionSetRequestSchema.safeParse(await requestJson(request));
  if (!input.success || (claims.role === "host" && input.data.clientId !== undefined)) {
    return json({ error: "invalid-connection-set" }, 400);
  }
  return sessionStub(env, sessionId).fetch("https://do.internal/internal/connection-sets", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      sessionId,
      subject: claims.subject,
      role: claims.role,
      ...(input.data.clientId === undefined ? {} : { clientId: input.data.clientId }),
    }),
  });
}

async function openWebSocket(
  request: Request,
  env: CloudEnv,
  sessionId: string,
  channel: "control" | "data",
): Promise<Response> {
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    return json({ error: "websocket-upgrade-required" }, 426);
  }
  const source = new URL(request.url);
  const ticket = source.searchParams.get("ticket");
  if (ticket === null) {
    return json({ error: "invalid-ticket" }, 401);
  }
  const internal = new URL(`https://do.internal/internal/ws/${channel}`);
  internal.searchParams.set("ticket", ticket);
  return sessionStub(env, sessionId).fetch(internal, {
    headers: { upgrade: "websocket" },
  });
}

async function fetch(request: Request, env: CloudEnv): Promise<Response> {
  const url = new URL(request.url);
  if (request.method === "POST" && url.pathname === "/api/v1/sessions") {
    return createSession(request, env);
  }

  const connectionSetMatch = ConnectionSetRoute.exec(url.pathname);
  if (request.method === "POST" && connectionSetMatch !== null) {
    return createConnectionSet(request, env, connectionSetMatch[1]!);
  }

  const capabilityMatch = CapabilityRoute.exec(url.pathname);
  if (request.method === "POST" && capabilityMatch !== null) {
    return handleCapabilityRequest(request, env, capabilityMatch[1]!, "mint");
  }

  const capabilityRefreshMatch = CapabilityRefreshRoute.exec(url.pathname);
  if (request.method === "POST" && capabilityRefreshMatch !== null) {
    return handleCapabilityRequest(request, env, capabilityRefreshMatch[1]!, "refresh");
  }

  const hostCapabilityReclaimMatch = HostCapabilityReclaimRoute.exec(url.pathname);
  if (request.method === "POST" && hostCapabilityReclaimMatch !== null) {
    return handleCapabilityRequest(request, env, hostCapabilityReclaimMatch[1]!, "reclaim-host");
  }

  const snapshotMatch = SnapshotRoute.exec(url.pathname);
  if (snapshotMatch !== null) {
    return handleSnapshotRequest(request, env, snapshotMatch[1]!, snapshotMatch[2]!);
  }

  const webSocketMatch = WebSocketRoute.exec(url.pathname);
  if (request.method === "GET" && webSocketMatch !== null) {
    return openWebSocket(request, env, webSocketMatch[1]!, webSocketMatch[2] as "control" | "data");
  }

  if (url.pathname.startsWith("/api/")) {
    return json({ error: "not-found" }, 404);
  }
  return env.ASSETS.fetch(request);
}

export default { fetch } satisfies ExportedHandler<CloudEnv>;
