import { PositiveDecimalU64Schema } from "@zhongduan/protocol";
import { z } from "zod";
import {
  AuthError,
  issueCapability,
  randomId,
  secretsEqual,
  verifyBearerCapability,
  type CapabilityClaims,
  type CapabilityRole,
} from "./auth";
import type { CloudEnv } from "./env";

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

const CreateSessionSchema = z.strictObject({
  engineId: z.string().min(1).max(512),
  sessionEpoch: PositiveDecimalU64Schema,
});

const ConnectionSetRequestSchema = z.strictObject({
  clientId: z
    .string()
    .regex(/^[A-Za-z0-9_-]{16,128}$/)
    .optional(),
});

const HOST_CAPABILITY_SECONDS = 24 * 60 * 60;
const BROWSER_CAPABILITY_SECONDS = 8 * 60 * 60;

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

async function capabilityFor(
  env: CloudEnv,
  sessionId: string,
  role: CapabilityRole,
  lifetimeSeconds: number,
): Promise<string> {
  const issuedAt = Math.floor(Date.now() / 1000);
  const claims: CapabilityClaims = {
    version: 1,
    sessionId,
    subject: randomId(),
    role,
    issuedAt,
    expiresAt: issuedAt + lifetimeSeconds,
    tokenId: randomId(),
  };
  return issueCapability(claims, env.CAPABILITY_SIGNING_KEY);
}

async function createSession(request: Request, env: CloudEnv): Promise<Response> {
  const bootstrap = bearerToken(request);
  if (bootstrap === undefined || !(await secretsEqual(bootstrap, env.BOOTSTRAP_TOKEN))) {
    return json({ error: "unauthorized" }, 401);
  }
  const input = CreateSessionSchema.safeParse(await requestJson(request));
  if (!input.success) {
    return json({ error: "invalid-session" }, 400);
  }

  const sessionId = randomId();
  const initialized = await sessionStub(env, sessionId).fetch(
    "https://do.internal/internal/initialize",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ sessionId, ...input.data }),
    },
  );
  if (!initialized.ok) {
    return json({ error: "session-initialization-failed" }, 503);
  }

  const [hostCapability, writerCapability, observerCapability] = await Promise.all([
    capabilityFor(env, sessionId, "host", HOST_CAPABILITY_SECONDS),
    capabilityFor(env, sessionId, "writer", BROWSER_CAPABILITY_SECONDS),
    capabilityFor(env, sessionId, "observer", BROWSER_CAPABILITY_SECONDS),
  ]);
  return json(
    {
      sessionId,
      engineId: input.data.engineId,
      sessionEpoch: input.data.sessionEpoch,
      hostCapability,
      writerCapability,
      observerCapability,
    },
    201,
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
