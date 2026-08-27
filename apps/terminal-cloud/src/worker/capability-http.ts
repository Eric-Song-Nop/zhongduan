import {
  CapabilityMintRequestSchema,
  CapabilityRefreshRequestSchema,
  CapabilityResponseSchema,
  HostCapabilityReclaimRequestSchema,
  type CapabilityResponse,
  type CapabilityRole,
} from "@zhongduan/protocol";
import {
  AuthError,
  issueCapability,
  randomId,
  secretsEqual,
  verifyBearerCapability,
  type CapabilityClaims,
} from "./auth";
import type { CloudEnv } from "./env";

export type CapabilityOperation = "mint" | "refresh" | "reclaim-host";

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

function capabilityLifetimeSeconds(role: CapabilityRole): number {
  return role === "host" ? HOST_CAPABILITY_SECONDS : BROWSER_CAPABILITY_SECONDS;
}

export async function issueSessionCapability(
  signingKey: string,
  sessionId: string,
  role: CapabilityRole,
  subject = randomId(),
): Promise<CapabilityResponse> {
  const issuedAt = Math.floor(Date.now() / 1000);
  const claims: CapabilityClaims = {
    version: 1,
    sessionId,
    subject,
    role,
    issuedAt,
    expiresAt: issuedAt + capabilityLifetimeSeconds(role),
    tokenId: randomId(),
  };
  return CapabilityResponseSchema.parse({
    capability: await issueCapability(claims, signingKey),
    expiresAt: claims.expiresAt,
    role,
  });
}

async function mintBrowserCapability(
  request: Request,
  env: CloudEnv,
  sessionId: string,
): Promise<Response> {
  try {
    await verifyBearerCapability(request, env.CAPABILITY_SIGNING_KEY, {
      sessionId,
      roles: ["host"],
    });
  } catch (error) {
    if (error instanceof AuthError) {
      return json(
        { error: error.code === "wrong-role" ? "forbidden" : "unauthorized" },
        error.code === "wrong-role" ? 403 : 401,
      );
    }
    throw error;
  }

  const input = CapabilityMintRequestSchema.safeParse(await requestJson(request));
  if (!input.success) return json({ error: "invalid-capability-request" }, 400);
  return json(
    await issueSessionCapability(env.CAPABILITY_SIGNING_KEY, sessionId, input.data.role),
    201,
  );
}

async function refreshCapability(
  request: Request,
  env: CloudEnv,
  sessionId: string,
): Promise<Response> {
  let claims: CapabilityClaims;
  try {
    claims = await verifyBearerCapability(request, env.CAPABILITY_SIGNING_KEY, { sessionId });
  } catch (error) {
    if (error instanceof AuthError) return json({ error: "unauthorized" }, 401);
    throw error;
  }

  const input = CapabilityRefreshRequestSchema.safeParse(await requestJson(request));
  if (!input.success) return json({ error: "invalid-capability-request" }, 400);
  return json(
    await issueSessionCapability(
      env.CAPABILITY_SIGNING_KEY,
      claims.sessionId,
      claims.role,
      claims.subject,
    ),
  );
}

async function reclaimHostCapability(
  request: Request,
  env: CloudEnv,
  sessionId: string,
): Promise<Response> {
  const bootstrap = bearerToken(request);
  if (bootstrap === undefined || !(await secretsEqual(bootstrap, env.BOOTSTRAP_TOKEN))) {
    return json({ error: "unauthorized" }, 401);
  }

  const input = HostCapabilityReclaimRequestSchema.safeParse(await requestJson(request));
  if (!input.success) return json({ error: "invalid-capability-request" }, 400);
  const verified = await env.TERMINAL_SESSIONS.get(
    env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`),
  ).fetch("https://do.internal/internal/session-identity/verify", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ sessionId, ...input.data }),
  });
  if (!verified.ok) {
    await verified.body?.cancel();
    if (verified.status !== 404 && verified.status !== 409) {
      return json({ error: "session-verification-failed" }, 503);
    }
    return json(
      { error: verified.status === 404 ? "session-not-found" : "session-identity-mismatch" },
      verified.status === 404 ? 404 : 409,
    );
  }
  await verified.body?.cancel();
  return json(await issueSessionCapability(env.CAPABILITY_SIGNING_KEY, sessionId, "host"));
}

export function handleCapabilityRequest(
  request: Request,
  env: CloudEnv,
  sessionId: string,
  operation: CapabilityOperation,
): Promise<Response> {
  if (operation === "mint") return mintBrowserCapability(request, env, sessionId);
  if (operation === "refresh") return refreshCapability(request, env, sessionId);
  return reclaimHostCapability(request, env, sessionId);
}
