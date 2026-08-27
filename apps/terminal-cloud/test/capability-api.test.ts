import { exports as workerExports } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { issueCapability, verifyCapability, type CapabilityClaims } from "../src/worker/auth";

interface CreatedSession {
  engineId: string;
  hostCapability: string;
  observerCapability: string;
  sessionEpoch: string;
  sessionId: string;
  writerCapability: string;
}

interface CapabilityResponse {
  capability: string;
  expiresAt: number;
  role: "host" | "writer" | "observer";
}

const origin = "https://terminal.example.test";
const bootstrap = "test-bootstrap-token-with-at-least-32-bytes";
const signingKey = "test-capability-key-with-at-least-32-bytes";
const engineId = "ghostty:test+snapshot-v1+wterm:test";
let sessionCounter = 0;

function nextSessionId(): string {
  sessionCounter += 1;
  return `session_capability_${sessionCounter.toString().padStart(16, "0")}`;
}

async function createSession(): Promise<CreatedSession> {
  const sessionId = nextSessionId();
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${bootstrap}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ sessionId, engineId, sessionEpoch: "7" }),
    }),
  );
  expect(response.status).toBe(201);
  return response.json<CreatedSession>();
}

function capabilityRequest(
  sessionId: string,
  path: "" | "/refresh" | "/host/reclaim",
  bearer: string,
  body: unknown,
): Promise<Response> {
  return workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${sessionId}/capabilities${path}`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${bearer}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(body),
    }),
  );
}

describe("capability lifecycle API", () => {
  it("lets only the session Host mint server-subjected browser capabilities", async () => {
    const session = await createSession();
    const mintedResponse = await capabilityRequest(session.sessionId, "", session.hostCapability, {
      role: "writer",
    });
    expect(mintedResponse.status).toBe(201);
    const minted = await mintedResponse.json<CapabilityResponse>();
    const mintedClaims = await verifyCapability(minted.capability, signingKey);
    expect(minted).toMatchObject({ role: "writer", expiresAt: mintedClaims.expiresAt });
    expect(mintedClaims).toMatchObject({
      sessionId: session.sessionId,
      role: "writer",
    });

    const secondResponse = await capabilityRequest(session.sessionId, "", session.hostCapability, {
      role: "observer",
    });
    expect(secondResponse.status).toBe(201);
    const second = await secondResponse.json<CapabilityResponse>();
    const secondClaims = await verifyCapability(second.capability, signingKey);
    expect(secondClaims.role).toBe("observer");
    expect(secondClaims.subject).not.toBe(mintedClaims.subject);

    const browserMint = await capabilityRequest(session.sessionId, "", session.writerCapability, {
      role: "observer",
    });
    expect(browserMint.status).toBe(403);
    await browserMint.body?.cancel();

    const callerChosenSubject = await capabilityRequest(
      session.sessionId,
      "",
      session.hostCapability,
      { role: "writer", subject: "caller_chosen_subject" },
    );
    expect(callerChosenSubject.status).toBe(400);
    await callerChosenSubject.body?.cancel();
  });

  it("refreshes only valid same-session capabilities without changing authority", async () => {
    const firstSession = await createSession();
    const secondSession = await createSession();
    const original = await verifyCapability(firstSession.writerCapability, signingKey);

    const refreshedResponse = await capabilityRequest(
      firstSession.sessionId,
      "/refresh",
      firstSession.writerCapability,
      {},
    );
    expect(refreshedResponse.status).toBe(200);
    const refreshed = await refreshedResponse.json<CapabilityResponse>();
    const refreshedClaims = await verifyCapability(refreshed.capability, signingKey);
    expect(refreshedClaims).toMatchObject({
      sessionId: original.sessionId,
      subject: original.subject,
      role: original.role,
    });
    expect(refreshedClaims.tokenId).not.toBe(original.tokenId);
    expect(refreshed.expiresAt).toBe(refreshedClaims.expiresAt);

    const crossSession = await capabilityRequest(
      secondSession.sessionId,
      "/refresh",
      firstSession.writerCapability,
      {},
    );
    expect(crossSession.status).toBe(401);
    await crossSession.body?.cancel();

    const roleOverride = await capabilityRequest(
      firstSession.sessionId,
      "/refresh",
      firstSession.writerCapability,
      { role: "observer" },
    );
    expect(roleOverride.status).toBe(400);
    await roleOverride.body?.cancel();
  });

  it("rejects expired and forged capabilities during refresh", async () => {
    const session = await createSession();
    const now = Math.floor(Date.now() / 1000);
    const baseClaims: CapabilityClaims = {
      version: 1,
      sessionId: session.sessionId,
      subject: "expired_subject_AAAAAAAA",
      role: "observer",
      issuedAt: now - 600,
      expiresAt: now - 1,
      tokenId: "expired_token_AAAAAAAAAA",
    };
    const expired = await issueCapability(baseClaims, signingKey);
    const forged = await issueCapability(
      { ...baseClaims, issuedAt: now, expiresAt: now + 600 },
      "different-test-signing-key-with-at-least-32-bytes",
    );

    for (const token of [expired, forged]) {
      const response = await capabilityRequest(session.sessionId, "/refresh", token, {});
      expect(response.status).toBe(401);
      await response.body?.cancel();
    }
  });

  it("reclaims Host authority only for an exact bootstrap-bound session identity", async () => {
    const session = await createSession();
    const reclaimedResponse = await capabilityRequest(
      session.sessionId,
      "/host/reclaim",
      bootstrap,
      { engineId: session.engineId, sessionEpoch: session.sessionEpoch },
    );
    expect(reclaimedResponse.status).toBe(200);
    const reclaimed = await reclaimedResponse.json<CapabilityResponse>();
    const claims = await verifyCapability(reclaimed.capability, signingKey);
    expect(claims).toMatchObject({ sessionId: session.sessionId, role: "host" });

    for (const identity of [
      { engineId: `${session.engineId}:wrong`, sessionEpoch: session.sessionEpoch },
      { engineId: session.engineId, sessionEpoch: "8" },
    ]) {
      const mismatch = await capabilityRequest(
        session.sessionId,
        "/host/reclaim",
        bootstrap,
        identity,
      );
      expect(mismatch.status).toBe(409);
      await mismatch.body?.cancel();
    }

    const browserBearer = await capabilityRequest(
      session.sessionId,
      "/host/reclaim",
      session.writerCapability,
      { engineId: session.engineId, sessionEpoch: session.sessionEpoch },
    );
    expect(browserBearer.status).toBe(401);
    await browserBearer.body?.cancel();

    const missingSession = await capabilityRequest(
      "missing_session_AAAAAAAA",
      "/host/reclaim",
      bootstrap,
      { engineId: session.engineId, sessionEpoch: session.sessionEpoch },
    );
    expect(missingSession.status).toBe(404);
    await missingSession.body?.cancel();
  });
});
