import { describe, expect, it } from "vitest";
import {
  AuthError,
  issueCapability,
  secretsEqual,
  verifyCapability,
  type CapabilityClaims,
} from "../src/worker/auth";

const secret = "a-test-signing-secret-that-is-longer-than-32-bytes";
const now = 1_800_000_000;

function claims(overrides: Partial<CapabilityClaims> = {}): CapabilityClaims {
  return {
    version: 1,
    sessionId: "session_AAAAAAAAAAAAAAAA",
    subject: "subject_AAAAAAAAAAAAAAAA",
    role: "writer",
    issuedAt: now,
    expiresAt: now + 600,
    tokenId: "token_AAAAAAAAAAAAAAAAAA",
    ...overrides,
  };
}

describe("session capabilities", () => {
  it("fails closed when the bootstrap secret is not configured", async () => {
    await expect(secretsEqual("", "")).resolves.toBe(false);
    await expect(secretsEqual(secret, secret)).resolves.toBe(true);
  });

  it("round-trips scoped, signed claims", async () => {
    const token = await issueCapability(claims(), secret);

    await expect(
      verifyCapability(token, secret, {
        sessionId: "session_AAAAAAAAAAAAAAAA",
        roles: ["writer"],
        nowSeconds: now + 1,
      }),
    ).resolves.toMatchObject({ role: "writer" });
  });

  it("rejects tampering, expiry, and a different session", async () => {
    const token = await issueCapability(claims(), secret);
    const parts = token.split(".");
    const signature = parts[2]!;
    const tampered = `${parts[0]}.${parts[1]}.${signature[0] === "A" ? "B" : "A"}${signature.slice(1)}`;

    await expect(verifyCapability(tampered, secret, { nowSeconds: now + 1 })).rejects.toMatchObject(
      {
        code: "invalid-signature",
      } satisfies Partial<AuthError>,
    );
    await expect(verifyCapability(token, secret, { nowSeconds: now + 601 })).rejects.toMatchObject({
      code: "expired-token",
    } satisfies Partial<AuthError>);
    await expect(
      verifyCapability(token, secret, {
        sessionId: "session_BBBBBBBBBBBBBBBB",
        nowSeconds: now + 1,
      }),
    ).rejects.toMatchObject({ code: "wrong-session" } satisfies Partial<AuthError>);
  });
});
