import { CapabilityRoleSchema, type CapabilityRole } from "@zhongduan/protocol";
import { z } from "zod";

const tokenId = z.string().regex(/^[A-Za-z0-9_-]{16,128}$/);

export { CapabilityRoleSchema };
export type { CapabilityRole };

export const CapabilityClaimsSchema = z.strictObject({
  version: z.literal(1),
  sessionId: tokenId,
  subject: tokenId,
  role: CapabilityRoleSchema,
  issuedAt: z.number().int().nonnegative(),
  expiresAt: z.number().int().positive(),
  tokenId,
});

export type CapabilityClaims = z.infer<typeof CapabilityClaimsSchema>;

export class AuthError extends Error {
  constructor(
    readonly code:
      | "malformed-token"
      | "invalid-signature"
      | "expired-token"
      | "wrong-session"
      | "wrong-role"
      | "missing-token",
  ) {
    super(code);
    this.name = "AuthError";
  }
}

const encoder = new TextEncoder();
const CAPABILITY_PREFIX = "zcap1";
const MAX_CLOCK_SKEW_SECONDS = 60;

function bytesToBase64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/u, "");
}

function base64UrlToBytes(encoded: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]+$/u.test(encoded)) {
    throw new AuthError("malformed-token");
  }
  const padded = encoded
    .replaceAll("-", "+")
    .replaceAll("_", "/")
    .padEnd(Math.ceil(encoded.length / 4) * 4, "=");
  try {
    return Uint8Array.from(atob(padded), (character) => character.charCodeAt(0));
  } catch {
    throw new AuthError("malformed-token");
  }
}

async function importSigningKey(secret: string): Promise<CryptoKey> {
  if (encoder.encode(secret).byteLength < 32) {
    throw new Error("CAPABILITY_SIGNING_KEY must contain at least 32 bytes");
  }
  return crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"],
  );
}

export async function issueCapability(claims: CapabilityClaims, secret: string): Promise<string> {
  const parsed = CapabilityClaimsSchema.parse(claims);
  if (parsed.expiresAt <= parsed.issuedAt) {
    throw new Error("capability expiry must follow issuance");
  }

  const payload = bytesToBase64Url(encoder.encode(JSON.stringify(parsed)));
  const signed = `${CAPABILITY_PREFIX}.${payload}`;
  const signature = await crypto.subtle.sign(
    "HMAC",
    await importSigningKey(secret),
    encoder.encode(signed),
  );
  return `${signed}.${bytesToBase64Url(new Uint8Array(signature))}`;
}

export interface VerifyCapabilityOptions {
  sessionId?: string;
  roles?: readonly CapabilityRole[];
  nowSeconds?: number;
}

export async function verifyCapability(
  token: string,
  secret: string,
  options: VerifyCapabilityOptions = {},
): Promise<CapabilityClaims> {
  if (token.length > 4096) {
    throw new AuthError("malformed-token");
  }
  const parts = token.split(".");
  if (parts.length !== 3 || parts[0] !== CAPABILITY_PREFIX) {
    throw new AuthError("malformed-token");
  }

  const payload = parts[1];
  const signaturePart = parts[2];
  if (payload === undefined || signaturePart === undefined) {
    throw new AuthError("malformed-token");
  }
  const signature = base64UrlToBytes(signaturePart);
  if (signature.byteLength !== 32) {
    throw new AuthError("malformed-token");
  }

  const valid = await crypto.subtle.verify(
    "HMAC",
    await importSigningKey(secret),
    signature.buffer as ArrayBuffer,
    encoder.encode(`${CAPABILITY_PREFIX}.${payload}`),
  );
  if (!valid) {
    throw new AuthError("invalid-signature");
  }

  let decoded: unknown;
  try {
    decoded = JSON.parse(new TextDecoder().decode(base64UrlToBytes(payload)));
  } catch {
    throw new AuthError("malformed-token");
  }
  const result = CapabilityClaimsSchema.safeParse(decoded);
  if (!result.success) {
    throw new AuthError("malformed-token");
  }

  const now = options.nowSeconds ?? Math.floor(Date.now() / 1000);
  if (result.data.expiresAt <= now || result.data.issuedAt > now + MAX_CLOCK_SKEW_SECONDS) {
    throw new AuthError("expired-token");
  }
  if (options.sessionId !== undefined && result.data.sessionId !== options.sessionId) {
    throw new AuthError("wrong-session");
  }
  if (options.roles !== undefined && !options.roles.includes(result.data.role)) {
    throw new AuthError("wrong-role");
  }
  return result.data;
}

export async function verifyBearerCapability(
  request: Request,
  secret: string,
  options: VerifyCapabilityOptions = {},
): Promise<CapabilityClaims> {
  const authorization = request.headers.get("authorization");
  if (authorization === null || !authorization.startsWith("Bearer ")) {
    throw new AuthError("missing-token");
  }
  return verifyCapability(authorization.slice("Bearer ".length), secret, options);
}

export async function secretsEqual(candidate: string, expected: string): Promise<boolean> {
  const expectedBytes = encoder.encode(expected);
  if (expectedBytes.byteLength < 32) {
    return false;
  }
  const [candidateHash, expectedHash] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(candidate)),
    crypto.subtle.digest("SHA-256", expectedBytes),
  ]);
  const left = new Uint8Array(candidateHash);
  const right = new Uint8Array(expectedHash);
  let difference = 0;
  for (let index = 0; index < left.length; index += 1) {
    difference |= left[index]! ^ right[index]!;
  }
  return difference === 0;
}

export function randomId(byteLength = 18): string {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  return bytesToBase64Url(bytes);
}

export async function sha256Hex(value: string): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(value)));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
