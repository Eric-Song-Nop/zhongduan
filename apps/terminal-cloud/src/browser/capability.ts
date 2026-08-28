import {
  BrowserCapabilityRoleSchema,
  CapabilityResponseSchema,
  CloudResourceIdSchema,
  type BrowserCapabilityRole,
} from "@zhongduan/protocol";

const CAPABILITY_FRAGMENT_KEY = "capability";
const STORAGE_PREFIX = "zhongduan:browser-capability:";
const MAX_CAPABILITY_CHARS = 4_096;
const MAX_STORED_CAPABILITY_JSON_CHARS = 8_192;
const MAX_CLOCK_SKEW_SECONDS = 60;

interface CapabilityClaimsView {
  sessionId: string;
  role: BrowserCapabilityRole;
  issuedAt: number;
  expiresAt: number;
}

interface StoredCapability {
  capability: string;
}

export interface CapabilityLocation {
  readonly hash: string;
  readonly pathname: string;
  readonly search: string;
}

export interface CapabilityHistory {
  readonly state: unknown;
  replaceState(data: unknown, unused: string, url?: string | URL | null): void;
}

export interface BrowserCapabilityBootstrap {
  capability: string;
  expiresAt: number;
  issuedAt: number;
  role: BrowserCapabilityRole;
  sessionId: string;
}

export interface CapabilityManagerOptions {
  bootstrap: BrowserCapabilityBootstrap;
  fetch?: typeof fetch;
  now?: () => number;
  random?: () => number;
  setTimer?: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
  storage?: Pick<Storage, "getItem" | "removeItem" | "setItem">;
  onRefreshError?: () => void;
}

function decodeBase64Url(value: string): string {
  if (!/^[A-Za-z0-9_-]+$/u.test(value)) throw new Error("capability payload is malformed");
  const base64 = value
    .replaceAll("-", "+")
    .replaceAll("_", "/")
    .padEnd(Math.ceil(value.length / 4) * 4, "=");
  return new TextDecoder().decode(
    Uint8Array.from(atob(base64), (character) => character.charCodeAt(0)),
  );
}

function readCapabilityClaims(capability: string): CapabilityClaimsView {
  if (capability.length > MAX_CAPABILITY_CHARS) {
    throw new Error("capability exceeds the supported length");
  }
  const parts = capability.split(".");
  if (parts.length !== 3 || parts[0] !== "zcap1" || parts[1] === undefined) {
    throw new Error("capability is malformed");
  }
  let input: unknown;
  try {
    input = JSON.parse(decodeBase64Url(parts[1]));
  } catch {
    throw new Error("capability payload is malformed");
  }
  if (typeof input !== "object" || input === null) {
    throw new Error("capability payload is malformed");
  }
  const claims = input as Record<string, unknown>;
  const sessionId = CloudResourceIdSchema.parse(claims.sessionId);
  const role = BrowserCapabilityRoleSchema.parse(claims.role);
  const issuedAt = claims.issuedAt;
  const expiresAt = claims.expiresAt;
  if (
    !Number.isSafeInteger(issuedAt) ||
    !Number.isSafeInteger(expiresAt) ||
    (issuedAt as number) < 0 ||
    (expiresAt as number) <= (issuedAt as number)
  ) {
    throw new Error("capability lifetime is malformed");
  }
  return {
    sessionId,
    role,
    issuedAt: issuedAt as number,
    expiresAt: expiresAt as number,
  };
}

function storageKey(sessionId: string): string {
  return `${STORAGE_PREFIX}${sessionId}`;
}

function readStoredCapability(
  storage: Pick<Storage, "getItem" | "removeItem">,
  sessionId: string,
): string | undefined {
  let stored: string | null;
  try {
    stored = storage.getItem(storageKey(sessionId));
  } catch {
    return undefined;
  }
  if (stored === null) return undefined;
  if (stored.length > MAX_STORED_CAPABILITY_JSON_CHARS) {
    removeStoredCapability(storage, sessionId);
    return undefined;
  }
  try {
    const value = JSON.parse(stored) as Partial<StoredCapability>;
    if (typeof value.capability !== "string" || value.capability.length > MAX_CAPABILITY_CHARS) {
      removeStoredCapability(storage, sessionId);
      return undefined;
    }
    return value.capability;
  } catch {
    removeStoredCapability(storage, sessionId);
    return undefined;
  }
}

function writeStoredCapability(
  storage: Pick<Storage, "setItem">,
  sessionId: string,
  capability: string,
): void {
  try {
    storage.setItem(
      storageKey(sessionId),
      JSON.stringify({ capability } satisfies StoredCapability),
    );
  } catch {
    // Capabilities remain memory-only when browser storage is unavailable.
  }
}

function removeStoredCapability(storage: Pick<Storage, "removeItem">, sessionId: string): void {
  try {
    storage.removeItem(storageKey(sessionId));
  } catch {
    // Invalid storage state cannot prevent fragment scrubbing or authentication.
  }
}

export function sessionIdFromPath(pathname: string): string {
  const segment = pathname.split("/").filter(Boolean).at(-1);
  return CloudResourceIdSchema.parse(segment);
}

/** Scrub the bearer from the address bar before any asynchronous application work starts. */
export function consumeBrowserCapability(
  location: CapabilityLocation,
  history: CapabilityHistory,
  storage: Pick<Storage, "getItem" | "removeItem" | "setItem">,
  now: () => number = Date.now,
): BrowserCapabilityBootstrap {
  const sessionId = sessionIdFromPath(location.pathname);
  const fragment = new URLSearchParams(location.hash.startsWith("#") ? location.hash.slice(1) : "");
  const fragmentCapability = fragment.get(CAPABILITY_FRAGMENT_KEY) ?? undefined;
  if (location.hash.length > 0) {
    history.replaceState(history.state, "", `${location.pathname}${location.search}`);
  }
  const fromStorage = fragmentCapability === undefined;
  const capability = fragmentCapability ?? readStoredCapability(storage, sessionId);
  if (capability === undefined) throw new Error("session capability is missing");

  let claims: CapabilityClaimsView;
  try {
    claims = readCapabilityClaims(capability);
    const nowSeconds = Math.floor(now() / 1_000);
    if (claims.expiresAt <= nowSeconds || claims.issuedAt > nowSeconds + MAX_CLOCK_SKEW_SECONDS) {
      throw new Error("session capability is outside its valid lifetime");
    }
    if (claims.sessionId !== sessionId) {
      throw new Error("capability belongs to another session");
    }
  } catch (error) {
    if (fromStorage) removeStoredCapability(storage, sessionId);
    throw error;
  }
  if (fragmentCapability !== undefined) writeStoredCapability(storage, sessionId, capability);
  return { capability, ...claims };
}

export class CapabilityManager {
  readonly #sessionId: string;
  readonly #fetch: typeof fetch;
  readonly #now: () => number;
  readonly #random: () => number;
  readonly #setTimer: NonNullable<CapabilityManagerOptions["setTimer"]>;
  readonly #clearTimer: NonNullable<CapabilityManagerOptions["clearTimer"]>;
  readonly #storage?: CapabilityManagerOptions["storage"];
  readonly #onRefreshError: (() => void) | undefined;
  #capability: string;
  #claims: CapabilityClaimsView;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #stopped = false;
  #refreshing: Promise<void> | null = null;

  constructor(options: CapabilityManagerOptions) {
    this.#sessionId = options.bootstrap.sessionId;
    this.#capability = options.bootstrap.capability;
    this.#claims = options.bootstrap;
    this.#fetch = options.fetch ?? fetch;
    this.#now = options.now ?? Date.now;
    this.#random = options.random ?? Math.random;
    this.#setTimer = options.setTimer ?? setTimeout;
    this.#clearTimer = options.clearTimer ?? clearTimeout;
    this.#storage = options.storage;
    this.#onRefreshError = options.onRefreshError;
  }

  get capability(): string {
    return this.#capability;
  }

  get role(): BrowserCapabilityRole {
    return this.#claims.role;
  }

  get expiresAt(): number {
    return this.#claims.expiresAt;
  }

  authorizationHeaders(): Readonly<Record<string, string>> {
    return { Authorization: `Bearer ${this.#capability}` };
  }

  start(): void {
    if (this.#stopped) return;
    this.#scheduleAtHalfLife();
  }

  stop(): void {
    this.#stopped = true;
    if (this.#timer !== null) this.#clearTimer(this.#timer);
    this.#timer = null;
  }

  refreshNow(): Promise<void> {
    if (this.#refreshing !== null) return this.#refreshing;
    this.#refreshing = this.#refresh().finally(() => {
      this.#refreshing = null;
    });
    return this.#refreshing;
  }

  #scheduleAtHalfLife(): void {
    if (this.#stopped) return;
    if (this.#timer !== null) this.#clearTimer(this.#timer);
    const lifetimeSeconds = this.#claims.expiresAt - this.#claims.issuedAt;
    const refreshFraction = 0.45 + this.#random() * 0.1;
    const refreshAtMs = (this.#claims.issuedAt + lifetimeSeconds * refreshFraction) * 1_000;
    this.#timer = this.#setTimer(
      () => {
        this.#timer = null;
        void this.refreshNow().catch(() => undefined);
      },
      Math.max(0, refreshAtMs - this.#now()),
    );
  }

  async #refresh(): Promise<void> {
    try {
      const response = await this.#fetch(
        `/api/v1/sessions/${encodeURIComponent(this.#sessionId)}/capabilities/refresh`,
        {
          method: "POST",
          credentials: "same-origin",
          redirect: "error",
          headers: {
            ...this.authorizationHeaders(),
            "Content-Type": "application/json",
          },
          body: "{}",
        },
      );
      if (!response.ok) throw new Error("capability refresh failed");
      const refreshed = CapabilityResponseSchema.parse(await response.json());
      const claims = readCapabilityClaims(refreshed.capability);
      if (
        claims.sessionId !== this.#sessionId ||
        claims.role !== this.#claims.role ||
        claims.expiresAt !== refreshed.expiresAt
      ) {
        throw new Error("capability refresh changed identity");
      }
      this.#capability = refreshed.capability;
      this.#claims = claims;
      if (this.#storage !== undefined) {
        writeStoredCapability(this.#storage, this.#sessionId, refreshed.capability);
      }
      this.#scheduleAtHalfLife();
    } catch (error) {
      this.#onRefreshError?.();
      if (!this.#stopped && this.#now() < this.#claims.expiresAt * 1_000) {
        const retryMs = 30_000 + Math.floor(this.#random() * 30_000);
        this.#timer = this.#setTimer(() => {
          this.#timer = null;
          void this.refreshNow().catch(() => undefined);
        }, retryMs);
      }
      throw error;
    }
  }
}
