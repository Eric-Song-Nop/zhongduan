import { describe, expect, it, vi } from "vitest";

import { CapabilityManager, consumeBrowserCapability } from "./capability";

const SESSION_ID = "session_123456789";
const NOW_SECONDS = 2_000_000_000;

class MemoryStorage {
  readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }
}

function capability(
  overrides: Partial<{ expiresAt: number; issuedAt: number; role: string; sessionId: string }> = {},
): string {
  const payload = Buffer.from(
    JSON.stringify({
      version: 1,
      sessionId: SESSION_ID,
      subject: "subject_123456789",
      role: "writer",
      issuedAt: NOW_SECONDS - 100,
      expiresAt: NOW_SECONDS + 100,
      tokenId: "token_1234567890",
      ...overrides,
    }),
  ).toString("base64url");
  return `zcap1.${payload}.${"s".repeat(43)}`;
}

describe("browser capability bootstrap", () => {
  it("scrubs the fragment synchronously and keeps the bearer only in session storage", () => {
    const storage = new MemoryStorage();
    const replaceState = vi.fn();
    const token = capability();

    const result = consumeBrowserCapability(
      {
        hash: `#capability=${encodeURIComponent(token)}`,
        pathname: `/sessions/${SESSION_ID}`,
        search: "?theme=dark",
      },
      { state: { retained: true }, replaceState },
      storage,
      () => NOW_SECONDS * 1_000,
    );

    expect(replaceState).toHaveBeenCalledOnce();
    expect(replaceState).toHaveBeenCalledWith(
      { retained: true },
      "",
      `/sessions/${SESSION_ID}?theme=dark`,
    );
    expect(result.capability).toBe(token);
    expect([...storage.values.values()].join(" ")).toContain(token);
  });

  it("scrubs but rejects an oversized fragment before decoding it", () => {
    const replaceState = vi.fn();
    expect(() =>
      consumeBrowserCapability(
        {
          hash: `#capability=${"x".repeat(4_097)}`,
          pathname: `/sessions/${SESSION_ID}`,
          search: "",
        },
        { state: null, replaceState },
        new MemoryStorage(),
        () => NOW_SECONDS * 1_000,
      ),
    ).toThrow(/supported length/);
    expect(replaceState).toHaveBeenCalledWith(null, "", `/sessions/${SESSION_ID}`);
  });

  it("removes an expired stored capability instead of failing repeatedly", () => {
    const storage = new MemoryStorage();
    const key = `zhongduan:browser-capability:${SESSION_ID}`;
    storage.setItem(
      key,
      JSON.stringify({ capability: capability({ expiresAt: NOW_SECONDS - 1 }) }),
    );

    expect(() =>
      consumeBrowserCapability(
        { hash: "", pathname: `/sessions/${SESSION_ID}`, search: "" },
        { state: null, replaceState: vi.fn() },
        storage,
        () => NOW_SECONDS * 1_000,
      ),
    ).toThrow(/valid lifetime/);
    expect(storage.getItem(key)).toBeNull();
  });

  it("scrubs and rejects a capability issued implausibly far in the future", () => {
    const replaceState = vi.fn();
    expect(() =>
      consumeBrowserCapability(
        {
          hash: `#capability=${encodeURIComponent(
            capability({ issuedAt: NOW_SECONDS + 61, expiresAt: NOW_SECONDS + 161 }),
          )}`,
          pathname: `/sessions/${SESSION_ID}`,
          search: "",
        },
        { state: null, replaceState },
        new MemoryStorage(),
        () => NOW_SECONDS * 1_000,
      ),
    ).toThrow(/valid lifetime/);
    expect(replaceState).toHaveBeenCalledOnce();
  });

  it("continues with a scrubbed fragment when session storage is disabled", () => {
    const replaceState = vi.fn();
    const throwingStorage = {
      getItem: () => {
        throw new DOMException("disabled", "SecurityError");
      },
      removeItem: () => {
        throw new DOMException("disabled", "SecurityError");
      },
      setItem: () => {
        throw new DOMException("disabled", "SecurityError");
      },
    };
    const token = capability();
    expect(
      consumeBrowserCapability(
        {
          hash: `#capability=${encodeURIComponent(token)}`,
          pathname: `/sessions/${SESSION_ID}`,
          search: "",
        },
        { state: null, replaceState },
        throwingStorage,
        () => NOW_SECONDS * 1_000,
      ).capability,
    ).toBe(token);
    expect(replaceState).toHaveBeenCalledOnce();
  });
});

describe("CapabilityManager", () => {
  it("handles scheduled refresh failure and retains rejection for manual refresh", async () => {
    const timers: Array<() => void> = [];
    const onRefreshError = vi.fn();
    const manager = new CapabilityManager({
      bootstrap: {
        capability: capability(),
        expiresAt: NOW_SECONDS + 100,
        issuedAt: NOW_SECONDS - 100,
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch: vi.fn().mockRejectedValue(new Error("offline")),
      now: () => NOW_SECONDS * 1_000,
      random: () => 0.5,
      setTimer: (callback) => {
        timers.push(callback);
        return timers.length as unknown as ReturnType<typeof setTimeout>;
      },
      clearTimer: vi.fn(),
      onRefreshError,
    });

    manager.start();
    expect(timers).toHaveLength(1);
    timers.shift()!();
    await vi.waitFor(() => expect(onRefreshError).toHaveBeenCalledOnce());
    expect(timers).toHaveLength(1);
    await expect(manager.refreshNow()).rejects.toThrow("offline");
    manager.stop();
  });
});
