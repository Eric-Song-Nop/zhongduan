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

class ManualTimers {
  readonly pending = new Map<
    ReturnType<typeof setTimeout>,
    { callback: () => void; delayMs: number }
  >();
  #nextId = 1;

  readonly setTimer = (callback: () => void, delayMs: number): ReturnType<typeof setTimeout> => {
    const id = this.#nextId as unknown as ReturnType<typeof setTimeout>;
    this.#nextId += 1;
    this.pending.set(id, { callback, delayMs });
    return id;
  };

  readonly clearTimer = (timer: ReturnType<typeof setTimeout>): void => {
    this.pending.delete(timer);
  };

  run(delayMs: number): void {
    const entry = [...this.pending].find(([, timer]) => timer.delayMs === delayMs);
    if (entry === undefined) throw new Error(`timer with delay ${delayMs} was not scheduled`);
    this.pending.delete(entry[0]);
    entry[1].callback();
  }

  delays(): number[] {
    return [...this.pending.values()].map(({ delayMs }) => delayMs);
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
  it("invokes default browser timers with the global receiver", async () => {
    const setTimer = vi.fn(function (this: unknown): ReturnType<typeof setTimeout> {
      if (this !== globalThis) throw new TypeError("Illegal invocation");
      return 1 as unknown as ReturnType<typeof setTimeout>;
    });
    const clearTimer = vi.fn(function (this: unknown): void {
      if (this !== globalThis) throw new TypeError("Illegal invocation");
    });
    vi.stubGlobal("setTimeout", setTimer);
    vi.stubGlobal("clearTimeout", clearTimer);
    try {
      const manager = new CapabilityManager({
        bootstrap: {
          capability: capability(),
          expiresAt: NOW_SECONDS + 100,
          issuedAt: NOW_SECONDS - 100,
          role: "writer",
          sessionId: SESSION_ID,
        },
        fetch: vi.fn(() => new Promise<Response>(() => undefined)),
      });

      const refreshing = manager.refreshNow();
      manager.stop();

      await expect(refreshing).rejects.toMatchObject({ name: "AbortError" });
      expect(setTimer).toHaveBeenCalledOnce();
      expect(clearTimer).toHaveBeenCalledOnce();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("handles scheduled refresh failure and retains rejection for manual refresh", async () => {
    const timers = new ManualTimers();
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
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
      onRefreshError,
    });

    manager.start();
    expect(timers.delays()).toEqual([0]);
    timers.run(0);
    await vi.waitFor(() => expect(onRefreshError).toHaveBeenCalledOnce());
    expect(timers.delays()).toEqual([45_000]);
    await expect(manager.refreshNow()).rejects.toThrow("offline");
    manager.stop();
  });

  it("aborts a refresh whose fetch exceeds the independent deadline", async () => {
    const timers = new ManualTimers();
    const onRefreshError = vi.fn();
    let requestSignal: AbortSignal | undefined;
    const fetchCapability = vi.fn<typeof fetch>((_input, init) => {
      requestSignal = init?.signal ?? undefined;
      return new Promise<Response>(() => undefined);
    });
    const manager = new CapabilityManager({
      bootstrap: {
        capability: capability(),
        expiresAt: NOW_SECONDS + 100,
        issuedAt: NOW_SECONDS - 100,
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch: fetchCapability,
      now: () => NOW_SECONDS * 1_000,
      random: () => 0.5,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
      onRefreshError,
    });

    const refreshing = manager.refreshNow();
    expect(timers.delays()).toEqual([10_000]);
    timers.run(10_000);

    await expect(refreshing).rejects.toMatchObject({ name: "TimeoutError" });
    expect(requestSignal?.aborted).toBe(true);
    expect(onRefreshError).toHaveBeenCalledOnce();
    expect(timers.delays()).toEqual([45_000]);
    manager.stop();
  });

  it("bounds response parsing and ignores a valid result that arrives after timeout", async () => {
    const timers = new ManualTimers();
    const originalCapability = capability();
    const refreshedCapability = capability({
      issuedAt: NOW_SECONDS,
      expiresAt: NOW_SECONDS + 200,
    });
    let resolveJson: ((value: unknown) => void) | undefined;
    const json = vi.fn(
      () =>
        new Promise<unknown>((resolve) => {
          resolveJson = resolve;
        }),
    );
    const manager = new CapabilityManager({
      bootstrap: {
        capability: originalCapability,
        expiresAt: NOW_SECONDS + 100,
        issuedAt: NOW_SECONDS - 100,
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch: vi.fn(async () => ({ json, ok: true }) as unknown as Response),
      now: () => NOW_SECONDS * 1_000,
      random: () => 0.5,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
    });

    const refreshing = manager.refreshNow();
    await vi.waitFor(() => expect(json).toHaveBeenCalledOnce());
    timers.run(10_000);
    await expect(refreshing).rejects.toMatchObject({ name: "TimeoutError" });

    resolveJson?.({
      capability: refreshedCapability,
      expiresAt: NOW_SECONDS + 200,
      role: "writer",
    });
    await Promise.resolve();
    expect(manager.capability).toBe(originalCapability);
    manager.stop();
  });

  it("stop aborts an active refresh without installing a late response", async () => {
    const timers = new ManualTimers();
    const onRefreshError = vi.fn();
    const originalCapability = capability();
    const refreshedCapability = capability({
      issuedAt: NOW_SECONDS,
      expiresAt: NOW_SECONDS + 200,
    });
    let requestSignal: AbortSignal | undefined;
    let resolveFetch: ((response: Response) => void) | undefined;
    const manager = new CapabilityManager({
      bootstrap: {
        capability: originalCapability,
        expiresAt: NOW_SECONDS + 100,
        issuedAt: NOW_SECONDS - 100,
        role: "writer",
        sessionId: SESSION_ID,
      },
      fetch: vi.fn((_input, init) => {
        requestSignal = init?.signal ?? undefined;
        return new Promise<Response>((resolve) => {
          resolveFetch = resolve;
        });
      }),
      now: () => NOW_SECONDS * 1_000,
      random: () => 0.5,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
      onRefreshError,
    });

    const refreshing = manager.refreshNow();
    manager.stop();
    await expect(refreshing).rejects.toMatchObject({ name: "AbortError" });
    expect(requestSignal?.aborted).toBe(true);
    expect(onRefreshError).not.toHaveBeenCalled();
    expect(timers.pending.size).toBe(0);

    resolveFetch?.(
      new Response(
        JSON.stringify({
          capability: refreshedCapability,
          expiresAt: NOW_SECONDS + 200,
          role: "writer",
        }),
        { headers: { "content-type": "application/json" } },
      ),
    );
    await Promise.resolve();
    expect(manager.capability).toBe(originalCapability);
  });
});
