import type { CapabilityResponse } from "@zhongduan/protocol";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  CapabilityRenewalTimeoutError,
  HostCapabilityManager,
  type HostCapabilityApi,
} from "./capability-manager";
import { CloudApiError, CloudTransportError } from "./cloud-api";

function response(capability: string, expiresAt: number): CapabilityResponse {
  return { capability, expiresAt, role: "host" };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

function manager(
  api: HostCapabilityApi,
  nowSeconds: () => number,
  renewalTimeoutMs?: number,
): HostCapabilityManager {
  return new HostCapabilityManager({
    api,
    bootstrapToken: "bootstrap-secret",
    engineId: "ghostty/test",
    initialCapability: "host-cap-1",
    initialExpiresAt: 200,
    nowSeconds,
    random: () => 0.5,
    ...(renewalTimeoutMs === undefined ? {} : { renewalTimeoutMs }),
    sessionEpoch: 7n,
    sessionId: "session_AAAAAAAAA",
  });
}

afterEach(() => vi.useRealTimers());

describe("HostCapabilityManager", () => {
  it("re-reads the bootstrap token provider after credentials rotate", async () => {
    let bootstrapToken = "expired-bootstrap";
    const api: HostCapabilityApi = {
      refreshCapability: vi.fn(),
      reclaimHostCapability: vi.fn(async (_sessionId, token) => {
        if (token !== "rotated-bootstrap") {
          throw new CloudApiError(401, "invalid-bootstrap");
        }
        return response("host-cap-2", 300);
      }),
    };
    const capabilities = new HostCapabilityManager({
      api,
      bootstrapToken: () => bootstrapToken,
      engineId: "ghostty/test",
      nowSeconds: () => 100,
      random: () => 0.5,
      sessionEpoch: 7n,
      sessionId: "session_AAAAAAAAA",
    });
    try {
      await expect(capabilities.current()).rejects.toBeInstanceOf(CloudApiError);
      bootstrapToken = "rotated-bootstrap";
      await expect(capabilities.current()).resolves.toBe("host-cap-2");
      expect(api.reclaimHostCapability).toHaveBeenNthCalledWith(
        2,
        "session_AAAAAAAAA",
        "rotated-bootstrap",
        "ghostty/test",
        7n,
        expect.any(AbortSignal),
      );
    } finally {
      capabilities.dispose();
    }
  });

  it.each([401, 403])(
    "refreshes at jittered half-life and reclaims exact authority after a %s",
    async (status) => {
      let now = 100;
      const api: HostCapabilityApi = {
        refreshCapability: vi
          .fn()
          .mockRejectedValueOnce(new CloudApiError(status, "expired-capability")),
        reclaimHostCapability: vi.fn(async () => response("host-cap-2", 300)),
      };
      const capabilities = manager(api, () => now);
      try {
        now = 149;
        await expect(capabilities.current()).resolves.toBe("host-cap-1");
        now = 150;
        await expect(capabilities.current()).resolves.toBe("host-cap-2");

        expect(api.refreshCapability).toHaveBeenCalledWith(
          "session_AAAAAAAAA",
          "host-cap-1",
          expect.any(AbortSignal),
        );
        expect(api.reclaimHostCapability).toHaveBeenCalledWith(
          "session_AAAAAAAAA",
          "bootstrap-secret",
          "ghostty/test",
          7n,
          expect.any(AbortSignal),
        );
      } finally {
        capabilities.dispose();
      }
    },
  );

  it("does not return an old capability when a slow refresh crosses expiry", async () => {
    let now = 100;
    const renewal = deferred<CapabilityResponse>();
    const api: HostCapabilityApi = {
      refreshCapability: vi.fn(() => renewal.promise),
      reclaimHostCapability: vi.fn(),
    };
    const capabilities = manager(api, () => now);
    now = 150;
    const pending = capabilities.current();
    const rejected = expect(pending).rejects.toBeInstanceOf(CloudTransportError);
    now = 201;
    renewal.reject(new CloudTransportError(new Error("offline")));

    await rejected;
    capabilities.dispose();
  });

  it("shares one renewal between proactive and rejected-capability callers", async () => {
    const renewal = deferred<CapabilityResponse>();
    const api: HostCapabilityApi = {
      refreshCapability: vi.fn(() => renewal.promise),
      reclaimHostCapability: vi.fn(),
    };
    let now = 100;
    const capabilities = manager(api, () => now);
    now = 150;
    const proactive = capabilities.current();
    const rejected = capabilities.recoverRejected("host-cap-1");
    renewal.resolve(response("host-cap-2", 300));

    await expect(Promise.all([proactive, rejected])).resolves.toEqual(["host-cap-2", "host-cap-2"]);
    expect(api.refreshCapability).toHaveBeenCalledOnce();
    capabilities.dispose();
  });

  it("does not let one caller abort cancel the shared renewal", async () => {
    const renewal = deferred<CapabilityResponse>();
    const api: HostCapabilityApi = {
      refreshCapability: vi.fn(() => renewal.promise),
      reclaimHostCapability: vi.fn(),
    };
    let now = 100;
    const capabilities = manager(api, () => now);
    now = 150;
    const caller = new AbortController();
    const abandoned = capabilities.current(caller.signal);
    const rejected = expect(abandoned).rejects.toMatchObject({ name: "AbortError" });
    const shared = capabilities.recoverRejected("host-cap-1");

    caller.abort(new DOMException("caller stopped waiting", "AbortError"));
    await rejected;
    renewal.resolve(response("host-cap-2", 300));
    await expect(shared).resolves.toBe("host-cap-2");
    expect(api.refreshCapability).toHaveBeenCalledOnce();

    now = 151;
    await expect(capabilities.current()).resolves.toBe("host-cap-2");
    capabilities.dispose();
  });

  it("bounds a bootstrap provider that ignores its abort signal", async () => {
    vi.useFakeTimers();
    const token = deferred<string>();
    const api: HostCapabilityApi = {
      refreshCapability: vi.fn(),
      reclaimHostCapability: vi.fn(),
    };
    let providerSignal: AbortSignal | undefined;
    const capabilities = new HostCapabilityManager({
      api,
      bootstrapToken: (signal) => {
        providerSignal = signal;
        return token.promise;
      },
      engineId: "ghostty/test",
      nowSeconds: () => 100,
      random: () => 0.5,
      renewalTimeoutMs: 25,
      sessionEpoch: 7n,
      sessionId: "session_AAAAAAAAA",
    });
    const pending = capabilities.current();
    const rejected = expect(pending).rejects.toBeInstanceOf(CapabilityRenewalTimeoutError);

    await vi.advanceTimersByTimeAsync(25);
    await rejected;
    expect(providerSignal?.aborted).toBe(true);
    token.resolve("late-bootstrap");
    await Promise.resolve();
    expect(api.reclaimHostCapability).not.toHaveBeenCalled();
    capabilities.dispose();
  });

  it("falls back before expiry and rejects after expiry when renewal reaches its deadline", async () => {
    vi.useFakeTimers();
    let now = 100;
    const api: HostCapabilityApi = {
      refreshCapability: vi.fn(() => new Promise<CapabilityResponse>(() => {})),
      reclaimHostCapability: vi.fn(),
    };
    const capabilities = manager(api, () => now, 25);
    now = 150;
    const beforeExpiry = capabilities.current();
    await vi.advanceTimersByTimeAsync(25);
    await expect(beforeExpiry).resolves.toBe("host-cap-1");

    now = 201;
    const afterExpiry = capabilities.current();
    const rejected = expect(afterExpiry).rejects.toBeInstanceOf(CapabilityRenewalTimeoutError);
    await vi.advanceTimersByTimeAsync(25);
    await rejected;
    expect(api.refreshCapability).toHaveBeenCalledTimes(2);
    capabilities.dispose();
  });

  it("does not install a late result from a timed-out renewal generation", async () => {
    vi.useFakeTimers();
    let now = 100;
    const late = deferred<CapabilityResponse>();
    const api: HostCapabilityApi = {
      refreshCapability: vi
        .fn<HostCapabilityApi["refreshCapability"]>()
        .mockImplementationOnce(() => late.promise)
        .mockResolvedValueOnce(response("host-cap-2", 300)),
      reclaimHostCapability: vi.fn(),
    };
    const capabilities = manager(api, () => now, 25);
    now = 150;
    const first = capabilities.current();
    await vi.advanceTimersByTimeAsync(25);
    await expect(first).resolves.toBe("host-cap-1");
    await expect(capabilities.current()).resolves.toBe("host-cap-2");

    late.resolve(response("late-host-cap", 400));
    await Promise.resolve();
    now = 151;
    await expect(capabilities.current()).resolves.toBe("host-cap-2");
    capabilities.dispose();
  });

  it("aborts a timer-started refresh when disposed", async () => {
    vi.useFakeTimers();
    let refreshSignal: AbortSignal | undefined;
    const api: HostCapabilityApi = {
      refreshCapability: vi.fn((_sessionId: string, _capability: string, signal?: AbortSignal) => {
        refreshSignal = signal;
        return new Promise<CapabilityResponse>((_resolve, reject) => {
          signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      }),
      reclaimHostCapability: vi.fn(),
    };
    const capabilities = manager(api, () => 100);

    await vi.advanceTimersByTimeAsync(50_000);
    expect(refreshSignal?.aborted).toBe(false);
    capabilities.dispose();
    expect(refreshSignal?.aborted).toBe(true);
    await Promise.resolve();
  });
});
