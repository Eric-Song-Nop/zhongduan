import type { CapabilityResponse } from "@zhongduan/protocol";

import { CloudApiClient, CloudApiError } from "./cloud-api";

export interface HostCapabilityApi {
  reclaimHostCapability: CloudApiClient["reclaimHostCapability"];
  refreshCapability: CloudApiClient["refreshCapability"];
}

export interface HostCapabilityManagerOptions {
  api: HostCapabilityApi;
  bootstrapToken: string | BootstrapTokenProvider;
  engineId: string;
  initialCapability?: string;
  initialExpiresAt?: number;
  nowSeconds?: () => number;
  random?: () => number;
  renewalTimeoutMs?: number;
  sessionEpoch: bigint;
  sessionId: string;
}

export type BootstrapTokenProvider = (signal?: AbortSignal) => string | Promise<string>;

export const HOST_CAPABILITY_RENEWAL_TIMEOUT_MS = 10_000;

export class BootstrapTokenUnavailableError extends Error {
  constructor(cause: unknown) {
    super("bootstrap token is unavailable", { cause });
    this.name = "BootstrapTokenUnavailableError";
  }
}

export class CapabilityRenewalTimeoutError extends Error {
  constructor() {
    super("host capability renewal timed out");
    this.name = "CapabilityRenewalTimeoutError";
  }
}

export class HostCapabilityManager {
  readonly #api: HostCapabilityApi;
  readonly #bootstrapToken: BootstrapTokenProvider;
  readonly #engineId: string;
  readonly #nowSeconds: () => number;
  readonly #random: () => number;
  readonly #renewalTimeoutMs: number;
  readonly #sessionEpoch: bigint;
  readonly #sessionId: string;
  readonly #lifetimeAbort = new AbortController();

  #capability: string | undefined;
  #expiresAt: number | undefined;
  #refreshAt = 0;
  #renewal: Promise<string> | undefined;
  #renewalGeneration = 0;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #disposed = false;

  constructor(options: HostCapabilityManagerOptions) {
    this.#api = options.api;
    this.#bootstrapToken =
      typeof options.bootstrapToken === "string"
        ? () => options.bootstrapToken as string
        : options.bootstrapToken;
    this.#engineId = options.engineId;
    this.#nowSeconds = options.nowSeconds ?? (() => Math.floor(Date.now() / 1000));
    this.#random = options.random ?? Math.random;
    this.#renewalTimeoutMs = positiveInteger(
      options.renewalTimeoutMs ?? HOST_CAPABILITY_RENEWAL_TIMEOUT_MS,
      "renewalTimeoutMs",
    );
    this.#sessionEpoch = options.sessionEpoch;
    this.#sessionId = options.sessionId;
    if ((options.initialCapability === undefined) !== (options.initialExpiresAt === undefined)) {
      throw new TypeError("initial capability and expiry must be provided together");
    }
    if (options.initialCapability !== undefined && options.initialExpiresAt !== undefined) {
      this.#install({
        capability: options.initialCapability,
        expiresAt: options.initialExpiresAt,
        role: "host",
      });
    }
  }

  async current(signal?: AbortSignal): Promise<string> {
    this.#assertActive();
    const now = this.#nowSeconds();
    if (this.#capability !== undefined && this.#expiresAt !== undefined && now < this.#refreshAt) {
      return this.#capability;
    }
    try {
      return await this.#renew(signal);
    } catch (error) {
      signal?.throwIfAborted();
      this.#assertActive();
      const afterRenewal = this.#nowSeconds();
      if (
        this.#capability !== undefined &&
        this.#expiresAt !== undefined &&
        afterRenewal < this.#expiresAt
      ) {
        this.#scheduleRetry();
        return this.#capability;
      }
      throw error;
    }
  }

  async recoverRejected(rejectedCapability: string, signal?: AbortSignal): Promise<string> {
    this.#assertActive();
    if (this.#capability !== undefined && this.#capability !== rejectedCapability) {
      return this.#capability;
    }
    return this.#renew(signal);
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#lifetimeAbort.abort(new DOMException("capability manager disposed", "AbortError"));
  }

  #renew(signal?: AbortSignal): Promise<string> {
    if (this.#renewal !== undefined) return raceAbort(this.#renewal, signal);
    const generation = ++this.#renewalGeneration;
    const operation = this.#performBoundedRenewal(generation);
    this.#renewal = operation;
    void operation.then(
      () => {
        if (this.#renewal === operation) this.#renewal = undefined;
      },
      () => {
        if (this.#renewal === operation) this.#renewal = undefined;
      },
    );
    return raceAbort(operation, signal);
  }

  async #performBoundedRenewal(generation: number): Promise<string> {
    const deadline = new AbortController();
    const timeout = setTimeout(
      () => deadline.abort(new CapabilityRenewalTimeoutError()),
      this.#renewalTimeoutMs,
    );
    const signal = AbortSignal.any([this.#lifetimeAbort.signal, deadline.signal]);
    try {
      return await raceAbort(this.#performRenewal(signal, generation), signal);
    } finally {
      clearTimeout(timeout);
    }
  }

  async #performRenewal(signal: AbortSignal, generation: number): Promise<string> {
    signal.throwIfAborted();
    let response: CapabilityResponse;
    if (this.#capability === undefined) {
      response = await this.#reclaim(signal);
    } else {
      try {
        response = await this.#api.refreshCapability(this.#sessionId, this.#capability, signal);
      } catch (error) {
        if (!(error instanceof CloudApiError) || (error.status !== 401 && error.status !== 403)) {
          throw error;
        }
        response = await this.#reclaim(signal);
      }
    }
    this.#assertInstallable(generation, signal);
    this.#install(response);
    return response.capability;
  }

  async #reclaim(signal: AbortSignal): Promise<CapabilityResponse> {
    signal.throwIfAborted();
    let bootstrapToken: string;
    try {
      bootstrapToken = await this.#bootstrapToken(signal);
    } catch (error) {
      signal.throwIfAborted();
      throw new BootstrapTokenUnavailableError(error);
    }
    signal.throwIfAborted();
    if (bootstrapToken.length === 0) {
      throw new BootstrapTokenUnavailableError(new Error("bootstrap token is empty"));
    }
    const response = await this.#api.reclaimHostCapability(
      this.#sessionId,
      bootstrapToken,
      this.#engineId,
      this.#sessionEpoch,
      signal,
    );
    signal.throwIfAborted();
    return response;
  }

  #install(response: CapabilityResponse): void {
    if (response.role !== "host") throw new Error("cloud API returned a non-host capability");
    const now = this.#nowSeconds();
    if (response.expiresAt <= now) throw new Error("cloud API returned an expired capability");
    const lifetime = response.expiresAt - now;
    const jitteredHalfLife = 0.45 + Math.min(1, Math.max(0, this.#random())) * 0.1;
    this.#capability = response.capability;
    this.#expiresAt = response.expiresAt;
    this.#refreshAt = now + Math.max(1, Math.floor(lifetime * jitteredHalfLife));
    this.#schedule(this.#refreshAt - now);
  }

  #schedule(delaySeconds: number): void {
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    if (this.#disposed) return;
    this.#timer = setTimeout(
      () => {
        this.#timer = undefined;
        void this.#renew().catch(() => this.#scheduleRetry());
      },
      Math.max(1, delaySeconds) * 1000,
    );
  }

  #scheduleRetry(): void {
    if (this.#disposed) return;
    const now = this.#nowSeconds();
    const remaining = this.#expiresAt === undefined ? 30 : Math.max(1, this.#expiresAt - now);
    this.#schedule(Math.min(30, remaining));
  }

  #assertActive(): void {
    if (this.#disposed) throw new Error("host capability manager is disposed");
  }

  #assertInstallable(generation: number, signal: AbortSignal): void {
    signal.throwIfAborted();
    this.#assertActive();
    if (generation !== this.#renewalGeneration) {
      throw new DOMException("capability renewal was superseded", "AbortError");
    }
  }
}

function raceAbort<T>(operation: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return operation;
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    void operation.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

function positiveInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value <= 0) throw new RangeError(`${label} must be positive`);
  return value;
}
