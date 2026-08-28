import type { TelemetrySink } from "@zhongduan/telemetry";

import type { TerminalSession } from "../session";
import {
  BootstrapTokenUnavailableError,
  CapabilityRenewalTimeoutError,
} from "./capability-manager";
import { CloudApiError } from "./cloud-api";
import type { SnapshotPublisherLike } from "./delivery-scheduler";
import { HostRelayConnection } from "./host-relay-connection";
import {
  openHostSocketPair,
  type HostConnectionApi,
  type RelayWebSocketFactory,
} from "./paired-websocket";
import {
  SnapshotPublisher,
  type HostCapabilityProvider,
  type SnapshotUploadApi,
} from "./snapshot-publisher";
import { SnapshotCheckpointCache } from "./snapshot-checkpoint-cache";

export type HostCloudApi = HostConnectionApi & SnapshotUploadApi;

export interface HostCloudRelayOptions {
  api: HostCloudApi;
  capabilities: HostCapabilityProvider;
  degradedReconnectDelayMs?: number;
  maxReconnectDelayMs?: number;
  monotonicNow?: () => number;
  random?: () => number;
  reconnectDelayMs?: number;
  session: TerminalSession;
  sessionId: string;
  snapshotPublisher?: SnapshotPublisherLike;
  stableConnectionMs?: number;
  telemetry?: TelemetrySink;
  webSocketFactory?: RelayWebSocketFactory;
}

export class HostCloudRelay {
  readonly #api: HostCloudApi;
  readonly #capabilities: HostCapabilityProvider;
  readonly #degradedReconnectDelayMs: number;
  readonly #maxReconnectDelayMs: number;
  readonly #monotonicNow: () => number;
  readonly #random: () => number;
  readonly #reconnectDelayMs: number;
  readonly #session: TerminalSession;
  readonly #sessionId: string;
  readonly #snapshotCheckpointCache = new SnapshotCheckpointCache();
  readonly #snapshotPublisher: SnapshotPublisherLike;
  readonly #stableConnectionMs: number;
  readonly #stopController = new AbortController();
  readonly #telemetry: TelemetrySink | undefined;
  readonly #webSocketFactory: RelayWebSocketFactory | undefined;

  #connection: HostRelayConnection | undefined;
  #runPromise: Promise<void> | undefined;
  #resolveFirstReady: (() => void) | undefined;
  #rejectFirstReady: ((error: unknown) => void) | undefined;
  #firstReady: Promise<void> | undefined;

  constructor(options: HostCloudRelayOptions) {
    this.#api = options.api;
    this.#capabilities = options.capabilities;
    this.#degradedReconnectDelayMs = options.degradedReconnectDelayMs ?? 30_000;
    if (!Number.isInteger(this.#degradedReconnectDelayMs) || this.#degradedReconnectDelayMs <= 0) {
      throw new RangeError("degradedReconnectDelayMs must be a positive integer");
    }
    this.#reconnectDelayMs = options.reconnectDelayMs ?? 250;
    if (!Number.isInteger(this.#reconnectDelayMs) || this.#reconnectDelayMs < 0) {
      throw new RangeError("reconnectDelayMs must not be negative");
    }
    this.#maxReconnectDelayMs = options.maxReconnectDelayMs ?? 10_000;
    if (
      !Number.isInteger(this.#maxReconnectDelayMs) ||
      this.#maxReconnectDelayMs < this.#reconnectDelayMs
    ) {
      throw new RangeError("maxReconnectDelayMs must be an integer at least reconnectDelayMs");
    }
    this.#random = options.random ?? Math.random;
    this.#monotonicNow = options.monotonicNow ?? performance.now.bind(performance);
    this.#session = options.session;
    this.#sessionId = options.sessionId;
    this.#snapshotPublisher =
      options.snapshotPublisher ??
      new SnapshotPublisher({
        api: options.api,
        capabilities: options.capabilities,
        sessionId: options.sessionId,
      });
    this.#stableConnectionMs = options.stableConnectionMs ?? 30_000;
    if (!Number.isInteger(this.#stableConnectionMs) || this.#stableConnectionMs <= 0) {
      throw new RangeError("stableConnectionMs must be a positive integer");
    }
    this.#telemetry = options.telemetry;
    this.#webSocketFactory = options.webSocketFactory;
  }

  start(): Promise<void> {
    if (this.#runPromise !== undefined) return this.#firstReady!;
    this.#firstReady = new Promise((resolve, reject) => {
      this.#resolveFirstReady = resolve;
      this.#rejectFirstReady = reject;
    });
    const running = this.#run();
    this.#runPromise = running;
    void running.then(
      () => {
        this.#rejectFirstReady?.(new Error("Host relay stopped before becoming ready"));
      },
      (error: unknown) => {
        this.#rejectFirstReady?.(error);
      },
    );
    return this.#firstReady;
  }

  waitUntilStopped(): Promise<void> {
    return this.#runPromise ?? Promise.resolve();
  }

  async stop(): Promise<void> {
    this.#stopController.abort(new DOMException("Host relay stopped", "AbortError"));
    this.#connection?.close();
    await this.#runPromise;
  }

  async #run(): Promise<void> {
    const signal = this.#stopController.signal;
    let failedAttempts = 0;
    while (!signal.aborted) {
      let capability: string | undefined;
      let degraded = false;
      let readyAt: number | undefined;
      try {
        capability = await this.#capabilities.current(signal);
        const pair = await openHostSocketPair({
          api: this.#api,
          capability,
          sessionId: this.#sessionId,
          signal,
          ...(this.#webSocketFactory === undefined
            ? {}
            : { webSocketFactory: this.#webSocketFactory }),
        });
        const connection = new HostRelayConnection({
          monotonicNow: this.#monotonicNow,
          pair,
          session: this.#session,
          snapshotCheckpointCache: this.#snapshotCheckpointCache,
          snapshotPublisher: this.#snapshotPublisher,
          ...(this.#telemetry === undefined ? {} : { telemetry: this.#telemetry }),
        });
        this.#connection = connection;
        await connection.start();
        readyAt = this.#monotonicNow();
        this.#resolveFirstReady?.();
        this.#resolveFirstReady = undefined;
        this.#rejectFirstReady = undefined;
        await connection.waitClosed();
      } catch (error) {
        if (signal.aborted) break;
        let retryError = error;
        let recoveredCapability = false;
        if (
          error instanceof CloudApiError &&
          (error.status === 401 || error.status === 403) &&
          capability !== undefined
        ) {
          try {
            await this.#capabilities.recoverRejected(capability, signal);
            recoveredCapability = true;
          } catch (recoveryError) {
            retryError = recoveryError;
          }
        }
        if (
          !recoveredCapability &&
          (retryError instanceof BootstrapTokenUnavailableError ||
            retryError instanceof CapabilityRenewalTimeoutError ||
            !isRetriableConnectionError(retryError))
        ) {
          degraded = true;
        }
      } finally {
        this.#connection?.close();
        this.#connection = undefined;
      }
      if (!signal.aborted) {
        const stable =
          readyAt !== undefined && this.#monotonicNow() - readyAt >= this.#stableConnectionMs;
        failedAttempts = Math.min((stable ? 0 : failedAttempts) + 1, 30);
        const exponential = Math.min(
          this.#maxReconnectDelayMs,
          this.#reconnectDelayMs * 2 ** Math.max(0, failedAttempts - 1),
        );
        const jitter = 0.8 + Math.min(1, Math.max(0, this.#random())) * 0.4;
        const retryDelay = Math.max(
          degraded ? this.#degradedReconnectDelayMs : 0,
          Math.min(this.#maxReconnectDelayMs, Math.max(0, Math.floor(exponential * jitter))),
        );
        try {
          await delay(retryDelay, signal);
        } catch {
          if (!signal.aborted) throw new Error("Host relay reconnect delay failed");
        }
      }
    }
  }
}

function isRetriableConnectionError(error: unknown): boolean {
  if (!(error instanceof CloudApiError)) return true;
  return error.status === 409 || error.status === 429 || error.status >= 500;
}

function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (milliseconds === 0) return Promise.resolve();
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    const onAbort = () => {
      clearTimeout(timeout);
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}
