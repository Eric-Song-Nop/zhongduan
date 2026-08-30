import {
  DELIVERY_ENVELOPE_HEADER_BYTES,
  DataFrameFlag,
  DataFrameKind,
  decodeDataFrame,
  decodeDeliveryEnvelope,
  decodeServerControlFrame,
  encodeDataFrame,
  encodeDeliveryEnvelope,
  type ConnectionSetResponse,
  type RecoveryStart,
  type RecoveryProgressFrame,
  type ReplicaCursor,
  type ResizePayload,
} from "@zhongduan/protocol";
import {
  RecoveryRuntime,
  type RecoveryRuntimeFailure,
  type ReplicaHost,
  type ReplicaSink,
  type SnapshotRestoreSource,
  type SnapshotTransport,
} from "@zhongduan/session-client";
import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";

import { FakeTerminalAuthority } from "../../host-daemon/src/fake-terminal-authority";
import { EventJournal } from "../../host-daemon/src/journal";
import type { PtyProcess } from "../../host-daemon/src/pty-process";
import { TerminalSession } from "../../host-daemon/src/session";
import { HostRelayConnection } from "../../host-daemon/src/cloud/host-relay-connection";
import type { HostSocketPair } from "../../host-daemon/src/cloud/paired-websocket";
import {
  RecoverySourceManager,
  type RecoverySourceManagerLimits,
} from "../../host-daemon/src/cloud/recovery-source-manager";
import { RelayRecoveryStore } from "../src/worker/relay-recovery-store";
import { SocketAttachmentSchema } from "../src/worker/relay-socket";
import { RelayDeliveryRing } from "../src/worker/relay-delivery-ring";
import { RelayDeliveryScheduler } from "../src/worker/relay-delivery-scheduler";
import { installMiniflareMultipartEtagShim, uploadSnapshot } from "./snapshot-test-helpers";

export interface CreatedThreeOwnerSession {
  hostCapability: string;
  observerCapability: string;
  sessionId: string;
  writerCapability: string;
}

export type LiteralMutation =
  | { readonly type: "output"; readonly bytes: Uint8Array }
  | { readonly type: "resize"; readonly dimensions: ResizePayload };

export interface LiteralOracleState {
  readonly eventSeq: bigint;
  readonly nextPtyOffset: bigint;
  readonly output: Uint8Array;
  readonly resizes: readonly ResizePayload[];
  readonly timeline: readonly LiteralMutation[];
}

export interface ProgressAttempt {
  readonly encoded: string;
  readonly frame: RecoveryProgressFrame;
  readonly sent: boolean;
}

interface OpenHost {
  readonly connection: HostRelayConnection;
  readonly pair: HostSocketPair;
}

interface BrowserFaults {
  dropFinalRecoveryReceipt?: boolean;
  dropFirstAdopted?: boolean;
  holdStart?: boolean;
}

interface HeldStart {
  readonly frame: RecoveryStart;
  readonly release: () => void;
}

const origin = "https://terminal.example.test";
const engineId = "fake-terminal-authority/v1";
const epoch = 7n;
const recoveryLimits: RecoverySourceManagerLimits = {
  maxCanonicalBytesPerSource: 256 * 1024,
  maxCanonicalFramesPerSource: 512,
  maxOwnedRecords: 1_024,
  maxOwnedWireBytes: 2 * 1024 * 1024,
  maxSources: 16,
  noProgressDeadlineMs: 15_000,
  recoveryDeadlineMs: 60_000,
};

export const recoveryAssemblerLimits = {
  maxApplyFramesPerCall: 64,
  maxGapSpan: 1_024n,
  maxOwnedBytes: 2 * 1024 * 1024,
  maxOwnedFrames: 1_024,
  noProgressDeadlineMs: 15_000,
  recoveryDeadlineMs: 60_000,
} as const;

class ManualPty implements PtyProcess {
  readonly pid = 26;
  readonly writes: Uint8Array[] = [];
  #dataListener: ((data: Uint8Array) => void) | undefined;
  #exitListener: ((exitCode: number, signal: number) => void) | undefined;

  onData(listener: (data: Uint8Array) => void): () => void {
    this.#dataListener = listener;
    return () => {
      if (this.#dataListener === listener) this.#dataListener = undefined;
    };
  }

  onExit(listener: (exitCode: number, signal: number) => void): () => void {
    this.#exitListener = listener;
    return () => {
      if (this.#exitListener === listener) this.#exitListener = undefined;
    };
  }

  write(data: Uint8Array): void {
    this.writes.push(data.slice());
  }

  resize(_dimensions: ResizePayload): void {}

  kill(): void {
    this.#exitListener?.(0, 0);
  }

  emit(data: Uint8Array): void {
    this.#dataListener?.(data.slice());
  }
}

class ManualRuntimeTimers {
  #now = 0;
  readonly #timers = new Map<
    ReturnType<typeof setTimeout>,
    { readonly at: number; readonly callback: () => void }
  >();

  readonly setTimer = (callback: () => void, delayMs: number): ReturnType<typeof setTimeout> => {
    const handle = setTimeout(() => undefined, 2_147_483_647);
    clearTimeout(handle);
    this.#timers.set(handle, { at: this.#now + delayMs, callback });
    return handle;
  };

  readonly clearTimer = (handle: ReturnType<typeof setTimeout>): void => {
    this.#timers.delete(handle);
  };

  runNext(): boolean {
    const next = [...this.#timers.entries()].sort(([, left], [, right]) => left.at - right.at)[0];
    if (next === undefined) return false;
    const [handle, timer] = next;
    this.#timers.delete(handle);
    this.#now = timer.at;
    timer.callback();
    return true;
  }

  clear(): void {
    this.#timers.clear();
  }
}

export class LiteralReplicaSink implements ReplicaSink {
  readonly engineId = engineId;
  readonly mutations: LiteralMutation[];
  disposed = false;

  constructor(mutations: readonly LiteralMutation[] = []) {
    this.mutations = mutations.map(cloneMutation);
  }

  writePty(data: Uint8Array): void {
    if (this.disposed) throw new Error("literal replica is disposed");
    this.mutations.push({ type: "output", bytes: data.slice() });
  }

  resize(dimensions: ResizePayload): void {
    if (this.disposed) throw new Error("literal replica is disposed");
    this.mutations.push({ type: "resize", dimensions: { ...dimensions } });
  }

  dispose(): void {
    this.disposed = true;
  }
}

export class LiteralReplicaHost implements ReplicaHost {
  readonly engineId = engineId;
  #active: LiteralReplicaSink | null;
  #restoreGate: ReturnType<typeof deferred<void>> | null = null;
  #restoreLoaded: ReturnType<typeof deferred<void>> | null = null;

  constructor(active: LiteralReplicaSink | null = null) {
    this.#active = active;
  }

  get active(): LiteralReplicaSink | null {
    return this.#active;
  }

  holdRestore(): void {
    if (this.#restoreGate !== null) throw new Error("restore is already held");
    this.#restoreGate = deferred<void>();
    this.#restoreLoaded = deferred<void>();
  }

  async waitForRestoreLoad(): Promise<void> {
    const loaded = this.#restoreLoaded;
    if (loaded === null) throw new Error("restore is not held");
    await loaded.promise;
  }

  releaseRestore(): void {
    const gate = this.#restoreGate;
    if (gate === null) throw new Error("restore is not held");
    this.#restoreGate = null;
    gate.resolve();
  }

  async restore(
    snapshot: Uint8Array,
    source: SnapshotRestoreSource,
    signal: AbortSignal,
  ): Promise<ReplicaSink> {
    signal.throwIfAborted();
    if (source.engineId !== engineId) throw new Error("literal snapshot engine mismatch");
    const decoded = JSON.parse(new TextDecoder().decode(snapshot)) as {
      engineId?: unknown;
      mutations?: unknown;
      version?: unknown;
    };
    if (
      decoded.version !== 1 ||
      decoded.engineId !== engineId ||
      !Array.isArray(decoded.mutations)
    ) {
      throw new Error("invalid literal snapshot");
    }
    const mutations = decoded.mutations.map(parseSnapshotMutation);
    this.#restoreLoaded?.resolve();
    const gate = this.#restoreGate;
    if (gate !== null) await waitForPromiseOrAbort(gate.promise, signal);
    signal.throwIfAborted();
    return new LiteralReplicaSink(mutations);
  }

  adopt(replica: ReplicaSink, _cursor: ReplicaCursor): void {
    if (!(replica instanceof LiteralReplicaSink)) {
      throw new Error("literal host received a foreign replica");
    }
    this.#active?.dispose();
    this.#active = replica;
  }
}

export interface OpenThreeOwnerBrowser {
  readonly clientId: string;
  readonly connection: ConnectionSetResponse;
  readonly control: WebSocket;
  readonly data: WebSocket;
  readonly failures: RecoveryRuntimeFailure[];
  readonly host: LiteralReplicaHost;
  readonly progress: ProgressAttempt[];
  readonly runtime: RecoveryRuntime;
  readonly timers: ManualRuntimeTimers;
  readonly trace: string[];
  readonly waitForHeldStart: () => Promise<RecoveryStart>;
  readonly releaseStart: () => void;
  close(): void;
}

export interface OpenRawRecoveryBrowser {
  readonly clientId: string;
  readonly connection: ConnectionSetResponse;
  readonly control: WebSocket;
  readonly data: WebSocket;
  readonly start: RecoveryStart;
}

type RecoveryServerControlFrame = ReturnType<typeof decodeServerControlFrame>;

type AttemptRow = Record<string, SqlStorageValue> & {
  readonly adopted_json: string | null;
  readonly recovery_id: string;
  readonly reset_reason: string | null;
  readonly source_closed_json: string | null;
  readonly state: string;
};

export interface HeldHostSourceClosed {
  readonly isHeld: () => boolean;
  readonly publish: () => void;
  readonly wait: () => Promise<string>;
}

export class ThreeOwnerHarness {
  readonly authority = new FakeTerminalAuthority();
  readonly pty = new ManualPty();
  readonly session: TerminalSession;
  readonly recoverySources: RecoverySourceManager;
  readonly cloud: CreatedThreeOwnerSession;
  readonly sockets = new Set<WebSocket>();
  readonly browsers = new Set<OpenThreeOwnerBrowser>();
  readonly rawBrowsers = new Set<OpenRawRecoveryBrowser>();
  readonly hosts: OpenHost[] = [];
  readonly hostControlTrace: string[] = [];
  readonly hostCloseTrace: Array<{ readonly code: number; readonly reason: string }> = [];
  #closed = false;

  private constructor(cloud: CreatedThreeOwnerSession) {
    this.cloud = cloud;
    this.session = new TerminalSession({
      authority: this.authority,
      journal: new EventJournal(),
      pty: this.pty,
      sessionEpoch: epoch,
    });
    this.recoverySources = new RecoverySourceManager({
      limits: recoveryLimits,
      monotonicNow: () => performance.now(),
      session: this.session,
    });
  }

  static async create(): Promise<ThreeOwnerHarness> {
    const cloud = await createCloudSession();
    const harness = new ThreeOwnerHarness(cloud);
    await harness.openHost();
    return harness;
  }

  get stub() {
    return env.TERMINAL_SESSIONS.get(
      env.TERMINAL_SESSIONS.idFromName(`v1:${this.cloud.sessionId}`),
    );
  }

  get currentHost(): OpenHost {
    const host = this.hosts.at(-1);
    if (host === undefined) throw new Error("three-owner Host is not open");
    return host;
  }

  async openHost(): Promise<OpenHost> {
    const connection = await createConnectionSet(this.cloud.sessionId, this.cloud.hostCapability);
    const control = await this.upgrade("control", connection.controlTicket);
    const data = await this.upgrade("data", connection.dataTicket);
    // The workerd hibernation-test client WebSocket omits the standard outbound buffer metric.
    // This harness does not model client-side transport backpressure (covered by the P2.5b
    // transport gate); a fixed zero only preserves the production finite-metric guard while
    // socket/DO delivery remains real.
    if (typeof data.bufferedAmount !== "number") {
      Object.defineProperty(data, "bufferedAmount", { configurable: true, value: 0 });
    }
    control.addEventListener("message", (event) => {
      if (typeof event.data === "string") this.hostControlTrace.push(event.data);
    });
    const pair: HostSocketPair = {
      connection,
      control,
      data,
      close: (code = 1000, reason = "three-owner Host closed") => {
        this.hostCloseTrace.push({ code, reason });
        closeSocket(data, code, reason);
        closeSocket(control, code, reason);
      },
    };
    const host = new HostRelayConnection({
      heartbeatIntervalMs: 60_000,
      heartbeatTimeoutMs: 120_000,
      pair,
      recoveryDeadlineTickMs: 60_000,
      recoverySourceManager: this.recoverySources,
      session: this.session,
    });
    await host.start();
    const opened = { connection: host, pair };
    this.hosts.push(opened);
    return opened;
  }

  async publishBaseSnapshot(
    snapshotId: string,
    mutations: readonly LiteralMutation[],
  ): Promise<Uint8Array> {
    const cursor = literalOracle(mutations);
    const body = new TextEncoder().encode(
      JSON.stringify({
        version: 1,
        engineId,
        mutations: mutations.map(serializeSnapshotMutation),
      }),
    );
    await installMiniflareMultipartEtagShim(this.cloud.sessionId);
    const response = await uploadSnapshot(this.cloud, snapshotId, this.cloud.hostCapability, {
      body,
      engineId,
      cutEventSeq: cursor.eventSeq.toString(),
      nextPtyOffset: cursor.nextPtyOffset.toString(),
      sessionEpoch: epoch.toString(),
    });
    if (response.status !== 201 && response.status !== 200) {
      throw new Error(`snapshot upload failed with ${response.status}`);
    }
    return body;
  }

  emit(mutation: LiteralMutation): void {
    if (mutation.type === "output") this.pty.emit(mutation.bytes);
    else this.session.resize(mutation.dimensions);
  }

  async waitForCloudCursor(expected: LiteralOracleState): Promise<void> {
    await waitForCondition(
      () =>
        runInDurableObject(this.stub, (_instance, state) => {
          const cursor = state.storage.sql
            .exec<{ head_event_seq: string; next_pty_offset: string }>(
              "SELECT head_event_seq, next_pty_offset FROM session_state WHERE singleton = 1",
            )
            .one();
          return (
            cursor.head_event_seq === expected.eventSeq.toString() &&
            cursor.next_pty_offset === expected.nextPtyOffset.toString()
          );
        }),
      `Cloud cursor ${expected.eventSeq}/${expected.nextPtyOffset}`,
    );
  }

  async openBrowser(
    options: {
      readonly capability?: string;
      readonly clientId?: string;
      readonly faults?: BrowserFaults;
      readonly host?: LiteralReplicaHost;
      readonly initialCursor?: ReplicaCursor;
    } = {},
  ): Promise<OpenThreeOwnerBrowser> {
    const capability = options.capability ?? this.cloud.observerCapability;
    const connection = await createConnectionSet(
      this.cloud.sessionId,
      capability,
      options.clientId,
    );
    if (connection.clientId === null) throw new Error("Browser connection lacks client identity");
    const control = await this.upgrade("control", connection.controlTicket);
    const data = await this.upgrade("data", connection.dataTicket);
    const replicaHost = options.host ?? new LiteralReplicaHost();
    const progress: ProgressAttempt[] = [];
    const failures: RecoveryRuntimeFailure[] = [];
    const trace: string[] = [];
    const timers = new ManualRuntimeTimers();
    let dropRecoveryReceipt = options.faults?.dropFinalRecoveryReceipt === true;
    let finalRecoveryDeliveryOrdinal: string | null = null;
    let dropAdopted = options.faults?.dropFirstAdopted === true;
    const heldStartReady = deferred<RecoveryStart>();
    let heldStart: HeldStart | null = null;

    const snapshots: SnapshotTransport = {
      load: async (manifest, signal) => {
        signal.throwIfAborted();
        const response = await workerExports.default.fetch(
          new Request(new URL(manifest.downloadPath, origin), {
            headers: { authorization: `Bearer ${capability}` },
            signal,
          }),
        );
        if (!response.ok) throw new Error(`snapshot download failed with ${response.status}`);
        const bytes = new Uint8Array(await response.arrayBuffer());
        signal.throwIfAborted();
        return bytes;
      },
    };

    const runtime = new RecoveryRuntime({
      deliveryGeneration: BigInt(connection.deliveryGeneration),
      engineId,
      host: replicaHost,
      ...(options.initialCursor === undefined ? {} : { initialCursor: options.initialCursor }),
      limits: recoveryAssemblerLimits,
      snapshots,
      streamId: connection.streamId,
      progressRetryMs: 5,
      setTimer: timers.setTimer,
      clearTimer: timers.clearTimer,
      onFailure: (reason) => failures.push(reason),
      onProgress: (frame) => {
        const encoded = JSON.stringify(frame);
        const shouldDrop =
          (dropRecoveryReceipt &&
            frame.type === "delivery-received" &&
            frame.lane === "recovery" &&
            frame.contiguousDeliveryOrdinal === finalRecoveryDeliveryOrdinal) ||
          (dropAdopted && frame.type === "recovery-adopted");
        if (shouldDrop) {
          if (frame.type === "recovery-adopted") dropAdopted = false;
          else dropRecoveryReceipt = false;
          progress.push({ encoded, frame, sent: false });
          trace.push(`progress:dropped:${progressKey(frame)}`);
          return;
        }
        progress.push({ encoded, frame, sent: true });
        trace.push(`progress:sent:${progressKey(frame)}`);
        control.send(encoded);
      },
    });

    control.addEventListener("message", (event) => {
      if (typeof event.data !== "string") return;
      let frame: RecoveryServerControlFrame;
      try {
        frame = decodeServerControlFrame(event.data);
      } catch {
        return;
      }
      trace.push(`control:${frame.type}`);
      if (frame.type === "recovery-start") {
        if (options.faults?.holdStart === true && heldStart === null) {
          const start = frame;
          heldStart = { frame: start, release: () => runtime.acceptStart(start) };
          heldStartReady.resolve(start);
        } else {
          runtime.acceptStart(frame);
        }
      } else if (frame.type === "recovery-source-closed") {
        runtime.acceptSourceClosed(frame);
      }
    });
    data.addEventListener("message", (event) => {
      void binaryEventBytes(event).then((bytes) => {
        const envelope = decodeDeliveryEnvelope(bytes);
        trace.push(`data:${envelope.lane}:${envelope.deliveryOrdinal}`);
        if (
          envelope.lane === "recovery" &&
          decodeDataFrame(envelope.payload).kind === DataFrameKind.RecoveryDone
        ) {
          finalRecoveryDeliveryOrdinal = envelope.deliveryOrdinal.toString();
        }
        runtime.acceptEnvelope(bytes);
      });
    });

    const browser: OpenThreeOwnerBrowser = {
      clientId: connection.clientId,
      connection,
      control,
      data,
      failures,
      host: replicaHost,
      progress,
      runtime,
      timers,
      trace,
      waitForHeldStart: () => heldStartReady.promise,
      releaseStart: () => {
        const start = heldStart;
        if (start === null) throw new Error("RecoveryStart is not held");
        heldStart = null;
        start.release();
      },
      close: () => {
        runtime.close();
        timers.clear();
        closeSocket(data, 1000, "three-owner Browser closed");
        closeSocket(control, 1000, "three-owner Browser closed");
      },
    };
    this.browsers.add(browser);
    control.send(
      JSON.stringify({
        type: "attach",
        engineId,
        deliveryGeneration: connection.deliveryGeneration,
        hasLiveReplica: options.initialCursor !== undefined,
        ...(options.initialCursor === undefined
          ? {}
          : {
              lastSessionEpoch: options.initialCursor.sessionEpoch.toString(),
              lastEventSeq: options.initialCursor.lastEventSeq.toString(),
              nextPtyOffset: options.initialCursor.nextPtyOffset.toString(),
            }),
      }),
    );
    return browser;
  }

  async openRawWarmBrowser(clientId?: string): Promise<OpenRawRecoveryBrowser> {
    const connection = await createConnectionSet(
      this.cloud.sessionId,
      this.cloud.observerCapability,
      clientId,
    );
    if (connection.clientId === null) throw new Error("raw Browser lacks client identity");
    const control = await this.upgrade("control", connection.controlTicket);
    const data = await this.upgrade("data", connection.dataTicket);
    const startMessage = nextServerFrame(control, (frame) => frame.type === "recovery-start");
    control.send(
      JSON.stringify({
        type: "attach",
        engineId,
        deliveryGeneration: connection.deliveryGeneration,
        hasLiveReplica: true,
        lastSessionEpoch: epoch.toString(),
        lastEventSeq: this.session.cursor.lastEventSeq.toString(),
        nextPtyOffset: this.session.cursor.nextPtyOffset.toString(),
      }),
    );
    const start = await within(startMessage, "timed out waiting for raw RecoveryStart", 3_000);
    if (start.type !== "recovery-start") throw new Error("expected RecoveryStart");
    const browser = { clientId: connection.clientId, connection, control, data, start };
    this.rawBrowsers.add(browser);
    return browser;
  }

  async attempt(recoveryId?: string): Promise<{
    readonly adopted_json: string | null;
    readonly recovery_id: string;
    readonly reset_reason: string | null;
    readonly source_closed_json: string | null;
    readonly state: string;
  }> {
    return runInDurableObject(this.stub, (_instance, state) => {
      const query =
        recoveryId === undefined
          ? "SELECT recovery_id, state, reset_reason, adopted_json, source_closed_json FROM recovery_attempt ORDER BY created_at DESC LIMIT 1"
          : "SELECT recovery_id, state, reset_reason, adopted_json, source_closed_json FROM recovery_attempt WHERE recovery_id = ?";
      return recoveryId === undefined
        ? state.storage.sql.exec<AttemptRow>(query).one()
        : state.storage.sql.exec<AttemptRow>(query, recoveryId).one();
    });
  }

  async deliveryStates(recoveryId: string): Promise<string[]> {
    return runInDurableObject(this.stub, (_instance, state) =>
      state.storage.sql
        .exec<{ state: string }>(
          "SELECT state FROM recovery_delivery_record WHERE recovery_id = ? ORDER BY lane, delivery_ordinal",
          recoveryId,
        )
        .toArray()
        .map(({ state: deliveryState }) => deliveryState),
    );
  }

  async diagnostics(recoveryId: string): Promise<object> {
    const durable = await runInDurableObject(this.stub, (_instance, state) => ({
      attempt: state.storage.sql
        .exec(
          "SELECT state, reset_reason, granted_cumulative_encoded_bytes FROM recovery_attempt WHERE recovery_id = ?",
          recoveryId,
        )
        .toArray(),
      lanes: state.storage.sql
        .exec(
          "SELECT lane, sent_delivery_ordinal, received_delivery_ordinal FROM recovery_delivery_lane WHERE recovery_id = ? ORDER BY lane",
          recoveryId,
        )
        .toArray(),
      outbox: state.storage.sql
        .exec(
          "SELECT kind, destination FROM recovery_control_outbox WHERE recovery_id = ? ORDER BY kind",
          recoveryId,
        )
        .toArray(),
      records: state.storage.sql
        .exec(
          "SELECT lane, delivery_ordinal, state FROM recovery_delivery_record WHERE recovery_id = ? ORDER BY lane, delivery_ordinal",
          recoveryId,
        )
        .toArray(),
    }));
    return {
      durable,
      hostSources: this.recoverySources.counters,
      host: {
        controlReadyState: this.currentHost.pair.control.readyState,
        dataReadyState: this.currentHost.pair.data.readyState,
        controlTrace: this.hostControlTrace,
        closeTrace: this.hostCloseTrace,
      },
    };
  }

  async seedUnsafeRecoveryDelivery(
    browser: OpenRawRecoveryBrowser,
    state: "queued" | "sending",
  ): Promise<void> {
    await runInDurableObject(this.stub, (instance, durable) => {
      const recoveries = Reflect.get(instance, "recoveries");
      if (!(recoveries instanceof RelayRecoveryStore)) {
        throw new Error("runtime recovery store is missing");
      }
      const encoded = emptyRecoveryDone(browser.connection);
      durable.storage.transactionSync(() => {
        const enqueued = recoveries.enqueueValidatedLaneDelivery(
          browser.start.recoveryId,
          encoded,
          Date.now(),
        );
        if (!enqueued.ok) throw new Error(`failed to seed delivery: ${enqueued.reason}`);
        if (state === "sending") {
          const begun = recoveries.beginLaneDeliverySend(enqueued.record, Date.now());
          if (!begun.ok) throw new Error(`failed to begin delivery: ${begun.reason}`);
        }
      });
    });
  }

  holdNextHostSourceClosed(): HeldHostSourceClosed {
    const socket = this.currentHost.pair.control;
    const original = socket.send.bind(socket);
    const held = deferred<string>();
    let encoded: string | null = null;
    let restored = false;
    const restore = () => {
      if (restored) return;
      restored = true;
      Object.defineProperty(socket, "send", { configurable: true, value: original });
    };
    Object.defineProperty(socket, "send", {
      configurable: true,
      value: (data: string) => {
        if (encoded === null) {
          let type: unknown;
          try {
            type = (JSON.parse(data) as { type?: unknown }).type;
          } catch {
            type = undefined;
          }
          if (type === "recovery-source-closed") {
            encoded = data;
            held.resolve(data);
            return;
          }
        }
        original(data);
      },
    });
    return {
      isHeld: () => encoded !== null,
      wait: () => held.promise,
      publish: () => {
        if (encoded === null) throw new Error("RecoverySourceClosed is not held yet");
        const message = encoded;
        encoded = null;
        restore();
        original(message);
      },
    };
  }

  async pauseRecoveryOutboxDrain(): Promise<void> {
    await runInDurableObject(this.stub, (instance) => {
      Reflect.set(instance, "recoveryOutboxDrainScheduled", true);
    });
  }

  async recoveryOutboxKinds(recoveryId: string): Promise<string[]> {
    return runInDurableObject(this.stub, (_instance, state) =>
      state.storage.sql
        .exec<{ kind: string }>(
          "SELECT kind FROM recovery_control_outbox WHERE recovery_id = ? ORDER BY kind",
          recoveryId,
        )
        .toArray()
        .map(({ kind }) => kind),
    );
  }

  async hibernate(): Promise<void> {
    await evictDurableObject(this.stub, { webSockets: "hibernate" });
    await runInDurableObject(this.stub, () => undefined);
  }

  async injectSlowObserver(browser: OpenThreeOwnerBrowser): Promise<void> {
    await runInDurableObject(this.stub, (_instance, state) => {
      const serverData = state
        .getWebSockets(`client:${browser.clientId}`)
        .find(
          (socket) =>
            SocketAttachmentSchema.parse(socket.deserializeAttachment()).channel === "data",
        );
      if (serverData === undefined) throw new Error("slow Browser data socket is missing");
      Object.defineProperty(serverData, "send", {
        configurable: true,
        value: () => {
          throw new Error("injected slow observer send failure");
        },
      });
    });
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    for (const browser of this.browsers) browser.close();
    for (const browser of this.rawBrowsers) {
      closeSocket(browser.data, 1000, "three-owner raw Browser closed");
      closeSocket(browser.control, 1000, "three-owner raw Browser closed");
    }
    for (const host of this.hosts) {
      host.connection.close();
    }
    this.recoverySources.dispose();
    this.session.dispose();
    for (const socket of this.sockets) closeSocket(socket, 1000, "three-owner cleanup");
    await scheduler.wait(0);
    await runInDurableObject(this.stub, async (instance) => {
      const deliveryScheduler = Reflect.get(instance, "recoveryDeliveryScheduler");
      if (deliveryScheduler instanceof RelayDeliveryScheduler) {
        await deliveryScheduler.whenIdle();
        if (deliveryScheduler.queuedRecords !== 0) {
          throw new Error("three-owner cleanup left scheduled delivery records");
        }
      }
      const ring = Reflect.get(instance, "recoveryDeliveryRing");
      if (ring instanceof RelayDeliveryRing) {
        const usage = ring.usage;
        if (usage.physicalBytes !== 0 || usage.physicalEntries !== 0 || usage.references !== 0) {
          throw new Error("three-owner cleanup left delivery-ring ownership");
        }
      }
      const refRecords = Reflect.get(instance, "recoveryDeliveryRefRecords");
      if (refRecords instanceof Map && refRecords.size !== 0) {
        throw new Error("three-owner cleanup left delivery ref owners");
      }
    });
    this.sockets.clear();
  }

  private async upgrade(channel: "control" | "data", ticket: string): Promise<WebSocket> {
    const response = await workerExports.default.fetch(
      new Request(
        `${origin}/api/v1/sessions/${this.cloud.sessionId}/ws/${channel}?ticket=${ticket}`,
        { headers: { upgrade: "websocket" } },
      ),
    );
    if (response.status !== 101 || response.webSocket === null) {
      throw new Error(`${channel} WebSocket upgrade failed with ${response.status}`);
    }
    response.webSocket.accept();
    this.sockets.add(response.webSocket);
    response.webSocket.addEventListener("close", () => this.sockets.delete(response.webSocket!), {
      once: true,
    });
    return response.webSocket;
  }
}

export function seededMutations(seed: number, count: number): LiteralMutation[] {
  if (!Number.isInteger(seed) || seed === 0)
    throw new RangeError("seed must be a non-zero integer");
  if (!Number.isInteger(count) || count <= 0) throw new RangeError("count must be positive");
  let state = seed >>> 0;
  const next = () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
  const mutations: LiteralMutation[] = [];
  for (let index = 0; index < count; index += 1) {
    const value = next();
    if (value % 4 === 0) {
      const cols = 70 + (value % 31);
      const rows = 20 + ((value >>> 8) % 16);
      mutations.push({
        type: "resize",
        dimensions: { cols, rows, widthPx: cols * 9, heightPx: rows * 18 },
      });
      continue;
    }
    const length = 1 + (value % 5);
    const bytes = new Uint8Array(length);
    for (let byteIndex = 0; byteIndex < length; byteIndex += 1) {
      bytes[byteIndex] = 0x20 + ((value >>> (byteIndex * 5)) % 0x5f);
    }
    mutations.push({ type: "output", bytes });
  }
  return mutations;
}

function cloneMutation(mutation: LiteralMutation): LiteralMutation {
  return mutation.type === "output"
    ? { type: "output", bytes: mutation.bytes.slice() }
    : { type: "resize", dimensions: { ...mutation.dimensions } };
}

function serializeSnapshotMutation(mutation: LiteralMutation): object {
  return mutation.type === "output"
    ? { type: "output", bytes: [...mutation.bytes] }
    : { type: "resize", dimensions: { ...mutation.dimensions } };
}

function parseSnapshotMutation(input: unknown): LiteralMutation {
  if (input === null || typeof input !== "object") {
    throw new Error("invalid literal snapshot mutation");
  }
  const record = input as Record<string, unknown>;
  if (record.type === "output" && Array.isArray(record.bytes)) {
    if (
      !record.bytes.every(
        (value): value is number => Number.isInteger(value) && value >= 0 && value <= 0xff,
      )
    ) {
      throw new Error("invalid literal snapshot output");
    }
    return { type: "output", bytes: Uint8Array.from(record.bytes) };
  }
  if (
    record.type === "resize" &&
    record.dimensions !== null &&
    typeof record.dimensions === "object"
  ) {
    const dimensions = record.dimensions as Record<string, unknown>;
    const { cols, rows, widthPx, heightPx } = dimensions;
    if (
      typeof cols !== "number" ||
      !Number.isInteger(cols) ||
      cols <= 0 ||
      typeof rows !== "number" ||
      !Number.isInteger(rows) ||
      rows <= 0 ||
      typeof widthPx !== "number" ||
      !Number.isInteger(widthPx) ||
      widthPx < 0 ||
      typeof heightPx !== "number" ||
      !Number.isInteger(heightPx) ||
      heightPx < 0
    ) {
      throw new Error("invalid literal snapshot resize");
    }
    return {
      type: "resize",
      dimensions: { cols, rows, widthPx, heightPx },
    };
  }
  throw new Error("invalid literal snapshot mutation");
}

/** Independent literal cursor reducer: deliberately does not call protocol cursor helpers. */
export function literalOracle(mutations: readonly LiteralMutation[]): LiteralOracleState {
  let eventSeq = 0n;
  let nextPtyOffset = 0n;
  const output: number[] = [];
  const resizes: ResizePayload[] = [];
  for (const mutation of mutations) {
    eventSeq += 1n;
    if (mutation.type === "output") {
      output.push(...mutation.bytes);
      nextPtyOffset += BigInt(mutation.bytes.byteLength);
    } else {
      resizes.push({ ...mutation.dimensions });
    }
  }
  return {
    eventSeq,
    nextPtyOffset,
    output: Uint8Array.from(output),
    resizes,
    timeline: mutations.map(cloneMutation),
  };
}

export function sinkOracle(sink: LiteralReplicaSink): LiteralOracleState {
  return literalOracle(sink.mutations);
}

export async function waitForCondition(
  condition: () => boolean | Promise<boolean>,
  description: string,
  turns = 400,
): Promise<void> {
  for (let turn = 0; turn < turns; turn += 1) {
    if (await condition()) return;
    await scheduler.wait(0);
  }
  throw new Error(`timed out waiting for ${description}`);
}

async function within<T>(promise: Promise<T>, label: string, timeoutMs: number): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => reject(new Error(label)), timeoutMs);
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

export function nextClose(socket: WebSocket): Promise<CloseEvent> {
  return new Promise((resolve) => socket.addEventListener("close", resolve, { once: true }));
}

function emptyRecoveryDone(connection: ConnectionSetResponse): Uint8Array {
  const payload = encodeDataFrame({
    kind: DataFrameKind.RecoveryDone,
    flags: DataFrameFlag.None,
    sessionEpoch: epoch,
    deliveryGeneration: 0n,
    eventSeq: 0n,
    ptyOffset: 0n,
    streamId: 0,
    payload: new Uint8Array(),
  });
  return encodeDeliveryEnvelope({
    lane: "recovery",
    deliveryGeneration: BigInt(connection.deliveryGeneration),
    deliveryOrdinal: 1n,
    cumulativeEncodedBytes: BigInt(DELIVERY_ENVELOPE_HEADER_BYTES + payload.byteLength),
    streamId: connection.streamId,
    payload,
  });
}

async function createCloudSession(): Promise<CreatedThreeOwnerSession> {
  const sessionId = `session_three_owner_${crypto.randomUUID().replaceAll("-", "")}`;
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
        "content-type": "application/json",
      },
      body: JSON.stringify({ sessionId, engineId, sessionEpoch: epoch.toString() }),
    }),
  );
  if (response.status !== 201) throw new Error(`session creation failed with ${response.status}`);
  return response.json<CreatedThreeOwnerSession>();
}

async function createConnectionSet(
  sessionId: string,
  capability: string,
  clientId?: string,
): Promise<ConnectionSetResponse> {
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${sessionId}/connection-sets`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${capability}`,
        "content-type": "application/json",
      },
      body: JSON.stringify(clientId === undefined ? {} : { clientId }),
    }),
  );
  if (response.status !== 200) {
    throw new Error(`connection-set creation failed with ${response.status}`);
  }
  return response.json<ConnectionSetResponse>();
}

function progressKey(frame: RecoveryProgressFrame): string {
  if (frame.type === "delivery-received") {
    return `${frame.type}:${frame.lane}:${frame.contiguousDeliveryOrdinal}:${frame.cumulativeEncodedBytes}`;
  }
  return frame.type;
}

async function binaryEventBytes(event: MessageEvent): Promise<Uint8Array> {
  if (event.data instanceof ArrayBuffer) return new Uint8Array(event.data);
  if (ArrayBuffer.isView(event.data)) {
    return new Uint8Array(event.data.buffer, event.data.byteOffset, event.data.byteLength).slice();
  }
  if (event.data instanceof Blob) return new Uint8Array(await event.data.arrayBuffer());
  throw new Error("expected binary Browser delivery");
}

function nextServerFrame(
  socket: WebSocket,
  matches: (frame: RecoveryServerControlFrame) => boolean,
): Promise<RecoveryServerControlFrame> {
  return new Promise((resolve, reject) => {
    const onMessage = (event: MessageEvent) => {
      if (typeof event.data !== "string") return;
      let frame: RecoveryServerControlFrame;
      try {
        frame = decodeServerControlFrame(event.data);
      } catch {
        return;
      }
      if (!matches(frame)) return;
      socket.removeEventListener("message", onMessage);
      resolve(frame);
    };
    socket.addEventListener("message", onMessage);
    socket.addEventListener("error", () => reject(new Error("Browser WebSocket failed")), {
      once: true,
    });
  });
}

function closeSocket(socket: WebSocket, code: number, reason: string): void {
  if (socket.readyState >= WebSocket.CLOSING) return;
  socket.close(code, reason.slice(0, 120));
}

function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly reject: (error: unknown) => void;
  readonly resolve: (value: T | PromiseLike<T>) => void;
} {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

async function waitForPromiseOrAbort(promise: Promise<void>, signal: AbortSignal): Promise<void> {
  signal.throwIfAborted();
  let onAbort!: () => void;
  const aborted = new Promise<never>((_resolve, reject) => {
    onAbort = () => reject(signal.reason ?? new DOMException("aborted", "AbortError"));
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    await Promise.race([promise, aborted]);
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}
