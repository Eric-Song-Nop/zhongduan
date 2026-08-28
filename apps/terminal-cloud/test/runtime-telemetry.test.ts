import { DataFrameKind, encodeDataFrame, encodeDeliveryBarrierPayload } from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { CloudTelemetryLogRecord } from "../src/worker/cloud-telemetry";

interface CreatedSession {
  hostCapability: string;
  sessionId: string;
  writerCapability: string;
}

interface ConnectionSet {
  clientId: string | null;
  connectionId: string;
  controlTicket: string;
  dataTicket: string;
  deliveryGeneration: string;
  streamId: number;
}

interface SocketEndpoint {
  inbox: SocketInbox;
  socket: WebSocket;
}

interface ScenarioResult {
  privateValues: string[];
  records: CloudTelemetryLogRecord[];
  summary: Record<string, unknown>;
}

const origin = "https://terminal.example.test";
const engineId = "ghostty:test+snapshot-v1+wterm:test";
const testSockets = new Set<WebSocket>();
const testSessions = new Set<string>();
let sessionCounter = 0;

class SocketInbox {
  readonly #messages: unknown[] = [];
  readonly #waiters: Array<(message: unknown) => void> = [];

  constructor(socket: WebSocket) {
    socket.addEventListener("message", (event) => {
      const waiter = this.#waiters.shift();
      if (waiter === undefined) this.#messages.push(event.data);
      else waiter(event.data);
    });
  }

  nextMessage(): Promise<unknown> {
    const message = this.#messages.shift();
    if (message !== undefined) return Promise.resolve(message);
    return new Promise((resolve) => this.#waiters.push(resolve));
  }

  async nextJson(): Promise<Record<string, unknown>> {
    const message = await this.nextMessage();
    if (typeof message !== "string") throw new Error("expected a text WebSocket message");
    return JSON.parse(message) as Record<string, unknown>;
  }
}

function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

async function drainSession(sessionId: string): Promise<void> {
  await runInDurableObject(sessionStub(sessionId), () => undefined);
}

async function createSession(): Promise<CreatedSession> {
  sessionCounter += 1;
  const sessionId = `session_telemetry_${sessionCounter.toString().padStart(16, "0")}`;
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
        "content-type": "application/json",
      },
      body: JSON.stringify({ sessionId, engineId, sessionEpoch: "7" }),
    }),
  );
  expect(response.status).toBe(201);
  return response.json<CreatedSession>();
}

async function createConnectionSet(
  session: CreatedSession,
  capability: string,
): Promise<ConnectionSet> {
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${session.sessionId}/connection-sets`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${capability}`,
        "content-type": "application/json",
      },
      body: "{}",
    }),
  );
  expect(response.status).toBe(200);
  return response.json<ConnectionSet>();
}

async function upgrade(
  session: CreatedSession,
  channel: "control" | "data",
  ticket: string,
): Promise<SocketEndpoint> {
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions/${session.sessionId}/ws/${channel}?ticket=${ticket}`, {
      headers: { upgrade: "websocket" },
    }),
  );
  expect(response.status).toBe(101);
  const socket = response.webSocket;
  if (socket === null) throw new Error("upgrade response did not include a WebSocket");
  socket.accept();
  testSockets.add(socket);
  testSessions.add(session.sessionId);
  socket.addEventListener("close", () => testSockets.delete(socket), { once: true });
  return { inbox: new SocketInbox(socket), socket };
}

async function openConnection(
  session: CreatedSession,
  capability: string,
): Promise<{ connection: ConnectionSet; control: SocketEndpoint; data: SocketEndpoint }> {
  const connection = await createConnectionSet(session, capability);
  const control = await upgrade(session, "control", connection.controlTicket);
  const data = await upgrade(session, "data", connection.dataTicket);
  return { connection, control, data };
}

function setTelemetryMode(mode: string): void {
  const current = Object.getOwnPropertyDescriptor(env, "CLOUD_TELEMETRY_MODE");
  Object.defineProperty(env, "CLOUD_TELEMETRY_MODE", {
    configurable: true,
    enumerable: current?.enumerable ?? true,
    value: mode,
  });
}

function isCloudTelemetryLogRecord(value: unknown): value is CloudTelemetryLogRecord {
  return (
    typeof value === "object" &&
    value !== null &&
    "type" in value &&
    value.type === "zhongduan.telemetry" &&
    "runtime" in value &&
    value.runtime === "cloud-do"
  );
}

function keyFrame(writerLease: string, clientInputSeq: string) {
  return {
    type: "key",
    writerLease,
    inputEpoch: "telemetry_equivalence",
    clientInputSeq,
    observedEventSeq: "0",
    code: "KeyE",
    key: "€",
    text: "€",
    modifiers: 0,
    action: "press",
    altGraph: true,
    composing: false,
    consumedModifiers: 0,
    unshiftedCodepoint: 0x65,
  };
}

function warmDeliveryBarrier(connection: ConnectionSet): Uint8Array {
  return encodeDataFrame({
    kind: DataFrameKind.DeliveryBarrier,
    flags: 0,
    sessionEpoch: 7n,
    deliveryGeneration: BigInt(connection.deliveryGeneration),
    eventSeq: 0n,
    ptyOffset: 0n,
    streamId: connection.streamId,
    payload: encodeDeliveryBarrierPayload({
      mode: "warm",
      connectionId: connection.connectionId,
    }),
  });
}

afterEach(async () => {
  for (const socket of testSockets) {
    if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
      socket.close(1000, "test cleanup");
    }
  }
  await Promise.all([...testSessions].map(drainSession));
  await Promise.all([...testSessions].map(drainSession));
  testSockets.clear();
  testSessions.clear();
  vi.restoreAllMocks();
});

describe("live Durable Object telemetry", () => {
  it("does not change attach, input forwarding, acknowledgement, or lease state", async () => {
    const originalMode = Object.getOwnPropertyDescriptor(env, "CLOUD_TELEMETRY_MODE");
    if (originalMode === undefined || originalMode.configurable !== true) {
      throw new Error("CLOUD_TELEMETRY_MODE must be a configurable test binding");
    }

    const collected: unknown[] = [];
    let collectorThrows = false;
    vi.spyOn(console, "info").mockImplementation((value: unknown) => {
      collected.push(value);
      if (collectorThrows) throw new Error("injected Cloud telemetry collector failure");
    });

    const runScenario = async (
      mode: string,
      throwFromCollector: boolean,
    ): Promise<ScenarioResult> => {
      setTelemetryMode(mode);
      collectorThrows = throwFromCollector;
      const firstRecord = collected.length;
      const session = await createSession();
      const host = await openConnection(session, session.hostCapability);
      host.control.socket.send(
        JSON.stringify({
          type: "host-ready",
          engineId,
          sessionEpoch: "7",
          headEventSeq: "0",
          nextPtyOffset: "0",
        }),
      );
      const ready = await host.control.inbox.nextJson();

      const browser = await openConnection(session, session.writerCapability);
      const browserClientId = browser.connection.clientId;
      if (browserClientId === null)
        throw new Error("writer connection did not receive a client ID");
      browser.control.socket.send(
        JSON.stringify({
          type: "attach",
          engineId,
          deliveryGeneration: browser.connection.deliveryGeneration,
          hasLiveReplica: true,
          lastSessionEpoch: "7",
          lastEventSeq: "0",
          nextPtyOffset: "0",
        }),
      );
      await drainSession(session.sessionId);
      const [welcome, attachRequest] = await Promise.all([
        browser.control.inbox.nextJson(),
        host.control.inbox.nextJson(),
      ]);
      const writerLease = String(welcome.writerLease);
      expect(writerLease).not.toBe("undefined");

      const forwardedInputs: Record<string, unknown>[] = [];
      const acknowledgements: Record<string, unknown>[] = [];
      for (let clientInputSeq = 1; clientInputSeq <= 16; clientInputSeq += 1) {
        const semanticInput = keyFrame(writerLease, clientInputSeq.toString());
        browser.control.socket.send(JSON.stringify(semanticInput));
        const forwardedInput = await host.control.inbox.nextJson();
        expect(forwardedInput).toMatchObject({
          ...semanticInput,
          connectionId: browser.connection.connectionId,
          clientId: browserClientId,
          writerFence: "1",
        });
        forwardedInputs.push(forwardedInput);

        host.control.socket.send(
          JSON.stringify({
            type: "input-ack",
            connectionId: forwardedInput.connectionId,
            inputEpoch: forwardedInput.inputEpoch,
            clientInputSeq: forwardedInput.clientInputSeq,
            status: "written",
            authorityEventSeq: "0",
          }),
        );
        acknowledgements.push(await browser.control.inbox.nextJson());
      }
      const forwarded = forwardedInputs[0]!;
      const acknowledgement = acknowledgements[0]!;

      host.data.socket.send(warmDeliveryBarrier(browser.connection));
      const [replayStart, barrierResult] = await Promise.all([
        browser.control.inbox.nextJson(),
        host.control.inbox.nextJson(),
      ]);
      expect(replayStart).toMatchObject({
        type: "replay-start",
        streamId: browser.connection.streamId,
        deliveryGeneration: browser.connection.deliveryGeneration,
        baseEventSeq: "0",
        basePtyOffset: "0",
        commitEventSeq: "0",
        commitPtyOffset: "0",
      });
      expect(barrierResult).toMatchObject({
        type: "delivery-barrier-result",
        mode: "warm",
        status: "ready",
        streamId: browser.connection.streamId,
        deliveryGeneration: browser.connection.deliveryGeneration,
        commitEventSeq: "0",
        commitPtyOffset: "0",
      });

      browser.data.socket.close(1000, "exercise telemetry delivery reset");
      const deliveryReset = await browser.control.inbox.nextJson();
      expect(deliveryReset).toMatchObject({
        type: "resync-required",
        deliveryGeneration: "2",
        reason: "data-disconnected",
      });
      const replacementDataTicket = String(deliveryReset.dataTicket);
      expect(replacementDataTicket).not.toBe("undefined");
      await drainSession(session.sessionId);
      await drainSession(session.sessionId);

      const relayState = await runInDurableObject(
        sessionStub(session.sessionId),
        (_instance, state) => {
          const lease = state.storage.sql
            .exec("SELECT client_id, fence, expires_at FROM writer_lease WHERE singleton = 1")
            .one();
          const control = state
            .getWebSockets(`client:${browserClientId}`)
            .find(
              (socket) =>
                (socket.deserializeAttachment() as { channel?: string }).channel === "control",
            )
            ?.deserializeAttachment() as
            | { controlState?: string; deliveryGeneration?: string; leaseFence?: string | null }
            | undefined;
          return {
            controlState: control?.controlState,
            deliveryGeneration: control?.deliveryGeneration,
            leaseActive: Number(lease.expires_at) > Date.now(),
            leaseFence: lease.fence,
            leaseMatchesClient: lease.client_id === browserClientId,
            socketLeaseFence: control?.leaseFence,
          };
        },
      );
      const records = collected.slice(firstRecord).filter(isCloudTelemetryLogRecord);

      return {
        privateValues: [
          session.sessionId,
          writerLease,
          browserClientId,
          browser.connection.connectionId,
          replacementDataTicket,
          "telemetry_equivalence",
          "KeyE",
          "€",
        ],
        records,
        summary: {
          ready: {
            type: ready.type,
            sessionEpoch: ready.sessionEpoch,
            headEventSeq: ready.headEventSeq,
            nextPtyOffset: ready.nextPtyOffset,
          },
          welcome: {
            type: welcome.type,
            engineId: welcome.engineId,
            sessionEpoch: welcome.sessionEpoch,
            deliveryGeneration: welcome.deliveryGeneration,
            hasWriterLease: typeof welcome.writerLease === "string",
          },
          attachRequest: {
            type: attachRequest.type,
            hasLiveReplica: attachRequest.hasLiveReplica,
            lastSessionEpoch: attachRequest.lastSessionEpoch,
            lastEventSeq: attachRequest.lastEventSeq,
            nextPtyOffset: attachRequest.nextPtyOffset,
            deliveryGeneration: attachRequest.deliveryGeneration,
          },
          forwarded: {
            type: forwarded.type,
            inputEpoch: forwarded.inputEpoch,
            clientInputSeq: forwarded.clientInputSeq,
            code: forwarded.code,
            key: forwarded.key,
            text: forwarded.text,
            action: forwarded.action,
            writerFence: forwarded.writerFence,
            trustedIdentityInjected:
              forwarded.connectionId === browser.connection.connectionId &&
              forwarded.clientId === browserClientId,
          },
          acknowledgement,
          replayStart,
          barrierResult: {
            type: barrierResult.type,
            mode: barrierResult.mode,
            status: barrierResult.status,
            streamId: barrierResult.streamId,
            deliveryGeneration: barrierResult.deliveryGeneration,
            commitEventSeq: barrierResult.commitEventSeq,
            commitPtyOffset: barrierResult.commitPtyOffset,
          },
          deliveryReset: {
            type: deliveryReset.type,
            deliveryGeneration: deliveryReset.deliveryGeneration,
            reason: deliveryReset.reason,
            hasDataTicket: typeof deliveryReset.dataTicket === "string",
            hasExpiry: typeof deliveryReset.expiresAt === "number",
          },
          relayState,
          survivingSocketsOpen:
            host.control.socket.readyState === WebSocket.OPEN &&
            host.data.socket.readyState === WebSocket.OPEN &&
            browser.control.socket.readyState === WebSocket.OPEN,
          replacedDataSocketClosed: browser.data.socket.readyState >= WebSocket.CLOSING,
        },
      };
    };

    try {
      const disabled = await runScenario("off", false);
      const enabled = await runScenario("workers-logs-v1", false);
      const throwing = await runScenario("workers-logs-v1", true);
      collectorThrows = false;

      expect(enabled.summary).toEqual(disabled.summary);
      expect(throwing.summary).toEqual(disabled.summary);
      expect(disabled.records).toEqual([]);

      for (const result of [enabled, throwing]) {
        expect(result.records).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              name: "cloud.input.forward",
              leaseOutcome: "active",
              outcome: "send-returned",
            }),
            expect.objectContaining({
              name: "cloud.recovery.transition",
              transition: "attach",
              outcome: "host-request-send-returned",
            }),
            expect.objectContaining({
              name: "cloud.recovery.barrier",
              mode: "warm",
              outcome: "ready",
              reason: "none",
              retryScope: "not-applicable",
            }),
            expect.objectContaining({
              name: "cloud.recovery.transition",
              transition: "reset",
              trigger: "data-disconnected",
              outcome: "issued",
              hostNotifyOutcome: "not-requested",
            }),
          ]),
        );
        const serialized = JSON.stringify(result.records);
        for (const privateValue of result.privateValues) {
          expect(serialized).not.toContain(privateValue);
        }
      }
    } finally {
      collectorThrows = false;
      Object.defineProperty(env, "CLOUD_TELEMETRY_MODE", originalMode);
    }
  }, 15_000);
});
