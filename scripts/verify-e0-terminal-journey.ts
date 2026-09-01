#!/usr/bin/env node

/** Reproducible E0 terminal journey, scenario merger, and candidate decision CLI. */

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import {
  createServer,
  type IncomingHttpHeaders,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { createServer as createNetServer } from "node:net";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { chromium, type Page, errors as playwrightErrors } from "playwright";
import WebSocket, { WebSocketServer, type RawData } from "ws";
import {
  EVENT_LOG_ENV,
  FLOOD_CHUNK_BYTES_ENV,
  FLOOD_MAX_BYTES_ENV,
  FLOOD_MAX_DURATION_MS_ENV,
  FLOOD_PREFIX,
  INPUT_CAPTURE_ENV,
  INTERRUPT_ARM_PREFIX,
  QUIET_PREFIX,
  READY_SENTINEL,
  RESULT_PREFIX,
  PROBE_PREFIX,
  SECURE_PREFIX,
} from "./e0-terminal-fixture.ts";
import {
  BASELINE_PATH,
  CONTRACT_PATH,
  EXPECTED_VARIANTS,
  ROOT,
  assembleCandidateReport,
  assembleCurrentReport,
  buildE4bDecision,
  canonicalSha256,
  isData,
  loadJson,
  loadReportBundle,
  matrixCells,
  parseJson,
  stringifyJson,
  validateContract,
  validateScenarioReport,
  writeReportBundle,
  type Data,
} from "./e0-terminal-journey.ts";

const APP_ROOT = join(ROOT, "apps", "terminal-cloud");
const HOST_CLI = join(ROOT, "apps", "host-daemon", "dist", "cli.mjs");
const VP = join(ROOT, "node_modules", "vite-plus", "bin", "vp");
const DEV_VARS = join(APP_ROOT, ".dev.vars");
const INIT_SCRIPT = readFileSync(join(ROOT, "scripts", "e0-browser-instrumentation.js"), "utf8");
const BOOTSTRAP_TOKEN = "e0-bootstrap-token-with-at-least-32-bytes";
const CAPABILITY_KEY = "e0-capability-key-with-at-least-32-bytes";
const E4_EVIDENCE_ENV = "ZHONGDUAN_E4_EVIDENCE_JSONL";
const E0_DISABLE_SNAPSHOT_REFRESH_ENV = "ZHONGDUAN_E0_DISABLE_SNAPSHOT_REFRESH";
const INPUT_FRAME_TYPES = new Set(["key", "text", "paste", "focus", "mouse", "resize-request"]);
const CONTROL_MODIFIER = 1 << 1;
const SNAPSHOT_CHECKPOINT_TTL_MS = 30_000;
const SNAPSHOT_CHECKPOINT_EXPIRY_CUSHION_MS = 2_000;
const SNAPSHOT_OVERLAP_SAMPLE_INTERVAL_MS = 3_000;
const SNAPSHOT_ATTACH_REMAINING_SAMPLES = 3;
const HOP_HEADERS = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

interface BrowserHarnessIntent {
  sampleId: string;
  localIntentId: string;
  terminal: Data | null;
  sends: Array<{ identity: string }>;
}

interface BrowserHarness {
  currentSample: string | null;
  duplicateSample: string | null;
  events: Data[];
  intents: BrowserHarnessIntent[];
  intentsBySample: Record<string, BrowserHarnessIntent | undefined>;
  consume(sampleId: string): string;
}

type HarnessWindow = Window & { __zhongduanE0: BrowserHarness };

export class JourneyError extends Error {
  override name = "JourneyError";
}

class Signal {
  private resolver: (() => void) | null = null;
  promise: Promise<void>;
  settled = false;

  constructor() {
    this.promise = new Promise((resolvePromise) => {
      this.resolver = resolvePromise;
    });
  }

  set(): void {
    if (this.settled) return;
    this.settled = true;
    this.resolver?.();
  }
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  return Promise.race([
    promise,
    delay(timeoutMs).then(() => {
      throw new JourneyError(message);
    }),
  ]);
}

function unixNanoseconds(): bigint {
  return BigInt(Date.now()) * 1_000_000n;
}

function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let value = Math.imul(state ^ (state >>> 15), 1 | state);
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
  };
}

export class ManagedProcess {
  readonly name: string;
  readonly process: ChildProcess;
  private readonly output: string[] = [];

  constructor(
    name: string,
    command: string,
    arguments_: string[],
    cwd: string,
    env: NodeJS.ProcessEnv,
  ) {
    this.name = name;
    this.process = spawn(command, arguments_, {
      cwd,
      env,
      detached: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    const retain = (chunk: Buffer): void => {
      for (const line of chunk.toString("utf8").split(/\r?\n/u)) {
        if (line.length > 0) this.output.push(line);
      }
      if (this.output.length > 120) this.output.splice(0, this.output.length - 120);
    };
    this.process.stdout?.on("data", retain);
    this.process.stderr?.on("data", retain);
  }

  assertRunning(): void {
    if (this.process.exitCode !== null || this.process.signalCode !== null) {
      throw new JourneyError(
        `${this.name} exited (${this.process.exitCode ?? this.process.signalCode}); sanitized tail:\n${this.sanitizedTail().slice(-20).join("\n")}`,
      );
    }
  }

  sanitizedTail(): string[] {
    return this.output.map((line) =>
      line
        .replaceAll(BOOTSTRAP_TOKEN, "[redacted-bootstrap]")
        .replaceAll(CAPABILITY_KEY, "[redacted-signing-key]")
        .replace(/Bearer\s+\S+/giu, "Bearer [redacted]")
        .replace(/([?&](?:ticket|capability)=)[^&\s]+/giu, "$1[redacted]"),
    );
  }

  async stop(): Promise<void> {
    if (this.process.exitCode !== null || this.process.pid === undefined) return;
    try {
      process.kill(-this.process.pid, "SIGTERM");
    } catch {}
    await Promise.race([
      new Promise<void>((resolvePromise) => this.process.once("exit", () => resolvePromise())),
      delay(5_000),
    ]);
    if (this.process.exitCode === null) {
      try {
        process.kill(-this.process.pid, "SIGKILL");
      } catch {}
    }
  }
}

function increment(map: Map<string, number>, key: string): number {
  const value = (map.get(key) ?? 0) + 1;
  map.set(key, value);
  return value;
}

export class TraceStore {
  readonly variant: string;
  events: Data[] = [];
  readonly identitySamples = new Map<string, string>();
  readonly browserSendCounts = new Map<string, number>();
  readonly hostReceiveCounts = new Map<string, number>();
  readonly sampleIdentities = new Map<string, string>();
  readonly sampleBrowserIdentities = new Map<string, Set<string>>();
  readonly sampleWireIdentities = new Map<string, Set<string>>();
  readonly held = new Map<string, Signal>();
  readonly releases = new Map<string, Signal>();
  disconnectSample: string | null = null;
  disconnectApplied = false;
  finalizedSnapshotId: string | null = null;
  snapshotFinalizations: Data[] = [];
  holdNextSnapshotAttach = false;
  snapshotAttachHeld = new Signal();
  snapshotAttachRelease = new Signal();
  pendingCtrlSample: string | null = null;
  hostReadyAcknowledged = new Signal();

  constructor(variant: string) {
    this.variant = variant;
  }

  event(
    name: string,
    sampleId: string,
    atUnixNs: bigint = unixNanoseconds(),
    fields: Data = {},
  ): void {
    this.events.push({ name, sampleId, variant: this.variant, atUnixNs, ...fields });
  }

  static browserIdentity(frame: Data): string | null {
    return typeof frame["inputEpoch"] === "string" && typeof frame["clientInputSeq"] === "string"
      ? `${frame["inputEpoch"]}/${frame["clientInputSeq"]}`
      : null;
  }

  static identity(frame: Data): string | null {
    const browserIdentity = TraceStore.browserIdentity(frame);
    return typeof frame["writerFence"] === "string" && browserIdentity !== null
      ? `${frame["writerFence"]}/${browserIdentity}`
      : null;
  }

  sample(frame: Data): string | null {
    if (
      frame["type"] === "key" &&
      frame["action"] === "press" &&
      frame["code"] === "KeyC" &&
      frame["key"] === "c" &&
      typeof frame["modifiers"] === "number" &&
      (frame["modifiers"] & CONTROL_MODIFIER) !== 0
    ) {
      const sample = this.pendingCtrlSample;
      this.pendingCtrlSample = null;
      return sample ?? "ctrl-c-pending";
    }
    const payload =
      frame["type"] === "text" || frame["type"] === "paste" ? frame["data"] : frame["text"];
    if (typeof payload !== "string") return null;
    for (const pattern of [
      /ZHONGDUAN_E0_PROBE:([A-Za-z0-9_-]+)\r/u,
      /ZHONGDUAN_E0_SECURE:([A-Za-z0-9_-]+)\r/u,
      /ZHONGDUAN_E0_FLOOD:([A-Za-z0-9_-]+)\r/u,
      /ZHONGDUAN_E0_INTERRUPT_ARM:[A-Za-z0-9_-]+:([A-Za-z0-9_-]+)\r/u,
    ]) {
      const match = pattern.exec(payload);
      if (match?.[1] !== undefined) return match[1];
    }
    if (payload === "\x03") {
      const sample = this.pendingCtrlSample;
      this.pendingCtrlSample = null;
      return sample ?? "ctrl-c-pending";
    }
    return null;
  }

  browserFrame(frame: Data, atUnixNs: bigint): { identity: string | null; sample: string | null } {
    const identity = TraceStore.browserIdentity(frame);
    if (identity === null || !INPUT_FRAME_TYPES.has(String(frame["type"])))
      return { identity: null, sample: null };
    const retained = this.identitySamples.get(identity);
    let sample = this.sample(frame) ?? retained ?? null;
    if (sample === "ctrl-c-pending")
      sample = retained ?? `ctrl-c-${String(this.sampleIdentities.size).padStart(3, "0")}`;
    if (sample === null) return { identity, sample: null };
    this.identitySamples.set(identity, sample);
    const browserIdentities = this.sampleBrowserIdentities.get(sample) ?? new Set<string>();
    browserIdentities.add(identity);
    this.sampleBrowserIdentities.set(sample, browserIdentities);
    const attempt = increment(this.browserSendCounts, identity);
    this.event("cloud.browser-receive-attempt", sample, atUnixNs, {
      browserIdentity: identity,
      attempt,
    });
    if (attempt === 1)
      this.event("cloud.browser-receive", sample, atUnixNs, { browserIdentity: identity });
    return { identity, sample };
  }

  hostFrame(frame: Data, sendAtUnixNs: bigint, receiveAtUnixNs: bigint): void {
    if (frame["type"] === "host-ready-ack") this.hostReadyAcknowledged.set();
    const identity = TraceStore.identity(frame);
    const browserIdentity = TraceStore.browserIdentity(frame);
    if (
      identity === null ||
      browserIdentity === null ||
      !INPUT_FRAME_TYPES.has(String(frame["type"]))
    )
      return;
    const sample = this.identitySamples.get(browserIdentity);
    if (sample === undefined) return;
    if (!this.sampleIdentities.has(sample)) this.sampleIdentities.set(sample, identity);
    const identities = this.sampleWireIdentities.get(sample) ?? new Set<string>();
    identities.add(identity);
    this.sampleWireIdentities.set(sample, identities);
    if (increment(this.hostReceiveCounts, identity) === 1) {
      this.event("cloud.host-send", sample, sendAtUnixNs, { wireIdentity: identity });
      this.event("host.receive", sample, receiveAtUnixNs, { wireIdentity: identity });
    }
  }

  hold(sampleId: string): { held: Signal; release: Signal } {
    const held = this.held.get(sampleId) ?? new Signal();
    const release = this.releases.get(sampleId) ?? new Signal();
    this.held.set(sampleId, held);
    this.releases.set(sampleId, release);
    return { held, release };
  }
}

function proxyHeaders(headers: IncomingHttpHeaders, websocket = false): Record<string, string> {
  const excluded = new Set([...HOP_HEADERS, "host", "content-length"]);
  if (websocket) {
    for (const name of [
      "sec-websocket-accept",
      "sec-websocket-extensions",
      "sec-websocket-key",
      "sec-websocket-protocol",
      "sec-websocket-version",
    ])
      excluded.add(name);
  }
  const result: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers)) {
    if (excluded.has(name.toLowerCase()) || value === undefined) continue;
    result[name] = Array.isArray(value) ? value.join(", ") : value;
  }
  return result;
}

function rawData(data: RawData): string | Buffer {
  if (Buffer.isBuffer(data)) return data;
  if (data instanceof ArrayBuffer) return Buffer.from(data);
  return Buffer.concat(data);
}

export class LinkProxy {
  readonly role: "browser" | "host";
  readonly upstream: string;
  readonly rttMs: number;
  readonly jitterMs: number;
  readonly trace: TraceStore;
  readonly random: () => number;
  origin: string | null = null;
  private server: ReturnType<typeof createServer> | null = null;
  private websocketServer: WebSocketServer | null = null;

  constructor(
    role: "browser" | "host",
    upstream: string,
    rttMs: number,
    jitterMs: number,
    trace: TraceStore,
    seed: number,
  ) {
    this.role = role;
    this.upstream = upstream.replace(/\/$/u, "");
    this.rttMs = rttMs;
    this.jitterMs = jitterMs;
    this.trace = trace;
    this.random = mulberry32(seed);
  }

  private async networkDelay(): Promise<void> {
    const jitter = this.jitterMs === 0 ? 0 : (this.random() * 2 - 1) * this.jitterMs;
    await delay(Math.max(0, this.rttMs / 2 + jitter));
  }

  async start(): Promise<void> {
    const wss = new WebSocketServer({
      noServer: true,
      perMessageDeflate: false,
      maxPayload: 16 * 1024 * 1024,
    });
    this.websocketServer = wss;
    this.server = createServer((request, response) => void this.handleHttp(request, response));
    this.server.on("upgrade", (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (downstream) => {
        void this.handleWebSocket(request, downstream).catch(() => downstream.terminate());
      });
    });
    await new Promise<void>((resolvePromise, reject) => {
      this.server!.once("error", reject);
      this.server!.listen(0, "127.0.0.1", () => resolvePromise());
    });
    const address = this.server.address();
    if (address === null || typeof address === "string")
      throw new JourneyError(`${this.role} proxy did not bind a TCP port`);
    this.origin = `http://127.0.0.1:${address.port}`;
  }

  async close(): Promise<void> {
    for (const client of this.websocketServer?.clients ?? []) client.terminate();
    this.websocketServer?.close();
    if (this.server !== null)
      await new Promise<void>((resolvePromise) => this.server!.close(() => resolvePromise()));
  }

  private async handleHttp(request: IncomingMessage, response: ServerResponse): Promise<void> {
    try {
      const chunks: Buffer[] = [];
      for await (const chunk of request)
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
      await this.networkDelay();
      const init: RequestInit = {
        method: request.method ?? "GET",
        headers: proxyHeaders(request.headers),
        redirect: "manual",
      };
      if (chunks.length > 0) init.body = new Uint8Array(Buffer.concat(chunks));
      const upstream = await fetch(`${this.upstream}${request.url ?? "/"}`, init);
      const snapshotMatch = /^\/api\/v1\/sessions\/[^/]+\/snapshots\/([^/?]+)/u.exec(
        new URL(request.url ?? "/", this.upstream).pathname,
      );
      if (
        this.role === "host" &&
        request.method === "PUT" &&
        snapshotMatch?.[1] !== undefined &&
        (upstream.status === 200 || upstream.status === 201)
      ) {
        const atUnixNs = unixNanoseconds();
        this.trace.finalizedSnapshotId = snapshotMatch[1];
        this.trace.snapshotFinalizations.push({ snapshotId: snapshotMatch[1], atUnixNs });
        this.trace.event("host.snapshot-finalized", `snapshot-${snapshotMatch[1]}`, atUnixNs, {
          snapshotId: snapshotMatch[1],
        });
      }
      const body = Buffer.from(await upstream.arrayBuffer());
      await this.networkDelay();
      response.statusCode = upstream.status;
      response.statusMessage = upstream.statusText;
      upstream.headers.forEach((value, name) => {
        if (!HOP_HEADERS.has(name.toLowerCase())) response.setHeader(name, value);
      });
      response.end(body);
    } catch (error: unknown) {
      response.statusCode = 502;
      response.end(error instanceof Error ? error.message : String(error));
    }
  }

  private async handleWebSocket(request: IncomingMessage, downstream: WebSocket): Promise<void> {
    const target = `${this.upstream}${request.url ?? "/"}`.replace(/^http/u, "ws");
    const protocols = String(request.headers["sec-websocket-protocol"] ?? "")
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
    const upstream = new WebSocket(target, protocols, {
      headers: proxyHeaders(request.headers, true),
      perMessageDeflate: false,
      maxPayload: 16 * 1024 * 1024,
    });
    const upstreamOpened = new Promise<void>((resolvePromise, reject) => {
      upstream.once("open", () => resolvePromise());
      upstream.once("error", reject);
    });
    const awaitOpen = async (socket: WebSocket): Promise<void> => {
      if (socket.readyState !== WebSocket.CONNECTING) return;
      await new Promise<void>((resolvePromise, reject) => {
        socket.once("open", () => resolvePromise());
        socket.once("error", reject);
        socket.once("close", () => reject(new JourneyError("WebSocket closed before opening")));
      });
    };
    const forward = (source: WebSocket, destination: WebSocket, toCloud: boolean): void => {
      source.on(
        "message",
        (payload, isBinary) =>
          void (async () => {
            let frame: Data | null = null;
            if (!isBinary) {
              try {
                const parsed = parseJson(rawData(payload).toString());
                if (isData(parsed)) frame = parsed;
              } catch {}
            }
            const cloudSendAtUnixNs = unixNanoseconds();
            await this.networkDelay();
            const hostReceiveAtUnixNs = unixNanoseconds();
            let sample: string | null = null;
            if (this.role === "browser" && toCloud && frame !== null) {
              ({ sample } = this.trace.browserFrame(frame, hostReceiveAtUnixNs));
              const held = sample === null ? undefined : this.trace.held.get(sample);
              if (sample !== null && held !== undefined) {
                held.set();
                await this.trace.releases.get(sample)!.promise;
              }
            }
            if (this.role === "host" && !toCloud && frame !== null) {
              this.trace.hostFrame(frame, cloudSendAtUnixNs, hostReceiveAtUnixNs);
              if (frame["type"] === "attach-request" && this.trace.holdNextSnapshotAttach) {
                this.trace.holdNextSnapshotAttach = false;
                this.trace.snapshotAttachHeld.set();
                await this.trace.snapshotAttachRelease.promise;
              }
            }
            await awaitOpen(destination);
            if (destination.readyState === WebSocket.OPEN)
              destination.send(payload, { binary: isBinary });
            if (
              this.role === "browser" &&
              toCloud &&
              sample !== null &&
              sample === this.trace.disconnectSample &&
              !this.trace.disconnectApplied
            ) {
              this.trace.disconnectApplied = true;
              destination.close(4001, "E0 acceptance uncertainty");
              source.close(4001, "E0 acceptance uncertainty");
            }
          })().catch(() => {
            source.terminate();
            destination.terminate();
          }),
      );
      source.on(
        "ping",
        (payload) => destination.readyState === WebSocket.OPEN && destination.ping(payload),
      );
      source.on(
        "pong",
        (payload) => destination.readyState === WebSocket.OPEN && destination.pong(payload),
      );
      source.on("close", (code, reason) => {
        if (destination.readyState >= WebSocket.CLOSING) return;
        // RFC 6455 reserves 1005/1006/1015 for local status reporting; ws rejects
        // attempts to put those values on the wire.
        if (code === 1000 || (code >= 3000 && code <= 4999)) destination.close(code, reason);
        else destination.terminate();
      });
      source.on("error", () => destination.terminate());
    };
    forward(downstream, upstream, true);
    forward(upstream, downstream, false);
    await upstreamOpened;
  }
}

async function freePort(): Promise<number> {
  const server = createNetServer();
  await new Promise<void>((resolvePromise, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolvePromise());
  });
  const address = server.address();
  if (address === null || typeof address === "string")
    throw new JourneyError("could not allocate a local port");
  await new Promise<void>((resolvePromise) => server.close(() => resolvePromise()));
  return address.port;
}

async function waitForHttp(
  url: string,
  managed: ManagedProcess,
  timeoutSeconds: number,
): Promise<void> {
  const deadline = performance.now() + timeoutSeconds * 1_000;
  while (performance.now() < deadline) {
    managed.assertRunning();
    try {
      if ((await fetch(url)).status === 200) return;
    } catch {}
    await delay(100);
  }
  throw new JourneyError("local Workerd/Vite server did not become ready");
}

async function waitForJson(
  path: string,
  managed: ManagedProcess,
  timeoutSeconds: number,
): Promise<Data> {
  const deadline = performance.now() + timeoutSeconds * 1_000;
  while (performance.now() < deadline) {
    managed.assertRunning();
    try {
      const parsed = parseJson(readFileSync(path, "utf8"));
      if (isData(parsed)) return parsed;
    } catch {}
    await delay(100);
  }
  throw new JourneyError("Host did not write session bootstrap metadata");
}

function readRawJsonl(path: string): Data[] {
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8")
    .split(/\r?\n/u)
    .filter(Boolean)
    .map((line) => {
      const value = parseJson(line);
      if (!isData(value)) throw new JourneyError("fixture event log contains a non-object record");
      return value;
    });
}

function readJsonl(path: string, variant: string): Data[] {
  return readRawJsonl(path).map((event) => ({ ...event, variant }));
}

async function waitForFixtureEvent(
  path: string,
  name: string,
  sampleId: string,
  timeoutSeconds: number,
): Promise<void> {
  const deadline = performance.now() + timeoutSeconds * 1_000;
  while (performance.now() < deadline) {
    if (
      readRawJsonl(path).some((event) => event["name"] === name && event["sampleId"] === sampleId)
    )
      return;
    await delay(50);
  }
  throw new JourneyError(`fixture did not record ${name} for ${sampleId}`);
}

function gitOutput(...arguments_: string[]): string {
  return execFileSync("git", arguments_, { cwd: ROOT, encoding: "utf8" }).trim();
}

function runAuthorityOracle(samples: number): Data {
  const output = execFileSync(
    "node",
    [join(ROOT, "scripts", "e0_authority_oracle.mjs"), "--allow-failure"],
    {
      cwd: ROOT,
      env: { ...process.env, ZHONGDUAN_E0_SAMPLES: String(samples) },
      encoding: "utf8",
    },
  );
  const value = parseJson(output);
  if (!isData(value)) throw new JourneyError("authority oracle returned a non-object result");
  return value;
}

async function setSample(page: Page, sampleId: string | null, duplicate = false): Promise<void> {
  await page.evaluate(
    ({ sampleId: activeSample, duplicate: shouldDuplicate }) => {
      const harness = (window as unknown as HarnessWindow).__zhongduanE0;
      harness.currentSample = activeSample;
      harness.duplicateSample = shouldDuplicate ? activeSample : null;
    },
    { sampleId, duplicate },
  );
}

async function appendBrowserEvent(page: Page, name: string, sampleId: string): Promise<void> {
  await page.evaluate(
    ({ name: eventName, sampleId: eventSample }) => {
      (window as unknown as HarnessWindow).__zhongduanE0.events.push({
        name: eventName,
        sampleId: eventSample,
        atUnixNs: Math.round((performance.timeOrigin + performance.now()) * 1_000_000),
      });
    },
    { name, sampleId },
  );
}

async function dispatchPaste(
  page: Page,
  payload: string,
  sampleId: string,
  duplicate = false,
): Promise<void> {
  await setSample(page, sampleId, duplicate);
  await page.evaluate(
    (consumedSample) => (window as unknown as HarnessWindow).__zhongduanE0.consume(consumedSample),
    sampleId,
  );
  await appendBrowserEvent(page, "browser.input-consumed", sampleId);
  const dispatched = await page
    .locator('[data-testid="wterm-surface"] textarea')
    .evaluate((textarea, pastePayload) => {
      const event = new Event("paste", { bubbles: true, cancelable: true });
      Object.defineProperty(event, "clipboardData", {
        value: {
          getData: (format: string) =>
            format === "text" || format === "text/plain" ? pastePayload : "",
        },
      });
      return textarea.dispatchEvent(event);
    }, payload);
  if (dispatched !== false)
    throw new JourneyError("WTerm did not consume the deterministic paste event");
  await page.waitForFunction(
    (sentSample) =>
      (window as unknown as HarnessWindow).__zhongduanE0.events.some(
        (event) => event["name"] === "browser.send-decision" && event["sampleId"] === sentSample,
      ),
    sampleId,
  );
  await setSample(page, null);
}

async function waitForGrid(page: Page, sentinel: string, timeoutMs: number): Promise<void> {
  await page.waitForFunction(
    (expected) => (document.querySelector(".term-grid")?.textContent ?? "").includes(expected),
    sentinel,
    { timeout: timeoutMs },
  );
}

async function markRender(page: Page, sampleId: string, matching = true): Promise<void> {
  await page.evaluate(
    () =>
      new Promise<void>((resolvePromise) =>
        requestAnimationFrame(() => requestAnimationFrame(() => resolvePromise())),
      ),
  );
  if (matching) await appendBrowserEvent(page, "browser.matching-render", sampleId);
  await appendBrowserEvent(page, "browser.useful-render", sampleId);
}

async function intentObservation(page: Page, sampleId: string, timeoutMs: number): Promise<Data> {
  try {
    await page.waitForFunction(
      (observedSample) => {
        const intent = (window as unknown as HarnessWindow).__zhongduanE0.intentsBySample[
          observedSample
        ];
        return intent !== undefined && intent.terminal !== null;
      },
      sampleId,
      { timeout: Math.min(timeoutMs, 5_000) },
    );
  } catch {}
  const intent = await page.evaluate((observedSample) => {
    const intent = (window as unknown as HarnessWindow).__zhongduanE0.intentsBySample[
      observedSample
    ];
    if (intent === undefined) return null;
    return {
      sampleId: intent.sampleId,
      localIntentId: intent.localIntentId,
      terminal: intent.terminal === null ? null : { ...intent.terminal },
      browserIdentities: [...new Set(intent.sends.map((send) => send.identity))].sort(),
      sendAttemptCount: intent.sends.length,
    };
  }, sampleId);
  if (!isData(intent) || typeof intent["localIntentId"] !== "string") {
    throw new JourneyError(`sample ${sampleId} was not recorded at UI consumption`);
  }
  const terminalRecords = isData(intent["terminal"]) ? [intent["terminal"]] : [];
  return {
    sampleId,
    localIntentId: intent["localIntentId"],
    consumed: true,
    terminalOutcomes: terminalRecords.map((record) => record["outcome"]),
    terminalRecords,
    passiveBrowserIdentities: Array.isArray(intent["browserIdentities"])
      ? intent["browserIdentities"]
      : [],
    passiveSendAttemptCount: intent["sendAttemptCount"] ?? 0,
  };
}

async function runProbe(page: Page, sampleId: string, timeoutMs: number): Promise<Data> {
  await dispatchPaste(page, `${PROBE_PREFIX.toString("ascii")}${sampleId}\r`, sampleId);
  await waitForGrid(page, `${RESULT_PREFIX}${sampleId}`, timeoutMs);
  await markRender(page, sampleId);
  return intentObservation(page, sampleId, timeoutMs);
}

async function runFaultCommand(
  page: Page,
  payload: string,
  sampleId: string,
  timeoutMs: number,
): Promise<Data> {
  await dispatchPaste(page, payload, sampleId);
  const observation = await intentObservation(page, sampleId, timeoutMs);
  let matchingOutputObserved = false;
  try {
    await waitForGrid(page, `${RESULT_PREFIX}${sampleId}`, Math.min(timeoutMs, 5_000));
    await markRender(page, sampleId);
    matchingOutputObserved = true;
  } catch (error: unknown) {
    if (!(error instanceof playwrightErrors.TimeoutError)) throw error;
  }
  observation["matchingOutputObserved"] = matchingOutputObserved;
  return observation;
}

async function runCtrlC(
  page: Page,
  sampleId: string,
  timeoutMs: number,
  trace: TraceStore,
  outputFlood: boolean,
  duplicate = false,
): Promise<{ intents: Data[]; ctrlC: Data }> {
  const intents: Data[] = [];
  if (outputFlood) {
    const floodSample = `flood-command-${sampleId}`;
    await dispatchPaste(page, `${FLOOD_PREFIX.toString("ascii")}${floodSample}\r`, floodSample);
    await waitForGrid(page, `E0-FLOOD-${floodSample}-`, timeoutMs);
    intents.push(await intentObservation(page, floodSample, timeoutMs));
  }
  const armSample = `arm-${sampleId}`;
  await dispatchPaste(
    page,
    `${INTERRUPT_ARM_PREFIX.toString("ascii")}${sampleId}:${armSample}\r`,
    armSample,
  );
  intents.push(await intentObservation(page, armSample, timeoutMs));
  trace.pendingCtrlSample = sampleId;
  await page.keyboard.down("Control");
  await setSample(page, sampleId, duplicate);
  await page.keyboard.down("KeyC");
  await setSample(page, null);
  await page.keyboard.up("KeyC");
  await page.keyboard.up("Control");
  await waitForGrid(page, `${QUIET_PREFIX}${sampleId}`, timeoutMs);
  await page.evaluate(
    () =>
      new Promise<void>((resolvePromise) =>
        requestAnimationFrame(() => requestAnimationFrame(() => resolvePromise())),
      ),
  );
  await appendBrowserEvent(page, "browser.application-quiet", sampleId);
  await markRender(page, sampleId);
  intents.push(await intentObservation(page, sampleId, timeoutMs));
  return { intents, ctrlC: { sampleId, outputFlood } };
}

async function waitLiveWriter(page: Page, timeoutMs: number): Promise<void> {
  await page.locator("main[data-phase='live']").waitFor({ timeout: timeoutMs });
  await page.locator(".ownership-button[data-owned='true']").waitFor({ timeout: timeoutMs });
  const textarea = page.locator('[data-testid="wterm-surface"] textarea');
  await textarea.waitFor({ timeout: timeoutMs });
  await textarea.focus();
}

async function waitLiveObserver(page: Page, timeoutMs: number): Promise<void> {
  await page.locator("main[data-phase='live']").waitFor({ timeout: timeoutMs });
  await page.locator('[data-testid="wterm-surface"] textarea').waitFor({ timeout: timeoutMs });
}

async function waitForSnapshotCount(
  trace: TraceStore,
  minimum: number,
  timeoutMs: number,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (trace.snapshotFinalizations.length < minimum) {
    if (performance.now() >= deadline)
      throw new JourneyError(`snapshot-enabled workload finalized fewer than ${minimum} snapshots`);
    await delay(50);
  }
}

async function waitForInitialSnapshotExpiry(trace: TraceStore, timeoutMs: number): Promise<void> {
  const first = trace.snapshotFinalizations[0];
  if (first === undefined)
    throw new JourneyError("snapshot-enabled workload lacks an initial checkpoint");
  const finalizedAt =
    typeof first["atUnixNs"] === "bigint" ? first["atUnixNs"] : BigInt(String(first["atUnixNs"]));
  const elapsedMs = Number(unixNanoseconds() - finalizedAt) / 1_000_000;
  const remainingMs =
    SNAPSHOT_CHECKPOINT_TTL_MS + SNAPSHOT_CHECKPOINT_EXPIRY_CUSHION_MS - elapsedMs;
  if (remainingMs <= 0) return;
  if (remainingMs >= timeoutMs)
    throw new JourneyError("snapshot checkpoint expiry exceeds the scenario deadline");
  await delay(remainingMs);
}

interface BrowserJourneyResult {
  observations: Data;
  warmupObservations: Data;
  browserEvents: Data[];
  workloadEvidence: Data;
}

async function browserJourney(
  browserOrigin: string,
  session: Data,
  trace: TraceStore,
  samples: number,
  warmups: number,
  variant: string,
  bulkBacklogBytes: number,
  timeoutMs: number,
): Promise<BrowserJourneyResult> {
  const launchOptions =
    process.env["PLAYWRIGHT_CHROMIUM_EXECUTABLE"] === undefined
      ? { headless: true }
      : { headless: true, executablePath: process.env["PLAYWRIGHT_CHROMIUM_EXECUTABLE"] };
  const browser = await chromium.launch(launchOptions);
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  const page = await context.newPage();
  const browserMessages: string[] = [];
  page.on("console", (message) =>
    browserMessages.push(`console:${message.type()}:${message.text()}`.slice(0, 2_000)),
  );
  page.on("pageerror", (error) => browserMessages.push(`pageerror:${error}`.slice(0, 2_000)));
  await page.addInitScript({ content: INIT_SCRIPT });
  const journeyPages: Page[] = [page];
  const snapshotRequested = new Signal();
  const releaseSnapshot = new Signal();
  await page.route(`${browserOrigin}/api/v1/sessions/*/snapshots/*`, async (route, request) => {
    if (request.method() !== "GET") {
      await route.continue();
      return;
    }
    snapshotRequested.set();
    await releaseSnapshot.promise;
    await route.continue();
  });
  const target = `${browserOrigin}/sessions/${String(session["sessionId"])}#capability=${String(session["writerCapability"])}`;
  let observerPage: Page | null = null;
  let observerNavigation: Promise<unknown> | null = null;
  let activePage = page;
  let stage = "navigate";
  try {
    await page.goto(target, { waitUntil: "commit", timeout: timeoutMs });
    stage = "await-snapshot-or-live";
    const firstState = await withTimeout(
      Promise.race([
        snapshotRequested.promise.then(() => "snapshot" as const),
        waitForGrid(page, READY_SENTINEL, timeoutMs).then(() => "live" as const),
      ]),
      timeoutMs,
      "Browser neither requested a snapshot nor reached live output",
    );
    const coldSnapshotExercised = firstState === "snapshot";
    const premature = coldSnapshotExercised
      ? await page
          .locator(".term-grid")
          .evaluate((grid, sentinel) => (grid.textContent ?? "").includes(sentinel), READY_SENTINEL)
      : null;
    releaseSnapshot.set();
    stage = "await-ready-after-snapshot";
    await waitForGrid(page, READY_SENTINEL, timeoutMs);
    stage = "await-live-writer";
    await waitLiveWriter(page, timeoutMs);
    if (variant === "snapshot-disabled" || variant === "snapshot-enabled")
      await waitForSnapshotCount(trace, 1, timeoutMs);

    const observations: Data = {
      intents: [],
      ctrlC: [],
      writerTransfers: [],
      coldCandidates: [],
      secureInput: [],
    };
    const warmupObservations: Data = { intents: [], ctrlC: [] };
    const measuredIntents = observations["intents"] as Data[];
    const measuredCtrlC = observations["ctrlC"] as Data[];
    const warmupIntents = warmupObservations["intents"] as Data[];
    const warmupCtrlC = warmupObservations["ctrlC"] as Data[];
    let acceptanceReconnectObserved = false;
    const primarySampleIds: string[] = [];
    const warmupPrimarySampleIds: string[] = [];
    const warmupSampleIds = new Set<string>();

    const executeSample = async (index: number, warmup: boolean): Promise<void> => {
      const prefix = warmup ? "warmup" : "measured";
      const primary = `${prefix}-${variant}-${String(index).padStart(3, "0")}`;
      if (warmup) {
        warmupSampleIds.add(primary);
        warmupPrimarySampleIds.push(primary);
      } else {
        primarySampleIds.push(primary);
      }
      const intentTarget = warmup ? warmupIntents : measuredIntents;
      const ctrlTarget = warmup ? warmupCtrlC : measuredCtrlC;
      if (
        ["steady", "snapshot-disabled", "snapshot-enabled", "correctness-faults"].includes(variant)
      ) {
        const probeSample = `probe-${primary}`;
        if (warmup) warmupSampleIds.add(probeSample);
        intentTarget.push(await runProbe(page, probeSample, timeoutMs));
      } else if (variant.startsWith("bulk-backlog-")) {
        if (bulkBacklogBytes > 0) {
          const floodSample = `flood-command-${primary}`;
          if (warmup) warmupSampleIds.add(floodSample);
          await dispatchPaste(
            page,
            `${FLOOD_PREFIX.toString("ascii")}${floodSample}\r`,
            floodSample,
          );
          await waitForGrid(page, `E0-FLOOD-${floodSample}-`, timeoutMs);
          intentTarget.push(await intentObservation(page, floodSample, timeoutMs));
        }
        const probeSample = `probe-${primary}`;
        if (warmup) warmupSampleIds.add(probeSample);
        intentTarget.push(await runProbe(page, probeSample, timeoutMs));
      } else if (variant === "output-flood") {
        const ctrlSample = `ctrl-c-${primary}`;
        const result = await runCtrlC(page, ctrlSample, timeoutMs, trace, true);
        if (warmup) {
          for (const item of result.intents) warmupSampleIds.add(String(item["sampleId"]));
          warmupSampleIds.add(ctrlSample);
        }
        intentTarget.push(...result.intents);
        ctrlTarget.push(result.ctrlC);
      } else {
        throw new JourneyError(`unsupported E0 workload variant ${variant}`);
      }
      if (variant === "steady") {
        const ctrlSample = `ctrl-c-${primary}`;
        const result = await runCtrlC(page, ctrlSample, timeoutMs, trace, false);
        if (warmup) {
          for (const item of result.intents) warmupSampleIds.add(String(item["sampleId"]));
          warmupSampleIds.add(ctrlSample);
        }
        intentTarget.push(...result.intents);
        ctrlTarget.push(result.ctrlC);
      }
    };

    for (let index = 0; index < warmups; index += 1) await executeSample(index, true);
    const snapshotCountBeforeMeasurement = trace.snapshotFinalizations.length;
    const measurementStartedAtUnixMs = Date.now();
    const snapshotPostMidpointSampleIds = new Set<string>();
    let snapshotInputOverlap: Data | null = null;
    const midpoint = Math.floor(samples / 2);
    const attachIndex = samples - SNAPSHOT_ATTACH_REMAINING_SAMPLES;
    for (let index = 0; index < samples; index += 1) {
      const probeSample = `probe-measured-${variant}-${String(index).padStart(3, "0")}`;
      if (variant === "snapshot-enabled" && index >= midpoint) {
        snapshotPostMidpointSampleIds.add(probeSample);
        if (index > midpoint) {
          stage = "pace-snapshot-overlap-inputs";
          await delay(SNAPSHOT_OVERLAP_SAMPLE_INTERVAL_MS);
        }
      }
      if (variant === "snapshot-enabled" && index === attachIndex) {
        stage = "await-expired-snapshot-checkpoint";
        await waitForInitialSnapshotExpiry(trace, timeoutMs);
        stage = "coordinate-snapshot-attach-with-input";
        observerPage = await context.newPage();
        journeyPages.push(observerPage);
        await observerPage.addInitScript({ content: INIT_SCRIPT });
        const observerTarget = `${browserOrigin}/sessions/${String(session["sessionId"])}#capability=${String(session["observerCapability"])}`;
        trace.snapshotAttachHeld = new Signal();
        trace.snapshotAttachRelease = new Signal();
        trace.holdNextSnapshotAttach = true;
        observerNavigation = observerPage.goto(observerTarget, {
          waitUntil: "commit",
          timeout: timeoutMs,
        });
        await withTimeout(
          trace.snapshotAttachHeld.promise,
          timeoutMs,
          "snapshot attach was not held",
        );
        const { held, release } = trace.hold(probeSample);
        const sampleTask = executeSample(index, false);
        try {
          await withTimeout(held.promise, timeoutMs, "snapshot overlap input did not reach Cloud");
          trace.snapshotAttachRelease.set();
          release.set();
          await sampleTask;
        } catch (error: unknown) {
          trace.snapshotAttachRelease.set();
          release.set();
          throw error;
        }
      } else {
        await executeSample(index, false);
      }
    }

    if (variant === "snapshot-enabled") {
      if (observerPage === null || observerNavigation === null)
        throw new JourneyError("snapshot observer was not created");
      await observerNavigation;
      await waitLiveObserver(observerPage, timeoutMs);
      await waitForSnapshotCount(trace, snapshotCountBeforeMeasurement + 1, timeoutMs);
      const postMidpointReceives = trace.events
        .filter(
          (event) =>
            event["name"] === "host.receive" &&
            snapshotPostMidpointSampleIds.has(String(event["sampleId"])),
        )
        .sort((left, right) =>
          Number(BigInt(String(left["atUnixNs"])) - BigInt(String(right["atUnixNs"]))),
        );
      const newFinalizations = trace.snapshotFinalizations.slice(snapshotCountBeforeMeasurement);
      if (postMidpointReceives.length < 2)
        throw new JourneyError("snapshot overlap lacks post-midpoint Host inputs");
      const firstReceive = postMidpointReceives[0]!;
      const lastReceive = postMidpointReceives.at(-1)!;
      const firstAt = BigInt(String(firstReceive["atUnixNs"]));
      const lastAt = BigInt(String(lastReceive["atUnixNs"]));
      const overlapped = newFinalizations.find((item) => {
        const at = BigInt(String(item["atUnixNs"]));
        return firstAt < at && at < lastAt;
      });
      if (overlapped === undefined)
        throw new JourneyError("snapshot finalization did not overlap post-midpoint Host inputs");
      snapshotInputOverlap = {
        firstHostReceiveAtUnixNs: firstReceive["atUnixNs"],
        firstSampleId: firstReceive["sampleId"],
        snapshotFinalizationAtUnixNs: overlapped["atUnixNs"],
        snapshotId: overlapped["snapshotId"],
        lastHostReceiveAtUnixNs: lastReceive["atUnixNs"],
        lastSampleId: lastReceive["sampleId"],
      };
    }

    if (variant === "correctness-faults") {
      (observations["coldCandidates"] as Data[]).push({
        sampleId: "cold-attach-000",
        visibleBeforeValidation: premature,
      });
      const duplicateSample = "duplicate-000";
      await dispatchPaste(
        page,
        `${PROBE_PREFIX.toString("ascii")}${duplicateSample}\r`,
        duplicateSample,
        true,
      );
      await waitForGrid(page, `${RESULT_PREFIX}${duplicateSample}`, timeoutMs);
      await markRender(page, duplicateSample);
      measuredIntents.push(await intentObservation(page, duplicateSample, timeoutMs));

      const uncertaintySample = "uncertain-000";
      trace.disconnectSample = uncertaintySample;
      await dispatchPaste(
        page,
        `${PROBE_PREFIX.toString("ascii")}${uncertaintySample}\r`,
        uncertaintySample,
      );
      stage = "await-uncertain-reconnect";
      await page.locator("main[data-phase='reconnecting']").waitFor({ timeout: timeoutMs });
      await waitLiveWriter(page, timeoutMs);
      acceptanceReconnectObserved = true;
      stage = "observe-uncertain-no-retry";
      await page.waitForTimeout(1_000);
      const uncertainty = await intentObservation(page, uncertaintySample, timeoutMs);
      uncertainty["acceptanceUncertaintyInjected"] = true;
      measuredIntents.push(uncertainty);

      stage = "exercise-writer-transfer";
      const oldSample = "old-writer-000";
      const heldSignals = trace.hold(oldSample);
      const pasteTask = dispatchPaste(
        page,
        `${PROBE_PREFIX.toString("ascii")}${oldSample}\r`,
        oldSample,
      );
      await withTimeout(
        heldSignals.held.promise,
        timeoutMs,
        "old writer frame did not reach Cloud",
      );
      const storage = await page.evaluate(() => Object.fromEntries(Object.entries(sessionStorage)));
      const secondPage = await context.newPage();
      journeyPages.push(secondPage);
      await secondPage.addInitScript({ content: INIT_SCRIPT });
      await secondPage.addInitScript({
        content: `(() => { const entries = ${JSON.stringify(storage)}; for (const [key, value] of Object.entries(entries)) sessionStorage.setItem(key, value); })();`,
      });
      await secondPage.goto(target, { waitUntil: "commit", timeout: timeoutMs });
      await waitLiveWriter(secondPage, timeoutMs);
      heldSignals.release.set();
      await pasteTask;
      measuredIntents.push(await intentObservation(page, oldSample, timeoutMs));
      (observations["writerTransfers"] as Data[]).push({
        sampleId: "writer-transfer-000",
        oldWriterSuccessfulEffects: 0,
      });
      activePage = secondPage;
      const newSample = "new-writer-000";
      measuredIntents.push(
        await runFaultCommand(
          activePage,
          `${PROBE_PREFIX.toString("ascii")}${newSample}\r`,
          newSample,
          timeoutMs,
        ),
      );
      const secureSample = "secure-000";
      measuredIntents.push(
        await runFaultCommand(
          activePage,
          `${SECURE_PREFIX.toString("ascii")}${secureSample}\r`,
          secureSample,
          timeoutMs,
        ),
      );
      const speculativePresentationCount = await activePage
        .locator(
          "[data-presentation='speculative'], [data-speculative='true'], .speculative-presentation",
        )
        .count();
      (observations["secureInput"] as Data[]).push({
        sampleId: secureSample,
        speculativePresentationCount,
      });
    }

    const measurementEndedAtUnixMs = Date.now();
    const browserEvents: Data[] = [];
    for (const sourcePage of journeyPages) {
      const pageEvents = await sourcePage.evaluate(
        () => (window as unknown as HarnessWindow).__zhongduanE0.events,
      );
      if (Array.isArray(pageEvents)) browserEvents.push(...pageEvents.filter(isData));
    }
    const filteredBrowserEvents = browserEvents.filter(
      (event) => !warmupSampleIds.has(String(event["sampleId"])),
    );
    const snapshotDelta = trace.snapshotFinalizations.length - snapshotCountBeforeMeasurement;
    return {
      observations,
      warmupObservations,
      browserEvents: filteredBrowserEvents,
      workloadEvidence: {
        primarySampleIds,
        warmupPrimarySampleIds,
        warmupCount: warmups,
        configuredBulkBacklogBytes: bulkBacklogBytes,
        snapshotFinalizationsDuringMeasurement: snapshotDelta,
        snapshotInputOverlap,
        measurementStartedAtUnixMs,
        measurementEndedAtUnixMs,
        outputFlood:
          variant === "output-flood" ||
          (variant.startsWith("bulk-backlog-") && bulkBacklogBytes > 0),
        acceptanceDisconnect: trace.disconnectApplied,
        acceptanceReconnectObserved,
        writerTransfer: (observations["writerTransfers"] as Data[]).length > 0,
        coldAttachValidation:
          (observations["coldCandidates"] as Data[]).length > 0 && coldSnapshotExercised,
        coldSnapshotRequested: coldSnapshotExercised,
      },
    };
  } catch (error: unknown) {
    const browserState: unknown[] = [];
    for (const diagnosticPage of journeyPages) {
      try {
        browserState.push(
          await diagnosticPage.evaluate(() => {
            const harness = (window as unknown as Partial<HarnessWindow>).__zhongduanE0;
            return {
              mainPhase: document.querySelector("main")?.getAttribute("data-phase") ?? null,
              gridText: (document.querySelector(".term-grid")?.textContent ?? "").slice(-4096),
              ownership:
                document.querySelector(".ownership-button")?.getAttribute("data-owned") ?? null,
              inputSurfaceTextareaCount: document.querySelectorAll(
                '[data-testid="wterm-surface"] textarea',
              ).length,
              activeElement: document.activeElement?.tagName ?? null,
              e0Events: (harness?.events ?? []).slice(-50),
              e0Intents: (harness?.intents ?? []).slice(-10),
            };
          }),
        );
      } catch (stateError: unknown) {
        browserState.push({ captureError: String(stateError) });
      }
    }
    throw new JourneyError(
      `browser stage ${stage} failed: ${String(error)}; state=${stringifyJson(browserState)}; messages=${stringifyJson(browserMessages.slice(-20))}`,
    );
  } finally {
    releaseSnapshot.set();
    trace.snapshotAttachRelease.set();
    await context.close().catch(() => undefined);
    await browser.close().catch(() => undefined);
  }
}

export function updateObservationEffects(
  observations: Data,
  trace: TraceStore,
  fixtureEvents: Data[],
): void {
  const effects = new Map<string, number>();
  for (const event of fixtureEvents) {
    if (event["name"] === "host.pty-write" && typeof event["sampleId"] === "string")
      increment(effects, event["sampleId"]);
  }
  const intents = Array.isArray(observations["intents"])
    ? observations["intents"].filter(isData)
    : [];
  for (const item of intents) {
    const sample = String(item["sampleId"] ?? item["localIntentId"]);
    const browserIdentities = [...(trace.sampleBrowserIdentities.get(sample) ?? [])].sort();
    const identities = [...(trace.sampleWireIdentities.get(sample) ?? [])].sort();
    item["ptyEffectCount"] = effects.get(sample) ?? 0;
    if (browserIdentities.length > 0) {
      item["browserIdentity"] = [...trace.identitySamples.entries()].find(
        ([, retained]) => retained === sample,
      )?.[0];
      item["browserIdentities"] = browserIdentities;
    }
    if (identities.length > 0) {
      item["wireIdentity"] = trace.sampleIdentities.get(sample);
      item["wireIdentities"] = identities;
    }
    if (item["acceptanceUncertaintyInjected"] === true) {
      const totalSends = browserIdentities.reduce(
        (sum, identity) => sum + (trace.browserSendCounts.get(identity) ?? 0),
        0,
      );
      item["automaticRetryCount"] = Math.max(0, totalSends - 1);
      item["identityChanged"] = browserIdentities.length > 1;
    }
  }
  const ctrlC = Array.isArray(observations["ctrlC"]) ? observations["ctrlC"].filter(isData) : [];
  for (const item of ctrlC) {
    const sample = String(item["sampleId"]);
    const identity = trace.sampleIdentities.get(sample);
    if (identity !== undefined) item["wireIdentity"] = identity;
    item["ptyEffectCount"] = effects.get(sample) ?? 0;
  }
  const transfers = Array.isArray(observations["writerTransfers"])
    ? observations["writerTransfers"].filter(isData)
    : [];
  if (transfers[0] !== undefined)
    transfers[0]["oldWriterSuccessfulEffects"] = effects.get("old-writer-000") ?? 0;
}

export interface RunnerArgs {
  browserCloudRttMs: number;
  cloudHostRttMs: number;
  jitterMs: number;
  samples: number;
  warmups: number;
  seed: number;
  timeoutSeconds: number;
  variant: string;
  networkFault: "none" | "jitter";
  report: string;
  mergeScenarios: string[] | null;
  artifactKind: "current" | "candidate";
  currentReport: string | null;
  e4bDecision: string | null;
  allowDirtyDevelopment: boolean;
  matrixPlan: boolean;
}

function nested(object: Data, field: string, label: string): Data {
  const value = object[field];
  if (!isData(value)) throw new JourneyError(`${label} is missing`);
  return value;
}

export async function runScenario(args: RunnerArgs): Promise<Data> {
  const contract = loadJson(CONTRACT_PATH);
  validateContract(contract);
  const sourceRevision = gitOutput("rev-parse", "HEAD");
  const sourceTreeGitOid = gitOutput("rev-parse", "HEAD^{tree}");
  const sourceTreeDirty = gitOutput("status", "--porcelain=v1", "--untracked-files=all") !== "";
  if (sourceTreeDirty && !args.allowDirtyDevelopment) {
    throw new JourneyError(
      "E0 scenarios must run from a clean committed tree; --allow-dirty-development may only produce an unmergeable development artifact",
    );
  }
  if (existsSync(DEV_VARS))
    throw new JourneyError(`refusing to overwrite existing local bindings: ${DEV_VARS}`);
  if (!existsSync(VP))
    throw new JourneyError(
      "dependencies are not installed; run pnpm install and pnpm exec playwright install chromium",
    );
  const temporary = mkdtempSync(join(tmpdir(), "zhongduan-e0-journey-"));
  const sessionInfo = join(temporary, "session.json");
  const capturePath = join(temporary, "input-capture.bin");
  const fixtureLog = join(temporary, "fixture-events.jsonl");
  const hostMeasurementsLog = join(temporary, "host-measurements.jsonl");
  const trace = new TraceStore(args.variant);
  const generatedPaths = [join(APP_ROOT, ".wrangler"), dirname(HOST_CLI)];
  const preexisting = new Set(generatedPaths.filter(existsSync));
  let workerd: ManagedProcess | null = null;
  let host: ManagedProcess | null = null;
  let browserProxy: LinkProxy | null = null;
  let hostProxy: LinkProxy | null = null;
  let devVarsCreated = false;
  try {
    writeFileSync(
      DEV_VARS,
      `BOOTSTRAP_TOKEN="${BOOTSTRAP_TOKEN}"\nCAPABILITY_SIGNING_KEY="${CAPABILITY_KEY}"\n`,
      { encoding: "utf8", flag: "wx" },
    );
    devVarsCreated = true;
    chmodSync(DEV_VARS, 0o600);
    execFileSync(process.execPath, [VP, "run", "build"], { cwd: ROOT, stdio: "inherit" });
    const workerdPort = await freePort();
    const workerdOrigin = `http://127.0.0.1:${workerdPort}`;
    workerd = new ManagedProcess(
      "Vite Workerd",
      process.execPath,
      [
        VP,
        "dev",
        "--host",
        "127.0.0.1",
        "--port",
        String(workerdPort),
        "--strictPort",
        "--mode",
        "e0-journey",
      ],
      APP_ROOT,
      { ...process.env, ZHONGDUAN_E0_CLOUDFLARE_STATE_PATH: join(temporary, "workerd-state") },
    );
    await waitForHttp(workerdOrigin, workerd, args.timeoutSeconds);
    browserProxy = new LinkProxy(
      "browser",
      workerdOrigin,
      args.browserCloudRttMs,
      args.jitterMs,
      trace,
      args.seed,
    );
    hostProxy = new LinkProxy(
      "host",
      workerdOrigin,
      args.cloudHostRttMs,
      args.jitterMs,
      trace,
      args.seed + 1,
    );
    await browserProxy.start();
    await hostProxy.start();
    if (browserProxy.origin === null || hostProxy.origin === null)
      throw new JourneyError("link proxies failed to start");
    const outputFlood = nested(
      nested(contract, "workload", "contract workload"),
      "outputFlood",
      "output flood workload",
    );
    const bulkBacklogBytes = args.variant.startsWith("bulk-backlog-")
      ? Number(args.variant.slice("bulk-backlog-".length))
      : 0;
    const maximumFloodBytes =
      bulkBacklogBytes > 0 ? bulkBacklogBytes : Number(outputFlood["maximumBytes"]);
    const hostEnv: NodeJS.ProcessEnv = {
      ...process.env,
      ZHONGDUAN_BOOTSTRAP_TOKEN: BOOTSTRAP_TOKEN,
      [INPUT_CAPTURE_ENV]: capturePath,
      [EVENT_LOG_ENV]: fixtureLog,
      [E4_EVIDENCE_ENV]: hostMeasurementsLog,
      [FLOOD_CHUNK_BYTES_ENV]: String(outputFlood["chunkBytes"]),
      [FLOOD_MAX_BYTES_ENV]: String(maximumFloodBytes),
      [FLOOD_MAX_DURATION_MS_ENV]: String(outputFlood["maximumDurationMs"]),
    };
    if (args.variant === "snapshot-disabled") hostEnv[E0_DISABLE_SNAPSHOT_REFRESH_ENV] = "1";
    host = new ManagedProcess(
      "Host relay",
      process.execPath,
      [
        HOST_CLI,
        "cloud",
        "--url",
        hostProxy.origin,
        "--session-info-file",
        sessionInfo,
        "--",
        process.execPath,
        join(ROOT, "scripts", "e0-terminal-fixture.ts"),
      ],
      ROOT,
      hostEnv,
    );
    const session = await waitForJson(sessionInfo, host, args.timeoutSeconds);
    await withTimeout(
      trace.hostReadyAcknowledged.promise,
      args.timeoutSeconds * 1_000,
      "Host did not receive host-ready-ack before the Browser journey",
    );
    host.assertRunning();
    const journey = await browserJourney(
      browserProxy.origin,
      session,
      trace,
      args.samples,
      args.warmups,
      args.variant,
      bulkBacklogBytes,
      args.timeoutSeconds * 1_000,
    );
    if (bulkBacklogBytes > 0) {
      await waitForFixtureEvent(
        fixtureLog,
        "fixture.flood-stopped",
        `flood-command-measured-${args.variant}-${String(args.samples - 1).padStart(3, "0")}`,
        args.timeoutSeconds,
      );
    }
    host.assertRunning();
    workerd.assertRunning();
    const warmupSampleIds = new Set(
      [
        ...(journey.warmupObservations["intents"] as Data[]),
        ...(journey.warmupObservations["ctrlC"] as Data[]),
      ]
        .map((item) => item["sampleId"])
        .filter((sample): sample is string => typeof sample === "string"),
    );
    const allFixtureEvents = readJsonl(fixtureLog, args.variant);
    const fixtureEvents = allFixtureEvents.filter(
      (event) => !warmupSampleIds.has(String(event["sampleId"])),
    );
    trace.events = trace.events.filter((event) => !warmupSampleIds.has(String(event["sampleId"])));
    updateObservationEffects(journey.observations, trace, fixtureEvents);
    updateObservationEffects(journey.warmupObservations, trace, allFixtureEvents);
    const events = [
      ...trace.events,
      ...fixtureEvents,
      ...journey.browserEvents.map((event) => ({ ...event, variant: args.variant })),
    ];
    const report: Data = {
      schemaVersion: "zhongduan-terminal-journey-scenario-v1",
      contractSha256: canonicalSha256(contract),
      status: "measured",
      sourceRevision,
      sourceTreeGitOid,
      sourceTreeDirty,
      generatedAt: new Date().toISOString().replace(/\.\d{3}Z$/u, "Z"),
      environment: {
        executionTier: "local-workerd",
        platform: `${process.platform}-${process.arch}`,
        node: process.version,
        harnessRuntime: "TypeScript on Node.js",
        wtermGitlink: gitOutput("rev-parse", "HEAD:vendor/wterm"),
      },
      variant: args.variant,
      samples: args.samples,
      warmups: args.warmups,
      appliedProfile: {
        browserCloudRttMs: args.browserCloudRttMs,
        cloudHostRttMs: args.cloudHostRttMs,
        jitterMs: args.jitterMs,
        networkFault: args.networkFault,
        seed: args.seed,
        networkImplementation: "two independent Node.js HTTP/WebSocket userspace proxies",
      },
      cloudSpanBoundary: {
        start: "browser proxy receive after configured Browser/Cloud link delay",
        end: "host proxy send before configured Cloud/Host link delay",
        includesBrowserLink: false,
        includesHostLink: false,
      },
      workloadEvidence: journey.workloadEvidence,
      observations: journey.observations,
      warmupObservations: journey.warmupObservations,
      hostMeasurements: readRawJsonl(hostMeasurementsLog),
      events,
      rawEventCount: events.length,
      deadlineMs: args.timeoutSeconds * 1_000,
      deadlineIsSlo: false,
    };
    validateScenarioReport(report, contract, { requireClean: !args.allowDirtyDevelopment });
    return report;
  } catch (error: unknown) {
    const diagnosticPath = join(tmpdir(), "zhongduan-e0-last-failure.json");
    const diagnostic = {
      schemaVersion: "zhongduan-e0-failure-diagnostic-v1",
      errorType: error instanceof Error ? error.name : typeof error,
      error: error instanceof Error ? error.message : String(error),
      variant: args.variant,
      sessionMetadataCreated: existsSync(sessionInfo),
      hostReadyAcknowledged: trace.hostReadyAcknowledged.settled,
      traceEvents: trace.events,
      snapshotFinalizations: trace.snapshotFinalizations,
      fixtureEvents: readRawJsonl(fixtureLog),
      hostMeasurements: readRawJsonl(hostMeasurementsLog),
      hostTail: host?.sanitizedTail() ?? [],
      workerdTail: workerd?.sanitizedTail() ?? [],
    };
    writeFileSync(diagnosticPath, `${stringifyJson(diagnostic, true)}\n`, "utf8");
    throw new JourneyError(`${String(error)}; diagnostics: ${diagnosticPath}`);
  } finally {
    await host?.stop();
    await browserProxy?.close().catch(() => undefined);
    await hostProxy?.close().catch(() => undefined);
    await workerd?.stop();
    if (devVarsCreated) rmSync(DEV_VARS, { force: true });
    rmSync(temporary, { recursive: true, force: true });
    for (const path of generatedPaths)
      if (!preexisting.has(path)) rmSync(path, { recursive: true, force: true });
  }
}

function argumentError(message: string): never {
  throw new JourneyError(`invalid arguments: ${message}`);
}

function numberArgument(raw: string | undefined, name: string): number {
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) argumentError(`${name} requires an integer`);
  return value;
}

export function parseArgs(argv = process.argv.slice(2)): RunnerArgs {
  const args: RunnerArgs = {
    browserCloudRttMs: 20,
    cloudHostRttMs: 20,
    jitterMs: 0,
    samples: 24,
    warmups: 4,
    seed: 450,
    timeoutSeconds: 120,
    variant: "steady",
    networkFault: "none",
    report: join(tmpdir(), "zhongduan-e0-scenario.json"),
    mergeScenarios: null,
    artifactKind: "current",
    currentReport: null,
    e4bDecision: null,
    allowDirtyDevelopment: false,
    matrixPlan: false,
  };
  const take = (index: number, option: string): string => {
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) argumentError(`${option} requires a value`);
    return value;
  };
  for (let index = 0; index < argv.length; index += 1) {
    const option = argv[index]!;
    if (option === "--allow-dirty-development") args.allowDirtyDevelopment = true;
    else if (option === "--matrix-plan") args.matrixPlan = true;
    else if (option === "--browser-cloud-rtt-ms")
      args.browserCloudRttMs = numberArgument(take(index++, option), option);
    else if (option === "--cloud-host-rtt-ms")
      args.cloudHostRttMs = numberArgument(take(index++, option), option);
    else if (option === "--jitter-ms")
      args.jitterMs = numberArgument(take(index++, option), option);
    else if (option === "--samples") args.samples = numberArgument(take(index++, option), option);
    else if (option === "--warmups") args.warmups = numberArgument(take(index++, option), option);
    else if (option === "--seed") args.seed = numberArgument(take(index++, option), option);
    else if (option === "--timeout-seconds")
      args.timeoutSeconds = numberArgument(take(index++, option), option);
    else if (option === "--variant") args.variant = take(index++, option);
    else if (option === "--network-fault")
      args.networkFault = take(index++, option) as RunnerArgs["networkFault"];
    else if (option === "--report") args.report = take(index++, option);
    else if (option === "--artifact-kind")
      args.artifactKind = take(index++, option) as RunnerArgs["artifactKind"];
    else if (option === "--current-report") args.currentReport = take(index++, option);
    else if (option === "--e4b-decision") args.e4bDecision = take(index++, option);
    else if (option === "--merge-scenarios") {
      const paths: string[] = [];
      while (index + 1 < argv.length && !argv[index + 1]!.startsWith("--"))
        paths.push(argv[++index]!);
      if (paths.length === 0) argumentError("--merge-scenarios requires at least one path");
      args.mergeScenarios = paths;
    } else if (option === "--help" || option === "-h") {
      process.stdout.write(
        "Usage: node scripts/verify-e0-terminal-journey.ts [--matrix-plan] [--variant NAME] [--merge-scenarios FILE...] [--artifact-kind current|candidate] [--report FILE]\n",
      );
      process.exit(0);
    } else argumentError(`unknown option ${option}`);
  }
  for (const [name, value, minimum] of [
    ["--browser-cloud-rtt-ms", args.browserCloudRttMs, 0],
    ["--cloud-host-rtt-ms", args.cloudHostRttMs, 0],
    ["--jitter-ms", args.jitterMs, 0],
    ["--samples", args.samples, 1],
    ["--warmups", args.warmups, 1],
    ["--timeout-seconds", args.timeoutSeconds, 1],
  ] as const) {
    if (value < minimum) argumentError(`${name} is outside the supported range`);
  }
  if (!EXPECTED_VARIANTS.has(args.variant)) argumentError(`unknown --variant ${args.variant}`);
  if (args.samples !== 24 || args.warmups !== 4)
    argumentError("checked E0 evidence requires exactly --samples 24 --warmups 4");
  if (args.networkFault !== "none" && args.networkFault !== "jitter")
    argumentError("--network-fault must be none or jitter");
  if (args.networkFault === "jitter" && args.jitterMs === 0)
    argumentError("--network-fault jitter requires a positive --jitter-ms");
  if (args.networkFault === "none" && args.jitterMs !== 0)
    argumentError("a non-zero --jitter-ms requires --network-fault jitter");
  if (args.artifactKind !== "current" && args.artifactKind !== "candidate")
    argumentError("--artifact-kind must be current or candidate");
  if (args.mergeScenarios === null && resolve(args.report) === resolve(BASELINE_PATH)) {
    argumentError("single scenarios cannot overwrite CURRENT; use --merge-scenarios");
  }
  if (
    args.artifactKind === "candidate" &&
    (args.mergeScenarios === null || args.currentReport === null)
  ) {
    argumentError("candidate merge requires --merge-scenarios and --current-report");
  }
  if (args.artifactKind === "current" && args.currentReport !== null)
    argumentError("--current-report is only valid with --artifact-kind candidate");
  if (args.e4bDecision !== null && args.artifactKind !== "candidate")
    argumentError("--e4b-decision is only valid with --artifact-kind candidate");
  return args;
}

export function mergeScenarioFiles(args: RunnerArgs, contract: Data): Data {
  const scenarios = (args.mergeScenarios ?? []).map(loadJson);
  if (scenarios.length === 0) throw new JourneyError("at least one scenario report is required");
  for (const scenario of scenarios) validateScenarioReport(scenario, contract);
  const first = scenarios[0]!;
  const sourceRevision = String(first["sourceRevision"]);
  const sourceTreeGitOid = String(first["sourceTreeGitOid"]);
  const environment = nested(first, "environment", "scenario environment");
  for (const scenario of scenarios.slice(1)) {
    if (
      scenario["sourceRevision"] !== sourceRevision ||
      scenario["sourceTreeGitOid"] !== sourceTreeGitOid
    ) {
      throw new JourneyError("all merged scenarios must come from the same E0 commit");
    }
    if (stringifyJson(scenario["environment"]) !== stringifyJson(environment)) {
      throw new JourneyError("all merged scenarios must use the same reproducible environment");
    }
  }
  const workload = nested(contract, "workload", "contract workload");
  const authority = runAuthorityOracle(Number(workload["samplesPerVariant"]));
  const mergedEnvironment: Data = {
    ...environment,
    ghosttyEngineId: authority["engineId"],
    ghosttyArtifactVerified: authority["artifactVerified"],
  };
  const evidenceBoundary: Data = {
    realCloudflareEdge: false,
    realComponents: [
      "Chromium",
      "WTerm/Ghostty WASM",
      "Browser application",
      "Vite Workerd/Miniflare Durable Object and R2",
      "Host daemon",
      "node-pty",
      "deterministic raw PTY child",
    ],
    simulated: ["link RTT", "link jitter", "acceptance-uncertainty disconnect"],
    notClaimed: [
      "Cloudflare edge latency",
      "real Durable Object hibernation",
      "real Host relay process replacement",
    ],
    cloudSpanBoundary: "after Browser/Cloud proxy delay through before Cloud/Host proxy delay",
  };
  const deadlinePolicy = nested(contract, "deadlinePolicy", "contract deadline policy");
  const common = {
    environment: mergedEnvironment,
    evidenceBoundary,
    generatedAt: new Date().toISOString().replace(/\.\d{3}Z$/u, "Z"),
    sourceRevision,
    sourceTreeGitOid,
    deadlineMs: Number(deadlinePolicy["scenarioDeadlineMs"]),
  };
  if (args.artifactKind === "current")
    return assembleCurrentReport(contract, scenarios, authority, common);
  if (args.currentReport === null) throw new JourneyError("candidate merge requires CURRENT");
  const currentReport = loadReportBundle(args.currentReport);
  const snapshotPhaseMeasurements = scenarios.flatMap((scenario) => {
    const evidence = isData(scenario["workloadEvidence"]) ? scenario["workloadEvidence"] : {};
    return (
      Array.isArray(scenario["hostMeasurements"]) ? scenario["hostMeasurements"].filter(isData) : []
    ).map((measurement) => ({
      ...measurement,
      scenarioVariant: scenario["variant"],
      measurementStartedAtUnixMs: evidence["measurementStartedAtUnixMs"],
      measurementEndedAtUnixMs: evidence["measurementEndedAtUnixMs"],
    }));
  });
  return assembleCandidateReport(contract, scenarios, authority, currentReport, {
    ...common,
    snapshotPhaseMeasurements,
  });
}

function statusCounts(report: Data): Record<string, number> {
  const counts: Record<string, number> = {};
  const oracles = isData(report["oracleResults"]) ? report["oracleResults"] : {};
  for (const value of Object.values(oracles)) {
    if (!isData(value)) continue;
    const status = String(value["status"]);
    counts[status] = (counts[status] ?? 0) + 1;
  }
  return counts;
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  const args = parseArgs(argv);
  const contract = loadJson(CONTRACT_PATH);
  validateContract(contract);
  if (args.matrixPlan) {
    process.stdout.write(`${stringifyJson(matrixCells(contract), true)}\n`);
    return;
  }
  const report =
    args.mergeScenarios === null ? await runScenario(args) : mergeScenarioFiles(args, contract);
  mkdirSync(dirname(resolve(args.report)), { recursive: true });
  let writtenSchema: unknown;
  if (args.mergeScenarios === null) {
    writeFileSync(args.report, `${stringifyJson(report, true)}\n`, "utf8");
    writtenSchema = report["schemaVersion"];
  } else {
    writtenSchema = writeReportBundle(args.report, report)["schemaVersion"];
  }
  if (args.e4bDecision !== null) {
    if (args.currentReport === null) throw new JourneyError("E4b decision requires CURRENT");
    const decision = buildE4bDecision(loadReportBundle(args.currentReport), report, contract);
    mkdirSync(dirname(resolve(args.e4bDecision)), { recursive: true });
    writeFileSync(args.e4bDecision, `${stringifyJson(decision, true)}\n`, "utf8");
  }
  const latencySamples = Array.isArray(report["latencySamples"])
    ? report["latencySamples"].length
    : 0;
  process.stdout.write(
    `${stringifyJson({ report: args.report, schemaVersion: writtenSchema, variant: report["variant"] ?? null, oracles: statusCounts(report), latencySamples })}\n`,
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error: unknown) => {
    process.stderr.write(
      `E0 journey failed: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}\n`,
    );
    process.exitCode = 1;
  });
}
