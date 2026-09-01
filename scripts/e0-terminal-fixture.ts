#!/usr/bin/env node

/** Deterministic raw PTY child used by the E0 real journey. */

import { appendFileSync, closeSync, openSync, writeSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { once } from "node:events";
import { stringifyJson } from "./e0-terminal-journey.ts";

export const READY_SENTINEL = "ZHONGDUAN_E0_READY";
export const PROBE_PREFIX = Buffer.from("ZHONGDUAN_E0_PROBE:", "ascii");
export const FLOOD_COMMAND = Buffer.from("ZHONGDUAN_E0_FLOOD", "ascii");
export const FLOOD_PREFIX = Buffer.from("ZHONGDUAN_E0_FLOOD:", "ascii");
export const INTERRUPT_ARM_PREFIX = Buffer.from("ZHONGDUAN_E0_INTERRUPT_ARM:", "ascii");
export const RESULT_PREFIX = "ZHONGDUAN_E0_RESULT:";
export const INTERRUPT_PREFIX = "ZHONGDUAN_E0_INTERRUPT:";
export const QUIET_PREFIX = "ZHONGDUAN_E0_QUIET:";
export const SECURE_PREFIX = Buffer.from("ZHONGDUAN_E0_SECURE:", "ascii");
export const INPUT_CAPTURE_ENV = "ZHONGDUAN_E0_INPUT_CAPTURE";
export const EVENT_LOG_ENV = "ZHONGDUAN_E0_EVENT_LOG";
export const FLOOD_CHUNK_BYTES_ENV = "ZHONGDUAN_E0_FLOOD_CHUNK_BYTES";
export const FLOOD_MAX_BYTES_ENV = "ZHONGDUAN_E0_FLOOD_MAX_BYTES";
export const FLOOD_MAX_DURATION_MS_ENV = "ZHONGDUAN_E0_FLOOD_MAX_DURATION_MS";

export class FixtureProtocolError extends Error {
  override name = "FixtureProtocolError";
}

export type FixtureEventKind = "probe" | "flood" | "secure" | "arm-interrupt" | "interrupt";
export type FixtureEvent = readonly [FixtureEventKind, string];

function decodeAscii(value: Buffer): string {
  if (value.some((byte) => byte > 0x7f)) {
    throw new FixtureProtocolError("fixture sample ids must be ASCII");
  }
  return value.toString("ascii");
}

function sampleIdAfter(command: Buffer, prefix: Buffer, label: string): string {
  const sampleId = decodeAscii(command.subarray(prefix.length));
  if (sampleId.length === 0 || sampleId.length > 128) {
    throw new FixtureProtocolError(`invalid ${label} sample id`);
  }
  return sampleId;
}

export class FixtureState {
  pending = Buffer.alloc(0);
  readonly effectCounts = new Map<string, number>();
  interruptCount = 0;
  floodRequested = false;
  nextInterruptSample: string | null = null;

  accept(payload: Buffer): FixtureEvent[] {
    if (!Buffer.isBuffer(payload)) {
      throw new FixtureProtocolError("fixture input must be bytes");
    }
    this.pending = Buffer.concat([this.pending, payload]);
    const events: FixtureEvent[] = [];
    while (this.pending.length > 0) {
      if (this.pending[0] === 0x03) {
        this.pending = this.pending.subarray(1);
        const sampleId =
          this.nextInterruptSample ?? `ctrl-c-${String(this.interruptCount).padStart(3, "0")}`;
        this.interruptCount += 1;
        this.nextInterruptSample = null;
        events.push(["interrupt", sampleId]);
        continue;
      }
      const carriage = this.pending.indexOf(0x0d);
      if (carriage < 0) {
        if (this.pending.length > 65_536) {
          throw new FixtureProtocolError("unterminated fixture command exceeded 64 KiB");
        }
        break;
      }
      const command = this.pending.subarray(0, carriage);
      this.pending = this.pending.subarray(carriage + 1);
      if (command.subarray(0, PROBE_PREFIX.length).equals(PROBE_PREFIX)) {
        const sampleId = sampleIdAfter(command, PROBE_PREFIX, "probe");
        this.incrementEffect(sampleId);
        events.push(["probe", sampleId]);
        continue;
      }
      if (command.equals(FLOOD_COMMAND)) {
        this.floodRequested = true;
        events.push(["flood", "output-flood"]);
        continue;
      }
      if (command.subarray(0, FLOOD_PREFIX.length).equals(FLOOD_PREFIX)) {
        const sampleId = sampleIdAfter(command, FLOOD_PREFIX, "flood");
        this.floodRequested = true;
        events.push(["flood", sampleId]);
        continue;
      }
      if (command.subarray(0, SECURE_PREFIX.length).equals(SECURE_PREFIX)) {
        const sampleId = sampleIdAfter(command, SECURE_PREFIX, "secure-input");
        this.incrementEffect(sampleId);
        events.push(["secure", sampleId]);
        continue;
      }
      if (command.subarray(0, INTERRUPT_ARM_PREFIX.length).equals(INTERRUPT_ARM_PREFIX)) {
        const fields = decodeAscii(command.subarray(INTERRUPT_ARM_PREFIX.length)).split(":");
        if (
          fields.length !== 2 ||
          fields.some((field) => field.length === 0 || field.length > 128)
        ) {
          throw new FixtureProtocolError("invalid interrupt sample id");
        }
        const [interruptSample, commandSample] = fields;
        if (interruptSample === undefined || commandSample === undefined) {
          throw new FixtureProtocolError("invalid interrupt sample id");
        }
        this.nextInterruptSample = interruptSample;
        this.incrementEffect(commandSample);
        events.push(["arm-interrupt", commandSample]);
        continue;
      }
      throw new FixtureProtocolError("fixture received an unexpected command");
    }
    return events;
  }

  private incrementEffect(sampleId: string): void {
    this.effectCounts.set(sampleId, (this.effectCounts.get(sampleId) ?? 0) + 1);
  }
}

const UNIX_EPOCH_OFFSET_NS = BigInt(Date.now()) * 1_000_000n - process.hrtime.bigint();

function unixNanoseconds(): bigint {
  return UNIX_EPOCH_OFFSET_NS + process.hrtime.bigint();
}

class JsonlRecorder {
  private readonly path: string;

  constructor(path: string) {
    this.path = path;
  }

  record(name: string, sampleId: string, fields: Record<string, unknown> = {}): void {
    appendFileSync(
      this.path,
      `${stringifyJson({ name, sampleId, atUnixNs: unixNanoseconds(), ...fields })}\n`,
      "utf8",
    );
  }
}

function positiveInteger(name: string, fallback: number): number {
  const value = Number.parseInt(process.env[name] ?? String(fallback), 10);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new FixtureProtocolError(`${name} must be positive`);
  }
  return value;
}

function immediate(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

async function writeOutput(payload: Buffer): Promise<void> {
  if (!process.stdout.write(payload)) {
    await once(process.stdout, "drain");
  }
}

export async function runFixture(): Promise<void> {
  const capturePath = process.env[INPUT_CAPTURE_ENV];
  const eventPath = process.env[EVENT_LOG_ENV];
  if (
    capturePath === undefined ||
    capturePath.length === 0 ||
    eventPath === undefined ||
    eventPath.length === 0
  ) {
    throw new FixtureProtocolError(`${INPUT_CAPTURE_ENV} and ${EVENT_LOG_ENV} are required`);
  }
  const recorder = new JsonlRecorder(eventPath);
  const state = new FixtureState();
  const chunkBytes = positiveInteger(FLOOD_CHUNK_BYTES_ENV, 4096);
  const maximumFloodBytes = positiveInteger(FLOOD_MAX_BYTES_ENV, 4 * 1024 * 1024);
  const maximumFloodDurationMs = positiveInteger(FLOOD_MAX_DURATION_MS_ENV, 5_000);
  let floodGeneration = 0;
  let floodPromise: Promise<void> | null = null;

  const emit = async (payload: Buffer, sampleId: string): Promise<void> => {
    await writeOutput(payload);
    recorder.record("pty.output", sampleId, { bytes: payload.length });
  };

  const stopFlood = async (): Promise<void> => {
    floodGeneration += 1;
    if (floodPromise !== null) {
      await floodPromise;
    }
    floodPromise = null;
  };

  const startFlood = async (sampleId: string): Promise<void> => {
    await stopFlood();
    const generation = floodGeneration;
    floodPromise = (async () => {
      let emitted = 0;
      let sequence = 0;
      const deadline = performance.now() + maximumFloodDurationMs;
      while (
        generation === floodGeneration &&
        emitted < maximumFloodBytes &&
        performance.now() < deadline
      ) {
        const prefix = Buffer.from(
          `E0-FLOOD-${sampleId}-${String(sequence).padStart(8, "0")} `,
          "ascii",
        );
        const padding = Buffer.alloc(Math.max(1, chunkBytes - prefix.length - 2), "x");
        const payload = Buffer.concat([prefix, padding, Buffer.from("\r\n")]).subarray(
          0,
          chunkBytes,
        );
        await emit(payload, `flood-${sampleId}`);
        emitted += payload.length;
        sequence += 1;
        if (sequence % 16 === 0) await immediate();
      }
      recorder.record("fixture.flood-stopped", sampleId, { bytes: emitted });
    })();
  };

  if (process.stdin.isTTY) process.stdin.setRawMode(true);
  await emit(Buffer.from(`\u001b[2J\u001b[H${READY_SENTINEL}\r\n`, "ascii"), "ready");
  const capture = openSync(capturePath, "a");
  try {
    for await (const value of process.stdin) {
      const payload = Buffer.isBuffer(value) ? value : Buffer.from(value);
      writeSync(capture, payload);
      for (const [kind, sampleId] of state.accept(payload)) {
        recorder.record("host.pty-write", sampleId, { kind });
        if (kind === "probe" || kind === "secure") {
          await emit(Buffer.from(`${RESULT_PREFIX}${sampleId}\r\n`, "ascii"), sampleId);
        } else if (kind === "flood") {
          await startFlood(sampleId);
        } else if (kind === "interrupt") {
          await stopFlood();
          await emit(Buffer.from(`${INTERRUPT_PREFIX}${sampleId}\r\n`, "ascii"), sampleId);
          await emit(Buffer.from(`${QUIET_PREFIX}${sampleId}\r\n`, "ascii"), sampleId);
        }
      }
    }
  } finally {
    await stopFlood();
    closeSync(capture);
  }
}

const invokedPath = process.argv[1];
if (invokedPath !== undefined && import.meta.url === pathToFileURL(invokedPath).href) {
  runFixture().catch((error: unknown) => {
    console.error(error instanceof Error ? error.stack : error);
    process.exitCode = 1;
  });
}
