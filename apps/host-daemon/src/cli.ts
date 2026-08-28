#!/usr/bin/env node

import { randomBytes } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import { DataFrameKind, decodeDataFrame } from "@zhongduan/protocol";

import { CloudApiClient } from "./cloud/cloud-api";
import { HostCapabilityManager, type BootstrapTokenProvider } from "./cloud/capability-manager";
import { HostCloudRelay } from "./cloud/host-cloud-relay";
import { startLocalSession } from "./local-session";
import type { PtyExit } from "./session";
import { telemetrySinkForTarget } from "./telemetry";

export const CLOUD_CREATE_TIMEOUT_MS = 10_000;
export const CLOUD_CREATE_RETRY_BASE_MS = 1_000;
export const CLOUD_CREATE_RETRY_MAX_MS = 30_000;

export async function main(argv: string[] = process.argv.slice(2)): Promise<number> {
  if (argv[0] === "local") return runLocal(argv.slice(1));
  if (argv[0] === "cloud") return runCloud(argv.slice(1));
  process.stderr.write(
    "usage: zhongduan-host local [--] [command [args...]]\n" +
      "       zhongduan-host cloud --url URL [--bootstrap-token-file PATH] --session-info-file PATH [--] [command [args...]]\n",
  );
  return 2;
}

async function runLocal(requested: string[]): Promise<number> {
  requested = requested.slice();
  if (requested[0] === "--") requested.shift();
  const { command, args } = shellCommand(requested);
  const dimensions = terminalDimensions();
  const session = await startLocalSession({ command, args, ...dimensions });
  let forcedExitCode: number | null = null;
  let cleaned = false;
  const wasRaw = process.stdin.isTTY ? process.stdin.isRaw : false;

  function cleanup(): void {
    if (cleaned) return;
    cleaned = true;
    process.stdin.off("data", onInput);
    process.stdout.off("resize", onResize);
    process.stdout.off("error", onOutputError);
    process.off("SIGTERM", onSigterm);
    process.off("SIGHUP", onSighup);
    try {
      if (process.stdin.isTTY) process.stdin.setRawMode(wasRaw);
    } finally {
      try {
        process.stdin.pause();
      } finally {
        session.dispose();
      }
    }
  }

  function stop(exitCode: number): void {
    forcedExitCode ??= exitCode;
    cleanup();
  }

  function onSigterm(): void {
    stop(128 + 15);
  }

  function onSighup(): void {
    stop(128 + 1);
  }

  function onOutputError(): void {
    stop(1);
  }

  function onInput(data: Buffer): void {
    try {
      session.writeRawInput(data);
    } catch {
      stop(1);
    }
  }

  function onResize(): void {
    try {
      session.resize(terminalDimensions());
    } catch {
      stop(1);
    }
  }

  try {
    session.subscribe((encoded) => {
      const frame = decodeDataFrame(encoded);
      if (frame.kind === DataFrameKind.PtyOutput) {
        try {
          process.stdout.write(Buffer.from(frame.payload));
        } catch {
          stop(1);
        }
      }
    });
    process.once("SIGTERM", onSigterm);
    process.once("SIGHUP", onSighup);
    process.stdout.once("error", onOutputError);
    if (process.stdin.isTTY) process.stdin.setRawMode(true);
    process.stdin.resume();
    process.stdin.on("data", onInput);
    process.stdout.on("resize", onResize);

    const exit = await session.waitForExit();
    if (forcedExitCode !== null) return forcedExitCode;
    return exit.status === "exited" ? exit.exitCode : 1;
  } finally {
    cleanup();
  }
}

interface CloudArguments {
  bootstrapToken: BootstrapTokenProvider;
  command: string;
  commandArguments: string[];
  sessionInfoFile: string;
  url: string;
}

export async function runCloud(argv: string[]): Promise<number> {
  let parsed: CloudArguments;
  let telemetry: ReturnType<typeof telemetrySinkForTarget>;
  try {
    parsed = parseCloudArguments(argv);
    telemetry = telemetrySinkForTarget(process.env.ZHONGDUAN_TELEMETRY, process.stderr);
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    return 2;
  }

  const api = new CloudApiClient(parsed.url);
  const dimensions = { cols: 80, rows: 24, widthPx: 0, heightPx: 0 };
  const session = await startLocalSession({
    command: parsed.command,
    args: parsed.commandArguments,
    ...dimensions,
  });
  const cloudSessionId = `session_${randomBytes(18).toString("base64url")}`;
  let capabilities: HostCapabilityManager | undefined;
  let relay: HostCloudRelay | undefined;
  let forcedExitCode: number | null = null;
  const ptyExit = session.waitForExit();
  let startupExit: Awaited<typeof ptyExit> | undefined;
  const startupAbort = new AbortController();
  void ptyExit.then((exit) => {
    startupExit = exit;
    startupAbort.abort(new DOMException("PTY exited during cloud startup", "AbortError"));
  });
  const onSigterm = () => {
    forcedExitCode ??= 128 + 15;
    session.dispose();
  };
  const onSighup = () => {
    forcedExitCode ??= 128 + 1;
    session.dispose();
  };
  process.once("SIGTERM", onSigterm);
  process.once("SIGHUP", onSighup);

  try {
    let created;
    let createAttempts = 0;
    while (created === undefined) {
      const requestDeadline = new AbortController();
      const requestTimeout = setTimeout(
        () =>
          requestDeadline.abort(
            new DOMException("cloud session creation timed out", "TimeoutError"),
          ),
        CLOUD_CREATE_TIMEOUT_MS,
      );
      const requestSignal = AbortSignal.any([startupAbort.signal, requestDeadline.signal]);
      try {
        const bootstrapToken = await raceAbort(
          Promise.resolve().then(() => parsed.bootstrapToken(requestSignal)),
          requestSignal,
        );
        requestSignal.throwIfAborted();
        created = await raceAbort(
          api.createSession(
            bootstrapToken,
            cloudSessionId,
            session.engineId,
            session.sessionEpoch,
            requestSignal,
          ),
          requestSignal,
        );
      } catch {
        if (startupExit !== undefined) return exitCode(startupExit, forcedExitCode);
        createAttempts = Math.min(createAttempts + 1, 30);
        const retryDelay = Math.min(
          CLOUD_CREATE_RETRY_MAX_MS,
          CLOUD_CREATE_RETRY_BASE_MS * 2 ** Math.min(20, createAttempts - 1),
        );
        try {
          await delay(retryDelay, startupAbort.signal);
        } catch {
          if (startupExit !== undefined) return exitCode(startupExit, forcedExitCode);
          throw startupAbort.signal.reason;
        }
      } finally {
        clearTimeout(requestTimeout);
      }
    }
    if (startupExit !== undefined) return exitCode(startupExit, forcedExitCode);
    await writeFile(
      parsed.sessionInfoFile,
      `${JSON.stringify(
        {
          cloudUrl: parsed.url,
          sessionId: created.sessionId,
          engineId: created.engineId,
          sessionEpoch: created.sessionEpoch,
          writerCapability: created.writerCapability,
          writerCapabilityExpiresAt: created.writerCapabilityExpiresAt,
          observerCapability: created.observerCapability,
          observerCapabilityExpiresAt: created.observerCapabilityExpiresAt,
        },
        null,
        2,
      )}\n`,
      { encoding: "utf8", flag: "wx", mode: 0o600 },
    );
    capabilities = new HostCapabilityManager({
      api,
      bootstrapToken: parsed.bootstrapToken,
      engineId: session.engineId,
      initialCapability: created.hostCapability,
      initialExpiresAt: created.hostCapabilityExpiresAt,
      sessionEpoch: session.sessionEpoch,
      sessionId: created.sessionId,
    });
    relay = new HostCloudRelay({
      api,
      capabilities,
      session,
      sessionId: created.sessionId,
      ...(telemetry === undefined ? {} : { telemetry }),
    });
    const firstReady = await Promise.race([
      relay.start().then(
        () => ({ type: "ready" as const }),
        (error: unknown) => ({ type: "relay-error" as const, error }),
      ),
      ptyExit.then((exit) => ({ type: "pty" as const, exit })),
    ]);
    if (firstReady.type === "pty") {
      await relay.stop();
      return exitCode(firstReady.exit, forcedExitCode);
    }
    if (firstReady.type === "relay-error") throw firstReady.error;
    const outcome = await Promise.race([
      ptyExit.then((exit) => ({ type: "pty" as const, exit })),
      relay.waitUntilStopped().then(() => ({ type: "relay" as const })),
    ]);
    if (forcedExitCode !== null) return forcedExitCode;
    if (outcome.type === "relay") throw new Error("Host relay stopped unexpectedly");
    return exitCode(outcome.exit, forcedExitCode);
  } finally {
    process.off("SIGTERM", onSigterm);
    process.off("SIGHUP", onSighup);
    if (relay !== undefined) {
      try {
        await relay.stop();
      } catch {
        // Preserve the primary CLI outcome while closing the network component.
      }
    }
    capabilities?.dispose();
    session.dispose();
  }
}

function exitCode(exit: PtyExit, forcedExitCode: number | null): number {
  if (forcedExitCode !== null) return forcedExitCode;
  return exit.status === "exited" ? exit.exitCode : 1;
}

function parseCloudArguments(argv: string[]): CloudArguments {
  const remaining = argv.slice();
  let url = process.env.ZHONGDUAN_CLOUD_URL;
  let bootstrapTokenFile = process.env.ZHONGDUAN_BOOTSTRAP_TOKEN_FILE;
  let sessionInfoFile = process.env.ZHONGDUAN_SESSION_INFO_FILE;
  while (remaining.length > 0 && remaining[0] !== "--") {
    const option = remaining.shift()!;
    const value = remaining.shift();
    if (value === undefined) throw new Error(`missing value for ${option}`);
    if (option === "--url") url = value;
    else if (option === "--bootstrap-token-file") bootstrapTokenFile = value;
    else if (option === "--session-info-file") sessionInfoFile = value;
    else throw new Error(`unknown cloud option: ${option}`);
  }
  if (remaining[0] === "--") remaining.shift();
  if (
    url === undefined ||
    sessionInfoFile === undefined ||
    (bootstrapTokenFile === undefined && process.env.ZHONGDUAN_BOOTSTRAP_TOKEN === undefined)
  ) {
    throw new Error(
      "cloud mode requires --url, --session-info-file, and a bootstrap token file or environment variable",
    );
  }
  const bootstrapToken: BootstrapTokenProvider =
    bootstrapTokenFile === undefined
      ? () => requiredBootstrapToken(process.env.ZHONGDUAN_BOOTSTRAP_TOKEN)
      : async (signal) =>
          requiredBootstrapToken(
            await readFile(bootstrapTokenFile, {
              encoding: "utf8",
              ...(signal === undefined ? {} : { signal }),
            }),
          );
  const { command, args } = shellCommand(remaining);
  return {
    url,
    bootstrapToken,
    sessionInfoFile,
    command,
    commandArguments: args,
  };
}

function requiredBootstrapToken(value: string | undefined): string {
  const token = value?.trim();
  if (token === undefined || token.length === 0) throw new Error("bootstrap token is empty");
  return token;
}

function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
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

function raceAbort<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
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

function shellCommand(requested: string[]): { command: string; args: string[] } {
  const command = requested.shift() ?? process.env.SHELL ?? "/bin/sh";
  const args = requested.length > 0 ? requested : command === process.env.SHELL ? ["-i"] : [];
  return { command, args };
}

function terminalDimensions() {
  return {
    cols: process.stdout.columns ?? 80,
    rows: process.stdout.rows ?? 24,
    widthPx: 0,
    heightPx: 0,
  };
}

const invokedPath = process.argv[1];
if (invokedPath !== undefined && import.meta.url === pathToFileURL(invokedPath).href) {
  main().then(
    (exitCode) => {
      process.exitCode = exitCode;
    },
    (error: unknown) => {
      process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
      process.exitCode = 1;
    },
  );
}
