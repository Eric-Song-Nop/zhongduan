#!/usr/bin/env node

import { pathToFileURL } from "node:url";

import { DataFrameKind, decodeDataFrame } from "@zhongduan/protocol";

import { startLocalSession } from "./local-session";

export async function main(argv: string[] = process.argv.slice(2)): Promise<number> {
  if (argv[0] !== "local") {
    process.stderr.write("usage: zhongduan-host local [--] [command [args...]]\n");
    return 2;
  }

  const requested = argv.slice(1);
  if (requested[0] === "--") requested.shift();
  const command = requested.shift() ?? process.env.SHELL ?? "/bin/sh";
  const args = requested.length > 0 ? requested : command === process.env.SHELL ? ["-i"] : [];
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
