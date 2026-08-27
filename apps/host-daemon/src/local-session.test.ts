import { DataFrameKind, decodeDataFrame } from "@zhongduan/protocol";
import { GHOSTTY_ENGINE_ID, GHOSTTY_TERMINAL_PROFILE } from "@wterm/ghostty";
import { describe, expect, it } from "vitest";

import { createSessionEpoch, startLocalSession } from "./local-session";

describe("local terminal session", () => {
  it("creates a fresh non-zero epoch for each PTY lifecycle", () => {
    const first = createSessionEpoch();
    const second = createSessionEpoch();

    expect(first).toBeGreaterThan(0n);
    expect(second).toBeGreaterThan(0n);
    expect(second).not.toBe(first);
  });

  it("drives a real shell through semantic input", async () => {
    const session = await startLocalSession({
      args: [],
      cols: 80,
      command: "/bin/sh",
      rows: 24,
    });
    const output: Uint8Array[] = [];
    session.subscribe((encoded) => {
      const frame = decodeDataFrame(encoded);
      if (frame.kind === DataFrameKind.PtyOutput) output.push(frame.payload.slice());
    });

    try {
      expect(session.engineId).toBe(GHOSTTY_ENGINE_ID);
      await expect(session.captureSnapshot()).resolves.toMatchObject({
        engineId: GHOSTTY_ENGINE_ID,
      });

      session.writePaste("printf '__ZHONGDUAN_HOST_OK__:%s__\\n' \"$TERM\"; exit");
      session.writeKey({
        action: "press",
        altGraph: false,
        code: "Enter",
        composing: false,
        consumedModifiers: 0,
        key: "Enter",
        modifiers: 0,
      });
      const exit = await withTimeout(session.waitForExit(), 5_000);

      expect(exit).toMatchObject({ status: "exited", exitCode: 0 });
      expect(Buffer.concat(output.map((chunk) => Buffer.from(chunk))).toString()).toContain(
        `__ZHONGDUAN_HOST_OK__:${GHOSTTY_TERMINAL_PROFILE.term}__`,
      );
    } finally {
      session.dispose();
    }
  });

  it("keeps a real Ghostty authority interruptible during sustained PTY output", async () => {
    const session = await startLocalSession({
      args: [],
      cols: 80,
      command: "/usr/bin/yes",
      journal: { maxBytes: 1024 * 1024 },
      rows: 24,
      scrollbackLimit: 1024 * 1024,
    });
    const epoch = session.sessionEpoch;
    let outputBytes = 0;
    let resolveOutputStarted!: () => void;
    const outputStarted = new Promise<void>((resolve) => {
      resolveOutputStarted = resolve;
    });
    session.subscribe((encoded) => {
      const frame = decodeDataFrame(encoded);
      if (frame.kind !== DataFrameKind.PtyOutput) return;
      outputBytes += frame.payload.byteLength;
      if (outputBytes >= 64 * 1024) resolveOutputStarted();
    });

    try {
      await withTimeout(outputStarted, 5_000);
      const snapshot = await session.captureSnapshot();
      await expect(
        session.submitText(
          {
            clientId: "client-local-test",
            clientInputSeq: 1n,
            inputEpoch: "writer-local-test",
            writerFence: 1n,
          },
          "\u0003",
        ),
      ).resolves.toMatchObject({ status: "written" });
      const exit = await withTimeout(session.waitForExit(), 5_000);

      expect(exit.status).toBe("exited");
      expect(snapshot.cutEventSeq).toBeGreaterThan(0n);
      expect(session.sessionEpoch).toBe(epoch);
    } finally {
      session.dispose();
    }
  });
});

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new Error("shell did not exit")), timeoutMs);
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}
