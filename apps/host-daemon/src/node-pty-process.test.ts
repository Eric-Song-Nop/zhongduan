import { describe, expect, it } from "vitest";

import { spawnNodePty } from "./node-pty-process";

describe("spawnNodePty", () => {
  it("preserves invalid UTF-8 output as raw bytes", async () => {
    const pty = spawnNodePty({
      args: ["-e", "process.stdout.write(Buffer.from([0xff, 0x41]))"],
      cols: 80,
      command: process.execPath,
      rows: 24,
    });
    const chunks: Uint8Array[] = [];

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        pty.kill();
        reject(new Error("PTY did not exit"));
      }, 5_000);
      pty.onData((data) => chunks.push(data));
      pty.onExit(() => {
        clearTimeout(timeout);
        resolve();
      });
    });

    const output = Buffer.concat(chunks.map((chunk) => Buffer.from(chunk)));
    expect(output).toEqual(Buffer.from([0xff, 0x41]));
  });
});
