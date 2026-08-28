import { DataFrameKind, decodeDataFrame } from "@zhongduan/protocol";
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
    const session = startLocalSession({
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

    session.writePaste("printf '__ZHONGDUAN_HOST_OK__\\n'; exit");
    session.writeKey({ code: "Enter", key: "Enter", modifiers: 0, repeat: false });
    const exit = await Promise.race([
      session.waitForExit(),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("shell did not exit")), 5_000),
      ),
    ]);
    session.dispose();

    expect(exit).toMatchObject({ status: "exited", exitCode: 0 });
    expect(Buffer.concat(output.map((chunk) => Buffer.from(chunk))).toString()).toContain(
      "__ZHONGDUAN_HOST_OK__",
    );
  });
});
