import {
  DataFrameKind,
  decodeDataFrame,
  KeyModifier,
  type ResizePayload,
} from "@zhongduan/protocol";
import { GHOSTTY_ENGINE_ID } from "@wterm/ghostty";
import { describe, expect, it } from "vitest";

import { EventJournal } from "./journal";
import type { PtyProcess } from "./pty-process";
import { TerminalSession } from "./session";
import type { SemanticKey } from "./terminal-authority";
import { WtermGhosttyAuthority, loadCommittedGhosttyRuntime } from "./wterm-ghostty-authority";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const dimensions = { cols: 20, rows: 4, widthPx: 0, heightPx: 0 } as const;

class RecordingPty implements PtyProcess {
  readonly pid = 42;
  readonly events: string[] = [];
  readonly writes: Uint8Array[] = [];
  killed = false;
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
    const copy = data.slice();
    this.writes.push(copy);
    this.events.push(`write:${decoder.decode(copy)}`);
  }

  resize(next: ResizePayload): void {
    this.events.push(`resize:${next.cols}x${next.rows}:${next.widthPx}x${next.heightPx}`);
  }

  kill(): void {
    this.killed = true;
  }

  emit(data: Uint8Array): void {
    this.#dataListener?.(data);
  }
}

describe("WtermGhosttyAuthority", () => {
  it("passes the explicit scrollback byte budget to Ghostty", async () => {
    await expect(
      WtermGhosttyAuthority.create({ ...dimensions, scrollbackLimit: -1 }),
    ).rejects.toThrow(/scrollbackLimit must be an unsigned 32-bit integer/);
  });

  it("loads the committed artifact and preserves invalid UTF-8 on the ordered data path", async () => {
    const runtime = await loadCommittedGhosttyRuntime();
    const authority = await WtermGhosttyAuthority.create(dimensions);
    const pty = new RecordingPty();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    const payloads: Uint8Array[] = [];
    session.subscribe((encoded) => {
      const frame = decodeDataFrame(encoded);
      if (frame.kind === DataFrameKind.PtyOutput) payloads.push(frame.payload.slice());
    });

    try {
      pty.emit(Uint8Array.of(0xff, 0x41));
      const snapshot = await session.captureSnapshot();

      expect(runtime.artifactVerified).toBe(true);
      expect(runtime.engineId).toBe(GHOSTTY_ENGINE_ID);
      expect(session.engineId).toBe(GHOSTTY_ENGINE_ID);
      expect(snapshot.engineId).toBe(GHOSTTY_ENGINE_ID);
      expect(decoder.decode(snapshot.bytes.subarray(0, 8))).toBe("GHOSTSNP");
      expect(payloads).toEqual([Uint8Array.of(0xff, 0x41)]);
    } finally {
      session.dispose();
    }
  });

  it.each([
    {
      name: "UTF-8",
      prefix: Uint8Array.of(0xe2, 0x82),
      tail: Uint8Array.of(0xac),
    },
    {
      name: "CSI",
      prefix: encoder.encode("\u001b[31"),
      tail: encoder.encode("mC"),
    },
  ])("restores a half $name continuation before applying its tail", async ({ prefix, tail }) => {
    const runtime = await loadCommittedGhosttyRuntime();
    const authority = await WtermGhosttyAuthority.create(dimensions);

    try {
      expect(authority.applyOutput(prefix)).toEqual([]);
      const restore = runtime.beginPassiveRestore(authority.encodeSnapshot(), {
        effects: "discard",
        maxContinuationBytes: 64 * 1024,
      });
      try {
        await restore.advanceToFinish({ yieldBetweenPages: false });
        expect(authority.applyOutput(tail)).toEqual([]);
        restore.writeRaw(tail);
        const restored = restore.takeCore();
        try {
          expect(restored.encodeSnapshot()).toEqual(authority.encodeSnapshot());
          expect(restored.drainEffects()).toEqual([]);
        } finally {
          restored.dispose();
        }
      } finally {
        restore.dispose();
      }
    } finally {
      authority.dispose();
    }
  });

  it("returns ordered DA and DSR binary effects exactly once", async () => {
    const authority = await WtermGhosttyAuthority.create(dimensions);
    try {
      expect(authority.applyOutput(encoder.encode("\u001b[c\u001b[6n"))).toEqual([
        encoder.encode("\u001b[?62;22c"),
        encoder.encode("\u001b[1;1R"),
      ]);
      expect(authority.applyOutput(encoder.encode("plain"))).toEqual([]);
    } finally {
      authority.dispose();
    }
  });

  it("encodes paste from the authoritative bracketed-paste mode", async () => {
    const authority = await WtermGhosttyAuthority.create(dimensions);
    try {
      expect(decoder.decode(authority.encodePaste("a\nb"))).toBe("a\rb");
      authority.applyOutput(encoder.encode("\u001b[?2004h"));
      expect(decoder.decode(authority.encodePaste("a\nb"))).toBe("\u001b[200~a\nb\u001b[201~");
    } finally {
      authority.dispose();
    }
  });

  it("encodes key release only after the authoritative Kitty event mode is enabled", async () => {
    const authority = await WtermGhosttyAuthority.create(dimensions);
    const release = semanticKey({
      action: "release",
      code: "KeyA",
      key: "a",
      text: "a",
    });
    try {
      expect(authority.encodeKey(release)).toEqual(new Uint8Array());
      authority.applyOutput(encoder.encode("\u001b[>3u"));
      expect(authority.encodeKey(release)).toEqual(encoder.encode("\u001b[97;1:3u"));
    } finally {
      authority.dispose();
    }
  });

  it("passes Ghostty-layout modifiers and AltGraph semantics without remapping", async () => {
    const authority = await WtermGhosttyAuthority.create(dimensions);
    try {
      expect(
        decoder.decode(
          authority.encodeKey(
            semanticKey({
              code: "KeyC",
              consumedModifiers: KeyModifier.Shift,
              key: "C",
              modifiers: KeyModifier.Shift | KeyModifier.Control,
              text: "C",
            }),
          ),
        ),
      ).toBe("\u001b[99;6u");
      expect(
        decoder.decode(
          authority.encodeKey(
            semanticKey({
              altGraph: true,
              code: "KeyQ",
              key: "@",
              modifiers: KeyModifier.Control | KeyModifier.Alt,
              text: "@",
            }),
          ),
        ),
      ).toBe("@");
    } finally {
      authority.dispose();
    }
  });

  it("writes a Kitty key release once and skips the empty legacy encoding", async () => {
    const authority = await WtermGhosttyAuthority.create(dimensions);
    const pty = new RecordingPty();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    const release = semanticKey({
      action: "release",
      code: "KeyA",
      key: "a",
      text: "a",
    });

    try {
      session.writeKey(release);
      expect(pty.writes).toEqual([]);

      pty.emit(encoder.encode("\u001b[>3u"));
      session.writeKey(release);
      expect(pty.writes).toEqual([encoder.encode("\u001b[97;1:3u")]);
    } finally {
      session.dispose();
    }
  });

  it("orders output, resize, and their write-PTY effects in one actor", async () => {
    const authority = await WtermGhosttyAuthority.create(dimensions);
    const pty = new RecordingPty();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    session.subscribe((encoded) => {
      const frame = decodeDataFrame(encoded);
      pty.events.push(frame.kind === DataFrameKind.PtyOutput ? "frame:output" : "frame:resize");
    });

    try {
      pty.emit(encoder.encode("\u001b[?2048h"));
      session.resize({ cols: 30, rows: 5, widthPx: 300, heightPx: 100 });

      expect(pty.events).toEqual([
        "frame:output",
        "write:\u001b[48;4;20;0;0t",
        "resize:30x5:300x100",
        "frame:resize",
        "write:\u001b[48;5;30;100;300t",
      ]);
    } finally {
      session.dispose();
    }
  });

  it("fails the session closed instead of returning partial effects on overflow", async () => {
    const authority = await WtermGhosttyAuthority.create(dimensions);
    const pty = new RecordingPty();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });

    pty.emit(encoder.encode("\u001b[6n".repeat(257)));
    const exit = await session.waitForExit();

    expect(exit).toMatchObject({ status: "failed" });
    expect(session.eventSeq).toBe(0n);
    expect(pty.writes).toEqual([]);
    expect(pty.killed).toBe(true);
    session.dispose();
  });

  it("releases every WASM owner across repeated idempotent disposal", async () => {
    const runtime = await loadCommittedGhosttyRuntime();
    const baseline = liveOwners(runtime);

    for (let iteration = 0; iteration < 100; iteration += 1) {
      const authority = await WtermGhosttyAuthority.create(dimensions);
      authority.dispose();
      authority.dispose();
      expect(liveOwners(runtime)).toEqual(baseline);
    }
  });
});

function semanticKey(overrides: Partial<SemanticKey> = {}): SemanticKey {
  return {
    action: "press",
    altGraph: false,
    code: "Enter",
    composing: false,
    consumedModifiers: 0,
    key: "Enter",
    modifiers: 0,
    ...overrides,
  };
}

function liveOwners(runtime: Awaited<ReturnType<typeof loadCommittedGhosttyRuntime>>) {
  return {
    buffers: runtime.wasm.exports.live_bridge_buffers(),
    handles: runtime.wasm.exports.live_restore_handles(),
    states: runtime.wasm.exports.live_terminal_states(),
  };
}
