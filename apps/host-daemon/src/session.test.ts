import {
  DataFrameKind,
  decodeDataFrame,
  decodeResizePayload,
  type ResizePayload,
} from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";

import { FakeTerminalAuthority } from "./fake-terminal-authority";
import { EventJournal } from "./journal";
import type { PtyProcess } from "./pty-process";
import { TerminalSession } from "./session";

class ManualPty implements PtyProcess {
  readonly pid = 42;
  readonly writes: Uint8Array[] = [];
  readonly resizes: ResizePayload[] = [];
  resizeError: Error | undefined;

  #dataListener: ((data: Uint8Array) => void) | undefined;
  #exitListener: ((exitCode: number, signal: number) => void) | undefined;

  onData(listener: (data: Uint8Array) => void): () => void {
    this.#dataListener = listener;
    return () => {
      this.#dataListener = undefined;
    };
  }

  onExit(listener: (exitCode: number, signal: number) => void): () => void {
    this.#exitListener = listener;
    return () => {
      this.#exitListener = undefined;
    };
  }

  write(data: Uint8Array): void {
    this.writes.push(data.slice());
  }

  resize(dimensions: ResizePayload): void {
    if (this.resizeError !== undefined) throw this.resizeError;
    this.resizes.push(dimensions);
  }

  kill(): void {
    this.#exitListener?.(0, 0);
  }

  emit(data: Uint8Array): void {
    this.#dataListener?.(data);
  }

  emitExit(exitCode = 0, signal = 0): void {
    this.#exitListener?.(exitCode, signal);
  }
}

describe("TerminalSession", () => {
  it("bounds the fake authority history used by injected tests", () => {
    const authority = new FakeTerminalAuthority({ maxOperationBytes: 2 });

    authority.applyOutput(Uint8Array.of(0x41));
    authority.applyOutput(Uint8Array.of(0x42));
    authority.applyOutput(Uint8Array.of(0x43));

    expect(authority.operations).toEqual([
      { type: "output", data: Uint8Array.of(0x42) },
      { type: "output", data: Uint8Array.of(0x43) },
    ]);
  });

  it("orders PTY output and resize through authority, journal, and subscribers", () => {
    const pty = new ManualPty();
    const authority = new FakeTerminalAuthority();
    const journal = new EventJournal({ now: () => 0 });
    const session = new TerminalSession({
      authority,
      journal,
      pty,
      sessionEpoch: 7n,
    });
    const delivered: Uint8Array[] = [];
    session.subscribe((frame) => delivered.push(frame));

    pty.emit(Uint8Array.of(0x41));
    session.resize({ cols: 100, rows: 30, widthPx: 800, heightPx: 600 });
    pty.emit(Uint8Array.of(0x42));

    expect(authority.operations).toEqual([
      { type: "output", data: Uint8Array.of(0x41) },
      {
        type: "resize",
        dimensions: { cols: 100, rows: 30, widthPx: 800, heightPx: 600 },
      },
      { type: "output", data: Uint8Array.of(0x42) },
    ]);
    expect(pty.resizes).toEqual([{ cols: 100, rows: 30, widthPx: 800, heightPx: 600 }]);

    const frames = delivered.map((frame) => decodeDataFrame(frame));
    expect(frames.map(({ kind, eventSeq, ptyOffset }) => ({ kind, eventSeq, ptyOffset }))).toEqual([
      { kind: DataFrameKind.PtyOutput, eventSeq: 1n, ptyOffset: 0n },
      { kind: DataFrameKind.ResizeApplied, eventSeq: 2n, ptyOffset: 1n },
      { kind: DataFrameKind.PtyOutput, eventSeq: 3n, ptyOffset: 1n },
    ]);
    expect(frames[0]?.payload).toEqual(Uint8Array.of(0x41));
    expect(decodeResizePayload(frames[1]!.payload)).toEqual({
      cols: 100,
      rows: 30,
      widthPx: 800,
      heightPx: 600,
    });
    expect(frames[2]?.payload).toEqual(Uint8Array.of(0x42));
    expect(journal.entries()).toEqual(delivered);
  });

  it("takes a snapshot at an actor barrier while ingress continues queuing", async () => {
    const pty = new ManualPty();
    const authority = new (class extends FakeTerminalAuthority {
      override encodeSnapshot(): Uint8Array {
        const snapshot = super.encodeSnapshot();
        pty.emit(Uint8Array.of(0x42));
        return snapshot;
      }
    })();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      monotonicNow: (() => {
        let now = 10;
        return () => now++;
      })(),
      pty,
      sessionEpoch: 3n,
    });

    pty.emit(Uint8Array.of(0x41));
    const snapshot = await session.captureSnapshot();

    expect(snapshot).toMatchObject({
      cutEventSeq: 1n,
      encodeMs: 1,
      engineId: authority.engineId,
      nextPtyOffset: 1n,
      sessionEpoch: 3n,
    });
    expect(JSON.parse(new TextDecoder().decode(snapshot.bytes))).toMatchObject({
      operations: [{ type: "output", data: [0x41] }],
    });
    expect(session.eventSeq).toBe(2n);
    expect(authority.operations).toEqual([
      { type: "output", data: Uint8Array.of(0x41) },
      { type: "output", data: Uint8Array.of(0x42) },
    ]);
  });

  it("encodes semantic input against current authority state and keeps replies out of journal", () => {
    const pty = new ManualPty();
    const journal = new EventJournal();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal,
      pty,
      sessionEpoch: 1n,
    });

    pty.emit(new TextEncoder().encode("\u001b[?1h"));
    session.writeKey({
      action: "press",
      altGraph: false,
      code: "ArrowUp",
      composing: false,
      consumedModifiers: 0,
      key: "ArrowUp",
      modifiers: 0,
    });
    session.writePaste("hello");
    pty.emit(new TextEncoder().encode("\u001b[6n"));

    expect(pty.writes).toEqual([
      new TextEncoder().encode("\u001bOA"),
      new TextEncoder().encode("hello"),
      new TextEncoder().encode("\u001b[1;1R"),
    ]);
    expect(journal.entries()).toHaveLength(2);
  });

  it("bridges journal replay to live delivery without a reentrant gap", () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    pty.emit(Uint8Array.of(0x41));
    const eventSeqs: bigint[] = [];

    const subscription = session.subscribe(
      (encoded) => {
        const frame = decodeDataFrame(encoded);
        eventSeqs.push(frame.eventSeq);
        if (frame.eventSeq === 1n) {
          pty.emit(Uint8Array.of(0x42));
          pty.emit(Uint8Array.of(0x43));
        }
        if (frame.eventSeq === 2n) pty.emit(Uint8Array.of(0x44));
      },
      { sessionEpoch: 1n, lastEventSeq: 0n, nextPtyOffset: 0n },
    );

    expect(subscription.status).toBe("attached");
    expect(eventSeqs).toEqual([1n, 2n, 3n, 4n]);
  });

  it("does not deliver the current frame to a subscriber attached during publish", () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    const lateEvents: bigint[] = [];
    session.subscribe((encoded) => {
      const current = decodeDataFrame(encoded);
      if (current.eventSeq === 1n) {
        session.subscribe((late) => lateEvents.push(decodeDataFrame(late).eventSeq), {
          sessionEpoch: 1n,
          lastEventSeq: 1n,
          nextPtyOffset: 1n,
        });
      }
    });

    pty.emit(Uint8Array.of(0x41));
    pty.emit(Uint8Array.of(0x42));

    expect(lateEvents).toEqual([2n]);
  });

  it("rejects replay from a different session epoch", () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 2n,
    });

    expect(
      session.subscribe(() => undefined, {
        sessionEpoch: 1n,
        lastEventSeq: 0n,
        nextPtyOffset: 0n,
      }),
    ).toEqual({ status: "gap" });
  });

  it("preserves raw input bytes without a UTF-8 round trip", () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });

    session.writeRawInput(Uint8Array.of(0xff, 0x00, 0x41));

    expect(pty.writes).toEqual([Uint8Array.of(0xff, 0x00, 0x41)]);
  });

  it("deduplicates semantic input and refuses an evicted old sequence", async () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      inputDedupEntries: 1,
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    const enter = {
      action: "press" as const,
      altGraph: false,
      code: "Enter",
      composing: false,
      consumedModifiers: 0,
      key: "Enter",
      modifiers: 0,
    };

    await expect(
      session.submitKey(
        {
          clientId: "client-1",
          clientInputSeq: 1n,
          inputEpoch: "writer-1",
          observedEventSeq: 0n,
        },
        enter,
      ),
    ).resolves.toMatchObject({ status: "written", authorityEventSeq: 0n });
    await expect(
      session.submitKey(
        {
          clientId: "client-1",
          clientInputSeq: 1n,
          inputEpoch: "writer-1",
          observedEventSeq: 0n,
        },
        enter,
      ),
    ).resolves.toMatchObject({ status: "duplicate" });
    await session.submitPaste(
      { clientId: "client-1", clientInputSeq: 2n, inputEpoch: "writer-1" },
      "next",
    );
    await expect(
      session.submitKey(
        {
          clientId: "client-1",
          clientInputSeq: 1n,
          inputEpoch: "writer-1",
          observedEventSeq: 0n,
        },
        enter,
      ),
    ).resolves.toMatchObject({ status: "uncertain" });
    await expect(
      session.submitKey(
        {
          clientId: "client-1",
          clientInputSeq: 3n,
          inputEpoch: "writer-1",
          observedEventSeq: 99n,
        },
        enter,
      ),
    ).resolves.toMatchObject({ status: "rejected" });

    expect(pty.writes).toEqual([new TextEncoder().encode("\r"), new TextEncoder().encode("next")]);
  });

  it("bounds semantic input epoch tracking and fails closed at capacity", async () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      inputDedupEpochs: 1,
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });

    await expect(
      session.submitPaste(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1" },
        "accepted",
      ),
    ).resolves.toMatchObject({ status: "written" });
    await expect(
      session.submitPaste(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-2" },
        "rejected",
      ),
    ).resolves.toMatchObject({ status: "rejected" });

    expect(pty.writes).toEqual([new TextEncoder().encode("accepted")]);
  });

  it("deduplicates resize through the shared semantic input sequence", async () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    const identity = {
      clientId: "client-1",
      clientInputSeq: 1n,
      inputEpoch: "writer-1",
      observedEventSeq: 0n,
    };
    const dimensions = { cols: 100, rows: 30, widthPx: 800, heightPx: 600 };

    await expect(session.submitResize(identity, dimensions)).resolves.toMatchObject({
      status: "written",
      authorityEventSeq: 1n,
    });
    await expect(session.submitResize(identity, dimensions)).resolves.toMatchObject({
      status: "duplicate",
      authorityEventSeq: 1n,
    });

    expect(pty.resizes).toEqual([dimensions]);
    expect(session.eventSeq).toBe(1n);
  });

  it("rejects submitted input after the PTY exits", async () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    pty.emitExit();

    await expect(
      session.submitPaste(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1" },
        "ignored",
      ),
    ).resolves.toMatchObject({ status: "rejected" });
    expect(pty.writes).toHaveLength(0);
  });

  it("settles waitForExit when explicitly disposed", async () => {
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty: new ManualPty(),
      sessionEpoch: 1n,
    });
    const exited = session.waitForExit();

    session.dispose();

    await expect(exited).resolves.toEqual({ status: "disposed" });
  });

  it("fails the current epoch when a canonical mutation is only partially applied", async () => {
    const pty = new ManualPty();
    pty.resizeError = new Error("resize failed");
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });

    expect(() => session.resize({ cols: 100, rows: 30, widthPx: 800, heightPx: 600 })).toThrow(
      "resize failed",
    );
    await expect(session.waitForExit()).resolves.toEqual({
      status: "failed",
      message: "resize failed",
    });
    await expect(
      session.submitPaste(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1" },
        "ignored",
      ),
    ).resolves.toMatchObject({ status: "rejected" });
  });

  it("owns snapshot bytes independently of the authority backing memory", async () => {
    const backing = Uint8Array.of(1, 2, 3);
    const authority = new (class extends FakeTerminalAuthority {
      override encodeSnapshot(): Uint8Array {
        return backing;
      }
    })();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      pty: new ManualPty(),
      sessionEpoch: 1n,
    });

    const snapshot = await session.captureSnapshot();
    backing.fill(9);

    expect(snapshot.bytes).toEqual(Uint8Array.of(1, 2, 3));
  });
});
