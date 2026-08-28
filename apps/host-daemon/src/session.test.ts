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
import { TerminalSession, type SubmittedInputResult } from "./session";

class ManualPty implements PtyProcess {
  readonly pid = 42;
  readonly writes: Uint8Array[] = [];
  readonly resizes: ResizePayload[] = [];
  resizeError: Error | undefined;
  writeError: Error | undefined;

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
    if (this.writeError !== undefined) throw this.writeError;
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
      encodeMs: 3,
      engineId: authority.engineId,
      nextPtyOffset: 1n,
      sessionEpoch: 3n,
      timing: {
        actorPauseMs: 3,
        authorityEncodeExportMs: 1,
        ownershipCopyMs: 1,
        queueWaitMs: 1,
      },
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
          writerFence: 1n,
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
          writerFence: 1n,
        },
        enter,
      ),
    ).resolves.toMatchObject({ status: "duplicate" });
    await session.submitPaste(
      { clientId: "client-1", clientInputSeq: 2n, inputEpoch: "writer-1", writerFence: 1n },
      "next",
    );
    const evicted = await session.submitKey(
      {
        clientId: "client-1",
        clientInputSeq: 1n,
        inputEpoch: "writer-1",
        observedEventSeq: 0n,
        writerFence: 1n,
      },
      enter,
    );
    expect(evicted).toMatchObject({
      status: "uncertain",
      timing: {
        effectStage: "not-attempted",
        ptyWriteAttempted: false,
        ptyBytes: 0,
      },
    });
    await expect(
      session.submitKey(
        {
          clientId: "client-1",
          clientInputSeq: 3n,
          inputEpoch: "writer-1",
          observedEventSeq: 99n,
          writerFence: 1n,
        },
        enter,
      ),
    ).resolves.toMatchObject({ status: "rejected" });

    expect(pty.writes).toEqual([new TextEncoder().encode("\r"), new TextEncoder().encode("next")]);
  });

  it("accepts more than 256 monotonically increasing writer fences", async () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });

    for (let fence = 1n; fence <= 300n; fence += 1n) {
      await expect(
        session.submitPaste(
          {
            clientId: `client-${fence}`,
            clientInputSeq: 1n,
            inputEpoch: `writer-${fence}`,
            writerFence: fence,
          },
          `input-${fence}`,
        ),
      ).resolves.toMatchObject({ status: "written" });
    }
    await expect(
      session.submitPaste(
        { clientId: "client-299", clientInputSeq: 2n, inputEpoch: "writer-299", writerFence: 299n },
        "stale",
      ),
    ).resolves.toMatchObject({ status: "rejected" });

    expect(pty.writes).toHaveLength(300);
  });

  it("binds one input epoch to each fence and advances before rejecting a bad first sequence", async () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });

    await expect(
      session.submitPaste(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        "first",
      ),
    ).resolves.toMatchObject({ status: "written" });
    await expect(
      session.submitPaste(
        { clientId: "client-1", clientInputSeq: 2n, inputEpoch: "changed", writerFence: 1n },
        "same-fence-new-epoch",
      ),
    ).resolves.toMatchObject({ status: "rejected" });
    await expect(
      session.submitPaste(
        { clientId: "client-2", clientInputSeq: 2n, inputEpoch: "writer-2", writerFence: 2n },
        "bad-first-sequence",
      ),
    ).resolves.toMatchObject({ status: "rejected" });
    await expect(
      session.submitPaste(
        { clientId: "client-1", clientInputSeq: 2n, inputEpoch: "writer-1", writerFence: 1n },
        "old-fence",
      ),
    ).resolves.toMatchObject({ status: "rejected" });
    await expect(
      session.submitPaste(
        { clientId: "client-2", clientInputSeq: 1n, inputEpoch: "writer-2", writerFence: 2n },
        "second",
      ),
    ).resolves.toMatchObject({ status: "written" });

    expect(pty.writes).toEqual([
      new TextEncoder().encode("first"),
      new TextEncoder().encode("second"),
    ]);
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
      writerFence: 1n,
    };
    const dimensions = { cols: 100, rows: 30, widthPx: 800, heightPx: 600 };

    const written = await session.submitResize(identity, dimensions);
    expect(written).toMatchObject({
      status: "written",
      authorityEventSeq: 1n,
      timing: {
        inputKind: "resize",
        effectStage: "completed",
        encodeKind: "resize",
        ptyResizeAttempted: true,
        ptyWriteAttempted: false,
        ptyBytes: 0,
      },
    });
    const duplicate = await session.submitResize(identity, dimensions);
    expect(duplicate).toMatchObject({
      status: "duplicate",
      authorityEventSeq: 1n,
      timing: {
        effectStage: "not-attempted",
        ptyResizeAttempted: false,
        ptyWriteAttempted: false,
      },
    });

    expect(pty.resizes).toEqual([dimensions]);
    expect(session.eventSeq).toBe(1n);
  });

  it("writes text as raw UTF-8 and shares dedup across text, focus, and mouse", async () => {
    const pty = new ManualPty();
    let focusCalls = 0;
    let mouseCalls = 0;
    const authority = new (class extends FakeTerminalAuthority {
      override encodePaste(data: string): Uint8Array {
        return new TextEncoder().encode(`[paste:${data}]`);
      }

      override encodeFocus(focused: boolean): Uint8Array {
        focusCalls += 1;
        return new TextEncoder().encode(focused ? "focus-in" : "focus-out");
      }

      override encodeMouse(): Uint8Array {
        mouseCalls += 1;
        return new TextEncoder().encode("mouse");
      }
    })();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    const first = {
      clientId: "client-1",
      clientInputSeq: 1n,
      inputEpoch: "writer-1",
      writerFence: 1n,
    };
    const second = { ...first, clientInputSeq: 2n };

    await expect(session.submitText(first, "你好")).resolves.toMatchObject({ status: "written" });
    await expect(session.submitFocus(first, true)).resolves.toMatchObject({ status: "duplicate" });
    await expect(
      session.submitMouse(second, {
        action: "press",
        altGraph: false,
        button: 0,
        buttons: 1,
        modifiers: 0,
        surface: { x: 10, y: 10 },
      }),
    ).resolves.toMatchObject({ status: "written" });
    await expect(session.submitPaste(second, "ignored")).resolves.toMatchObject({
      status: "duplicate",
    });

    expect(pty.writes).toEqual([
      new TextEncoder().encode("你好"),
      new TextEncoder().encode("mouse"),
    ]);
    expect(focusCalls).toBe(0);
    expect(mouseCalls).toBe(1);
  });

  it("advances an unbound fence before payload validation and binds only a valid seq 1", async () => {
    const pty = new ManualPty();
    let mouseCalls = 0;
    const authority = new (class extends FakeTerminalAuthority {
      override encodeMouse(): Uint8Array {
        mouseCalls += 1;
        return Uint8Array.of(1);
      }
    })();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    session.resize({ cols: 80, rows: 24, widthPx: 0, heightPx: 0 });

    await expect(
      session.submitText(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        "first",
      ),
    ).resolves.toMatchObject({ status: "written" });

    await expect(
      session.submitMouse(
        { clientId: "client-2", clientInputSeq: 1n, inputEpoch: "malformed", writerFence: 2n },
        {
          action: "press",
          altGraph: false,
          button: 0,
          buttons: 1,
          modifiers: 0,
          surface: { x: 10, y: 10 },
        },
      ),
    ).resolves.toMatchObject({ status: "rejected" });
    await expect(
      session.submitText(
        { clientId: "client-1", clientInputSeq: 2n, inputEpoch: "writer-1", writerFence: 1n },
        "old-fence",
      ),
    ).resolves.toMatchObject({ status: "rejected" });
    await expect(
      session.submitText(
        { clientId: "client-3", clientInputSeq: 1n, inputEpoch: "writer-2", writerFence: 2n },
        "alive",
      ),
    ).resolves.toMatchObject({ status: "written" });
    await expect(
      session.submitText(
        { clientId: "client-3", clientInputSeq: 2n, inputEpoch: "changed", writerFence: 2n },
        "changed",
      ),
    ).resolves.toMatchObject({ status: "rejected" });

    expect(mouseCalls).toBe(0);
    expect(pty.writes).toEqual([
      new TextEncoder().encode("first"),
      new TextEncoder().encode("alive"),
    ]);
  });

  it("marks a stateful encoder throw uncertain and fails the authority epoch closed", async () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new (class extends FakeTerminalAuthority {
        override encodeFocus(): Uint8Array {
          throw new Error("focus encoder failed after entry");
        }
      })(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });

    const result = await session.submitFocus(
      { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
      false,
    );
    expect(result).toMatchObject({
      status: "uncertain",
      timing: {
        effectStage: "threw",
        encodeKind: "ghostty",
        ptyWriteAttempted: false,
        ptyBytes: 0,
      },
    });
    await expect(session.waitForExit()).resolves.toEqual({
      status: "failed",
      message: "focus encoder failed after entry",
    });
    expect(pty.writes).toEqual([]);
  });

  it("distinguishes zero-byte input from a failed PTY write attempt", async () => {
    const zeroPty = new ManualPty();
    const zeroSession = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty: zeroPty,
      sessionEpoch: 1n,
    });
    const zero = await zeroSession.submitFocus(
      { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
      true,
    );
    expect(zero).toMatchObject({
      status: "written",
      timing: {
        effectStage: "completed",
        encodeKind: "ghostty",
        ptyWriteAttempted: false,
        ptyBytes: 0,
      },
    });

    const failingPty = new ManualPty();
    failingPty.writeError = new Error("secret PTY failure");
    const failingSession = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty: failingPty,
      sessionEpoch: 2n,
    });
    const failed = await failingSession.submitText(
      { clientId: "client-2", clientInputSeq: 1n, inputEpoch: "writer-2", writerFence: 1n },
      "private",
    );
    expect(failed).toMatchObject({
      status: "uncertain",
      timing: {
        effectStage: "threw",
        encodeKind: "utf8",
        ptyWriteAttempted: true,
        ptyBytes: 7,
      },
    });
    await expect(failingSession.waitForExit()).resolves.toMatchObject({ status: "failed" });
  });

  it("runs the snapshot fence before draining reentrant PTY output", async () => {
    const pty = new ManualPty();
    const order: string[] = [];
    const authority = new (class extends FakeTerminalAuthority {
      override encodeSnapshot(): Uint8Array {
        const bytes = super.encodeSnapshot();
        pty.emit(Uint8Array.of(0x42));
        return bytes;
      }
    })();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    session.subscribe((encoded) => order.push(`event:${decodeDataFrame(encoded).eventSeq}`));
    pty.emit(Uint8Array.of(0x41));

    const snapshot = await session.captureSnapshotWithFence((capture) => {
      order.push(`fence:${capture.cutEventSeq}`);
      session.writeRawInput(Uint8Array.of(0x03));
    });

    expect(snapshot.cutEventSeq).toBe(1n);
    expect(order).toEqual(["event:1", "fence:1", "event:2"]);
    expect(pty.writes).toEqual([Uint8Array.of(0x03)]);
  });

  it("returns submitted input timing without observing terminal content in the actor", async () => {
    let now = 0;
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      monotonicNow: () => now,
      pty,
      sessionEpoch: 1n,
    });
    const identity = {
      clientId: "client-1",
      clientInputSeq: 1n,
      inputEpoch: "writer-1",
      writerFence: 1n,
    };
    let input!: Promise<SubmittedInputResult>;

    await session.captureSnapshotWithFence(() => {
      now = 10;
      input = session.submitText(identity, "secret-terminal-input");
      now = 15;
    });
    const written = await input;
    const duplicate = await session.submitText(identity, "must-not-be-recorded");

    expect(written).toMatchObject({
      status: "written",
      timing: {
        inputKind: "text",
        effectStage: "completed",
        encodeKind: "utf8",
        actorQueueWaitMs: 5,
        actorProcessingMs: 0,
        inputEncodeMs: 0,
        ptyBytes: new TextEncoder().encode("secret-terminal-input").byteLength,
        ptyWriteAttempted: true,
        ptyWriteMs: 0,
      },
    });
    expect(duplicate).toMatchObject({
      status: "duplicate",
      timing: {
        inputKind: "text",
        effectStage: "not-attempted",
        encodeKind: "utf8",
        actorQueueWaitMs: 0,
        actorProcessingMs: 0,
        ptyBytes: 0,
        ptyWriteAttempted: false,
        ptyWriteMs: 0,
      },
    });
    const serialized = JSON.stringify([written, duplicate], (_key, value) =>
      typeof value === "bigint" ? value.toString() : value,
    );
    expect(serialized).not.toContain("secret-terminal-input");
    expect(serialized).not.toContain("must-not-be-recorded");
  });

  it("separates authority encoding from the PTY write call on one Host clock", async () => {
    let now = 0;
    const authority = new (class extends FakeTerminalAuthority {
      override encodeKey(input: Parameters<FakeTerminalAuthority["encodeKey"]>[0]): Uint8Array {
        const encoded = super.encodeKey(input);
        now += 2;
        return encoded;
      }
    })();
    const pty = new (class extends ManualPty {
      override write(data: Uint8Array): void {
        now += 3;
        super.write(data);
      }
    })();
    const session = new TerminalSession({
      authority,
      journal: new EventJournal(),
      monotonicNow: () => now,
      pty,
      sessionEpoch: 1n,
    });

    const result = await session.submitKey(
      { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
      {
        action: "press",
        altGraph: false,
        code: "KeyA",
        composing: false,
        consumedModifiers: 0,
        key: "a",
        modifiers: 0,
        text: "a",
      },
    );

    expect(result).toMatchObject({
      status: "written",
      timing: {
        actorProcessingMs: 5,
        inputEncodeMs: 2,
        ptyWriteAttempted: true,
        ptyWriteMs: 3,
        ptyBytes: 1,
      },
    });
  });

  it("fails the authority epoch when Ghostty snapshot encoding throws", async () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new (class extends FakeTerminalAuthority {
        override encodeSnapshot(): Uint8Array {
          throw new Error("snapshot encoder invariant failed");
        }
      })(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });

    await expect(session.captureSnapshot()).rejects.toThrow("snapshot encoder invariant failed");
    await expect(session.waitForExit()).resolves.toEqual({
      status: "failed",
      message: "snapshot encoder invariant failed",
    });
    await expect(
      session.submitText(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        "ignored",
      ),
    ).resolves.toMatchObject({ status: "rejected" });
  });

  it("fails the authority epoch when Ghostty returns an empty snapshot", async () => {
    const session = new TerminalSession({
      authority: new (class extends FakeTerminalAuthority {
        override encodeSnapshot(): Uint8Array {
          return new Uint8Array();
        }
      })(),
      journal: new EventJournal(),
      pty: new ManualPty(),
      sessionEpoch: 1n,
    });

    await expect(session.captureSnapshot()).rejects.toThrow("empty snapshot");
    await expect(session.waitForExit()).resolves.toEqual({
      status: "failed",
      message: "terminal authority returned an empty snapshot",
    });
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
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
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
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
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
