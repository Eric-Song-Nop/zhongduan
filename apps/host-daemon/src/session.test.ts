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

  it("rejects canonical subscription from a different session epoch", () => {
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
    ).resolves.toMatchObject({ status: "uncertain" });
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

    await expect(
      session.submitFocus(
        { clientId: "client-1", clientInputSeq: 1n, inputEpoch: "writer-1", writerFence: 1n },
        false,
      ),
    ).resolves.toMatchObject({ status: "uncertain" });
    await expect(session.waitForExit()).resolves.toEqual({
      status: "failed",
      message: "focus encoder failed after entry",
    });
    expect(pty.writes).toEqual([]);
  });

  it("prepares an owned gap and commits its H fence before reentrant H+1", async () => {
    const pty = new ManualPty();
    const journal = new EventJournal();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal,
      pty,
      sessionEpoch: 1n,
    });
    const order: string[] = [];
    session.subscribe((encoded) => order.push(`event:${decodeDataFrame(encoded).eventSeq}`));
    pty.emit(Uint8Array.of(0x41));

    const result = await session.prepareRecoveryGap(
      { sessionEpoch: 1n, lastEventSeq: 0n, nextPtyOffset: 0n },
      { maxEncodedBytes: 1024, maxFrames: 1 },
      (gap) => {
        order.push(`fence:${gap.committedThrough.lastEventSeq}`);
        pty.emit(Uint8Array.of(0x42));
        return true;
      },
    );

    expect(result.status).toBe("prepared");
    if (result.status !== "prepared") throw new Error("expected a prepared recovery gap");
    expect(result.gap).toMatchObject({
      base: { sessionEpoch: 1n, lastEventSeq: 0n, nextPtyOffset: 0n },
      committedThrough: { sessionEpoch: 1n, lastEventSeq: 1n, nextPtyOffset: 1n },
      exactFrames: 1,
    });
    expect(result.gap.exactEncodedBytes).toBe(result.gap.frames[0]!.byteLength);
    expect(decodeDataFrame(result.gap.frames[0]!).eventSeq).toBe(1n);
    expect(order).toEqual(["event:1", "fence:1", "event:2"]);
    expect(journal.entries()).toHaveLength(2);
  });

  it("rejects gap and capacity misses before invoking the recovery fence", async () => {
    const pty = new ManualPty();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal: new EventJournal(),
      pty,
      sessionEpoch: 1n,
    });
    pty.emit(Uint8Array.of(0x41));
    let commits = 0;
    const commit = () => {
      commits += 1;
      return true;
    };

    await expect(
      session.prepareRecoveryGap(
        { sessionEpoch: 2n, lastEventSeq: 0n, nextPtyOffset: 0n },
        { maxEncodedBytes: 1024, maxFrames: 1 },
        commit,
      ),
    ).resolves.toEqual({ status: "unavailable", reason: "journal-gap" });
    await expect(
      session.prepareRecoveryGap(
        { sessionEpoch: 1n, lastEventSeq: 0n, nextPtyOffset: 0n },
        { maxEncodedBytes: 1024, maxFrames: 0 },
        commit,
      ),
    ).resolves.toEqual({ status: "unavailable", reason: "capacity" });
    expect(commits).toBe(0);
  });

  it("keeps the session live when a recovery fence refuses or throws", async () => {
    const pty = new ManualPty();
    const journal = new EventJournal();
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal,
      pty,
      sessionEpoch: 1n,
    });
    pty.emit(Uint8Array.of(0x41));
    const base = { sessionEpoch: 1n, lastEventSeq: 0n, nextPtyOffset: 0n };
    const limits = { maxEncodedBytes: 1024, maxFrames: 1 };

    await expect(session.prepareRecoveryGap(base, limits, () => false)).resolves.toEqual({
      status: "unavailable",
      reason: "fence-unavailable",
    });
    await expect(
      session.prepareRecoveryGap(base, limits, () => {
        throw new Error("fence queue full");
      }),
    ).resolves.toEqual({ status: "unavailable", reason: "fence-unavailable" });

    pty.emit(Uint8Array.of(0x42));
    expect(session.eventSeq).toBe(2n);
    expect(journal.entries().map((encoded) => decodeDataFrame(encoded).eventSeq)).toEqual([1n, 2n]);
  });

  it("retains prepared frame ownership after the journal advances and prunes", async () => {
    let now = 0;
    const pty = new ManualPty();
    const journal = new EventJournal({
      maxAgeMs: 1,
      now: () => now,
      segmentAgeMs: 1,
      segmentBytes: 1,
    });
    const session = new TerminalSession({
      authority: new FakeTerminalAuthority(),
      journal,
      pty,
      sessionEpoch: 1n,
    });
    pty.emit(Uint8Array.of(0x41));
    const result = await session.prepareRecoveryGap(
      { sessionEpoch: 1n, lastEventSeq: 0n, nextPtyOffset: 0n },
      { maxEncodedBytes: 1024, maxFrames: 1 },
      () => true,
    );
    if (result.status !== "prepared") throw new Error("expected a prepared recovery gap");

    now = 10;
    pty.emit(Uint8Array.of(0x42));

    expect(journal.entries().map((encoded) => decodeDataFrame(encoded).eventSeq)).toEqual([2n]);
    expect(result.gap.frames).toHaveLength(1);
    expect(decodeDataFrame(result.gap.frames[0]!)).toMatchObject({
      eventSeq: 1n,
      payload: Uint8Array.of(0x41),
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
