import {
  DataFrameKind,
  decodeDataFrame,
  decodeRecoveryStartFence,
  encodeRecoveryStartFence,
  type ResizePayload,
} from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";

import { FakeTerminalAuthority } from "../fake-terminal-authority";
import { EventJournal } from "../journal";
import type { PtyProcess } from "../pty-process";
import { TerminalSession, type ReplayCursor } from "../session";
import { CanonicalPublisher, HOST_CANONICAL_QUEUE_LIMITS } from "./canonical-publisher";

class ManualPty implements PtyProcess {
  readonly pid = 42;
  #dataListener: ((data: Uint8Array) => void) | undefined;

  onData(listener: (data: Uint8Array) => void): () => void {
    this.#dataListener = listener;
    return () => {
      if (this.#dataListener === listener) this.#dataListener = undefined;
    };
  }

  onExit(_listener: (exitCode: number, signal: number) => void): () => void {
    return () => {};
  }

  write(_data: Uint8Array): void {}

  resize(_dimensions: ResizePayload): void {}

  kill(): void {}

  emit(data: Uint8Array): void {
    this.#dataListener?.(data);
  }
}

function harness(yieldIo: () => Promise<void> = () => Promise.resolve()) {
  const pty = new ManualPty();
  const journal = new EventJournal();
  const session = new TerminalSession({
    authority: new FakeTerminalAuthority(),
    journal,
    pty,
    sessionEpoch: 1n,
  });
  const sent: Uint8Array[] = [];
  const failures: string[] = [];
  const publisher = new CanonicalPublisher({
    onFailure: (reason) => failures.push(reason),
    sendData: (frame) => sent.push(frame.slice()),
    session,
    yieldIo,
  });
  return { failures, journal, pty, publisher, sent, session };
}

function recoveryStartFence(cursor: ReplayCursor, identity = "AAAAAAAAAAA"): Uint8Array {
  return encodeRecoveryStartFence({
    type: "recovery-start-fence",
    recoveryId: `recovery_${identity}`,
    connectionId: "connection_AAAAAAAAA",
    deliveryGeneration: "3",
    streamId: 42,
    engineId: "engine",
    base: { sessionEpoch: cursor.sessionEpoch.toString(), eventSeq: "0", nextPtyOffset: "0" },
    source: { kind: "warm" },
    committedThrough: {
      sessionEpoch: cursor.sessionEpoch.toString(),
      eventSeq: cursor.lastEventSeq.toString(),
      nextPtyOffset: cursor.nextPtyOffset.toString(),
    },
    liveFloor: {
      sessionEpoch: cursor.sessionEpoch.toString(),
      nextEventSeq: (cursor.lastEventSeq + 1n).toString(),
      nextPtyOffset: cursor.nextPtyOffset.toString(),
    },
  });
}

function deliveryOrder(frames: readonly Uint8Array[]): string[] {
  return frames.map((encoded) => {
    const frame = decodeDataFrame(encoded);
    return frame.kind === DataFrameKind.RecoveryStartFence
      ? `fence:${decodeRecoveryStartFence(encoded).recoveryId}`
      : `event:${frame.eventSeq}`;
  });
}

async function settle(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
  await Promise.resolve();
}

describe("CanonicalPublisher", () => {
  it("holds output until the exact host-ready cursor is activated", () => {
    const target = harness();
    const baseline = target.publisher.prepare();
    target.pty.emit(Uint8Array.of(0x41));

    expect(target.sent).toEqual([]);
    expect(() =>
      target.publisher.activate({
        sessionEpoch: 1n,
        lastEventSeq: 1n,
        nextPtyOffset: 1n,
      }),
    ).toThrow(/does not match/);
    target.publisher.activate(baseline);
    expect(target.sent.map((frame) => decodeDataFrame(frame).eventSeq)).toEqual([1n]);
  });

  it("inserts a queued recovery fence exactly between H and H+1", () => {
    const target = harness();
    const baseline = target.publisher.prepare();
    target.pty.emit(Uint8Array.of(0x41));
    const committedThrough = target.session.cursor;

    expect(
      target.publisher.tryEnqueueRecoveryStartFence(recoveryStartFence(committedThrough)),
    ).toBe(true);
    target.pty.emit(Uint8Array.of(0x42));
    expect(target.session.eventSeq).toBe(2n);
    expect(target.journal.entries()).toHaveLength(2);
    expect(target.sent).toEqual([]);

    target.publisher.activate(baseline);
    expect(deliveryOrder(target.sent)).toEqual([
      "event:1",
      "fence:recovery_AAAAAAAAAAA",
      "event:2",
    ]);
  });

  it("sends a recovery fence immediately when the active queue is empty", async () => {
    const target = harness();
    target.publisher.activate(target.publisher.prepare());
    target.pty.emit(Uint8Array.of(0x41));
    await settle();

    expect(
      target.publisher.tryEnqueueRecoveryStartFence(recoveryStartFence(target.session.cursor)),
    ).toBe(true);
    expect(deliveryOrder(target.sent)).toEqual(["event:1", "fence:recovery_AAAAAAAAAAA"]);
  });

  it("preserves FIFO for multiple ordered markers at the same H", () => {
    const target = harness();
    const baseline = target.publisher.prepare();
    target.pty.emit(Uint8Array.of(0x41));
    const committedThrough = target.session.cursor;

    expect(
      target.publisher.tryEnqueueRecoveryStartFence(
        recoveryStartFence(committedThrough, "AAAAAAAAAAA"),
      ),
    ).toBe(true);
    expect(
      target.publisher.tryEnqueueRecoveryStartFence(
        recoveryStartFence(committedThrough, "BBBBBBBBBBB"),
      ),
    ).toBe(true);
    target.pty.emit(Uint8Array.of(0x42));
    target.publisher.activate(baseline);

    expect(deliveryOrder(target.sent)).toEqual([
      "event:1",
      "fence:recovery_AAAAAAAAAAA",
      "fence:recovery_BBBBBBBBBBB",
      "event:2",
    ]);
  });

  it("retains marker ordering while the pump is yielding", async () => {
    let releaseYield!: () => void;
    const yielded = new Promise<void>((resolve) => {
      releaseYield = resolve;
    });
    const target = harness(() => yielded);
    target.publisher.activate(target.publisher.prepare());
    for (let index = 0; index < 65; index += 1) {
      target.pty.emit(Uint8Array.of(index));
    }
    await Promise.resolve();

    expect(target.sent).toHaveLength(65);
    const committedThrough = target.session.cursor;
    expect(
      target.publisher.tryEnqueueRecoveryStartFence(recoveryStartFence(committedThrough)),
    ).toBe(true);
    target.pty.emit(Uint8Array.of(0x42));
    releaseYield();
    await settle();

    expect(deliveryOrder(target.sent).slice(-3)).toEqual([
      "event:65",
      "fence:recovery_AAAAAAAAAAA",
      "event:66",
    ]);
  });

  it("rejects malformed, mismatched, and over-budget recovery fences without stopping", () => {
    const target = harness();
    target.publisher.prepare();
    target.pty.emit(Uint8Array.of(0x41));

    expect(target.publisher.tryEnqueueRecoveryStartFence(Uint8Array.of(0x00))).toBe(false);
    expect(
      target.publisher.tryEnqueueRecoveryStartFence(
        recoveryStartFence({
          ...target.session.cursor,
          lastEventSeq: target.session.cursor.lastEventSeq + 1n,
        }),
      ),
    ).toBe(false);

    for (let index = 1; index < HOST_CANONICAL_QUEUE_LIMITS.maxFrames - 1; index += 1) {
      target.pty.emit(Uint8Array.of(index & 0xff));
    }
    expect(
      target.publisher.tryEnqueueRecoveryStartFence(recoveryStartFence(target.session.cursor)),
    ).toBe(true);
    expect(
      target.publisher.tryEnqueueRecoveryStartFence(
        recoveryStartFence(target.session.cursor, "BBBBBBBBBBB"),
      ),
    ).toBe(false);
    expect(target.failures).toEqual([]);
  });

  it("fails closed at the 1024-frame ingress bound before host-ready", () => {
    const target = harness();
    target.publisher.prepare();

    for (let index = 0; index < HOST_CANONICAL_QUEUE_LIMITS.maxFrames + 1; index += 1) {
      target.pty.emit(Uint8Array.of(index & 0xff));
    }

    expect(target.failures).toEqual(["canonical publisher queue exceeded"]);
    expect(target.sent).toEqual([]);
  });
});
