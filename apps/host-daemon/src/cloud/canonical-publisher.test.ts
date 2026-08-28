import { decodeDataFrame, type ResizePayload } from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";

import { FakeTerminalAuthority } from "../fake-terminal-authority";
import { EventJournal } from "../journal";
import type { PtyProcess } from "../pty-process";
import { TerminalSession } from "../session";
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

function harness(canInterruptRecovery = false) {
  const pty = new ManualPty();
  const session = new TerminalSession({
    authority: new FakeTerminalAuthority(),
    journal: new EventJournal(),
    pty,
    sessionEpoch: 1n,
  });
  const sent: Uint8Array[] = [];
  const failures: string[] = [];
  let pressure = 0;
  let publisher: CanonicalPublisher;
  publisher = new CanonicalPublisher({
    canInterruptRecovery: () => canInterruptRecovery,
    onFailure: (reason) => failures.push(reason),
    onIngress: () => {},
    onRecoveryPressure: () => {
      pressure += 1;
      publisher.resume();
    },
    sendData: (frame) => sent.push(frame.slice()),
    session,
    yieldIo: () => Promise.resolve(),
  });
  return { failures, pressure: () => pressure, pty, publisher, sent, session };
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

  it("flushes exactly through a paused cut and holds C+1 until resume", async () => {
    const target = harness();
    target.publisher.activate(target.publisher.prepare());
    target.pty.emit(Uint8Array.of(0x41));
    await target.publisher.pause();
    target.pty.emit(Uint8Array.of(0x42));
    target.pty.emit(Uint8Array.of(0x43));
    const commit = target.session.cursor;

    await target.publisher.flushThrough(commit);
    target.pty.emit(Uint8Array.of(0x44));
    expect(target.sent.map((frame) => decodeDataFrame(frame).eventSeq)).toEqual([1n, 2n, 3n]);

    target.publisher.resume();
    expect(target.sent.map((frame) => decodeDataFrame(frame).eventSeq)).toEqual([1n, 2n, 3n, 4n]);
  });

  it("fails closed at the 1024-frame ingress bound without a safe pre-marker interrupt", async () => {
    const target = harness();
    target.publisher.activate(target.publisher.prepare());
    await target.publisher.pause();

    for (let index = 0; index < HOST_CANONICAL_QUEUE_LIMITS.maxFrames + 1; index += 1) {
      target.pty.emit(Uint8Array.of(index & 0xff));
    }

    expect(target.failures).toEqual(["canonical publisher queue exceeded"]);
    expect(target.sent).toEqual([]);
  });

  it("accepts one pressure frame when a pre-marker recovery resumes the pump", async () => {
    const target = harness(true);
    target.publisher.activate(target.publisher.prepare());
    await target.publisher.pause();

    for (let index = 0; index < HOST_CANONICAL_QUEUE_LIMITS.maxFrames + 1; index += 1) {
      target.pty.emit(Uint8Array.of(index & 0xff));
    }
    await settle();

    expect(target.pressure()).toBe(1);
    expect(target.failures).toEqual([]);
    expect(target.sent).toHaveLength(HOST_CANONICAL_QUEUE_LIMITS.maxFrames + 1);
  });
});
