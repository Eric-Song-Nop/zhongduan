import type { TerminalInputEvent, TerminalMouseInputEvent } from "@wterm/core";
import type { ClientControlFrame } from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";

import { InputDispatcher } from "./input-dispatcher";

const RESIZE = { type: "resize", cols: 100, rows: 30, widthPx: 900, heightPx: 600 } as const;

function mouse(action: TerminalMouseInputEvent["action"], x: number): TerminalMouseInputEvent {
  return {
    type: "mouse",
    action,
    button: action === "press" || action === "release" ? 0 : null,
    buttons: action === "release" ? 0 : 1,
    modifiers: 0,
    altGraph: false,
    surface: { x, y: 8 },
    cell: { column: 1, row: 1 },
    viewport: {
      columns: 100,
      rows: 30,
      width: 900,
      height: 600,
      cellWidth: 9,
      cellHeight: 20,
    },
  };
}

function key(): TerminalInputEvent {
  return {
    type: "key",
    action: "press",
    altGraph: false,
    code: "KeyA",
    composing: false,
    consumedModifiers: 0,
    key: "a",
    modifiers: 0,
    repeat: false,
    text: "a",
    unshiftedCodepoint: 97,
  };
}

describe("InputDispatcher", () => {
  it("waits for an exact authoritative resize before sending mouse intent", () => {
    const microtasks: Array<() => void> = [];
    const frames: ClientControlFrame[] = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_1234567890",
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport((frame) => (frames.push(frame), true), "lease_1234567890");
    dispatcher.send(RESIZE);
    microtasks.splice(0).forEach((callback) => callback());

    dispatcher.setReplicaCurrent(true);
    dispatcher.confirmAuthoritativeResize(RESIZE);
    const nextResize = {
      type: "resize",
      cols: 120,
      rows: 40,
      widthPx: 1_080,
      heightPx: 800,
    } as const;
    dispatcher.send(nextResize);
    microtasks.splice(0).forEach((callback) => callback());
    dispatcher.send(mouse("press", 5));
    expect(microtasks).toHaveLength(0);

    dispatcher.confirmAuthoritativeResize(RESIZE);
    dispatcher.send(mouse("press", 6));
    expect(microtasks).toHaveLength(0);

    dispatcher.confirmAuthoritativeResize(nextResize);
    dispatcher.send(mouse("press", 6));
    microtasks.splice(0).forEach((callback) => callback());

    expect(frames.map((frame) => frame.type)).toEqual([
      "resize-request",
      "resize-request",
      "mouse",
    ]);
    expect(frames[2]).not.toHaveProperty("cell");
    expect(frames[2]).not.toHaveProperty("viewport");
  });

  it("coalesces only adjacent moves without crossing ordered input", () => {
    const microtasks: Array<() => void> = [];
    const frames: ClientControlFrame[] = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_1234567890",
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.send(RESIZE);
    dispatcher.attachTransport((frame) => (frames.push(frame), true), "lease_1234567890");
    microtasks.splice(0).forEach((callback) => callback());
    dispatcher.setReplicaCurrent(true);
    dispatcher.confirmAuthoritativeResize(RESIZE);
    dispatcher.send(mouse("press", 1));
    dispatcher.send(mouse("move", 2));
    dispatcher.send(mouse("move", 3));
    dispatcher.send(key());
    dispatcher.send(mouse("move", 4));
    dispatcher.send(mouse("move", 5));
    dispatcher.send(mouse("release", 6));
    microtasks.splice(0).forEach((callback) => callback());

    expect(
      frames
        .slice(1)
        .map((frame) =>
          frame.type === "mouse" ? `${frame.type}:${frame.action}:${frame.surface.x}` : frame.type,
        ),
    ).toEqual(["mouse:press:1", "mouse:move:3", "key", "mouse:move:5", "mouse:release:6"]);
  });

  it("resends the latest resize and closes the mouse gate on transport replacement", () => {
    const microtasks: Array<() => void> = [];
    const first: ClientControlFrame[] = [];
    const second: ClientControlFrame[] = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_1234567890",
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport((frame) => (first.push(frame), true), "lease_1234567890");
    dispatcher.send(RESIZE);
    microtasks.splice(0).forEach((callback) => callback());
    dispatcher.setReplicaCurrent(true);
    dispatcher.confirmAuthoritativeResize(RESIZE);
    dispatcher.send(mouse("press", 1));
    microtasks.splice(0).forEach((callback) => callback());

    dispatcher.detachTransport();
    dispatcher.attachTransport((frame) => (second.push(frame), true), "lease_1234567890");
    microtasks.splice(0).forEach((callback) => callback());
    dispatcher.send(mouse("press", 2));
    expect(microtasks).toHaveLength(0);

    dispatcher.confirmAuthoritativeResize(RESIZE);
    dispatcher.send(mouse("press", 3));
    expect(microtasks).toHaveLength(0);
    dispatcher.setReplicaCurrent(true);
    dispatcher.send(mouse("press", 3));
    microtasks.splice(0).forEach((callback) => callback());

    expect(first.map((frame) => frame.type)).toEqual(["resize-request", "mouse"]);
    expect(second.map((frame) => frame.type)).toEqual(["resize-request", "mouse"]);
  });

  it("keeps text and paste distinct and never resends uncertain input", () => {
    const microtasks: Array<() => void> = [];
    const first: ClientControlFrame[] = [];
    const second: ClientControlFrame[] = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_1234567890",
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport((frame) => (first.push(frame), true), "lease_1234567890");
    dispatcher.send({ type: "text", text: "中", source: "composition" });
    dispatcher.send({ type: "paste", text: "pasted" });
    microtasks.splice(0).forEach((callback) => callback());
    expect(first.map((frame) => frame.type)).toEqual(["text", "paste"]);

    dispatcher.detachTransport();
    dispatcher.attachTransport((frame) => (second.push(frame), true), "lease_1234567890");
    microtasks.splice(0).forEach((callback) => callback());
    expect(second).toEqual([]);
    expect(dispatcher.status.lastStatus).toBe("uncertain");
  });

  it("rejects one invalid local event and continues with the ordered queue", () => {
    const microtasks: Array<() => void> = [];
    const frames: ClientControlFrame[] = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_1234567890",
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport((frame) => (frames.push(frame), true), "lease_1234567890");
    dispatcher.send({ type: "text", text: "x".repeat(1024 * 1024 + 1), source: "input" });
    dispatcher.send(key());
    microtasks.splice(0).forEach((callback) => callback());

    expect(frames.map((frame) => frame.type)).toEqual(["key"]);
    expect(dispatcher.status.lastStatus).toBe("rejected");
  });

  it("flushes a full local queue synchronously without dropping keyboard input", () => {
    const microtasks: Array<() => void> = [];
    const frames: ClientControlFrame[] = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_1234567890",
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport((frame) => (frames.push(frame), true), "lease_1234567890");

    for (let index = 0; index < 257; index += 1) dispatcher.send(key());
    microtasks.splice(0).forEach((callback) => callback());

    expect(frames).toHaveLength(257);
    expect(frames.every((frame) => frame.type === "key")).toBe(true);
    expect(dispatcher.status.lastStatus).toBe("idle");
  });

  it("uses the empty replica cursor so Ctrl-C remains available during cold recovery", () => {
    const microtasks: Array<() => void> = [];
    const frames: ClientControlFrame[] = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => null,
      inputEpoch: "epoch_cold_ctrl_c1",
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport((frame) => (frames.push(frame), true), "lease_cold_ctrl_c1");

    dispatcher.send({
      type: "key",
      action: "press",
      altGraph: false,
      code: "KeyC",
      composing: false,
      consumedModifiers: 2,
      key: "c",
      modifiers: 2,
      repeat: false,
      text: "\u0003",
      unshiftedCodepoint: 99,
    });
    microtasks.splice(0).forEach((callback) => callback());

    expect(frames).toHaveLength(1);
    expect(frames[0]).toMatchObject({ type: "key", observedEventSeq: "0", text: "\u0003" });
  });

  it("starts a fresh identity at sequence one for each writer welcome", () => {
    const microtasks: Array<() => void> = [];
    const frames: ClientControlFrame[] = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_writer_first1",
      createInputEpoch: () => "epoch_writer_second",
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport((frame) => (frames.push(frame), true), "lease_writer_first1");
    dispatcher.send(key());
    microtasks.splice(0).forEach((callback) => callback());

    dispatcher.detachTransport();
    dispatcher.startNewInputEpoch();
    dispatcher.attachTransport((frame) => (frames.push(frame), true), "lease_writer_second");
    dispatcher.send(key());
    microtasks.splice(0).forEach((callback) => callback());

    expect(frames).toMatchObject([
      { type: "key", inputEpoch: "epoch_writer_first1", clientInputSeq: "1" },
      { type: "key", inputEpoch: "epoch_writer_second", clientInputSeq: "1" },
    ]);
  });
});
