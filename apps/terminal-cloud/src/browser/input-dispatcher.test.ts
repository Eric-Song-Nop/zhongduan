import type { TerminalInputEvent, TerminalMouseInputEvent } from "@wterm/core";
import type { ClientControlFrame, ServerControlFrame } from "@zhongduan/protocol";
import { describe, expect, it, vi } from "vitest";

import { InputDispatcher } from "./input-dispatcher";
import type {
  BrowserInputLifecycleProbe,
  BrowserPresentationDiagnostics,
} from "./presentation-diagnostics";

const RESIZE = { type: "resize", cols: 100, rows: 30, widthPx: 900, heightPx: 600 } as const;

type InputAck = Extract<ServerControlFrame, { type: "input-ack" }>;

function probe(id: string): BrowserInputLifecycleProbe {
  return { id } as unknown as BrowserInputLifecycleProbe;
}

function acknowledgement(
  inputEpoch: string,
  clientInputSeq: string,
  status: InputAck["status"] = "written",
): InputAck {
  return {
    type: "input-ack",
    inputEpoch,
    clientInputSeq,
    status,
    authorityEventSeq: "0",
  };
}

function presentationMock(begin: (kind: TerminalInputEvent["type"]) => unknown) {
  const presentation = {
    beginInputDispatch: vi.fn(begin),
    markInputQueued: vi.fn(),
    recordInputSendDecision: vi.fn(),
    recordInputAck: vi.fn(),
    finishInput: vi.fn(),
    endInputEpoch: vi.fn(),
  };
  return {
    presentation: presentation as unknown as BrowserPresentationDiagnostics,
    spies: presentation,
  };
}

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

  it("starts a sampled semantic dispatch before the queued microtask and pairs its exact ACK", () => {
    const sampled = probe("exact");
    const order: string[] = [];
    const { presentation, spies } = presentationMock(() => {
      order.push("begin");
      return sampled;
    });
    spies.markInputQueued.mockImplementation(() => order.push("queued"));
    spies.recordInputSendDecision.mockImplementation(() => order.push("send-decision"));
    spies.recordInputAck.mockImplementation(() => order.push("ack"));
    const microtasks: Array<() => void> = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_exact_input1",
      presentation,
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport(() => {
      order.push("sender");
      return true;
    }, "lease_exact_input1");

    dispatcher.send(key());

    expect(order).toEqual(["begin", "queued"]);
    expect(spies.beginInputDispatch).toHaveBeenCalledWith("key");
    expect(spies.recordInputSendDecision).not.toHaveBeenCalled();
    microtasks.splice(0).forEach((callback) => callback());
    expect(order).toEqual(["begin", "queued", "sender", "send-decision"]);
    expect(spies.recordInputSendDecision).toHaveBeenCalledWith(
      sampled,
      { inputEpoch: "epoch_exact_input1", clientInputSeq: "1" },
      "sent",
    );

    dispatcher.acceptAcknowledgement(acknowledgement("epoch_exact_input1", "1", "duplicate"));

    expect(order.at(-1)).toBe("ack");
    expect(spies.recordInputAck).toHaveBeenCalledWith({
      inputEpoch: "epoch_exact_input1",
      clientInputSeq: "1",
      status: "duplicate",
    });
  });

  it("ends sampled local failures at their exact decision boundary", () => {
    const notWritable = probe("not-writable");
    const unavailable = presentationMock(() => notWritable);
    const detached = new InputDispatcher({
      getObservedEventSeq: () => 0n,
      inputEpoch: "epoch_not_writable1",
      presentation: unavailable.presentation,
    });
    detached.send(key());
    expect(unavailable.spies.finishInput).toHaveBeenCalledWith(notWritable, "not-writable");

    const gatedProbe = probe("gated");
    const gated = presentationMock(() => gatedProbe);
    const gatedDispatcher = new InputDispatcher({
      getObservedEventSeq: () => 0n,
      inputEpoch: "epoch_mouse_gated1",
      presentation: gated.presentation,
    });
    gatedDispatcher.attachTransport(() => true, "lease_mouse_gated1");
    gatedDispatcher.send(mouse("press", 1));
    expect(gated.spies.finishInput).toHaveBeenCalledWith(gatedProbe, "policy-rejected");

    const invalidProbe = probe("invalid");
    const invalid = presentationMock(() => invalidProbe);
    const invalidMicrotasks: Array<() => void> = [];
    const invalidDispatcher = new InputDispatcher({
      getObservedEventSeq: () => 0n,
      inputEpoch: "epoch_invalid_input1",
      presentation: invalid.presentation,
      queueMicrotask: (callback) => invalidMicrotasks.push(callback),
    });
    invalidDispatcher.attachTransport(() => true, "lease_invalid_input1");
    invalidDispatcher.send({ type: "text", text: "x".repeat(1024 * 1024 + 1), source: "input" });
    invalidMicrotasks.splice(0).forEach((callback) => callback());
    expect(invalid.spies.finishInput).toHaveBeenCalledWith(invalidProbe, "validation-failed");
    expect(invalid.spies.recordInputSendDecision).not.toHaveBeenCalled();

    const falseProbe = probe("false");
    const returnedFalse = presentationMock(() => falseProbe);
    const falseMicrotasks: Array<() => void> = [];
    const falseDispatcher = new InputDispatcher({
      getObservedEventSeq: () => 0n,
      inputEpoch: "epoch_sender_false1",
      presentation: returnedFalse.presentation,
      queueMicrotask: (callback) => falseMicrotasks.push(callback),
    });
    falseDispatcher.attachTransport(() => false, "lease_sender_false1");
    falseDispatcher.send(key());
    falseMicrotasks.splice(0).forEach((callback) => callback());
    expect(returnedFalse.spies.recordInputSendDecision).toHaveBeenCalledWith(
      falseProbe,
      { inputEpoch: "epoch_sender_false1", clientInputSeq: "1" },
      "rejected",
    );

    const throwProbe = probe("throw");
    const threw = presentationMock(() => throwProbe);
    const throwMicrotasks: Array<() => void> = [];
    const throwDispatcher = new InputDispatcher({
      getObservedEventSeq: () => 0n,
      inputEpoch: "epoch_sender_throw1",
      presentation: threw.presentation,
      queueMicrotask: (callback) => throwMicrotasks.push(callback),
    });
    throwDispatcher.attachTransport(() => {
      throw new Error("send failed");
    }, "lease_sender_throw1");
    throwDispatcher.send(key());
    throwMicrotasks.splice(0).forEach((callback) => callback());
    expect(threw.spies.recordInputSendDecision).toHaveBeenCalledWith(
      throwProbe,
      { inputEpoch: "epoch_sender_throw1", clientInputSeq: "1" },
      "uncertain",
    );
  });

  it("finishes a sampled mouse move when a newer adjacent move coalesces it", () => {
    const firstMove = probe("move-1");
    const secondMove = probe("move-2");
    const moves = [firstMove, secondMove];
    const { presentation, spies } = presentationMock((kind) =>
      kind === "mouse" ? moves.shift() : undefined,
    );
    const microtasks: Array<() => void> = [];
    const frames: ClientControlFrame[] = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_mouse_sample1",
      presentation,
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport((frame) => (frames.push(frame), true), "lease_mouse_sample1");
    dispatcher.send(RESIZE);
    microtasks.splice(0).forEach((callback) => callback());
    dispatcher.setReplicaCurrent(true);
    dispatcher.confirmAuthoritativeResize(RESIZE);

    dispatcher.send(mouse("move", 1));
    dispatcher.send(mouse("move", 2));

    expect(spies.finishInput).toHaveBeenCalledWith(firstMove, "coalesced");
    microtasks.splice(0).forEach((callback) => callback());
    expect(frames.at(-1)).toMatchObject({ type: "mouse", action: "move", surface: { x: 2 } });
    expect(spies.recordInputSendDecision).toHaveBeenCalledWith(
      secondMove,
      { inputEpoch: "epoch_mouse_sample1", clientInputSeq: "2" },
      "sent",
    );
  });

  it("finishes the sampled identity evicted by the pending ACK capacity", () => {
    const oldest = probe("oldest");
    let dispatches = 0;
    const { presentation, spies } = presentationMock(() =>
      dispatches++ === 0 ? oldest : undefined,
    );
    const microtasks: Array<() => void> = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_pending_capacity1",
      presentation,
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport(() => true, "lease_pending_capacity1");

    for (let index = 0; index < 1_025; index += 1) dispatcher.send(key());
    microtasks.splice(0).forEach((callback) => callback());

    expect(dispatcher.status.pending).toBe(1_024);
    expect(spies.finishInput).toHaveBeenCalledWith(oldest, "pending-capacity");
  });

  it("does not forward stale or duplicate ACKs to sampled diagnostics", () => {
    const sampled = probe("ack-once");
    const { presentation, spies } = presentationMock(() => sampled);
    const microtasks: Array<() => void> = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_ack_once123",
      presentation,
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport(() => true, "lease_ack_once123");
    dispatcher.send(key());
    microtasks.splice(0).forEach((callback) => callback());

    dispatcher.acceptAcknowledgement(acknowledgement("epoch_stale_ack1", "1"));
    dispatcher.acceptAcknowledgement(acknowledgement("epoch_ack_once123", "1"));
    dispatcher.acceptAcknowledgement(acknowledgement("epoch_ack_once123", "1", "duplicate"));

    expect(spies.recordInputAck).toHaveBeenCalledTimes(1);
    expect(spies.recordInputAck).toHaveBeenCalledWith({
      inputEpoch: "epoch_ack_once123",
      clientInputSeq: "1",
      status: "written",
    });
  });

  it("finishes sampled pending input when attach replaces the writable transport", () => {
    const sampled = probe("transport-replaced");
    const { presentation, spies } = presentationMock(() => sampled);
    const microtasks: Array<() => void> = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_transport_swap1",
      presentation,
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport(() => true, "lease_transport_one1");
    dispatcher.send(key());
    microtasks.splice(0).forEach((callback) => callback());

    dispatcher.attachTransport(() => true, "lease_transport_two2");

    expect(spies.finishInput).toHaveBeenCalledWith(sampled, "transport-replaced");
    expect(dispatcher.status.pending).toBe(0);
  });

  it("ends diagnostics at the explicit input-epoch and transport boundaries", () => {
    const { presentation, spies } = presentationMock(() => undefined);
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_boundary_one1",
      createInputEpoch: () => "epoch_boundary_two2",
      presentation,
    });

    dispatcher.startNewInputEpoch();
    dispatcher.detachTransport();

    expect(spies.endInputEpoch.mock.calls).toEqual([["input-epoch-ended"], ["transport-replaced"]]);
  });

  it("keeps unsampled raw input behavior byte-for-byte equivalent", () => {
    const { presentation, spies } = presentationMock(() => undefined);
    const baselineTasks: Array<() => void> = [];
    const observedTasks: Array<() => void> = [];
    const baselineFrames: ClientControlFrame[] = [];
    const observedFrames: ClientControlFrame[] = [];
    const baseline = new InputDispatcher({
      getObservedEventSeq: () => 17n,
      inputEpoch: "epoch_raw_equiv123",
      queueMicrotask: (callback) => baselineTasks.push(callback),
    });
    const observed = new InputDispatcher({
      getObservedEventSeq: () => 17n,
      inputEpoch: "epoch_raw_equiv123",
      presentation,
      queueMicrotask: (callback) => observedTasks.push(callback),
    });
    baseline.attachTransport((frame) => (baselineFrames.push(frame), true), "lease_raw_equiv123");
    observed.attachTransport((frame) => (observedFrames.push(frame), true), "lease_raw_equiv123");
    const events: TerminalInputEvent[] = [
      key(),
      { type: "text", text: "中", source: "composition" },
      { type: "paste", text: "paste" },
      { type: "focus", focused: true },
      RESIZE,
    ];

    for (const event of events) {
      baseline.send(event);
      observed.send(event);
    }
    baselineTasks.splice(0).forEach((callback) => callback());
    observedTasks.splice(0).forEach((callback) => callback());

    expect(observedFrames).toEqual(baselineFrames);
    expect(observed.status).toEqual(baseline.status);
    expect(spies.markInputQueued).not.toHaveBeenCalled();
    expect(spies.recordInputSendDecision).not.toHaveBeenCalled();
    expect(spies.finishInput).not.toHaveBeenCalled();
  });

  it("does not create a semantic dispatch for the resize replayed on attach", () => {
    const sampled = probe("original-resize");
    const { presentation, spies } = presentationMock(() => sampled);
    const microtasks: Array<() => void> = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch_resize_replay1",
      presentation,
      queueMicrotask: (callback) => microtasks.push(callback),
    });

    dispatcher.send(RESIZE);
    dispatcher.attachTransport(() => true, "lease_resize_replay1");
    microtasks.splice(0).forEach((callback) => callback());

    expect(spies.beginInputDispatch).toHaveBeenCalledTimes(1);
    expect(spies.finishInput).toHaveBeenCalledWith(sampled, "not-writable");
    expect(spies.markInputQueued).not.toHaveBeenCalled();
    expect(spies.recordInputSendDecision).not.toHaveBeenCalled();
  });
});
