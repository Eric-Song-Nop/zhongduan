import type { TerminalInputEvent, TerminalMouseInputEvent } from "@wterm/core";
import type { ClientControlFrame } from "@zhongduan/protocol";
import { describe, expect, it } from "vitest";

import {
  INPUT_QUEUE_CONTRACT,
  InputDispatcher,
  type InputIdentity,
  type InputIntentResult,
  type InputTransportSendResult,
} from "./input-dispatcher";

type InputFrame = Exclude<ClientControlFrame, { type: "ack" | "attach" | "writer-lease-renew" }>;

const RESIZE = { type: "resize", cols: 100, rows: 30, widthPx: 900, heightPx: 600 } as const;

function key(overrides: Partial<Extract<TerminalInputEvent, { type: "key" }>> = {}) {
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
    ...overrides,
  } satisfies Extract<TerminalInputEvent, { type: "key" }>;
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

class ManualClock {
  now = 0;
  #nextTimer = 1;
  readonly timers = new Map<number, { at: number; callback: () => void }>();

  readonly setTimer = (callback: () => void, delayMs: number): ReturnType<typeof setTimeout> => {
    const id = this.#nextTimer++;
    this.timers.set(id, { at: this.now + delayMs, callback });
    return id as unknown as ReturnType<typeof setTimeout>;
  };

  readonly clearTimer = (timer: ReturnType<typeof setTimeout>): void => {
    this.timers.delete(timer as unknown as number);
  };

  advance(milliseconds: number): void {
    const target = this.now + milliseconds;
    for (;;) {
      let next: [number, { at: number; callback: () => void }] | undefined;
      for (const entry of this.timers) {
        if (entry[1].at <= target && (next === undefined || entry[1].at < next[1].at)) {
          next = entry;
        }
      }
      if (next === undefined) break;
      this.now = next[1].at;
      this.timers.delete(next[0]);
      next[1].callback();
    }
    this.now = target;
  }

  elapseWithoutTimers(milliseconds: number): void {
    this.now += milliseconds;
  }
}

interface Harness {
  readonly clock: ManualClock;
  readonly dispatcher: InputDispatcher;
  readonly frames: InputFrame[];
  readonly microtasks: Array<() => void>;
  readonly results: InputIntentResult[];
  attach(options?: {
    decision?: InputTransportSendResult;
    fence?: string;
    generation?: number;
    onSend?: (frame: InputFrame) => InputTransportSendResult;
  }): boolean;
  flush(): void;
  runNext(): void;
}

function harness(
  options: {
    assertInvariants?: boolean;
    getObservedEventSeq?: () => bigint | null;
    onIntentResult?: (result: InputIntentResult) => void;
    policy?: (event: TerminalInputEvent) => boolean;
  } = {},
): Harness {
  const clock = new ManualClock();
  const frames: InputFrame[] = [];
  const microtasks: Array<() => void> = [];
  const results: InputIntentResult[] = [];
  let localSequence = 0;
  let epochSequence = 1;
  const dispatcher = new InputDispatcher({
    ...(options.assertInvariants === undefined
      ? {}
      : { assertInvariants: options.assertInvariants }),
    getObservedEventSeq: options.getObservedEventSeq ?? (() => 9n),
    inputEpoch: "epoch-1",
    createInputEpoch: () => `epoch-${++epochSequence}`,
    createLocalIntentId: () => `local-${++localSequence}`,
    now: () => clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    queueMicrotask: (callback) => microtasks.push(callback),
    scheduleSendDecision: (callback) => microtasks.push(callback),
    ...(options.policy === undefined ? {} : { policy: options.policy }),
    onIntentResult: (result) => {
      results.push(result);
      options.onIntentResult?.(result);
    },
  });
  const flush = () => {
    let iterations = 0;
    while (microtasks.length > 0) {
      if (++iterations > 20_000) throw new Error("microtask loop did not quiesce");
      microtasks.shift()!();
    }
  };
  return {
    clock,
    dispatcher,
    frames,
    microtasks,
    results,
    attach: ({ decision = "accepted", fence = "1", generation = Number(fence), onSend } = {}) =>
      dispatcher.attachTransport({
        generation,
        writerFence: fence,
        writerLease: `lease-${fence}`,
        sender: (frame) => {
          frames.push(frame);
          return onSend?.(frame) ?? decision;
        },
      }),
    flush,
    runNext: () => microtasks.shift()?.(),
  };
}

function identityFor(dispatcher: InputDispatcher, localIntentId: string): InputIdentity {
  const identity = dispatcher.getIntent(localIntentId)?.identity;
  if (identity === null || identity === undefined) throw new Error("intent has no identity");
  return identity;
}

function acknowledge(
  dispatcher: InputDispatcher,
  identity: InputIdentity,
  status: "duplicate" | "rejected" | "uncertain" | "written" = "written",
): boolean {
  return dispatcher.acceptAcknowledgement({
    type: "input-ack",
    ...identity,
    status,
    authorityEventSeq: "12",
  });
}

describe("InputDispatcher E1 admission owner", () => {
  it("keeps the non-coalescible key path in one owner turn", () => {
    const microtasks: Array<() => void> = [];
    const frames: InputFrame[] = [];
    const dispatcher = new InputDispatcher({
      getObservedEventSeq: () => 9n,
      inputEpoch: "epoch-inline",
      createLocalIntentId: () => "local-inline",
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    expect(
      dispatcher.attachTransport({
        generation: 1,
        writerFence: "1",
        writerLease: "lease-1",
        sender: (frame) => (frames.push(frame), "accepted"),
      }),
    ).toBe(true);

    const localIntentId = dispatcher.send(key());

    expect(microtasks).toEqual([]);
    expect(frames).toMatchObject([{ type: "key", clientInputSeq: "1" }]);
    expect(dispatcher.getIntent(localIntentId)).toMatchObject({ state: "sent" });
  });

  it("fails closed before admission when transport identity fields exceed wire bounds", () => {
    const setup = harness();
    expect(
      setup.dispatcher.attachTransport({
        generation: 1,
        writerFence: "18446744073709551616",
        writerLease: "lease",
        sender: () => "accepted",
      }),
    ).toBe(false);
    expect(
      setup.dispatcher.attachTransport({
        generation: 2,
        writerFence: "1",
        writerLease: "x".repeat(129),
        sender: () => "accepted",
      }),
    ).toBe(false);
    const localIntentId = setup.dispatcher.send(key());
    expect(setup.dispatcher.getResult(localIntentId)).toMatchObject({
      identity: null,
      outcome: "not-sent",
      reason: "not-writable",
    });
  });

  it("allocates Browser identity only after admission and converges an ACK exactly once", () => {
    const setup = harness();
    expect(setup.attach()).toBe(true);

    const localIntentId = setup.dispatcher.send(key());
    expect(setup.dispatcher.getIntent(localIntentId)).toMatchObject({
      state: "queued",
      identity: expect.objectContaining({ clientInputSeq: "1" }),
    });
    setup.flush();

    expect(setup.frames).toMatchObject([
      { type: "key", inputEpoch: "epoch-1", clientInputSeq: "1" },
    ]);
    const identity = identityFor(setup.dispatcher, localIntentId);
    expect(identity).toEqual({ writerFence: "1", inputEpoch: "epoch-1", clientInputSeq: "1" });
    expect(acknowledge(setup.dispatcher, identity)).toBe(true);
    setup.flush();

    expect(setup.results).toEqual([
      expect.objectContaining({
        localIntentId,
        identity,
        outcome: "deterministic",
        reason: "input-ack",
        result: "written",
      }),
    ]);
    expect(Object.isFrozen(setup.results[0])).toBe(true);
    expect(acknowledge(setup.dispatcher, identity, "rejected")).toBe(false);
    expect(setup.dispatcher.getResult(localIntentId)).toBe(setup.results[0]);
  });

  it("does not advance sequence for malformed, schema, size, policy, or authority rejection", () => {
    const setup = harness({ policy: (event) => event.type !== "focus" });
    const malformed = Object.defineProperty({}, "type", {
      get: () => {
        throw new Error("hostile getter");
      },
    }) as TerminalInputEvent;

    const rejected = [
      setup.dispatcher.send(malformed),
      setup.dispatcher.send(key({ modifiers: 0, consumedModifiers: 2 })),
      setup.dispatcher.send({ type: "text", text: "x".repeat(1024 * 1024 + 1), source: "input" }),
      setup.dispatcher.send({ type: "focus", focused: true }),
      setup.dispatcher.send(key()),
    ];
    expect(rejected.map((id) => setup.dispatcher.getResult(id)?.identity)).toEqual([
      null,
      null,
      null,
      null,
      null,
    ]);

    setup.attach();
    const accepted = setup.dispatcher.send(key());
    setup.flush();
    expect(identityFor(setup.dispatcher, accepted).clientInputSeq).toBe("1");
    expect(setup.frames).toHaveLength(1);
    expect(setup.results.map((result) => result.reason)).toEqual([
      "malformed",
      "validation",
      "validation",
      "policy",
      "not-writable",
    ]);
  });

  it("atomically bounds queue count and leaves no sequence gap after overload", () => {
    const setup = harness();
    setup.attach();
    const ids = Array.from({ length: INPUT_QUEUE_CONTRACT.maxCount + 1 }, () =>
      setup.dispatcher.send(key()),
    );
    expect(setup.dispatcher.getResult(ids.at(-1)!)).toMatchObject({
      outcome: "not-sent",
      reason: "overload",
      identity: null,
    });

    setup.flush();
    expect(setup.frames).toHaveLength(INPUT_QUEUE_CONTRACT.maxCount);
    expect(setup.frames.at(-1)).toMatchObject({ clientInputSeq: "256" });
    const next = setup.dispatcher.send(key());
    setup.flush();
    expect(identityFor(setup.dispatcher, next).clientInputSeq).toBe("257");
  });

  it("atomically bounds aggregate encoded queue bytes without allocating the overflow identity", () => {
    const setup = harness();
    setup.attach();
    const maximumPaste = "x".repeat(1024 * 1024);
    const ids = Array.from({ length: 7 }, () =>
      setup.dispatcher.send({ type: "paste", text: maximumPaste }),
    );

    expect(setup.dispatcher.status.preAdmissionBytes).toBeLessThanOrEqual(
      INPUT_QUEUE_CONTRACT.maxBytes,
    );
    expect(setup.dispatcher.getResult(ids.at(-1)!)).toMatchObject({
      outcome: "not-sent",
      reason: "overload",
      identity: null,
    });
    setup.flush();
    expect(setup.frames).toHaveLength(6);
    expect(setup.frames.at(-1)).toMatchObject({ clientInputSeq: "6" });

    const next = setup.dispatcher.send(key());
    setup.flush();
    expect(identityFor(setup.dispatcher, next).clientInputSeq).toBe("7");
  });

  it("expires pre-admission work without allocating a sequence", () => {
    const setup = harness();
    setup.attach();
    const expired = setup.dispatcher.send({ type: "text", text: "expires", source: "input" });

    setup.clock.advance(INPUT_QUEUE_CONTRACT.maxPreAdmissionAgeMs);
    setup.flush();
    expect(setup.dispatcher.getResult(expired)).toMatchObject({
      outcome: "not-sent",
      reason: "pre-admission-expired",
      identity: null,
    });
    expect(setup.frames).toEqual([]);

    const next = setup.dispatcher.send(key());
    setup.flush();
    expect(identityFor(setup.dispatcher, next).clientInputSeq).toBe("1");
  });

  it("bounds pending ACK identities without consuming the rejected sequence", () => {
    const setup = harness();
    setup.attach();
    const ids: string[] = [];
    for (let index = 0; index < INPUT_QUEUE_CONTRACT.maxPending; index += 1) {
      ids.push(setup.dispatcher.send(key()));
      setup.flush();
    }
    const overloaded = setup.dispatcher.send(key());
    setup.flush();
    expect(setup.dispatcher.getResult(overloaded)).toMatchObject({
      outcome: "not-sent",
      reason: "overload",
      identity: null,
    });

    acknowledge(setup.dispatcher, identityFor(setup.dispatcher, ids[0]!));
    const next = setup.dispatcher.send(key());
    setup.flush();
    expect(identityFor(setup.dispatcher, next).clientInputSeq).toBe("1025");
  });

  it("coalesces resize and adjacent mouse moves only before identity allocation", () => {
    const setup = harness();
    setup.attach();
    const oldResize = setup.dispatcher.send(RESIZE);
    const currentResize = { ...RESIZE, cols: 120, widthPx: 1_080 } as const;
    const newResize = setup.dispatcher.send(currentResize);
    expect(setup.dispatcher.getResult(oldResize)).toMatchObject({
      outcome: "not-sent",
      reason: "superseded",
      identity: null,
    });
    setup.flush();
    expect(identityFor(setup.dispatcher, newResize).clientInputSeq).toBe("1");
    expect(setup.frames).toMatchObject([{ type: "resize-request", cols: 120 }]);

    setup.dispatcher.setReplicaCurrent(true);
    setup.dispatcher.confirmAuthoritativeResize(currentResize);
    const firstMove = setup.dispatcher.send(mouse("move", 1));
    const secondMove = setup.dispatcher.send(mouse("move", 2));
    setup.flush();
    expect(setup.dispatcher.getResult(firstMove)).toMatchObject({
      reason: "superseded",
      identity: null,
    });
    expect(identityFor(setup.dispatcher, secondMove).clientInputSeq).toBe("2");
    expect(setup.frames.at(-1)).toMatchObject({ type: "mouse", action: "move", surface: { x: 2 } });
  });

  it("never silently coalesces an already admitted identity", () => {
    const setup = harness();
    setup.attach();
    const first = setup.dispatcher.send(RESIZE);
    setup.runNext(); // admission; keep the send-decision microtask pending
    expect(setup.dispatcher.getIntent(first)).toMatchObject({ state: "queued" });

    const second = setup.dispatcher.send({ ...RESIZE, cols: 101 });
    setup.flush();
    expect(setup.frames).toMatchObject([
      { type: "resize-request", cols: 100, clientInputSeq: "1" },
      { type: "resize-request", cols: 101, clientInputSeq: "2" },
    ]);
    expect(setup.dispatcher.getResult(first)).toBeUndefined();
    expect(setup.dispatcher.getResult(second)).toBeUndefined();
  });

  it("returns observable mouse-gate results and strips replica-only coordinates", () => {
    const setup = harness();
    setup.attach();
    const noReplica = setup.dispatcher.send(mouse("press", 1));
    setup.dispatcher.setReplicaCurrent(true);
    const noResize = setup.dispatcher.send(mouse("press", 2));
    setup.dispatcher.send(RESIZE);
    setup.flush();
    setup.dispatcher.confirmAuthoritativeResize(RESIZE);
    const accepted = setup.dispatcher.send(mouse("press", 3));
    setup.flush();

    expect(setup.dispatcher.getResult(noReplica)).toMatchObject({
      reason: "replica-not-current",
      identity: null,
    });
    expect(setup.dispatcher.getResult(noResize)).toMatchObject({
      reason: "mouse-gate",
      identity: null,
    });
    expect(setup.dispatcher.getIntent(accepted)?.identity).not.toBeNull();
    expect(setup.frames.at(-1)).not.toHaveProperty("cell");
    expect(setup.frames.at(-1)).not.toHaveProperty("viewport");
  });

  it("classifies pre-admission, queued, sent, and ACKed work across full replacement", () => {
    const pre = harness();
    pre.attach();
    const preId = pre.dispatcher.send({ type: "text", text: "pre", source: "input" });
    pre.dispatcher.detachTransport();
    expect(pre.dispatcher.getResult(preId)).toMatchObject({
      outcome: "not-sent",
      reason: "control-replaced",
      identity: null,
    });

    const queued = harness();
    queued.attach();
    const queuedId = queued.dispatcher.send(key());
    queued.dispatcher.detachTransport();
    expect(queued.dispatcher.getResult(queuedId)).toMatchObject({
      outcome: "not-sent",
      reason: "control-replaced",
      identity: expect.objectContaining({ clientInputSeq: "1" }),
    });

    const sent = harness();
    sent.attach();
    const sentId = sent.dispatcher.send(key());
    sent.flush();
    sent.dispatcher.detachTransport();
    expect(sent.dispatcher.getIntent(sentId)).toMatchObject({ state: "terminating" });
    expect(sent.dispatcher.getResult(sentId)).toBeUndefined();

    const acked = harness();
    acked.attach();
    const ackedId = acked.dispatcher.send(key());
    acked.flush();
    acknowledge(acked.dispatcher, identityFor(acked.dispatcher, ackedId));
    const result = acked.dispatcher.getResult(ackedId);
    acked.dispatcher.detachTransport();
    expect(acked.dispatcher.getResult(ackedId)).toBe(result);
  });

  it("keeps data-only replacement inside the same input transport and epoch", () => {
    const setup = harness();
    setup.attach();
    const first = setup.dispatcher.send(key());
    setup.flush();
    setup.dispatcher.noteDataTransportReplacement();
    const second = setup.dispatcher.send(key());
    setup.flush();

    expect(identityFor(setup.dispatcher, first)).toMatchObject({
      inputEpoch: "epoch-1",
      clientInputSeq: "1",
    });
    expect(identityFor(setup.dispatcher, second)).toMatchObject({
      inputEpoch: "epoch-1",
      clientInputSeq: "2",
    });
  });

  it("seals a proven-not-accepted identity and starts sequence one only on a higher fence", () => {
    const setup = harness();
    setup.attach({ decision: "proven-not-accepted", fence: "2" });
    const failed = setup.dispatcher.send(key());
    setup.flush();
    expect(setup.dispatcher.getResult(failed)).toMatchObject({
      outcome: "not-sent",
      reason: "transport-rejected",
      identity: expect.objectContaining({ clientInputSeq: "1" }),
    });
    expect(setup.dispatcher.status).toMatchObject({
      writable: false,
      controlReplacementRequired: true,
    });
    const sealed = setup.dispatcher.send(key());
    expect(setup.dispatcher.getResult(sealed)).toMatchObject({
      reason: "not-writable",
      identity: null,
    });

    expect(setup.attach({ fence: "2", generation: 3 })).toBe(false);
    expect(setup.dispatcher.status).toMatchObject({
      writable: false,
      controlReplacementRequired: true,
    });
    const stillSealed = setup.dispatcher.send(key());
    expect(setup.dispatcher.getResult(stillSealed)).toMatchObject({
      outcome: "not-sent",
      reason: "not-writable",
      identity: null,
    });
    expect(setup.attach({ fence: "1", generation: 4 })).toBe(false);
    expect(setup.dispatcher.status.controlReplacementRequired).toBe(true);
    expect(setup.attach({ fence: "3", generation: 5 })).toBe(true);
    expect(setup.dispatcher.status.controlReplacementRequired).toBe(false);
    const recovered = setup.dispatcher.send(key());
    setup.flush();
    expect(identityFor(setup.dispatcher, recovered)).toEqual({
      writerFence: "3",
      inputEpoch: "epoch-2",
      clientInputSeq: "1",
    });
  });

  it("waits a bounded interval for proof after transport uncertainty", () => {
    const setup = harness();
    setup.attach({ decision: "uncertain" });
    const localIntentId = setup.dispatcher.send(key());
    setup.flush();
    const identity = identityFor(setup.dispatcher, localIntentId);
    expect(setup.dispatcher.getIntent(localIntentId)).toMatchObject({ state: "terminating" });
    expect(setup.dispatcher.acceptTombstoneProof({ ...identity, authorityEventSeq: "13" })).toBe(
      true,
    );
    setup.flush();
    expect(setup.dispatcher.getResult(localIntentId)).toMatchObject({
      outcome: "deterministic",
      reason: "tombstone-proof",
      result: "rejected",
    });
  });

  it("converges sent age through terminating timeout and ignores a late ACK", () => {
    const setup = harness();
    setup.attach();
    const localIntentId = setup.dispatcher.send(key());
    setup.flush();
    const identity = identityFor(setup.dispatcher, localIntentId);
    expect(setup.clock.timers.size).toBe(1);

    setup.clock.advance(INPUT_QUEUE_CONTRACT.maxSentAgeMs);
    expect(setup.dispatcher.getIntent(localIntentId)).toMatchObject({ state: "terminating" });
    expect(setup.dispatcher.status.controlReplacementRequired).toBe(true);
    setup.clock.advance(INPUT_QUEUE_CONTRACT.maxTerminationAgeMs);
    setup.flush();
    const result = setup.dispatcher.getResult(localIntentId);
    expect(result).toMatchObject({ outcome: "uncertain", reason: "termination-timeout" });
    expect(acknowledge(setup.dispatcher, identity)).toBe(false);
    expect(setup.dispatcher.getResult(localIntentId)).toBe(result);
    expect(setup.results.filter((item) => item.localIntentId === localIntentId)).toHaveLength(1);
    expect(setup.clock.timers.size).toBe(0);
  });

  it("enforces elapsed sent and proof deadlines before accepting an ACK", () => {
    const setup = harness();
    setup.attach();
    const localIntentId = setup.dispatcher.send(key());
    setup.flush();
    const identity = identityFor(setup.dispatcher, localIntentId);

    setup.clock.elapseWithoutTimers(
      INPUT_QUEUE_CONTRACT.maxSentAgeMs + INPUT_QUEUE_CONTRACT.maxTerminationAgeMs + 1,
    );

    expect(acknowledge(setup.dispatcher, identity)).toBe(false);
    expect(setup.dispatcher.getResult(localIntentId)).toMatchObject({
      outcome: "uncertain",
      reason: "termination-timeout",
    });
    expect(setup.dispatcher.status).toMatchObject({
      writable: false,
      controlReplacementRequired: true,
    });
  });

  it("seals an elapsed sent epoch before admitting another input", () => {
    const setup = harness();
    setup.attach();
    const first = setup.dispatcher.send(key());
    setup.flush();

    setup.clock.elapseWithoutTimers(INPUT_QUEUE_CONTRACT.maxSentAgeMs + 1);
    const second = setup.dispatcher.send(key());

    expect(setup.dispatcher.getIntent(first)).toMatchObject({ state: "terminating" });
    expect(setup.dispatcher.getResult(second)).toMatchObject({
      identity: null,
      outcome: "not-sent",
      reason: "not-writable",
    });
    expect(setup.frames).toHaveLength(1);
    expect(setup.dispatcher.status.controlReplacementRequired).toBe(true);
  });

  it("enforces an elapsed proof deadline before accepting a tombstone", () => {
    const setup = harness();
    setup.attach({ decision: "uncertain" });
    const localIntentId = setup.dispatcher.send(key());
    setup.flush();
    const identity = identityFor(setup.dispatcher, localIntentId);

    setup.clock.elapseWithoutTimers(INPUT_QUEUE_CONTRACT.maxTerminationAgeMs + 1);

    expect(setup.dispatcher.acceptTombstoneProof({ ...identity, authorityEventSeq: "13" })).toBe(
      false,
    );
    expect(setup.dispatcher.getResult(localIntentId)).toMatchObject({
      outcome: "uncertain",
      reason: "transport-uncertain",
    });
  });

  it("expires an admitted queue item before sender invocation and never replays it", () => {
    const setup = harness();
    setup.attach();
    const localIntentId = setup.dispatcher.send(key());
    expect(setup.dispatcher.getIntent(localIntentId)).toMatchObject({ state: "queued" });

    setup.clock.advance(INPUT_QUEUE_CONTRACT.maxQueuedAgeMs);
    setup.flush();
    expect(setup.frames).toEqual([]);
    expect(setup.dispatcher.getResult(localIntentId)).toMatchObject({
      outcome: "not-sent",
      reason: "queue-expired",
      identity: expect.objectContaining({ clientInputSeq: "1" }),
    });
    expect(setup.dispatcher.status.controlReplacementRequired).toBe(true);
  });

  it("treats a throwing sender as acceptance-uncertain", () => {
    const setup = harness();
    setup.attach({
      onSend: () => {
        throw new Error("WebSocket.send failed");
      },
    });
    const localIntentId = setup.dispatcher.send(key());
    setup.flush();
    expect(setup.dispatcher.getIntent(localIntentId)).toMatchObject({ state: "terminating" });
    setup.clock.advance(INPUT_QUEUE_CONTRACT.maxTerminationAgeMs);
    setup.flush();
    expect(setup.dispatcher.getResult(localIntentId)).toMatchObject({
      outcome: "uncertain",
      reason: "transport-uncertain",
    });
  });

  it("does not let reentrant observed-cursor replacement enqueue on an old transport", () => {
    const firstFrames: InputFrame[] = [];
    const secondFrames: InputFrame[] = [];
    const microtasks: Array<() => void> = [];
    let replace = false;
    let dispatcher!: InputDispatcher;
    dispatcher = new InputDispatcher({
      inputEpoch: "epoch-first",
      createInputEpoch: () => "epoch-second",
      createLocalIntentId: () => "local-reentrant",
      getObservedEventSeq: () => {
        if (replace) {
          replace = false;
          dispatcher.detachTransport();
          dispatcher.attachTransport({
            generation: 2,
            writerFence: "2",
            writerLease: "lease-2",
            sender: (frame) => (secondFrames.push(frame), "accepted"),
          });
        }
        return 9n;
      },
      queueMicrotask: (callback) => microtasks.push(callback),
    });
    dispatcher.attachTransport({
      generation: 1,
      writerFence: "1",
      writerLease: "lease-1",
      sender: (frame) => (firstFrames.push(frame), "accepted"),
    });

    replace = true;
    const localIntentId = dispatcher.send(key());
    while (microtasks.length > 0) microtasks.shift()!();
    expect(firstFrames).toEqual([]);
    expect(secondFrames).toHaveLength(1);
    expect(identityFor(dispatcher, localIntentId)).toEqual({
      writerFence: "2",
      inputEpoch: "epoch-second",
      clientInputSeq: "1",
    });
  });

  it("does not let a reentrant sender return overwrite replacement termination", () => {
    const setup = harness();
    setup.attach({
      onSend: () => {
        setup.dispatcher.detachTransport();
        return "accepted";
      },
    });
    const localIntentId = setup.dispatcher.send(key());
    setup.flush();
    expect(setup.dispatcher.getIntent(localIntentId)).toMatchObject({ state: "terminating" });
    expect(setup.dispatcher.getResult(localIntentId)).toBeUndefined();
  });

  it("notifies result observers synchronously after owner state is committed", () => {
    let dispatcher!: InputDispatcher;
    let followup: string | null = null;
    let observedCommitted = false;
    const setup = harness({
      onIntentResult: (result) => {
        if (result.reason === "validation") {
          observedCommitted = dispatcher.getResult(result.localIntentId) === result;
          followup = dispatcher.send(key());
        }
      },
    });
    dispatcher = setup.dispatcher;
    setup.attach();
    setup.dispatcher.send(key({ modifiers: 0, consumedModifiers: 2 }));

    expect(observedCommitted).toBe(true);
    expect(followup).not.toBeNull();
    setup.flush();

    expect(identityFor(setup.dispatcher, followup!).clientInputSeq).toBe("1");
    expect(setup.frames).toHaveLength(1);
  });

  it("does not accumulate a deferred notification queue for synchronous local rejection", () => {
    const microtasks: Array<() => void> = [];
    let delivered = 0;
    const dispatcher = new InputDispatcher({
      // This test measures 50,000 committed notifications. The randomized command trace below
      // performs the deep per-turn invariant audit; repeating a full 4,096-record scan here would
      // measure the test oracle rather than the production transition.
      assertInvariants: false,
      getObservedEventSeq: () => 0n,
      createLocalIntentId: () => "bulk-local-rejection",
      queueMicrotask: (callback) => microtasks.push(callback),
      onIntentResult: () => {
        delivered += 1;
      },
    });
    let first = "";
    let last = "";

    for (let index = 0; index < 50_000; index += 1) {
      const id = dispatcher.send(key());
      if (index === 0) first = id;
      last = id;
    }

    expect(delivered).toBe(50_000);
    expect(microtasks).toEqual([]);
    expect(dispatcher.getResult(first)).toBeUndefined();
    expect(dispatcher.getResult(last)).toMatchObject({
      outcome: "not-sent",
      reason: "not-writable",
    });
  });

  it("creates a new consumption and identity for resize reconciliation", () => {
    const setup = harness();
    const original = setup.dispatcher.send(RESIZE);
    expect(setup.dispatcher.getResult(original)).toMatchObject({
      outcome: "not-sent",
      reason: "not-writable",
      identity: null,
    });

    setup.attach();
    const reconciliation = setup.dispatcher.reconcileLatestResize();
    setup.flush();
    expect(reconciliation).not.toBe(original);
    expect(identityFor(setup.dispatcher, reconciliation!).clientInputSeq).toBe("1");
    expect(setup.frames).toMatchObject([{ type: "resize-request" }]);
  });

  it("retains only the declared bounded terminal-result window", () => {
    const setup = harness();
    const ids = Array.from({ length: INPUT_QUEUE_CONTRACT.maxRetainedResults + 1 }, () =>
      setup.dispatcher.send(key()),
    );
    expect(setup.dispatcher.getIntent(ids[0]!)).toBeUndefined();
    expect(setup.dispatcher.getResult(ids.at(-1)!)).toMatchObject({
      outcome: "not-sent",
      reason: "not-writable",
    });
  });

  it("preserves global invariants across a deterministic interleaving command trace", () => {
    let randomState = 0x45e1cafe;
    const random = () => {
      randomState = (Math.imul(randomState, 1_664_525) + 1_013_904_223) >>> 0;
      return randomState;
    };
    const setup = harness({ assertInvariants: true });
    const localIntentIds: string[] = [];
    const observedResults = new Map<string, InputIntentResult>();
    let highestFence = 1;
    let generation = 1;
    setup.attach({
      fence: String(highestFence),
      generation,
      onSend: () => (["accepted", "proven-not-accepted", "uncertain"] as const)[random() % 3]!,
    });

    for (let step = 0; step < 1_000; step += 1) {
      const command = random() % 12;
      if (command <= 3) {
        const input =
          command === 0
            ? key({ code: `Key${String.fromCharCode(65 + (random() % 26))}` })
            : command === 1
              ? ({ type: "text", text: `trace-${step}`, source: "input" } as const)
              : command === 2
                ? ({ ...RESIZE, cols: 80 + (random() % 80) } as const)
                : mouse(random() % 2 === 0 ? "move" : "press", random() % 900);
        localIntentIds.push(setup.dispatcher.send(input));
      } else if (command === 4) {
        setup.runNext();
      } else if (command === 5) {
        setup.flush();
      } else if (command === 6) {
        const active = localIntentIds
          .map((localIntentId) => setup.dispatcher.getIntent(localIntentId))
          .filter(
            (snapshot): snapshot is NonNullable<typeof snapshot> =>
              snapshot?.identity !== null &&
              snapshot?.identity !== undefined &&
              (snapshot.state === "sent" || snapshot.state === "terminating"),
          );
        const candidate = active[random() % Math.max(1, active.length)];
        if (candidate?.identity !== null && candidate?.identity !== undefined) {
          acknowledge(
            setup.dispatcher,
            candidate.identity,
            (["written", "rejected", "duplicate", "uncertain"] as const)[random() % 4]!,
          );
        }
      } else if (command === 7) {
        setup.dispatcher.detachTransport(random() % 4 === 0 ? "closed" : "control-replaced");
      } else if (command === 8) {
        const useHigherFence = random() % 3 !== 0;
        if (useHigherFence) highestFence += 1;
        generation += 1;
        setup.attach({
          fence: String(useHigherFence ? highestFence : Math.max(1, highestFence - 1)),
          generation,
          onSend: () => (["accepted", "proven-not-accepted", "uncertain"] as const)[random() % 3]!,
        });
      } else if (command === 9) {
        setup.dispatcher.revokeWriterAuthority();
      } else if (command === 10) {
        setup.clock.elapseWithoutTimers(random() % 40_001);
      } else {
        setup.clock.advance(random() % 40_001);
      }

      setup.dispatcher.setReplicaCurrent(random() % 2 === 0);
      if (random() % 5 === 0) setup.dispatcher.confirmAuthoritativeResize(RESIZE);
      for (const result of setup.results) {
        const previous = observedResults.get(result.localIntentId);
        if (previous === undefined) observedResults.set(result.localIntentId, result);
        else expect(result).toBe(previous);
      }
      expect(setup.dispatcher.status).toMatchObject({
        pending: expect.any(Number),
        preAdmission: expect.any(Number),
        queued: expect.any(Number),
      });
      expect(setup.dispatcher.status.pending).toBeLessThanOrEqual(INPUT_QUEUE_CONTRACT.maxPending);
      expect(
        setup.dispatcher.status.preAdmission + setup.dispatcher.status.queued,
      ).toBeLessThanOrEqual(INPUT_QUEUE_CONTRACT.maxCount);
      expect(
        setup.dispatcher.status.preAdmissionBytes + setup.dispatcher.status.queuedBytes,
      ).toBeLessThanOrEqual(INPUT_QUEUE_CONTRACT.maxBytes);
    }

    setup.flush();
    setup.dispatcher.detachTransport("closed");
    setup.flush();
    const sentIdentities = setup.frames.map(
      (frame) => `${frame.writerLease}\u0000${frame.inputEpoch}\u0000${frame.clientInputSeq}`,
    );
    expect(new Set(sentIdentities).size).toBe(sentIdentities.length);
    expect(new Set(setup.results.map((result) => result.localIntentId)).size).toBe(
      setup.results.length,
    );
  });
});
